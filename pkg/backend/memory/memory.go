// Package memory is an in-process implementation of backend.Backend. It keeps
// the exact same append-only, single-log MVCC semantics as the Spanner backend
// but holds everything in a slice guarded by a mutex. It exists so the server
// logic can be unit-tested and run locally without a Spanner instance
// (--backend=memory). It is NOT durable and NOT for production.
package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/geoffsdesk/ledger/pkg/backend"
)

// row is one entry in the global log. id is the revision.
type row struct {
	id          int64
	name        string
	created     bool
	deleted     bool
	createRev   int64
	prevRev     int64
	lease       int64
	value       []byte
	oldValue    []byte
}

// Backend is an in-memory backend.Backend.
type Backend struct {
	mu        sync.RWMutex
	rows      []row // append-only, ordered by id ascending
	revision  int64
	compactRev int64
}

// New returns an empty in-memory backend.
func New() *Backend { return &Backend{} }

func (b *Backend) Start(context.Context) error { return nil }
func (b *Backend) Close() error                { return nil }

func (b *Backend) CurrentRevision(context.Context) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.revision, nil
}

func (b *Backend) CompactRevision(context.Context) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.compactRev, nil
}

// latestLocked returns the newest row for name at or below rev (rev<=0 = any),
// or nil. The caller holds b.mu.
func (b *Backend) latestLocked(name string, rev int64) *row {
	for i := len(b.rows) - 1; i >= 0; i-- {
		r := &b.rows[i]
		if r.name != name {
			continue
		}
		if rev > 0 && r.id > rev {
			continue
		}
		return r
	}
	return nil
}

func toKV(r *row) *backend.KeyValue {
	if r == nil || r.deleted {
		return nil
	}
	return &backend.KeyValue{
		Key:            r.name,
		CreateRevision: r.createRev,
		ModRevision:    r.id,
		Version:        r.id - r.createRev + 1,
		Value:          r.value,
		Lease:          r.lease,
	}
}

// checkRev validates a requested read revision against compaction/future bounds.
func (b *Backend) checkRev(rev int64) error {
	if rev <= 0 {
		return nil
	}
	if rev < b.compactRev {
		return backend.ErrCompacted
	}
	if rev > b.revision {
		return backend.ErrFutureRev
	}
	return nil
}

func (b *Backend) Get(_ context.Context, key string, rev int64) (int64, *backend.KeyValue, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if err := b.checkRev(rev); err != nil {
		return b.revision, nil, err
	}
	return b.revision, toKV(b.latestLocked(key, rev)), nil
}

func inRange(name, key, end string) bool {
	if name < key {
		return false
	}
	if end == "" { // unbounded
		return true
	}
	return name < end
}

func (b *Backend) List(_ context.Context, key, end string, limit, rev int64) (int64, []*backend.KeyValue, bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if err := b.checkRev(rev); err != nil {
		return b.revision, nil, false, err
	}

	// Collect the latest row per distinct name in range.
	latest := map[string]*row{}
	for i := range b.rows {
		r := &b.rows[i]
		if !inRange(r.name, key, end) {
			continue
		}
		if rev > 0 && r.id > rev {
			continue
		}
		latest[r.name] = &b.rows[i] // rows are in ascending id order, so this keeps the newest
	}

	names := make([]string, 0, len(latest))
	for n, r := range latest {
		if r.deleted {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)

	more := false
	if limit > 0 && int64(len(names)) > limit {
		names = names[:limit]
		more = true
	}
	kvs := make([]*backend.KeyValue, 0, len(names))
	for _, n := range names {
		kvs = append(kvs, toKV(latest[n]))
	}
	return b.revision, kvs, more, nil
}

func (b *Backend) Count(_ context.Context, key, end string, rev int64) (int64, int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if err := b.checkRev(rev); err != nil {
		return b.revision, 0, err
	}
	latest := map[string]*row{}
	for i := range b.rows {
		r := &b.rows[i]
		if !inRange(r.name, key, end) {
			continue
		}
		if rev > 0 && r.id > rev {
			continue
		}
		latest[r.name] = &b.rows[i]
	}
	var count int64
	for _, r := range latest {
		if !r.deleted {
			count++
		}
	}
	return b.revision, count, nil
}

// appendLocked allocates the next revision and appends a row. Caller holds b.mu.
func (b *Backend) appendLocked(r row) int64 {
	b.revision++
	r.id = b.revision
	b.rows = append(b.rows, r)
	return b.revision
}

func (b *Backend) Create(_ context.Context, key string, value []byte, lease int64) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur := b.latestLocked(key, 0); cur != nil && !cur.deleted {
		return b.revision, backend.ErrKeyExists
	}
	prev := int64(0)
	if cur := b.latestLocked(key, 0); cur != nil {
		prev = cur.id
	}
	rev := b.revision + 1
	b.appendLocked(row{
		name:      key,
		created:   true,
		createRev: rev,
		prevRev:   prev,
		lease:     lease,
		value:     value,
	})
	return b.revision, nil
}

func (b *Backend) Update(_ context.Context, key string, value []byte, expectedRev, lease int64) (bool, int64, *backend.KeyValue, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cur := b.latestLocked(key, 0)
	curMod := int64(0)
	if cur != nil && !cur.deleted {
		curMod = cur.id
	}
	if curMod != expectedRev {
		return false, b.revision, toKV(cur), nil
	}
	createRev := b.revision + 1
	var old []byte
	if cur != nil {
		createRev = cur.createRev
		old = cur.value
	}
	rev := b.appendLocked(row{
		name:      key,
		createRev: createRev,
		prevRev:   curMod,
		lease:     lease,
		value:     value,
		oldValue:  old,
	})
	return true, rev, toKV(&b.rows[len(b.rows)-1]), nil
}

func (b *Backend) Delete(_ context.Context, key string, expectedRev int64) (bool, int64, *backend.KeyValue, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cur := b.latestLocked(key, 0)
	curMod := int64(0)
	if cur != nil && !cur.deleted {
		curMod = cur.id
	}
	if curMod != expectedRev {
		return false, b.revision, toKV(cur), nil
	}
	deleted := toKV(cur)
	b.appendLocked(row{
		name:      key,
		deleted:   true,
		createRev: cur.createRev,
		prevRev:   curMod,
		oldValue:  cur.value,
	})
	return true, b.revision, deleted, nil
}

func (b *Backend) After(_ context.Context, key, end string, rev, limit int64) ([]*backend.Event, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var events []*backend.Event
	for i := range b.rows {
		r := &b.rows[i]
		if r.id <= rev {
			continue
		}
		if !inRange(r.name, key, end) {
			continue
		}
		events = append(events, rowEvent(r))
		if limit > 0 && int64(len(events)) >= limit {
			break
		}
	}
	return events, nil
}

func rowEvent(r *row) *backend.Event {
	ev := &backend.Event{Create: r.created, Delete: r.deleted}
	if r.deleted {
		// For a delete, KV carries the key + the revision of the deletion.
		ev.KV = &backend.KeyValue{Key: r.name, ModRevision: r.id, CreateRevision: r.createRev}
		if r.oldValue != nil || r.prevRev != 0 {
			ev.PrevKV = &backend.KeyValue{
				Key:            r.name,
				CreateRevision: r.createRev,
				ModRevision:    r.prevRev,
				Value:          r.oldValue,
			}
		}
	} else {
		ev.KV = &backend.KeyValue{
			Key:            r.name,
			CreateRevision: r.createRev,
			ModRevision:    r.id,
			Version:        r.id - r.createRev + 1,
			Value:          r.value,
			Lease:          r.lease,
		}
		if !r.created {
			ev.PrevKV = &backend.KeyValue{
				Key:            r.name,
				CreateRevision: r.createRev,
				ModRevision:    r.prevRev,
				Value:          r.oldValue,
			}
		}
	}
	return ev
}

func (b *Backend) Compact(_ context.Context, rev int64) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if rev <= b.compactRev {
		return b.revision, nil
	}
	if rev > b.revision {
		rev = b.revision
	}
	// Keep the latest row per name; drop everything else with id <= rev.
	latestID := map[string]int64{}
	for i := range b.rows {
		r := &b.rows[i]
		if r.id > latestID[r.name] {
			latestID[r.name] = r.id
		}
	}
	kept := b.rows[:0:0]
	for i := range b.rows {
		r := b.rows[i]
		superseded := r.id <= rev && r.id != latestID[r.name]
		tombstone := r.deleted && r.id <= rev
		if superseded || tombstone {
			continue
		}
		kept = append(kept, r)
	}
	b.rows = kept
	b.compactRev = rev
	return b.revision, nil
}

func (b *Backend) DeleteByLease(_ context.Context, leaseID int64) ([]*backend.Event, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Find live keys bound to the lease (latest row per name).
	latest := map[string]*row{}
	for i := range b.rows {
		r := &b.rows[i]
		latest[r.name] = &b.rows[i]
	}
	var events []*backend.Event
	for _, cur := range latest {
		if cur.deleted || cur.lease != leaseID {
			continue
		}
		b.appendLocked(row{
			name:      cur.name,
			deleted:   true,
			createRev: cur.createRev,
			prevRev:   cur.id,
			oldValue:  cur.value,
		})
		events = append(events, rowEvent(&b.rows[len(b.rows)-1]))
	}
	return events, nil
}

var _ backend.Backend = (*Backend)(nil)
