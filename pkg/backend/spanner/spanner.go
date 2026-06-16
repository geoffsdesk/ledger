// Package spanner implements backend.Backend on top of Google Cloud Spanner.
//
// Mapping (see schema.go for the DDL):
//   - Each Put/Delete appends a row to `kine`; the row's primary key `id` is the
//     global revision.
//   - The current value of a key is the highest-id row for that name; a row with
//     deleted=TRUE is a tombstone.
//   - A read "as of revision R" filters id <= R.
//   - etcd's compare-and-swap (Txn) becomes a Spanner read-write transaction:
//     read the key's latest mod_revision, compare, and on match append a new row
//     while bumping the revision counter — all atomic and serializable.
//   - A watch is served by polling `id > cursor` (see pkg/server/watch.go).
package spanner

import (
	"context"
	"errors"
	"fmt"
	"math"

	"cloud.google.com/go/spanner"
	"github.com/geoffsdesk/ledger/pkg/backend"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

const (
	revKey     = "revision"
	compactKey = "compact_revision"
)

// Backend is a Spanner-backed backend.Backend.
type Backend struct {
	client *spanner.Client
}

// New connects to the database at dbPath
// ("projects/P/instances/I/databases/D"). When SPANNER_EMULATOR_HOST is set the
// client targets the emulator automatically.
func New(ctx context.Context, dbPath string) (*Backend, error) {
	c, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("spanner client: %w", err)
	}
	return &Backend{client: c}, nil
}

func (b *Backend) Start(ctx context.Context) error {
	// Touch the counter row so reads have a consistent starting point. Missing
	// is treated as revision 0, so this is best-effort connectivity validation.
	_, err := b.CurrentRevision(ctx)
	return err
}

func (b *Backend) Close() error {
	b.client.Close()
	return nil
}

// readCounterRO reads a kine_meta counter inside a read-only snapshot.
func readCounterRO(ctx context.Context, ro *spanner.ReadOnlyTransaction, key string) (int64, error) {
	r, err := ro.ReadRow(ctx, "kine_meta", spanner.Key{key}, []string{"v"})
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return 0, nil
		}
		return 0, err
	}
	var v int64
	if err := r.Column(0, &v); err != nil {
		return 0, err
	}
	return v, nil
}

// readCounterTx reads a kine_meta counter inside a read-write transaction.
func readCounterTx(ctx context.Context, txn *spanner.ReadWriteTransaction, key string) (int64, error) {
	r, err := txn.ReadRow(ctx, "kine_meta", spanner.Key{key}, []string{"v"})
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return 0, nil
		}
		return 0, err
	}
	var v int64
	if err := r.Column(0, &v); err != nil {
		return 0, err
	}
	return v, nil
}

func (b *Backend) CurrentRevision(ctx context.Context) (int64, error) {
	ro := b.client.Single()
	defer ro.Close()
	return readCounterRO(ctx, ro, revKey)
}

func (b *Backend) CompactRevision(ctx context.Context) (int64, error) {
	ro := b.client.Single()
	defer ro.Close()
	return readCounterRO(ctx, ro, compactKey)
}

// latestStmt builds the query for the newest row of a key at or below rev.
func latestStmt(name string, rev int64) spanner.Statement {
	return spanner.Statement{
		SQL: `SELECT id, name, created, deleted, create_revision, prev_revision, lease, value
		      FROM kine
		      WHERE name = @name AND (@rev = 0 OR id <= @rev)
		      ORDER BY id DESC LIMIT 1`,
		Params: map[string]interface{}{"name": name, "rev": rev},
	}
}

// scanLatest reads a single row produced by latestStmt (8 columns).
func scanLatest(r *spanner.Row) (*kvRow, error) {
	var row kvRow
	if err := r.Columns(&row.id, &row.name, &row.created, &row.deleted,
		&row.createRev, &row.prevRev, &row.lease, &row.value); err != nil {
		return nil, err
	}
	return &row, nil
}

type kvRow struct {
	id        int64
	name      string
	created   bool
	deleted   bool
	createRev int64
	prevRev   int64
	lease     int64
	value     []byte
	oldValue  []byte
}

func (r *kvRow) toKV() *backend.KeyValue {
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

// queryLatest runs latestStmt on a transaction (RO or RW share the iterator API).
func queryLatest(ctx context.Context, q interface {
	Query(context.Context, spanner.Statement) *spanner.RowIterator
}, name string, rev int64) (*kvRow, error) {
	iter := q.Query(ctx, latestStmt(name, rev))
	defer iter.Stop()
	r, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return scanLatest(r)
}

func (b *Backend) checkRev(ctx context.Context, ro *spanner.ReadOnlyTransaction, rev int64) (cur int64, err error) {
	cur, err = readCounterRO(ctx, ro, revKey)
	if err != nil {
		return 0, err
	}
	if rev <= 0 {
		return cur, nil
	}
	compact, err := readCounterRO(ctx, ro, compactKey)
	if err != nil {
		return 0, err
	}
	if rev < compact {
		return cur, backend.ErrCompacted
	}
	if rev > cur {
		return cur, backend.ErrFutureRev
	}
	return cur, nil
}

func (b *Backend) Get(ctx context.Context, key string, rev int64) (int64, *backend.KeyValue, error) {
	ro := b.client.ReadOnlyTransaction()
	defer ro.Close()
	cur, err := b.checkRev(ctx, ro, rev)
	if err != nil {
		return cur, nil, err
	}
	row, err := queryLatest(ctx, ro, key, rev)
	if err != nil {
		return cur, nil, err
	}
	return cur, row.toKV(), nil
}

func (b *Backend) List(ctx context.Context, key, end string, limit, rev int64) (int64, []*backend.KeyValue, bool, error) {
	ro := b.client.ReadOnlyTransaction()
	defer ro.Close()
	cur, err := b.checkRev(ctx, ro, rev)
	if err != nil {
		return cur, nil, false, err
	}

	sqlLimit := int64(math.MaxInt64)
	if limit > 0 {
		sqlLimit = limit + 1 // fetch one extra to detect truncation
	}
	stmt := spanner.Statement{
		SQL: `SELECT k.id, k.name, k.create_revision, k.lease, k.value
		      FROM kine AS k
		      JOIN (
		          SELECT name AS n, MAX(id) AS mid
		          FROM kine
		          WHERE name >= @start AND (@end = '' OR name < @end)
		            AND (@rev = 0 OR id <= @rev)
		          GROUP BY name
		      ) AS m ON k.name = m.n AND k.id = m.mid
		      WHERE k.deleted = FALSE
		      ORDER BY k.name
		      LIMIT @limit`,
		Params: map[string]interface{}{"start": key, "end": end, "rev": rev, "limit": sqlLimit},
	}
	iter := ro.Query(ctx, stmt)
	defer iter.Stop()

	var kvs []*backend.KeyValue
	for {
		r, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return cur, nil, false, err
		}
		var row kvRow
		if err := r.Columns(&row.id, &row.name, &row.createRev, &row.lease, &row.value); err != nil {
			return cur, nil, false, err
		}
		kvs = append(kvs, &backend.KeyValue{
			Key:            row.name,
			CreateRevision: row.createRev,
			ModRevision:    row.id,
			Version:        row.id - row.createRev + 1,
			Value:          row.value,
			Lease:          row.lease,
		})
	}

	more := false
	if limit > 0 && int64(len(kvs)) > limit {
		kvs = kvs[:limit]
		more = true
	}
	return cur, kvs, more, nil
}

func (b *Backend) Count(ctx context.Context, key, end string, rev int64) (int64, int64, error) {
	ro := b.client.ReadOnlyTransaction()
	defer ro.Close()
	cur, err := b.checkRev(ctx, ro, rev)
	if err != nil {
		return cur, 0, err
	}
	stmt := spanner.Statement{
		SQL: `SELECT COUNTIF(NOT k.deleted)
		      FROM kine AS k
		      JOIN (
		          SELECT name AS n, MAX(id) AS mid
		          FROM kine
		          WHERE name >= @start AND (@end = '' OR name < @end)
		            AND (@rev = 0 OR id <= @rev)
		          GROUP BY name
		      ) AS m ON k.name = m.n AND k.id = m.mid`,
		Params: map[string]interface{}{"start": key, "end": end, "rev": rev},
	}
	iter := ro.Query(ctx, stmt)
	defer iter.Stop()
	r, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return cur, 0, nil
	}
	if err != nil {
		return cur, 0, err
	}
	var count int64
	if err := r.Column(0, &count); err != nil {
		return cur, 0, err
	}
	return cur, count, nil
}

// insertRowMut builds the mutation for one appended log row.
func insertRowMut(rev int64, name string, created, deleted bool, createRev, prevRev, lease int64, value, oldValue []byte) *spanner.Mutation {
	if value == nil {
		value = []byte{}
	}
	if oldValue == nil {
		oldValue = []byte{}
	}
	return spanner.Insert("kine",
		[]string{"id", "name", "created", "deleted", "create_revision", "prev_revision", "lease", "value", "old_value"},
		[]interface{}{rev, name, created, deleted, createRev, prevRev, lease, value, oldValue})
}

func setCounterMut(key string, v int64) *spanner.Mutation {
	return spanner.InsertOrUpdate("kine_meta", []string{"k", "v"}, []interface{}{key, v})
}

func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	var outRev int64
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		cur, err := queryLatest(ctx, txn, key, 0)
		if err != nil {
			return err
		}
		if cur != nil && !cur.deleted {
			return backend.ErrKeyExists
		}
		counter, err := readCounterTx(ctx, txn, revKey)
		if err != nil {
			return err
		}
		rev := counter + 1
		prev := int64(0)
		if cur != nil {
			prev = cur.id
		}
		outRev = rev
		return txn.BufferWrite([]*spanner.Mutation{
			insertRowMut(rev, key, true, false, rev, prev, lease, value, nil),
			setCounterMut(revKey, rev),
		})
	})
	if err != nil {
		return 0, err
	}
	return outRev, nil
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, expectedRev, lease int64) (bool, int64, *backend.KeyValue, error) {
	var (
		ok     bool
		outRev int64
		outKV  *backend.KeyValue
	)
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		ok = false
		outKV = nil
		cur, err := queryLatest(ctx, txn, key, 0)
		if err != nil {
			return err
		}
		curMod := int64(0)
		if cur != nil && !cur.deleted {
			curMod = cur.id
		}
		if curMod != expectedRev {
			outKV = cur.toKV()
			counter, err := readCounterTx(ctx, txn, revKey)
			if err != nil {
				return err
			}
			outRev = counter
			return nil
		}
		counter, err := readCounterTx(ctx, txn, revKey)
		if err != nil {
			return err
		}
		rev := counter + 1
		createRev := rev
		var old []byte
		if cur != nil {
			createRev = cur.createRev
			old = cur.value
		}
		ok = true
		outRev = rev
		outKV = &backend.KeyValue{
			Key:            key,
			CreateRevision: createRev,
			ModRevision:    rev,
			Version:        rev - createRev + 1,
			Value:          value,
			Lease:          lease,
		}
		return txn.BufferWrite([]*spanner.Mutation{
			insertRowMut(rev, key, false, false, createRev, curMod, lease, value, old),
			setCounterMut(revKey, rev),
		})
	})
	if err != nil {
		return false, 0, nil, err
	}
	return ok, outRev, outKV, nil
}

func (b *Backend) Delete(ctx context.Context, key string, expectedRev int64) (bool, int64, *backend.KeyValue, error) {
	var (
		ok     bool
		outRev int64
		outKV  *backend.KeyValue
	)
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		ok = false
		outKV = nil
		cur, err := queryLatest(ctx, txn, key, 0)
		if err != nil {
			return err
		}
		curMod := int64(0)
		if cur != nil && !cur.deleted {
			curMod = cur.id
		}
		if curMod != expectedRev {
			outKV = cur.toKV()
			counter, err := readCounterTx(ctx, txn, revKey)
			if err != nil {
				return err
			}
			outRev = counter
			return nil
		}
		counter, err := readCounterTx(ctx, txn, revKey)
		if err != nil {
			return err
		}
		rev := counter + 1
		ok = true
		outRev = rev
		outKV = cur.toKV()
		return txn.BufferWrite([]*spanner.Mutation{
			insertRowMut(rev, key, false, true, cur.createRev, curMod, 0, nil, cur.value),
			setCounterMut(revKey, rev),
		})
	})
	if err != nil {
		return false, 0, nil, err
	}
	return ok, outRev, outKV, nil
}

func (b *Backend) After(ctx context.Context, key, end string, rev, limit int64) ([]*backend.Event, error) {
	if limit <= 0 {
		limit = 1000
	}
	stmt := spanner.Statement{
		SQL: `SELECT id, name, created, deleted, create_revision, prev_revision, lease, value, old_value
		      FROM kine
		      WHERE id > @rev
		        AND (@start = '' OR name >= @start)
		        AND (@end = '' OR name < @end)
		      ORDER BY id
		      LIMIT @limit`,
		Params: map[string]interface{}{"rev": rev, "start": key, "end": end, "limit": limit},
	}
	ro := b.client.Single()
	defer ro.Close()
	iter := ro.Query(ctx, stmt)
	defer iter.Stop()

	var events []*backend.Event
	for {
		r, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		var row kvRow
		if err := r.Columns(&row.id, &row.name, &row.created, &row.deleted,
			&row.createRev, &row.prevRev, &row.lease, &row.value, &row.oldValue); err != nil {
			return nil, err
		}
		events = append(events, rowEvent(&row))
	}
	return events, nil
}

func rowEvent(r *kvRow) *backend.Event {
	ev := &backend.Event{Create: r.created, Delete: r.deleted}
	if r.deleted {
		ev.KV = &backend.KeyValue{Key: r.name, ModRevision: r.id, CreateRevision: r.createRev}
		ev.PrevKV = &backend.KeyValue{
			Key:            r.name,
			CreateRevision: r.createRev,
			ModRevision:    r.prevRev,
			Value:          r.oldValue,
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

// Compact deletes superseded rows (id <= rev that are not the latest for their
// key) and fully-compacted tombstones, in batches to stay well under Spanner's
// per-transaction mutation limit. It then advances the compaction watermark.
func (b *Backend) Compact(ctx context.Context, rev int64) (int64, error) {
	cur, err := b.CurrentRevision(ctx)
	if err != nil {
		return 0, err
	}
	if rev > cur {
		rev = cur
	}
	const batch = 1000
	for {
		var ids []int64
		_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			ids = ids[:0]
			stmt := spanner.Statement{
				SQL: `SELECT k.id FROM kine AS k
				      WHERE k.id <= @rev AND (
				          k.deleted = TRUE
				          OR k.id < (SELECT MAX(x.id) FROM kine AS x WHERE x.name = k.name)
				      )
				      LIMIT @batch`,
				Params: map[string]interface{}{"rev": rev, "batch": int64(batch)},
			}
			iter := txn.Query(ctx, stmt)
			defer iter.Stop()
			var muts []*spanner.Mutation
			for {
				r, err := iter.Next()
				if errors.Is(err, iterator.Done) {
					break
				}
				if err != nil {
					return err
				}
				var id int64
				if err := r.Column(0, &id); err != nil {
					return err
				}
				ids = append(ids, id)
				muts = append(muts, spanner.Delete("kine", spanner.Key{id}))
			}
			if len(muts) == 0 {
				return nil
			}
			return txn.BufferWrite(muts)
		})
		if err != nil {
			return cur, err
		}
		if len(ids) == 0 {
			break
		}
	}
	_, err = b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite([]*spanner.Mutation{setCounterMut(compactKey, rev)})
	})
	if err != nil {
		return cur, err
	}
	return cur, nil
}

func (b *Backend) DeleteByLease(ctx context.Context, leaseID int64) ([]*backend.Event, error) {
	var events []*backend.Event
	_, err := b.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		events = nil
		stmt := spanner.Statement{
			SQL: `SELECT k.id, k.name, k.create_revision, k.value
			      FROM kine AS k
			      JOIN (SELECT name AS n, MAX(id) AS mid FROM kine GROUP BY name) AS m
			        ON k.name = m.n AND k.id = m.mid
			      WHERE k.deleted = FALSE AND k.lease = @lease`,
			Params: map[string]interface{}{"lease": leaseID},
		}
		iter := txn.Query(ctx, stmt)
		defer iter.Stop()
		type live struct {
			id        int64
			name      string
			createRev int64
			value     []byte
		}
		var victims []live
		for {
			r, err := iter.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return err
			}
			var v live
			if err := r.Columns(&v.id, &v.name, &v.createRev, &v.value); err != nil {
				return err
			}
			victims = append(victims, v)
		}
		if len(victims) == 0 {
			return nil
		}
		counter, err := readCounterTx(ctx, txn, revKey)
		if err != nil {
			return err
		}
		muts := make([]*spanner.Mutation, 0, len(victims)+1)
		for i, v := range victims {
			rev := counter + int64(i) + 1
			muts = append(muts, insertRowMut(rev, v.name, false, true, v.createRev, v.id, 0, nil, v.value))
			events = append(events, &backend.Event{
				Delete: true,
				KV:     &backend.KeyValue{Key: v.name, ModRevision: rev, CreateRevision: v.createRev},
				PrevKV: &backend.KeyValue{Key: v.name, CreateRevision: v.createRev, ModRevision: v.id, Value: v.value},
			})
		}
		muts = append(muts, setCounterMut(revKey, counter+int64(len(victims))))
		return txn.BufferWrite(muts)
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

var _ backend.Backend = (*Backend)(nil)
