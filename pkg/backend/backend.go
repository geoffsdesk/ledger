// Package backend defines the storage contract that the etcd v3 shim is
// implemented against, plus the wire-independent value types it exchanges with
// the gRPC layer.
//
// The whole design rests on one idea borrowed from etcd's MVCC store (and from
// k3s's "kine"): every mutation is appended to a single, globally ordered log,
// and the position in that log IS the revision. The Kubernetes apiserver relies
// on exactly three properties of that log:
//
//  1. Revisions are int64, strictly monotonic, and assigned in commit order.
//  2. A read can be served "as of" any revision that has not been compacted.
//  3. A watch can be resumed from any revision and will observe every change
//     after it, in order.
//
// Any backend that honours those three properties can stand in for etcd.
package backend

import (
	"context"
	"errors"
)

// Sentinel errors returned by Backend implementations. The gRPC layer maps
// these onto the corresponding etcd error codes the apiserver understands.
var (
	// ErrKeyExists is returned by Create when the key already has a live value.
	ErrKeyExists = errors.New("key already exists")
	// ErrCompacted is returned when a read/watch targets a revision that has
	// already been compacted away (etcd: ErrCompacted / mvcc: required revision
	// has been compacted).
	ErrCompacted = errors.New("required revision has been compacted")
	// ErrFutureRev is returned when a read targets a revision newer than the
	// current store revision.
	ErrFutureRev = errors.New("required revision is a future revision")
)

// KeyValue mirrors mvccpb.KeyValue but lives here so backends never import the
// etcd wire types. The gRPC layer is the only place that converts between the
// two.
type KeyValue struct {
	Key            string
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Value          []byte
	Lease          int64
}

// Event is a single change to a key, mirroring an etcd watch event.
type Event struct {
	Create bool // first appearance of the key
	Delete bool // key was deleted at ModRevision
	KV     *KeyValue
	PrevKV *KeyValue
}

// Backend is the storage abstraction. Every revision returned is a position in
// the single global log described in the package doc. Implementations must be
// safe for concurrent use.
type Backend interface {
	// Start ensures the schema exists (migrations) and starts any background
	// goroutines owned by the backend. It must be idempotent.
	Start(ctx context.Context) error

	// CurrentRevision returns the latest committed store revision. This is the
	// value the gRPC layer stamps into every ResponseHeader.Revision, which the
	// apiserver consumes as the resourceVersion of a list/read.
	CurrentRevision(ctx context.Context) (int64, error)

	// CompactRevision returns the lowest revision still readable. Reads/watches
	// below this must fail with ErrCompacted.
	CompactRevision(ctx context.Context) (int64, error)

	// Get returns the live value of key at or below revision. revision <= 0
	// means "current". A nil KeyValue with a nil error means the key does not
	// exist (or was deleted) at that revision. The returned rev is always the
	// current store revision, to be echoed in the response header.
	Get(ctx context.Context, key string, revision int64) (rev int64, kv *KeyValue, err error)

	// List returns the latest live version of every key in the half-open range
	// [key, end) at or below revision, sorted ascending by key, capped at limit
	// (<= 0 means unlimited). end == "" means unbounded to the end of the
	// keyspace. more reports whether the result was truncated by limit.
	List(ctx context.Context, key, end string, limit, revision int64) (rev int64, kvs []*KeyValue, more bool, err error)

	// Count returns the number of live keys in [key, end) at revision.
	Count(ctx context.Context, key, end string, revision int64) (rev int64, count int64, err error)

	// Create inserts key=value iff the key has no live value. On conflict it
	// returns ErrKeyExists (the gRPC Txn handler turns that into the etcd
	// "compare failed" path).
	Create(ctx context.Context, key string, value []byte, lease int64) (rev int64, err error)

	// Update sets key=value iff the key's current ModRevision == expectedRev.
	// ok reports whether the compare-and-swap fired. On success kv is the new
	// value; on failure kv is the current value (nil if the key is absent).
	Update(ctx context.Context, key string, value []byte, expectedRev, lease int64) (ok bool, rev int64, kv *KeyValue, err error)

	// Delete removes key iff its current ModRevision == expectedRev. On success
	// kv is the value that was deleted.
	Delete(ctx context.Context, key string, expectedRev int64) (ok bool, rev int64, kv *KeyValue, err error)

	// After returns change events with revision > rev whose key is in [key, end),
	// ordered by revision ascending, capped at limit (<= 0 means a backend
	// default). key/end == "" means unbounded on that side. Used by the watch
	// poller (full keyspace) and for per-watcher catch-up (a single range).
	After(ctx context.Context, key, end string, rev, limit int64) (events []*Event, err error)

	// Compact discards superseded revisions up to and including revision and
	// advances the compaction watermark. Returns the current store revision.
	Compact(ctx context.Context, revision int64) (rev int64, err error)

	// DeleteByLease removes every key currently attached to leaseID (lease
	// expiry / revoke). Returns the delete events produced so the watch stream
	// can observe them.
	DeleteByLease(ctx context.Context, leaseID int64) ([]*Event, error)

	// Close releases backend resources.
	Close() error
}

// PrefixEnd returns the exclusive end of the range covering every key that
// starts with prefix. It matches clientv3.GetPrefixRangeEnd, which is how the
// apiserver forms a prefix scan. An empty string means "no upper bound".
func PrefixEnd(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			end := make([]byte, i+1)
			copy(end, b[:i+1])
			end[i]++
			return string(end)
		}
	}
	// prefix is empty or all 0xff: unbounded to the end of the keyspace.
	return ""
}
