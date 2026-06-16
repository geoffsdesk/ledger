// Package server implements the minimal subset of the etcd v3 gRPC API that the
// Kubernetes apiserver actually uses (Range, Put, DeleteRange, Txn, Compact,
// Watch, Lease, Maintenance.Status) on top of a backend.Backend. It is the
// "shim" half of the system: it speaks etcd on the wire so kube-apiserver can't
// tell the difference, and delegates all storage to the backend.
package server

import (
	"context"
	"errors"
	"time"

	"github.com/geoffsdesk/ledger/pkg/backend"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	clusterID   = 0x1000
	memberID    = 0x1001
	etcdVersion = "3.5.13" // version advertised to the apiserver's health checks
)

// Server implements etcdserverpb.{KV,Watch,Lease,Maintenance}Server.
//
// It deliberately does NOT embed the generated Unimplemented*Server structs:
// etcd's gogo-generated interfaces don't use the mustEmbed pattern, so every
// method is implemented explicitly (unused ones return codes.Unimplemented).
type Server struct {
	be      backend.Backend
	watcher *watcher
	leases  *leaseManager
}

// New builds a Server over the given backend. pollInterval controls watch
// latency (how often the backend is polled for new revisions).
func New(be backend.Backend, pollInterval time.Duration) *Server {
	return NewWithNotifier(be, backend.NewPollNotifier(be, pollInterval))
}

// NewWithNotifier builds a Server with a custom watch Notifier — e.g. a Spanner
// Change Streams notifier in place of the default poller. Watch fan-out and
// every other path are unchanged; only the source of change events differs.
func NewWithNotifier(be backend.Backend, n backend.Notifier) *Server {
	return &Server{
		be:      be,
		watcher: newWatcher(be, n),
		leases:  newLeaseManager(be),
	}
}

// Start brings up the backend and the background poller / lease reaper.
func (s *Server) Start(ctx context.Context) error {
	if err := s.be.Start(ctx); err != nil {
		return err
	}
	if err := s.watcher.start(ctx); err != nil {
		return err
	}
	s.leases.start(ctx)
	return nil
}

// Register wires the shim's services (plus a gRPC health service) onto a server.
func (s *Server) Register(gs *grpc.Server) {
	etcdserverpb.RegisterKVServer(gs, s)
	etcdserverpb.RegisterWatchServer(gs, s)
	etcdserverpb.RegisterLeaseServer(gs, s)
	etcdserverpb.RegisterMaintenanceServer(gs, s)

	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, hs)
}

func (s *Server) header(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		ClusterId: clusterID,
		MemberId:  memberID,
		Revision:  rev,
		RaftTerm:  1,
	}
}

func toMvccKV(kv *backend.KeyValue) *mvccpb.KeyValue {
	if kv == nil {
		return nil
	}
	return &mvccpb.KeyValue{
		Key:            []byte(kv.Key),
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		Value:          kv.Value,
		Lease:          kv.Lease,
	}
}

// etcd error strings the clientv3 maps back to typed errors (rpctypes). They
// must match exactly so the apiserver recognises a compaction and relists.
var (
	errGRPCCompacted = status.Error(codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")
	errGRPCFutureRev = status.Error(codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")
)

func toGRPC(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, backend.ErrCompacted):
		return errGRPCCompacted
	case errors.Is(err, backend.ErrFutureRev):
		return errGRPCFutureRev
	default:
		return status.Error(codes.Unknown, err.Error())
	}
}

// normalizeRange translates an etcd (key, range_end) pair into the half-open
// [start, end) range the backend expects. An empty returned end means unbounded.
// etcd uses a single 0x00 byte to mean "open ended".
func normalizeRange(key, end string) (string, string) {
	if key == "\x00" {
		key = ""
	}
	if end == "\x00" {
		end = ""
	}
	return key, end
}

// normalizeWatchRange behaves like normalizeRange but, for a watch on a single
// key (empty range_end), produces a range that matches only that exact key.
func normalizeWatchRange(key, end string) (string, string) {
	if len(end) == 0 {
		return key, key + "\x00"
	}
	return normalizeRange(key, end)
}
