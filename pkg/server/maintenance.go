package server

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Status is what the apiserver's etcd health check and version probe call. The
// values are synthesised: we report a 3.x version (so the apiserver is happy),
// the current revision as the raft index, and a single-member, self-led
// "cluster".
func (s *Server) Status(ctx context.Context, _ *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	cur, err := s.be.CurrentRevision(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	return &etcdserverpb.StatusResponse{
		Header:           s.header(cur),
		Version:          etcdVersion,
		DbSize:           0,
		DbSizeInUse:      0,
		Leader:           memberID,
		RaftIndex:        uint64(cur),
		RaftTerm:         1,
		RaftAppliedIndex: uint64(cur),
	}, nil
}

// Alarm always reports no alarms (the shim has no etcd-style space/corruption
// alarms; backend capacity is Spanner's concern).
func (s *Server) Alarm(ctx context.Context, _ *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
	cur, _ := s.be.CurrentRevision(ctx)
	return &etcdserverpb.AlarmResponse{Header: s.header(cur)}, nil
}

// Defragment is a no-op: there is no Bolt file to defragment.
func (s *Server) Defragment(ctx context.Context, _ *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	cur, _ := s.be.CurrentRevision(ctx)
	return &etcdserverpb.DefragmentResponse{Header: s.header(cur)}, nil
}

func (s *Server) Hash(ctx context.Context, _ *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	cur, _ := s.be.CurrentRevision(ctx)
	return &etcdserverpb.HashResponse{Header: s.header(cur)}, nil
}

func (s *Server) HashKV(ctx context.Context, _ *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	cur, _ := s.be.CurrentRevision(ctx)
	return &etcdserverpb.HashKVResponse{Header: s.header(cur)}, nil
}

func (s *Server) Snapshot(_ *etcdserverpb.SnapshotRequest, _ etcdserverpb.Maintenance_SnapshotServer) error {
	return status.Error(codes.Unimplemented, "snapshot is not supported by the Spanner backend; use Spanner backup/restore")
}

func (s *Server) MoveLeader(context.Context, *etcdserverpb.MoveLeaderRequest) (*etcdserverpb.MoveLeaderResponse, error) {
	return nil, status.Error(codes.Unimplemented, "single logical member; no leader to move")
}

func (s *Server) Downgrade(context.Context, *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "downgrade is not supported")
}
