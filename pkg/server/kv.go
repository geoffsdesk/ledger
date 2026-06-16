package server

import (
	"context"

	"github.com/geoffsdesk/ledger/pkg/backend"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// Range serves reads. The apiserver uses two shapes: a single-key get
// (range_end empty) and a prefix list (range_end = prefixEnd, with Limit for
// pagination and Revision for a consistent snapshot).
func (s *Server) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	key := string(r.Key)
	end := string(r.RangeEnd)

	// Single-key get.
	if len(r.RangeEnd) == 0 {
		cur, kv, err := s.be.Get(ctx, key, r.Revision)
		if err != nil {
			return nil, toGRPC(err)
		}
		resp := &etcdserverpb.RangeResponse{Header: s.header(cur)}
		if kv != nil {
			resp.Count = 1
			if !r.CountOnly {
				mv := toMvccKV(kv)
				if r.KeysOnly {
					mv.Value = nil
				}
				resp.Kvs = []*mvccpb.KeyValue{mv}
			}
		}
		return resp, nil
	}

	bkey, bend := normalizeRange(key, end)

	if r.CountOnly {
		cur, count, err := s.be.Count(ctx, bkey, bend, r.Revision)
		if err != nil {
			return nil, toGRPC(err)
		}
		return &etcdserverpb.RangeResponse{Header: s.header(cur), Count: count}, nil
	}

	cur, kvs, more, err := s.be.List(ctx, bkey, bend, r.Limit, r.Revision)
	if err != nil {
		return nil, toGRPC(err)
	}
	resp := &etcdserverpb.RangeResponse{Header: s.header(cur), More: more}
	for _, kv := range kvs {
		mv := toMvccKV(kv)
		if r.KeysOnly {
			mv.Value = nil
		}
		resp.Kvs = append(resp.Kvs, mv)
	}
	// Count must be the TOTAL matched (not the truncated page) so the apiserver
	// can compute RemainingItemCount. Pin it to the same revision we just read.
	if more {
		_, total, err := s.be.Count(ctx, bkey, bend, cur)
		if err != nil {
			return nil, toGRPC(err)
		}
		resp.Count = total
	} else {
		resp.Count = int64(len(resp.Kvs))
	}
	return resp, nil
}

// Put is a blind upsert. The apiserver does not use it (it goes through Txn),
// but etcdctl and other tooling do, so it is implemented for completeness.
func (s *Server) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	key := string(r.Key)
	for attempt := 0; attempt < 8; attempt++ {
		_, cur, err := s.be.Get(ctx, key, 0)
		if err != nil {
			return nil, toGRPC(err)
		}
		if cur == nil {
			rev, err := s.be.Create(ctx, key, r.Value, r.Lease)
			if err == backend.ErrKeyExists {
				continue // lost the race; retry as update
			}
			if err != nil {
				return nil, toGRPC(err)
			}
			return &etcdserverpb.PutResponse{Header: s.header(rev)}, nil
		}
		ok, rev, _, err := s.be.Update(ctx, key, r.Value, cur.ModRevision, r.Lease)
		if err != nil {
			return nil, toGRPC(err)
		}
		if !ok {
			continue
		}
		resp := &etcdserverpb.PutResponse{Header: s.header(rev)}
		if r.PrevKv {
			resp.PrevKv = toMvccKV(cur)
		}
		return resp, nil
	}
	cur, _ := s.be.CurrentRevision(ctx)
	return &etcdserverpb.PutResponse{Header: s.header(cur)}, nil
}

// DeleteRange deletes a single key or a range. The apiserver deletes single
// keys via Txn; range deletes are best-effort (list then delete each).
func (s *Server) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	key := string(r.Key)

	if len(r.RangeEnd) == 0 {
		cur, kv, err := s.be.Get(ctx, key, 0)
		if err != nil {
			return nil, toGRPC(err)
		}
		if kv == nil {
			return &etcdserverpb.DeleteRangeResponse{Header: s.header(cur)}, nil
		}
		ok, rev, deleted, err := s.be.Delete(ctx, key, kv.ModRevision)
		if err != nil {
			return nil, toGRPC(err)
		}
		if !ok {
			return &etcdserverpb.DeleteRangeResponse{Header: s.header(rev)}, nil
		}
		resp := &etcdserverpb.DeleteRangeResponse{Header: s.header(rev), Deleted: 1}
		if r.PrevKv {
			resp.PrevKvs = []*mvccpb.KeyValue{toMvccKV(deleted)}
		}
		return resp, nil
	}

	bkey, bend := normalizeRange(key, string(r.RangeEnd))
	_, kvs, _, err := s.be.List(ctx, bkey, bend, 0, 0)
	if err != nil {
		return nil, toGRPC(err)
	}
	resp := &etcdserverpb.DeleteRangeResponse{}
	var lastRev int64
	for _, kv := range kvs {
		ok, rev, deleted, err := s.be.Delete(ctx, kv.Key, kv.ModRevision)
		if err != nil {
			return nil, toGRPC(err)
		}
		if ok {
			resp.Deleted++
			lastRev = rev
			if r.PrevKv {
				resp.PrevKvs = append(resp.PrevKvs, toMvccKV(deleted))
			}
		}
	}
	if lastRev == 0 {
		lastRev, _ = s.be.CurrentRevision(ctx)
	}
	resp.Header = s.header(lastRev)
	return resp, nil
}

// Compact discards superseded revisions. The apiserver calls this periodically.
func (s *Server) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	cur, err := s.be.Compact(ctx, r.Revision)
	if err != nil {
		return nil, toGRPC(err)
	}
	return &etcdserverpb.CompactionResponse{Header: s.header(cur)}, nil
}
