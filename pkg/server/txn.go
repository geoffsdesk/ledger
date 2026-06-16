package server

import (
	"bytes"
	"context"

	"github.com/geoffsdesk/ledger/pkg/backend"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Txn is where correctness lives. The apiserver expresses every write as an
// optimistic compare-and-swap:
//
//	create: If(ModRevision(key) == 0)        Then(Put(key,val)) Else(Get(key))
//	update: If(ModRevision(key) == expected)  Then(Put(key,val)) Else(Get(key))
//	delete: If(ModRevision(key) == expected)  Then(Delete(key))  Else(Get(key))
//
// We recognise those single-key shapes and execute them as one atomic backend
// CAS (a single Spanner read-write transaction). Anything else falls back to a
// best-effort non-atomic evaluation — sufficient for tooling and read-only
// txns, which is all the apiserver ever issues outside the shapes above.
func (s *Server) Txn(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if resp, handled, err := s.tryAtomicCAS(ctx, r); handled || err != nil {
		return resp, err
	}
	return s.genericTxn(ctx, r)
}

// tryAtomicCAS handles the apiserver's single-key compare-and-swap shapes.
// handled=false means the request was not one of those shapes.
func (s *Server) tryAtomicCAS(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, bool, error) {
	if len(r.Compare) == 0 || len(r.Success) != 1 {
		return nil, false, nil
	}
	key := r.Compare[0].Key

	// All compares must be EQUAL on Mod/Create revision of the same single key.
	var expected int64
	for _, c := range r.Compare {
		if !bytes.Equal(c.Key, key) || len(c.RangeEnd) != 0 {
			return nil, false, nil
		}
		if c.Result != etcdserverpb.Compare_EQUAL {
			return nil, false, nil
		}
		switch c.Target {
		case etcdserverpb.Compare_MOD:
			expected = c.GetModRevision()
		case etcdserverpb.Compare_CREATE:
			// Only "create_revision == 0" (key absent) is a shape we model.
			if c.GetCreateRevision() != 0 {
				return nil, false, nil
			}
			expected = 0
		default:
			return nil, false, nil
		}
	}

	op := r.Success[0]
	switch {
	case op.GetRequestPut() != nil:
		put := op.GetRequestPut()
		if !bytes.Equal(put.Key, key) {
			return nil, false, nil
		}
		if expected == 0 {
			rev, err := s.be.Create(ctx, string(key), put.Value, put.Lease)
			if err == backend.ErrKeyExists {
				return s.casFailure(ctx, r)
			}
			if err != nil {
				return nil, true, toGRPC(err)
			}
			return s.casSuccess(rev, respPut(&etcdserverpb.PutResponse{Header: s.header(rev)})), true, nil
		}
		ok, rev, _, err := s.be.Update(ctx, string(key), put.Value, expected, put.Lease)
		if err != nil {
			return nil, true, toGRPC(err)
		}
		if !ok {
			return s.casFailure(ctx, r)
		}
		return s.casSuccess(rev, respPut(&etcdserverpb.PutResponse{Header: s.header(rev)})), true, nil

	case op.GetRequestDeleteRange() != nil:
		dr := op.GetRequestDeleteRange()
		if !bytes.Equal(dr.Key, key) || len(dr.RangeEnd) != 0 {
			return nil, false, nil
		}
		ok, rev, deleted, err := s.be.Delete(ctx, string(key), expected)
		if err != nil {
			return nil, true, toGRPC(err)
		}
		if !ok {
			return s.casFailure(ctx, r)
		}
		dResp := &etcdserverpb.DeleteRangeResponse{Header: s.header(rev), Deleted: 1}
		if dr.PrevKv {
			dResp.PrevKvs = append(dResp.PrevKvs, toMvccKV(deleted))
		}
		return s.casSuccess(rev, respDelete(dResp)), true, nil
	}
	return nil, false, nil
}

func (s *Server) casSuccess(rev int64, op *etcdserverpb.ResponseOp) *etcdserverpb.TxnResponse {
	return &etcdserverpb.TxnResponse{
		Header:    s.header(rev),
		Succeeded: true,
		Responses: []*etcdserverpb.ResponseOp{op},
	}
}

// casFailure runs the Failure branch (always a Get of the key) and reports the
// current value, mirroring etcd's behaviour when a compare fails.
func (s *Server) casFailure(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, bool, error) {
	ops, rev, err := s.runOps(ctx, r.Failure)
	if err != nil {
		return nil, true, err
	}
	return &etcdserverpb.TxnResponse{Header: s.header(rev), Succeeded: false, Responses: ops}, true, nil
}

// genericTxn evaluates compares against current state (non-atomic) and runs the
// matching branch. Used only for read-only / unrecognised txns.
func (s *Server) genericTxn(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	succeeded := true
	for _, c := range r.Compare {
		ok, err := s.evalCompare(ctx, c)
		if err != nil {
			return nil, toGRPC(err)
		}
		if !ok {
			succeeded = false
			break
		}
	}
	branch := r.Success
	if !succeeded {
		branch = r.Failure
	}
	ops, rev, err := s.runOps(ctx, branch)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.TxnResponse{Header: s.header(rev), Succeeded: succeeded, Responses: ops}, nil
}

func (s *Server) evalCompare(ctx context.Context, c *etcdserverpb.Compare) (bool, error) {
	_, kv, err := s.be.Get(ctx, string(c.Key), 0)
	if err != nil {
		return false, err
	}
	var actual int64
	switch c.Target {
	case etcdserverpb.Compare_MOD:
		if kv != nil {
			actual = kv.ModRevision
		}
		return compareInt(actual, c.GetModRevision(), c.Result), nil
	case etcdserverpb.Compare_CREATE:
		if kv != nil {
			actual = kv.CreateRevision
		}
		return compareInt(actual, c.GetCreateRevision(), c.Result), nil
	case etcdserverpb.Compare_VERSION:
		if kv != nil {
			actual = kv.Version
		}
		return compareInt(actual, c.GetVersion(), c.Result), nil
	case etcdserverpb.Compare_VALUE:
		var val []byte
		if kv != nil {
			val = kv.Value
		}
		cmp := bytes.Compare(val, c.GetValue())
		return compareResult(cmp, c.Result), nil
	default:
		return false, nil
	}
}

func compareInt(a, b int64, res etcdserverpb.Compare_CompareResult) bool {
	switch {
	case a < b:
		return compareResult(-1, res)
	case a > b:
		return compareResult(1, res)
	default:
		return compareResult(0, res)
	}
}

func compareResult(cmp int, res etcdserverpb.Compare_CompareResult) bool {
	switch res {
	case etcdserverpb.Compare_EQUAL:
		return cmp == 0
	case etcdserverpb.Compare_GREATER:
		return cmp > 0
	case etcdserverpb.Compare_LESS:
		return cmp < 0
	case etcdserverpb.Compare_NOT_EQUAL:
		return cmp != 0
	default:
		return false
	}
}

// runOps executes a branch of plain operations and returns the response ops
// plus the highest header revision observed.
func (s *Server) runOps(ctx context.Context, ops []*etcdserverpb.RequestOp) ([]*etcdserverpb.ResponseOp, int64, error) {
	out := make([]*etcdserverpb.ResponseOp, 0, len(ops))
	var rev int64
	for _, op := range ops {
		switch {
		case op.GetRequestRange() != nil:
			rr, err := s.Range(ctx, op.GetRequestRange())
			if err != nil {
				return nil, 0, err
			}
			rev = maxInt64(rev, rr.Header.Revision)
			out = append(out, respRange(rr))
		case op.GetRequestPut() != nil:
			pr, err := s.Put(ctx, op.GetRequestPut())
			if err != nil {
				return nil, 0, err
			}
			rev = maxInt64(rev, pr.Header.Revision)
			out = append(out, respPut(pr))
		case op.GetRequestDeleteRange() != nil:
			dr, err := s.DeleteRange(ctx, op.GetRequestDeleteRange())
			if err != nil {
				return nil, 0, err
			}
			rev = maxInt64(rev, dr.Header.Revision)
			out = append(out, respDelete(dr))
		}
	}
	if rev == 0 {
		rev, _ = s.be.CurrentRevision(ctx)
	}
	return out, rev, nil
}

func respRange(rr *etcdserverpb.RangeResponse) *etcdserverpb.ResponseOp {
	return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: rr}}
}
func respPut(pr *etcdserverpb.PutResponse) *etcdserverpb.ResponseOp {
	return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: pr}}
}
func respDelete(dr *etcdserverpb.DeleteRangeResponse) *etcdserverpb.ResponseOp {
	return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: dr}}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
