package server

import (
	"context"
	"testing"
	"time"

	"github.com/geoffsdesk/ledger/pkg/backend"
	"github.com/geoffsdesk/ledger/pkg/backend/memory"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func modCompare(key string, rev int64) *etcdserverpb.Compare {
	return &etcdserverpb.Compare{
		Result:      etcdserverpb.Compare_EQUAL,
		Target:      etcdserverpb.Compare_MOD,
		Key:         []byte(key),
		TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: rev},
	}
}

func getOp(key string) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{Key: []byte(key)},
	}}
}

func putOp(key, val string) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte(val)},
	}}
}

func delOp(key string) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte(key)},
	}}
}

func casTxn(cmp *etcdserverpb.Compare, success, failure *etcdserverpb.RequestOp) *etcdserverpb.TxnRequest {
	return &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{cmp},
		Success: []*etcdserverpb.RequestOp{success},
		Failure: []*etcdserverpb.RequestOp{failure},
	}
}

func TestTxnApiserverShapes(t *testing.T) {
	ctx := context.Background()
	s := New(memory.New(), time.Hour)

	// create: If(Mod==0) Then(Put) Else(Get)
	resp, err := s.Txn(ctx, casTxn(modCompare("/k", 0), putOp("/k", "v1"), getOp("/k")))
	if err != nil || !resp.Succeeded {
		t.Fatalf("create txn ok=%v err=%v", resp.Succeeded, err)
	}
	if resp.Header.Revision != 1 {
		t.Fatalf("create revision = %d, want 1", resp.Header.Revision)
	}

	// create again: compare fails, Else(Get) returns the existing value.
	resp, err = s.Txn(ctx, casTxn(modCompare("/k", 0), putOp("/k", "v2"), getOp("/k")))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Succeeded {
		t.Fatal("second create should fail the compare")
	}
	rr := resp.Responses[0].GetResponseRange()
	if rr == nil || len(rr.Kvs) != 1 || string(rr.Kvs[0].Value) != "v1" {
		t.Fatalf("failure branch did not return current value: %+v", rr)
	}

	// update: If(Mod==1) Then(Put)
	resp, err = s.Txn(ctx, casTxn(modCompare("/k", 1), putOp("/k", "v2"), getOp("/k")))
	if err != nil || !resp.Succeeded {
		t.Fatalf("update txn ok=%v err=%v", resp.Succeeded, err)
	}
	updRev := resp.Header.Revision

	// stale update fails.
	resp, _ = s.Txn(ctx, casTxn(modCompare("/k", 1), putOp("/k", "v3"), getOp("/k")))
	if resp.Succeeded {
		t.Fatal("stale update should fail the compare")
	}

	// confirm value is v2.
	g, _ := s.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k")})
	if len(g.Kvs) != 1 || string(g.Kvs[0].Value) != "v2" {
		t.Fatalf("get = %+v, want v2", g.Kvs)
	}

	// delete: If(Mod==updRev) Then(Delete)
	resp, err = s.Txn(ctx, casTxn(modCompare("/k", updRev), delOp("/k"), getOp("/k")))
	if err != nil || !resp.Succeeded {
		t.Fatalf("delete txn ok=%v err=%v", resp.Succeeded, err)
	}
	g, _ = s.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k")})
	if len(g.Kvs) != 0 {
		t.Fatalf("key still present after delete: %+v", g.Kvs)
	}
}

func TestRangeListPagination(t *testing.T) {
	ctx := context.Background()
	s := New(memory.New(), time.Hour)
	for _, k := range []string{"/reg/b", "/reg/a", "/reg/c"} {
		if _, err := s.Txn(ctx, casTxn(modCompare(k, 0), putOp(k, "x"), getOp(k))); err != nil {
			t.Fatal(err)
		}
	}
	end := backend.PrefixEnd("/reg/")

	full, err := s.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/reg/"), RangeEnd: []byte(end)})
	if err != nil || len(full.Kvs) != 3 || full.More {
		t.Fatalf("full list = %d more=%v err=%v", len(full.Kvs), full.More, err)
	}
	if string(full.Kvs[0].Key) != "/reg/a" {
		t.Fatalf("list not sorted: %s", full.Kvs[0].Key)
	}

	page, err := s.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/reg/"), RangeEnd: []byte(end), Limit: 2})
	if err != nil || len(page.Kvs) != 2 || !page.More || page.Count != 3 {
		t.Fatalf("page len=%d more=%v count=%d err=%v", len(page.Kvs), page.More, page.Count, err)
	}
}
