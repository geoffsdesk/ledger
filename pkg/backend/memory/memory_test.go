package memory

import (
	"context"
	"testing"

	"github.com/geoffsdesk/ledger/pkg/backend"
)

func TestCreateGetUpdateDelete(t *testing.T) {
	ctx := context.Background()
	b := New()

	rev1, err := b.Create(ctx, "/a", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if rev1 != 1 {
		t.Fatalf("first revision = %d, want 1", rev1)
	}

	// Duplicate create fails.
	if _, err := b.Create(ctx, "/a", []byte("x"), 0); err != backend.ErrKeyExists {
		t.Fatalf("duplicate create err = %v, want ErrKeyExists", err)
	}

	// Read back.
	_, kv, err := b.Get(ctx, "/a", 0)
	if err != nil || kv == nil || string(kv.Value) != "v1" || kv.ModRevision != rev1 {
		t.Fatalf("get = %+v err=%v", kv, err)
	}

	// CAS update with wrong revision fails.
	ok, _, _, err := b.Update(ctx, "/a", []byte("v2"), 999, 0)
	if err != nil || ok {
		t.Fatalf("stale update ok=%v err=%v, want ok=false", ok, err)
	}

	// CAS update with correct revision succeeds.
	ok, rev2, kv, err := b.Update(ctx, "/a", []byte("v2"), rev1, 0)
	if err != nil || !ok || rev2 <= rev1 || string(kv.Value) != "v2" {
		t.Fatalf("update ok=%v rev=%d kv=%+v err=%v", ok, rev2, kv, err)
	}
	if kv.CreateRevision != rev1 {
		t.Fatalf("create_revision = %d, want %d", kv.CreateRevision, rev1)
	}

	// Historical read still sees v1 at rev1.
	_, old, err := b.Get(ctx, "/a", rev1)
	if err != nil || old == nil || string(old.Value) != "v1" {
		t.Fatalf("historical get = %+v err=%v", old, err)
	}

	// Delete with correct revision.
	ok, _, deleted, err := b.Delete(ctx, "/a", rev2)
	if err != nil || !ok || string(deleted.Value) != "v2" {
		t.Fatalf("delete ok=%v kv=%+v err=%v", ok, deleted, err)
	}
	_, kv, _ = b.Get(ctx, "/a", 0)
	if kv != nil {
		t.Fatalf("get after delete = %+v, want nil", kv)
	}
}

func TestListPaginationAndCount(t *testing.T) {
	ctx := context.Background()
	b := New()
	for _, k := range []string{"/reg/c", "/reg/a", "/reg/b"} {
		if _, err := b.Create(ctx, k, []byte("x"), 0); err != nil {
			t.Fatal(err)
		}
	}
	end := backend.PrefixEnd("/reg/")

	// Full list is sorted ascending.
	_, kvs, more, err := b.List(ctx, "/reg/", end, 0, 0)
	if err != nil || more || len(kvs) != 3 {
		t.Fatalf("list = %d more=%v err=%v", len(kvs), more, err)
	}
	if kvs[0].Key != "/reg/a" || kvs[2].Key != "/reg/c" {
		t.Fatalf("not sorted: %v", []string{kvs[0].Key, kvs[1].Key, kvs[2].Key})
	}

	// Limit truncates and sets more.
	_, page, more, err := b.List(ctx, "/reg/", end, 1, 0)
	if err != nil || !more || len(page) != 1 || page[0].Key != "/reg/a" {
		t.Fatalf("page = %v more=%v err=%v", page, more, err)
	}

	_, count, err := b.Count(ctx, "/reg/", end, 0)
	if err != nil || count != 3 {
		t.Fatalf("count = %d err=%v", count, err)
	}
}

func TestAfterAndCompact(t *testing.T) {
	ctx := context.Background()
	b := New()
	r1, _ := b.Create(ctx, "/a", []byte("1"), 0)
	_, r2, _, _ := b.Update(ctx, "/a", []byte("2"), r1, 0)
	b.Create(ctx, "/b", []byte("1"), 0)

	// After(0) sees every change in order.
	evs, err := b.After(ctx, "", "", 0, 0)
	if err != nil || len(evs) != 3 {
		t.Fatalf("after = %d err=%v", len(evs), err)
	}
	if !evs[0].Create || evs[1].Create {
		t.Fatalf("event create flags wrong: %+v %+v", evs[0], evs[1])
	}
	if evs[1].PrevKV == nil || string(evs[1].PrevKV.Value) != "1" {
		t.Fatalf("update event missing prev_kv: %+v", evs[1])
	}

	// Compact to r2 drops the superseded first version.
	if _, err := b.Compact(ctx, r2); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if _, _, err := b.Get(ctx, "/a", r1); err != backend.ErrCompacted {
		t.Fatalf("get at compacted rev err = %v, want ErrCompacted", err)
	}
	// Current value still readable.
	_, kv, _ := b.Get(ctx, "/a", 0)
	if kv == nil || string(kv.Value) != "2" {
		t.Fatalf("current get after compact = %+v", kv)
	}
}

func TestDeleteByLease(t *testing.T) {
	ctx := context.Background()
	b := New()
	b.Create(ctx, "/leased", []byte("x"), 42)
	b.Create(ctx, "/kept", []byte("y"), 0)

	evs, err := b.DeleteByLease(ctx, 42)
	if err != nil || len(evs) != 1 || !evs[0].Delete {
		t.Fatalf("delete by lease evs=%+v err=%v", evs, err)
	}
	if _, kv, _ := b.Get(ctx, "/leased", 0); kv != nil {
		t.Fatalf("leased key still present: %+v", kv)
	}
	if _, kv, _ := b.Get(ctx, "/kept", 0); kv == nil {
		t.Fatalf("non-leased key was removed")
	}
}
