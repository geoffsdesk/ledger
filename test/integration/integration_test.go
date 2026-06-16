
// Package server_test drives the shim over real gRPC with etcd's own clientv3 —
// the exact library kube-apiserver uses — to prove wire compatibility.
package integration_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/geoffsdesk/ledger/pkg/backend/memory"
	"github.com/geoffsdesk/ledger/pkg/server"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func startShim(t *testing.T) (*clientv3.Client, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	be := memory.New()
	srv := server.New(be, 20*time.Millisecond)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{lis.Addr().String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("clientv3: %v", err)
	}
	return cli, func() {
		cli.Close()
		gs.Stop()
		cancel()
	}
}

func TestClientv3PutGet(t *testing.T) {
	cli, done := startShim(t)
	defer done()
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/foo", "bar"); err != nil {
		t.Fatalf("put: %v", err)
	}
	gr, err := cli.Get(ctx, "/foo")
	if err != nil || len(gr.Kvs) != 1 || string(gr.Kvs[0].Value) != "bar" {
		t.Fatalf("get = %+v err=%v", gr.Kvs, err)
	}
}

func TestClientv3TxnCAS(t *testing.T) {
	cli, done := startShim(t)
	defer done()
	ctx := context.Background()

	// create (apiserver shape): If(ModRevision==0) Then(Put) Else(Get)
	tr, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/cas"), "=", 0)).
		Then(clientv3.OpPut("/cas", "v1")).
		Else(clientv3.OpGet("/cas")).
		Commit()
	if err != nil || !tr.Succeeded {
		t.Fatalf("create txn succeeded=%v err=%v", tr.Succeeded, err)
	}

	// second create must fail and return the current value via Else(Get).
	tr, err = cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/cas"), "=", 0)).
		Then(clientv3.OpPut("/cas", "v2")).
		Else(clientv3.OpGet("/cas")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	if tr.Succeeded {
		t.Fatal("expected the second create to fail the compare")
	}
	rr := tr.Responses[0].GetResponseRange()
	if rr == nil || len(rr.Kvs) != 1 || string(rr.Kvs[0].Value) != "v1" {
		t.Fatalf("else-branch get = %+v", rr)
	}
}

func TestClientv3WatchPrefix(t *testing.T) {
	cli, done := startShim(t)
	defer done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wch := cli.Watch(ctx, "/w/", clientv3.WithPrefix())
	time.Sleep(100 * time.Millisecond) // allow the watch to establish

	if _, err := cli.Put(ctx, "/w/a", "x"); err != nil {
		t.Fatal(err)
	}

	select {
	case wr := <-wch:
		if err := wr.Err(); err != nil {
			t.Fatalf("watch err: %v", err)
		}
		if len(wr.Events) == 0 || string(wr.Events[0].Kv.Key) != "/w/a" {
			t.Fatalf("watch events = %+v", wr.Events)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}
}
