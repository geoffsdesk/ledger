// Multi-master tests: two stateless ledger front-ends over ONE shared backend
// model the production topology (one Spanner database, N interchangeable
// masters). They prove the masters need no quorum or coordination among
// themselves — correctness comes from the shared store.
package integration_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/geoffsdesk/ledger/pkg/backend"
	"github.com/geoffsdesk/ledger/pkg/backend/memory"
	"github.com/geoffsdesk/ledger/pkg/server"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type master struct {
	cli  *clientv3.Client
	stop func()
}

// startMaster brings up one shim front-end over the shared backend be.
func startMaster(t *testing.T, be backend.Backend) *master {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	srv := server.New(be, 20*time.Millisecond)
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(16<<20), grpc.MaxSendMsgSize(16<<20))
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{lis.Addr().String()}, DialTimeout: 5 * time.Second})
	if err != nil {
		gs.Stop()
		cancel()
		t.Fatalf("client: %v", err)
	}
	return &master{cli: cli, stop: func() { cli.Close(); gs.Stop(); cancel() }}
}

// Writes through either master are visible through the other.
func TestTwoMastersSharedState(t *testing.T) {
	be := memory.New()
	a := startMaster(t, be)
	defer a.stop()
	b := startMaster(t, be)
	defer b.stop()
	ctx := context.Background()

	if _, err := a.cli.Put(ctx, "/x", "fromA"); err != nil {
		t.Fatal(err)
	}
	if gr, err := b.cli.Get(ctx, "/x"); err != nil || len(gr.Kvs) != 1 || string(gr.Kvs[0].Value) != "fromA" {
		t.Fatalf("B did not see A's write: %+v err=%v", gr.Kvs, err)
	}
	if _, err := b.cli.Put(ctx, "/y", "fromB"); err != nil {
		t.Fatal(err)
	}
	if gr, err := a.cli.Get(ctx, "/y"); err != nil || len(gr.Kvs) != 1 || string(gr.Kvs[0].Value) != "fromB" {
		t.Fatalf("A did not see B's write: %+v err=%v", gr.Kvs, err)
	}
}

// Interleaved writes across both masters share one strictly-increasing revision
// sequence (the backend serializes the counter).
func TestTwoMastersMonotonicRevisions(t *testing.T) {
	be := memory.New()
	a := startMaster(t, be)
	defer a.stop()
	b := startMaster(t, be)
	defer b.stop()
	ctx := context.Background()

	var last int64
	for i := 0; i < 50; i++ {
		m := a
		if i%2 == 1 {
			m = b
		}
		resp, err := m.cli.Put(ctx, "/seq", "v")
		if err != nil {
			t.Fatal(err)
		}
		if resp.Header.Revision <= last {
			t.Fatalf("revision not strictly increasing across masters: %d after %d", resp.Header.Revision, last)
		}
		last = resp.Header.Revision
	}
}

// A watch on master B observes a write made through master A.
func TestCrossMasterWatch(t *testing.T) {
	be := memory.New()
	a := startMaster(t, be)
	defer a.stop()
	b := startMaster(t, be)
	defer b.stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wch := b.cli.Watch(ctx, "/w/", clientv3.WithPrefix())
	time.Sleep(100 * time.Millisecond) // let the watch establish

	if _, err := a.cli.Put(ctx, "/w/a", "viaA"); err != nil {
		t.Fatal(err)
	}
	select {
	case wr := <-wch:
		if wr.Err() != nil {
			t.Fatalf("watch: %v", wr.Err())
		}
		if len(wr.Events) == 0 || string(wr.Events[0].Kv.Value) != "viaA" {
			t.Fatalf("B's watch did not see A's write: %+v", wr.Events)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: B's watch never observed A's write")
	}
}
