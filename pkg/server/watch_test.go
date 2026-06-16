package server

import (
	"context"
	"testing"
	"time"

	"github.com/geoffsdesk/ledger/pkg/backend"
	"github.com/geoffsdesk/ledger/pkg/backend/memory"
)

// TestWatchPollDelivers exercises the shared poller + broadcaster: a write to a
// watched range must surface on the subscription with a revision past the
// subscribe boundary, and writes outside the range must be filtered out.
func TestWatchPollDelivers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	be := memory.New()
	w := newWatcher(be, backend.NewPollNotifier(be, 5*time.Millisecond))
	if err := w.start(ctx); err != nil {
		t.Fatal(err)
	}

	end := backend.PrefixEnd("/reg/")
	sub, boundary := w.subscribe("/reg/", end)
	defer w.unsubscribe(sub)

	if _, err := be.Create(ctx, "/other", []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := be.Create(ctx, "/reg/a", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	select {
	case evs := <-sub.ch:
		if len(evs) != 1 {
			t.Fatalf("expected exactly the in-range event, got %d", len(evs))
		}
		if evs[0].KV.Key != "/reg/a" {
			t.Fatalf("delivered out-of-range key %q", evs[0].KV.Key)
		}
		if evs[0].KV.ModRevision <= boundary {
			t.Fatalf("event revision %d not past boundary %d", evs[0].KV.ModRevision, boundary)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}
}
