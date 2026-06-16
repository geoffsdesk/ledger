package server

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	"github.com/geoffsdesk/ledger/pkg/backend"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// subBuffer bounds how far a single watcher may fall behind before it is
// dropped (and the apiserver forced to relist). Sized for the apiserver's
// bursty informer traffic.
const subBuffer = 1024

type subscription struct {
	id   int64
	key  string
	end  string // "" means unbounded to the end of the keyspace
	ch   chan []*backend.Event
	once sync.Once
}

func (sub *subscription) close() { sub.once.Do(func() { close(sub.ch) }) }

// watcher is a single shared poller that tails the backend's global log and
// fans new events out to every active subscription. One poll loop feeds all
// watches, which is what keeps the load on the backend independent of the
// number of apiserver watches (there can be tens of thousands).
type watcher struct {
	be       backend.Backend
	notifier backend.Notifier

	mu      sync.Mutex
	subs    map[int64]*subscription
	nextSub int64
	cursor  int64 // highest revision fanned out so far
	current int64 // current store revision
}

func newWatcher(be backend.Backend, n backend.Notifier) *watcher {
	return &watcher{be: be, notifier: n, subs: map[int64]*subscription{}}
}

func (w *watcher) start(ctx context.Context) error {
	cur, err := w.be.CurrentRevision(ctx)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.cursor, w.current = cur, cur
	w.mu.Unlock()
	go w.loop(ctx)
	return nil
}

func (w *watcher) loop(ctx context.Context) {
	t := time.NewTicker(w.notifier.Interval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.poll(ctx)
		}
	}
}

func (w *watcher) poll(ctx context.Context) {
	w.mu.Lock()
	from := w.cursor
	w.mu.Unlock()

	events, err := w.notifier.Poll(ctx, from)
	if err != nil {
		log.Printf("ledger: watch poll error: %v", err)
		return
	}
	if len(events) == 0 {
		if cur, err := w.be.CurrentRevision(ctx); err == nil {
			w.mu.Lock()
			if cur > w.current {
				w.current = cur
			}
			w.mu.Unlock()
		}
		return
	}
	last := events[len(events)-1].KV.ModRevision
	w.broadcast(events, last)
}

func (w *watcher) broadcast(events []*backend.Event, last int64) {
	w.mu.Lock()
	w.cursor = last
	if last > w.current {
		w.current = last
	}
	subs := make([]*subscription, 0, len(w.subs))
	for _, s := range w.subs {
		subs = append(subs, s)
	}
	w.mu.Unlock()

	for _, sub := range subs {
		filtered := filterEvents(events, sub.key, sub.end)
		if len(filtered) == 0 {
			continue
		}
		select {
		case sub.ch <- filtered:
		default:
			// Slow consumer: drop it; the handler will see the closed channel
			// and cancel the watch so the apiserver relists.
			w.removeSub(sub.id)
			sub.close()
		}
	}
}

func filterEvents(events []*backend.Event, key, end string) []*backend.Event {
	var out []*backend.Event
	for _, e := range events {
		if e.KV != nil && inRange(e.KV.Key, key, end) {
			out = append(out, e)
		}
	}
	return out
}

func inRange(name, key, end string) bool {
	if name < key {
		return false
	}
	if end == "" {
		return true
	}
	return name < end
}

// subscribe registers a watch range and returns the boundary revision: every
// event already fanned out has revision <= boundary, every event the
// subscription will receive has revision > boundary. The handler replays
// (startRev, boundary] from the backend and streams (boundary, ∞) from the
// channel — no gaps, no duplicates.
func (w *watcher) subscribe(key, end string) (*subscription, int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.nextSub++
	sub := &subscription{id: w.nextSub, key: key, end: end, ch: make(chan []*backend.Event, subBuffer)}
	w.subs[sub.id] = sub
	return sub, w.cursor
}

func (w *watcher) removeSub(id int64) {
	w.mu.Lock()
	delete(w.subs, id)
	w.mu.Unlock()
}

func (w *watcher) unsubscribe(sub *subscription) {
	w.removeSub(sub.id)
	sub.close()
}

func (w *watcher) cur() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.current
}

// Watch multiplexes any number of individual watches over one gRPC stream,
// exactly as clientv3 expects. A reader goroutine dispatches create/cancel/
// progress requests; each active watch runs in its own goroutine; all writes to
// the stream are serialised through send.
func (s *Server) Watch(stream etcdserverpb.Watch_WatchServer) error {
	ctx := stream.Context()
	var sendMu sync.Mutex
	send := func(resp *etcdserverpb.WatchResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(resp)
	}

	var wmu sync.Mutex
	cancels := map[int64]context.CancelFunc{}
	var nextID int64
	var wg sync.WaitGroup

	defer func() {
		wmu.Lock()
		for _, c := range cancels {
			c()
		}
		wmu.Unlock()
		wg.Wait()
	}()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch u := req.RequestUnion.(type) {
		case *etcdserverpb.WatchRequest_CreateRequest:
			cr := u.CreateRequest
			wmu.Lock()
			nextID++
			id := nextID
			wctx, cancel := context.WithCancel(ctx)
			cancels[id] = cancel
			wmu.Unlock()

			wg.Add(1)
			go func() {
				defer wg.Done()
				s.runWatch(wctx, id, cr, send)
				wmu.Lock()
				delete(cancels, id)
				wmu.Unlock()
			}()

		case *etcdserverpb.WatchRequest_CancelRequest:
			id := u.CancelRequest.WatchId
			wmu.Lock()
			if c, ok := cancels[id]; ok {
				c()
				delete(cancels, id)
			}
			wmu.Unlock()
			_ = send(&etcdserverpb.WatchResponse{Header: s.header(s.watcher.cur()), WatchId: id, Canceled: true})

		case *etcdserverpb.WatchRequest_ProgressRequest:
			// Emit a bookmark (current revision, no events) for each live watch.
			wmu.Lock()
			ids := make([]int64, 0, len(cancels))
			for id := range cancels {
				ids = append(ids, id)
			}
			wmu.Unlock()
			rev := s.watcher.cur()
			for _, id := range ids {
				_ = send(&etcdserverpb.WatchResponse{Header: s.header(rev), WatchId: id})
			}
		}
	}
}

func (s *Server) runWatch(ctx context.Context, id int64, cr *etcdserverpb.WatchCreateRequest, send func(*etcdserverpb.WatchResponse) error) {
	key, end := normalizeWatchRange(string(cr.Key), string(cr.RangeEnd))
	startRev := cr.StartRevision
	withPrev := cr.PrevKv

	// If resuming from a compacted revision, tell the client to relist.
	if startRev > 0 {
		if compact, err := s.be.CompactRevision(ctx); err == nil && startRev < compact {
			_ = send(&etcdserverpb.WatchResponse{
				Header:          s.header(s.watcher.cur()),
				WatchId:         id,
				Created:         true,
				Canceled:        true,
				CompactRevision: compact,
				CancelReason:    "required revision has been compacted",
			})
			return
		}
	}

	sub, boundary := s.watcher.subscribe(key, end)
	defer s.watcher.unsubscribe(sub)

	if err := send(&etcdserverpb.WatchResponse{Header: s.header(s.watcher.cur()), WatchId: id, Created: true}); err != nil {
		return
	}

	// Replay history (startRev, boundary] for this range.
	if startRev > 0 && startRev <= boundary {
		from := startRev - 1
		for from < boundary {
			evs, err := s.be.After(ctx, key, end, from, 1000)
			if err != nil {
				_ = send(&etcdserverpb.WatchResponse{Header: s.header(s.watcher.cur()), WatchId: id, Canceled: true, CancelReason: err.Error()})
				return
			}
			if len(evs) == 0 {
				break
			}
			var batch []*mvccpb.Event
			var last int64
			capped := false
			for _, e := range evs {
				if e.KV.ModRevision > boundary {
					capped = true
					break
				}
				batch = append(batch, toEtcdEvent(e, withPrev))
				last = e.KV.ModRevision
			}
			if len(batch) > 0 {
				if err := send(&etcdserverpb.WatchResponse{Header: s.header(s.watcher.cur()), WatchId: id, Events: batch}); err != nil {
					return
				}
			}
			if capped || last == 0 || last >= boundary {
				break
			}
			from = last
		}
	}

	// Live stream.
	for {
		select {
		case <-ctx.Done():
			return
		case evs, ok := <-sub.ch:
			if !ok {
				_ = send(&etcdserverpb.WatchResponse{Header: s.header(s.watcher.cur()), WatchId: id, Canceled: true, CancelReason: "watcher fell behind"})
				return
			}
			var batch []*mvccpb.Event
			for _, e := range evs {
				if e.KV.ModRevision <= boundary {
					continue
				}
				if startRev > 0 && e.KV.ModRevision < startRev {
					continue
				}
				batch = append(batch, toEtcdEvent(e, withPrev))
			}
			if len(batch) == 0 {
				continue
			}
			if err := send(&etcdserverpb.WatchResponse{Header: s.header(s.watcher.cur()), WatchId: id, Events: batch}); err != nil {
				return
			}
		}
	}
}

func toEtcdEvent(e *backend.Event, withPrev bool) *mvccpb.Event {
	ev := &mvccpb.Event{}
	if e.Delete {
		ev.Type = mvccpb.DELETE
		ev.Kv = &mvccpb.KeyValue{
			Key:            []byte(e.KV.Key),
			ModRevision:    e.KV.ModRevision,
			CreateRevision: e.KV.CreateRevision,
		}
	} else {
		ev.Type = mvccpb.PUT
		ev.Kv = toMvccKV(e.KV)
	}
	if withPrev && e.PrevKV != nil {
		ev.PrevKv = toMvccKV(e.PrevKV)
	}
	return ev
}
