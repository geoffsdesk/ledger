// Command loadgen drives ledger with two synthetic workload profiles
// that mirror the control-plane stress of modern AI/ML clusters:
//
//	agentic — the GKE Agent Sandbox pattern: a large, mostly-idle population of
//	          session objects, with steady create/delete churn and frequent cold
//	          read "wakeups". Stresses object count and the read path.
//	hpt     — hyperparameter-tuning sweeps: bursty waves of short-lived trial
//	          objects, each updated a few times then deleted. Stresses write QPS,
//	          delete throughput and compaction.
//
// It speaks the etcd v3 API via clientv3 (the client kube-apiserver uses) and
// uses the apiserver's compare-and-swap Txn shapes, so it exercises the exact
// path the real control plane does. By default it boots an in-process,
// memory-backed shim so it runs anywhere; point --endpoints at a real
// Spanner-backed shim to measure the backend itself.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/geoffsdesk/ledger/pkg/backend/memory"
	"github.com/geoffsdesk/ledger/pkg/server"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var (
	endpoints = flag.String("endpoints", "", "etcd endpoint(s) of the shim, comma-separated; empty = boot an in-process memory-backed shim")
	profile   = flag.String("profile", "hpt", "workload profile: agentic | hpt")
	duration  = flag.Duration("duration", 15*time.Second, "steady-state duration")
	writers   = flag.Int("writers", 16, "concurrent writer goroutines")
	readers   = flag.Int("readers", 16, "agentic: concurrent reader goroutines (cold wakeups)")
	watchers  = flag.Int("watchers", 4, "concurrent prefix watchers (measure propagation lag)")
	objects   = flag.Int("objects", 50000, "agentic: idle session objects to preload")
	valueSize = flag.Int("value-size", 3072, "object value size in bytes (~pod-sized)")
	hptWave   = flag.Int("hpt-wave", 200, "hpt: trial objects per wave per writer")
	hptUpd    = flag.Int("hpt-updates", 3, "hpt: status updates per trial before deletion")
	base      = flag.String("prefix", "/registry/loadgen", "key prefix")
)

var (
	start  time.Time
	gOps   int64 // successful ops, for throughput
	wWrite = newRecorder()
	wRead  = newRecorder()
	wDelete = newRecorder()
	wUpdate = newRecorder()
	wLag    = newRecorder()
	ring    = newKeyRing(200000)
)

// recorder collects latency samples (reservoir-bounded) and an error count.
type recorder struct {
	mu      sync.Mutex
	samples []time.Duration
	capN    int
	seen    int64
	errs    int64
	rng     *rand.Rand
}

func newRecorder() *recorder { return &recorder{capN: 500000, rng: rand.New(rand.NewSource(1))} }

func (r *recorder) ok(d time.Duration) {
	atomic.AddInt64(&gOps, 1)
	r.mu.Lock()
	r.seen++
	if len(r.samples) < r.capN {
		r.samples = append(r.samples, d)
	} else if j := int(r.rng.Int63n(r.seen)); j < r.capN {
		r.samples[j] = d
	}
	r.mu.Unlock()
}

func (r *recorder) fail() { atomic.AddInt64(&r.errs, 1) }

func (r *recorder) summary() (n, errs int64, p50, p99, p999 time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, errs = r.seen, atomic.LoadInt64(&r.errs)
	if len(r.samples) == 0 {
		return
	}
	s := append([]time.Duration(nil), r.samples...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	pick := func(p float64) time.Duration {
		i := int(p * float64(len(s)))
		if i >= len(s) {
			i = len(s) - 1
		}
		return s[i]
	}
	return n, errs, pick(0.50), pick(0.99), pick(0.999)
}

// keyRing is a bounded set of recently-written keys for readers to sample.
type keyRing struct {
	mu   sync.Mutex
	buf  []string
	capN int
	idx  int
}

func newKeyRing(capN int) *keyRing { return &keyRing{capN: capN} }

func (k *keyRing) add(s string) {
	k.mu.Lock()
	if len(k.buf) < k.capN {
		k.buf = append(k.buf, s)
	} else {
		k.buf[k.idx] = s
		k.idx = (k.idx + 1) % k.capN
	}
	k.mu.Unlock()
}

func (k *keyRing) sample(rng *rand.Rand) (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.buf) == 0 {
		return "", false
	}
	return k.buf[rng.Intn(len(k.buf))], true
}

func makeValue(size int) []byte {
	if size < 8 {
		size = 8
	}
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b, uint64(time.Now().UnixNano()))
	return b
}

func lagOf(v []byte) (time.Duration, bool) {
	if len(v) < 8 {
		return 0, false
	}
	ts := int64(binary.BigEndian.Uint64(v[:8]))
	if ts <= 0 {
		return 0, false
	}
	return time.Since(time.Unix(0, ts)), true
}

func opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

// create/update/del use the apiserver's compare-and-swap Txn shapes.
func create(cli *clientv3.Client, key string, val []byte) (int64, bool, error) {
	ctx, cancel := opCtx()
	defer cancel()
	tr, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(val))).
		Else(clientv3.OpGet(key)).Commit()
	if err != nil {
		return 0, false, err
	}
	return tr.Header.Revision, tr.Succeeded, nil
}

func update(cli *clientv3.Client, key string, rev int64, val []byte) (int64, bool, error) {
	ctx, cancel := opCtx()
	defer cancel()
	tr, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
		Then(clientv3.OpPut(key, string(val))).
		Else(clientv3.OpGet(key)).Commit()
	if err != nil {
		return 0, false, err
	}
	return tr.Header.Revision, tr.Succeeded, nil
}

func del(cli *clientv3.Client, key string, rev int64) (bool, error) {
	ctx, cancel := opCtx()
	defer cancel()
	tr, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
		Then(clientv3.OpDelete(key)).
		Else(clientv3.OpGet(key)).Commit()
	if err != nil {
		return false, err
	}
	return tr.Succeeded, nil
}

func main() {
	flag.Parse()
	if *profile != "agentic" && *profile != "hpt" {
		log.Fatalf("unknown --profile %q (want agentic|hpt)", *profile)
	}

	eps := *endpoints
	var stopShim func()
	if eps == "" {
		addr, stop := startInProcessShim()
		eps, stopShim = addr, stop
		log.Printf("booted in-process memory-backed shim at %s", addr)
		defer stopShim()
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(eps, ","), DialTimeout: 5 * time.Second})
	if err != nil {
		log.Fatalf("clientv3: %v", err)
	}
	defer cli.Close()

	start = time.Now()
	if *profile == "agentic" {
		preloadAgentic(cli)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < *watchers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); watcher(ctx, cli) }()
	}
	for i := 0; i < *writers; i++ {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id) + 1))
			if *profile == "agentic" {
				agenticWriter(ctx, cli, id, rng)
			} else {
				hptWriter(ctx, cli, id, rng)
			}
		}()
	}
	if *profile == "agentic" {
		for i := 0; i < *readers; i++ {
			wg.Add(1)
			id := i
			go func() {
				defer wg.Done()
				reader(ctx, cli, rand.New(rand.NewSource(int64(id)+1000)))
			}()
		}
	}
	go reporter(ctx)

	wg.Wait()
	printSummary()
}

func preloadAgentic(cli *clientv3.Client) {
	log.Printf("preloading %d idle session objects...", *objects)
	var wg sync.WaitGroup
	ch := make(chan int, 1024)
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range ch {
				k := fmt.Sprintf("%s/session/idle-%09d", *base, n)
				if _, _, err := create(cli, k, makeValue(*valueSize)); err == nil {
					ring.add(k)
				}
			}
		}()
	}
	for n := 0; n < *objects; n++ {
		ch <- n
	}
	close(ch)
	wg.Wait()
	log.Printf("preload done (%d objects); starting steady state", *objects)
}

// agenticWriter keeps a bounded working set of its own sessions, churning
// create/update/delete at full speed while the large preloaded population sits
// idle for readers to wake.
func agenticWriter(ctx context.Context, cli *clientv3.Client, id int, rng *rand.Rand) {
	revs := map[string]int64{}
	seq := 0
	cap := *objects/max(1, *writers) + 1
	for ctx.Err() == nil {
		seq++
		k := fmt.Sprintf("%s/session/w%03d-%09d", *base, id, seq)
		t0 := time.Now()
		if rev, ok, err := create(cli, k, makeValue(*valueSize)); err != nil {
			wWrite.fail()
		} else {
			wWrite.ok(time.Since(t0))
			if ok {
				revs[k] = rev
				ring.add(k)
			}
		}
		if len(revs) > 0 && rng.Intn(3) == 0 { // active session update
			for kk, rv := range revs {
				t1 := time.Now()
				if nr, ok, err := update(cli, kk, rv, makeValue(*valueSize)); err != nil {
					wUpdate.fail()
				} else {
					wUpdate.ok(time.Since(t1))
					if ok {
						revs[kk] = nr
					}
				}
				break
			}
		}
		if len(revs) > cap { // session ended
			for kk, rv := range revs {
				t2 := time.Now()
				if _, err := del(cli, kk, rv); err != nil {
					wDelete.fail()
				} else {
					wDelete.ok(time.Since(t2))
				}
				delete(revs, kk)
				break
			}
		}
	}
}

func reader(ctx context.Context, cli *clientv3.Client, rng *rand.Rand) {
	for ctx.Err() == nil {
		k, ok := ring.sample(rng)
		if !ok {
			time.Sleep(time.Millisecond)
			continue
		}
		c, cancel := opCtx()
		t0 := time.Now()
		_, err := cli.Get(c, k)
		cancel()
		if err != nil {
			wRead.fail()
		} else {
			wRead.ok(time.Since(t0))
		}
	}
}

// hptWriter runs sweeps: create a wave of trials, update each a few times, then
// delete the whole wave. Short lifetimes -> heavy write/delete/compaction churn.
func hptWriter(ctx context.Context, cli *clientv3.Client, id int, rng *rand.Rand) {
	type trial struct {
		key string
		rev int64
	}
	waveNum := 0
	for ctx.Err() == nil {
		waveNum++
		trials := make([]trial, 0, *hptWave)
		for i := 0; i < *hptWave && ctx.Err() == nil; i++ {
			k := fmt.Sprintf("%s/pods/trial-w%03d-%06d-%04d", *base, id, waveNum, i)
			t0 := time.Now()
			if rev, ok, err := create(cli, k, makeValue(*valueSize)); err != nil {
				wWrite.fail()
			} else {
				wWrite.ok(time.Since(t0))
				if ok {
					trials = append(trials, trial{k, rev})
					ring.add(k)
				}
			}
		}
		for u := 0; u < *hptUpd && ctx.Err() == nil; u++ {
			for i := range trials {
				t0 := time.Now()
				if nr, ok, err := update(cli, trials[i].key, trials[i].rev, makeValue(*valueSize)); err != nil {
					wUpdate.fail()
				} else {
					wUpdate.ok(time.Since(t0))
					if ok {
						trials[i].rev = nr
					}
				}
			}
		}
		for i := range trials {
			t0 := time.Now()
			if _, err := del(cli, trials[i].key, trials[i].rev); err != nil {
				wDelete.fail()
			} else {
				wDelete.ok(time.Since(t0))
			}
		}
	}
}

func watcher(ctx context.Context, cli *clientv3.Client) {
	wch := cli.Watch(ctx, *base, clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return
		case wr, ok := <-wch:
			if !ok || wr.Err() != nil {
				return
			}
			for _, ev := range wr.Events {
				if ev.Kv != nil {
					if lag, ok := lagOf(ev.Kv.Value); ok {
						wLag.ok(lag)
					}
				}
			}
		}
	}
}

func reporter(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	var last int64
	lastT := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			cur := atomic.LoadInt64(&gOps)
			log.Printf("t+%4.0fs  ops=%-9d  rate=%.0f ops/s", time.Since(start).Seconds(), cur, float64(cur-last)/now.Sub(lastT).Seconds())
			last, lastT = cur, now
		}
	}
}

func printSummary() {
	dur := time.Since(start).Seconds()
	total := atomic.LoadInt64(&gOps)
	fmt.Printf("\n==== loadgen summary (profile=%s, %.1fs) ====\n", *profile, dur)
	fmt.Printf("total successful ops: %d   (avg %.0f ops/s)\n", total, float64(total)/dur)
	row := func(name string, r *recorder) {
		n, errs, p50, p99, p999 := r.summary()
		if n == 0 && errs == 0 {
			return
		}
		fmt.Printf("  %-9s n=%-9d errs=%-6d p50=%-9v p99=%-9v p999=%v\n",
			name, n, errs, p50.Round(time.Microsecond), p99.Round(time.Microsecond), p999.Round(time.Microsecond))
	}
	row("create", wWrite)
	row("update", wUpdate)
	row("delete", wDelete)
	row("read", wRead)
	if n, _, p50, p99, _ := wLag.summary(); n > 0 {
		fmt.Printf("  %-9s n=%-9d              p50=%-9v p99=%v\n", "watchlag", n, p50.Round(time.Microsecond), p99.Round(time.Microsecond))
	}
}

func startInProcessShim() (string, func()) {
	be := memory.New()
	srv := server.New(be, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("shim start: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(16<<20), grpc.MaxSendMsgSize(16<<20))
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	return lis.Addr().String(), func() { gs.Stop(); cancel() }
}
