# Scale test & resilience plan

How we validate the Spanner-backed shim against — and past — the publicly
disclosed control-plane numbers, and how it behaves under zonal/regional
failure.

## The bar to beat

| Source | Headline numbers |
|--------|------------------|
| EKS ultra scale (re:Invent 2025) | 100K nodes, 900K pods, >10M objects, ~32 GB aggregate etcd, scheduler 500 pods/s; journal consensus + tmpfs BoltDB; key-space partitioned by resource type (~5× write throughput) |
| GKE | 65K → 130K nodes on Spanner-backed storage; **Hypercluster** ~256K nodes / ~1M chips spanning multiple regions; **Agent Sandbox**: tens-to-hundreds of millions of mostly-idle instances, hundreds of sandboxes/s |
| Kubernetes SLOs (must hold throughout) | p99 ≤ 1s for single-object GET/PUT; p99 ≤ 30s for LIST |

The dimension we can own is **object count / total store size / multi-region**:
etcd (and EKS's in-memory tmpfs design) is memory-bounded (their ~20 GB cap);
Spanner is disk-backed and horizontally sharded, so "100M+ mostly-idle objects,
multi-region" is exactly where it wins.

## Layered method ("both, layered")

**L1 — storage-layer microbench (`test/integration/cmd/loadgen`).**
Drives the shim's etcd v3 API directly via `clientv3` using the apiserver's
compare-and-swap Txn shapes. Isolates the Spanner backend from scheduler/node
costs, so it finds the *storage* ceiling and is cheap to iterate. Runs in-process
(memory backend) for smoke tests, or against a real Spanner-backed shim
(`--endpoints`) for real numbers.

**L2 — end-to-end (Kubemark + clusterloader2).**
Real `kube-apiserver` pointed at the shim (`--etcd-servers`), Kubemark hollow
nodes, and the SIG-scalability `load`/`density` suites for the credible
cluster-scale claim. Inherit the recent apiserver wins (consistent reads from
cache, streaming list responses, v1.34 snapshottable cache) by running a recent
Kubernetes — per the etcd maintainer, those did as much of the unlock as the
storage swap, so the K8s version matters.

## Workload profiles (implemented in loadgen)

### `agentic` — GKE Agent Sandbox pattern
- **Shape:** preload a large, mostly-idle population of session objects; steady
  create/delete churn on a small working set; heavy **cold-read "wakeups"** on
  random idle objects; periodic active-session updates.
- **Stresses:** object count, total store size, point-read latency on cold
  objects, watch fan-out.
- **What it proves:** Spanner serves 50–100M+ idle objects with reads inside SLO
  — past etcd/tmpfs limits. (In-process memory runs use a modest `--objects`;
  push to 50–100M against real Spanner.)
- **Key metrics:** live object count, store size, read p99 (cold), watch lag,
  steady create/delete QPS.

### `hpt` — hyperparameter-tuning sweeps
- **Shape:** bursty waves of short-lived trial objects; each updated N times
  (status) then deleted; repeat.
- **Stresses:** write QPS, update QPS, delete throughput, and **compaction**
  (short lifetimes → tombstone storms).
- **What it proves:** sustained high churn within a single hot resource while
  online compaction keeps up — no etcd-style stop-the-world defrag stalls.
- **Key metrics:** peak/sustained write+delete QPS, p99 write/delete, compaction
  lag, revision-allocation contention (the single-counter hotspot).

### loadgen flags
```
--profile agentic|hpt   --duration 60s   --writers N   --readers N
--watchers N            --objects N (agentic preload)   --value-size 3072
--hpt-wave N            --hpt-updates N   --endpoints host:port (real shim)
```

## Pass/fail

A run passes if, at the target scale, p99 GET/PUT ≤ 1s and p99 LIST ≤ 30s with
error rate ≈ 0, sustained for the run duration. We report p50/p99/p999 per op
plus watch-propagation lag (which, on the polling watcher, is ≈ poll interval —
this is the number that motivates the Change Streams notifier).

## Resilience / failure-domain matrix

The shim is **stateless**; durability, ordering, and quorum live entirely in
Spanner. So the control-plane masters are a serving tier, not a consensus group.

| Scenario | Regional Spanner (3 replicas / 3 zones) | Multi-region Spanner |
|---|---|---|
| 1 of 3 zones lost | **Full read/write** — Spanner keeps Paxos quorum (2/3), surviving masters serve | Full read/write |
| 2 of 3 zones lost | Writes **block** until a zone returns (quorum lost); cached reads still served; **no data loss, auto-recovery**; same fundamental limit as a 3-node etcd losing 2 members | **Full read/write** — quorum spans regions |
| Whole region lost | Unavailable until recovery (single-region durability) | **Survives** — promote/serve from another region |
| Any/all masters lost (Spanner healthy) | Lost capacity only; survivors serve; **no quorum/leader election among masters** | Same |

Test cases to run (L2, on real Spanner):
1. Kill 1 zone's master + observe continued writes; confirm Spanner stayed in quorum.
2. Kill 2 zones (regional config); confirm writes block but no corruption, then recover a zone and confirm clean catch-up (revision continuity, watches resume).
3. Multi-region config: fail an entire region; confirm writes continue within SLO.
4. Rolling master restart (upgrade sim): confirm zero write-quorum impact and watch continuity.

### "Does it support 3 masters?"

Yes — and it removes the constraint that made "3" special. In etcd the master
count *is* the Raft quorum (bounded by quorum math; you tolerate ⌊(n-1)/2⌋
failures). Here, consensus is offloaded to Spanner (TrueTime-ordered, the same
move EKS made with its journal), so:

- Run **N stateless masters** across **≥3 zones** for availability and read/watch
  fan-out — recommended ≥3, but the count is a capacity/HA choice, **not** a
  correctness requirement. They don't vote and don't talk to each other.
- Fault tolerance comes from the **Spanner instance config**: regional tolerates
  one zone; multi-region tolerates a region.

### Design lever this exposes (revision allocation vs. resilience)

To keep every master interchangeable with no leader to elect, revision
allocation should be **transactional in Spanner** (the next revision is assigned
inside the write transaction Spanner already serializes). That preserves
"revision increases in commit order" across concurrent masters with zero
coordination — at the cost of contention on the revision counter (the documented
hotspot). The higher-throughput alternative — a per-shard write leader with a
batched in-memory sequencer — reintroduces a per-shard leader (sub-second
lease-based failover, Spanner still the source of truth, still no quorum among
masters). Both are valid; the transactional path is the most resilient and is
what we'd ship first.
