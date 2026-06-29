# ledger

![Nick Eberts Compatibility](https://img.shields.io/badge/Nick_Eberts-NOT_SUPPORTED-red)

An **etcd v3 gRPC server backed by Google Cloud Spanner**. It runs as a proxy on
the Kubernetes control-plane VM, beside `kube-apiserver`, and is pointed at via
`--etcd-servers`. The apiserver believes it is talking to etcd; storage actually
lives in Spanner.

This is the same architectural pattern GKE uses for its largest clusters
(Spanner-backed control-plane storage that exposes the etcd API for backward
compatibility). The open-source precedent is k3s's
[`kine`](https://github.com/k3s-io/kine) ("kine is not etcd"), which implements
the etcd API over SQL backends. This project is, in effect, **kine with a Spanner
backend** — built from scratch so the Spanner-specific concerns (revision
allocation, transactions, change polling) are explicit.

> Status: working prototype. The in-memory backend and core gRPC/Txn/Watch logic
> are unit-tested. The Spanner backend targets the Cloud Spanner emulator or a
> real instance. Not production-hardened (see *Limitations*).

---

## Why this works

`kube-apiserver` only relies on a handful of properties of etcd's MVCC store:

1. **A single global revision** — an `int64` that increases by one on every
   mutation, assigned in commit order.
2. **Read-as-of-revision** — a list/get can be served at any non-compacted
   revision (this is how consistent LISTs and pagination work).
3. **Resumable watch** — a watch started at revision *R* sees every change after
   *R*, in order.
4. **Compare-and-swap** — every write is an optimistic transaction conditioned on
   a key's current `ModRevision` (this is how `resourceVersion` concurrency is
   enforced).

Any backend that honours those four properties can stand in for etcd. The shim
provides them on top of Spanner.

---

## Architecture

```
                         control-plane VM
   ┌───────────────────────────────────────────────────────────┐
   │  kube-apiserver ──etcd v3 gRPC (127.0.0.1:2379, mTLS)──┐    │
   │                                                        ▼    │
   │                                            ledger │
   │                                   ┌────────────────────────┐ │
   │                                   │ KV / Txn / Watch / Lease│ │
   │                                   │ Maintenance.Status      │ │
   │                                   └───────────┬────────────┘ │
   └───────────────────────────────────────────────┼─────────────┘
                                                    │ Spanner client
                                                    ▼
                                        Google Cloud Spanner
                                   (table `kine`, append-only log)
```

The shim implements only the subset of the etcd v3 API the apiserver uses:
`KV.Range/Put/DeleteRange/Txn/Compact`, `Watch`, `Lease.*`, and
`Maintenance.Status` (plus a gRPC health service). Everything else returns
`Unimplemented`.

### Storage model (the `kine` insight)

The whole keyspace is one append-only table. Each `Put`/`Delete` inserts a row
whose primary key `id` **is** the global revision.

| column            | meaning                                            |
|-------------------|----------------------------------------------------|
| `id`              | global revision (primary key)                      |
| `name`            | the key                                            |
| `created`         | true if this row is the key's first appearance     |
| `deleted`         | true if this row is a tombstone                    |
| `create_revision` | revision the key was created at                    |
| `prev_revision`   | `id` of the previous row for this key              |
| `lease`           | attached lease id (0 = none)                       |
| `value`           | the value                                          |
| `old_value`       | previous value (for watch `prev_kv`)               |

- **Current value of a key** = the highest-`id` row for that `name` (unless it's
  a tombstone).
- **Read at revision R** = highest-`id` row with `id <= R`.
- **List a prefix** = latest row per `name` in `[key, prefixEnd)`, `deleted=false`.
- **Watch** = poll `id > cursor` and convert rows to events.
- **Compact(R)** = delete superseded rows and tombstones with `id <= R`.

A second table `kine_meta` holds two single-row counters: `revision` (the
allocator) and `compact_revision` (the watermark).

### etcd → Spanner mapping

| etcd operation                  | Spanner implementation                                              |
|---------------------------------|---------------------------------------------------------------------|
| `Range` (get/list)              | read-only snapshot query, latest-per-key, `ORDER BY name`           |
| `Txn` create `If(Mod==0)`       | read-write txn: verify key absent, insert row, bump `revision`      |
| `Txn` update `If(Mod==N)`       | read-write txn: verify `Mod==N`, insert row, bump `revision`        |
| `Txn` delete `If(Mod==N)`       | read-write txn: verify `Mod==N`, insert tombstone, bump `revision`  |
| `Watch`                         | one shared poll loop (`id > cursor`) fanned out to all watchers     |
| `Lease` + TTL expiry            | in-memory lease table + reaper that tombstones attached keys        |
| `Compact`                       | batched deletes of superseded rows; advance watermark               |
| `Maintenance.Status`            | synthesised (version `3.5.x`, current revision as raft index)       |

The etcd compare-and-swap maps **directly** onto a Spanner read-write
transaction: the read of the key's current `ModRevision`, the comparison, and
the conditional append all happen inside one serializable transaction. That is
the entire reason this approach is correct.

---

## The revision counter: correctness vs. the write hotspot

etcd revisions must be **strictly monotonic and assigned in commit order**.
Spanner has no auto-increment, and bit-reversed sequences are deliberately
*non*-monotonic (they exist to avoid hotspots), so they can't be used here.

This prototype uses the simple correct thing: a **single counter row**
(`kine_meta` where `k='revision'`) read-and-incremented inside the same
read-write transaction as the data write. Serializable isolation guarantees a
total order, so revisions come out monotonic and gap-free. It also guarantees
that if revision *N* is visible, every revision `< N` is committed — which is
what lets the watch poller safely advance its cursor.

The cost: **that counter row is the global write-serialization point** — the
classic monotonic-key hotspot. It bounds single-shard write throughput. This is
exactly the problem a production Spanner-backed control plane has to solve, via
approaches such as:

- **Sharding the keyspace** across multiple Spanner databases/instances (e.g.
  per-resource-type or hash-based), each with its own revision domain, and
  composing them behind the shim. (GKE's scale numbers come from sharding.)
- **Batching** many apiserver writes per Spanner transaction to amortise the
  counter contention.
- Replacing the append-PK with a **hash-prefixed primary key** so row inserts
  don't also hotspot the end of the table's key range.

The shim is structured around a `backend.Backend` interface precisely so a
sharded/batched backend can replace the naive single-counter one without
touching the gRPC layer.

---

## Build

```bash
go build ./...
go test ./...
```

### End-to-end test (real etcd client)

`test/integration` is a **separate module** (so etcd's `clientv3` and its gRPC
version don't perturb the Spanner client's dependency graph). It boots the shim
in-process and drives it with the same `clientv3` library kube-apiserver uses —
Put/Get, the Txn compare-and-swap create shape, and a prefix Watch:

```bash
cd test/integration && go test ./...
```

## Run against the Spanner emulator

```bash
# 1. Start the emulator (Docker), or `gcloud emulators spanner start`.
docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator

# 2. Point the client at it.
export SPANNER_EMULATOR_HOST=localhost:9010

# 3. Create instance + database + schema, then serve.
go run ./cmd/ledger \
  --backend=spanner \
  --spanner-database=projects/test/instances/kcp/databases/cluster \
  --init \
  --listen=127.0.0.1:2379
```

No-Spanner smoke test (in-memory backend):

```bash
go run ./cmd/ledger --backend=memory --listen=127.0.0.1:2379
# in another shell:
ETCDCTL_API=3 etcdctl --endpoints=127.0.0.1:2379 put /foo bar
ETCDCTL_API=3 etcdctl --endpoints=127.0.0.1:2379 get /foo
ETCDCTL_API=3 etcdctl --endpoints=127.0.0.1:2379 watch --prefix /
```

## Point a real apiserver at it

```bash
kube-apiserver \
  --etcd-servers=http://127.0.0.1:2379 \
  ...
# with mTLS, run the shim with --cert-file/--key-file/--trusted-ca-file and use
# --etcd-servers=https://127.0.0.1:2379 plus --etcd-cafile/--etcd-certfile/--etcd-keyfile
```

On the control-plane VM, install the binary and use `deploy/ledger.service`.
Authentication to Spanner uses the VM's service account (ADC); grant it
`roles/spanner.databaseUser`.

## Flags

| flag                    | default              | meaning                                      |
|-------------------------|----------------------|----------------------------------------------|
| `--listen`              | `127.0.0.1:2379`     | address to serve the etcd API on             |
| `--backend`             | `spanner`            | `spanner` or `memory`                        |
| `--spanner-database`    | —                    | `projects/P/instances/I/databases/D`         |
| `--init`                | `false`              | create instance/db/schema before serving     |
| `--watch-poll-interval` | `100ms`              | backend poll cadence (watch latency)         |
| `--cert-file` / `--key-file` / `--trusted-ca-file` | — | TLS / mTLS for the etcd endpoint |

---

## Layout

```
cmd/ledger/   entrypoint: flags, wiring, TLS, shutdown
pkg/backend/             Backend interface + wire-independent types + key helpers
pkg/backend/memory/      in-memory backend (tests / dev)
pkg/backend/spanner/     Spanner backend + schema/DDL + emulator bootstrap
pkg/server/              etcd v3 gRPC: kv, txn, watch, lease, maintenance
deploy/                  schema.sql, systemd unit
test/integration/        separate module: end-to-end test via etcd clientv3
```

---

## Limitations (prototype)

- **Single revision counter** → write throughput is bounded; needs sharding/
  batching for production scale (see above).
- **Watch is poll-based** (Kine-style). Latency ≈ poll interval. A
  production build would switch the poller for **Spanner Change Streams** behind
  the same notifier seam.
- **Leases are in-memory** → lease state is lost on restart (attached keys are
  not reaped until re-granted). Fine for Events; persist leases for correctness.
- `Txn` implements the apiserver's CAS shapes atomically and a best-effort
  non-atomic path for everything else; it is **not** a complete etcd Txn engine.
- `Maintenance.Status` reports a synthetic `DbSize` of 0.
- No multi-member/raft endpoints (`MemberList`, `MoveLeader`) — single logical
  member.
