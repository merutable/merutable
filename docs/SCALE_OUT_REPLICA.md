# Scale-out RO replica: base + log-replayed tail with hot-swap rebase

Issue #32. This document describes the architecture merutable uses to
turn "the mirror exists" (Issue #31) into "remote readers see a
fresh, low-latency view of the primary."

## What problem this solves

The #31 hybrid mirror makes the primary's flushed state available on
an object store. But "available" is not "useful" until something
reads it intelligently. A naive remote reader has two bad options:

1. **Cold scan from object store every query.** Pays object-store
   latency per query. No reuse across queries. No tail awareness.
2. **Replay everything from scratch on every advance.** Stop-the-world
   reconstruction on every mirror tick. Reader has visible-no-
   progress windows.

The right shape is a **scale-out RO replica**: a long-lived process
that maintains hot in-memory state on top of an object-store base,
advances incrementally as the mirror advances, and consumes the
in-flight tail from a sequenced log. This is what makes the HTAP
claim end-to-end deliverable to a remote analytical consumer.

## Composition

The replica's view is composed from two sources:

- **Base**: an object-store layout (a #31 mirror destination, or a
  full `CommitMode::ObjectStore` bucket — same shape). Read via
  `OpenOptions::read_only(true) + CommitMode::ObjectStore`.
- **Tail**: streamed from a sequenced log. The log source is
  pluggable via the `LogSource` trait.

```
┌─────────────────────────────────┐   ┌──────────────────────────────┐
│  merutable replica process      │   │  primary (POSIX + mirror)    │
│                                 │   │                              │
│  ┌──────────────┐  merge ┌────┐ │   │                              │
│  │ ObjectStore  │──────▶│Read│◀├───┤ primary writes POSIX          │
│  │ base (#26 l  │        │path│ │   │                              │
│  │ ayout,       │        └────┘ │   │ mirror worker uploads SSTs + │
│  │ via #31 mirr)│         ▲     │   │ manifests to S3 (#31)        │
│  └──────────────┘         │     │   │                              │
│                           │     │   │ change-feed Flight SQL       │
│  ┌──────────────┐         │     │   │ endpoint streams ops (#29)   │
│  │ Tail state   │─────────┘     │◀──┤                              │
│  │ (log replay) │               │   │                              │
│  └──────────────┘               │   │                              │
└─────────────────────────────────┘   └──────────────────────────────┘
```

The replica's `visible_seq = tail.latest_seq` — tracks the LOG, not
the mirror. The mirror is a periodic rebase point that bounds tail
length and reconstructs the in-memory state cheaply.

## `LogSource` trait: pluggability is the contract

v1 ships exactly one impl: `ChangeFeedLogSource` consuming
`merutable_changes` (#29) over Flight SQL.

Future impls plug in without changing the replica core:

- `RaftLogSource` — HA primary topology.
- `KafkaLogSource` — federated CDC topology.
- `ObjectStoreLogSource` — if we ever expose the WAL on object store.

Trait shape (Phase 1 — merutable-replica crate):

```rust
#[async_trait]
pub trait LogSource: Send + Sync + 'static {
    async fn stream(&self, since: u64) -> Result<BoxStream<'static, Result<OpRecord>>>;
    async fn latest_seq(&self) -> Result<u64>;
}
```

The trait is the contract. Picking one impl at `OpenOptions` time,
with the others as follow-on plugins, is the v1 shape.

## State machine

```rust
struct ReplicaState {
    base_snapshot_id: SnapshotId,  // anchor in the object store
    base_seq: Seq,                  // visible_seq at base_snapshot_id
    tail: Arc<MemtableLikeState>,   // log ops replayed on top
    visible_seq: Seq,               // base_seq + count of tail ops
}
```

Reads merge the base (object-store-backed Parquet scan) with the
tail (in-memory replayed log). Merge is per-PK last-writer-wins by
seq — the same logic the primary's reader path already does.

### Append-only tail advance

Common case, cheap. When `LogSource::latest_seq()` exceeds
`visible_seq`:

1. Stream ops from `(visible_seq, latest_seq]`.
2. Apply each op into `tail`. Tombstones become tombstones;
   INSERT/UPDATE become row entries.
3. Advance `visible_seq` atomically.

No rebase. Tail grows monotonically until the mirror advances.

### Hot-swap rebase

Heart of the issue. When the mirror lands a new snapshot at
`mirror_seq > base_seq`:

1. **Spawn a new `ReplicaState` in parallel.** New state takes
   `base_snapshot_id = new`, `base_seq = mirror_seq`, empty tail.
   Begin replaying the log from `mirror_seq` forward.
2. **Old state continues serving.** In-flight reads see a
   consistent `(base_seq=S, tail=...)` view.
3. **New state warms up.** Replays ops from `mirror_seq` forward
   until its `visible_seq >= old.visible_seq`. Catch-up cost is
   bounded by `(visible_seq - mirror_seq)` — small, mirror-lag-
   bounded.
4. **Hot-swap.** `ArcSwap<ReplicaState>` atomically retargets. New
   incoming reads land on new state.
5. **Drain + drop.** Old state retained for `rebase_drain_ttl_secs`
   (default 60). After TTL, in-flight readers on the old state
   either finish or get a structured migration error.

**Design decision: new reads during warmup land on OLD state**, not
new. Option (a) gives zero serving gap with at-most-one-mirror-tick
staleness (the same RPO operators already accept from the mirror).
Option (b) blocks reads for warmup duration. The whole point of
hot-swap is zero serving gap; (a) preserves it.

## Failure modes

1. **Log gap** (`LogGap` / `ChangeFeedBelowRetention`): replica's
   `since` is below the log source's earliest available seq. Recovery:
   hard reset. New `ReplicaState` anchored on the latest mirror
   snapshot with empty tail. Loud warning — long-lived replicas must
   keep up with retention, or they restart cold.
2. **Mirror stale**: no new mirror snapshots. Tail grows. Replica
   continues serving correctly. Warn above a configured tail-length
   threshold.
3. **New state warmup never catches up**: log advancing faster than
   warmup can replay. v1 logs a warning and retries. v2 may add
   adaptive batching.
4. **Object-store unavailable mid-base-read**: existing object-store
   retry behavior applies. Serves from memory where possible; cold
   block scans return a degraded-mode error.

## What this enables

- **Cross-region read scaling.** RO replicas in any region that can
  reach the object store + log source.
- **Multi-replica fan-out.** N replicas, each independent, all
  consistent up to their own `visible_seq`.
- **Zero serving gap on mirror advance.** Hot-swap keeps old state
  serving until new state is caught up.

## What this does NOT include

- **Multi-region writes** — separate consensus problem.
- **Write-side replica promotion** (replica → primary) — different
  correctness model.
- **Cross-region Raft quorum on the log source** — the trait allows
  a Raft impl; landing one is its own issue.

## Phases

- **Phase 1 (shipped, merutable-replica crate)**: `LogSource`
  trait, `OpRecord`, `LogGap`, placeholder `ChangeFeedLogSource`
  stub. No `ReplicaState` yet.
- **Phase 2 (planned)**: real `ChangeFeedLogSource` over Flight SQL
  (depends on #29 Phase 2).
- **Phase 3 (planned)**: `ReplicaState` + append-only tail advance.
  Growing tail OK for this phase; no rebase.
- **Phase 4 (planned)**: hot-swap rebase worker + drain TTL.
- **Phase 5 (planned)**: metrics surface, log-gap recovery, stress
  test harness.

## Metrics (Phase 5)

- `replica_tail_length` — current tail size in ops.
- `replica_visible_seq` — reader-visible seq.
- `replica_base_seq` — base snapshot's seq.
- `replica_rebase_count` — lifetime rebase events.
- `replica_rebase_warmup_secs` — histogram of warmup durations.

## Dependencies

- **#29** (change feed) — the v1 `LogSource` impl consumes it.
- **#31** (hybrid mirror) — writes the object-store layout the
  replica mounts.
- **#26 + #28** — the layout itself.

The full chain: `#26 + #28 (layout)` → `#31 (writes layout from POSIX
primary)` → `#32 (reads layout + consumes tail)` → `#29 (v1 tail source)`.
