# Issue #39 — ghost-rows regression investigation synthesis

**Status**: closed without a code fix. Exhaustive symbolic audit
and three orthogonal stress-test reproducers (50k single-writer,
20k four-reader, 40k eight-reader with synchronous mid-run
checkpoints reaching L2) all surface **zero post-quiescence
ghosts**. The `#38` crate-collapse diff is byte-identical on every
engine internal (flush / compaction / read-path / cache / memtable)
so the collapse cannot be the cause.

Reopen with new evidence — specifically the reporter's stress-test
harness source or an op-sequence reproducer that fires under this
repository's `cargo test` — and the analysis below is the hand-off.

## The bug (as reported)

- Turbo profile: 16 MB memtable, L0 trigger=2, 4 compaction +
  2 flush workers, 8 concurrent readers.
- Target: 2 GB logical writes.
- 1 GB integrity checkpoint: **1 ghost in 50 sampled deleted keys**
  (2.00%). 2 GB and 3 GB checkpoints: zero. Reproducible across
  two independent runs at the 1 GB mark.
- Symptom: `db.delete(K)` succeeded, but a later `db.get(K)` on the
  same key returned `Some(row)`.

## Why `#38` is not causal

The crate-collapse refactor moved source files from ten
`merutable-*/` sub-crates into one `merutable/src/*/` tree. The
engine internals that could plausibly cause a tombstone-loss are:

- `engine/flush.rs`
- `engine/read_path.rs`
- `engine/cache.rs`
- `engine/compaction/iterator.rs`
- `engine/compaction/job.rs`
- `engine/engine.rs`

`git diff 44e15a3 d1b8fcd -- <those paths>` shows **only** path
rewrites:
- `merutable_types::` → `crate::types::`
- `merutable_iceberg::` → `crate::iceberg::`
- `merutable_parquet::` → `crate::parquet::`
- `crate::metrics::` → `crate::engine::metrics::`

No control flow, no type changes, no reordered operations, no
semantic differences.

## Symbolic audit checklist

All of the tombstone-loss paths I could enumerate, and why each
cannot produce a ghost under the 0.1 engine as currently
implemented:

| Path | Outcome of audit |
|---|---|
| Memtable → L0 flush drops tombstone | `flush.rs` writes every `iter()`-yielded entry including tombstones; `MemtableIterator` preserves tombstones explicitly (comment line 10-11) |
| L0 → L1 compaction drops tombstone | `should_drop_tombstones(L1, 4) = false`; iterator only drops when output level strictly exceeds `num_configured_levels` |
| L0 file key_min/key_max understates range | flush tracks min on first entry (sorted PK ASC) and max every iteration; iterator guarantees PK-ascending order |
| Compaction OutputChunk min/max bug | `push` tracks both on every entry with `<` / `>` comparisons — correct regardless of input order |
| Overlap pull-in misses an older-Put file | `compute_union_range` + filter on `f.key_min <= umax && f.key_max >= umin`; files with empty key_min/key_max treated as unbounded; no file with K's key can be excluded |
| Read path skips a file with a tombstone | L0 sorted seq_max DESC; iterated newest-first. Bloom/range gates only prune files that genuinely can't contain the key (no false negatives — bloom is conservative) |
| Row cache serves stale Put | Generation-tagged `insert_if_fresh` rejects stale inserts; `cache.invalidate` fires on every delete (write_path.rs line 244) |
| Memtable `get` returns stale version | `iter_all_versions` and `get` both scan seq DESC; dedup keeps newest visible |
| `engine.read_seq` returns stale seq | `visible_seq` is atomic, set inside the WAL lock AFTER memtable apply; monotonic |
| Seq number reuse / reversal | `global_seq.allocate_n` under WAL lock; strictly monotonic |
| DV stamps a tombstone row | merutable compaction is full-rewrite; no DV stamping path writes post-compaction |
| WAL recovery mis-types a Delete | WAL decodes `OpType::Delete` explicitly; memtable apply respects the op type |
| Parquet codec mis-types op_type byte | InternalKey encoding stamps a single byte; decode masks `& 0xFF`; no drift path found |

## Local reproducer coverage

Three tests exercise progressively harsher workloads:

- `engine_ghost_rows_turbo.rs` — 50k ops, single writer, turbo.
  784s on a dev laptop. **Zero ghosts.**
- `engine_issue39_concurrent_ghost.rs` — 20k ops, 4 concurrent
  readers, turbo. 297s. **Zero post-quiescence ghosts.**
- `engine_issue39_concurrent_ghost_large.rs` (`#[ignore]`) —
  40k ops, 8 concurrent readers, compressed `level_target_bytes`
  so the tree reaches L2, four synchronous mid-run integrity
  checkpoints (each drains flush+compact, then sweeps every
  shadow entry from inside the write loop). 1332s. **Zero
  post-quiescence ghosts at every checkpoint, zero final
  ghosts, zero missing live keys.** Concurrent in-flight
  ghosts (from readers racing shadow updates) are tracked
  separately, never asserted — they're the shadow-update-lag
  pattern the `#23` closure attributed similar reports to.

## The remaining hypothesis (un-verifiable from outside)

The reporter's previous ghost-rows report (`#23`) closed with the
diagnosis that the integrity check's `shadow.insert(K, false)`
was not atomic with `engine.delete(K).await.unwrap()`. A
concurrent reader scheduled between the two lines observed
`shadow = deleted` while the engine still had the prior Put —
producing a false ghost report that was really a harness race.

`#39`'s wording ("successfully delete()d still returns Some") is
consistent with either a real engine bug **or** the same harness
race. Without the reporter's harness source or an
op-sequence reproducer that fires `cargo test` here, the two are
indistinguishable from outside — and the 40k-op 8-reader
synchronous-checkpoint test explicitly eliminates the harness-
race confound and still finds no ghost.

## What would reopen this

1. An op-sequence reproducer (key writes + deletes + reads, no
   harness dependency) that makes `engine_issue39_concurrent_ghost_large`
   or a sibling regression test fire a post-quiescence ghost.
2. The reporter's harness source, pointing at a specific
   delete-vs-check race that the synchronous-checkpoint sweep
   doesn't already cover.

Either is sufficient to reopen. Until then, the audit budget is
exhausted and the local coverage is as tight as it can be without
a shared reproducer.
