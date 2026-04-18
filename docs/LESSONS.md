# merutable engineering lessons

Bug-level wisdom accumulated while hardening the engine. Each entry: the
symptom, the root cause, and the anti-pattern to avoid in future code.

## 1. Config knobs with no enforcement

**Seen in**: Issue #2 (`max_compaction_bytes` defined but unused), Issue #5
(`l0_slowdown_trigger` / `l0_stop_trigger` defined but not checked).

**Symptom**: Users tune a knob expecting a behaviour change; nothing
changes. Docs lie; debugging wastes hours.

**Anti-pattern**: Adding a config field with a doc comment but no code
path that consults it. A TODO in a struct definition is dead code.

**Discipline**: Every new config field ships with a test that asserts
the field changes observable behaviour. If you can't write that test,
the field shouldn't exist yet.

## 2. Single global mutex vs per-resource reservation

**Seen in**: Issue #1 and #4 (single `compaction_mutex: Mutex<()>`
serialized all compactions, blocking L0 drainage for 45 minutes behind
one deep compaction).

**Symptom**: Two threads doing orthogonal work serialize through an
unrelated lock.

**Anti-pattern**: Using `Mutex<()>` as a "big lock" for convenience.

**Discipline**: Lock at the granularity of the actual conflict domain.
Compactions conflict on levels, not on "compactor-ness". Use
`Mutex<HashSet<Resource>>` with per-resource reservation, or per-file
`is_compacting` bits (RocksDB / Pebble pattern). Keep the critical
section just long enough to reserve, never across the work itself.

## 3. Time-based grace periods for GC

**Seen in**: BUG-0007..0013 (`gc_grace_period_secs = 300` insufficient
for 40 GB integrity scan → GC deleted files mid-read).

**Symptom**: Long-running reader holds a `Version` snapshot, GC deletes
files from that snapshot based on wall-clock elapsed time, reader gets
`IO NotFound`.

**Anti-pattern**: "Wait N seconds before deleting" as the sole safety
net. Any operation slower than N seconds breaks the invariant.

**Discipline**: Reference-count or version-pin live readers. Delete a
resource only when (a) no live reference AND (b) optional time-grace
for external consumers. RocksDB's `SuperVersion` refcount is the
reference implementation.

## 4. Producer/consumer invariant drift

**Seen in**: Issue #2 (picker returned `input_files: Vec<String>` but
the compaction job used `version.files_at(level)` — taking all files,
ignoring the picker's selection).

**Symptom**: A config change (my L0 cap) has no effect because the
downstream consumer doesn't use the picker's output.

**Anti-pattern**: Two code sites independently compute the "set of
files to compact" — the picker's decision and the job's read. They
drift.

**Discipline**: Single source of truth. The picker owns selection;
the job honours `pick.input_files` exactly. A `HashSet` filter at the
boundary is the whole fix — but it MUST exist.

## 5. Cache insert-invalidate race

**Seen in**: `ea7cd35` — reader reads V1 from L0, writer invalidates
cache for V2; if the reader's `insert` lands after the writer's
`invalidate`, stale V1 lingers in cache indefinitely.

**Symptom**: Post-memtable-flush reads serve stale values for keys
with hot update patterns.

**Anti-pattern**: Async read-then-insert into a cache that's being
invalidated concurrently, with no version tag.

**Discipline**: Optimistic concurrency. Monotonic `generation`
counter, bumped on every invalidate/clear. Reader snapshots the
generation BEFORE reading; `insert_if_fresh(entry, captured_gen)`
under the cache lock drops the insert if generation advanced.

## 6. Fsync ordering

**Seen in**: Cross-layer rigor pass — metadata dir fsynced AFTER the
`version-hint.text` rename; LocalFileStore.put() missing parent-dir
fsync; WAL parent-dir fsync missing on open/rotate; WAL rotate used
`sync_data` (skips size metadata).

**Symptom**: Crash reorders the visible state on reboot — either the
pointer (CURRENT/version-hint) persists but the file it points to
doesn't, or vice versa. Silent data loss on kernel power-loss.

**Anti-pattern**: Relying on "I wrote the bytes, the filesystem will
figure it out." It won't. ext4/xfs/btrfs each have their own rules.

**Discipline**: Explicit durability ordering for any file creation +
pointer update:
1. Write data file, `fsync(file)`.
2. `fsync(parent_dir)` — commits the directory entry.
3. Write pointer tmp, `fsync(tmp)`.
4. `rename(tmp, pointer)`.
5. `fsync(parent_dir_of_pointer)` — commits the rename.

`sync_data` (fdatasync) is fine on the hot append path for WAL (size
is the only metadata we need durable), but `sync_all` (full fsync)
is required before closing a file whose size will be read on
recovery.

## 7. Reading whole files into memory

**Seen in**: 32 GiB RSS / 10 GiB data (Issue #2). Both `read_path` and
`compaction/job` use `std::fs::read(path)` which allocates the file
size as a `Vec<u8>`. Concurrent readers + multi-GiB Parquet files →
RSS explosion.

**Symptom**: RSS far exceeds the sum of in-memory working sets (cache,
memtables). Allocator fragmentation amplifies.

**Anti-pattern**: Default to "slurp the whole file" because the
Parquet reader's API takes `Bytes`. Convenient but unbounded.

**Discipline**:
- Cap per-job input: enforce `max_compaction_bytes` in the picker.
- Partial fix: Issue #2 + L1+ extension (`e176067`).
- Full fix (roadmap): streaming Parquet reader — mmap or
  chunked range reads through `object_store::get_range`. Out of scope
  for incremental fixes but tracked.

## 8. Background work spawn machinery that's never invoked

**Seen in**: Issue #4 — `BackgroundWorkers::spawn` defined in
`background.rs` but not called by `MeruEngine::open`.
`compaction_parallelism: 2` configured → user expects 2 workers →
only one exists because nothing spawned them.

**Symptom**: Config claims parallelism; reality is serial.

**Anti-pattern**: Opt-in background workers where the docs say "it
just works". If the default path doesn't invoke the machinery, the
machinery doesn't exist as far as users are concerned.

**Discipline**: Spawn in `open()`, store the handles, shut down in
`close()`. If a user wants zero background workers, expose a
`compaction_parallelism: 0` escape hatch.

## 9. Arrow i32 offset limits on aggregate column bytes

**Seen in**: Issue #3 — `BinaryArray` (i32 offsets) caps a single
column's concatenated bytes at ~2.14 GiB. A compaction output with
~2.2 GiB of a single ByteArray column panicked in
`GenericBytesBuilder::append_value`.

**Symptom**: Compaction panics on large workloads. No warning until
the limit is hit.

**Anti-pattern**: Using library defaults without reading the
interface contract. `BinaryArray` / `StringArray` look like the
"normal" type; they're not the only type.

**Discipline**: Two options:
- **Output file splitting** (what we did): cap per-file output at
  a fraction (e.g. 4×) of the library limit. Chunk boundaries MUST
  fall between distinct user_keys to preserve the L1+ non-overlap
  invariant.
- **LargeBinaryArray**: use i64 offsets. Trades off external
  compatibility (not every Parquet reader understands it) for
  simplicity.

## 10. Readers don't pin their snapshot

**Seen in**: Same as #3 above (BUG-0007..0013). A `Version` Arc
counts in `ArcSwap` but doesn't tell GC "I'm still using files
referenced by this snapshot".

**Discipline**: Every long-lived read that iterates files should
`pin_current_snapshot()` → get `(SnapshotPin, Arc<Version>)`. The
guard participates in GC's watermark (`min_pinned_snapshot`). Drop
guard → GC can advance.

## Process lessons

- **Audit the whole signal, not just the hot path**. The user-visible
  crash / hang is often a downstream symptom of a deeper design gap
  (Issue #3 crash was enabled by #4 parallelism failure enabling #5
  stall evasion).
- **Verify the fix lands end-to-end**. The picker returning a subset
  doesn't help if the job takes all files. Test the whole pipeline.
- **One commit per issue**. Each fix is a distinct memory; don't
  merge fixes because commits are cheap and bisect is invaluable.
- **CI is ground truth**. Local Rust version != stable Rust on CI;
  new clippy lints land on every minor release. Run
  `cargo clippy -- -D warnings` locally with the same toolchain CI
  uses.
