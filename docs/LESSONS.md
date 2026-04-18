# merutable engineering lessons

Bug-level wisdom accumulated while hardening the engine. Each entry: the
symptom, the root cause, and the anti-pattern to avoid in future code.

## 1. Config knobs with no enforcement

**Seen in**:
- Issue #2 (`max_compaction_bytes` defined but unused in the picker).
- Issue #5 (`l0_slowdown_trigger` / `l0_stop_trigger` defined but not
  checked by the write path).
- Issue #4 (`compaction_parallelism` promised N workers but
  `BackgroundWorkers::spawn` was never called from `MeruDB::open`,
  so every deployment ran single-threaded regardless).

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

## 11. `Notify::notify_waiters` race across registration windows

**Seen in**: Issue #4 — `MeruDB::close` hung under `tokio::test`'s
default `current_thread` runtime because background workers missed
the shutdown notification.

**Symptom**: `JoinHandle::await` never completes. Works on
multi-thread runtimes (worker gets scheduled, times out after 1s,
re-enters select, picks up a fresh `notified()`); deadlocks on
single-thread runtimes (worker never gets scheduled because close()
is awaiting its handle).

**Anti-pattern**: Using `tokio::sync::Notify::notify_waiters()` as
the sole shutdown signal. `notify_waiters()` only wakes tasks
CURRENTLY registered as waiters via `notified().await` — any task
between iterations of its select has missed the notification.

**Discipline**: Dual signal. `AtomicBool` flag for state, `Notify`
for wake-up. `shutdown()` sets flag FIRST, then notifies. Worker
checks flag at top of loop and inside select. A worker that missed
the notify sees the flag on its next iteration; a worker that got
the notify exits via the select branch. Both paths deterministic.

Alternative: `tokio_util::sync::CancellationToken` — proper
semantics, but adds a dep. The flag+notify pattern is in-tree.

## 12. Output-size limits in downstream libraries

**Seen in**: Issue #3 — Arrow's `BinaryArray` uses i32 offsets,
capping a single column's concatenated bytes at ~2.14 GiB. A
compaction output with >2 GiB of a ByteArray column panicked
unrecoverably.

**Symptom**: A stress test crashes ~10 GiB in. No warning before
the limit; the panic is in third-party library code.

**Anti-pattern**: Using a library's default type without reading
its interface contract. "Normal" doesn't mean "unbounded."

**Discipline**: For every downstream type, find the hard limit
(i32? i64? page size? memory cap?) and size our own thresholds
below it with a generous margin (4× in our case). Capping at the
producer (what we write) is safer than relying on the consumer
to handle overflow.

## 13. Key encoding prefix-collision across tag boundary

**Seen in**: Issue #7 — ByteArray PKs with empty or null-byte prefixes
produced correctly distinct encoded user_keys, yet point lookups
returned wrong rows. The encoded user_key terminator (`0x00`)
collided with the first byte of the following tag (`0xFF` for normal
seqs), making a shorter user_key's full ikey sort AFTER a longer
user_key's ikey — the opposite of raw-byte order.

**Symptom**: Data corruption / wrong-row reads for keys near the
empty/null-prefix region. Manifests silently correct; Parquet
silently correct; just the memtable/skiplist iteration was wrong.

**Anti-pattern**: Using a single-byte terminator for variable-length
field encoding without verifying that the next byte (the start of
the next field or the tag) ALWAYS sorts strictly greater than the
terminator. Even a "sort-preserving escape" scheme can be defeated
by what follows it.

**Discipline**: Choose a terminator that sorts strictly BEFORE any
possible continuation byte. `[0x00, 0x00]` works because the second
`0x00` is strictly less than any escape-continuation byte (`0xFF`).
CockroachDB and FoundationDB independently arrived at the same
scheme. If your encoding has a variable-length field followed by a
fixed-size suffix (tag, seq, anything), the encoding's terminator
must be bitwise-less-than every possible first byte of that suffix
— across all values the suffix can take.

## 14. State reloaders that forget some state

**Seen in**: Issue #6 — read-only replica's `refresh()` reloaded the
manifest (`Version`) but left `visible_seq` pinned at the open-time
value. New data with seq > pinned visible_seq was filtered out of
every read.

**Symptom**: "Refresh works for manifest/file list but returns
None for any key whose seq was allocated after open." The files are
there; reads don't see them.

**Anti-pattern**: Thinking of "current state" as a single unit
(here, the Version) when it's actually several correlated pieces
(Version + seq counters + in-memory mirrors). A reload function
must update EVERY piece that can become stale.

**Discipline**: When writing a reload / refresh function, ask: "if
this function was called immediately after a primary-side commit,
what ELSE in our state would be inconsistent with the new manifest?"
For merutable: the manifest's `seq_max` field is the ground truth
for seq consistency after refresh — `visible_seq` and `global_seq`
are mirrors that must be advanced.

## 15. Semantic off-by-one in monotonic counters

**Seen in**: Issue #8 — `visible_seq` was exclusive-upper-bound
("next seq to become visible") but consumers naturally treated it
as inclusive ("latest visible seq"). On a fresh DB, `visible_seq
== 1` meant "no data is at seq 1 yet," but the first put
immediately returned seq = 1, violating monotonicity the user
expected from `put_seq > read_seq_before_put`.

**Symptom**: User-facing invariants fail at the very first operation
of a fresh database. Cosmetic in isolation, but erodes trust.

**Anti-pattern**: Not documenting the semantic of a counter
(exclusive vs inclusive, pre-allocation vs post-allocation). Both
producer and consumer have to reason about it, and they'll each
pick the interpretation that makes their code simpler.

**Discipline**: Counters that readers inspect should use the
**inclusive-latest** semantic ("the highest thing that has
happened"). Internal allocation counters should use
**next-to-allocate** semantic. Keep them as separate types if the
invariant needs to hold across threads. merutable now has:
- `global_seq`: next-to-allocate (`fetch_add` before use).
- `visible_seq`: inclusive-latest-visible (`set_at_least(seq)`
  after apply).

## 16. Arrow i32 offset limits on aggregate column bytes

(Moved to entry 9; retained here so the table of contents around
Issue #3 still makes sense.)

## 17. Hard-coded physical layout keyed off logical level

**Seen in**: Issue #15 — the row-blob fast path (`_merutable_value`
column) was hard-coded to "L0 iff `level == 0`". Operators tuning for
OLTP-heavy workloads (hot L1) had no way to extend the fast path;
OLAP / append-only workloads had no way to disable it.

**Symptom**: A knob that should be workload-driven is instead hard-wired
to a proxy dimension. The code reads the proxy (level) and pretends it's
the answer (format). When a user wants to decouple them, every call site
has to change.

**Anti-pattern**: `if level == 0 { dual_format } else { columnar }`
scattered across writer, reader, codec, compactor. Each call site looks
"obvious" in isolation, but together they form a distributed hard-coded
policy that's invisible to grep-for-config.

**Discipline**: When physical layout derives from logical identity,
stamp the physical layout into the file's own metadata at write time
and read it back via that stamp — never recompute from the logical
identity on the read side. `ParquetFileMeta::format: Option<FileFormat>`
makes the format a persistent property of the file; `EngineConfig::
file_format_for(level)` is the single decision point at write time.
Legacy files (without the stamp) fall back to
`FileFormat::default_for_level(level)` which reproduces the old
hardcoded behavior, guaranteeing zero-migration safety.

## 18. Regex surgery on multi-field structs is dangerous

**Seen in**: During Issue #15 I added a `format: Option<FileFormat>`
field to `ParquetFileMeta` and used a Python regex to insert
`format: None,` at every construction site. The pattern was "after
`dv_length: ... ,` and before the closing `}`" — but `DvLocation`
also has a `dv_length` field (and closes with `}`), so the regex
fired inside structs it had no business touching. Build broke in
half a dozen places with "struct X has no field named format".

**Symptom**: A "safe" batch edit that compiles half the callers and
silently corrupts the other half. The compiler catches it eventually,
but only after the diff has spread across 10+ files and mental state
has drifted.

**Anti-pattern**: Regex-editing source as if field names are unique
across structs. They aren't. `path`, `dv_length`, `meta` all appear
in multiple structs in this codebase; `dv_length` is especially
dangerous because it appears in both `ParquetFileMeta` *and*
`DvLocation`.

**Discipline**: For struct-field additions across many files: add
`#[serde(default)]` to the new field AND make the struct use
`..Default::default()` or a constructor so callers don't have to
change at all. When mass edits are unavoidable, use an AST tool
(rust-analyzer "add field" refactor) or handwrite each call site —
regex on Rust struct literals is a foot-cannon.

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
