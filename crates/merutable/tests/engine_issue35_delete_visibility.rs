//! Issue #35 regression: deleted keys must never be visible from
//! `get()`, across every ordering of (flush, compaction, read, cache
//! populate). Pins the specific code paths audited while
//! investigating #35 (follow-on from #23's closure).
//!
//! #35's stress-test harness reports 2/50 ghost rows at a 1 GB data
//! checkpoint under turbo config, clearing by 2 GB. Symbolic review
//! of the write/read paths plus the `ghost_rows_turbo` 50k-op
//! single-writer stress test (784s, zero ghosts) failed to
//! reproduce the bug locally. The scale in this test (~1k ops,
//! seconds to run) is chosen to pin the specific audited paths that
//! COULD produce a ghost if a future refactor broke them — not to
//! reproduce the reporter's GB-scale load.
//!
//! Audited paths pinned here:
//!
//! 1. **cache-populated-before-delete**: a reader populates the row
//!    cache via an L0 hit, then a delete arrives. The delete's
//!    `cache.invalidate(uk)` MUST remove the stale Put entry or a
//!    subsequent `get` returns the pre-delete row.
//!
//! 2. **flush-then-delete-then-read**: Put + flush places the row
//!    in L0. A post-flush delete lands in the memtable. `get()`
//!    must see the memtable tombstone first.
//!
//! 3. **flush-delete-flush-read**: Put + flush (L0 file F1), Delete
//!    + flush (L0 file F2 with tombstone). `get()` must traverse
//!      L0 seq_max DESC and find F2's tombstone before F1's Put.
//!
//! 4. **flush-delete-compact-read**: after (3), compaction merges
//!    F1+F2 into L1. The tombstone must survive the L0→L1
//!    compaction (num_levels>=1 means drop_tombstones=false at
//!    L1). Reader hits L1 and sees the tombstone.
//!
//! 5. **delete-without-flush-interleave**: alternating Put/Delete
//!    under mixed flush cadence produces no stale state at ANY
//!    checkpoint, including on the boundary where the active
//!    memtable just rotated but hasn't been dropped yet.

use std::sync::Arc;

use bytes::Bytes;
use merutable::engine::{EngineConfig, MeruEngine};
use merutable::types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};
use tempfile::TempDir;

fn schema() -> TableSchema {
    TableSchema {
        table_name: "issue35".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
                ..Default::default()
            },
            ColumnDef {
                name: "payload".into(),
                col_type: ColumnType::ByteArray,
                nullable: true,
                ..Default::default()
            },
        ],
        primary_key: vec![0],
        ..Default::default()
    }
}

fn turbo_config(tmp: &TempDir) -> EngineConfig {
    EngineConfig {
        schema: schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        l0_compaction_trigger: 2,
        l0_slowdown_trigger: 8,
        l0_stop_trigger: 12,
        // Match #35's reported turbo profile.
        memtable_size_bytes: 16 * 1024 * 1024,
        row_cache_capacity: 1_000,
        ..Default::default()
    }
}

fn row(id: i64, payload: &[u8]) -> Row {
    Row::new(vec![
        Some(FieldValue::Int64(id)),
        Some(FieldValue::Bytes(Bytes::copy_from_slice(payload))),
    ])
}

fn pk(id: i64) -> Vec<FieldValue> {
    vec![FieldValue::Int64(id)]
}

/// Path 1: cache-populated-before-delete. A `get()` that populates
/// the row cache via an L0 hit MUST be invalidated by a subsequent
/// delete. Without the invalidate (or if generation-tagged
/// `insert_if_fresh` lost its guard), the stale Put would serve
/// from cache forever after the delete's memtable entry is flushed.
#[tokio::test]
async fn cache_populated_get_does_not_survive_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let engine: Arc<MeruEngine> = MeruEngine::open(turbo_config(&tmp)).await.unwrap();

    // Put + flush so the row lives in L0, not memtable.
    engine.put(pk(1), row(1, b"v1")).await.unwrap();
    engine.flush().await.unwrap();

    // Populate the row cache via a get() that traverses L0.
    let hit = engine.get(&pk(1)).unwrap();
    assert!(hit.is_some(), "get should hit L0 after flush");

    // Delete. write_path invalidates cache after the WAL lock is
    // released.
    engine.delete(pk(1)).await.unwrap();

    // Memtable check comes FIRST in point_lookup, so the tombstone
    // shadows any stale cache entry that's still lingering.
    assert!(engine.get(&pk(1)).unwrap().is_none(), "immediate get");

    // Flush the delete so memtable is empty of this key. Now the
    // read relies on cache-being-invalidated OR L0-tombstone-being-
    // found. Either way the get must return None.
    engine.flush().await.unwrap();

    assert!(
        engine.get(&pk(1)).unwrap().is_none(),
        "delete must survive flush — cache invalidation or L0 tombstone"
    );
}

/// Path 2: flush-then-delete. Put lives in L0, delete in memtable.
/// Memtable tombstone wins.
#[tokio::test]
async fn delete_in_memtable_shadows_put_in_l0() {
    let tmp = tempfile::tempdir().unwrap();
    let engine: Arc<MeruEngine> = MeruEngine::open(turbo_config(&tmp)).await.unwrap();

    engine.put(pk(2), row(2, b"v2")).await.unwrap();
    engine.flush().await.unwrap();

    engine.delete(pk(2)).await.unwrap();
    // No flush — tombstone stays in active memtable.

    assert!(engine.get(&pk(2)).unwrap().is_none());
}

/// Path 3: Put+flush, Delete+flush. Two L0 files (F1=Put, F2=Del).
/// Read path iterates L0 seq_max DESC: F2 first, hits tombstone,
/// returns None. The iteration order is the invariant that Bug J+K
/// exposed (#15063f3) when seq_max wasn't tracked per-file.
#[tokio::test]
async fn delete_in_l0_shadows_put_in_l0() {
    let tmp = tempfile::tempdir().unwrap();
    let engine: Arc<MeruEngine> = MeruEngine::open(turbo_config(&tmp)).await.unwrap();

    engine.put(pk(3), row(3, b"v3")).await.unwrap();
    engine.flush().await.unwrap();
    engine.delete(pk(3)).await.unwrap();
    engine.flush().await.unwrap();

    assert!(
        engine.get(&pk(3)).unwrap().is_none(),
        "L0 seq_max DESC traversal must hit the tombstone before the put"
    );
}

/// Path 4: after (3), compact L0→L1. Tombstone must survive —
/// `should_drop_tombstones` is false at L1 with 4 configured levels
/// (Bug I, commit 39eb777). If that invariant ever regresses, this
/// test fires.
#[tokio::test]
async fn delete_survives_l0_to_l1_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let engine: Arc<MeruEngine> = MeruEngine::open(turbo_config(&tmp)).await.unwrap();

    engine.put(pk(4), row(4, b"v4")).await.unwrap();
    engine.flush().await.unwrap();
    engine.delete(pk(4)).await.unwrap();
    engine.flush().await.unwrap();

    // Force compaction. With l0_compaction_trigger=2 the job will
    // merge both L0 files to L1. Tombstone kept (non-bottom level).
    engine.compact().await.unwrap();

    assert!(
        engine.get(&pk(4)).unwrap().is_none(),
        "tombstone MUST survive L0→L1 compaction when not at bottom level"
    );
}

/// Path 5: mixed workload — alternating Put/Delete on a pool of
/// keys with periodic flushes, checking each operation's visibility
/// immediately after the op. Drives the same state transitions as
/// the reporter's GB-scale harness.
///
/// `#[ignore]` because the 1000-op + per-50-flush + final-compact
/// budget runs for several minutes on a dev laptop and is too slow
/// for default CI. Matches the pattern set by `ghost_rows_turbo`.
/// Run explicitly:
///     cargo test -p merutable-engine --test issue35_delete_visibility \
///         alternating_put_delete_has_no_ghosts -- --ignored
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn alternating_put_delete_has_no_ghosts() {
    let tmp = tempfile::tempdir().unwrap();
    let engine: Arc<MeruEngine> = MeruEngine::open(turbo_config(&tmp)).await.unwrap();

    const POOL: i64 = 100;
    const OPS: i64 = 1_000;

    let mut expected_present: std::collections::HashMap<i64, bool> =
        std::collections::HashMap::new();

    for i in 0..OPS {
        let key = i % POOL;
        if i % 7 == 0 {
            engine.delete(pk(key)).await.unwrap();
            expected_present.insert(key, false);
        } else {
            engine
                .put(pk(key), row(key, format!("v{i}").as_bytes()))
                .await
                .unwrap();
            expected_present.insert(key, true);
        }

        // Periodic flush cadence: every 50 ops. Keeps L0 growing
        // with interleaved put/delete files, which is the
        // configuration that stresses Bug J+K's tombstone-ordering
        // invariant.
        if i > 0 && i % 50 == 0 {
            engine.flush().await.unwrap();
        }

        // Every 10 ops, sample 5 keys and assert visibility. The
        // assertion runs serialized AFTER the write, so shadow-lag
        // is not possible.
        if i > 0 && i % 10 == 0 {
            for (k, &present) in expected_present.iter().take(5) {
                let got = engine.get(&pk(*k)).unwrap();
                assert_eq!(
                    got.is_some(),
                    present,
                    "op={i} key={k} expected present={present} got={got:?}"
                );
            }
        }
    }

    // Final flush + compact, then full sweep.
    engine.flush().await.unwrap();
    engine.compact().await.unwrap();

    for (k, &present) in &expected_present {
        let got = engine.get(&pk(*k)).unwrap();
        assert_eq!(
            got.is_some(),
            present,
            "post-compact key={k} expected present={present} got={got:?}"
        );
    }
}

/// Issue #35 narrow pin: row cache invalidation order. The write
/// path releases the WAL lock BEFORE calling cache.invalidate, so
/// a reader that captures visible_seq between the two events
/// sees the new tombstone in memtable AND may have a stale cache.
/// Memtable-first ordering in point_lookup must mean the tombstone
/// wins. This test stresses exactly that window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memtable_tombstone_beats_stale_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let engine: Arc<MeruEngine> = MeruEngine::open(turbo_config(&tmp)).await.unwrap();

    // Populate cache: put + flush + get (cache populates from L0).
    engine.put(pk(7), row(7, b"original")).await.unwrap();
    engine.flush().await.unwrap();
    let _ = engine.get(&pk(7)).unwrap();

    // Delete and immediately read IN THE SAME TASK. Serialized after
    // the delete's completion, so visible_seq reflects the delete,
    // and the cache has been invalidated BEFORE we return from
    // delete.await.
    engine.delete(pk(7)).await.unwrap();
    let got = engine.get(&pk(7)).unwrap();
    assert!(
        got.is_none(),
        "delete must be visible immediately; got {got:?}"
    );

    // Hammer: 100 put/delete pairs on the same key, each followed
    // by a serialized get. At every step the expected state is
    // determined by the most recent op on this key. A cache bug
    // would fire within a few iterations.
    for i in 0..100i64 {
        engine.put(pk(7), row(7, b"v")).await.unwrap();
        assert!(engine.get(&pk(7)).unwrap().is_some(), "iter {i} after put");
        engine.delete(pk(7)).await.unwrap();
        assert!(engine.get(&pk(7)).unwrap().is_none(), "iter {i} after del");
    }
}
