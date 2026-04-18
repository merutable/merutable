//! Regression for Issue #22: orphaned WAL files from crash-recovery
//! paths must be registered with the new `WalManager` so the first
//! post-recovery `mark_flushed_seq()` GCs them.
//!
//! Without the fix, every reopen after a crash rediscovers and
//! re-replays the orphaned files. Under racing background compaction
//! this produces a data-loss window: memtable entries from the stale
//! WAL replay carry pre-compaction seqs, while the compacted L1
//! output carries newer seqs — the merge iterator drops the memtable
//! entries as "older versions of same key," effectively losing rows.
//!
//! This test reproduces the chaos-monkey Phase 57 scenario in a
//! deterministic, non-racing form:
//!
//! 1. Write 50 rows, flush, close (cycle 0).
//! 2. Write 50 more rows, DROP without close (crash — WAL survives).
//! 3. Reopen: recovery replays the 50 rows, flushes, closes.
//! 4. Reopen again: the recovered WAL file must already be GC'd,
//!    so recovery replays ZERO batches. Before the fix, recovery
//!    replayed the 50 rows *again*.

use std::sync::Arc;

use bytes::Bytes;
use merutable_engine::{EngineConfig, MeruEngine};
use merutable_types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};
use tempfile::TempDir;

fn schema() -> TableSchema {
    TableSchema {
        table_name: "wal_orphan".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
            },
            ColumnDef {
                name: "val".into(),
                col_type: ColumnType::ByteArray,
                nullable: true,
            },
        ],
        primary_key: vec![0],
    }
}

fn config(tmp: &TempDir) -> EngineConfig {
    EngineConfig {
        schema: schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        // Disable background workers so nothing races with our
        // deterministic flush calls.
        flush_parallelism: 0,
        compaction_parallelism: 0,
        ..Default::default()
    }
}

fn make_row(id: i64) -> Row {
    Row::new(vec![
        Some(FieldValue::Int64(id)),
        Some(FieldValue::Bytes(Bytes::from(format!("v{id}")))),
    ])
}

fn count_wal_files(wal_dir: &std::path::Path) -> usize {
    if !wal_dir.exists() {
        return 0;
    }
    std::fs::read_dir(wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphaned_wal_is_gcd_after_first_post_recovery_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");

    // Cycle 0: clean write + flush + close.
    {
        let engine: Arc<MeruEngine> = MeruEngine::open(config(&tmp)).await.unwrap();
        for i in 0..50i64 {
            engine
                .put(vec![FieldValue::Int64(i)], make_row(i))
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
        engine.close().await.unwrap();
    }
    // After a clean close, the engine's flush should have GC'd the
    // rotated WAL — one empty current WAL file should remain.
    let after_clean = count_wal_files(&wal_dir);
    assert!(
        after_clean <= 1,
        "clean close should leave at most one WAL file, got {after_clean}"
    );

    // Cycle 1: write + CRASH (no flush, no close).
    {
        let engine: Arc<MeruEngine> = MeruEngine::open(config(&tmp)).await.unwrap();
        for i in 50..100i64 {
            engine
                .put(vec![FieldValue::Int64(i)], make_row(i))
                .await
                .unwrap();
        }
        // Drop without close — simulates crash. WAL survives.
        drop(engine);
    }
    let after_crash = count_wal_files(&wal_dir);
    assert!(
        after_crash >= 1,
        "crash should leave the WAL on disk, got {after_crash}"
    );

    // Cycle 2: recover, flush, close. Issue #22 fix: the recovered
    // (orphaned) WAL file must be registered as a closed log so the
    // flush's mark_flushed_seq GCs it.
    let count_before_cycle2_close = {
        let engine: Arc<MeruEngine> = MeruEngine::open(config(&tmp)).await.unwrap();
        engine.flush().await.unwrap();
        // After flush but before close, the orphaned WAL from cycle 1
        // must be gone — mark_flushed_seq in the flush path is the
        // only GC trigger we rely on here.
        let count_after_flush = count_wal_files(&wal_dir);
        engine.close().await.unwrap();
        count_after_flush
    };
    // The orphan (log from cycle 1) + current (cycle 2's new log) was
    // 2 files going into cycle 2. After flush, we should have only
    // cycle 2's current log (which rotates on flush, leaving the new
    // active log). Strictly: at most 1 file — anything more means the
    // orphan survived.
    assert!(
        count_before_cycle2_close <= 1,
        "Issue #22 regression: orphan WAL survived the post-recovery \
         flush; expected <= 1 WAL file after flush, got {}",
        count_before_cycle2_close
    );

    // Cycle 3: reopen. Recovery must see ZERO batches because the
    // orphaned WAL from cycle 1 was GC'd in cycle 2.
    {
        let engine: Arc<MeruEngine> = MeruEngine::open(config(&tmp)).await.unwrap();
        // All 100 rows must be visible via scan — they were flushed to L0 in
        // cycle 0 and cycle 2. None should be lost.
        let rows = engine.scan(None, None).unwrap();
        assert_eq!(
            rows.len(),
            100,
            "expected 100 rows across both cycles; got {} \
             (regression: orphaned WAL replay interacted with compaction \
             to shadow live data)",
            rows.len()
        );
        engine.close().await.unwrap();
    }

    // Final invariant: after the recovery-driven GC, no stale WAL
    // file from the cycle-1 crash should survive. A steady-state clean
    // shutdown leaves the current log + possibly one rotate-on-close
    // file — so we bound at 2. Before the Issue #22 fix this count
    // grew monotonically with each crash/reopen cycle because every
    // orphaned log stayed on disk forever.
    let final_count = count_wal_files(&wal_dir);
    assert!(
        final_count <= 2,
        "orphaned WAL file survived past post-recovery flush; \
         expected <= 2 remaining, got {final_count}"
    );
}
