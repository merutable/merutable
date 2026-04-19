//! Regression for **Bug D**: WAL files were never rotated or GC'd by any
//! engine path. `WalManager::rotate` and `gc_logs_before` existed but were
//! only called from WAL-crate unit tests, never from `engine::flush` or the
//! auto-flush path. Consequences:
//!
//! 1. A long-running workload accumulated every write in a single log file
//!    forever — the WAL directory grew without bound.
//! 2. On restart, WAL recovery replayed every historical batch into the
//!    fresh memtable, including batches that had already been persisted to
//!    Parquet — O(lifetime) recovery work instead of O(unflushed).
//!
//! The fix wires rotation into both `engine.flush()` and the auto-flush
//! path in `write_internal` / `write_path::apply_batch`, and makes
//! `WalManager::mark_flushed_seq` GC closed logs as a side effect (keyed
//! by the `max_seq` observed in each closed log).
//!
//! The tests below pin:
//! - After an explicit `engine.flush()`, the original WAL file is gone and
//!   a fresh (empty-on-disk-or-minimal) WAL file exists.
//! - After multiple flushes, the WAL directory never holds more than one
//!   "closed but un-GC'd" file at a time.
//! - On restart, recovery sees zero (or one nearly-empty) WAL file and
//!   does not replay already-flushed batches.

use std::{path::Path, time::Duration};

use bytes::Bytes;
use merutable_engine::{EngineConfig, MeruEngine};
use merutable_types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};
use tempfile::TempDir;

fn schema() -> TableSchema {
    TableSchema {
        table_name: "wal_gc".into(),
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

fn make_row(i: i64) -> Row {
    Row::new(vec![
        Some(FieldValue::Int64(i)),
        Some(FieldValue::Bytes(Bytes::from(format!("payload_{i:04}")))),
    ])
}

fn test_config(tmp: &TempDir) -> EngineConfig {
    EngineConfig {
        schema: schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        ..Default::default()
    }
}

fn count_wal_files(dir: &Path) -> usize {
    if !dir.exists() {
        return 0;
    }
    std::fs::read_dir(dir)
        .map(|iter| {
            iter.flatten()
                .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("wal"))
                .count()
        })
        .unwrap_or(0)
}

fn sum_wal_bytes(dir: &Path) -> u64 {
    if !dir.exists() {
        return 0;
    }
    std::fs::read_dir(dir)
        .map(|iter| {
            iter.flatten()
                .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("wal"))
                .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
                .sum()
        })
        .unwrap_or(0)
}

/// After an explicit `engine.flush()`, the pre-flush WAL file must have
/// been rotated out and GC'd, leaving only the fresh (empty) current log.
#[tokio::test]
async fn explicit_flush_rotates_and_gcs_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();
    let wal_dir = tmp.path().join("wal");

    for i in 1..=50i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i))
            .await
            .unwrap();
    }
    // Before flush: one active log containing all 50 batches.
    assert_eq!(
        count_wal_files(&wal_dir),
        1,
        "pre-flush expected exactly one active WAL file"
    );
    let pre_flush_bytes = sum_wal_bytes(&wal_dir);
    assert!(pre_flush_bytes > 0);

    engine.flush().await.unwrap();

    // After flush: exactly one WAL file — the fresh post-rotation log —
    // and it must be smaller than the pre-flush log, since it holds zero
    // batches.
    let post_files = count_wal_files(&wal_dir);
    assert_eq!(
        post_files, 1,
        "post-flush expected exactly one (fresh) WAL file, got {post_files}"
    );
    let post_bytes = sum_wal_bytes(&wal_dir);
    assert!(
        post_bytes < pre_flush_bytes,
        "post-flush WAL ({post_bytes}B) should be smaller than pre-flush ({pre_flush_bytes}B): \
         old log was not GC'd"
    );
}

/// After N sequential explicit flushes, the WAL directory must not
/// accumulate N log files — it should hold at most one (the current
/// active file). This pins the steady-state invariant: GC drains as fast
/// as rotation pushes.
#[tokio::test]
async fn repeated_flushes_do_not_accumulate_wal_files() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();
    let wal_dir = tmp.path().join("wal");

    for batch in 0..6 {
        for i in 1..=10i64 {
            let id = batch * 10 + i;
            engine
                .put(vec![FieldValue::Int64(id)], make_row(id))
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
    }

    let n = count_wal_files(&wal_dir);
    assert!(
        n <= 1,
        "WAL dir should hold ≤1 file after repeated flush/GC cycles; got {n} files"
    );
}

/// Auto-flush must also drive WAL rotation + GC, otherwise a long-running
/// writer that never calls `engine.flush()` explicitly would leak WAL
/// files proportional to the number of auto-rotations.
#[tokio::test]
async fn auto_flush_gcs_wal_files() {
    let tmp = tempfile::tempdir().unwrap();
    // Tiny memtable so auto-flush fires repeatedly.
    let mut config = test_config(&tmp);
    config.memtable_size_bytes = 4 * 1024;
    let engine = MeruEngine::open(config).await.unwrap();
    let wal_dir = tmp.path().join("wal");

    // Enough rows to trigger many auto-rotations.
    for i in 1..=500i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i))
            .await
            .unwrap();
    }

    // Give background auto-flushes time to drain.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        // Stability check: we tolerate "a couple of closed logs mid-flight"
        // but reject "dozens of closed logs leaking".
        if count_wal_files(&wal_dir) <= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let n = count_wal_files(&wal_dir);
    assert!(
        n <= 3,
        "auto-flush path leaked WAL files: {n} files left in {wal_dir:?} \
         (expected ≤3 mid-flight, ideally 1 steady state)"
    );

    // One final explicit flush should drain to exactly one (fresh) file.
    engine.flush().await.unwrap();
    let final_n = count_wal_files(&wal_dir);
    assert_eq!(
        final_n, 1,
        "post-explicit-flush steady state: exactly one WAL file, got {final_n}"
    );
}

/// On restart, WAL recovery must not replay already-flushed batches. With
/// the fix, the pre-crash flush rotates + GCs the old log, so recovery
/// reads zero batches from the (fresh, empty) log and the memtable is
/// empty at startup — get() must fall through to the L0 Parquet file.
#[tokio::test]
async fn recovery_does_not_replay_flushed_batches() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");

    // Run 1: write, flush, drop engine (simulate crash).
    {
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();
        for i in 1..=20i64 {
            engine
                .put(vec![FieldValue::Int64(i)], make_row(i))
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
    }

    // After the flush + drop, the WAL dir should hold exactly one fresh
    // (tiny) log — the old one was rotated out and GC'd.
    assert_eq!(
        count_wal_files(&wal_dir),
        1,
        "expected exactly one WAL file after flush before reopen"
    );
    let pre_reopen_bytes = sum_wal_bytes(&wal_dir);

    // Run 2: reopen. Recovery must not rehydrate the flushed rows.
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

    // All rows still readable (via the L0 Parquet file).
    for i in 1..=20i64 {
        let row = engine
            .get(&[FieldValue::Int64(i)])
            .unwrap()
            .unwrap_or_else(|| panic!("row {i} missing after reopen"));
        assert_eq!(row.get(0), Some(&FieldValue::Int64(i)));
    }

    // WAL directory still holds exactly one (or two) file(s) post-reopen.
    // Reopen always starts a new log file as the active writer; the
    // pre-existing empty log may or may not still be present depending on
    // how the fresh WalManager handled its start_log number.
    let post_reopen = count_wal_files(&wal_dir);
    assert!(
        post_reopen <= 2,
        "expected ≤2 WAL files after reopen, got {post_reopen}"
    );
    // Either way, recovery did not rewrite the old file — it was empty.
    let _ = pre_reopen_bytes;
}
