//! Regression tests for the IMP (Improvement Report) fixes.
//!
//! Each test targets a specific IMP-NN fix and verifies the correct behavior
//! that was previously missing or broken.

use merutable_engine::{EngineConfig, MeruEngine};
use merutable_types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};

fn test_schema() -> TableSchema {
    TableSchema {
        table_name: "imp_test".into(),
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

fn test_config(tmp: &tempfile::TempDir) -> EngineConfig {
    EngineConfig {
        schema: test_schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        // Small memtable for fast flush in tests.
        memtable_size_bytes: 512,
        // No GC grace period in tests — delete immediately.
        gc_grace_period_secs: 0,
        ..Default::default()
    }
}

/// IMP-03 regression: row cache must be cleared after compaction.
/// Without the fix, a cached entry from an obsoleted file would be
/// served even after compaction resolves the MVCC version.
#[tokio::test]
async fn imp03_row_cache_cleared_after_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let config = EngineConfig {
        row_cache_capacity: 1000,
        ..test_config(&tmp)
    };
    let engine = MeruEngine::open(config).await.unwrap();

    // Write v1, flush to L0.
    engine
        .put(
            vec![FieldValue::Int64(1)],
            Row::new(vec![
                Some(FieldValue::Int64(1)),
                Some(FieldValue::Bytes(bytes::Bytes::from("v1"))),
            ]),
        )
        .await
        .unwrap();
    engine.flush().await.unwrap();

    // Read key 1 — populates the row cache.
    let row = engine.get(&[FieldValue::Int64(1)]).unwrap().unwrap();
    assert_eq!(
        row.get(1).cloned(),
        Some(FieldValue::Bytes(bytes::Bytes::from("v1")))
    );

    // Overwrite with v2, flush again.
    engine
        .put(
            vec![FieldValue::Int64(1)],
            Row::new(vec![
                Some(FieldValue::Int64(1)),
                Some(FieldValue::Bytes(bytes::Bytes::from("v2"))),
            ]),
        )
        .await
        .unwrap();
    engine.flush().await.unwrap();

    // Compact — merges L0 files, keeps only v2 (latest).
    engine.compact().await.unwrap();

    // Read again — must see v2, not stale cached v1.
    let row = engine.get(&[FieldValue::Int64(1)]).unwrap().unwrap();
    assert_eq!(
        row.get(1).cloned(),
        Some(FieldValue::Bytes(bytes::Bytes::from("v2"))),
        "IMP-03: cache must be cleared after compaction — stale v1 was served"
    );
}

/// IMP-02 regression: `read_seq()` must return the visible sequence, not
/// the allocated sequence. After a put returns, the data must be visible.
#[tokio::test]
async fn imp02_visible_seq_matches_memtable() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

    let seq_before = engine.read_seq().0;

    engine
        .put(
            vec![FieldValue::Int64(42)],
            Row::new(vec![
                Some(FieldValue::Int64(42)),
                Some(FieldValue::Bytes(bytes::Bytes::from("hello"))),
            ]),
        )
        .await
        .unwrap();

    let seq_after = engine.read_seq().0;
    assert_eq!(
        seq_after,
        seq_before + 1,
        "IMP-02: visible_seq must advance by 1 after a single put"
    );

    // The data must be readable at the new visible_seq.
    let row = engine.get(&[FieldValue::Int64(42)]).unwrap();
    assert!(
        row.is_some(),
        "IMP-02: data must be visible immediately after put returns"
    );
}

/// IMP-02 regression: batch writes must advance visible_seq by the batch size.
#[tokio::test]
async fn imp02_batch_advances_visible_seq() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

    let seq_before = engine.read_seq().0;

    let mut batch = merutable_engine::write_path::MutationBatch::new();
    for i in 1..=5i64 {
        batch.put(
            vec![FieldValue::Int64(i)],
            Row::new(vec![
                Some(FieldValue::Int64(i)),
                Some(FieldValue::Bytes(bytes::Bytes::from(format!("v{i}")))),
            ]),
        );
    }
    merutable_engine::write_path::apply_batch(&engine, batch)
        .await
        .unwrap();

    let seq_after = engine.read_seq().0;
    assert_eq!(
        seq_after,
        seq_before + 5,
        "IMP-02: visible_seq must advance by batch size (5)"
    );

    // All 5 keys must be readable.
    for i in 1..=5i64 {
        let row = engine.get(&[FieldValue::Int64(i)]).unwrap();
        assert!(row.is_some(), "key {i} must be readable after batch");
    }
}

/// IMP-15 regression: read-only replica refresh must validate that all
/// referenced files exist before swapping the version.
#[tokio::test]
async fn imp15_refresh_rejects_missing_files() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = test_config(&tmp);
    config.memtable_size_bytes = 64 * 1024 * 1024; // don't auto-flush

    // Primary: write and flush.
    let primary = MeruEngine::open(config.clone()).await.unwrap();
    primary
        .put(
            vec![FieldValue::Int64(1)],
            Row::new(vec![Some(FieldValue::Int64(1)), None]),
        )
        .await
        .unwrap();
    primary.flush().await.unwrap();
    drop(primary);

    // Open read-only replica.
    config.read_only = true;
    let replica = MeruEngine::open(config).await.unwrap();

    // Delete the data file that the manifest references.
    let l0_dir = tmp.path().join("data").join("L0");
    if l0_dir.exists() {
        for entry in std::fs::read_dir(&l0_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.path().extension().is_some_and(|e| e == "parquet") {
                std::fs::remove_file(entry.path()).unwrap();
            }
        }
    }

    // Refresh should fail because the referenced file is missing.
    let result = replica.refresh().await;
    assert!(
        result.is_err(),
        "IMP-15: refresh must reject when referenced data files are missing"
    );
}

/// Compaction loop regression: a single `compact()` call must drain the
/// tree until no level is above its trigger. Previously `compact()`
/// executed exactly one job and returned, so a caller (or the background
/// worker) had to keep calling it. With concurrent flushes, L0 could
/// grow unboundedly while a single deep compaction was in flight.
///
/// This test forces multiple L0 files to accumulate, drives the tree
/// past the L0 compaction trigger, and verifies that one `compact()`
/// call reduces L0 below the trigger.
#[tokio::test]
async fn compact_loops_until_tree_healthy() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = test_config(&tmp);
    // Small memtable so each put flushes quickly — but we'll force flushes
    // manually to control the L0 file count precisely.
    config.memtable_size_bytes = 64 * 1024 * 1024; // don't auto-flush
    config.gc_grace_period_secs = 0;
    let engine = MeruEngine::open(config).await.unwrap();

    // Create 6 L0 files (above the default l0_compaction_trigger=4).
    // Each flush produces one L0 file.
    for batch in 0..6i64 {
        for i in 0..10i64 {
            let key = batch * 100 + i;
            engine
                .put(
                    vec![FieldValue::Int64(key)],
                    Row::new(vec![
                        Some(FieldValue::Int64(key)),
                        Some(FieldValue::Bytes(bytes::Bytes::from(format!("v{key}")))),
                    ]),
                )
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
    }

    // Confirm we have at least the trigger count of L0 files.
    let stats_before = engine.stats();
    let l0_before = stats_before
        .levels
        .iter()
        .find(|l| l.level == 0)
        .map(|l| l.file_count)
        .unwrap_or(0);
    assert!(
        l0_before >= 4,
        "setup invariant: need ≥4 L0 files, got {l0_before}"
    );

    // One compact() call must drain L0 below the trigger.
    engine.compact().await.unwrap();

    let stats_after = engine.stats();
    let l0_after = stats_after
        .levels
        .iter()
        .find(|l| l.level == 0)
        .map(|l| l.file_count)
        .unwrap_or(0);
    assert!(
        l0_after < 4,
        "compact() should have drained L0 below trigger, got {l0_after} files"
    );
}

/// Graceful shutdown: close() must flush memtable data and reject
/// subsequent writes while keeping reads available.
#[tokio::test]
async fn close_flushes_and_rejects_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = test_config(&tmp);
    config.memtable_size_bytes = 64 * 1024 * 1024; // don't auto-flush
    let engine = MeruEngine::open(config).await.unwrap();

    // Write data into the memtable (no flush).
    for i in 0..20i64 {
        engine
            .put(
                vec![FieldValue::Int64(i)],
                Row::new(vec![
                    Some(FieldValue::Int64(i)),
                    Some(FieldValue::Bytes(bytes::Bytes::from(format!("v{i}")))),
                ]),
            )
            .await
            .unwrap();
    }

    // Close: flushes memtable to Parquet.
    engine.close().await.unwrap();

    // Reads still work.
    for i in 0..20i64 {
        let row = engine.get(&[FieldValue::Int64(i)]).unwrap();
        assert!(row.is_some(), "key {i} must be readable after close");
    }

    // Writes fail.
    let err = engine
        .put(
            vec![FieldValue::Int64(99)],
            Row::new(vec![Some(FieldValue::Int64(99)), None]),
        )
        .await;
    assert!(err.is_err(), "put must fail after close");
}

/// Graceful shutdown: data written before close() survives reopen
/// without relying on WAL recovery.
#[tokio::test]
async fn close_data_durable_across_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(&tmp);

    {
        let mut cfg = config.clone();
        cfg.memtable_size_bytes = 64 * 1024 * 1024; // don't auto-flush
        let engine = MeruEngine::open(cfg).await.unwrap();
        for i in 0..30i64 {
            engine
                .put(
                    vec![FieldValue::Int64(i)],
                    Row::new(vec![
                        Some(FieldValue::Int64(i)),
                        Some(FieldValue::Bytes(bytes::Bytes::from(format!("data_{i}")))),
                    ]),
                )
                .await
                .unwrap();
        }
        engine.close().await.unwrap();
    }

    // Reopen — data should come from Parquet, not WAL.
    let engine = MeruEngine::open(config).await.unwrap();
    for i in 0..30i64 {
        let row = engine.get(&[FieldValue::Int64(i)]).unwrap();
        assert!(row.is_some(), "key {i} must survive close + reopen");
    }
}

/// IMP-12 regression: compaction-obsoleted files should be tracked for
/// deferred deletion, not immediately removed.
#[tokio::test]
async fn imp12_gc_grace_period() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = test_config(&tmp);
    // Set grace period to 10 seconds — files should NOT be deleted immediately.
    config.gc_grace_period_secs = 10;
    config.memtable_size_bytes = 512;

    let engine = MeruEngine::open(config).await.unwrap();

    // Write enough data for two flushes.
    for i in 0..50i64 {
        engine
            .put(
                vec![FieldValue::Int64(i)],
                Row::new(vec![
                    Some(FieldValue::Int64(i)),
                    Some(FieldValue::Bytes(bytes::Bytes::from(format!("data_{i}")))),
                ]),
            )
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    for i in 50..100i64 {
        engine
            .put(
                vec![FieldValue::Int64(i)],
                Row::new(vec![
                    Some(FieldValue::Int64(i)),
                    Some(FieldValue::Bytes(bytes::Bytes::from(format!("data_{i}")))),
                ]),
            )
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    // Count L0 files before compaction.
    let l0_dir = tmp.path().join("data").join("L0");
    let files_before: Vec<_> = std::fs::read_dir(&l0_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    assert!(
        files_before.len() >= 2,
        "should have at least 2 L0 files before compaction"
    );

    // Compact.
    engine.compact().await.unwrap();

    // With a 10-second grace period, the old files should still exist.
    let files_after: Vec<_> = std::fs::read_dir(&l0_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    // The old files should NOT have been deleted yet (grace period hasn't elapsed).
    assert!(
        files_after.len() >= files_before.len(),
        "IMP-12: files should be preserved during grace period, \
         before={} after={}",
        files_before.len(),
        files_after.len()
    );
}
