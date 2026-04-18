//! Regression for Issue #24: `export_iceberg()` must hold a
//! `SnapshotPin` for the lifetime of the export call, otherwise
//! concurrent compaction + `gc_pending_deletions` can delete a
//! Parquet file out from under the in-flight export. Before the fix,
//! `export_iceberg` delegated directly to the catalog with no pin —
//! the same BUG-0007..0013 hazard the read path was fixed for, minus
//! the fix on this call site.
//!
//! This test verifies the contract directly: while `export_iceberg`
//! is running, `min_pinned_snapshot()` must be `Some(_)`, proving
//! the pin is held. The prior behaviour had `min_pinned_snapshot()`
//! return `None` throughout, making the export GC-invisible.

use std::{sync::Arc, time::Duration};

use merutable_engine::{EngineConfig, MeruEngine};
use merutable_types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};
use tempfile::TempDir;

fn schema() -> TableSchema {
    TableSchema {
        table_name: "exp".into(),
        columns: vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Int64,
            nullable: false,
        }],
        primary_key: vec![0],
    }
}

fn config(tmp: &TempDir) -> EngineConfig {
    EngineConfig {
        schema: schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        flush_parallelism: 0,
        compaction_parallelism: 0,
        ..Default::default()
    }
}

/// Spawn `export_iceberg` as a separate task and poll
/// `min_pinned_snapshot()` concurrently. The pin must be observed
/// as held at least once during the export window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_iceberg_holds_snapshot_pin_for_lifetime_of_call() {
    let tmp = tempfile::tempdir().unwrap();
    let engine: Arc<MeruEngine> = MeruEngine::open(config(&tmp)).await.unwrap();

    // Put something so the manifest has a non-empty snapshot to
    // export. Not strictly required — empty manifests export fine —
    // but a realistic setup exercises more of the catalog write path.
    for i in 0..10i64 {
        engine
            .put(
                vec![FieldValue::Int64(i)],
                Row::new(vec![Some(FieldValue::Int64(i))]),
            )
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    // Baseline: nothing should be pinned before the export.
    assert_eq!(
        engine.min_pinned_snapshot(),
        None,
        "precondition: no pin before export"
    );

    let export_engine = engine.clone();
    let export_dir = tmp.path().join("exported");
    let export_task = tokio::spawn(async move { export_engine.export_iceberg(export_dir).await });

    // Poll min_pinned_snapshot for up to 1s; we expect to observe the
    // pin at least once while the export is in flight. With Issue #24
    // unfixed, `min_pinned_snapshot()` would return `None` for the
    // entire duration.
    let mut saw_pin = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        if engine.min_pinned_snapshot().is_some() {
            saw_pin = true;
            break;
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let result = export_task.await.unwrap();
    result.expect("export_iceberg succeeded");

    assert!(
        saw_pin,
        "Issue #24 regression: export_iceberg() ran to completion \
         without ever incrementing live_snapshots — GC is free to \
         delete files out from under the in-flight export"
    );

    // Post-export the pin must be released.
    assert_eq!(
        engine.min_pinned_snapshot(),
        None,
        "pin must be released after export_iceberg returns"
    );

    engine.close().await.unwrap();
}
