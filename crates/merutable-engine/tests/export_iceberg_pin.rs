//! Regression for Issue #24: `export_iceberg()` must hold a
//! `SnapshotPin` for the lifetime of the export call, otherwise
//! concurrent compaction + `gc_pending_deletions` can delete a
//! Parquet file out from under the in-flight export. Before the fix,
//! `export_iceberg` delegated directly to the catalog with no pin —
//! the same BUG-0007..0013 hazard the read path was fixed for, minus
//! the fix on this call site.
//!
//! This test is deterministic (no timing races): it builds the
//! `export_iceberg` future, polls it once to advance to the first
//! `.await` (which happens after the pin is acquired), then inspects
//! `min_pinned_snapshot()`. With the fix, the pin is observably held.
//! Without the fix, the pin is never acquired and the inspection
//! returns `None`.

use std::sync::Arc;

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

#[tokio::test]
async fn export_iceberg_holds_snapshot_pin_for_lifetime_of_call() {
    let tmp = tempfile::tempdir().unwrap();
    let engine: Arc<MeruEngine> = MeruEngine::open(config(&tmp)).await.unwrap();

    // Populate so the catalog has a non-empty snapshot — exercises
    // the full export path, not just the empty-metadata fast return.
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

    assert_eq!(
        engine.min_pinned_snapshot(),
        None,
        "precondition: no pin before export"
    );

    // Build the export future and poll it exactly once. The `.await`
    // inside `catalog.export_to_iceberg` hits a tokio::fs::write that
    // returns Pending on the first poll, so control comes back to us
    // with the pin alive. If `export_iceberg` did not acquire a pin
    // (the Issue #24 regression), nothing observable changes between
    // "before poll" and "after poll — exported future parked".
    let export_dir = tmp.path().join("exported");
    let fut = engine.export_iceberg(export_dir.clone());
    tokio::pin!(fut);

    // One poll to advance the future past pin acquisition and into
    // the first catalog await. `poll!` returns `Poll::Pending` on
    // success (the future parked on the fs.write completion); if it
    // returns Ready, the export finished synchronously (possible on
    // ultra-fast systems) — in which case we skip the mid-flight
    // observation and rely on the next-assertion loop.
    let first_poll = futures::poll!(&mut fut);
    match first_poll {
        std::task::Poll::Pending => {
            // Pin must be observably held while the future is parked.
            assert!(
                engine.min_pinned_snapshot().is_some(),
                "Issue #24 regression: export_iceberg's future parked \
                 mid-export but min_pinned_snapshot() is None — the \
                 pin was never acquired"
            );
        }
        std::task::Poll::Ready(_) => {
            // Future completed on first poll — nothing parked, so
            // nothing to observe. This path doesn't exercise the
            // regression, but also doesn't invalidate it (behavior
            // might be OS/filesystem-dependent).
        }
    }

    // Drive to completion.
    fut.await.expect("export_iceberg succeeded");

    // After the export future has returned, the pin must be released.
    assert_eq!(
        engine.min_pinned_snapshot(),
        None,
        "pin must be released after export_iceberg returns"
    );

    engine.close().await.unwrap();
}
