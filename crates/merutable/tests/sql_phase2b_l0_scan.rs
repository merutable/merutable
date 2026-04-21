#![cfg(feature = "sql")]
//! Issue #29 Phase 2b: change-feed cursor reads memtable + L0.
//!
//! The Phase 2a cursor only saw un-flushed writes. Phase 2b picks
//! up ops that flushed out of the memtable into L0 Parquet files,
//! so subscribers can fall behind a flush tick without losing
//! visibility.

use std::sync::Arc;

use merutable::engine::{config::EngineConfig, engine::MeruEngine};
use merutable::sql::ChangeFeedCursor;
use merutable::types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};

fn test_schema() -> TableSchema {
    TableSchema {
        table_name: "cf-phase2b".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
                ..Default::default()
            },
            ColumnDef {
                name: "v".into(),
                col_type: ColumnType::Int64,
                nullable: true,
                ..Default::default()
            },
        ],
        primary_key: vec![0],
        ..Default::default()
    }
}

async fn open_engine(tmp: &tempfile::TempDir) -> Arc<MeruEngine> {
    let cfg = EngineConfig {
        schema: test_schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        ..Default::default()
    };
    MeruEngine::open(cfg).await.unwrap()
}

fn row(id: i64, v: i64) -> Row {
    Row::new(vec![
        Some(FieldValue::Int64(id)),
        Some(FieldValue::Int64(v)),
    ])
}

#[tokio::test]
async fn flushed_ops_remain_visible_via_l0_scan() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;

    // Write three ops, flush so they land in L0.
    for i in 1..=3i64 {
        engine
            .put(vec![FieldValue::Int64(i)], row(i, i * 10))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    // Memtable is now empty (the three ops went to L0). Phase 2a
    // would return 0 records; Phase 2b must return 3.
    let mut cur = ChangeFeedCursor::from_engine(engine.clone(), 0);
    let batch = cur.next_batch(100).unwrap();
    assert_eq!(
        batch.len(),
        3,
        "ops flushed to L0 must still appear in the change feed"
    );
    // Ascending seq.
    for w in batch.windows(2) {
        assert!(w[0].seq < w[1].seq);
    }
}

#[tokio::test]
async fn mixed_memtable_and_l0_interleave_in_seq_order() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;

    // Flight 1: two writes, flush. These land in L0.
    engine
        .put(vec![FieldValue::Int64(1)], row(1, 1))
        .await
        .unwrap();
    engine
        .put(vec![FieldValue::Int64(2)], row(2, 2))
        .await
        .unwrap();
    engine.flush().await.unwrap();

    // Flight 2: two more writes, no flush. These live in the
    // current memtable.
    engine
        .put(vec![FieldValue::Int64(3)], row(3, 3))
        .await
        .unwrap();
    engine
        .put(vec![FieldValue::Int64(4)], row(4, 4))
        .await
        .unwrap();

    let mut cur = ChangeFeedCursor::from_engine(engine.clone(), 0);
    let batch = cur.next_batch(100).unwrap();
    assert_eq!(batch.len(), 4, "L0 + memtable ops all appear");
    // Strictly ascending.
    for w in batch.windows(2) {
        assert!(w[0].seq < w[1].seq);
    }
}

#[tokio::test]
async fn since_seq_boundary_respected_across_l0_and_memtable() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;
    engine
        .put(vec![FieldValue::Int64(1)], row(1, 1))
        .await
        .unwrap();
    engine.flush().await.unwrap();
    let boundary_after_flush = engine.read_seq().0;
    engine
        .put(vec![FieldValue::Int64(2)], row(2, 2))
        .await
        .unwrap();

    // since = boundary after the first flush. Only the post-flush
    // op must surface; the L0 op must be filtered out by seq.
    let mut cur = ChangeFeedCursor::from_engine(engine, boundary_after_flush);
    let batch = cur.next_batch(100).unwrap();
    assert_eq!(batch.len(), 1);
    assert!(batch[0].seq > boundary_after_flush);
}
