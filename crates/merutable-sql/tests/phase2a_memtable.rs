//! Issue #29 Phase 2a: change-feed cursor over the memtable.
//!
//! The cursor returns seq-ordered records for every op in
//! `(since_seq, read_seq]` that still lives in the un-flushed tail.
//! Post-flush ops require Phase 2b's L0 scan to surface.

use std::sync::Arc;

use merutable_engine::{config::EngineConfig, engine::MeruEngine};
use merutable_sql::{ChangeFeedCursor, ChangeOp};
use merutable_types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};

fn test_schema() -> TableSchema {
    TableSchema {
        table_name: "cf-test".into(),
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
async fn memtable_scan_returns_ops_above_since_in_seq_order() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;

    // Three puts, then a delete. Each put assigns a new seq.
    engine
        .put(vec![FieldValue::Int64(1)], row(1, 100))
        .await
        .unwrap();
    engine
        .put(vec![FieldValue::Int64(2)], row(2, 200))
        .await
        .unwrap();
    engine
        .put(vec![FieldValue::Int64(3)], row(3, 300))
        .await
        .unwrap();
    engine.delete(vec![FieldValue::Int64(2)]).await.unwrap();

    let mut cur = ChangeFeedCursor::from_engine(engine.clone(), 0);
    let batch = cur.next_batch(100).unwrap();
    // Four ops total.
    assert_eq!(batch.len(), 4);
    // Ascending seq.
    for w in batch.windows(2) {
        assert!(w[0].seq < w[1].seq);
    }
    // Ops: three inserts (Phase 2a tags every Put as Insert until
    // pre-image reconstruction lands) and one delete.
    let ops: Vec<ChangeOp> = batch.iter().map(|r| r.op).collect();
    assert_eq!(
        ops,
        vec![
            ChangeOp::Insert,
            ChangeOp::Insert,
            ChangeOp::Insert,
            ChangeOp::Delete
        ]
    );
    // Cursor now advances past the last returned seq.
    assert_eq!(cur.since_seq(), batch.last().unwrap().seq);
}

#[tokio::test]
async fn cursor_filters_ops_at_or_below_since_seq() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;
    engine
        .put(vec![FieldValue::Int64(1)], row(1, 1))
        .await
        .unwrap();
    let boundary = engine.read_seq().0;
    engine
        .put(vec![FieldValue::Int64(2)], row(2, 2))
        .await
        .unwrap();
    engine
        .put(vec![FieldValue::Int64(3)], row(3, 3))
        .await
        .unwrap();

    // Start at `boundary` — must skip the first put.
    let mut cur = ChangeFeedCursor::from_engine(engine.clone(), boundary);
    let batch = cur.next_batch(100).unwrap();
    assert_eq!(batch.len(), 2);
    assert!(batch.iter().all(|r| r.seq > boundary));
}

#[tokio::test]
async fn max_rows_caps_batch_and_cursor_advances_incrementally() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;
    for i in 1..=5i64 {
        engine
            .put(vec![FieldValue::Int64(i)], row(i, i * 10))
            .await
            .unwrap();
    }

    let mut cur = ChangeFeedCursor::from_engine(engine.clone(), 0);
    // First batch of 2.
    let b1 = cur.next_batch(2).unwrap();
    assert_eq!(b1.len(), 2);
    let after_b1 = cur.since_seq();

    // Second batch of 2.
    let b2 = cur.next_batch(2).unwrap();
    assert_eq!(b2.len(), 2);
    assert!(
        b2[0].seq > after_b1,
        "next batch must continue past the previous since_seq"
    );

    // Final batch of 1.
    let b3 = cur.next_batch(2).unwrap();
    assert_eq!(b3.len(), 1);

    // Exhausted: empty batch.
    let b4 = cur.next_batch(2).unwrap();
    assert_eq!(b4.len(), 0);
}

#[tokio::test]
async fn empty_memtable_returns_empty_batch_not_error() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;
    let mut cur = ChangeFeedCursor::from_engine(engine, 0);
    assert!(cur.next_batch(10).unwrap().is_empty());
}

#[tokio::test]
async fn delete_op_carries_pre_image_phase_2c() {
    // Phase 2a originally asserted `row.fields.len() == 0` for
    // Delete ops (pre-image reconstruction deferred). Phase 2c
    // lifts that restriction: a Delete now carries the prior
    // live row via a point lookup at `seq - 1`.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;
    engine
        .put(vec![FieldValue::Int64(7)], row(7, 777))
        .await
        .unwrap();
    engine.delete(vec![FieldValue::Int64(7)]).await.unwrap();

    let mut cur = ChangeFeedCursor::from_engine(engine, 0);
    let batch = cur.next_batch(10).unwrap();
    let del = batch
        .iter()
        .find(|r| matches!(r.op, ChangeOp::Delete))
        .unwrap();
    // Phase 2c contract: delete pre-image is the Put's value.
    assert_eq!(del.row.fields.len(), 2);
    match del.row.fields.get(1).and_then(|f| f.as_ref()) {
        Some(FieldValue::Int64(n)) => assert_eq!(*n, 777),
        other => panic!("unexpected pre-image v field: {other:?}"),
    }
}
