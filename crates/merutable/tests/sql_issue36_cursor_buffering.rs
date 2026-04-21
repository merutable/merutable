#![cfg(feature = "sql")]
//! Issue #36 regression: `ChangeFeedCursor::next_batch` must serve
//! batches in linear total time (O(N) scan + O(1) per returned
//! record), not quadratic.
//!
//! Pre-#36, every `next_batch` call re-scanned the entire
//! `(since_seq, read_seq]` range and returned only the first
//! `max_rows`. For a 3M-record drain at batch_size=1024 that's
//! ~3k batches × ~3M scan = O(N²); the 30s budget blew out at
//! ~1M records (see issue body).
//!
//! The fix buffers the scan: on the first `next_batch` the full
//! range is read into an in-cursor `Vec`; subsequent calls pop
//! from the head in O(1). These tests pin:
//!   1. The buffer exists and is populated after the first scan.
//!   2. Across many `next_batch` calls, ONE underlying scan covers
//!      the whole range (not N scans). We measure this via wall-
//!      clock: a 10k-record drain should take tens of ms, NOT
//!      seconds.
//!   3. Output is identical to the pre-#36 eager-scan behavior
//!      (seq-ascending, complete, no duplicates).

use std::sync::Arc;

use merutable::engine::{config::EngineConfig, engine::MeruEngine};
use merutable::sql::{ChangeFeedCursor, ChangeOp};
use merutable::types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};

fn schema() -> TableSchema {
    TableSchema {
        table_name: "cf-issue36".into(),
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

fn row(id: i64, v: i64) -> Row {
    Row::new(vec![
        Some(FieldValue::Int64(id)),
        Some(FieldValue::Int64(v)),
    ])
}

async fn open_engine(tmp: &tempfile::TempDir) -> Arc<MeruEngine> {
    let cfg = EngineConfig {
        schema: schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        ..Default::default()
    };
    MeruEngine::open(cfg).await.unwrap()
}

/// Buffer is populated after the first scan: `buffered_len`
/// transitions from 0 → non-zero → decreasing as batches drain.
#[tokio::test]
async fn buffer_is_populated_once_and_drained_across_batches() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;
    for i in 1..=500i64 {
        engine
            .put(vec![FieldValue::Int64(i)], row(i, i * 10))
            .await
            .unwrap();
    }

    let mut cursor = ChangeFeedCursor::from_engine(engine, 0);
    assert_eq!(cursor.buffered_len(), 0, "fresh cursor has empty buffer");

    // First call triggers the scan. All 500 records land in the
    // buffer; we drain 100.
    let b1 = cursor.next_batch(100).unwrap();
    assert_eq!(b1.len(), 100, "first batch returns 100");
    assert_eq!(
        cursor.buffered_len(),
        400,
        "remaining 400 stay in the buffer — NOT rescanned next call"
    );

    // Second call does NOT rescan: it pops 100 more from the
    // buffer.
    let b2 = cursor.next_batch(100).unwrap();
    assert_eq!(b2.len(), 100);
    assert_eq!(cursor.buffered_len(), 300);
    assert_eq!(
        b2[0].seq,
        b1.last().unwrap().seq + 1,
        "batches are contiguous in seq order"
    );

    // Drain the rest.
    let b3 = cursor.next_batch(10_000).unwrap();
    assert_eq!(b3.len(), 300);
    assert_eq!(cursor.buffered_len(), 0);

    // No more records → empty batch, no error.
    let b4 = cursor.next_batch(100).unwrap();
    assert!(b4.is_empty());
}

/// Linear-time drain: 10k records in <5s. Pre-#36 this was
/// quadratic and took minutes. We don't assert a tight ms bound
/// (CI variance), but ANY quadratic regression blows past 5s.
#[tokio::test]
async fn large_drain_completes_in_linear_time() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;
    const N: i64 = 10_000;
    for i in 1..=N {
        engine
            .put(vec![FieldValue::Int64(i)], row(i, i))
            .await
            .unwrap();
    }

    let mut cursor = ChangeFeedCursor::from_engine(engine, 0);
    let started = std::time::Instant::now();
    let mut total = 0usize;
    loop {
        let batch = cursor.next_batch(1024).unwrap();
        if batch.is_empty() {
            break;
        }
        total += batch.len();
    }
    let elapsed = started.elapsed();

    assert_eq!(total, N as usize, "drained every record");
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "10k-record drain took {elapsed:?} — quadratic regression"
    );
}

/// Output completeness: the buffered drain returns exactly the
/// same records as a single call draining everything at once.
/// Guards against the buffer skipping records or emitting them
/// out-of-order.
#[tokio::test]
async fn buffered_drain_matches_single_shot_drain() {
    let tmp_a = tempfile::tempdir().unwrap();
    let engine_a = open_engine(&tmp_a).await;
    let tmp_b = tempfile::tempdir().unwrap();
    let engine_b = open_engine(&tmp_b).await;
    for i in 1..=250i64 {
        engine_a
            .put(vec![FieldValue::Int64(i)], row(i, i))
            .await
            .unwrap();
        engine_b
            .put(vec![FieldValue::Int64(i)], row(i, i))
            .await
            .unwrap();
    }

    // Approach A: drain in small batches.
    let mut cursor_a = ChangeFeedCursor::from_engine(engine_a, 0);
    let mut got_a: Vec<u64> = Vec::new();
    loop {
        let batch = cursor_a.next_batch(7).unwrap();
        if batch.is_empty() {
            break;
        }
        got_a.extend(batch.iter().map(|r| r.seq));
    }

    // Approach B: drain in one huge batch.
    let mut cursor_b = ChangeFeedCursor::from_engine(engine_b, 0);
    let batch = cursor_b.next_batch(10_000).unwrap();
    let got_b: Vec<u64> = batch.iter().map(|r| r.seq).collect();

    assert_eq!(got_a, got_b, "small-batch drain must match single-shot");
    assert!(got_a.windows(2).all(|w| w[0] < w[1]), "strictly ascending");
    assert_eq!(got_a.len(), 250);
}

/// After the buffer drains, a subsequent `next_batch` refills
/// with records committed between the first scan and now.
/// Pins that the cursor remains usable as a live tail, not just
/// a one-shot snapshot.
#[tokio::test]
async fn cursor_refills_after_drain_for_new_records() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;
    for i in 1..=50i64 {
        engine
            .put(vec![FieldValue::Int64(i)], row(i, i))
            .await
            .unwrap();
    }

    let mut cursor = ChangeFeedCursor::from_engine(engine.clone(), 0);
    let first = cursor.next_batch(1_000).unwrap();
    assert_eq!(first.len(), 50);
    assert_eq!(cursor.buffered_len(), 0);

    // Commit more records.
    for i in 51..=100i64 {
        engine
            .put(vec![FieldValue::Int64(i)], row(i, i))
            .await
            .unwrap();
    }

    // Cursor picks them up — a fresh scan triggers on the empty
    // buffer.
    let second = cursor.next_batch(1_000).unwrap();
    assert_eq!(second.len(), 50);
    assert!(second.iter().all(|r| r.seq > first.last().unwrap().seq));
}

/// `skip_update_discrimination(true)` path exercises the buffer
/// with the cheap-Insert classification (no per-Put point lookup).
/// Guards the cold branch against buffer-scope regressions.
#[tokio::test]
async fn buffered_path_honors_skip_update_discrimination() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_engine(&tmp).await;
    // Put twice on the same PK — pre-#36's default path would
    // classify the second as Update. With skip=true, both are
    // Insert.
    engine
        .put(vec![FieldValue::Int64(1)], row(1, 10))
        .await
        .unwrap();
    engine
        .put(vec![FieldValue::Int64(1)], row(1, 20))
        .await
        .unwrap();

    let mut cursor = ChangeFeedCursor::from_engine(engine, 0).skip_update_discrimination(true);
    let batch = cursor.next_batch(100).unwrap();
    assert_eq!(batch.len(), 2);
    for rec in &batch {
        assert_eq!(rec.op, ChangeOp::Insert);
    }
}
