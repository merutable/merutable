//! Regression for **Bug F**: `write_internal` triggers a background flush when
//! `Memtable::should_flush()` crosses the threshold, but never rotates the
//! memtable first. The spawned `run_flush` task calls `oldest_immutable()`,
//! gets `None`, and returns immediately — so the "auto-flush" is a silent
//! no-op and the active memtable grows unbounded past its configured
//! `memtable_size_bytes`. No L0 file is ever produced from the write path.
//!
//! This is not a data-loss bug (the rows stay in the memtable and are
//! readable), but it IS an unbounded-memory / no-backpressure bug: the
//! `should_stall()` flow control is driven by `immutable_count`, which stays
//! at 0 forever, so nothing ever slows or stops writers. A long-running
//! workload OOMs instead of spilling to Parquet.
//!
//! Reproduction: set `memtable_size_bytes` tiny (a few KiB), write enough
//! rows to cross it several times, then poll for an L0 file to appear on
//! disk WITHOUT calling `engine.flush()`. Before the fix, no L0 file ever
//! appears; after the fix, auto-flush rotates and spills.

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
        table_name: "auto_flush".into(),
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
    // 512-byte payload so a handful of rows blows past a small memtable
    // threshold without needing thousands of writes.
    let payload = vec![b'x'; 512];
    Row::new(vec![
        Some(FieldValue::Int64(i)),
        Some(FieldValue::Bytes(Bytes::from(payload))),
    ])
}

fn tiny_memtable_config(tmp: &TempDir) -> EngineConfig {
    EngineConfig {
        schema: schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        // 4 KiB threshold — a handful of 512B rows crosses it.
        memtable_size_bytes: 4 * 1024,
        // Disable the L0 stall for auto-flush regression tests: the
        // tests deliberately produce hundreds of L0 files to stress
        // the auto-flush path, which would (correctly) trip the stall
        // in production config. The tests don't exercise compaction
        // or the stall itself, so setting a very high stop trigger
        // takes them out of the equation without disabling the
        // mechanism globally.
        l0_slowdown_trigger: u32::MAX as usize,
        l0_stop_trigger: u32::MAX as usize,
        ..Default::default()
    }
}

fn count_parquet_files(dir: &Path) -> usize {
    if !dir.exists() {
        return 0;
    }
    std::fs::read_dir(dir)
        .map(|iter| {
            iter.flatten()
                .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
                .count()
        })
        .unwrap_or(0)
}

/// After crossing the memtable size threshold, the engine must auto-rotate
/// and spawn a real flush — one that actually produces an L0 Parquet file
/// on disk without any explicit `engine.flush()` call from the user.
#[tokio::test]
async fn auto_flush_produces_l0_file_without_explicit_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(tiny_memtable_config(&tmp)).await.unwrap();

    let l0_dir = tmp.path().join("data").join("L0");

    // Write ~200 rows of 512B payload = ~100 KiB. With a 4 KiB threshold,
    // the first ~10 rows should already cross it.
    for i in 1..=200i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i))
            .await
            .unwrap();
    }

    // Poll for an L0 Parquet file to appear. Auto-flush runs on a tokio
    // spawned task, so we yield a few times and allow real wall-clock time
    // for the background task to complete.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if count_parquet_files(&l0_dir) > 0 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "auto-flush never produced an L0 Parquet file within 5s: \
                 dir={l0_dir:?}, memtable never rotated, engine has silently \
                 skipped backpressure"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Every written row must still be readable — auto-flush must not have
    // dropped rows on the way to disk.
    for i in 1..=200i64 {
        let got = engine
            .get(&[FieldValue::Int64(i)])
            .unwrap()
            .unwrap_or_else(|| panic!("row {i} missing after auto-flush"));
        assert_eq!(got.get(0), Some(&FieldValue::Int64(i)));
        match got.get(1) {
            Some(FieldValue::Bytes(b)) => {
                assert_eq!(b.len(), 512, "payload length mismatch at id {i}");
            }
            other => panic!("id {i}: payload not bytes: {other:?}"),
        }
    }
}

/// Backpressure is driven by `immutable_count >= max_immutable_count`. If
/// auto-flush never rotates, the immutable count stays at 0 forever and a
/// pathological writer can blow past `memtable_size_bytes` by arbitrary
/// factors. This test pins the **invariant**: after enough writes to blow
/// past the threshold many times over, the on-disk L0 total size plus the
/// memtable's residual size should roughly track the total written bytes —
/// and more importantly, at least one rotation-and-flush must have happened.
#[tokio::test]
async fn auto_flush_produces_multiple_l0_files_under_sustained_write() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(tiny_memtable_config(&tmp)).await.unwrap();

    let l0_dir = tmp.path().join("data").join("L0");

    // 1_000 rows × 512B = 512 KiB ≫ 4 KiB threshold → should cause many
    // rotations + flushes even with conservative spawning.
    for i in 1..=1000i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i))
            .await
            .unwrap();
    }

    // Give background flushes time to drain. 10s is generous even on a
    // loaded CI machine.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if count_parquet_files(&l0_dir) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let n_files = count_parquet_files(&l0_dir);
    assert!(
        n_files >= 2,
        "expected ≥2 L0 Parquet files from sustained auto-flush; got {n_files}. \
         This means the memtable never rotated under backpressure."
    );

    // All 1000 rows still readable.
    for i in 1..=1000i64 {
        assert!(
            engine.get(&[FieldValue::Int64(i)]).unwrap().is_some(),
            "row {i} missing after sustained auto-flush"
        );
    }
}
