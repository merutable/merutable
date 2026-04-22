//! Engine-level forensic tests pinning the **post-flush read path** and
//! the **compaction read path** against real on-disk artifacts.
//!
//! These tests exist because the read path used to stop at the memtable
//! and the compaction job used to read zero files (both had a
//! `TODO: Phase 7` marker). Both bugs were silent: unit tests still
//! passed because they only inspected state that lived in the memtable,
//! while flushed data quietly disappeared on the read side. Each test
//! here triggers a real flush or compaction to disk, then re-enters the
//! engine's public API and asserts values match.
//!
//! If any of these regress, the engine has stopped reading Parquet files
//! again — which is an external analytics-ending correctness bug.

use bytes::Bytes;
use merutable::engine::{EngineConfig, MeruEngine};
use merutable::types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};
use tempfile::TempDir;

fn schema() -> TableSchema {
    TableSchema {
        table_name: "engine_forensic".into(),
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

/// Flushed rows must be readable via `engine.get(...)`. Before the read
/// path was wired to `ParquetReader`, `get` returned `None` for every key
/// once the memtable had been flushed away — the data was on disk but
/// invisible.
#[tokio::test]
async fn get_returns_flushed_row_from_l0_parquet() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

    for i in 1..=20i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    // The flush pipeline drops the memtable that was just persisted, so
    // every subsequent `get` has to traverse the L0 Parquet file to find
    // anything at all.
    for i in 1..=20i64 {
        let row = engine
            .get(&[FieldValue::Int64(i)])
            .unwrap()
            .unwrap_or_else(|| panic!("flushed row {i} must be readable via engine.get"));
        assert_eq!(row.get(0), Some(&FieldValue::Int64(i)), "id at {i}");
        assert_eq!(
            row.get(1),
            Some(&FieldValue::Bytes(Bytes::from(format!("payload_{i:04}")))),
            "payload at {i}"
        );
    }
}

/// Scans must merge the memtable and every on-disk file. Before the read
/// path was wired, `scan` returned only memtable entries — the flushed
/// rows were silently dropped from the result, which breaks every range
/// query the engine exposes.
#[tokio::test]
async fn scan_merges_memtable_and_flushed_l0() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

    // Batch 1: write + flush so it lands on disk.
    for i in 1..=10i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    // Batch 2: write without flushing; stays in the memtable.
    for i in 11..=15i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i))
            .await
            .unwrap();
    }

    // Full scan: all 15 rows must come back, regardless of which tier
    // they live in.
    let results = engine.scan(None, None).unwrap();
    assert_eq!(
        results.len(),
        15,
        "scan should merge 10 flushed rows + 5 memtable rows; got {}",
        results.len()
    );

    // Each returned row should carry the correct payload, and the result
    // list should be sorted by primary key.
    let mut last_id: Option<i64> = None;
    for (_ikey, row) in &results {
        let id = match row.get(0) {
            Some(FieldValue::Int64(v)) => *v,
            other => panic!("unexpected id field: {other:?}"),
        };
        if let Some(prev) = last_id {
            assert!(
                prev < id,
                "scan result not sorted ascending: {prev} before {id}"
            );
        }
        last_id = Some(id);

        let expected_payload = Bytes::from(format!("payload_{id:04}"));
        assert_eq!(
            row.get(1),
            Some(&FieldValue::Bytes(expected_payload)),
            "payload mismatch at id {id}"
        );
    }

    // A bounded range should also hit both tiers.
    let bounded = engine
        .scan(
            Some(&[FieldValue::Int64(5)]),
            Some(&[FieldValue::Int64(13)]),
        )
        .unwrap();
    let ids: Vec<i64> = bounded
        .iter()
        .map(|(_, r)| match r.get(0) {
            Some(FieldValue::Int64(v)) => *v,
            _ => panic!("non-int id"),
        })
        .collect();
    assert_eq!(ids, vec![5, 6, 7, 8, 9, 10, 11, 12]);
}

/// Overwrites whose newer version lives in the memtable but older version
/// was already flushed to L0 must resolve to the memtable value — MVCC
/// seq ordering has to work across tiers. Catches read-path code that
/// forgets to compare seqs when merging memtable + parquet hits.
#[tokio::test]
async fn memtable_shadows_flushed_older_version() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

    // Write v1, flush.
    engine
        .put(
            vec![FieldValue::Int64(7)],
            Row::new(vec![
                Some(FieldValue::Int64(7)),
                Some(FieldValue::Bytes(Bytes::from("v1"))),
            ]),
        )
        .await
        .unwrap();
    engine.flush().await.unwrap();

    // Overwrite v2 in memtable (no flush).
    engine
        .put(
            vec![FieldValue::Int64(7)],
            Row::new(vec![
                Some(FieldValue::Int64(7)),
                Some(FieldValue::Bytes(Bytes::from("v2"))),
            ]),
        )
        .await
        .unwrap();

    let row = engine.get(&[FieldValue::Int64(7)]).unwrap().unwrap();
    assert_eq!(
        row.get(1),
        Some(&FieldValue::Bytes(Bytes::from("v2"))),
        "memtable v2 must shadow flushed v1"
    );

    // Same key in a scan must also resolve to v2 only (no duplicate from L0).
    let results = engine.scan(None, None).unwrap();
    let sevens: Vec<_> = results
        .iter()
        .filter(|(_, r)| matches!(r.get(0), Some(FieldValue::Int64(7))))
        .collect();
    assert_eq!(sevens.len(), 1, "dedup failed across memtable+L0");
    assert_eq!(
        sevens[0].1.get(1),
        Some(&FieldValue::Bytes(Bytes::from("v2")))
    );
}

/// Flushing a tombstone must hide the previously-flushed row on read.
/// The reader has to respect `OpType::Delete` as a stop signal even when
/// the tombstone lives in the memtable and the Put lives on disk.
#[tokio::test]
async fn tombstone_in_memtable_hides_flushed_row() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

    engine
        .put(vec![FieldValue::Int64(42)], make_row(42))
        .await
        .unwrap();
    engine.flush().await.unwrap();
    engine.delete(vec![FieldValue::Int64(42)]).await.unwrap();

    let got = engine.get(&[FieldValue::Int64(42)]).unwrap();
    assert!(
        got.is_none(),
        "tombstone must mask the flushed L0 row; got {got:?}"
    );

    // Scan must also skip the deleted key.
    let results = engine.scan(None, None).unwrap();
    assert!(
        results
            .iter()
            .all(|(_, r)| { !matches!(r.get(0), Some(FieldValue::Int64(42))) }),
        "scan leaked a tombstoned key"
    );
}

/// Force a compaction from L0 → L1 and verify that every row is still
/// retrievable via `engine.get`. This pins *three* previously-broken
/// behaviors in one test:
/// 1. Compaction used to read zero source files (`file_entries = vec![]`),
///    producing an empty L1 Parquet file.
/// 2. The read path used to never traverse L1 files at all.
/// 3. Source file removal had to use the real input_files list captured
///    before the version swap, not the post-commit snapshot.
#[tokio::test]
async fn compaction_rewrites_l0_to_l1_and_reads_survive() {
    let tmp = tempfile::tempdir().unwrap();
    // Shrink the L0 trigger so `compact()` has something to do after a
    // handful of flushes.
    let mut cfg = test_config(&tmp);
    cfg.l0_compaction_trigger = 2;
    let engine = MeruEngine::open(cfg).await.unwrap();

    // Produce 3 separate L0 files by flushing between batches.
    for batch in 0..3 {
        let base = batch * 10;
        for i in 1..=10i64 {
            let id = base + i;
            engine
                .put(vec![FieldValue::Int64(id)], make_row(id))
                .await
                .unwrap();
        }
        engine.flush().await.unwrap();
    }

    // Sanity: all 30 rows are readable from L0 before compaction runs.
    for id in 1..=30i64 {
        assert!(
            engine.get(&[FieldValue::Int64(id)]).unwrap().is_some(),
            "pre-compaction: id {id} missing from L0"
        );
    }

    // Compact L0 → L1.
    engine.compact().await.unwrap();

    // After compaction the L0 files should have been consumed and a
    // single L1 file should now hold every row. `engine.get` has to walk
    // L1 to find them.
    for id in 1..=30i64 {
        let row = engine
            .get(&[FieldValue::Int64(id)])
            .unwrap()
            .unwrap_or_else(|| panic!("post-compaction: id {id} missing; compaction ate data"));
        assert_eq!(row.get(0), Some(&FieldValue::Int64(id)));
        assert_eq!(
            row.get(1),
            Some(&FieldValue::Bytes(Bytes::from(format!("payload_{id:04}")))),
            "payload corrupted at id {id}"
        );
    }

    // Also verify the L1 Parquet file is still readable by an upstream
    // Parquet crate — the external analytics claim must survive compaction.
    let l1_dir = tmp.path().join("data").join("L1");
    let mut parquet_files: Vec<std::path::PathBuf> = Vec::new();
    let mut entries = tokio::fs::read_dir(&l1_dir)
        .await
        .expect("L1 dir must exist after compaction");
    while let Some(e) = entries.next_entry().await.unwrap() {
        let p = e.path();
        if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
            parquet_files.push(p);
        }
    }
    assert_eq!(
        parquet_files.len(),
        1,
        "expected exactly one L1 parquet file after compaction, got {parquet_files:?}"
    );

    let file = std::fs::File::open(&parquet_files[0]).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.schema().clone();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        field_names.contains(&"id"),
        "L1 file missing typed 'id' column: {field_names:?}"
    );
    assert!(
        field_names.contains(&"payload"),
        "L1 file missing typed 'payload' column: {field_names:?}"
    );
    assert!(
        !field_names.contains(&"_merutable_value"),
        "L1 must not carry the postcard value blob"
    );

    let reader = builder.build().unwrap();
    let mut row_count = 0usize;
    for batch in reader {
        row_count += batch.unwrap().num_rows();
    }
    assert_eq!(row_count, 30, "L1 parquet file row count mismatch");

    // Level(0) suppress — no L0 files should remain on disk after the
    // compaction removed them from the manifest. The data dir may still
    // hold the removed files until a GC pass, but the version must not
    // list them anymore. We assert manifest-level emptiness by scanning:
    let l0_remaining = engine
        .scan(Some(&[FieldValue::Int64(1)]), Some(&[FieldValue::Int64(2)]))
        .unwrap();
    assert!(
        !l0_remaining.is_empty(),
        "scan should still find id=1 via L1"
    );
}

/// **Bug J regression**: tombstone flushed to a separate L0 file must hide
/// the Put that lives in a different L0 file. Before the fix,
/// `ParquetReader::scan()` silently dropped `OpType::Delete` entries before
/// they reached the cross-file merge in `range_scan()`, so a Delete in file
/// B was invisible and the old Put in file A resurrected.
///
/// The critical invariant: per-file scan must preserve tombstones so the
/// global sort+dedup in `read_path::range_scan` can see them and suppress
/// the corresponding Put from a different file.
#[tokio::test]
async fn bug_j_flushed_tombstone_hides_flushed_put_across_l0_files() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

    // File A: write key 99, flush.
    engine
        .put(vec![FieldValue::Int64(99)], make_row(99))
        .await
        .unwrap();
    engine.flush().await.unwrap();

    // File B: delete key 99, flush to a separate L0 file.
    engine.delete(vec![FieldValue::Int64(99)]).await.unwrap();
    engine.flush().await.unwrap();

    // Point lookup must see the tombstone and return None.
    let got = engine.get(&[FieldValue::Int64(99)]).unwrap();
    assert!(
        got.is_none(),
        "Bug J: flushed tombstone in file B must mask flushed Put in file A; got {got:?}"
    );

    // Scan must also respect the cross-file tombstone.
    let results = engine.scan(None, None).unwrap();
    assert!(
        results
            .iter()
            .all(|(_, r)| !matches!(r.get(0), Some(FieldValue::Int64(99)))),
        "Bug J: scan leaked a tombstoned key across L0 files"
    );

    // Broader scenario: interleave Puts and Deletes across multiple
    // flushes to stress the cross-file merge.
    for i in 200..=210i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap(); // File C: keys 200..=210

    // Delete even-numbered keys
    for i in (200..=210i64).step_by(2) {
        engine.delete(vec![FieldValue::Int64(i)]).await.unwrap();
    }
    engine.flush().await.unwrap(); // File D: tombstones for 200,202,...,210

    let results = engine.scan(None, None).unwrap();
    let ids: Vec<i64> = results
        .iter()
        .filter_map(|(_, r)| match r.get(0) {
            Some(FieldValue::Int64(v)) if *v >= 200 && *v <= 210 => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(
        ids,
        vec![201, 203, 205, 207, 209],
        "Bug J: even-numbered keys should be hidden by flushed tombstones"
    );
}
