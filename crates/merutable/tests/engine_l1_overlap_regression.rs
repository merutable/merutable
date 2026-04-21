//! Regression for **Bug C**: L0→L1 compaction can produce overlapping L1
//! files, but `Version::find_file_for_key` assumes L1+ is non-overlapping
//! and only consults a single file per level. Result: point lookups in the
//! overlap region return stale data from the older file, while the newer
//! value in a second L1 file is silently invisible.
//!
//! Reproduction:
//!
//! 1. Open engine with `l0_compaction_trigger = 1` so every flushed L0
//!    file immediately becomes a compaction candidate.
//! 2. Write keys 10..=30 with payload `"v1"`, `flush`, `compact` → L1 has
//!    one file with `key_min=enc(10)`, `key_max=enc(30)`.
//! 3. Write keys 1..=20 with payload `"v2"` (overwriting 10..20 with newer
//!    sequence numbers), `flush`, `compact` → L1 now has a SECOND file with
//!    `key_min=enc(1)`, `key_max=enc(20)`.
//! 4. Read key 15. The newer value `"v2"` lives in file B (keys 1..20) but
//!    `find_file_for_key` returns file A (keys 10..30, rightmost by
//!    `key_min` ≤ 15) and never looks at B — so the reader sees stale `"v1"`.
//!
//! The scan path is not affected (it iterates every file at every level),
//! but point lookups via `engine.get(...)` are. A merged-scan fallback
//! would mask the bug in integration tests that only call `scan`; this test
//! pins the `get` behavior explicitly.

use std::path::PathBuf;

use bytes::Bytes;
use merutable::engine::{EngineConfig, MeruEngine};
use merutable::types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};
use tempfile::TempDir;

fn schema() -> TableSchema {
    TableSchema {
        table_name: "l1_overlap".into(),
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

fn make_row(i: i64, tag: &str) -> Row {
    Row::new(vec![
        Some(FieldValue::Int64(i)),
        Some(FieldValue::Bytes(Bytes::from(tag.to_string()))),
    ])
}

fn test_config(tmp: &TempDir) -> EngineConfig {
    EngineConfig {
        schema: schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        // Every flushed L0 file is immediately a compaction candidate.
        l0_compaction_trigger: 1,
        ..Default::default()
    }
}

/// Key insight: after two successive L0→L1 compactions with overlapping
/// input key ranges (and different sequence numbers), L1 ends up with TWO
/// files whose key ranges overlap. The picker doesn't pull in existing L1
/// files as compaction inputs, so the overlap is never merged away.
///
/// `find_file_for_key` then binary-searches by `key_min` and returns the
/// rightmost file whose `key_min ≤ probe` — which picks the OLDER file
/// (larger `key_min`) in the worst case, silently masking the newer value.
#[tokio::test]
async fn overlapping_l1_files_serve_correct_version_on_get() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

    // ── Batch 1: keys 10..=30 with payload "v1" ──
    for i in 10..=30i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i, "v1"))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.compact().await.unwrap();

    // After compaction, L1 should hold exactly one file covering keys 10..30.
    {
        let l1 = list_parquet_files(&tmp.path().join("data").join("L1"));
        assert_eq!(
            l1.len(),
            1,
            "L1 should have one file after first compaction; got {l1:?}"
        );
    }

    // ── Batch 2: keys 1..=20 with payload "v2" ──
    // Keys 10..20 overlap batch 1 and carry NEWER sequence numbers, so the
    // correct v2 answer for those keys lives in the batch-2 L1 file.
    for i in 1..=20i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i, "v2"))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.compact().await.unwrap();

    // L1 now has 2 files on disk. The two key ranges overlap by construction
    // (batch 1 = [10..30], batch 2 = [1..20] → overlap in [10..20]).
    {
        let l1 = list_parquet_files(&tmp.path().join("data").join("L1"));
        assert_eq!(
            l1.len(),
            2,
            "L1 should have two files after second compaction; got {l1:?}"
        );
    }

    // ── Assertions: every key must resolve to the freshest visible value ──
    //
    // 1..=9:   only in batch 2 → "v2"
    // 10..=20: overlap region, newer seq in batch 2 → "v2"
    // 21..=30: only in batch 1 → "v1"
    let mut failures: Vec<String> = Vec::new();
    for i in 1..=9i64 {
        check_get(&engine, i, "v2", &mut failures);
    }
    for i in 10..=20i64 {
        check_get(&engine, i, "v2", &mut failures);
    }
    for i in 21..=30i64 {
        check_get(&engine, i, "v1", &mut failures);
    }
    assert!(
        failures.is_empty(),
        "{} point-lookup failures across overlapping L1 files:\n{}",
        failures.len(),
        failures.join("\n")
    );

    // And scans must also resolve correctly — catches a regression where a
    // future fix to the point lookup path breaks scan dedup across L1 files.
    let scan = engine.scan(None, None).unwrap();
    let mut observed: std::collections::BTreeMap<i64, Vec<u8>> = std::collections::BTreeMap::new();
    for (_ikey, row) in scan {
        let id = match row.get(0) {
            Some(FieldValue::Int64(v)) => *v,
            other => panic!("non-int id in scan result: {other:?}"),
        };
        let payload = match row.get(1) {
            Some(FieldValue::Bytes(b)) => b.to_vec(),
            other => panic!("non-bytes payload in scan result: {other:?}"),
        };
        assert!(
            observed.insert(id, payload).is_none(),
            "scan returned duplicate id {id}"
        );
    }
    for i in 1..=9i64 {
        assert_eq!(
            observed.get(&i).map(|v| v.as_slice()),
            Some(b"v2".as_slice()),
            "scan: id {i} expected v2"
        );
    }
    for i in 10..=20i64 {
        assert_eq!(
            observed.get(&i).map(|v| v.as_slice()),
            Some(b"v2".as_slice()),
            "scan: id {i} expected v2 (overlap region)"
        );
    }
    for i in 21..=30i64 {
        assert_eq!(
            observed.get(&i).map(|v| v.as_slice()),
            Some(b"v1".as_slice()),
            "scan: id {i} expected v1"
        );
    }
    assert_eq!(observed.len(), 30, "scan missed keys");
}

fn list_parquet_files(dir: &std::path::Path) -> Vec<PathBuf> {
    if !dir.exists() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
            out.push(p);
        }
    }
    out.sort();
    out
}

fn check_get(engine: &MeruEngine, id: i64, expected: &str, failures: &mut Vec<String>) {
    let got = engine.get(&[FieldValue::Int64(id)]).unwrap();
    match got {
        Some(row) => match row.get(1) {
            Some(FieldValue::Bytes(b)) if b.as_ref() == expected.as_bytes() => {}
            Some(FieldValue::Bytes(b)) => failures.push(format!(
                "id {id}: expected {expected:?}, got {:?}",
                String::from_utf8_lossy(b)
            )),
            other => failures.push(format!(
                "id {id}: expected {expected:?}, got non-bytes {other:?}"
            )),
        },
        None => failures.push(format!("id {id}: expected {expected:?}, got None")),
    }
}
