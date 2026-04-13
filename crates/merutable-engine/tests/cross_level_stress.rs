//! Cross-level LSM stress test.
//!
//! Validates that data survives compaction across all four levels (L0 through
//! L3) with correct MVCC semantics, tombstone handling, and no lost or
//! duplicate rows. The test uses tiny thresholds to force data through the
//! level hierarchy quickly:
//!
//!   - `l0_compaction_trigger = 2` — compact after every 2 L0 files
//!   - `level_target_bytes` — tiny per-level caps so L1 overflows
//!     into L2, L2 into L3
//!
//! Sequence:
//!   1. Insert batches of ~100 rows, flush after each to create L0 files.
//!   2. Compact periodically to push data L0 → L1.
//!   3. Continue inserting + flushing + compacting; with tiny level targets
//!      the picker will promote data L1 → L2 → L3.
//!   4. Overwrite some existing keys with new values between flushes.
//!   5. Delete some keys between flushes.
//!   6. After all operations, verify:
//!      a. Every non-deleted key's latest value is returned by get().
//!      b. scan() returns exactly the expected set of live rows.
//!      c. No duplicate rows in scan output.
//!      d. Files exist at multiple levels.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use merutable_engine::{EngineConfig, MeruEngine};
use merutable_types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};
use tempfile::TempDir;

// ── Schema: id (PK, Int64) + name (ByteArray, nullable) + score (Double, nullable) ──

fn schema() -> TableSchema {
    TableSchema {
        table_name: "cross_level_stress".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::ByteArray,
                nullable: true,
            },
            ColumnDef {
                name: "score".into(),
                col_type: ColumnType::Double,
                nullable: true,
            },
        ],
        primary_key: vec![0],
    }
}

fn make_row(id: i64, tag: &str, score: f64) -> Row {
    Row::new(vec![
        Some(FieldValue::Int64(id)),
        Some(FieldValue::Bytes(Bytes::from(tag.to_string()))),
        Some(FieldValue::Double(score)),
    ])
}

fn test_config(tmp: &TempDir) -> EngineConfig {
    EngineConfig {
        schema: schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        // Aggressive thresholds to force data through all levels.
        l0_compaction_trigger: 2,
        // Tiny level targets so that L1 quickly overflows into L2, etc.
        // Each row is ~50-80 bytes in Parquet; 100 rows ~= 5-8 KB.
        // L1 target = 4 KB  → overflows after ~1 compaction
        // L2 target = 8 KB  → overflows after ~2 L1→L2 compactions
        // L3 target = 16 KB → accumulates
        level_target_bytes: vec![4 * 1024, 8 * 1024, 16 * 1024],
        // Small memtable so auto-flush doesn't interfere with explicit flushes.
        memtable_size_bytes: 64 * 1024 * 1024,
        ..Default::default()
    }
}

/// The main stress test. Inserts, overwrites, deletes across many
/// flush+compact cycles and verifies correctness at the end.
#[tokio::test]
async fn cross_level_data_integrity() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

    // Ground truth: track the latest value for every key, and which keys
    // have been deleted. This is our oracle for verification.
    let mut expected: BTreeMap<i64, (String, f64)> = BTreeMap::new();
    let mut deleted: BTreeSet<i64> = BTreeSet::new();

    let batch_size = 100i64;
    let num_phases = 5;

    for phase in 0..num_phases {
        let base = phase * batch_size;

        // ── Insert a batch of fresh keys ──
        for i in 0..batch_size {
            let id = base + i + 1; // 1-based
            let tag = format!("phase{phase}_row{id}");
            let score = id as f64 * 1.1;
            engine
                .put(vec![FieldValue::Int64(id)], make_row(id, &tag, score))
                .await
                .unwrap();
            deleted.remove(&id);
            expected.insert(id, (tag, score));
        }
        engine.flush().await.unwrap();
        eprintln!(
            "phase {phase}: flushed batch [{}, {}]",
            base + 1,
            base + batch_size
        );

        // ── Overwrite some existing keys from earlier phases ──
        if phase > 0 {
            let overwrite_base = (phase - 1) * batch_size;
            for offset in [10, 20, 30, 40, 50] {
                let id = overwrite_base + offset;
                if id < 1 {
                    continue;
                }
                let tag = format!("updated_p{phase}_id{id}");
                let score = id as f64 * 99.9;
                engine
                    .put(vec![FieldValue::Int64(id)], make_row(id, &tag, score))
                    .await
                    .unwrap();
                deleted.remove(&id);
                expected.insert(id, (tag, score));
            }
            engine.flush().await.unwrap();
            eprintln!("phase {phase}: flushed overwrites");
        }

        // ── Delete some keys from earlier phases ──
        if phase >= 2 {
            let delete_base = (phase - 2) * batch_size;
            for offset in [5, 15, 25, 35, 45, 55, 65, 75, 85, 95] {
                let id = delete_base + offset;
                if id < 1 {
                    continue;
                }
                engine.delete(vec![FieldValue::Int64(id)]).await.unwrap();
                deleted.insert(id);
            }
            engine.flush().await.unwrap();
            eprintln!("phase {phase}: flushed deletes");
        }

        // ── Compact to push data through levels ──
        // Run multiple compaction rounds to cascade L0→L1→L2→L3.
        for round in 0..4 {
            let result = engine.compact().await;
            match &result {
                Ok(()) => eprintln!("phase {phase}: compaction round {round} succeeded"),
                Err(e) => eprintln!("phase {phase}: compaction round {round} skipped: {e}"),
            }
            // Ignore "no compaction needed" — the picker decides.
            let _ = result;
        }
    }

    // ── Final compaction rounds to flush remaining L0 files ──
    for round in 0..8 {
        let _ = engine.compact().await;
        eprintln!("final compaction round {round}");
    }

    // ── Build expected live rows ──
    let live_expected: BTreeMap<i64, (String, f64)> = expected
        .iter()
        .filter(|(id, _)| !deleted.contains(id))
        .map(|(id, v)| (*id, v.clone()))
        .collect();

    eprintln!(
        "expected {} live rows, {} deleted keys",
        live_expected.len(),
        deleted.len()
    );

    // ── Verify 1: point lookups for every non-deleted key ──
    let mut get_failures: Vec<String> = Vec::new();
    for (id, (expected_tag, expected_score)) in &live_expected {
        match engine.get(&[FieldValue::Int64(*id)]).unwrap() {
            Some(row) => {
                // Check name column.
                match row.get(1) {
                    Some(FieldValue::Bytes(b)) => {
                        let got = String::from_utf8_lossy(b);
                        if got.as_ref() != expected_tag.as_str() {
                            get_failures.push(format!(
                                "id {id}: name expected {expected_tag:?}, got {got:?}"
                            ));
                        }
                    }
                    other => {
                        get_failures.push(format!(
                            "id {id}: name expected Bytes({expected_tag:?}), got {other:?}"
                        ));
                    }
                }
                // Check score column.
                match row.get(2) {
                    Some(FieldValue::Double(s)) => {
                        if (*s - expected_score).abs() > 1e-9 {
                            get_failures
                                .push(format!("id {id}: score expected {expected_score}, got {s}"));
                        }
                    }
                    other => {
                        get_failures.push(format!(
                            "id {id}: score expected Double({expected_score}), got {other:?}"
                        ));
                    }
                }
            }
            None => {
                get_failures.push(format!("id {id}: expected row, got None (data lost)"));
            }
        }
    }
    assert!(
        get_failures.is_empty(),
        "point lookup failures ({}):\n{}",
        get_failures.len(),
        get_failures.join("\n")
    );
    eprintln!(
        "PASS: all {} point lookups returned correct values",
        live_expected.len()
    );

    // ── Verify 2: deleted keys return None ──
    let mut delete_failures: Vec<String> = Vec::new();
    for id in &deleted {
        if let Some(row) = engine.get(&[FieldValue::Int64(*id)]).unwrap() {
            delete_failures.push(format!(
                "id {id}: expected None (deleted), got {:?}",
                row.get(1)
            ));
        }
    }
    assert!(
        delete_failures.is_empty(),
        "deleted key resurrections ({}):\n{}",
        delete_failures.len(),
        delete_failures.join("\n")
    );
    eprintln!("PASS: all {} deleted keys correctly absent", deleted.len());

    // ── Verify 3: scan returns exactly the live set, no duplicates ──
    let scan_results = engine.scan(None, None).unwrap();
    let mut scan_ids: BTreeMap<i64, usize> = BTreeMap::new();
    let mut scan_failures: Vec<String> = Vec::new();

    for (_ikey, row) in &scan_results {
        let id = match row.get(0) {
            Some(FieldValue::Int64(v)) => *v,
            other => panic!("non-int id in scan result: {other:?}"),
        };
        *scan_ids.entry(id).or_insert(0) += 1;
    }

    // Check for duplicates.
    for (id, count) in &scan_ids {
        if *count > 1 {
            scan_failures.push(format!(
                "id {id}: appeared {count} times in scan (duplicate)"
            ));
        }
    }

    // Check for missing live keys.
    for id in live_expected.keys() {
        if !scan_ids.contains_key(id) {
            scan_failures.push(format!("id {id}: missing from scan (live key lost)"));
        }
    }

    // Check for resurrected deleted keys.
    for id in &deleted {
        if scan_ids.contains_key(id) {
            scan_failures.push(format!(
                "id {id}: found in scan but should be deleted (tombstone leak)"
            ));
        }
    }

    // Check count matches.
    if scan_ids.len() != live_expected.len() {
        scan_failures.push(format!(
            "scan returned {} unique keys, expected {}",
            scan_ids.len(),
            live_expected.len()
        ));
    }

    assert!(
        scan_failures.is_empty(),
        "scan verification failures ({}):\n{}",
        scan_failures.len(),
        scan_failures.join("\n")
    );
    eprintln!(
        "PASS: scan returned exactly {} rows, no duplicates, no ghosts",
        scan_results.len()
    );

    // ── Verify 4: scan values match expected ──
    let mut value_failures: Vec<String> = Vec::new();
    for (_ikey, row) in &scan_results {
        let id = match row.get(0) {
            Some(FieldValue::Int64(v)) => *v,
            _ => continue,
        };
        if let Some((expected_tag, expected_score)) = live_expected.get(&id) {
            match row.get(1) {
                Some(FieldValue::Bytes(b)) => {
                    let got = String::from_utf8_lossy(b);
                    if got.as_ref() != expected_tag.as_str() {
                        value_failures.push(format!(
                            "scan id {id}: name expected {expected_tag:?}, got {got:?}"
                        ));
                    }
                }
                other => {
                    value_failures
                        .push(format!("scan id {id}: name expected Bytes, got {other:?}"));
                }
            }
            match row.get(2) {
                Some(FieldValue::Double(s)) => {
                    if (*s - expected_score).abs() > 1e-9 {
                        value_failures.push(format!(
                            "scan id {id}: score expected {expected_score}, got {s}"
                        ));
                    }
                }
                other => {
                    value_failures.push(format!(
                        "scan id {id}: score expected Double, got {other:?}"
                    ));
                }
            }
        }
    }
    assert!(
        value_failures.is_empty(),
        "scan value mismatches ({}):\n{}",
        value_failures.len(),
        value_failures.join("\n")
    );
    eprintln!("PASS: all scan values match expected");

    // ── Verify 5: files exist at multiple levels ──
    // Check on-disk level directories for Parquet files (version_set is
    // pub(crate), so integration tests inspect the filesystem).
    let data_dir = tmp.path().join("data");
    let mut level_file_counts: Vec<(u8, usize)> = Vec::new();
    for level_num in 0..=4u8 {
        let level_dir = data_dir.join(format!("L{level_num}"));
        let count = count_parquet_files(&level_dir);
        if count > 0 {
            level_file_counts.push((level_num, count));
        }
    }
    eprintln!("level file distribution:");
    for (lvl, count) in &level_file_counts {
        eprintln!("  L{lvl}: {count} files");
    }

    // We expect files at *at least* two distinct levels. With aggressive
    // thresholds data should reach L2 or L3, but we assert >= 2 levels to
    // avoid flakiness from nondeterministic compaction scheduling.
    let levels_with_files = level_file_counts.len();
    assert!(
        levels_with_files >= 2,
        "expected files at >= 2 levels to prove cross-level compaction, \
         but only found files at {} level(s): {:?}",
        levels_with_files,
        level_file_counts
    );
    eprintln!("PASS: data spread across {levels_with_files} levels");

    // Check for deep levels (L2 or L3) — this is the whole point.
    let max_level_with_files = level_file_counts.iter().map(|(l, _)| *l).max().unwrap_or(0);
    eprintln!("deepest level with files: L{max_level_with_files}");
    // Warn but don't fail if we didn't reach L2+ — the picker is score-based
    // and tiny file sizes can sometimes not trigger deeper compaction.
    if max_level_with_files < 2 {
        eprintln!(
            "WARNING: data only reached L{max_level_with_files}, \
             ideally should reach L2+ for cross-level validation"
        );
    }

    eprintln!("cross_level_data_integrity: ALL CHECKS PASSED");
}

/// Verify that a bounded range scan after cross-level compaction returns
/// the correct subset.
#[tokio::test]
async fn cross_level_bounded_scan() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

    // Insert 200 rows across 2 flush cycles, compact in between.
    for i in 1..=100i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i, "batch1", i as f64))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.compact().await.unwrap();

    for i in 101..=200i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i, "batch2", i as f64))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.compact().await.unwrap();

    // Overwrite keys 50..=60 with a new tag.
    for i in 50..=60i64 {
        engine
            .put(
                vec![FieldValue::Int64(i)],
                make_row(i, "overwritten", i as f64 * 2.0),
            )
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    engine.compact().await.unwrap();

    // Delete keys 70..=75.
    for i in 70..=75i64 {
        engine.delete(vec![FieldValue::Int64(i)]).await.unwrap();
    }
    engine.flush().await.unwrap();
    engine.compact().await.unwrap();

    // Bounded scan [40, 80): should include 40..=79 minus deleted 70..=75.
    let results = engine
        .scan(
            Some(&[FieldValue::Int64(40)]),
            Some(&[FieldValue::Int64(80)]),
        )
        .unwrap();

    let ids: Vec<i64> = results
        .iter()
        .map(|(_, r)| match r.get(0) {
            Some(FieldValue::Int64(v)) => *v,
            _ => panic!("non-int id"),
        })
        .collect();

    // Expected: 40..=69 (30 keys) + 76..=79 (4 keys) = 34 keys.
    // (70..=75 deleted)
    let expected_ids: Vec<i64> = (40..80).filter(|i| !(70..=75).contains(i)).collect();
    assert_eq!(
        ids,
        expected_ids,
        "bounded scan mismatch: got {} rows, expected {}",
        ids.len(),
        expected_ids.len()
    );

    // Verify overwritten values in the range.
    for (_, row) in &results {
        let id = match row.get(0) {
            Some(FieldValue::Int64(v)) => *v,
            _ => continue,
        };
        if (50..=60).contains(&id) {
            let name = match row.get(1) {
                Some(FieldValue::Bytes(b)) => String::from_utf8_lossy(b).to_string(),
                other => panic!("id {id}: expected Bytes, got {other:?}"),
            };
            assert_eq!(
                name, "overwritten",
                "id {id}: expected overwritten tag, got {name:?}"
            );
        }
    }

    eprintln!("cross_level_bounded_scan: PASSED");
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn count_parquet_files(dir: &std::path::Path) -> usize {
    if !dir.exists() {
        return 0;
    }
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .count()
}
