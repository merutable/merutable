//! Issue #44 Stage 3 regression: a file written under an older
//! schema MUST remain readable after a reopen with an additive
//! schema extension.
//!
//! Stage 1 (iceberg/catalog.rs `check_schema_compatible`) accepts
//! reopens that add nullable-or-default columns. Before Stage 3,
//! that acceptance was a footgun: every subsequent read of an
//! older L0/L1 Parquet file would fail at `build_projection_mask`
//! (L1+) or at the postcard decode (L0) because the file lacked
//! the newly-added column. Stage 3 teaches both paths to fill
//! missing columns with the column's `initial_default` (or null
//! if nullable with no default).
//!
//! Properties pinned here:
//!   1. Write → close → reopen with one nullable column appended
//!      → reads of rows written under the OLD schema return
//!      `None` for the new column.
//!   2. Same flow with `initial_default` set on the new column
//!      → reads return the default.
//!   3. Works across both L0 (dual / blob-path) and L1 (columnar)
//!      files — the blob is padded post-decode, and the columnar
//!      projection skips missing leaves + codec fills defaults.
//!   4. The prefix columns continue to decode correctly.

use merutable::types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};
use merutable::{MeruDB, OpenOptions};
use std::path::PathBuf;

fn base_schema(name: &str) -> TableSchema {
    TableSchema {
        table_name: name.into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
                ..Default::default()
            },
            ColumnDef {
                name: "score".into(),
                col_type: ColumnType::Double,
                nullable: true,
                ..Default::default()
            },
        ],
        primary_key: vec![0],
        ..Default::default()
    }
}

fn extended_schema_nullable(name: &str) -> TableSchema {
    let mut s = base_schema(name);
    s.columns.push(ColumnDef {
        name: "extra_opt".into(),
        col_type: ColumnType::Int64,
        nullable: true,
        ..Default::default()
    });
    s
}

fn extended_schema_with_default(name: &str) -> TableSchema {
    let mut s = base_schema(name);
    s.columns.push(ColumnDef {
        name: "extra_def".into(),
        col_type: ColumnType::Int64,
        nullable: false,
        initial_default: Some(FieldValue::Int64(-7)),
        write_default: Some(FieldValue::Int64(-7)),
        ..Default::default()
    });
    s
}

fn row(id: i64, score: f64) -> Row {
    Row::new(vec![
        Some(FieldValue::Int64(id)),
        Some(FieldValue::Double(score)),
    ])
}

async fn open_with(path: &std::path::Path, schema: TableSchema) -> MeruDB {
    MeruDB::open(
        OpenOptions::new(schema)
            .wal_dir(path.join("wal"))
            .catalog_uri(path.to_string_lossy().to_string()),
    )
    .await
    .expect("open")
}

fn tempdir_path() -> (tempfile::TempDir, PathBuf) {
    let td = tempfile::tempdir().unwrap();
    let p = td.path().to_path_buf();
    (td, p)
}

/// Stage 3 core: write under schema A, reopen under schema A + new
/// nullable column, verify the old rows read back with the new
/// column projected as `None`.
#[tokio::test]
async fn additive_nullable_column_fills_with_none_on_old_rows() {
    let (_td, path) = tempdir_path();

    // Write + flush so rows land in an L0 Parquet file.
    let db = open_with(&path, base_schema("evo-null")).await;
    for i in 1..=50i64 {
        db.put(row(i, i as f64 * 0.5)).await.unwrap();
    }
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // Reopen with an added nullable column. Stage 1 accepts it;
    // Stage 3 makes the reads actually work.
    let db2 = open_with(&path, extended_schema_nullable("evo-null")).await;

    // Sampled point reads.
    for i in [1, 10, 25, 50i64] {
        let got = db2
            .get(&[FieldValue::Int64(i)])
            .unwrap()
            .unwrap_or_else(|| {
                panic!("row {i} missing after additive reopen");
            });
        assert_eq!(got.fields.len(), 3, "arity must match new schema");
        match got.fields[0].as_ref() {
            Some(FieldValue::Int64(v)) => assert_eq!(*v, i),
            other => panic!("row {i}: id field wrong: {other:?}"),
        }
        match got.fields[1].as_ref() {
            Some(FieldValue::Double(v)) => assert!((*v - i as f64 * 0.5).abs() < 1e-9),
            other => panic!("row {i}: score field wrong: {other:?}"),
        }
        assert!(
            got.fields[2].is_none(),
            "row {i}: new nullable column must read as None for pre-evolution rows, got {:?}",
            got.fields[2]
        );
    }

    // Full scan also returns the extended arity.
    let scanned = db2.scan(None, None).unwrap();
    assert_eq!(scanned.len(), 50, "all 50 rows survive the reopen");
    for (_ikey, r) in &scanned {
        assert_eq!(r.fields.len(), 3);
        assert!(r.fields[2].is_none());
    }
    db2.close().await.unwrap();
}

/// Stage 3 with a non-null `initial_default`: old rows should
/// read back the default in place of the missing column.
#[tokio::test]
async fn additive_default_column_fills_with_default_on_old_rows() {
    let (_td, path) = tempdir_path();

    let db = open_with(&path, base_schema("evo-def")).await;
    for i in 1..=20i64 {
        db.put(row(i, i as f64)).await.unwrap();
    }
    db.flush().await.unwrap();
    db.close().await.unwrap();

    let db2 = open_with(&path, extended_schema_with_default("evo-def")).await;

    for i in [1, 7, 20i64] {
        let got = db2.get(&[FieldValue::Int64(i)]).unwrap().unwrap();
        assert_eq!(got.fields.len(), 3);
        match got.fields[2].as_ref() {
            Some(FieldValue::Int64(v)) => assert_eq!(
                *v, -7,
                "row {i}: missing column must fill with initial_default"
            ),
            other => {
                panic!("row {i}: new column should read as initial_default(-7), got {other:?}")
            }
        }
    }
    db2.close().await.unwrap();
}

/// Stage 3 across the L0→L1 compaction boundary: the added column
/// survives through full-file rewrite since compaction reads via
/// the same Stage 3 codec path.
#[tokio::test]
async fn additive_read_projection_works_after_compact() {
    let (_td, path) = tempdir_path();

    let db = open_with(&path, base_schema("evo-compact")).await;
    for i in 1..=30i64 {
        db.put(row(i, i as f64)).await.unwrap();
    }
    db.flush().await.unwrap();
    db.close().await.unwrap();

    let db2 = open_with(&path, extended_schema_nullable("evo-compact")).await;
    db2.compact().await.unwrap();

    for i in [1, 15, 30i64] {
        let got = db2.get(&[FieldValue::Int64(i)]).unwrap().unwrap();
        assert_eq!(got.fields.len(), 3, "arity preserved post-compact");
        assert!(
            got.fields[2].is_none(),
            "row {i}: new nullable column still None after compact"
        );
    }
    db2.close().await.unwrap();
}
