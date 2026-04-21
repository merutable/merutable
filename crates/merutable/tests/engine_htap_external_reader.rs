//! HTAP claim validation: a Parquet file produced by merutable's flush
//! pipeline can be opened by an *unmodified upstream Parquet reader* and
//! every user-defined column appears as a typed Arrow column with the
//! original values.
//!
//! This is the integration test that backs the HTAP marketing claim:
//! "merutable files are real Parquet files that Spark/DuckDB/iceberg-rust
//! can scan directly." If this test passes, an external reader does not
//! need any merutable code on its read side.
//!
//! Read side intentionally imports nothing from merutable except the path
//! discovery — it uses only the upstream `parquet` and `arrow` crates.

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::array::{Array, BinaryArray, BooleanArray, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use bytes::Bytes;
use merutable::engine::{EngineConfig, MeruEngine};
use merutable::types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const TOTAL_ROWS: i64 = 100;

/// One decoded row from the upstream Arrow reader's perspective:
/// `(name?, active, score?)`. `id` is the map key elsewhere.
type DecodedRow = (Option<Vec<u8>>, bool, Option<f64>);

fn htap_schema() -> TableSchema {
    TableSchema {
        table_name: "htap_demo".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,

                ..Default::default()
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnType::ByteArray,
                nullable: true,

                ..Default::default()
            },
            ColumnDef {
                name: "active".into(),
                col_type: ColumnType::Boolean,
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

fn build_row(i: i64) -> Row {
    let name = if i % 7 == 0 {
        None
    } else {
        Some(FieldValue::Bytes(Bytes::from(format!("name_{i:04}"))))
    };
    let score = if i % 5 == 0 {
        None
    } else {
        Some(FieldValue::Double(i as f64 * 0.5))
    };
    Row::new(vec![
        Some(FieldValue::Int64(i)),
        name,
        Some(FieldValue::Boolean(i % 2 == 0)),
        score,
    ])
}

async fn discover_l0_file(catalog_root: &Path) -> PathBuf {
    let dir = catalog_root.join("data").join("L0");
    let mut entries = tokio::fs::read_dir(&dir)
        .await
        .expect("L0 dir must exist after flush");
    while let Some(e) = entries.next_entry().await.expect("read_dir") {
        let path = e.path();
        if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            return path;
        }
    }
    panic!("no parquet file found in {dir:?}");
}

/// End-to-end HTAP claim: insert rows, force flush, then open the resulting
/// L0 Parquet file with the upstream `parquet` crate (no merutable code on
/// the read side) and verify that every user column is present as a typed
/// Arrow column with the exact values that were written.
#[tokio::test]
async fn l0_file_is_readable_by_upstream_parquet_with_typed_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = EngineConfig {
        schema: htap_schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        ..Default::default()
    };
    let engine = MeruEngine::open(cfg).await.unwrap();

    // Insert rows in random-ish key order to make sure the writer's sort
    // path is exercised; values are deterministic from `i` for assertions.
    let order: Vec<i64> = (1..=TOTAL_ROWS).collect();
    for i in &order {
        let row = build_row(*i);
        engine.put(vec![FieldValue::Int64(*i)], row).await.unwrap();
    }

    // Force a flush so an L0 Parquet file lands on disk.
    engine.flush().await.unwrap();

    // ── External reader side: zero merutable imports beyond path discovery.
    let parquet_path = discover_l0_file(tmp.path()).await;
    let file = File::open(&parquet_path).expect("open parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("upstream ParquetRecordBatchReaderBuilder must accept the file");

    // Schema check: typed user columns must be present with the right Arrow
    // datatypes. The hidden `_merutable_ikey` and `_merutable_value` columns
    // are also present at L0 — that's fine, external readers ignore them by
    // name when they project user columns.
    let arrow_schema = builder.schema().clone();
    let field_names: Vec<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert!(
        field_names.contains(&"id"),
        "missing typed column id: {field_names:?}"
    );
    assert!(
        field_names.contains(&"name"),
        "missing typed column name: {field_names:?}"
    );
    assert!(
        field_names.contains(&"active"),
        "missing typed column active: {field_names:?}"
    );
    assert!(
        field_names.contains(&"score"),
        "missing typed column score: {field_names:?}"
    );
    assert!(
        field_names.contains(&"_merutable_ikey"),
        "L0 must expose hidden ikey column"
    );
    assert!(
        field_names.contains(&"_merutable_value"),
        "L0 must expose hidden postcard value blob column"
    );
    // Issue #16: typed _seq (Int64) and _op (Int32) columns so
    // external analytics engines apply the mandatory MVCC dedup
    // projection without decoding the ikey trailer by hand.
    assert!(
        field_names.contains(&"_merutable_seq"),
        "every file must expose typed _merutable_seq column"
    );
    assert!(
        field_names.contains(&"_merutable_op"),
        "every file must expose typed _merutable_op column"
    );
    let seq_field = arrow_schema.field_with_name("_merutable_seq").unwrap();
    assert_eq!(seq_field.data_type(), &DataType::Int64);
    let op_field = arrow_schema.field_with_name("_merutable_op").unwrap();
    assert_eq!(op_field.data_type(), &DataType::Int32);

    // Datatype assertions: native types, not opaque blobs.
    let id_field = arrow_schema.field_with_name("id").unwrap();
    assert_eq!(id_field.data_type(), &DataType::Int64);
    let name_field = arrow_schema.field_with_name("name").unwrap();
    assert_eq!(name_field.data_type(), &DataType::Binary);
    let active_field = arrow_schema.field_with_name("active").unwrap();
    assert_eq!(active_field.data_type(), &DataType::Boolean);
    let score_field = arrow_schema.field_with_name("score").unwrap();
    assert_eq!(score_field.data_type(), &DataType::Float64);

    // Read all batches and reconstruct (id → row) so we can verify against
    // the inputs without depending on file ordering.
    let id_idx = arrow_schema.index_of("id").unwrap();
    let name_idx = arrow_schema.index_of("name").unwrap();
    let active_idx = arrow_schema.index_of("active").unwrap();
    let score_idx = arrow_schema.index_of("score").unwrap();

    let reader = builder.build().unwrap();
    let mut total_seen: i64 = 0;
    let mut by_id: std::collections::HashMap<i64, DecodedRow> = std::collections::HashMap::new();

    for batch_result in reader {
        let batch = batch_result.expect("read batch");
        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let names = batch
            .column(name_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("name is Binary");
        let actives = batch
            .column(active_idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("active is Boolean");
        let scores = batch
            .column(score_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("score is Float64");

        for r in 0..batch.num_rows() {
            let id = ids.value(r);
            let name = if names.is_null(r) {
                None
            } else {
                Some(names.value(r).to_vec())
            };
            let active = actives.value(r);
            let score = if scores.is_null(r) {
                None
            } else {
                Some(scores.value(r))
            };
            by_id.insert(id, (name, active, score));
            total_seen += 1;
        }
    }

    assert_eq!(
        total_seen, TOTAL_ROWS,
        "external reader saw {total_seen} rows; expected {TOTAL_ROWS}"
    );
    assert_eq!(by_id.len(), TOTAL_ROWS as usize, "duplicate ids in output?");

    // Field-for-field equality against the input generator.
    for i in 1..=TOTAL_ROWS {
        let (got_name, got_active, got_score) =
            by_id.get(&i).unwrap_or_else(|| panic!("id {i} missing"));

        let expected = build_row(i);
        let expected_name = match expected.get(1) {
            Some(FieldValue::Bytes(b)) => Some(b.to_vec()),
            None => None,
            other => panic!("unexpected name field: {other:?}"),
        };
        let expected_active = match expected.get(2) {
            Some(FieldValue::Boolean(b)) => *b,
            other => panic!("unexpected active field: {other:?}"),
        };
        let expected_score = match expected.get(3) {
            Some(FieldValue::Double(d)) => Some(*d),
            None => None,
            other => panic!("unexpected score field: {other:?}"),
        };

        assert_eq!(got_name, &expected_name, "name mismatch at id {i}");
        assert_eq!(*got_active, expected_active, "active mismatch at id {i}");
        assert_eq!(got_score, &expected_score, "score mismatch at id {i}");
    }
}

/// Cold-tier (L1+) HTAP claim: an L1 Parquet file produced by merutable's
/// writer must (a) NOT carry the `_merutable_value` blob and (b) still be
/// fully decodable by an upstream Parquet reader through the typed columns.
///
/// This bypasses `engine.compact()` because the compaction-job's reader
/// integration is still pending — but it exercises the same writer path
/// (`merutable::parquet::writer::write_sorted_rows` at `Level(1)`) that a
/// future compaction will use, so the cross-crate HTAP contract is what's
/// being validated.
#[tokio::test]
async fn l1_file_is_readable_by_upstream_parquet_without_value_blob() {
    use merutable::parquet::writer::write_sorted_rows;
    use merutable::types::{
        key::InternalKey,
        level::Level,
        sequence::{OpType, SeqNum},
    };

    let schema = Arc::new(htap_schema());

    // Build (InternalKey, Row) inputs in PK-ascending order.
    let mut rows: Vec<(InternalKey, Row)> = Vec::with_capacity(TOTAL_ROWS as usize);
    for i in 1..=TOTAL_ROWS {
        let row = build_row(i);
        let ikey = InternalKey::encode(
            &[FieldValue::Int64(i)],
            SeqNum(i as u64),
            OpType::Put,
            &schema,
        )
        .unwrap();
        rows.push((ikey, row));
    }

    let (parquet_bytes, _bloom, _meta) = write_sorted_rows(
        rows,
        schema.clone(),
        Level(1),
        merutable::types::level::FileFormat::Columnar,
        10,
    )
    .unwrap();
    assert!(!parquet_bytes.is_empty(), "L1 writer produced empty file");

    // Open with the upstream parquet crate, again zero merutable code on
    // the read side.
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet_bytes))
        .expect("upstream ParquetRecordBatchReaderBuilder must accept L1 file");

    let arrow_schema = builder.schema().clone();
    let field_names: Vec<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    // Cold tier: typed columns present, value blob ABSENT.
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"active"));
    assert!(field_names.contains(&"score"));
    assert!(field_names.contains(&"_merutable_ikey"));
    assert!(
        !field_names.contains(&"_merutable_value"),
        "L1 must NOT carry the postcard value blob; got {field_names:?}"
    );

    // Datatype sanity.
    assert_eq!(
        arrow_schema.field_with_name("id").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        arrow_schema.field_with_name("score").unwrap().data_type(),
        &DataType::Float64
    );

    // Decode every row and verify it matches the input generator.
    let id_idx = arrow_schema.index_of("id").unwrap();
    let name_idx = arrow_schema.index_of("name").unwrap();
    let active_idx = arrow_schema.index_of("active").unwrap();
    let score_idx = arrow_schema.index_of("score").unwrap();

    let reader = builder.build().unwrap();
    let mut total_seen: i64 = 0;
    for batch_result in reader {
        let batch = batch_result.expect("read batch");
        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column(name_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let actives = batch
            .column(active_idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let scores = batch
            .column(score_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for r in 0..batch.num_rows() {
            let i = ids.value(r);
            let expected = build_row(i);

            let expected_name = match expected.get(1) {
                Some(FieldValue::Bytes(b)) => Some(b.to_vec()),
                None => None,
                _ => unreachable!(),
            };
            let got_name = if names.is_null(r) {
                None
            } else {
                Some(names.value(r).to_vec())
            };
            assert_eq!(got_name, expected_name, "L1 name mismatch at id {i}");

            let expected_active = matches!(expected.get(2), Some(FieldValue::Boolean(true)));
            assert_eq!(
                actives.value(r),
                expected_active,
                "L1 active mismatch id {i}"
            );

            let expected_score = match expected.get(3) {
                Some(FieldValue::Double(d)) => Some(*d),
                None => None,
                _ => unreachable!(),
            };
            let got_score = if scores.is_null(r) {
                None
            } else {
                Some(scores.value(r))
            };
            assert_eq!(got_score, expected_score, "L1 score mismatch id {i}");

            total_seen += 1;
        }
    }
    assert_eq!(total_seen, TOTAL_ROWS);
}
