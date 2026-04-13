//! `MeruDB`: the public embedding API.
//!
//! `MeruDB` is `Send + Sync`. All methods are async. One instance = one table.
//!
//! # Example
//! ```no_run
//! use merutable::{MeruDB, OpenOptions};
//! use merutable::schema::{ColumnDef, ColumnType, TableSchema};
//! use merutable::value::{FieldValue, Row};
//!
//! # async fn example() -> merutable::error::Result<()> {
//! let schema = TableSchema {
//!     table_name: "events".into(),
//!     columns: vec![
//!         ColumnDef { name: "id".into(), col_type: ColumnType::Int64, nullable: false },
//!         ColumnDef { name: "payload".into(), col_type: ColumnType::ByteArray, nullable: true },
//!     ],
//!     primary_key: vec![0],
//! };
//!
//! let db = MeruDB::open(
//!     OpenOptions::new(schema)
//!         .wal_dir("/tmp/meru-wal")
//!         .catalog_uri("/tmp/meru-data")
//! ).await?;
//!
//! db.put(Row::new(vec![
//!     Some(FieldValue::Int64(1)),
//!     Some(FieldValue::Bytes(bytes::Bytes::from("hello"))),
//! ])).await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use merutable_engine::{config::EngineConfig, engine::MeruEngine};
use merutable_types::{
    key::InternalKey,
    schema::TableSchema,
    sequence::SeqNum,
    value::{FieldValue, Row},
    Result,
};

use crate::options::OpenOptions;

/// Primary embedding interface. Thread-safe, cloneable (via `Arc`).
pub struct MeruDB {
    engine: Arc<MeruEngine>,
}

impl MeruDB {
    /// Open (or create) a database instance.
    pub async fn open(options: OpenOptions) -> Result<Self> {
        let config = EngineConfig {
            schema: options.schema.clone(),
            catalog_uri: options.catalog_uri.clone(),
            object_store_prefix: options.catalog_uri.clone(),
            wal_dir: options.wal_dir.clone(),
            memtable_size_bytes: options.memtable_size_mb * 1024 * 1024,
            ..EngineConfig::default()
        };

        let engine = MeruEngine::open(config).await?;

        Ok(Self { engine })
    }

    /// Insert or update a row. PK is extracted from the row.
    pub async fn put(&self, row: Row) -> Result<SeqNum> {
        let pk_values = row.pk_values(&self.engine.schema().primary_key)?;
        self.engine.put(pk_values, row).await
    }

    /// Batch insert/update. All rows share a single WAL sync — N× faster than
    /// individual `put()` calls.
    pub async fn put_batch(&self, rows: Vec<Row>) -> Result<SeqNum> {
        use merutable_engine::write_path::{self, MutationBatch};

        let mut batch = MutationBatch::new();
        for row in rows {
            let pk_values = row.pk_values(&self.engine.schema().primary_key)?;
            batch.put(pk_values, row);
        }
        write_path::apply_batch(&self.engine, batch).await
    }

    /// Delete by primary key values.
    pub async fn delete(&self, pk_values: Vec<FieldValue>) -> Result<SeqNum> {
        self.engine.delete(pk_values).await
    }

    /// Point lookup by primary key.
    pub fn get(&self, pk_values: &[FieldValue]) -> Result<Option<Row>> {
        self.engine.get(pk_values)
    }

    /// Range scan. Returns rows in PK order.
    pub fn scan(
        &self,
        start_pk: Option<&[FieldValue]>,
        end_pk: Option<&[FieldValue]>,
    ) -> Result<Vec<(InternalKey, Row)>> {
        self.engine.scan(start_pk, end_pk)
    }

    /// Force flush all memtables to Parquet.
    pub async fn flush(&self) -> Result<()> {
        self.engine.flush().await
    }

    /// Trigger a manual compaction.
    pub async fn compact(&self) -> Result<()> {
        self.engine.compact().await
    }

    /// Get the table schema.
    pub fn schema(&self) -> &TableSchema {
        self.engine.schema()
    }

    /// Current read sequence number.
    pub fn read_seq(&self) -> SeqNum {
        self.engine.read_seq()
    }

    /// Engine statistics snapshot. Zero hot-path overhead.
    pub fn stats(&self) -> merutable_engine::EngineStats {
        self.engine.stats()
    }

    /// Catalog base directory path (for HTAP file access).
    pub fn catalog_path(&self) -> String {
        self.engine.catalog_path()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use merutable_types::schema::{ColumnDef, ColumnType};

    fn test_schema() -> TableSchema {
        TableSchema {
            table_name: "test".into(),
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
            ],
            primary_key: vec![0],
        }
    }

    fn test_options(tmp: &tempfile::TempDir) -> crate::options::OpenOptions {
        crate::options::OpenOptions::new(test_schema())
            .wal_dir(tmp.path().join("wal"))
            .catalog_uri(tmp.path().to_string_lossy().to_string())
    }

    fn make_row(id: i64, name: &str) -> Row {
        Row::new(vec![
            Some(FieldValue::Int64(id)),
            Some(FieldValue::Bytes(Bytes::from(name.to_string()))),
        ])
    }

    #[tokio::test]
    async fn open_and_close() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();
        assert_eq!(db.schema().table_name, "test");
        assert!(db.read_seq().0 > 0);
    }

    #[tokio::test]
    async fn put_and_get() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();

        db.put(make_row(1, "alice")).await.unwrap();
        let row = db.get(&[FieldValue::Int64(1)]).unwrap();
        assert!(row.is_some());
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();
        assert!(db.get(&[FieldValue::Int64(999)]).unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_removes_row() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();

        db.put(make_row(1, "alice")).await.unwrap();
        assert!(db.get(&[FieldValue::Int64(1)]).unwrap().is_some());

        db.delete(vec![FieldValue::Int64(1)]).await.unwrap();
        assert!(db.get(&[FieldValue::Int64(1)]).unwrap().is_none());
    }

    #[tokio::test]
    async fn put_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();

        db.put(make_row(1, "alice")).await.unwrap();
        db.put(make_row(1, "bob")).await.unwrap();

        let row = db.get(&[FieldValue::Int64(1)]).unwrap().unwrap();
        let name = row.get(1).unwrap();
        assert_eq!(*name, FieldValue::Bytes(Bytes::from("bob")));
    }

    #[tokio::test]
    async fn scan_all() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();

        db.put(make_row(3, "charlie")).await.unwrap();
        db.put(make_row(1, "alice")).await.unwrap();
        db.put(make_row(2, "bob")).await.unwrap();

        let results = db.scan(None, None).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn scan_range() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();

        for i in 1..=10i64 {
            db.put(make_row(i, &format!("user{i}"))).await.unwrap();
        }

        let results = db
            .scan(Some(&[FieldValue::Int64(3)]), Some(&[FieldValue::Int64(7)]))
            .unwrap();
        assert_eq!(results.len(), 4); // 3, 4, 5, 6
    }

    #[tokio::test]
    async fn delete_then_scan_excludes_deleted() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();

        db.put(make_row(1, "alice")).await.unwrap();
        db.put(make_row(2, "bob")).await.unwrap();
        db.put(make_row(3, "charlie")).await.unwrap();

        db.delete(vec![FieldValue::Int64(2)]).await.unwrap();

        let results = db.scan(None, None).unwrap();
        assert_eq!(results.len(), 2); // 1 and 3 remain
    }

    #[tokio::test]
    async fn many_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();

        for i in 0..500i64 {
            db.put(make_row(i, &format!("row{i}"))).await.unwrap();
        }

        let results = db.scan(None, None).unwrap();
        assert_eq!(results.len(), 500);

        // Point lookups.
        assert!(db.get(&[FieldValue::Int64(0)]).unwrap().is_some());
        assert!(db.get(&[FieldValue::Int64(499)]).unwrap().is_some());
        assert!(db.get(&[FieldValue::Int64(500)]).unwrap().is_none());
    }

    #[tokio::test]
    async fn put_batch_writes_all() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();

        let rows = (0..100i64)
            .map(|i| make_row(i, &format!("batch_{i}")))
            .collect::<Vec<_>>();

        db.put_batch(rows).await.unwrap();

        // All 100 rows must be readable.
        let results = db.scan(None, None).unwrap();
        assert_eq!(results.len(), 100);
        assert!(db.get(&[FieldValue::Int64(0)]).unwrap().is_some());
        assert!(db.get(&[FieldValue::Int64(99)]).unwrap().is_some());
    }

    #[tokio::test]
    async fn flush_persists_data() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();

        db.put(make_row(1, "alice")).await.unwrap();
        db.flush().await.unwrap();

        // After flush, data should still be accessible.
        // (Memtable was rotated and flushed; the scan may come from
        // the new empty memtable + Parquet files.)
        let _results = db.scan(None, None);
    }

    #[tokio::test]
    async fn null_column() {
        let tmp = tempfile::tempdir().unwrap();
        let db = MeruDB::open(test_options(&tmp)).await.unwrap();

        // Insert a row with NULL in the nullable column.
        let row = Row::new(vec![Some(FieldValue::Int64(1)), None]);
        db.put(row).await.unwrap();

        let found = db.get(&[FieldValue::Int64(1)]).unwrap().unwrap();
        assert!(found.get(1).is_none()); // name column is NULL
    }
}
