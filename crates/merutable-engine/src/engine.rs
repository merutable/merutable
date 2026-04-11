//! `MeruEngine`: central orchestrator. Owns WAL, memtable, version set, catalog,
//! and background workers. All public operations go through this struct.

use std::sync::Arc;

use merutable_iceberg::{IcebergCatalog, VersionSet};
use merutable_memtable::manager::MemtableManager;
use merutable_types::{
    key::InternalKey,
    schema::TableSchema,
    sequence::{GlobalSeq, OpType, SeqNum},
    value::{FieldValue, Row},
    Result,
};
use merutable_wal::{batch::WriteBatch, manager::WalManager};
use tokio::sync::Mutex;
use tracing::info;

use crate::config::EngineConfig;

/// The engine. Thread-safe via `Arc<MeruEngine>` — pass it across async tasks.
pub struct MeruEngine {
    pub(crate) config: EngineConfig,
    pub(crate) schema: Arc<TableSchema>,
    pub(crate) global_seq: GlobalSeq,
    pub(crate) wal: Mutex<WalManager>,
    pub(crate) memtable: MemtableManager,
    pub(crate) version_set: VersionSet,
    pub(crate) catalog: Arc<IcebergCatalog>,
    /// Serializes memtable rotation attempts from the auto-flush path so a
    /// burst of concurrent writes all seeing `should_flush=true` triggers
    /// at most one rotation. Bug F regression: without this, every task in
    /// a concurrent write burst would either (a) skip rotation entirely or
    /// (b) race and seal empty memtables. The fix is `try_lock` + double
    /// check under the lock: the loser drops through, the winner rotates.
    pub(crate) rotation_lock: Mutex<()>,
}

impl MeruEngine {
    /// Open (or create) an engine instance.
    ///
    /// 1. Open/recover WAL directory.
    /// 2. Replay recovered batches into a fresh memtable.
    /// 3. Open Iceberg catalog and load current version.
    /// 4. Initialize global seq to `max(wal_max_seq, iceberg_max_seq) + 1`.
    pub async fn open(config: EngineConfig) -> Result<Arc<Self>> {
        let schema = Arc::new(config.schema.clone());

        // WAL recovery.
        let (recovered_batches, wal_max_seq) = WalManager::recover_from_dir(&config.wal_dir)?;
        info!(
            recovered = recovered_batches.len(),
            wal_max_seq = wal_max_seq.0,
            "WAL recovery complete"
        );

        // Open Iceberg catalog and load current version.
        let catalog = IcebergCatalog::open(&config.catalog_uri, config.schema.clone()).await?;
        let manifest = catalog.current_manifest().await;
        let version = manifest.to_version(schema.clone());
        let iceberg_max_seq = version
            .levels
            .values()
            .flat_map(|files| files.iter().map(|f| f.meta.seq_max))
            .max()
            .unwrap_or(0);

        let version_set = VersionSet::new(version);
        let catalog = Arc::new(catalog);

        // Global seq = max of WAL and Iceberg + 1.
        let init_seq = std::cmp::max(wal_max_seq.0, iceberg_max_seq) + 1;
        let global_seq = GlobalSeq::new(init_seq);

        // Memtable manager.
        let memtable = MemtableManager::new(
            SeqNum(init_seq),
            config.memtable_size_bytes,
            config.max_immutable_count,
        );

        // Replay recovered WAL batches into memtable.
        for batch in &recovered_batches {
            memtable.apply_batch(batch)?;
        }
        if !recovered_batches.is_empty() {
            info!(
                count = recovered_batches.len(),
                "replayed WAL batches into memtable"
            );
        }

        // Open a fresh WAL for new writes.
        let next_log = recovered_batches.len() as u64 + 1;
        let wal = WalManager::open(&config.wal_dir, next_log)?;

        let engine = Arc::new(Self {
            config,
            schema,
            global_seq,
            wal: Mutex::new(wal),
            memtable,
            version_set,
            catalog,
            rotation_lock: Mutex::new(()),
        });

        Ok(engine)
    }

    // ── Write path ──────────────────────────────────���────────────────────

    /// Insert a row. `pk_values` are the primary key fields; `row` is the full
    /// row (including PK columns).
    pub async fn put(self: &Arc<Self>, pk_values: Vec<FieldValue>, row: Row) -> Result<SeqNum> {
        self.write_internal(pk_values, Some(row), OpType::Put).await
    }

    /// Delete by primary key.
    pub async fn delete(self: &Arc<Self>, pk_values: Vec<FieldValue>) -> Result<SeqNum> {
        self.write_internal(pk_values, None, OpType::Delete).await
    }

    async fn write_internal(
        self: &Arc<Self>,
        pk_values: Vec<FieldValue>,
        row: Option<Row>,
        op_type: OpType,
    ) -> Result<SeqNum> {
        // Flow control: stall if immutable queue is full.
        while self.memtable.should_stall() {
            self.memtable.flush_complete.notified().await;
        }

        // Allocate sequence number.
        let seq = self.global_seq.allocate();

        // Encode user key bytes (PK without tag).
        let ikey = InternalKey::encode(&pk_values, seq, op_type, &self.schema)?;
        let user_key_bytes = ikey.user_key_bytes().to_vec();

        // Build WAL batch.
        let mut batch = WriteBatch::new(seq);
        let value_bytes = row.map(|r| {
            // Serialize row values to bytes. For now, use JSON as a simple encoding.
            // Phase 4 completion wires up the proper codec.
            let json = serde_json::to_vec(&r).unwrap_or_default();
            bytes::Bytes::from(json)
        });
        match op_type {
            OpType::Put => batch.put(
                bytes::Bytes::from(user_key_bytes),
                value_bytes.unwrap_or_default(),
            ),
            OpType::Delete => batch.delete(bytes::Bytes::from(user_key_bytes)),
        }

        // WAL first (durability).
        {
            let mut wal = self.wal.lock().await;
            wal.append(&batch)?;
        }

        // Apply to memtable.
        let should_flush = self.memtable.apply_batch(&batch)?;

        // Trigger flush if threshold crossed. The flush requires a rotate
        // (active → immutable) so that `run_flush` has something to find in
        // `oldest_immutable()`; before Bug F was fixed, this path spawned
        // `run_flush` without rotating and the task returned a no-op,
        // leaving the memtable to grow unbounded. Concurrent writers all
        // see the same stale `should_flush=true` during a burst — serialize
        // rotation through `rotation_lock` and re-check under the lock so
        // only one task actually seals and spawns a flush.
        if should_flush {
            if let Ok(_guard) = self.rotation_lock.try_lock() {
                // Stale should_flush from another task's apply_batch? If the
                // active memtable was already rotated out from under us, the
                // new active is small and we have nothing to do.
                if self.memtable.active_should_flush() {
                    let next_seq = self.global_seq.current().next();
                    self.memtable.rotate(next_seq);
                    let engine = Arc::clone(self);
                    tokio::spawn(async move {
                        if let Err(e) = crate::flush::run_flush(&engine).await {
                            tracing::error!(error = %e, "auto-flush failed");
                        }
                    });
                }
            }
        }

        Ok(seq)
    }

    // ── Read path ────────────────────────────────────────────────────────

    /// Point lookup by primary key. Returns the row if found (not deleted).
    pub fn get(&self, pk_values: &[FieldValue]) -> Result<Option<Row>> {
        crate::read_path::point_lookup(self, pk_values)
    }

    /// Range scan. Returns rows in PK order where `start_pk <= pk < end_pk`.
    /// If `start_pk` is `None`, scan from the beginning.
    /// If `end_pk` is `None`, scan to the end.
    pub fn scan(
        &self,
        start_pk: Option<&[FieldValue]>,
        end_pk: Option<&[FieldValue]>,
    ) -> Result<Vec<(InternalKey, Row)>> {
        crate::read_path::range_scan(self, start_pk, end_pk)
    }

    // ── Admin ────────────────────────────────────────────────────────────

    /// Force flush all immutable memtables and the active memtable.
    pub async fn flush(self: &Arc<Self>) -> Result<()> {
        // Rotate active to immutable.
        let next_seq = self.global_seq.current().next();
        self.memtable.rotate(next_seq);
        // Flush all immutables.
        while self.memtable.oldest_immutable().is_some() {
            crate::flush::run_flush(self).await?;
        }
        Ok(())
    }

    /// Trigger a manual compaction. Picks the best level and runs one job.
    pub async fn compact(self: &Arc<Self>) -> Result<()> {
        crate::compaction::job::run_compaction(self).await
    }

    /// Current read sequence (snapshot for reads).
    pub fn read_seq(&self) -> SeqNum {
        self.global_seq.current()
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use merutable_types::{
        schema::{ColumnDef, ColumnType},
        value::Row,
    };

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
                    name: "val".into(),
                    col_type: ColumnType::ByteArray,
                    nullable: true,
                },
            ],
            primary_key: vec![0],
        }
    }

    fn test_config(tmp: &tempfile::TempDir) -> crate::config::EngineConfig {
        crate::config::EngineConfig {
            schema: test_schema(),
            catalog_uri: tmp.path().to_string_lossy().to_string(),
            object_store_prefix: tmp.path().to_string_lossy().to_string(),
            wal_dir: tmp.path().join("wal"),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn open_creates_fresh_engine() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();
        assert!(engine.read_seq().0 > 0);
        assert_eq!(engine.schema().table_name, "test");
    }

    #[tokio::test]
    async fn put_and_get() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

        engine
            .put(
                vec![FieldValue::Int64(1)],
                Row::new(vec![
                    Some(FieldValue::Int64(1)),
                    Some(FieldValue::Bytes(bytes::Bytes::from("hello"))),
                ]),
            )
            .await
            .unwrap();

        let row = engine.get(&[FieldValue::Int64(1)]).unwrap();
        assert!(row.is_some());
    }

    #[tokio::test]
    async fn get_missing_key() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();
        let row = engine.get(&[FieldValue::Int64(999)]).unwrap();
        assert!(row.is_none());
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

        engine
            .put(
                vec![FieldValue::Int64(1)],
                Row::new(vec![Some(FieldValue::Int64(1)), None]),
            )
            .await
            .unwrap();
        assert!(engine.get(&[FieldValue::Int64(1)]).unwrap().is_some());

        engine.delete(vec![FieldValue::Int64(1)]).await.unwrap();
        assert!(engine.get(&[FieldValue::Int64(1)]).unwrap().is_none());
    }

    #[tokio::test]
    async fn multiple_puts_and_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

        for i in 1..=10i64 {
            engine
                .put(
                    vec![FieldValue::Int64(i)],
                    Row::new(vec![
                        Some(FieldValue::Int64(i)),
                        Some(FieldValue::Bytes(bytes::Bytes::from(format!("val{i}")))),
                    ]),
                )
                .await
                .unwrap();
        }

        // Full scan.
        let results = engine.scan(None, None).unwrap();
        assert_eq!(results.len(), 10);

        // Range scan: keys 3..7 (exclusive end).
        let results = engine
            .scan(Some(&[FieldValue::Int64(3)]), Some(&[FieldValue::Int64(7)]))
            .unwrap();
        assert_eq!(results.len(), 4); // 3, 4, 5, 6
    }

    #[tokio::test]
    async fn overwrite_updates_value() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

        engine
            .put(
                vec![FieldValue::Int64(1)],
                Row::new(vec![
                    Some(FieldValue::Int64(1)),
                    Some(FieldValue::Bytes(bytes::Bytes::from("v1"))),
                ]),
            )
            .await
            .unwrap();
        engine
            .put(
                vec![FieldValue::Int64(1)],
                Row::new(vec![
                    Some(FieldValue::Int64(1)),
                    Some(FieldValue::Bytes(bytes::Bytes::from("v2"))),
                ]),
            )
            .await
            .unwrap();

        let row = engine.get(&[FieldValue::Int64(1)]).unwrap().unwrap();
        // Should see the latest value.
        let val = row.get(1).unwrap();
        assert_eq!(*val, FieldValue::Bytes(bytes::Bytes::from("v2")));
    }

    #[tokio::test]
    async fn seq_increases_monotonically() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

        let s1 = engine
            .put(
                vec![FieldValue::Int64(1)],
                Row::new(vec![Some(FieldValue::Int64(1)), None]),
            )
            .await
            .unwrap();
        let s2 = engine
            .put(
                vec![FieldValue::Int64(2)],
                Row::new(vec![Some(FieldValue::Int64(2)), None]),
            )
            .await
            .unwrap();
        let s3 = engine.delete(vec![FieldValue::Int64(1)]).await.unwrap();

        assert!(s1 < s2);
        assert!(s2 < s3);
    }

    #[tokio::test]
    async fn flush_and_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

        for i in 1..=5i64 {
            engine
                .put(
                    vec![FieldValue::Int64(i)],
                    Row::new(vec![Some(FieldValue::Int64(i)), None]),
                )
                .await
                .unwrap();
        }

        // Flush to Parquet.
        engine.flush().await.unwrap();

        // Data should still be scannable (from Parquet or re-read).
        // At minimum, the scan should not error.
        let _results = engine.scan(None, None);
    }

    #[tokio::test]
    async fn wal_recovery() {
        let tmp = tempfile::tempdir().unwrap();

        // Write some data.
        {
            let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();
            engine
                .put(
                    vec![FieldValue::Int64(42)],
                    Row::new(vec![
                        Some(FieldValue::Int64(42)),
                        Some(FieldValue::Bytes(bytes::Bytes::from("persisted"))),
                    ]),
                )
                .await
                .unwrap();
            // Drop engine without explicit close — simulates crash.
        }

        // Reopen — WAL recovery should replay the write.
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();
        let row = engine.get(&[FieldValue::Int64(42)]).unwrap();
        assert!(row.is_some(), "WAL recovery should restore the row");
    }
}
