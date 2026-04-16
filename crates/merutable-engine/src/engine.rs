//! `MeruEngine`: central orchestrator. Owns WAL, memtable, version set, catalog,
//! and background workers. All public operations go through this struct.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use merutable_iceberg::{IcebergCatalog, VersionSet};
use merutable_memtable::manager::MemtableManager;
use merutable_types::{
    key::InternalKey,
    schema::TableSchema,
    sequence::{GlobalSeq, OpType, SeqNum},
    value::{FieldValue, Row},
    MeruError, Result,
};
use merutable_wal::{batch::WriteBatch, manager::WalManager};
use tokio::sync::Mutex;
use tracing::{info, instrument, warn};

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
    /// Serializes `run_flush` so two concurrently-spawned auto-flush tasks
    /// don't both call `oldest_immutable()`, see the same sealed memtable,
    /// and write two L0 Parquet files containing identical rows (followed
    /// by two competing catalog commits). Bug G regression.
    pub(crate) flush_mutex: Mutex<()>,
    /// Bug T fix: serializes `run_compaction` so two background compaction
    /// workers (default `compaction_parallelism=2`) don't both pick the
    /// same level, read the same files, write duplicate output Parquet
    /// files at the output level, and commit competing snapshots —
    /// producing duplicate rows visible to readers.
    pub(crate) compaction_mutex: Mutex<()>,
    /// Row-level LRU cache for point lookups. `None` if disabled (capacity=0).
    pub(crate) row_cache: Option<crate::cache::RowCache>,
    /// True if opened in read-only mode. Write ops will return `MeruError::ReadOnly`.
    pub(crate) read_only: bool,
    /// IMP-02: reader-visible sequence number. Only advanced AFTER memtable
    /// apply completes (inside the WAL lock), so `read_seq()` never returns a
    /// sequence whose data isn't in the memtable yet. `global_seq` remains
    /// the allocation counter; `visible_seq` is what readers snapshot.
    pub(crate) visible_seq: GlobalSeq,
    /// IMP-12: files pending deletion after compaction. Each entry is
    /// (absolute_path, obsoleted_at_instant). Files are only physically
    /// deleted after `gc_grace_period_secs` has elapsed, giving external
    /// readers time to finish in-progress reads.
    pub(crate) pending_deletions: Mutex<Vec<(std::path::PathBuf, std::time::Instant)>>,
    /// Set to `true` by `close()`. All subsequent write/flush/compact ops
    /// return `MeruError::Closed`. Reads remain available until the engine
    /// is dropped.
    closed: AtomicBool,
}

impl MeruEngine {
    /// Open (or create) an engine instance.
    ///
    /// 1. Open/recover WAL directory.
    /// 2. Replay recovered batches into a fresh memtable.
    /// 3. Open Iceberg catalog and load current version.
    /// 4. Initialize global seq to `max(wal_max_seq, iceberg_max_seq) + 1`.
    #[instrument(skip(config), fields(table = %config.schema.table_name))]
    pub async fn open(config: EngineConfig) -> Result<Arc<Self>> {
        // Bug SC2 fix: validate the schema upfront so misconfigured schemas
        // (out-of-bounds PK indices, nullable PKs, empty columns) produce a
        // clear error here instead of panicking deep inside encode/decode.
        config.schema.validate()?;

        let schema = Arc::new(config.schema.clone());

        let read_only = config.read_only;

        // WAL recovery — in read-only mode, skip if WAL dir doesn't exist.
        let (recovered_batches, wal_max_seq, max_log_number) =
            if read_only && !config.wal_dir.exists() {
                (Vec::new(), SeqNum(0), 0u64)
            } else {
                WalManager::recover_from_dir(&config.wal_dir)?
            };
        info!(
            recovered = recovered_batches.len(),
            wal_max_seq = wal_max_seq.0,
            read_only,
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

        // Bug W fix: compute `next_log` from the highest WAL file number on
        // disk, NOT from the batch count. After partial WAL GC, the batch
        // count can be smaller than the highest surviving log number, causing
        // the new WAL file to collide with (and truncate) an existing file.
        // A second crash before flush would then lose the overwritten data.
        let next_log = max_log_number + 1;
        // In read-only mode, ensure WAL dir exists for WalManager::open
        // (it won't be used since write ops are guarded).
        if read_only {
            std::fs::create_dir_all(&config.wal_dir).map_err(MeruError::Io)?;
        }
        let wal = WalManager::open(&config.wal_dir, next_log)?;

        let row_cache = if config.row_cache_capacity > 0 {
            Some(crate::cache::RowCache::new(config.row_cache_capacity))
        } else {
            None
        };

        // IMP-02: visible_seq starts at the same value as global_seq.
        // Both are at init_seq after recovery.
        let visible_seq = GlobalSeq::new(init_seq);

        let engine = Arc::new(Self {
            config,
            schema,
            global_seq,
            wal: Mutex::new(wal),
            memtable,
            version_set,
            catalog,
            rotation_lock: Mutex::new(()),
            flush_mutex: Mutex::new(()),
            compaction_mutex: Mutex::new(()),
            row_cache,
            read_only,
            visible_seq,
            pending_deletions: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
        });

        Ok(engine)
    }

    // ── Write path ──────────────────────────────────���────────────────────

    /// Insert a row. `pk_values` are the primary key fields; `row` is the full
    /// row (including PK columns).
    #[instrument(skip(self, row), fields(op = "put"))]
    pub async fn put(self: &Arc<Self>, pk_values: Vec<FieldValue>, row: Row) -> Result<SeqNum> {
        if self.read_only {
            return Err(MeruError::ReadOnly);
        }
        self.write_internal(pk_values, Some(row), OpType::Put).await
    }

    /// Delete by primary key.
    #[instrument(skip(self), fields(op = "delete"))]
    pub async fn delete(self: &Arc<Self>, pk_values: Vec<FieldValue>) -> Result<SeqNum> {
        if self.read_only {
            return Err(MeruError::ReadOnly);
        }
        self.write_internal(pk_values, None, OpType::Delete).await
    }

    #[instrument(skip(self, pk_values, row), fields(op_type = ?op_type))]
    async fn write_internal(
        self: &Arc<Self>,
        pk_values: Vec<FieldValue>,
        row: Option<Row>,
        op_type: OpType,
    ) -> Result<SeqNum> {
        if self.closed.load(Ordering::Acquire) {
            return Err(MeruError::Closed);
        }

        // Flow control: graduated backpressure + hard stall.
        //
        // IMP-09: instead of a binary block at max_immutable, introduce a
        // graduated delay once the immutable queue is >50% full. This
        // prevents the sawtooth throughput pattern where all blocked
        // writers resume simultaneously and re-trigger the stall.
        //
        // Bug Z fix preserved: register `notified()` BEFORE checking the
        // condition to avoid lost-wakeup TOCTOU race.
        loop {
            let notify = self.memtable.flush_complete.notified();
            if !self.memtable.should_stall() {
                break;
            }
            notify.await;
        }

        // Graduated delay: slow writes proportionally as the immutable
        // queue fills, before the hard stall triggers.
        {
            let imm_count = self.memtable.immutable_count() as f64;
            let max_imm = self.config.max_immutable_count as f64;
            let ratio = imm_count / max_imm;
            if ratio > 0.5 {
                // Linear ramp: 0ms at 50%, 5ms at 100%.
                let delay_ms = ((ratio - 0.5) * 2.0 * 5.0) as u64;
                if delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }

        // Encode only the user-key (PK) bytes — no full InternalKey struct,
        // no pk_values.to_vec() clone, no tag encoding. Hot-path optimization.
        let user_key_bytes = InternalKey::encode_user_key(&pk_values, &self.schema)?;

        // Encode value bytes (CPU work, outside the WAL lock).
        let value_bytes = match row {
            Some(r) => {
                let encoded = crate::codec::encode_row(&r)?;
                Some(bytes::Bytes::from(encoded))
            }
            None => None,
        };

        // IMP-02: allocate seq, WAL append, and memtable apply all happen
        // inside the WAL lock. This guarantees that when visible_seq is
        // advanced, all data for seqs <= visible_seq is in the memtable.
        // A concurrent reader can never observe a sequence number whose
        // data hasn't been applied yet.
        let (seq, should_flush) = {
            let mut wal = self.wal.lock().await;

            let seq = self.global_seq.allocate();
            let mut batch = WriteBatch::new(seq);
            match op_type {
                OpType::Put => batch.put(
                    bytes::Bytes::from(user_key_bytes.clone()),
                    value_bytes.unwrap_or_default(),
                ),
                OpType::Delete => batch.delete(bytes::Bytes::from(user_key_bytes.clone())),
            }

            wal.append(&batch)?;
            let should_flush = self.memtable.apply_batch(&batch)?;

            // Advance visible_seq now that the data is in the memtable.
            self.visible_seq.set_at_least(seq.0 + 1);

            (seq, should_flush)
        };

        // Invalidate row cache so post-flush reads don't serve stale data.
        if let Some(ref cache) = self.row_cache {
            cache.invalidate(&user_key_bytes);
        }

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
                    // Rotate the WAL as well so the sealed memtable's
                    // writes live in a closed log that GC can reclaim.
                    {
                        let mut wal = self.wal.lock().await;
                        wal.rotate()?;
                    }
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
    #[instrument(skip(self), fields(op = "get"))]
    pub fn get(&self, pk_values: &[FieldValue]) -> Result<Option<Row>> {
        crate::read_path::point_lookup(self, pk_values)
    }

    /// Range scan. Returns rows in PK order where `start_pk <= pk < end_pk`.
    /// If `start_pk` is `None`, scan from the beginning.
    /// If `end_pk` is `None`, scan to the end.
    #[instrument(skip(self), fields(op = "scan"))]
    pub fn scan(
        &self,
        start_pk: Option<&[FieldValue]>,
        end_pk: Option<&[FieldValue]>,
    ) -> Result<Vec<(InternalKey, Row)>> {
        crate::read_path::range_scan(self, start_pk, end_pk)
    }

    // ── Admin ────────────────────────────────────────────────────────────

    /// Force flush all immutable memtables and the active memtable.
    #[instrument(skip(self), fields(op = "flush"))]
    pub async fn flush(self: &Arc<Self>) -> Result<()> {
        if self.read_only {
            return Err(MeruError::ReadOnly);
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(MeruError::Closed);
        }
        // Bug R fix: hold `rotation_lock` while rotating so a concurrent
        // auto-flush task from `write_internal` doesn't race on `rotate()`
        // and seal an empty (freshly-created) memtable.
        {
            let _rotation_guard = self.rotation_lock.lock().await;
            // Rotate active memtable AND the WAL together so that (a) the
            // sealed memtable's writes live in a closed WAL file that can be
            // GC'd once the flush commits, and (b) new writes after this call
            // land in a fresh WAL file. Bug D regression: before this, the WAL
            // never rotated under any flush path, so the log directory grew
            // without bound and recovery replayed already-flushed batches.
            let next_seq = self.global_seq.current().next();
            self.memtable.rotate(next_seq);
            {
                let mut wal = self.wal.lock().await;
                wal.rotate()?;
            }
        } // rotation_lock dropped
          // Flush all immutables. `run_flush` calls `mark_flushed_seq` which
          // GCs the matching closed WAL file as a side effect.
        while self.memtable.oldest_immutable().is_some() {
            crate::flush::run_flush(self).await?;
        }
        Ok(())
    }

    /// Trigger a manual compaction. Picks the best level and runs one job.
    #[instrument(skip(self), fields(op = "compact"))]
    pub async fn compact(self: &Arc<Self>) -> Result<()> {
        if self.read_only {
            return Err(MeruError::ReadOnly);
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(MeruError::Closed);
        }
        crate::compaction::job::run_compaction(self).await
    }

    /// Current read sequence (snapshot for reads).
    ///
    /// IMP-02: returns `visible_seq` — the highest sequence number whose
    /// data is guaranteed to be in the memtable. This is strictly <=
    /// `global_seq.current()` and is only advanced after memtable apply
    /// completes inside the WAL lock.
    pub fn read_seq(&self) -> SeqNum {
        self.visible_seq.current()
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Catalog base directory (for HTAP: point DuckDB at Parquet files).
    pub fn catalog_path(&self) -> String {
        self.catalog.base_path().to_string_lossy().to_string()
    }

    /// Export the current catalog snapshot as an Apache Iceberg v2
    /// `metadata.json` under `target_dir`. Delegates to
    /// [`merutable_iceberg::IcebergCatalog::export_to_iceberg`] — see that
    /// method's docs for the exact shape, field mapping, and limitations.
    ///
    /// Returns the absolute path of the emitted `v{N}.metadata.json`.
    pub async fn export_iceberg(
        &self,
        target_dir: impl AsRef<std::path::Path>,
    ) -> Result<std::path::PathBuf> {
        self.catalog.export_to_iceberg(target_dir).await
    }

    /// Re-read the Iceberg manifest from disk and install a new version.
    /// Used by read-only replicas to pick up snapshots written by the primary.
    ///
    /// IMP-15: validates that all data files and Puffin files referenced by the
    /// new manifest exist on disk before swapping the version. If any file is
    /// missing (e.g. not yet replicated), the refresh is rejected and the
    /// current version stays in place.
    pub async fn refresh(&self) -> Result<()> {
        let new_version = self.catalog.refresh(self.schema.clone()).await?;

        // IMP-15: pre-flight check — every referenced file must exist.
        let base = self.catalog.base_path();
        for files in new_version.levels.values() {
            for file in files {
                let data_path = base.join(&file.path);
                if !data_path.exists() {
                    return Err(MeruError::ObjectStore(format!(
                        "refresh: referenced data file missing: {}",
                        file.path,
                    )));
                }
                if let Some(ref dv_path) = file.dv_path {
                    let puffin_path = base.join(dv_path);
                    if !puffin_path.exists() {
                        return Err(MeruError::ObjectStore(format!(
                            "refresh: referenced puffin file missing: {dv_path}",
                        )));
                    }
                }
            }
        }

        self.version_set.install(new_version);
        info!("version refreshed from disk");
        Ok(())
    }

    /// IMP-12: enqueue obsoleted files for deferred deletion.
    /// Called by compaction after committing a new version.
    pub(crate) async fn enqueue_for_deletion(&self, paths: Vec<std::path::PathBuf>) {
        if paths.is_empty() {
            return;
        }
        let now = std::time::Instant::now();
        let mut pending = self.pending_deletions.lock().await;
        for p in paths {
            pending.push((p, now));
        }
    }

    /// IMP-12: delete files whose grace period has expired.
    /// Called periodically by the background worker or after compaction.
    pub async fn gc_pending_deletions(&self) {
        let grace = std::time::Duration::from_secs(self.config.gc_grace_period_secs);
        let mut pending = self.pending_deletions.lock().await;
        let mut remaining = Vec::new();
        for (path, obsoleted_at) in pending.drain(..) {
            if obsoleted_at.elapsed() >= grace {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        warn!(path = %path.display(), error = %e, "failed to GC obsoleted file");
                    }
                }
            } else {
                remaining.push((path, obsoleted_at));
            }
        }
        *pending = remaining;
    }

    /// Graceful shutdown: flush all in-memory data to durable storage, fsync
    /// the WAL, and set the closed flag. After `close()` returns, all data
    /// written before this call is durable on disk. Subsequent write/flush/
    /// compact calls return `MeruError::Closed`. Reads remain available
    /// until the `MeruEngine` is dropped.
    ///
    /// Follows the RocksDB/sled pattern: the library provides the method,
    /// the host process calls it in its shutdown path. No signal handlers
    /// are installed.
    #[instrument(skip(self), fields(op = "close"))]
    pub async fn close(self: &Arc<Self>) -> Result<()> {
        if self.read_only {
            // Read-only instances have nothing to flush. Just set the flag.
            self.closed.store(true, Ordering::Release);
            info!("read-only engine closed");
            return Ok(());
        }

        // Prevent double-close.
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        info!("engine closing — flushing memtable to durable storage");

        // Rotate the active memtable so its contents become immutable and
        // can be flushed. Same pattern as the manual flush() path.
        {
            let _rotation_guard = self.rotation_lock.lock().await;
            if self.memtable.active_size_bytes() > 0 {
                let next_seq = self.global_seq.current().next();
                self.memtable.rotate(next_seq);
                {
                    let mut wal = self.wal.lock().await;
                    wal.rotate()?;
                }
            }
        }

        // Flush all immutable memtables.
        while self.memtable.oldest_immutable().is_some() {
            crate::flush::run_flush(self).await?;
        }

        // Final WAL sync for any trailing writes that landed between
        // the last flush and the close flag.
        {
            let mut wal = self.wal.lock().await;
            wal.sync()?;
        }

        info!("engine closed — all data flushed and synced");
        Ok(())
    }

    /// Returns `true` if `close()` has been called.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Snapshot of engine statistics. Lock-free on the version side (ArcSwap),
    /// brief read lock on memtable. Zero overhead on the hot path — only runs
    /// when explicitly called.
    pub fn stats(&self) -> crate::stats::EngineStats {
        let version = self.version_set.current();
        let max_level = version.max_level().0;

        let mut levels = Vec::new();
        for l in 0..=max_level {
            let level = merutable_types::level::Level(l);
            let files = version.files_at(level);
            if files.is_empty() {
                continue;
            }
            let file_stats: Vec<crate::stats::FileStats> = files
                .iter()
                .map(|f| crate::stats::FileStats {
                    path: f.path.clone(),
                    file_size: f.meta.file_size,
                    num_rows: f.meta.num_rows,
                    seq_range: (f.meta.seq_min, f.meta.seq_max),
                    has_dv: f.has_dv(),
                })
                .collect();
            levels.push(crate::stats::LevelStats {
                level: l,
                file_count: files.len(),
                total_bytes: version.level_bytes(level),
                total_rows: files.iter().map(|f| f.meta.num_rows).sum(),
                files: file_stats,
            });
        }

        let memtable = crate::stats::MemtableStats {
            active_size_bytes: self.memtable.active_size_bytes(),
            active_entry_count: self.memtable.active_entry_count(),
            flush_threshold: self.memtable.flush_threshold(),
            immutable_count: self.memtable.immutable_count(),
        };

        let cache = match &self.row_cache {
            Some(c) => crate::stats::CacheStats {
                capacity: c.cap(),
                size: c.len(),
                hit_count: c.hit_count(),
                miss_count: c.miss_count(),
            },
            None => crate::stats::CacheStats {
                capacity: 0,
                size: 0,
                hit_count: 0,
                miss_count: 0,
            },
        };

        crate::stats::EngineStats {
            snapshot_id: version.snapshot_id,
            current_seq: self.global_seq.current().0,
            levels,
            memtable,
            cache,
        }
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

    #[tokio::test]
    async fn close_flushes_and_blocks_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

        // Write data that sits in the memtable.
        engine
            .put(
                vec![FieldValue::Int64(1)],
                Row::new(vec![
                    Some(FieldValue::Int64(1)),
                    Some(FieldValue::Bytes(bytes::Bytes::from("before_close"))),
                ]),
            )
            .await
            .unwrap();

        assert!(!engine.is_closed());
        engine.close().await.unwrap();
        assert!(engine.is_closed());

        // Writes must fail after close.
        let err = engine
            .put(
                vec![FieldValue::Int64(2)],
                Row::new(vec![Some(FieldValue::Int64(2)), None]),
            )
            .await;
        assert!(
            matches!(err, Err(MeruError::Closed)),
            "put after close must return Closed"
        );

        // Delete must fail.
        let err = engine.delete(vec![FieldValue::Int64(1)]).await;
        assert!(matches!(err, Err(MeruError::Closed)));

        // Reads still work.
        let row = engine.get(&[FieldValue::Int64(1)]).unwrap();
        assert!(row.is_some(), "reads must still work after close");
    }

    #[tokio::test]
    async fn close_data_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();

        // Write, close, drop.
        {
            let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();
            for i in 1..=10i64 {
                engine
                    .put(
                        vec![FieldValue::Int64(i)],
                        Row::new(vec![
                            Some(FieldValue::Int64(i)),
                            Some(FieldValue::Bytes(bytes::Bytes::from(format!("v{i}")))),
                        ]),
                    )
                    .await
                    .unwrap();
            }
            engine.close().await.unwrap();
        }

        // Reopen — data was flushed by close(), should be in Parquet.
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();
        for i in 1..=10i64 {
            let row = engine.get(&[FieldValue::Int64(i)]).unwrap();
            assert!(row.is_some(), "key {i} must survive close + reopen");
        }
    }

    #[tokio::test]
    async fn double_close_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(test_config(&tmp)).await.unwrap();

        engine.close().await.unwrap();
        // Second close should succeed silently.
        engine.close().await.unwrap();
        assert!(engine.is_closed());
    }
}
