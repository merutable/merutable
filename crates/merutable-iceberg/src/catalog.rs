//! `IcebergCatalog`: file-system-based Iceberg catalog for embedded use.
//!
//! Layout on disk:
//! ```text
//! {base_path}/
//! ├── metadata/
//! │   ├── v1.metadata.json      # manifest snapshots
//! │   ├── v2.metadata.json
//! │   └── ...
//! ├── data/
//! │   ├── L0/
//! │   │   ├── {uuid}.parquet
//! │   │   └── {uuid}.dv-{snap}.puffin
//! │   ├── L1/
//! │   │   └── ...
//! │   └── ...
//! └── version-hint.text         # current metadata version pointer
//! ```
//!
//! This is a file-system catalog that writes merutable's **native manifest
//! format** — a JSON superset of Iceberg's `TableMetadata` — rather than
//! Iceberg's on-wire format (`TableMetadata` JSON + Avro manifest-list +
//! Avro manifest files). The native format is chosen for efficiency: one
//! fsyncable JSON file per commit instead of four, no Avro dependency on
//! the hot path.
//!
//! The on-disk layout is **losslessly translatable** to a real Apache
//! Iceberg v2 table via [`crate::translate`]. The translator can be run
//! online (to expose the catalog to pyiceberg / Spark / Trino / DuckDB /
//! Snowflake) or offline (as a one-shot migration). See the translate
//! module's docs for the mapping from merutable fields to Iceberg spec
//! fields.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use merutable_types::{level::Level, schema::TableSchema, MeruError, Result};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::{
    deletion_vector::DeletionVector,
    manifest::{DvLocation, Manifest},
    snapshot::SnapshotTransaction,
    version::Version,
};

// ── IcebergCatalog ───────────────────────────────────────────────────────────

/// File-system Iceberg catalog. Manages manifest JSON files and the
/// version pointer. Thread-safe: commit serialized via `Mutex`.
pub struct IcebergCatalog {
    base_path: PathBuf,
    /// Current manifest (the source of truth on disk).
    current: Mutex<Manifest>,
    /// Next metadata version number.
    next_version: Mutex<i64>,
}

impl IcebergCatalog {
    /// Open or create a catalog at `base_path`.
    /// If the directory already has metadata, loads the latest manifest.
    /// Otherwise, creates an empty initial manifest.
    pub async fn open(base_path: impl AsRef<Path>, schema: TableSchema) -> Result<Self> {
        let base = base_path.as_ref().to_path_buf();

        // Ensure directory structure exists.
        let metadata_dir = base.join("metadata");
        let data_dir = base.join("data");
        tokio::fs::create_dir_all(&metadata_dir)
            .await
            .map_err(MeruError::Io)?;
        tokio::fs::create_dir_all(&data_dir)
            .await
            .map_err(MeruError::Io)?;

        // Crash-recovery housekeeping: remove any leftover
        // `version-hint.text.tmp` from a commit that crashed after the
        // write but before the rename. Leaving it around is harmless
        // (it's never read), but cleaning up keeps the base dir tidy
        // and prevents ambiguity for human operators.
        let tmp_hint = base.join("version-hint.text.tmp");
        if tmp_hint.exists() {
            let _ = tokio::fs::remove_file(&tmp_hint).await;
        }

        // Try to load existing manifest from version-hint.
        let hint_path = base.join("version-hint.text");
        let (manifest, next_ver) = if hint_path.exists() {
            let hint = tokio::fs::read_to_string(&hint_path)
                .await
                .map_err(MeruError::Io)?;
            let ver: i64 = hint
                .trim()
                .parse()
                .map_err(|_| MeruError::Corruption("bad version-hint".into()))?;
            let meta_path = metadata_dir.join(format!("v{ver}.metadata.json"));
            let data = tokio::fs::read(&meta_path).await.map_err(MeruError::Io)?;
            let mut manifest = Manifest::from_json(&data)?;
            // Legacy-upgrade path: manifests written by pre-Iceberg-enrichment
            // merutable carry neither a `table_uuid` nor `last_updated_ms`.
            // Mint the uuid here — `apply()` will carry it forward to every
            // future snapshot. `last_updated_ms` stays at whatever the old
            // manifest had (0 by default) until the next commit stamps a
            // real clock.
            if manifest.table_uuid.is_empty() {
                manifest.table_uuid = uuid::Uuid::new_v4().to_string();
            }
            (manifest, ver + 1)
        } else {
            // No version-hint.text. Detect silent data loss: if the
            // metadata/ directory already contains snapshot files, this
            // means version-hint was lost (manual deletion, bad restore,
            // filesystem corruption) and initializing to an empty
            // manifest would orphan the existing snapshots. Error out
            // so the operator can recover explicitly rather than
            // silently clobbering data.
            let mut has_existing_metadata = false;
            if let Ok(mut entries) = tokio::fs::read_dir(&metadata_dir).await {
                while let Ok(Some(e)) = entries.next_entry().await {
                    let name = e.file_name();
                    let n = name.to_string_lossy();
                    if n.starts_with('v') && n.ends_with(".metadata.json") {
                        has_existing_metadata = true;
                        break;
                    }
                }
            }
            if has_existing_metadata {
                return Err(MeruError::Corruption(format!(
                    "version-hint.text is missing but {}/ contains snapshot \
                     metadata files — refusing to initialize a fresh catalog \
                     over existing data. Restore version-hint.text from backup \
                     or move the existing metadata/ directory aside.",
                    metadata_dir.display(),
                )));
            }
            (Manifest::empty(schema), 1)
        };

        info!(base_path = %base.display(), "catalog opened");
        Ok(Self {
            base_path: base,
            current: Mutex::new(manifest),
            next_version: Mutex::new(next_ver),
        })
    }

    /// Commit a `SnapshotTransaction`. This is the linearization point
    /// for every flush and compaction.
    ///
    /// 1. Upload DV puffin files for partial compactions.
    /// 2. Apply transaction to current manifest → new manifest.
    /// 3. Write new manifest JSON to `metadata/v{N}.metadata.json`.
    /// 4. Atomically update `version-hint.text`.
    /// 5. Return the new `Version`.
    pub async fn commit(
        &self,
        txn: &SnapshotTransaction,
        schema: Arc<TableSchema>,
    ) -> Result<Version> {
        let mut current = self.current.lock().await;
        let mut next_ver = self.next_version.lock().await;

        let new_snapshot_id = *next_ver;
        debug!(snapshot_id = new_snapshot_id, "committing snapshot");

        // IMP-06: track puffin files written during this commit so we can
        // delete them if any later step fails (prevents orphaned blobs).
        let mut pending_puffin_files: Vec<std::path::PathBuf> = Vec::new();

        // Upload puffin files for DV updates AND record their real
        // on-storage blob coordinates so the manifest can point at the
        // exact byte range of each roaring-bitmap blob. Skipping this
        // step was a real bug: the manifest used to stamp (0, 0)
        // placeholders and every deleted row reappeared on reload.
        let mut dv_locations: HashMap<String, DvLocation> = HashMap::new();
        for (parquet_path, new_dv) in &txn.dvs {
            // If the file already has a DV from a prior partial compaction,
            // load it and union with the new DV. Without this, the second
            // partial compaction's DV replaces the first and rows deleted
            // in the first compaction silently reappear.
            let merged_dv = match current
                .entries
                .iter()
                .find(|e| e.status != "deleted" && e.path == *parquet_path)
            {
                Some(entry)
                    if entry.dv_path.is_some()
                        && entry.dv_offset.is_some()
                        && entry.dv_length.is_some() =>
                {
                    let existing_puffin_path = self.base_path.join(entry.dv_path.as_ref().unwrap());
                    let puffin_data = tokio::fs::read(&existing_puffin_path)
                        .await
                        .map_err(MeruError::Io)?;
                    let offset_raw = entry.dv_offset.unwrap();
                    let length_raw = entry.dv_length.unwrap();
                    if offset_raw < 0 || length_raw < 0 {
                        return Err(MeruError::Corruption(format!(
                            "existing DV for '{}' has negative offset ({offset_raw}) or length ({length_raw})",
                            parquet_path,
                        )));
                    }
                    let offset = offset_raw as usize;
                    let length = length_raw as usize;
                    let end = offset.checked_add(length).ok_or_else(|| {
                        MeruError::Corruption(format!(
                            "existing DV for '{}' offset {offset} + length {length} overflows usize",
                            parquet_path,
                        ))
                    })?;
                    if end > puffin_data.len() {
                        return Err(MeruError::Corruption(format!(
                            "existing DV blob for '{}' at offset {offset} length {length} \
                             exceeds puffin file size {}",
                            parquet_path,
                            puffin_data.len()
                        )));
                    }
                    let existing_dv =
                        DeletionVector::from_puffin_blob(&puffin_data[offset..offset + length])?;
                    let existing_card = existing_dv.cardinality();
                    let new_card = new_dv.cardinality();
                    let mut merged = existing_dv;
                    merged.union_with(new_dv);

                    // IMP-17: a union can never shrink — if it does, the
                    // bitmap library dropped bits and deleted rows will
                    // silently reappear.
                    let merged_card = merged.cardinality();
                    let min_expected = existing_card.max(new_card);
                    if merged_card < min_expected {
                        return Err(MeruError::Corruption(format!(
                            "DV union for '{}' shrank: existing={existing_card} new={new_card} \
                             merged={merged_card}",
                            parquet_path,
                        )));
                    }

                    merged
                }
                _ => new_dv.clone(),
            };

            let encoded =
                merged_dv.encode_puffin(parquet_path, new_snapshot_id, new_snapshot_id)?;
            let puffin_filename = format!(
                "{}.dv-{new_snapshot_id}.puffin",
                Path::new(parquet_path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
            );
            // Determine the level directory from the parquet path.
            let level_dir = Path::new(parquet_path)
                .parent()
                .unwrap_or(Path::new("data/L0"));
            let rel_puffin_path = level_dir.join(&puffin_filename);
            let abs_puffin_path = self.base_path.join(&rel_puffin_path);
            // Ensure parent dir exists.
            if let Some(parent) = abs_puffin_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(MeruError::Io)?;
            }
            tokio::fs::write(&abs_puffin_path, &encoded.bytes)
                .await
                .map_err(MeruError::Io)?;
            pending_puffin_files.push(abs_puffin_path.clone());
            // fsync the puffin file so deleted-row bitmaps survive a crash.
            tokio::fs::File::open(&abs_puffin_path)
                .await
                .map_err(MeruError::Io)?
                .sync_all()
                .await
                .map_err(MeruError::Io)?;
            // fsync the parent directory so the new directory entry is durable.
            if let Some(parent) = abs_puffin_path.parent() {
                tokio::fs::File::open(parent)
                    .await
                    .map_err(MeruError::Io)?
                    .sync_all()
                    .await
                    .map_err(MeruError::Io)?;
            }

            dv_locations.insert(
                parquet_path.clone(),
                DvLocation {
                    dv_path: rel_puffin_path.to_string_lossy().into_owned(),
                    dv_offset: encoded.blob_offset,
                    dv_length: encoded.blob_length,
                },
            );
        }

        // Apply transaction to produce new manifest. `apply` validates
        // that every DV in `txn.dvs` has a matching entry in
        // `dv_locations`; it errors out if the caller forgot any, so
        // the zero-placeholder bug cannot recur silently.
        //
        // IMP-06: everything below is wrapped so that on any error the
        // puffin files written above are cleaned up (best-effort).
        let result: Result<Version> = async {
            let new_manifest = current.apply(txn, new_snapshot_id, &dv_locations)?;

            // Write manifest JSON.
            let meta_path = self
                .base_path
                .join("metadata")
                .join(format!("v{new_snapshot_id}.metadata.json"));
            let json = new_manifest.to_json()?;
            tokio::fs::write(&meta_path, &json)
                .await
                .map_err(MeruError::Io)?;
            // fsync the metadata JSON so it is complete before the version-hint
            // points at it. Without this, a crash can leave a truncated file.
            tokio::fs::File::open(&meta_path)
                .await
                .map_err(MeruError::Io)?
                .sync_all()
                .await
                .map_err(MeruError::Io)?;
            // fsync the metadata DIRECTORY so the new metadata.json's
            // directory entry is durably linked BEFORE the version-hint
            // points at it. Without this, a crash between the version-
            // hint rename and the metadata dir fsync can leave
            // version-hint pointing at a filename that isn't yet in the
            // directory listing (ext4/btrfs journal it separately), and
            // recovery fails with "metadata.json not found".
            let metadata_dir = self.base_path.join("metadata");
            tokio::fs::File::open(&metadata_dir)
                .await
                .map_err(MeruError::Io)?
                .sync_all()
                .await
                .map_err(MeruError::Io)?;

            // Update version hint (atomic: write tmp + rename).
            let hint_path = self.base_path.join("version-hint.text");
            let tmp_hint = self.base_path.join("version-hint.text.tmp");
            tokio::fs::write(&tmp_hint, new_snapshot_id.to_string())
                .await
                .map_err(MeruError::Io)?;
            // fsync the tmp hint file before rename so the content is durable.
            tokio::fs::File::open(&tmp_hint)
                .await
                .map_err(MeruError::Io)?
                .sync_all()
                .await
                .map_err(MeruError::Io)?;
            tokio::fs::rename(&tmp_hint, &hint_path)
                .await
                .map_err(MeruError::Io)?;
            // fsync the base directory so the version-hint rename is
            // durably linked — without this, the rename can "roll back"
            // on a crash and readers see the old version.
            tokio::fs::File::open(&self.base_path)
                .await
                .map_err(MeruError::Io)?
                .sync_all()
                .await
                .map_err(MeruError::Io)?;

            // Build Version from the new manifest.
            let version = new_manifest.to_version(schema);

            // Update in-memory state.
            *current = new_manifest;
            *next_ver = new_snapshot_id + 1;

            Ok(version)
        }
        .await;

        // IMP-06: on commit failure, best-effort cleanup of orphaned puffin
        // files that were written but never referenced by a manifest.
        if result.is_err() {
            for puffin_path in &pending_puffin_files {
                let _ = tokio::fs::remove_file(puffin_path).await;
            }
        }

        result
    }

    /// Re-read the current manifest from disk. Used by read-only replicas
    /// to pick up new snapshots written by the primary.
    ///
    /// Reads `version-hint.text` -> loads the corresponding `v{N}.metadata.json`
    /// -> updates the in-memory manifest. Returns the new `Version`.
    pub async fn refresh(&self, schema: Arc<TableSchema>) -> Result<Version> {
        let hint_path = self.base_path.join("version-hint.text");
        let hint = tokio::fs::read_to_string(&hint_path)
            .await
            .map_err(MeruError::Io)?;
        let ver: i64 = hint
            .trim()
            .parse()
            .map_err(|_| MeruError::Corruption("bad version-hint on refresh".into()))?;

        let meta_path = self
            .base_path
            .join("metadata")
            .join(format!("v{ver}.metadata.json"));
        let data = tokio::fs::read(&meta_path).await.map_err(MeruError::Io)?;
        let manifest = Manifest::from_json(&data)?;
        let version = manifest.to_version(schema);

        let mut current = self.current.lock().await;
        *current = manifest;
        let mut next_ver = self.next_version.lock().await;
        *next_ver = ver + 1;

        Ok(version)
    }

    /// Get the current manifest (for inspection/debugging).
    pub async fn current_manifest(&self) -> Manifest {
        self.current.lock().await.clone()
    }

    /// Get the base path.
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    /// Construct the data file path for a new Parquet file.
    pub fn data_file_path(&self, level: Level, file_id: &str) -> PathBuf {
        self.base_path
            .join("data")
            .join(format!("L{}", level.0))
            .join(format!("{file_id}.parquet"))
    }

    /// Ensure the data directory for a level exists.
    pub async fn ensure_level_dir(&self, level: Level) -> Result<()> {
        let dir = self.base_path.join("data").join(format!("L{}", level.0));
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(MeruError::Io)?;
        Ok(())
    }

    /// Export the current catalog snapshot as an Apache Iceberg v2
    /// `metadata.json` file.
    ///
    /// This is the **enabler artifact** for Iceberg interop: it projects
    /// the in-memory `Manifest` onto the Iceberg v2 `TableMetadata` shape
    /// (see [`crate::translate`]) and writes the result to
    /// `{target_dir}/metadata/v{N}.metadata.json` alongside a
    /// `version-hint.text` file pointing at it.
    ///
    /// # Use cases
    ///
    /// - **Register with an external catalog.** After calling this,
    ///   a pyiceberg / Spark / Trino / DuckDB / Snowflake / Athena
    ///   client can load the exported directory as an Iceberg v2 table
    ///   (read-only; data files live under the original `base_path`).
    /// - **Lineage / audit.** Diff two exported metadata.json files
    ///   across snapshots to see schema evolution, snapshot history,
    ///   and sequence-number progression in Iceberg-spec terms.
    /// - **One-shot migration.** Call once at end-of-life to hand the
    ///   table over to a different stack without re-writing the data.
    ///
    /// # Current limitation
    ///
    /// This writes the `TableMetadata` JSON but **not** the accompanying
    /// manifest-list Avro or manifest Avro files that a full Iceberg
    /// table requires for data-file discovery. A v2 reader that tries
    /// to list files will follow the `manifest-list` path referenced in
    /// the exported snapshot and fail to find the Avro file.
    ///
    /// For inspection-only workflows (catalog registration, lineage,
    /// schema audit), this JSON is sufficient. For full data-read
    /// interop, use the exported metadata as a starting point and emit
    /// the Avro manifest files separately (tracked as follow-on work;
    /// see `translate.rs` for the field mapping helpers).
    pub async fn export_to_iceberg(&self, target_dir: impl AsRef<Path>) -> Result<PathBuf> {
        let target = target_dir.as_ref().to_path_buf();
        let meta_dir = target.join("metadata");
        tokio::fs::create_dir_all(&meta_dir)
            .await
            .map_err(MeruError::Io)?;

        // Snapshot the current manifest under the lock, then release it
        // so the projection work runs off the critical path.
        let manifest = {
            let guard = self.current.lock().await;
            guard.clone()
        };

        // Use an absolute `file://` URI so downstream Iceberg readers
        // resolve the table location consistently regardless of cwd.
        let canonical = tokio::fs::canonicalize(&self.base_path)
            .await
            .unwrap_or_else(|_| self.base_path.clone());
        let table_location = format!("file://{}", canonical.display());

        let json =
            crate::translate::to_iceberg_v2_table_metadata_bytes(&manifest, &table_location)?;

        let out_path = meta_dir.join(format!("v{}.metadata.json", manifest.snapshot_id));
        tokio::fs::write(&out_path, &json)
            .await
            .map_err(MeruError::Io)?;
        // fsync before updating version-hint.
        tokio::fs::File::open(&out_path)
            .await
            .map_err(MeruError::Io)?
            .sync_all()
            .await
            .map_err(MeruError::Io)?;

        let hint_path = target.join("version-hint.text");
        tokio::fs::write(&hint_path, manifest.snapshot_id.to_string())
            .await
            .map_err(MeruError::Io)?;
        tokio::fs::File::open(&hint_path)
            .await
            .map_err(MeruError::Io)?
            .sync_all()
            .await
            .map_err(MeruError::Io)?;

        info!(
            target = %target.display(),
            snapshot_id = manifest.snapshot_id,
            table_uuid = %manifest.table_uuid,
            "exported Iceberg v2 metadata"
        );
        Ok(out_path)
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deletion_vector::DeletionVector;
    use crate::snapshot::IcebergDataFile;
    use merutable_types::{
        level::{Level, ParquetFileMeta},
        schema::{ColumnDef, ColumnType},
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

    fn test_meta(level: u8) -> ParquetFileMeta {
        ParquetFileMeta {
            level: Level(level),
            seq_min: 1,
            seq_max: 10,
            key_min: vec![0x01],
            key_max: vec![0xFF],
            num_rows: 100,
            file_size: 1024,
            dv_path: None,
            dv_offset: None,
            dv_length: None,
            format: None,
        }
    }

    #[tokio::test]
    async fn open_creates_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let _catalog = IcebergCatalog::open(tmp.path(), test_schema())
            .await
            .unwrap();
        assert!(tmp.path().join("metadata").exists());
        assert!(tmp.path().join("data").exists());
    }

    #[tokio::test]
    async fn commit_flush_and_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema());

        // Open, commit a flush.
        let catalog = IcebergCatalog::open(tmp.path(), test_schema())
            .await
            .unwrap();
        let mut txn = SnapshotTransaction::new();
        txn.add_file(IcebergDataFile {
            path: "data/L0/abc.parquet".into(),
            file_size: 1024,
            num_rows: 100,
            meta: test_meta(0),
        });
        txn.set_prop("merutable.job", "flush");
        let v = catalog.commit(&txn, schema.clone()).await.unwrap();
        assert_eq!(v.snapshot_id, 1);
        assert_eq!(v.l0_file_count(), 1);

        // Reopen from disk.
        let catalog2 = IcebergCatalog::open(tmp.path(), test_schema())
            .await
            .unwrap();
        let m = catalog2.current_manifest().await;
        assert_eq!(m.snapshot_id, 1);
        assert_eq!(m.live_file_count(), 1);
    }

    #[tokio::test]
    async fn multiple_commits() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema());
        let catalog = IcebergCatalog::open(tmp.path(), test_schema())
            .await
            .unwrap();

        // Commit 1: flush file A.
        let mut txn1 = SnapshotTransaction::new();
        txn1.add_file(IcebergDataFile {
            path: "data/L0/a.parquet".into(),
            file_size: 1024,
            num_rows: 100,
            meta: test_meta(0),
        });
        let v1 = catalog.commit(&txn1, schema.clone()).await.unwrap();
        assert_eq!(v1.snapshot_id, 1);

        // Commit 2: flush file B.
        let mut txn2 = SnapshotTransaction::new();
        txn2.add_file(IcebergDataFile {
            path: "data/L0/b.parquet".into(),
            file_size: 2048,
            num_rows: 200,
            meta: {
                let mut m = test_meta(0);
                m.seq_min = 11;
                m.seq_max = 20;
                m
            },
        });
        let v2 = catalog.commit(&txn2, schema.clone()).await.unwrap();
        assert_eq!(v2.snapshot_id, 2);
        assert_eq!(v2.l0_file_count(), 2);

        // Commit 3: compact both into L1.
        let mut txn3 = SnapshotTransaction::new();
        txn3.remove_file("data/L0/a.parquet".into());
        txn3.remove_file("data/L0/b.parquet".into());
        txn3.add_file(IcebergDataFile {
            path: "data/L1/merged.parquet".into(),
            file_size: 3072,
            num_rows: 300,
            meta: test_meta(1),
        });
        let v3 = catalog.commit(&txn3, schema.clone()).await.unwrap();
        assert_eq!(v3.snapshot_id, 3);
        assert_eq!(v3.l0_file_count(), 0);
        assert_eq!(v3.files_at(Level(1)).len(), 1);
    }

    #[tokio::test]
    async fn commit_with_dv() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema());
        let catalog = IcebergCatalog::open(tmp.path(), test_schema())
            .await
            .unwrap();

        // Flush a file.
        let mut txn1 = SnapshotTransaction::new();
        txn1.add_file(IcebergDataFile {
            path: "data/L0/a.parquet".into(),
            file_size: 1024,
            num_rows: 100,
            meta: test_meta(0),
        });
        catalog.commit(&txn1, schema.clone()).await.unwrap();

        // Partial compaction — promote some rows, add DV to source.
        let mut txn2 = SnapshotTransaction::new();
        let mut dv = DeletionVector::new();
        for i in 0..50u32 {
            dv.mark_deleted(i);
        }
        txn2.add_dv("data/L0/a.parquet".into(), dv);
        txn2.add_file(IcebergDataFile {
            path: "data/L1/promoted.parquet".into(),
            file_size: 512,
            num_rows: 50,
            meta: test_meta(1),
        });

        let v = catalog.commit(&txn2, schema.clone()).await.unwrap();
        assert_eq!(v.l0_file_count(), 1); // L0 file still present
        let l0 = &v.files_at(Level(0))[0];
        assert!(l0.dv_path.is_some()); // but now has a DV
        assert_eq!(v.files_at(Level(1)).len(), 1);
    }

    /// Regression test: two successive partial compactions on the same
    /// file must produce a DV that is the union of both. Before the fix,
    /// the second DV replaced the first and rows deleted in the first
    /// partial compaction silently reappeared.
    #[tokio::test]
    async fn successive_dv_updates_are_unioned() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema());
        let catalog = IcebergCatalog::open(tmp.path(), test_schema())
            .await
            .unwrap();

        // Flush a file with 100 rows.
        let mut txn1 = SnapshotTransaction::new();
        txn1.add_file(IcebergDataFile {
            path: "data/L0/a.parquet".into(),
            file_size: 1024,
            num_rows: 100,
            meta: test_meta(0),
        });
        catalog.commit(&txn1, schema.clone()).await.unwrap();

        // First partial compaction: delete rows 0..10.
        let mut txn2 = SnapshotTransaction::new();
        let mut dv1 = DeletionVector::new();
        for i in 0..10u32 {
            dv1.mark_deleted(i);
        }
        txn2.add_dv("data/L0/a.parquet".into(), dv1);
        txn2.add_file(IcebergDataFile {
            path: "data/L1/batch1.parquet".into(),
            file_size: 256,
            num_rows: 10,
            meta: test_meta(1),
        });
        let v2 = catalog.commit(&txn2, schema.clone()).await.unwrap();
        let l0_after_first = &v2.files_at(Level(0))[0];
        assert!(l0_after_first.dv_path.is_some());

        // Read back the DV puffin written by the first partial compaction
        // to confirm it has exactly 10 deleted rows.
        let puffin_path_1 = tmp.path().join(l0_after_first.dv_path.as_ref().unwrap());
        let puffin_data_1 = tokio::fs::read(&puffin_path_1).await.unwrap();
        let dv_after_first = DeletionVector::from_puffin_bytes(&puffin_data_1).unwrap();
        assert_eq!(dv_after_first.cardinality(), 10);

        // Second partial compaction: delete rows 50..60 (disjoint from
        // the first set). The commit must union with the existing DV.
        let mut txn3 = SnapshotTransaction::new();
        let mut dv2 = DeletionVector::new();
        for i in 50..60u32 {
            dv2.mark_deleted(i);
        }
        txn3.add_dv("data/L0/a.parquet".into(), dv2);
        txn3.add_file(IcebergDataFile {
            path: "data/L1/batch2.parquet".into(),
            file_size: 256,
            num_rows: 10,
            meta: {
                let mut m = test_meta(1);
                m.seq_min = 11;
                m.seq_max = 20;
                m
            },
        });
        let v3 = catalog.commit(&txn3, schema.clone()).await.unwrap();

        // The L0 file must still exist with a DV.
        assert_eq!(v3.l0_file_count(), 1);
        let l0_after_second = &v3.files_at(Level(0))[0];
        assert!(l0_after_second.dv_path.is_some());

        // Read back the DV puffin and verify it is the UNION of both
        // partial compactions: rows 0..10 AND 50..60 = 20 deleted rows.
        let puffin_path_2 = tmp.path().join(l0_after_second.dv_path.as_ref().unwrap());
        let puffin_data_2 = tokio::fs::read(&puffin_path_2).await.unwrap();
        let dv_merged = DeletionVector::from_puffin_bytes(&puffin_data_2).unwrap();
        assert_eq!(
            dv_merged.cardinality(),
            20,
            "DV must be union of both partial compactions (10 + 10 = 20)"
        );
        // Verify specific row positions from both compactions.
        for i in 0..10u32 {
            assert!(
                dv_merged.is_deleted(i),
                "row {i} from first compaction missing"
            );
        }
        for i in 50..60u32 {
            assert!(
                dv_merged.is_deleted(i),
                "row {i} from second compaction missing"
            );
        }
        // Rows outside both ranges must NOT be deleted.
        for i in 10..50u32 {
            assert!(!dv_merged.is_deleted(i), "row {i} should not be deleted");
        }
    }

    /// Every commit must stamp a non-zero `table_uuid`, bump
    /// `sequence_number`, set `parent_snapshot_id`, and roll
    /// `last_updated_ms` forward. These are the fields `crate::translate`
    /// relies on for lossless Iceberg v2 projection.
    #[tokio::test]
    async fn commit_enriches_iceberg_fields() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema());
        let catalog = IcebergCatalog::open(tmp.path(), test_schema())
            .await
            .unwrap();

        // Commit #1.
        let mut txn1 = SnapshotTransaction::new();
        txn1.add_file(IcebergDataFile {
            path: "data/L0/a.parquet".into(),
            file_size: 1024,
            num_rows: 100,
            meta: test_meta(0),
        });
        catalog.commit(&txn1, schema.clone()).await.unwrap();
        let m1 = catalog.current_manifest().await;

        assert!(
            !m1.table_uuid.is_empty(),
            "first commit must mint a table_uuid"
        );
        assert!(m1.last_updated_ms > 0, "last_updated_ms must be set");
        assert_eq!(m1.sequence_number, 1);
        assert_eq!(m1.parent_snapshot_id, None, "first commit has no parent");

        // Commit #2 — must carry uuid, bump sequence, point parent at #1.
        let mut txn2 = SnapshotTransaction::new();
        txn2.add_file(IcebergDataFile {
            path: "data/L0/b.parquet".into(),
            file_size: 2048,
            num_rows: 200,
            meta: {
                let mut m = test_meta(0);
                m.seq_min = 11;
                m.seq_max = 20;
                m
            },
        });
        catalog.commit(&txn2, schema.clone()).await.unwrap();
        let m2 = catalog.current_manifest().await;

        assert_eq!(
            m2.table_uuid, m1.table_uuid,
            "table_uuid must persist across commits"
        );
        assert_eq!(m2.sequence_number, 2);
        assert_eq!(m2.parent_snapshot_id, Some(1));
        assert!(m2.last_updated_ms >= m1.last_updated_ms);
    }

    /// `export_to_iceberg` must produce a `metadata.json` that parses
    /// cleanly with the `iceberg` crate's `TableMetadata` deserializer.
    /// That struct runs every spec validation the crate knows about —
    /// if it accepts the payload, every V2-aware reader (pyiceberg,
    /// Spark, Trino, DuckDB, Snowflake, Athena) will too.
    #[tokio::test]
    async fn export_produces_iceberg_spec_compliant_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let schema_arc = Arc::new(test_schema());
        let catalog = IcebergCatalog::open(tmp.path(), test_schema())
            .await
            .unwrap();

        // Commit a file so the exported snapshot isn't empty.
        let mut txn = SnapshotTransaction::new();
        txn.add_file(IcebergDataFile {
            path: "data/L0/a.parquet".into(),
            file_size: 1024,
            num_rows: 100,
            meta: test_meta(0),
        });
        catalog.commit(&txn, schema_arc.clone()).await.unwrap();

        let target = tempfile::tempdir().unwrap();
        let out = catalog.export_to_iceberg(target.path()).await.unwrap();

        // File exists under metadata/ with the expected name.
        assert!(out.exists());
        assert!(out.starts_with(target.path().join("metadata")));
        assert!(out
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .ends_with(".metadata.json"));

        // version-hint.text must point at the emitted snapshot.
        let hint = tokio::fs::read_to_string(target.path().join("version-hint.text"))
            .await
            .unwrap();
        assert_eq!(hint.trim(), "1");

        // Parse with the iceberg crate to enforce spec compliance.
        let bytes = tokio::fs::read(&out).await.unwrap();
        let parsed: std::result::Result<iceberg::spec::TableMetadata, _> =
            serde_json::from_slice(&bytes);
        assert!(
            parsed.is_ok(),
            "iceberg-rs rejected exported metadata: {:?}\n\nfile: {}\n\ncontent:\n{}",
            parsed.err(),
            out.display(),
            String::from_utf8_lossy(&bytes)
        );
        let tm = parsed.unwrap();
        assert_eq!(tm.last_sequence_number(), 1);
        assert_eq!(tm.current_snapshot_id(), Some(1));
    }
}
