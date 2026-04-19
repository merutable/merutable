//! Issue #26 Phase 3: the `ObjectStoreCatalog` — a first-class catalog
//! that uses conditional-PUT commits against a `MeruStore` backend.
//!
//! Composes:
//!   - Phase 2a: `LocalFileStore::put_if_absent` (or any backend-
//!     native equivalent). Race-safe commit primitive.
//!   - Phase 2b: `head_discovery::discover_head` — O(log HEAD)
//!     probe-based HEAD recovery, no LIST calls.
//!   - Phase 2c: `object_store_commit::commit_with_retry` — retry
//!     loop with exponential backoff on `AlreadyExists`.
//!   - #28 Phase 2: protobuf manifest format, "MRUB" magic + length
//!     framing.
//!
//! # Responsibilities
//!
//! - `open(store, schema)` runs HEAD discovery; loads the current
//!   manifest if any, or emits the genesis v1 manifest on an empty
//!   store.
//! - `commit(txn, schema)` applies the transaction to the current
//!   manifest and commits via `commit_with_retry`. On race-loss
//!   (another writer committed), re-reads the new HEAD and rebuilds
//!   on top before retrying.
//! - `current_manifest()` returns a clone of the last-committed
//!   manifest. `refresh()` re-discovers HEAD and loads the latest.
//!
//! # Not in this phase
//!
//! - Wire-up into `MeruEngine` (flush + compaction call
//!   `catalog.commit`; those paths need a shared commit trait that
//!   both `IcebergCatalog` and `ObjectStoreCatalog` implement).
//!   Phase 4.
//! - Puffin DV file handling. The existing `SnapshotTransaction`
//!   shape includes a DV map; the ObjectStore catalog currently
//!   errors on a non-empty `txn.dvs`. Phase 5.
//! - Manifest GC walking back via `previous_snapshot_id`. Phase 6.

use std::sync::Arc;

use merutable_store::traits::MeruStore;
use merutable_types::{schema::TableSchema, MeruError, Result};
use tokio::sync::Mutex;

use crate::head_discovery::discover_head;
use crate::manifest::{DvLocation, Manifest};
use crate::object_store_commit::manifest_path;
use crate::snapshot::SnapshotTransaction;
use crate::version::Version;

/// ObjectStore-mode catalog. Generic over any `MeruStore` — tests
/// use `LocalFileStore`; S3/GCS/Azure plug in via the
/// `object_store` crate integration (Phase 4b).
pub struct ObjectStoreCatalog<S: MeruStore + ?Sized> {
    store: Arc<S>,
    /// Current in-memory manifest. Updated on every successful
    /// commit + refresh.
    current: Mutex<Manifest>,
    /// Current HEAD version — matches `current.snapshot_id` in the
    /// normal case; held separately so commit_with_retry gets a
    /// cheap integer reference.
    head: Mutex<i64>,
}

impl<S: MeruStore> ObjectStoreCatalog<S> {
    /// Open a catalog against `store`. If the store contains no
    /// manifests, emits the genesis `v1.manifest.bin` on behalf of
    /// the caller so every subsequent read goes through a stable
    /// chain. If the store has existing manifests, runs HEAD
    /// discovery and loads the latest.
    pub async fn open(store: Arc<S>, schema: TableSchema) -> Result<Self> {
        let head = discover_head(|v| {
            let s = store.clone();
            async move { s.exists(&manifest_path(v)).await }
        })
        .await?;

        let (manifest, current_head) = if head == 0 {
            // Empty store — write genesis v1.
            let mut m = Manifest::empty(schema);
            m.snapshot_id = 1;
            m.sequence_number = 1;
            m.parent_snapshot_id = None;
            let bytes = m.to_protobuf()?;
            store
                .put_if_absent(&manifest_path(1), bytes::Bytes::from(bytes))
                .await?;
            (m, 1)
        } else {
            // Load HEAD manifest from store.
            let bytes = store.get(&manifest_path(head)).await?;
            let m = Manifest::from_protobuf(&bytes)?;
            (m, head)
        };

        Ok(Self {
            store,
            current: Mutex::new(manifest),
            head: Mutex::new(current_head),
        })
    }

    /// Return a clone of the current manifest.
    pub async fn current_manifest(&self) -> Manifest {
        self.current.lock().await.clone()
    }

    /// Return the current HEAD version number.
    pub async fn head(&self) -> i64 {
        *self.head.lock().await
    }

    /// Re-discover HEAD and reload the current manifest from the
    /// store. Used by read-only replicas to pick up commits from
    /// other writers.
    pub async fn refresh(&self) -> Result<()> {
        let store = self.store.clone();
        let new_head = discover_head(|v| {
            let s = store.clone();
            async move { s.exists(&manifest_path(v)).await }
        })
        .await?;
        if new_head == 0 {
            return Err(MeruError::Corruption(
                "refresh: HEAD discovery returned 0 against a non-empty catalog".into(),
            ));
        }
        let bytes = self.store.get(&manifest_path(new_head)).await?;
        let m = Manifest::from_protobuf(&bytes)?;
        *self.current.lock().await = m;
        *self.head.lock().await = new_head;
        Ok(())
    }

    /// Commit a snapshot transaction. Applies `txn` to the current
    /// in-memory manifest, puts via `put_if_absent`, and on race-loss
    /// re-reads HEAD, reloads the winning manifest from the store,
    /// rebuilds on top, and retries. The retry loop lives here (not
    /// in `commit_with_retry`) so the rebuild step can be genuinely
    /// async — loading a manifest from the store requires `await`.
    pub async fn commit(
        &self,
        txn: &SnapshotTransaction,
        schema: Arc<TableSchema>,
    ) -> Result<Version> {
        if !txn.dvs.is_empty() {
            return Err(MeruError::InvalidArgument(
                "ObjectStoreCatalog does not yet support DV writes in a transaction \
                 (Issue #26 Phase 5). Use CommitMode::Posix if your workload produces \
                 partial-compaction DVs."
                    .into(),
            ));
        }

        let dv_locations: std::collections::HashMap<String, DvLocation> =
            std::collections::HashMap::new();

        // Start from cached HEAD + cached manifest; fall through to a
        // store-reload on race-loss. Bounded retries and exponential
        // backoff mirror `commit_with_retry`.
        const MAX_RETRIES: usize = 8;
        let mut head = *self.head.lock().await;
        let mut base: Manifest = self.current.lock().await.clone();

        let committed_version: i64 = {
            let mut committed: Option<i64> = None;
            for attempt in 0..MAX_RETRIES {
                let new_manifest = base.apply(txn, head + 1, &dv_locations)?;
                let bytes = new_manifest.to_protobuf()?;
                let path = manifest_path(head + 1);
                match self
                    .store
                    .put_if_absent(&path, bytes::Bytes::from(bytes))
                    .await
                {
                    Ok(()) => {
                        committed = Some(head + 1);
                        break;
                    }
                    Err(MeruError::AlreadyExists(_)) => {
                        // Race-lost. Back off, re-discover HEAD,
                        // reload the winning manifest.
                        let backoff_ms = 2u64.pow(attempt as u32).min(200);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        let store = self.store.clone();
                        let new_head = discover_head(|v| {
                            let s = store.clone();
                            async move { s.exists(&manifest_path(v)).await }
                        })
                        .await?;
                        let winner_bytes = self.store.get(&manifest_path(new_head)).await?;
                        base = Manifest::from_protobuf(&winner_bytes)?;
                        head = new_head;
                    }
                    Err(other) => return Err(other),
                }
            }
            committed.ok_or_else(|| {
                MeruError::ObjectStore(format!(
                    "ObjectStoreCatalog::commit: exceeded {MAX_RETRIES} retries"
                ))
            })?
        };

        // Commit succeeded — reload from the store to keep our
        // in-memory state byte-consistent with disk.
        let bytes = self.store.get(&manifest_path(committed_version)).await?;
        let new_manifest = Manifest::from_protobuf(&bytes)?;
        let new_version = new_manifest.to_version(schema);
        *self.current.lock().await = new_manifest;
        *self.head.lock().await = committed_version;
        Ok(new_version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::IcebergDataFile;
    use merutable_store::local::LocalFileStore;
    use merutable_types::{
        level::{Level, ParquetFileMeta},
        schema::{ColumnDef, ColumnType},
    };

    fn test_schema() -> TableSchema {
        TableSchema {
            table_name: "osc".into(),
            columns: vec![ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
                ..Default::default()
            }],
            primary_key: vec![0],
            ..Default::default()
        }
    }

    fn test_data_file(path: &str) -> IcebergDataFile {
        IcebergDataFile {
            path: path.into(),
            file_size: 1024,
            num_rows: 100,
            meta: ParquetFileMeta {
                level: Level(0),
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
                column_stats: None,
            },
        }
    }

    /// Open on an empty store writes genesis v1. Reopen sees HEAD=1
    /// and the genesis manifest.
    #[tokio::test]
    async fn open_empty_store_writes_genesis() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());

        let cat = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();
        assert_eq!(cat.head().await, 1);
        let m = cat.current_manifest().await;
        assert_eq!(m.snapshot_id, 1);
        assert_eq!(m.parent_snapshot_id, None);

        // Reopen — should pick up the existing genesis, not write a new one.
        let cat2 = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();
        assert_eq!(cat2.head().await, 1);
        // The table_uuid must match — reopening must not mint a new UUID.
        assert_eq!(cat2.current_manifest().await.table_uuid, m.table_uuid);
    }

    /// Commit path: apply a transaction, observe HEAD advance to 2,
    /// manifest entries updated, backward pointer set.
    #[tokio::test]
    async fn commit_advances_head_and_chains_backward() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let schema_arc = Arc::new(test_schema());

        let cat = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();

        let mut txn = SnapshotTransaction::new();
        txn.add_file(test_data_file("data/L0/a.parquet"));
        cat.commit(&txn, schema_arc.clone()).await.unwrap();

        assert_eq!(cat.head().await, 2);
        let m = cat.current_manifest().await;
        assert_eq!(m.snapshot_id, 2);
        assert_eq!(m.parent_snapshot_id, Some(1));
        assert_eq!(m.entries.len(), 1);
        assert_eq!(m.entries[0].path, "data/L0/a.parquet");

        // v1 and v2 both on disk.
        assert!(store.exists("metadata/v1.manifest.bin").await.unwrap());
        assert!(store.exists("metadata/v2.manifest.bin").await.unwrap());
    }

    /// Multiple sequential commits form a chain; reopening then
    /// running HEAD discovery picks up the latest.
    #[tokio::test]
    async fn multi_commit_chain_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let schema_arc = Arc::new(test_schema());

        {
            let cat = ObjectStoreCatalog::open(store.clone(), test_schema())
                .await
                .unwrap();
            for i in 0..5 {
                let mut txn = SnapshotTransaction::new();
                txn.add_file(test_data_file(&format!("data/L0/f{i}.parquet")));
                cat.commit(&txn, schema_arc.clone()).await.unwrap();
            }
            assert_eq!(cat.head().await, 6);
        }
        // Reopen.
        let cat2 = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();
        assert_eq!(cat2.head().await, 6);
        assert_eq!(cat2.current_manifest().await.entries.len(), 5);
    }

    /// Concurrent-writer regression: two catalogs against the same
    /// store, each committing N times. HEAD must advance to 2*N + 1
    /// (genesis + N + N), every commit must have a unique version,
    /// and the backward-pointer chain must be intact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_chain_correctly() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let schema_arc = Arc::new(test_schema());

        // First writer creates genesis.
        let cat_a = Arc::new(
            ObjectStoreCatalog::open(store.clone(), test_schema())
                .await
                .unwrap(),
        );
        // Second writer — opens the SAME store, sees HEAD=1 via
        // discovery, does NOT re-emit genesis.
        let cat_b = Arc::new(
            ObjectStoreCatalog::open(store.clone(), test_schema())
                .await
                .unwrap(),
        );

        let n = 5;
        let a_handle = {
            let cat = cat_a.clone();
            let schema = schema_arc.clone();
            tokio::spawn(async move {
                for i in 0..n {
                    let mut txn = SnapshotTransaction::new();
                    txn.add_file(test_data_file(&format!("data/L0/a{i}.parquet")));
                    cat.commit(&txn, schema.clone()).await.unwrap();
                }
            })
        };
        let b_handle = {
            let cat = cat_b.clone();
            let schema = schema_arc.clone();
            tokio::spawn(async move {
                for i in 0..n {
                    let mut txn = SnapshotTransaction::new();
                    txn.add_file(test_data_file(&format!("data/L0/b{i}.parquet")));
                    cat.commit(&txn, schema.clone()).await.unwrap();
                }
            })
        };
        a_handle.await.unwrap();
        b_handle.await.unwrap();

        // HEAD = 1 (genesis) + 5 (a) + 5 (b) = 11. Verify every
        // version from 1..=11 exists on disk and the backward-
        // pointer chain is unbroken.
        let cat_final = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();
        assert_eq!(cat_final.head().await, 11);
        for v in 1..=11 {
            assert!(
                store
                    .exists(&format!("metadata/v{v}.manifest.bin"))
                    .await
                    .unwrap(),
                "missing v{v}"
            );
            let bytes = store
                .get(&format!("metadata/v{v}.manifest.bin"))
                .await
                .unwrap();
            let m = Manifest::from_protobuf(&bytes).unwrap();
            assert_eq!(m.snapshot_id, v, "snapshot_id mismatch at v{v}");
            if v == 1 {
                assert_eq!(m.parent_snapshot_id, None);
            } else {
                assert_eq!(m.parent_snapshot_id, Some(v - 1));
            }
        }
    }

    /// `refresh()` on a reader catalog picks up commits from a
    /// writer catalog against the same store.
    #[tokio::test]
    async fn refresh_picks_up_writer_commits() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let schema_arc = Arc::new(test_schema());

        let writer = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();
        let reader = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();
        assert_eq!(reader.head().await, 1);

        // Writer commits twice.
        for i in 0..2 {
            let mut txn = SnapshotTransaction::new();
            txn.add_file(test_data_file(&format!("data/L0/w{i}.parquet")));
            writer.commit(&txn, schema_arc.clone()).await.unwrap();
        }

        // Reader still at 1 until refresh.
        assert_eq!(reader.head().await, 1);
        reader.refresh().await.unwrap();
        assert_eq!(reader.head().await, 3);
        assert_eq!(reader.current_manifest().await.entries.len(), 2);
    }
}
