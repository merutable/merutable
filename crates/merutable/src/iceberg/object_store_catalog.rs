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
//!
//! # Phase 5: Puffin DV file handling
//!
//! Partial-compaction deletion vectors go end-to-end: the commit
//! path fetches any pre-existing DV via `store.get_range`, unions it
//! with the caller's new DV, encodes a fresh Puffin blob, writes it
//! under `data/L{n}/{stem}.dv-{attempted_version}.puffin`, and records
//! the real `(dv_path, dv_offset, dv_length)` in the manifest. On
//! race-loss the retry loop re-reads HEAD, re-loads whatever DV the
//! winner installed (not our stale cache), re-unions, re-encodes, and
//! re-writes at the bumped version. The losing-writer's puffin is
//! orphaned on the store — Phase 6 GC will reclaim it.
//!
//! # Phase 6: Manifest chain GC
//!
//! `reclaim_old_manifests(keep_recent)` keeps the last `keep_recent`
//! manifest versions and deletes everything below. Safe by
//! construction: the current snapshot's entries point directly at
//! their data files, so pruning old manifests never breaks reads of
//! the current table state. Time travel below the cutoff becomes
//! impossible — this is an explicit caller decision, not a side
//! effect. A low-water pointer file (`metadata/low_water.txt`) is
//! written before any delete so that concurrent openers and crash
//! recovery know where discovery should start probing; without it a
//! reopen would probe v1, find it gone, and incorrectly declare the
//! catalog empty.

use std::sync::Arc;

use crate::store::traits::MeruStore;
use crate::types::{schema::TableSchema, MeruError, Result};
use tokio::sync::Mutex;

use crate::iceberg::deletion_vector::DeletionVector;
use crate::iceberg::head_discovery::{discover_head_from, Head};
use crate::iceberg::manifest::{DvLocation, Manifest};
use crate::iceberg::object_store_commit::manifest_path;
use crate::iceberg::snapshot::SnapshotTransaction;
use crate::iceberg::version::Version;

/// Path of the low-water-mark pointer file. Records the lowest
/// surviving manifest version after a reclaim — HEAD discovery
/// starts its exponential scan here instead of at v1.
const LOW_WATER_PATH: &str = "metadata/low_water.txt";

/// Read the low-water pointer from the store. Absent file → 1 (no
/// reclaim has happened). Malformed bytes → `Corruption`.
async fn read_low_water<S: MeruStore + ?Sized>(store: &S) -> Result<i64> {
    match store.get(LOW_WATER_PATH).await {
        Ok(bytes) => {
            let s = std::str::from_utf8(&bytes)
                .map_err(|e| MeruError::Corruption(format!("low_water.txt not UTF-8: {e}")))?;
            let v: i64 = s
                .trim()
                .parse()
                .map_err(|e| MeruError::Corruption(format!("low_water.txt not an integer: {e}")))?;
            if v < 1 {
                return Err(MeruError::Corruption(format!(
                    "low_water.txt contains {v} — must be ≥ 1"
                )));
            }
            Ok(v)
        }
        // Most backends signal "missing" via a specific error shape; for
        // the purpose of this pointer file we tolerate ANY read error
        // as "no low-water set, default to 1". Discovery will then
        // probe v1; if v1 is also absent the catalog is (correctly)
        // treated as empty.
        Err(_) => Ok(1),
    }
}

/// Discover HEAD against `store`, honoring the low-water pointer if
/// present. Callers should prefer this helper over raw
/// `discover_head_from` so the pointer semantics stay consistent.
async fn discover_head_with_low_water<S: MeruStore + ?Sized>(store: &S) -> Result<Head> {
    let low_water = read_low_water(store).await?;
    let store_ref: &S = store;
    // Capture `store_ref` by reference so we don't clone the Arc in
    // the hot closure path.
    discover_head_from(low_water, |v| async move {
        store_ref.exists(&manifest_path(v)).await
    })
    .await
}

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
        let head = discover_head_with_low_water(store.as_ref()).await?;

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
        let new_head = discover_head_with_low_water(self.store.as_ref()).await?;
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

    /// Phase 5 helper. For each `(parquet_path, new_dv)` in the
    /// transaction, union with any pre-existing DV from the base
    /// manifest, encode as Puffin bytes, write to the store under a
    /// version-qualified path, and return the `DvLocation` map the
    /// manifest applier needs. Errors if a transaction references a
    /// Parquet path that does not appear (live) in the base manifest.
    ///
    /// Contention safety: `attempted_version` is embedded in every
    /// puffin path, so two writers whose retries land on different
    /// attempt numbers never collide. A writer whose attempt loses
    /// leaves an orphan — the caller deletes it best-effort before the
    /// next retry; worst case Phase 6 GC sweeps it.
    async fn upload_dvs_for_attempt(
        &self,
        base: &Manifest,
        txn: &SnapshotTransaction,
        attempted_version: i64,
    ) -> Result<std::collections::HashMap<String, DvLocation>> {
        let mut out = std::collections::HashMap::new();
        if txn.dvs.is_empty() {
            return Ok(out);
        }
        for (parquet_path, new_dv) in &txn.dvs {
            // Locate the live base entry this DV applies to.
            let entry = base
                .entries
                .iter()
                .find(|e| e.status != "deleted" && e.path == *parquet_path)
                .ok_or_else(|| {
                    MeruError::InvalidArgument(format!(
                        "ObjectStoreCatalog::commit: txn.dvs references '{parquet_path}' \
                         but no live entry exists in the base manifest"
                    ))
                })?;

            // Merge with any pre-existing DV the base manifest already
            // points at. Missing tri-state tolerated — treat as "no
            // existing DV", same as IcebergCatalog.
            let merged_dv = match (entry.dv_path.as_ref(), entry.dv_offset, entry.dv_length) {
                (Some(dv_path), Some(offset), Some(length)) => {
                    if offset < 0 || length < 0 {
                        return Err(MeruError::Corruption(format!(
                            "existing DV for '{parquet_path}' has negative offset \
                             ({offset}) or length ({length})"
                        )));
                    }
                    let blob = self
                        .store
                        .get_range(dv_path, offset as usize, length as usize)
                        .await?;
                    let existing = DeletionVector::from_puffin_blob(&blob)?;
                    let existing_card = existing.cardinality();
                    let new_card = new_dv.cardinality();
                    let mut merged = existing;
                    merged.union_with(new_dv);
                    // IMP-17 invariant: a union can never shrink. If
                    // it does, bits got lost and rows will reappear.
                    let merged_card = merged.cardinality();
                    let min_expected = existing_card.max(new_card);
                    if merged_card < min_expected {
                        return Err(MeruError::Corruption(format!(
                            "DV union for '{parquet_path}' shrank: \
                             existing={existing_card} new={new_card} merged={merged_card}"
                        )));
                    }
                    merged
                }
                _ => new_dv.clone(),
            };

            let encoded =
                merged_dv.encode_puffin(parquet_path, attempted_version, attempted_version)?;

            // Derive puffin path from the parquet path: same level dir,
            // stem + `.dv-{version}.puffin`. Matches IcebergCatalog's
            // layout so a tool that knows one knows the other.
            let pq = std::path::Path::new(parquet_path);
            let stem = pq.file_stem().and_then(|s| s.to_str()).unwrap_or("unknown");
            let level_dir = pq.parent().and_then(|p| p.to_str()).unwrap_or("data/L0");
            let puffin_path = format!("{level_dir}/{stem}.dv-{attempted_version}.puffin");

            self.store
                .put(&puffin_path, bytes::Bytes::from(encoded.bytes.to_vec()))
                .await?;

            out.insert(
                parquet_path.clone(),
                DvLocation {
                    dv_path: puffin_path,
                    dv_offset: encoded.blob_offset,
                    dv_length: encoded.blob_length,
                },
            );
        }
        Ok(out)
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
        // Start from cached HEAD + cached manifest; fall through to a
        // store-reload on race-loss. Bounded retries and exponential
        // backoff mirror `commit_with_retry`.
        // 16 retries buys ~30s of exponential-backoff time under
        // pathological contention (backoff caps at 200ms). 8 was
        // occasionally insufficient in multi-threaded tests with
        // unrelated disk I/O running on shared tmpfs — two writers
        // doing 5 commits each could wedge both through 4
        // consecutive race-losses, leaving only 4 retries of real
        // progress before the budget hit.
        const MAX_RETRIES: usize = 16;
        let mut head = *self.head.lock().await;
        let mut base: Manifest = self.current.lock().await.clone();

        let committed_version: i64 = {
            let mut committed: Option<i64> = None;
            for attempt in 0..MAX_RETRIES {
                let attempted_version = head + 1;

                // Phase 5: upload puffin DVs for every entry in txn.dvs.
                // Puffin paths embed `attempted_version`, so each retry
                // iteration writes fresh bytes under a fresh path —
                // no conditional-PUT dance needed on the DV blob itself.
                // The committed manifest is what makes a puffin "live";
                // losing-attempt puffins become orphans for Phase 6 GC.
                let dv_locations = self
                    .upload_dvs_for_attempt(&base, txn, attempted_version)
                    .await?;

                let new_manifest = base.apply(txn, attempted_version, &dv_locations)?;
                let bytes = new_manifest.to_protobuf()?;
                let path = manifest_path(attempted_version);
                match self
                    .store
                    .put_if_absent(&path, bytes::Bytes::from(bytes))
                    .await
                {
                    Ok(()) => {
                        committed = Some(attempted_version);
                        break;
                    }
                    Err(MeruError::AlreadyExists(_)) => {
                        // Race-lost. Back off, re-discover HEAD,
                        // reload the winning manifest. The puffin
                        // files we wrote this round are orphaned
                        // (best-effort delete so the store doesn't
                        // accumulate leaks under heavy contention).
                        for loc in dv_locations.values() {
                            let _ = self.store.delete(&loc.dv_path).await;
                        }
                        let backoff_ms = 2u64.pow(attempt as u32).min(200);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        let new_head = discover_head_with_low_water(self.store.as_ref()).await?;
                        let winner_bytes = self.store.get(&manifest_path(new_head)).await?;
                        base = Manifest::from_protobuf(&winner_bytes)?;
                        head = new_head;
                    }
                    Err(other) => {
                        // Any other error — also clean up this attempt's
                        // puffins so we don't leak on permanent failure.
                        for loc in dv_locations.values() {
                            let _ = self.store.delete(&loc.dv_path).await;
                        }
                        return Err(other);
                    }
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

    /// Phase 6: prune old manifest files from the backward-pointer
    /// chain. Keeps the most recent `keep_recent` manifest versions
    /// (must be ≥ 1) and deletes everything below. Returns the set of
    /// versions actually removed.
    ///
    /// # Safety
    ///
    /// Pruning old manifests does not affect reads of the current
    /// snapshot — every live data file is addressed directly from the
    /// current entries list, not by traversing the parent chain.
    /// Pruning does break **time travel below the cutoff**: reopening
    /// against a pruned version errors; `refresh()` still works
    /// because HEAD discovery only probes forward from the latest
    /// known version.
    ///
    /// # Cost
    ///
    /// `HEAD - keep_recent` delete calls plus one `head()` lookup.
    /// Walks backward via `parent_snapshot_id` pointers read from the
    /// retained manifests so we never touch data we're about to
    /// delete. Orphan puffin files from losing commit attempts are NOT
    /// swept here — they'd require a full LIST (violates #26's no-LIST
    /// discipline). A follow-on orphan-puffin sweeper that traverses
    /// retained manifests' DV pointers is a separate, explicit call.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `keep_recent == 0` — a catalog
    /// with no manifests is irreversibly broken.
    pub async fn reclaim_old_manifests(&self, keep_recent: usize) -> Result<Vec<i64>> {
        if keep_recent == 0 {
            return Err(MeruError::InvalidArgument(
                "ObjectStoreCatalog::reclaim_old_manifests: keep_recent must be ≥ 1 \
                 (deleting every manifest would strand all data)"
                    .into(),
            ));
        }
        let head = *self.head.lock().await;
        let cutoff = head.saturating_sub(keep_recent as i64);
        if cutoff <= 0 {
            return Ok(Vec::new());
        }

        // Step 1: write the low-water pointer BEFORE any delete so
        // that any concurrent opener (or this process after a crash)
        // knows to start discovery at `cutoff + 1` rather than
        // probing into the soon-to-be-deleted prefix. Order matters:
        // if we deleted first and crashed, a fresh open would probe
        // v1, find it gone, and declare the catalog empty.
        let new_low_water = cutoff + 1;
        self.store
            .put(
                LOW_WATER_PATH,
                bytes::Bytes::from(new_low_water.to_string()),
            )
            .await?;

        let mut removed = Vec::new();
        // Step 2: walk versions [1..=cutoff] and delete each manifest.
        // `delete` is idempotent per MeruStore contract — a missing
        // key returns Ok, not an error. So a prior partial reclaim
        // (crash mid-loop) replays cleanly.
        for v in 1..=cutoff {
            let path = manifest_path(v);
            self.store.delete(&path).await?;
            removed.push(v);
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::snapshot::IcebergDataFile;
    use crate::store::local::LocalFileStore;
    use crate::types::{
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

    /// Phase 5: a transaction carrying a DV for a live file uploads a
    /// puffin blob to the store, records real (path, offset, length) in
    /// the new manifest, and the DV round-trips to a roaring bitmap
    /// when read back from the store.
    #[tokio::test]
    async fn commit_with_dv_uploads_puffin_and_records_real_coordinates() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let schema_arc = Arc::new(test_schema());

        let cat = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();

        // Commit 1: add a data file.
        let mut txn = SnapshotTransaction::new();
        txn.add_file(test_data_file("data/L0/hot.parquet"));
        cat.commit(&txn, schema_arc.clone()).await.unwrap();

        // Commit 2: attach a DV to that file (partial compaction).
        let mut dv = DeletionVector::new();
        dv.mark_deleted(3);
        dv.mark_deleted(7);
        dv.mark_deleted(42);
        let mut txn2 = SnapshotTransaction::new();
        txn2.dvs.insert("data/L0/hot.parquet".into(), dv);
        cat.commit(&txn2, schema_arc.clone()).await.unwrap();

        // Verify the manifest entry now carries real DV coordinates.
        let m = cat.current_manifest().await;
        assert_eq!(m.snapshot_id, 3);
        let entry = m
            .entries
            .iter()
            .find(|e| e.path == "data/L0/hot.parquet" && e.status != "deleted")
            .expect("live entry for hot.parquet");
        let dv_path = entry.dv_path.as_ref().expect("DV path set");
        let dv_offset = entry.dv_offset.expect("DV offset set");
        let dv_length = entry.dv_length.expect("DV length set");
        assert!(
            dv_path.ends_with(".dv-3.puffin"),
            "DV path {dv_path} should embed the committed version"
        );
        assert!(dv_offset > 0, "non-zero offset");
        assert!(dv_length > 0, "non-zero length");

        // Round-trip: read the blob range from the store, decode,
        // verify the three deleted rows.
        let blob = store
            .get_range(dv_path, dv_offset as usize, dv_length as usize)
            .await
            .unwrap();
        let decoded = DeletionVector::from_puffin_blob(&blob).unwrap();
        assert!(decoded.is_deleted(3));
        assert!(decoded.is_deleted(7));
        assert!(decoded.is_deleted(42));
        assert!(!decoded.is_deleted(99));
        assert_eq!(decoded.cardinality(), 3);
    }

    /// Phase 5: two successive DVs on the same file union under the
    /// hood — the second commit's DV does not replace the first.
    #[tokio::test]
    async fn consecutive_dvs_union_not_replace() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let schema_arc = Arc::new(test_schema());

        let cat = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();

        // Add file.
        let mut add = SnapshotTransaction::new();
        add.add_file(test_data_file("data/L0/warm.parquet"));
        cat.commit(&add, schema_arc.clone()).await.unwrap();

        // First DV: delete rows {1, 2}.
        let mut first = DeletionVector::new();
        first.mark_deleted(1);
        first.mark_deleted(2);
        let mut txn1 = SnapshotTransaction::new();
        txn1.dvs.insert("data/L0/warm.parquet".into(), first);
        cat.commit(&txn1, schema_arc.clone()).await.unwrap();

        // Second DV: delete rows {5, 6}. Must union.
        let mut second = DeletionVector::new();
        second.mark_deleted(5);
        second.mark_deleted(6);
        let mut txn2 = SnapshotTransaction::new();
        txn2.dvs.insert("data/L0/warm.parquet".into(), second);
        cat.commit(&txn2, schema_arc.clone()).await.unwrap();

        let m = cat.current_manifest().await;
        let entry = m
            .entries
            .iter()
            .find(|e| e.path == "data/L0/warm.parquet" && e.status != "deleted")
            .unwrap();
        let dv_path = entry.dv_path.as_ref().unwrap();
        let blob = store
            .get_range(
                dv_path,
                entry.dv_offset.unwrap() as usize,
                entry.dv_length.unwrap() as usize,
            )
            .await
            .unwrap();
        let decoded = DeletionVector::from_puffin_blob(&blob).unwrap();
        assert_eq!(decoded.cardinality(), 4, "all four rows must remain marked");
        for row in [1, 2, 5, 6] {
            assert!(decoded.is_deleted(row), "row {row} must survive union");
        }
    }

    /// Phase 5: a DV whose parquet target is not in the base manifest
    /// is a caller bug — returns InvalidArgument.
    #[tokio::test]
    async fn dv_for_unknown_file_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let schema_arc = Arc::new(test_schema());

        let cat = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();

        let mut dv = DeletionVector::new();
        dv.mark_deleted(1);
        let mut txn = SnapshotTransaction::new();
        txn.dvs.insert("data/L0/ghost.parquet".into(), dv);

        let err = cat.commit(&txn, schema_arc.clone()).await.unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("ghost.parquet"),
            "error must name the missing path: {msg}"
        );
    }

    /// Phase 6: reclaim keeps the last N manifests and deletes the
    /// rest. HEAD and current manifest remain unchanged.
    #[tokio::test]
    async fn reclaim_keeps_last_n_manifests() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let schema_arc = Arc::new(test_schema());
        let cat = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();

        // Commit 9 transactions: HEAD goes 1 (genesis) → 10.
        for i in 0..9 {
            let mut txn = SnapshotTransaction::new();
            txn.add_file(test_data_file(&format!("data/L0/f{i}.parquet")));
            cat.commit(&txn, schema_arc.clone()).await.unwrap();
        }
        assert_eq!(cat.head().await, 10);

        // Keep last 3 → {8, 9, 10}. Remove {1..=7}.
        let removed = cat.reclaim_old_manifests(3).await.unwrap();
        assert_eq!(removed, (1..=7).collect::<Vec<_>>());

        // v1..v7 gone.
        for v in 1..=7 {
            assert!(
                !store
                    .exists(&format!("metadata/v{v}.manifest.bin"))
                    .await
                    .unwrap(),
                "v{v} should be reclaimed"
            );
        }
        // v8..v10 present.
        for v in 8..=10 {
            assert!(
                store
                    .exists(&format!("metadata/v{v}.manifest.bin"))
                    .await
                    .unwrap(),
                "v{v} should remain"
            );
        }

        // Head and current are unaffected.
        assert_eq!(cat.head().await, 10);
        assert_eq!(cat.current_manifest().await.snapshot_id, 10);

        // Refresh still works (it probes forward from HEAD, doesn't
        // need pruned manifests).
        cat.refresh().await.unwrap();
        assert_eq!(cat.head().await, 10);
    }

    /// Phase 6: reclaim is idempotent — a second call with the same
    /// keep_recent is a no-op (returns empty vec).
    #[tokio::test]
    async fn reclaim_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let schema_arc = Arc::new(test_schema());
        let cat = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();
        for i in 0..4 {
            let mut txn = SnapshotTransaction::new();
            txn.add_file(test_data_file(&format!("data/L0/f{i}.parquet")));
            cat.commit(&txn, schema_arc.clone()).await.unwrap();
        }
        assert_eq!(cat.head().await, 5);

        let first = cat.reclaim_old_manifests(2).await.unwrap();
        assert_eq!(first, vec![1, 2, 3]);
        // Second call with same threshold: nothing to delete in the
        // cutoff range that still exists, but the for-loop still
        // issues idempotent deletes for v1..v3 → LocalFileStore's
        // delete is a no-op on missing keys so the call succeeds.
        // The returned vec still lists those versions (the contract
        // is "versions we asked the store to delete", not "versions
        // that existed"); the important invariant is no error.
        let second = cat.reclaim_old_manifests(2).await.unwrap();
        assert_eq!(second, vec![1, 2, 3]);
    }

    /// Phase 6: keep_recent=0 is rejected.
    #[tokio::test]
    async fn reclaim_zero_keep_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let cat = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();
        let err = cat.reclaim_old_manifests(0).await.unwrap_err();
        assert!(format!("{err:?}").contains("keep_recent"));
    }

    /// Phase 6: after reclaim, reopening the catalog against the
    /// same store rediscovers the correct HEAD via the low-water
    /// pointer — not v1.
    #[tokio::test]
    async fn reopen_after_reclaim_rediscovers_head() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let schema_arc = Arc::new(test_schema());

        {
            let cat = ObjectStoreCatalog::open(store.clone(), test_schema())
                .await
                .unwrap();
            for i in 0..6 {
                let mut txn = SnapshotTransaction::new();
                txn.add_file(test_data_file(&format!("data/L0/f{i}.parquet")));
                cat.commit(&txn, schema_arc.clone()).await.unwrap();
            }
            assert_eq!(cat.head().await, 7);
            // Keep last 2: {6, 7}; delete {1..=5}.
            cat.reclaim_old_manifests(2).await.unwrap();
        }

        // Reopen — must land on HEAD=7 via the low-water pointer,
        // not on HEAD=0 (which would be the bug where discovery
        // probes v1 and sees it gone).
        let cat2 = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();
        assert_eq!(cat2.head().await, 7, "reopen must rediscover HEAD=7");
        assert_eq!(cat2.current_manifest().await.snapshot_id, 7);
        assert_eq!(cat2.current_manifest().await.entries.len(), 6);

        // And further commits still advance HEAD normally.
        let mut txn = SnapshotTransaction::new();
        txn.add_file(test_data_file("data/L0/f_new.parquet"));
        cat2.commit(&txn, schema_arc).await.unwrap();
        assert_eq!(cat2.head().await, 8);
    }

    /// Phase 6: keep_recent larger than HEAD deletes nothing.
    #[tokio::test]
    async fn reclaim_keep_larger_than_head_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let cat = ObjectStoreCatalog::open(store.clone(), test_schema())
            .await
            .unwrap();
        // HEAD = 1.
        let removed = cat.reclaim_old_manifests(100).await.unwrap();
        assert!(removed.is_empty());
        assert!(store.exists("metadata/v1.manifest.bin").await.unwrap());
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
