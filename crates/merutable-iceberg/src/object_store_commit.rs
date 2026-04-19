//! Issue #26 Phase 2c: conditional-PUT commit mechanics.
//!
//! Composes the pieces from Phases 2a (`MeruStore::put_if_absent`) and
//! 2b (HEAD discovery via exponential probe) with the #28 protobuf
//! manifest format to produce a complete one-shot commit primitive
//! for `CommitMode::ObjectStore`. Not yet wired into `MeruEngine`;
//! the full catalog wrapper is Phase 3.
//!
//! # Protocol
//!
//! 1. Build the next manifest on top of HEAD: `previous_snapshot_id =
//!    head`, `snapshot_id = head + 1`, `sequence_number = head + 1`.
//! 2. Serialize to protobuf bytes (framing header + prost payload).
//! 3. `store.put_if_absent("metadata/v{head+1}.manifest.bin", bytes)`.
//! 4. On `AlreadyExists` → another writer raced us. Re-discover
//!    HEAD, rebuild on top, retry. Bounded retries with exponential
//!    backoff so a pathological thundering-herd terminates.
//! 5. Return the committed version.
//!
//! # Cost
//!
//! - Uncontended commit: one `put_if_absent` call.
//! - Contended commit: N rediscovery rounds, each costing a HEAD
//!   discovery (O(log HEAD) probes) plus one put_if_absent. Bounded
//!   by `MAX_RETRIES` = 8 — enough to survive a transient burst,
//!   small enough that a malicious sustained-contention writer
//!   doesn't wedge the caller forever.

use std::time::Duration;

use bytes::Bytes;
use merutable_store::traits::MeruStore;
use merutable_types::{MeruError, Result};

use crate::head_discovery::discover_head;
use crate::manifest::Manifest;

/// Max commit retries on `AlreadyExists`. Every retry costs one
/// HEAD-rediscovery + one put_if_absent; 8 rounds survives a
/// realistic thundering herd without wedging.
const MAX_RETRIES: usize = 8;

/// Path in the store for the manifest at version `v`. All
/// ObjectStore-mode catalogs agree on this layout so HEAD
/// discovery, commit, and refresh all probe identical keys.
pub fn manifest_path(v: i64) -> String {
    format!("metadata/v{v}.manifest.bin")
}

/// Commit a manifest to `store` under ObjectStore mode. `base_manifest`
/// is the caller's in-memory HEAD; if it's already stale (another
/// writer has since committed), we re-discover HEAD, ask `build_on_top`
/// to rebuild on the new HEAD, and retry. `build_on_top` takes the
/// newly-observed HEAD version and returns the new manifest to write.
///
/// Returns the committed version number (always `prev_HEAD + 1` at
/// time of success).
///
/// # Why the closure
///
/// Making the caller pass a `Manifest` directly would lose the
/// ability to rebuild on race-loss. The closure lets the caller
/// re-apply their `SnapshotTransaction` against whichever
/// predecessor version won the race.
pub async fn commit_with_retry<S, F>(
    store: &S,
    initial_head: i64,
    mut build_on_top: F,
) -> Result<i64>
where
    S: MeruStore + ?Sized,
    F: FnMut(i64) -> Result<Manifest>,
{
    let mut current_head = initial_head;
    for attempt in 0..MAX_RETRIES {
        let manifest = build_on_top(current_head)?;
        let expected_version = current_head + 1;
        // Sanity: the caller's manifest must declare itself at the
        // version we're about to commit AND carry the correct
        // backward pointer. Otherwise a bug in build_on_top could
        // break the chain silently.
        if manifest.snapshot_id != expected_version {
            return Err(MeruError::Corruption(format!(
                "commit_with_retry: manifest.snapshot_id={} but expected_version={}",
                manifest.snapshot_id, expected_version
            )));
        }
        if manifest.parent_snapshot_id != Some(current_head) && current_head != 0 {
            return Err(MeruError::Corruption(format!(
                "commit_with_retry: manifest.parent_snapshot_id={:?} but current_head={}",
                manifest.parent_snapshot_id, current_head
            )));
        }

        let bytes = Bytes::from(manifest.to_protobuf()?);
        let path = manifest_path(expected_version);
        match store.put_if_absent(&path, bytes).await {
            Ok(()) => return Ok(expected_version),
            Err(MeruError::AlreadyExists(_)) => {
                // Another writer won. Exponential backoff before
                // re-discovery so a thundering herd doesn't DDoS
                // the store with rediscovery probes.
                let backoff_ms = 2u64.pow(attempt as u32).min(200);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                // Re-discover HEAD against the live store.
                current_head =
                    discover_head(|v| async move { store.exists(&manifest_path(v)).await }).await?;
                continue;
            }
            Err(other) => return Err(other),
        }
    }
    Err(MeruError::ObjectStore(format!(
        "commit_with_retry: exceeded {MAX_RETRIES} retries on ObjectStore commit"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::Manifest;
    use merutable_store::local::LocalFileStore;
    use merutable_types::schema::TableSchema;

    fn test_schema() -> TableSchema {
        TableSchema {
            table_name: "ocs".into(),
            columns: vec![merutable_types::schema::ColumnDef {
                name: "id".into(),
                col_type: merutable_types::schema::ColumnType::Int64,
                nullable: false,
                ..Default::default()
            }],
            primary_key: vec![0],
            ..Default::default()
        }
    }

    fn genesis_manifest() -> Manifest {
        let mut m = Manifest::empty(test_schema());
        m.snapshot_id = 1;
        m.sequence_number = 1;
        m.parent_snapshot_id = None;
        m
    }

    fn child_manifest(head: i64, uuid: String) -> Manifest {
        let mut m = Manifest::empty(test_schema());
        m.table_uuid = uuid;
        m.snapshot_id = head + 1;
        m.sequence_number = head + 1;
        m.parent_snapshot_id = Some(head);
        m
    }

    /// Single-writer commit path: no contention, one put_if_absent,
    /// HEAD advances to 1.
    #[tokio::test]
    async fn genesis_commit_writes_v1() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(tmp.path()).unwrap();
        let m = genesis_manifest();
        let v = commit_with_retry(&store, 0, |_head| Ok(m.clone()))
            .await
            .unwrap();
        assert_eq!(v, 1);
        assert!(store.exists("metadata/v1.manifest.bin").await.unwrap());
    }

    /// Sequential commits chain: each new commit advances HEAD by one
    /// and carries `previous_snapshot_id` pointing at the prior HEAD.
    #[tokio::test]
    async fn sequential_commits_chain_via_previous_pointer() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(tmp.path()).unwrap();
        // Genesis.
        let uuid = "test-uuid".to_string();
        let mut m = genesis_manifest();
        m.table_uuid = uuid.clone();
        commit_with_retry(&store, 0, |_head| Ok(m.clone()))
            .await
            .unwrap();
        // 4 more commits.
        for i in 1..=4 {
            let u = uuid.clone();
            let v = commit_with_retry(&store, i, |head| Ok(child_manifest(head, u.clone())))
                .await
                .unwrap();
            assert_eq!(v, i + 1);
        }
        // Verify the on-disk chain: read each manifest back and
        // check the backward pointer.
        for v in 2..=5 {
            let bytes = store
                .get(&format!("metadata/v{v}.manifest.bin"))
                .await
                .unwrap();
            let m = Manifest::from_protobuf(&bytes).unwrap();
            assert_eq!(m.snapshot_id, v);
            assert_eq!(m.parent_snapshot_id, Some(v - 1));
            assert_eq!(m.table_uuid, uuid);
        }
    }

    /// Race: inject a pre-existing v2 before the caller commits. The
    /// caller's first put_if_absent loses with AlreadyExists; retry
    /// re-discovers HEAD=2, builds v3, wins. Final HEAD = 3.
    #[tokio::test]
    async fn race_loss_rediscovers_and_retries() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(tmp.path()).unwrap();
        // Seed v1 + v2 — simulate another writer already committed two
        // versions while the caller thought head=1.
        let uuid = "race-uuid".to_string();
        let mut g = genesis_manifest();
        g.table_uuid = uuid.clone();
        store
            .put_if_absent(
                "metadata/v1.manifest.bin",
                Bytes::from(g.to_protobuf().unwrap()),
            )
            .await
            .unwrap();
        let v2_m = child_manifest(1, uuid.clone());
        store
            .put_if_absent(
                "metadata/v2.manifest.bin",
                Bytes::from(v2_m.to_protobuf().unwrap()),
            )
            .await
            .unwrap();

        // Caller thinks HEAD is still 1 (stale). `build_on_top` is
        // called twice: first time with current_head=1 (loses), second
        // time with current_head=2 (wins).
        let mut attempts = 0;
        let u = uuid.clone();
        let v = commit_with_retry(&store, 1, |head| {
            attempts += 1;
            Ok(child_manifest(head, u.clone()))
        })
        .await
        .unwrap();

        assert_eq!(v, 3, "after race, committed at v3");
        assert!(attempts >= 2, "build_on_top must be called on retry");
        let bytes = store.get("metadata/v3.manifest.bin").await.unwrap();
        let m = Manifest::from_protobuf(&bytes).unwrap();
        assert_eq!(m.parent_snapshot_id, Some(2));
    }

    /// Sanity: committing a manifest whose snapshot_id doesn't match
    /// the expected version is a corruption error (build_on_top bug).
    #[tokio::test]
    async fn mismatched_snapshot_id_is_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(tmp.path()).unwrap();
        let mut bad = genesis_manifest();
        bad.snapshot_id = 42; // wrong — expected 1
        let err = commit_with_retry(&store, 0, |_head| Ok(bad.clone()))
            .await
            .unwrap_err();
        assert!(format!("{err:?}").contains("snapshot_id=42"));
    }
}
