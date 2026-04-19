//! Issue #32 Phase 3b: Replica composite — base MeruDB + ReplicaTail.
//!
//! Verifies tail-first read semantics, base fallback for unseen
//! keys, and delete authoritativeness (tail Delete hides a base row
//! that hasn't been flushed + mirrored yet).

use std::sync::Arc;

use merutable::MeruDB;
use merutable_replica::{InProcessLogSource, Replica};
use merutable_types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};

fn schema() -> TableSchema {
    TableSchema {
        table_name: "replica-compose-test".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
                ..Default::default()
            },
            ColumnDef {
                name: "v".into(),
                col_type: ColumnType::Int64,
                nullable: true,
                ..Default::default()
            },
        ],
        primary_key: vec![0],
        ..Default::default()
    }
}

async fn open_primary(tmp: &tempfile::TempDir) -> Arc<MeruDB> {
    Arc::new(
        MeruDB::open(
            merutable::OpenOptions::new(schema())
                .wal_dir(tmp.path().join("wal"))
                .catalog_uri(tmp.path().to_string_lossy().to_string()),
        )
        .await
        .unwrap(),
    )
}

fn row(id: i64, v: i64) -> Row {
    Row::new(vec![
        Some(FieldValue::Int64(id)),
        Some(FieldValue::Int64(v)),
    ])
}

#[tokio::test]
async fn replica_serves_from_tail_when_present() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let primary = open_primary(&primary_dir).await;

    // Seed the primary's catalog with a committed row so the
    // replica's base mount has something to read on open.
    primary.put(row(1, 100)).await.unwrap();
    primary.flush().await.unwrap();

    // Open the replica's base against the primary's catalog
    // (read-only). The replica tail comes from an in-process
    // log source.
    let base_opts = merutable::OpenOptions::new(schema())
        .wal_dir(replica_dir.path().join("wal"))
        .catalog_uri(primary_dir.path().to_string_lossy().to_string());
    let log = Arc::new(InProcessLogSource::new(primary.clone()));
    let replica = Replica::open(base_opts, log).await.unwrap();

    // Primary writes a NEW row post-replica-open. The tail should
    // surface it before any flush.
    primary.put(row(2, 200)).await.unwrap();
    replica.advance().await.unwrap();

    let r1 = replica.get(&[FieldValue::Int64(1)]).await.unwrap();
    assert!(r1.is_some(), "base reads resolve unchanged entries");
    let r2 = replica.get(&[FieldValue::Int64(2)]).await.unwrap();
    assert!(r2.is_some(), "tail surfaces post-open writes without flush");
}

#[tokio::test]
async fn replica_falls_through_to_base_on_tail_miss() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let primary = open_primary(&primary_dir).await;
    primary.put(row(42, 9999)).await.unwrap();
    primary.flush().await.unwrap();

    let base_opts = merutable::OpenOptions::new(schema())
        .wal_dir(replica_dir.path().join("wal"))
        .catalog_uri(primary_dir.path().to_string_lossy().to_string());
    let log = Arc::new(InProcessLogSource::new(primary.clone()));
    let replica = Replica::open(base_opts, log).await.unwrap();
    // No advance. The tail is empty.
    assert_eq!(replica.visible_seq().await, 0);

    let r = replica.get(&[FieldValue::Int64(42)]).await.unwrap();
    assert!(r.is_some(), "tail miss falls through to base");
}

#[tokio::test]
async fn tail_delete_is_authoritative_over_base_row() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let primary = open_primary(&primary_dir).await;

    // Commit row to base.
    primary.put(row(1, 111)).await.unwrap();
    primary.flush().await.unwrap();

    let base_opts = merutable::OpenOptions::new(schema())
        .wal_dir(replica_dir.path().join("wal"))
        .catalog_uri(primary_dir.path().to_string_lossy().to_string());
    let log = Arc::new(InProcessLogSource::new(primary.clone()));
    let replica = Replica::open(base_opts, log).await.unwrap();

    // Sanity: without any tail ops the replica sees the row.
    let before = replica.get(&[FieldValue::Int64(1)]).await.unwrap();
    assert!(before.is_some());

    // Primary deletes. Replica advances; the tail captures the
    // Delete op. Read should return None — the tail's tombstone
    // hides the base row.
    primary.delete(vec![FieldValue::Int64(1)]).await.unwrap();
    replica.advance().await.unwrap();
    let after = replica.get(&[FieldValue::Int64(1)]).await.unwrap();
    assert!(
        after.is_none(),
        "tail Delete must shadow the (stale) base row"
    );
}

#[tokio::test]
async fn replica_base_seq_and_visible_seq_track_independently() {
    let primary_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let primary = open_primary(&primary_dir).await;
    primary.put(row(1, 1)).await.unwrap();
    primary.flush().await.unwrap();

    let base_opts = merutable::OpenOptions::new(schema())
        .wal_dir(replica_dir.path().join("wal"))
        .catalog_uri(primary_dir.path().to_string_lossy().to_string());
    let log = Arc::new(InProcessLogSource::new(primary.clone()));
    let replica = Replica::open(base_opts, log).await.unwrap();
    let initial_base_seq = replica.base_seq();

    // Primary writes several more unflushed ops. Replica advances.
    for i in 2..=5 {
        primary.put(row(i, i)).await.unwrap();
    }
    replica.advance().await.unwrap();
    assert!(replica.visible_seq().await >= 5);
    // Base is read-only — its seq doesn't change from further
    // primary writes until the replica is closed + reopened to
    // mount a new snapshot (that's Phase 4's rebase).
    assert_eq!(replica.base_seq(), initial_base_seq);
}
