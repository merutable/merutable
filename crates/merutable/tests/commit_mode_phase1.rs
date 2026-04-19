//! Issue #26 Phase 1: the `CommitMode` type shape is locked.
//!
//! The behavioral implementation lands in later phases (protobuf
//! manifest, conditional PUT, HEAD discovery). This test pins the
//! API contract that Phase-1 callers can rely on:
//!
//! 1. `CommitMode::Posix` is the default (no breaking change vs
//!    pre-#26 behavior).
//! 2. `OpenOptions::commit_mode` builder exists and accepts both
//!    variants.
//! 3. Selecting `ObjectStore` at open-time errors cleanly with a
//!    message that names the tracking issue — callers can see
//!    *why* it's rejected without digging through docs.

use merutable::{CommitMode, MeruDB, OpenOptions};
use merutable_types::schema::{ColumnDef, ColumnType, TableSchema};

fn schema() -> TableSchema {
    TableSchema {
        table_name: "t".into(),
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

#[test]
fn default_commit_mode_is_posix() {
    let opts = OpenOptions::new(schema());
    assert_eq!(opts.commit_mode, CommitMode::Posix);
}

#[test]
fn builder_accepts_posix_and_objectstore() {
    let posix = OpenOptions::new(schema()).commit_mode(CommitMode::Posix);
    let objectstore = OpenOptions::new(schema()).commit_mode(CommitMode::ObjectStore);
    assert_eq!(posix.commit_mode, CommitMode::Posix);
    assert_eq!(objectstore.commit_mode, CommitMode::ObjectStore);
}

#[tokio::test]
async fn objectstore_mode_rejected_with_pointer_to_issue() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = OpenOptions::new(schema())
        .wal_dir(tmp.path().join("wal"))
        .catalog_uri(tmp.path().to_string_lossy().to_string())
        .commit_mode(CommitMode::ObjectStore);

    let err = match MeruDB::open(opts).await {
        Err(e) => e,
        Ok(_) => panic!("expected ObjectStore mode to be rejected"),
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("ObjectStore") && msg.contains("Issue #26"),
        "error must name both CommitMode::ObjectStore and Issue #26: {msg}"
    );
}

#[tokio::test]
async fn posix_mode_opens_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = OpenOptions::new(schema())
        .wal_dir(tmp.path().join("wal"))
        .catalog_uri(tmp.path().to_string_lossy().to_string())
        .commit_mode(CommitMode::Posix);
    let db = MeruDB::open(opts).await.expect("Posix mode must work");
    db.close().await.unwrap();
}
