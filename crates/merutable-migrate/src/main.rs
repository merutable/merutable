//! `merutable-migrate` — one-shot layout/commit-protocol migrator.
//!
//! Issue #27. The tool converts a merutable deployment from one
//! `CommitMode` layout to another. The headline case is
//! `--from posix --to object-store`: an existing POSIX (atomic-
//! rename) layout → a single-file conditional-PUT layout suitable
//! for S3 / GCS / Azure Blob.
//!
//! # Phases
//!
//! - Phase 1 (shipped): CLI argument shape locked; every run exited
//!   with a "pending" message.
//! - Phase 2 (shipped): `--dry-run` reads a POSIX source catalog
//!   and prints the migration plan — file paths, sizes, total bytes,
//!   planned v1 genesis shape. NO writes against the destination.
//!   Exits 0 on a well-formed plan.
//! - Phase 3 (this commit): execution path for `--from posix --to
//!   object-store`. Opens an `ObjectStoreCatalog` at the destination
//!   (writes genesis v1), streams the source's live data files via
//!   `tokio::fs::copy` with bounded parallelism, then commits a
//!   single transaction adding every copied file (v2). No DV
//!   migration yet — a source catalog with non-empty DVs errors out;
//!   Phase 3b will extend the copy step to puffin blobs.
//! - Phase 4a (shipped): `--verify` re-reads every destination
//!   file and SHA-256 compares against the source. Catches silent
//!   corruption / truncation beyond what tokio::fs::copy's OK return
//!   proves.
//! - Phase 4b (this commit): `--resume` makes a re-run against an
//!   already-migrated destination idempotent. Destinations whose
//!   committed manifest diverges from the source are rejected with a
//!   clear error pointing at cross-migration conflicts.
//! - Phase 4c (planned): `--keep-source` / `--delete-source` lifecycle.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use merutable_iceberg::{
    manifest::Manifest,
    object_store_catalog::ObjectStoreCatalog,
    snapshot::{IcebergDataFile, SnapshotTransaction},
    IcebergCatalog,
};
use merutable_store::local::LocalFileStore;
use merutable_types::{
    level::ParquetFileMeta,
    schema::{ColumnDef, ColumnType, TableSchema},
    MeruError, Result,
};

/// Source / destination commit layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Layout {
    /// Atomic-rename POSIX commit protocol (merutable's today-default).
    Posix,
    /// Conditional-PUT single-file commit protocol. S3 / GCS / Azure.
    ObjectStore,
}

/// Convert a merutable deployment between commit-mode layouts.
#[derive(Parser, Debug)]
#[command(
    name = "merutable-migrate",
    about = "One-shot migrator between merutable CommitMode layouts (Issue #27)."
)]
struct Args {
    /// Source layout to read from.
    #[arg(long, value_enum)]
    from: Layout,
    /// Source path (filesystem path or object-store URI).
    #[arg(long)]
    src: String,
    /// Destination layout to write to.
    #[arg(long, value_enum)]
    to: Layout,
    /// Destination path (filesystem path or object-store URI).
    #[arg(long)]
    dst: String,
    /// Print the plan without performing any PUTs.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    /// After migration, re-read every destination file and verify
    /// byte-equality with source. Doubles cost.
    #[arg(long, default_value_t = false)]
    verify: bool,
    /// Parallelism for the copy step.
    #[arg(long, default_value_t = 8)]
    copy_parallelism: usize,
    /// Resume from a prior incomplete migration
    /// (`MIGRATION_INCOMPLETE.json` at the destination root).
    #[arg(long, default_value_t = false)]
    resume: bool,
    /// Write a `MIGRATED.txt` marker at source root but do not
    /// delete source files. Default.
    #[arg(long, default_value_t = true)]
    keep_source: bool,
    /// Actually delete source files after successful migration.
    /// Requires `--keep-source=false`.
    #[arg(long, default_value_t = false)]
    delete_source: bool,
}

#[derive(Debug)]
struct MigrationPlan {
    source_snapshot_id: i64,
    source_table_uuid: String,
    data_files: Vec<PlanEntry>,
    total_bytes: u64,
}

#[derive(Debug)]
struct PlanEntry {
    path: String,
    bytes: u64,
    num_rows: u64,
    dv_path: Option<String>,
}

/// Issue #27 Phase 2: read the source POSIX catalog's current
/// manifest and build a migration plan — file references + sizes.
/// No writes against the destination.
async fn build_plan_posix_source(src: &str) -> Result<MigrationPlan> {
    // Open the catalog read-only. We don't have the table schema a
    // priori; the catalog deserializes the schema from the on-disk
    // manifest, so a minimal placeholder schema suffices for open().
    // (IcebergCatalog::open only uses the schema to stamp genesis
    // on empty catalogs; existing catalogs carry their own schema.)
    let placeholder = TableSchema {
        table_name: "migrate-placeholder".into(),
        columns: vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Int64,
            nullable: false,
            ..Default::default()
        }],
        primary_key: vec![0],
        ..Default::default()
    };
    let catalog = IcebergCatalog::open(src, placeholder).await?;
    let manifest: Manifest = catalog.current_manifest().await;

    let mut data_files = Vec::new();
    let mut total_bytes = 0u64;
    for e in &manifest.entries {
        // Skip deleted entries — migration destination starts at v1
        // with the current live state; tombstone bookkeeping is
        // redundant across a format migration.
        if e.status == "deleted" {
            continue;
        }
        total_bytes += e.meta.file_size;
        if let Some(dv_len) = e.dv_length {
            if dv_len > 0 {
                total_bytes += dv_len as u64;
            }
        }
        data_files.push(PlanEntry {
            path: e.path.clone(),
            bytes: e.meta.file_size,
            num_rows: e.meta.num_rows,
            dv_path: e.dv_path.clone(),
        });
    }

    Ok(MigrationPlan {
        source_snapshot_id: manifest.snapshot_id,
        source_table_uuid: manifest.table_uuid,
        data_files,
        total_bytes,
    })
}

/// Phase 3: execute the POSIX → ObjectStore migration. The source
/// manifest has already been loaded to build `plan`; this function
/// copies the bytes and commits a single transaction adding the
/// migrated files to the destination catalog.
///
/// The destination catalog opens fresh: `ObjectStoreCatalog::open`
/// writes genesis v1 (empty manifest), then we commit v2 with all
/// the migrated adds. A caller inspecting the destination after
/// migration sees HEAD=2 with every source file live.
///
/// Copies are bounded by `copy_parallelism`. Each worker uses
/// `tokio::fs::copy` — streaming, no full-file buffering, single
/// syscall pair per file.
///
/// # Current limitations (Phase 3, will be lifted in 3b / 4)
/// - No DV migration: source manifests with live DVs error out early.
/// - No resume: a crash mid-copy leaves orphan files at the
///   destination; rerun with the same args to re-do the copy (puts
///   are idempotent via `tokio::fs::copy` overwrite semantics).
/// - No verify: `--verify` is accepted but not wired to a re-read
///   pass yet.
async fn execute_posix_to_object_store(
    src: &str,
    dst: &str,
    plan: &MigrationPlan,
    copy_parallelism: usize,
    source_schema: TableSchema,
    source_manifest_entries: &[merutable_iceberg::manifest::ManifestEntry],
    resume: bool,
) -> Result<i64> {
    // Inspect the source manifest entries directly (not the plan)
    // because DV coordinates live on `ManifestEntry` or
    // `ParquetFileMeta`, and the plan only carries a subset.
    let has_dv = source_manifest_entries.iter().any(|e| {
        e.status != "deleted"
            && (e.dv_path.is_some()
                || e.meta.dv_path.is_some()
                || e.meta.dv_length.unwrap_or(0) > 0)
    });
    if has_dv {
        return Err(MeruError::InvalidArgument(
            "Issue #27 Phase 3 does not yet migrate deletion vectors. The source \
             catalog contains live DVs; rerun after compacting them away (`MeruDB::compact()`) \
             or wait for Phase 3b which will extend the copy step to puffin blobs."
                .into(),
        ));
    }

    let src_path = PathBuf::from(src);
    let dst_path = PathBuf::from(dst);
    tokio::fs::create_dir_all(&dst_path)
        .await
        .map_err(MeruError::Io)?;

    let store = Arc::new(
        LocalFileStore::new(&dst_path)
            .map_err(|e| MeruError::ObjectStore(format!("open destination store: {e}")))?,
    );

    // Open the destination catalog: writes genesis v1 (empty) if the
    // destination is a fresh directory; otherwise re-uses HEAD.
    let catalog = ObjectStoreCatalog::open(store.clone(), source_schema).await?;

    // Phase 4b: `--resume`. If the destination already contains the
    // full migrated state (HEAD > 1 AND every source file is live on
    // the destination manifest), skip both copy and commit. This
    // makes a crashed-after-commit migration re-runnable without
    // error or duplicated work.
    //
    // Without --resume, re-running against a committed destination
    // would attempt to write a second "all files added" transaction,
    // producing either duplicate entries or an applier error depending
    // on the source state. --resume opts into the short-circuit.
    if resume {
        let dst_manifest = catalog.current_manifest().await;
        if dst_manifest.snapshot_id > 1 {
            let dst_live: std::collections::HashSet<&str> = dst_manifest
                .entries
                .iter()
                .filter(|e| e.status != "deleted")
                .map(|e| e.path.as_str())
                .collect();
            let src_live: std::collections::HashSet<&str> = source_manifest_entries
                .iter()
                .filter(|e| e.status != "deleted")
                .map(|e| e.path.as_str())
                .collect();
            if dst_live == src_live {
                return Ok(dst_manifest.snapshot_id);
            }
            // HEAD > 1 but file set differs — the destination was
            // committed for a different migration. Refuse to overwrite.
            return Err(MeruError::InvalidArgument(format!(
                "--resume: destination at v{} has {} live files but source has {} — \
                 file sets differ. This is NOT a resumable state; either the \
                 destination was used for a different migration or the source \
                 has evolved. Manual intervention required.",
                dst_manifest.snapshot_id,
                dst_live.len(),
                src_live.len(),
            )));
        }
        // HEAD == 1 (genesis-only) — resume proceeds as a normal
        // migration; the only cost is redundant copies for files that
        // were transferred before the earlier crash (tokio::fs::copy
        // overwrites safely).
    }

    // Copy phase. We deliberately copy BEFORE we write to the dest
    // catalog so that a crash mid-copy never leaves the catalog
    // referencing files that don't yet exist on the destination.
    let semaphore = Arc::new(tokio::sync::Semaphore::new(copy_parallelism.max(1)));
    let mut join = tokio::task::JoinSet::new();
    for entry in &plan.data_files {
        let sem = semaphore.clone();
        let src_abs = src_path.join(&entry.path);
        let dst_abs = dst_path.join(&entry.path);
        join.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore never closed");
            if let Some(parent) = dst_abs.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(MeruError::Io)?;
            }
            tokio::fs::copy(&src_abs, &dst_abs)
                .await
                .map_err(MeruError::Io)?;
            Ok::<(), MeruError>(())
        });
    }
    while let Some(res) = join.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(join_err) => {
                return Err(MeruError::ObjectStore(format!(
                    "copy task panicked: {join_err}"
                )));
            }
        }
    }

    // Rebuild IcebergDataFile records from the source manifest
    // entries and commit them as a single transaction against the
    // destination catalog. The source manifest is already our source
    // of truth for per-file metadata (level, seq range, key range,
    // column stats); nothing to recompute.
    let mut txn = SnapshotTransaction::new();
    for entry in source_manifest_entries {
        if entry.status == "deleted" {
            continue;
        }
        txn.add_file(IcebergDataFile {
            path: entry.path.clone(),
            file_size: entry.meta.file_size,
            num_rows: entry.meta.num_rows,
            meta: clone_meta(&entry.meta),
        });
    }
    let schema_arc = Arc::new(catalog.current_manifest().await.schema);
    let version = catalog.commit(&txn, schema_arc).await?;
    Ok(version.snapshot_id)
}

/// `ParquetFileMeta` doesn't derive `Clone` in every field; the
/// manifest applier consumes the whole struct so an explicit
/// field-level clone keeps us honest about what's being carried
/// across.
fn clone_meta(m: &ParquetFileMeta) -> ParquetFileMeta {
    ParquetFileMeta {
        level: m.level,
        seq_min: m.seq_min,
        seq_max: m.seq_max,
        key_min: m.key_min.clone(),
        key_max: m.key_max.clone(),
        num_rows: m.num_rows,
        file_size: m.file_size,
        dv_path: m.dv_path.clone(),
        dv_offset: m.dv_offset,
        dv_length: m.dv_length,
        format: m.format,
        column_stats: m.column_stats.clone(),
    }
}

/// Phase 4a: `--verify`. Re-read every migrated file from the
/// destination and compare its content hash against the source.
/// Doubles the I/O cost of a migration but catches corruption or
/// truncation that slipped past the copy step (silent disk errors,
/// partial writes on a crash, etc.).
///
/// We use SHA-256 so caller error reports don't accidentally match
/// on a weak hash collision. The operations are parallelized via the
/// same semaphore budget as the copy step.
async fn verify_destination(
    src: &str,
    dst: &str,
    plan: &MigrationPlan,
    parallelism: usize,
) -> Result<()> {
    use sha2::{Digest, Sha256};

    let src_path = PathBuf::from(src);
    let dst_path = PathBuf::from(dst);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(parallelism.max(1)));
    let mut join = tokio::task::JoinSet::new();
    for entry in &plan.data_files {
        let sem = semaphore.clone();
        let src_abs = src_path.join(&entry.path);
        let dst_abs = dst_path.join(&entry.path);
        let path = entry.path.clone();
        join.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore never closed");
            let src_bytes = tokio::fs::read(&src_abs).await.map_err(MeruError::Io)?;
            let dst_bytes = tokio::fs::read(&dst_abs).await.map_err(MeruError::Io)?;
            let src_hash = Sha256::digest(&src_bytes);
            let dst_hash = Sha256::digest(&dst_bytes);
            if src_hash != dst_hash {
                return Err(MeruError::Corruption(format!(
                    "verify: content mismatch at '{path}' \
                     (src sha256 {src_hash:x} != dst sha256 {dst_hash:x})"
                )));
            }
            Ok::<(), MeruError>(())
        });
    }
    while let Some(res) = join.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(join_err) => {
                return Err(MeruError::ObjectStore(format!(
                    "verify task panicked: {join_err}"
                )));
            }
        }
    }
    Ok(())
}

/// Re-open the source catalog to grab the schema + entries — the
/// initial plan builder hands back only file-level info, but Phase 3
/// needs the full manifest to rebuild `IcebergDataFile` records.
async fn read_source_manifest(src: &str) -> Result<Manifest> {
    let placeholder = TableSchema {
        table_name: "migrate-placeholder".into(),
        columns: vec![ColumnDef {
            name: "id".into(),
            col_type: ColumnType::Int64,
            nullable: false,
            ..Default::default()
        }],
        primary_key: vec![0],
        ..Default::default()
    };
    let catalog = IcebergCatalog::open(Path::new(src), placeholder).await?;
    Ok(catalog.current_manifest().await)
}

fn print_plan(plan: &MigrationPlan, args: &Args) {
    let total_rows: u64 = plan.data_files.iter().map(|e| e.num_rows).sum();
    eprintln!();
    eprintln!("migration plan:");
    eprintln!("  source snapshot id: {}", plan.source_snapshot_id);
    eprintln!("  source table uuid:  {}", plan.source_table_uuid);
    eprintln!("  data files:         {}", plan.data_files.len());
    eprintln!("  total rows:         {total_rows}");
    eprintln!(
        "  total bytes:        {} ({:.2} MiB)",
        plan.total_bytes,
        plan.total_bytes as f64 / (1024.0 * 1024.0)
    );
    let with_dv = plan
        .data_files
        .iter()
        .filter(|e| e.dv_path.is_some())
        .count();
    if with_dv > 0 {
        eprintln!("  files with DV:      {with_dv}");
    }
    // Sample the first few paths so operators can sanity-check.
    eprintln!("  first 5 paths:");
    for e in plan.data_files.iter().take(5) {
        eprintln!("    {} ({} bytes)", e.path, e.bytes);
    }
    eprintln!();
    eprintln!("destination plan ({:?}):", args.to);
    eprintln!("  dst:                {}", args.dst);
    eprintln!("  genesis version:   v1 (no snapshot history preserved)");
    eprintln!(
        "  parallelism:        {} (would stream {} files)",
        args.copy_parallelism,
        plan.data_files.len()
    );
    eprintln!();
    if args.dry_run {
        eprintln!("DRY RUN: no bytes written to destination.");
    }
}

fn run() -> std::result::Result<(), (i32, String)> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let args = Args::parse();

    if args.from == args.to {
        return Err((
            2,
            format!(
                "--from and --to are both `{:?}`. Same-layout migrations are no-ops; \
                 use the engine directly to copy data. Aborting.",
                args.from
            ),
        ));
    }

    eprintln!(
        "merutable-migrate: --from {:?} --src {} --to {:?} --dst {}",
        args.from, args.src, args.to, args.dst
    );
    eprintln!(
        "  dry_run={} verify={} copy_parallelism={} resume={} keep_source={} delete_source={}",
        args.dry_run,
        args.verify,
        args.copy_parallelism,
        args.resume,
        args.keep_source,
        args.delete_source
    );
    eprintln!(
        "warning: snapshot history is NOT preserved. Destination starts at v1 \
         with the latest source state."
    );

    // Phase 2 supports `--from posix` only; ObjectStore source
    // requires the `ObjectStoreCatalog` read-path wiring (Phase 3).
    if matches!(args.from, Layout::ObjectStore) {
        return Err((
            3,
            "--from object-store is not yet implemented (Issue #27 Phase 3 pending).".into(),
        ));
    }

    // Source-read path: available now for POSIX.
    let runtime = tokio::runtime::Runtime::new().map_err(|e| (4, format!("tokio runtime: {e}")))?;
    let plan = runtime
        .block_on(build_plan_posix_source(&args.src))
        .map_err(|e| (5, format!("build migration plan: {e}")))?;
    print_plan(&plan, &args);

    if args.dry_run {
        eprintln!(
            "dry run complete — {} files would be migrated.",
            plan.data_files.len()
        );
        return Ok(());
    }

    // Real execution for `--from posix --to object-store`. Load the
    // full manifest (Phase 2 only carried per-file basics) so we can
    // carry seq ranges, key ranges, and column stats forward into
    // the destination transaction.
    if matches!(args.to, Layout::ObjectStore) {
        let source_manifest = runtime
            .block_on(read_source_manifest(&args.src))
            .map_err(|e| (7, format!("read source manifest: {e}")))?;
        let schema = source_manifest.schema.clone();
        let entries = source_manifest.entries.clone();
        let committed_version = runtime
            .block_on(execute_posix_to_object_store(
                &args.src,
                &args.dst,
                &plan,
                args.copy_parallelism,
                schema,
                &entries,
                args.resume,
            ))
            .map_err(|e| (8, format!("execute migration: {e}")))?;
        eprintln!(
            "migration complete: destination committed at v{committed_version} ({} files)",
            plan.data_files.len()
        );
        if args.verify {
            eprintln!(
                "--verify: SHA-256 comparing {} source↔destination files...",
                plan.data_files.len()
            );
            runtime
                .block_on(verify_destination(
                    &args.src,
                    &args.dst,
                    &plan,
                    args.copy_parallelism,
                ))
                .map_err(|e| (9, format!("verification failed: {e}")))?;
            eprintln!("--verify: all {} files match.", plan.data_files.len());
        }
        return Ok(());
    }

    // Destination `posix` was already rejected as same-layout. We
    // can't reach here unless a future layout variant is added and
    // not plumbed through. Fail loudly.
    Err((
        99,
        format!(
            "unhandled destination layout `{:?}`. This is a bug — please report.",
            args.to
        ),
    ))
}

fn main() {
    match run() {
        Ok(()) => std::process::exit(0),
        Err((code, msg)) => {
            eprintln!("\nerror: {msg}");
            std::process::exit(code);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use merutable_iceberg::snapshot::{IcebergDataFile, SnapshotTransaction};
    use merutable_types::level::{Level, ParquetFileMeta};
    use std::sync::Arc;

    fn test_schema() -> TableSchema {
        TableSchema {
            table_name: "mig-test".into(),
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

    fn test_meta(file_size: u64) -> ParquetFileMeta {
        ParquetFileMeta {
            level: Level(0),
            seq_min: 1,
            seq_max: 10,
            key_min: vec![0x01],
            key_max: vec![0xFF],
            num_rows: 100,
            file_size,
            dv_path: None,
            dv_offset: None,
            dv_length: None,
            format: None,
            column_stats: None,
        }
    }

    /// Phase 2 contract: given a POSIX catalog with N live data
    /// files, `build_plan_posix_source` reports the correct file
    /// count, total bytes, and snapshot id.
    #[tokio::test]
    async fn build_plan_reports_live_files() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema());
        let catalog = IcebergCatalog::open(tmp.path(), test_schema())
            .await
            .unwrap();
        // Commit two data files.
        let mut txn = SnapshotTransaction::new();
        for i in 0..3 {
            txn.add_file(IcebergDataFile {
                path: format!("data/L0/f{i}.parquet"),
                file_size: 2048,
                num_rows: 100,
                meta: test_meta(2048),
            });
        }
        catalog.commit(&txn, schema).await.unwrap();

        let plan = build_plan_posix_source(tmp.path().to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(plan.source_snapshot_id, 1);
        assert_eq!(plan.data_files.len(), 3);
        assert_eq!(plan.total_bytes, 3 * 2048);
        for e in &plan.data_files {
            assert!(e.path.starts_with("data/L0/"));
            assert_eq!(e.bytes, 2048);
            assert_eq!(e.num_rows, 100);
            assert_eq!(e.dv_path, None);
        }
    }

    /// Deleted entries must NOT surface in the plan. Migration
    /// destination starts at v1 with the current live state.
    #[tokio::test]
    async fn build_plan_skips_deleted_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema());
        let catalog = IcebergCatalog::open(tmp.path(), test_schema())
            .await
            .unwrap();
        let mut txn1 = SnapshotTransaction::new();
        txn1.add_file(IcebergDataFile {
            path: "data/L0/keep.parquet".into(),
            file_size: 1024,
            num_rows: 50,
            meta: test_meta(1024),
        });
        txn1.add_file(IcebergDataFile {
            path: "data/L0/drop.parquet".into(),
            file_size: 2048,
            num_rows: 100,
            meta: test_meta(2048),
        });
        catalog.commit(&txn1, schema.clone()).await.unwrap();

        let mut txn2 = SnapshotTransaction::new();
        txn2.remove_file("data/L0/drop.parquet".into());
        catalog.commit(&txn2, schema).await.unwrap();

        let plan = build_plan_posix_source(tmp.path().to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(plan.data_files.len(), 1);
        assert_eq!(plan.data_files[0].path, "data/L0/keep.parquet");
        assert_eq!(plan.total_bytes, 1024);
    }

    /// Phase 3: end-to-end POSIX → ObjectStore migration.
    /// Writes three parquet files under a POSIX catalog, runs the
    /// execution path, then opens the destination as an
    /// ObjectStoreCatalog and verifies HEAD=2 (genesis v1 +
    /// migration commit v2), every source file present on the
    /// destination, and the manifest entries match.
    #[tokio::test]
    async fn execute_posix_to_object_store_copies_files_and_commits() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();

        // Seed the source catalog with three real on-disk parquet
        // byte strings (not real parquet, but the migrator just
        // copies bytes, doesn't parse).
        let schema = Arc::new(test_schema());
        let catalog = IcebergCatalog::open(src.path(), test_schema())
            .await
            .unwrap();
        tokio::fs::create_dir_all(src.path().join("data/L0"))
            .await
            .unwrap();
        let mut txn = SnapshotTransaction::new();
        for i in 0..3 {
            let path = format!("data/L0/f{i}.parquet");
            let body = format!("pq-body-{i}").into_bytes();
            tokio::fs::write(src.path().join(&path), &body)
                .await
                .unwrap();
            txn.add_file(IcebergDataFile {
                path,
                file_size: body.len() as u64,
                num_rows: 100,
                meta: test_meta(body.len() as u64),
            });
        }
        catalog.commit(&txn, schema).await.unwrap();

        let plan = build_plan_posix_source(src.path().to_str().unwrap())
            .await
            .unwrap();
        let source_manifest = read_source_manifest(src.path().to_str().unwrap())
            .await
            .unwrap();

        let committed = execute_posix_to_object_store(
            src.path().to_str().unwrap(),
            dst.path().to_str().unwrap(),
            &plan,
            /*copy_parallelism=*/ 2,
            source_manifest.schema.clone(),
            &source_manifest.entries,
            /*resume=*/ false,
        )
        .await
        .unwrap();
        assert_eq!(committed, 2, "destination HEAD is genesis + migration");

        // Every source file must exist at the destination with
        // byte-equal contents.
        for i in 0..3 {
            let path = format!("data/L0/f{i}.parquet");
            let src_bytes = tokio::fs::read(src.path().join(&path)).await.unwrap();
            let dst_bytes = tokio::fs::read(dst.path().join(&path)).await.unwrap();
            assert_eq!(src_bytes, dst_bytes, "byte-equal at {path}");
        }

        // Reopening the destination as an ObjectStoreCatalog must
        // land on HEAD=2 with 3 live entries.
        let dst_store = Arc::new(LocalFileStore::new(dst.path()).unwrap());
        let dst_cat = ObjectStoreCatalog::open(dst_store, source_manifest.schema.clone())
            .await
            .unwrap();
        assert_eq!(dst_cat.head().await, 2);
        let m = dst_cat.current_manifest().await;
        assert_eq!(
            m.entries.iter().filter(|e| e.status != "deleted").count(),
            3
        );
    }

    /// Phase 4a: verify_destination passes when the destination
    /// bytes match source bytes and fails loudly when any file
    /// diverges.
    #[tokio::test]
    async fn verify_detects_destination_divergence() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(src.path().join("data/L0"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(dst.path().join("data/L0"))
            .await
            .unwrap();

        // Write three files to both directories; the third one
        // differs between src and dst.
        for i in 0..3 {
            let path = format!("data/L0/f{i}.parquet");
            let src_body = format!("body-{i}").into_bytes();
            tokio::fs::write(src.path().join(&path), &src_body)
                .await
                .unwrap();
            let dst_body = if i == 2 {
                b"CORRUPT".to_vec()
            } else {
                src_body.clone()
            };
            tokio::fs::write(dst.path().join(&path), &dst_body)
                .await
                .unwrap();
        }

        let plan = MigrationPlan {
            source_snapshot_id: 1,
            source_table_uuid: "fake".into(),
            data_files: (0..3)
                .map(|i| PlanEntry {
                    path: format!("data/L0/f{i}.parquet"),
                    bytes: 10,
                    num_rows: 1,
                    dv_path: None,
                })
                .collect(),
            total_bytes: 30,
        };

        let err = verify_destination(
            src.path().to_str().unwrap(),
            dst.path().to_str().unwrap(),
            &plan,
            2,
        )
        .await
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("f2.parquet"),
            "error must name divergent file: {msg}"
        );
        assert!(msg.contains("mismatch"), "error must name mismatch: {msg}");
    }

    /// Phase 4b: `--resume` against an already-migrated destination
    /// short-circuits and returns the existing HEAD instead of
    /// trying to re-commit. Rerunning a successful migration must
    /// therefore be idempotent.
    #[tokio::test]
    async fn resume_after_successful_migration_is_idempotent() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema());
        let catalog = IcebergCatalog::open(src.path(), test_schema())
            .await
            .unwrap();
        tokio::fs::create_dir_all(src.path().join("data/L0"))
            .await
            .unwrap();
        let mut txn = SnapshotTransaction::new();
        for i in 0..3 {
            let path = format!("data/L0/f{i}.parquet");
            tokio::fs::write(src.path().join(&path), format!("body-{i}"))
                .await
                .unwrap();
            txn.add_file(IcebergDataFile {
                path,
                file_size: 10,
                num_rows: 1,
                meta: test_meta(10),
            });
        }
        catalog.commit(&txn, schema).await.unwrap();

        let plan = build_plan_posix_source(src.path().to_str().unwrap())
            .await
            .unwrap();
        let source_manifest = read_source_manifest(src.path().to_str().unwrap())
            .await
            .unwrap();

        // First run: full migration lands at v2.
        let v1 = execute_posix_to_object_store(
            src.path().to_str().unwrap(),
            dst.path().to_str().unwrap(),
            &plan,
            2,
            source_manifest.schema.clone(),
            &source_manifest.entries,
            /*resume=*/ false,
        )
        .await
        .unwrap();
        assert_eq!(v1, 2);

        // Second run with --resume: short-circuits, returns same v.
        let v2 = execute_posix_to_object_store(
            src.path().to_str().unwrap(),
            dst.path().to_str().unwrap(),
            &plan,
            2,
            source_manifest.schema.clone(),
            &source_manifest.entries,
            /*resume=*/ true,
        )
        .await
        .unwrap();
        assert_eq!(v2, 2, "resume returns existing HEAD unchanged");
    }

    /// Phase 4b: --resume rejects a destination whose committed
    /// manifest diverges from the source (signals a cross-migration
    /// conflict that needs manual intervention).
    #[tokio::test]
    async fn resume_rejects_divergent_destination() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema());

        // Source has 3 files.
        let s_cat = IcebergCatalog::open(src.path(), test_schema())
            .await
            .unwrap();
        tokio::fs::create_dir_all(src.path().join("data/L0"))
            .await
            .unwrap();
        let mut s_txn = SnapshotTransaction::new();
        for i in 0..3 {
            let path = format!("data/L0/src_{i}.parquet");
            tokio::fs::write(src.path().join(&path), "x").await.unwrap();
            s_txn.add_file(IcebergDataFile {
                path,
                file_size: 1,
                num_rows: 1,
                meta: test_meta(1),
            });
        }
        s_cat.commit(&s_txn, schema.clone()).await.unwrap();

        // Destination pre-committed with different files.
        let dst_store = Arc::new(LocalFileStore::new(dst.path()).unwrap());
        let dst_cat = ObjectStoreCatalog::open(dst_store, test_schema())
            .await
            .unwrap();
        let mut d_txn = SnapshotTransaction::new();
        d_txn.add_file(IcebergDataFile {
            path: "data/L0/OTHER.parquet".into(),
            file_size: 1,
            num_rows: 1,
            meta: test_meta(1),
        });
        dst_cat.commit(&d_txn, schema.clone()).await.unwrap();

        let plan = build_plan_posix_source(src.path().to_str().unwrap())
            .await
            .unwrap();
        let source_manifest = read_source_manifest(src.path().to_str().unwrap())
            .await
            .unwrap();

        let err = execute_posix_to_object_store(
            src.path().to_str().unwrap(),
            dst.path().to_str().unwrap(),
            &plan,
            2,
            source_manifest.schema.clone(),
            &source_manifest.entries,
            /*resume=*/ true,
        )
        .await
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("file sets differ") || msg.contains("NOT a resumable"),
            "divergence error should be explicit: {msg}"
        );
    }

    /// Phase 4a: verify_destination passes when every file matches.
    #[tokio::test]
    async fn verify_passes_on_match() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(src.path().join("data/L0"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(dst.path().join("data/L0"))
            .await
            .unwrap();
        for i in 0..3 {
            let path = format!("data/L0/f{i}.parquet");
            let body = format!("body-{i}").into_bytes();
            tokio::fs::write(src.path().join(&path), &body)
                .await
                .unwrap();
            tokio::fs::write(dst.path().join(&path), &body)
                .await
                .unwrap();
        }

        let plan = MigrationPlan {
            source_snapshot_id: 1,
            source_table_uuid: "fake".into(),
            data_files: (0..3)
                .map(|i| PlanEntry {
                    path: format!("data/L0/f{i}.parquet"),
                    bytes: 10,
                    num_rows: 1,
                    dv_path: None,
                })
                .collect(),
            total_bytes: 30,
        };

        verify_destination(
            src.path().to_str().unwrap(),
            dst.path().to_str().unwrap(),
            &plan,
            2,
        )
        .await
        .unwrap();
    }

    /// Phase 3 refuses to migrate a source with live DVs (deferred
    /// to Phase 3b). The error must name the limitation. We bypass
    /// the catalog and hand-craft the manifest-entries slice so the
    /// DV field is guaranteed to be set regardless of how
    /// IcebergCatalog handles the pass-through.
    #[tokio::test]
    async fn execute_rejects_source_with_dvs() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();

        // Build a tiny fake manifest entry carrying a DV directly.
        let entries = vec![merutable_iceberg::manifest::ManifestEntry {
            path: "data/L0/fake.parquet".into(),
            meta: test_meta(1024),
            dv_path: Some("data/L0/fake.puffin".into()),
            dv_offset: Some(4),
            dv_length: Some(32),
            status: "added".into(),
        }];
        let plan = MigrationPlan {
            source_snapshot_id: 1,
            source_table_uuid: "fake".into(),
            data_files: vec![PlanEntry {
                path: "data/L0/fake.parquet".into(),
                bytes: 1024,
                num_rows: 100,
                dv_path: Some("data/L0/fake.puffin".into()),
            }],
            total_bytes: 1024,
        };

        let err = execute_posix_to_object_store(
            src.path().to_str().unwrap(),
            dst.path().to_str().unwrap(),
            &plan,
            2,
            test_schema(),
            &entries,
            /*resume=*/ false,
        )
        .await
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("deletion vectors") || msg.contains("DV"),
            "error should name the DV limitation: {msg}"
        );
    }
}
