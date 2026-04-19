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
//! - Phase 2 (this commit): `--dry-run` reads a POSIX source catalog
//!   and prints the migration plan — file paths, sizes, total bytes,
//!   planned v1 genesis shape. NO writes against the destination.
//!   Exits 0 on a well-formed plan.
//! - Phase 3 (planned): actual copy (`MeruStore::put` streaming,
//!   configurable parallelism) + destination genesis write via
//!   `ObjectStoreCatalog::open` + `commit`.
//! - Phase 4 (planned): `--resume`, `--verify`, `--keep-source` /
//!   `--delete-source` lifecycle.

use clap::{Parser, ValueEnum};
use merutable_iceberg::{manifest::Manifest, IcebergCatalog};
use merutable_types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    Result,
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

    // Destination-write path: Phase 3. For Phase 2, if the caller
    // is not in --dry-run, refuse rather than silently exit success.
    if matches!(args.to, Layout::ObjectStore) && !args.dry_run {
        return Err((
            6,
            "destination `object-store` write is Issue #27 Phase 3 (not yet implemented). \
             The --dry-run plan above shows what WOULD be migrated; re-run with --dry-run \
             to see it without the error. \
             Track: https://github.com/merutable/merutable/issues/27"
                .to_string(),
        ));
    }

    if args.dry_run {
        eprintln!(
            "dry run complete — {} files would be migrated.",
            plan.data_files.len()
        );
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
}
