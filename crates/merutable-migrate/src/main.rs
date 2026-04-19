//! `merutable-migrate` — one-shot layout/commit-protocol migrator.
//!
//! Issue #27. The tool converts a merutable deployment from one
//! `CommitMode` layout to another. The headline case is
//! `--from posix --to object-store`: an existing POSIX (atomic-
//! rename) layout → a single-file conditional-PUT layout suitable
//! for S3 / GCS / Azure Blob.
//!
//! # Status
//!
//! Phase 1 surface (this binary): CLI argument shape is locked so
//! downstream operators can script against it. Every invocation
//! currently exits with an "Issue #26 Phase 2 pending" message
//! because the destination `CommitMode::ObjectStore` has no working
//! write path yet.
//!
//! When Issue #26 Phase 2 (protobuf manifest + conditional PUT) and
//! Phase 3 (HEAD discovery + inline GC) land, the body of this
//! binary fills in:
//!
//! 1. Open source read-only with `CommitMode::Posix`.
//! 2. Enumerate source Parquet + Puffin DV artifacts from the
//!    current manifest.
//! 3. Stream each file to destination via `MeruStore::put`
//!    (`--copy-parallelism` configurable).
//! 4. Emit destination genesis `metadata/v1.manifest.bin` via
//!    `put_if_absent` (#26's atomic-create primitive).
//! 5. Optional `--verify` re-reads + checksums every destination
//!    file.
//! 6. Optional `--dry-run` prints the plan without PUTs.
//! 7. Optional `--resume` picks up from `MIGRATION_INCOMPLETE.json`.
//!
//! Snapshot history is NOT preserved across migration; destination
//! starts at v1 with the latest source state. This is documented in
//! `--help` and printed as a banner on every run.

use clap::{Parser, ValueEnum};

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

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Reject same-layout runs — that's almost always a mistake.
    if args.from == args.to {
        eprintln!(
            "error: --from and --to are both `{:?}`. Same-layout migrations are \
             no-ops; use the engine directly to copy data. Aborting.",
            args.from
        );
        std::process::exit(2);
    }

    // Print the migration plan.
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
        args.delete_source,
    );
    eprintln!(
        "warning: snapshot history is NOT preserved. Destination starts at v1 \
         with the latest source state."
    );

    // Phase 1 gating: destination `ObjectStore` mode is not yet
    // implemented (Issue #26 Phase 2 pending).
    if matches!(args.to, Layout::ObjectStore) {
        eprintln!(
            "\nerror: --to object-store is not yet implemented.\n\
             \n\
             This tool depends on Issue #26 Phase 2 (protobuf manifest +\n\
             conditional-PUT `put_if_absent`) and Phase 3 (HEAD discovery\n\
             + inline manifest GC). The CLI argument shape above is stable;\n\
             the behavioral implementation lands alongside #26.\n\
             \n\
             Track: https://github.com/merutable/merutable/issues/26\n\
             Track: https://github.com/merutable/merutable/issues/27\n"
        );
        std::process::exit(3);
    }

    // Source `ObjectStore` read path also depends on Issue #26.
    if matches!(args.from, Layout::ObjectStore) {
        eprintln!(
            "\nerror: --from object-store is not yet implemented (Issue #26 Phase 3 pending).\n"
        );
        std::process::exit(3);
    }

    unreachable!(
        "from == to was rejected earlier; only ObjectStore branches remain, \
         both of which exit with a phase-2 message above"
    );
}
