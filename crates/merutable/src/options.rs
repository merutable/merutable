//! `OpenOptions`: the stable tuning surface for `MeruDB::open`.
//!
//! Every field here is a deliberate, documented knob. The internal
//! `EngineConfig` has additional fields, but callers should not reach
//! into `merutable-engine` directly — that crate is `publish = false`
//! for a reason (Issue #9).
//!
//! All knobs default to sane production values; builder methods let
//! you override individually. Unset knobs pass `EngineConfig::default()`
//! through.

pub use merutable_engine::config::CommitMode;
use merutable_types::schema::TableSchema;
use std::path::PathBuf;
use std::sync::Arc;

/// Issue #31: async object-store mirror of flushed files + manifests.
/// Attached to a `CommitMode::Posix` deployment; never to
/// `CommitMode::ObjectStore` (the latter IS the object-store commit
/// surface, so mirroring is redundant and would double-write every
/// manifest).
///
/// The mirror layout is byte-for-byte identical to the
/// `CommitMode::ObjectStore` layout (Issue #26), which means a remote
/// reader can open the mirror destination with
/// `OpenOptions::read_only(true) + CommitMode::ObjectStore` and see
/// the primary's committed state modulo `mirror_lag`.
///
/// ## Crash-loss model
///
/// The WAL is NEVER mirrored. A primary crash loses the un-flushed
/// in-memory tail. Readers on the mirror see the most recent
/// fully-mirrored snapshot. For stricter RPO, use
/// `CommitMode::ObjectStore` directly.
///
/// ## Phases
///
/// - Phase 1 (this type): struct, builder, validation. Creating a
///   `MirrorConfig` and attaching it compiles and round-trips through
///   `OpenOptions`; the mirror worker is not yet spawned.
/// - Phase 2 (planned): mirror worker spawned alongside flush +
///   compaction workers, commit-order-preserving upload loop.
/// - Phase 3 (planned): `mirror_seq` tracking via `stats()`.
/// - Phase 4 (planned): alert on `max_lag_alert_secs`.
#[derive(Clone)]
pub struct MirrorConfig {
    /// S3 / GCS / Azure destination. Must implement `MeruStore`.
    pub target: Arc<dyn merutable_store::traits::MeruStore>,
    /// Warn above this lag (seconds between primary commit_time and
    /// last-mirrored commit_time). Alert-only in v1; writes never
    /// block on mirror lag.
    pub max_lag_alert_secs: u64,
    /// Concurrent uploads during a single mirror sweep. Higher =
    /// faster catch-up after a sustained primary burst; higher also
    /// = more in-flight object-store connections. Default: 4.
    pub mirror_parallelism: usize,
}

impl std::fmt::Debug for MirrorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorConfig")
            .field("target", &"Arc<dyn MeruStore>")
            .field("max_lag_alert_secs", &self.max_lag_alert_secs)
            .field("mirror_parallelism", &self.mirror_parallelism)
            .finish()
    }
}

impl MirrorConfig {
    /// Production defaults for lag alert (60s) and parallelism (4).
    /// Callers must still provide `target`.
    pub fn new(target: Arc<dyn merutable_store::traits::MeruStore>) -> Self {
        Self {
            target,
            max_lag_alert_secs: 60,
            mirror_parallelism: 4,
        }
    }

    pub fn max_lag_alert_secs(mut self, secs: u64) -> Self {
        self.max_lag_alert_secs = secs;
        self
    }

    pub fn mirror_parallelism(mut self, n: usize) -> Self {
        self.mirror_parallelism = n.max(1);
        self
    }
}

/// Builder for opening a `MeruDB` instance.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    pub schema: TableSchema,
    pub catalog_uri: String,
    pub object_store_url: String,
    pub wal_dir: PathBuf,

    // Memtable
    pub memtable_size_mb: usize,
    pub max_immutable_count: usize,

    // Row cache
    pub row_cache_capacity: usize,

    // Compaction targets
    /// Per-level size targets for L1 and beyond, in bytes.
    /// `level_target_bytes[0]` is L1 target, `[1]` is L2, etc.
    /// Default: [256 MiB, 2 GiB, 16 GiB, 128 GiB].
    pub level_target_bytes: Vec<u64>,

    // L0 triggers
    pub l0_compaction_trigger: usize,
    pub l0_slowdown_trigger: usize,
    pub l0_stop_trigger: usize,

    // Parquet tuning
    pub bloom_bits_per_key: u8,

    // Compaction I/O cap
    pub max_compaction_bytes: u64,

    // Background parallelism
    pub flush_parallelism: usize,
    pub compaction_parallelism: usize,

    // GC grace
    pub gc_grace_period_secs: u64,

    // Lifecycle
    pub read_only: bool,

    /// Issue #15: highest level (inclusive) that carries the row-blob
    /// fast path. `Some(0)` matches the default; `None` = columnar
    /// everywhere; `Some(N)` pushes fast-path deeper for OLTP-heavy
    /// workloads.
    pub dual_format_max_level: Option<u8>,

    /// Issue #26: catalog commit strategy. `Posix` (default) uses
    /// atomic rename; `ObjectStore` uses conditional-PUT for
    /// S3/GCS/Azure correctness.
    pub commit_mode: CommitMode,

    /// Issue #31: optional async mirror to an object-store target.
    /// Valid only with `commit_mode = Posix`. See [`MirrorConfig`].
    pub mirror: Option<MirrorConfig>,
}

impl OpenOptions {
    /// Construct a builder with the given table schema and
    /// production defaults for every other field. The defaults come
    /// from `merutable_engine::config::EngineConfig::default()` — see
    /// that type for the authoritative values.
    pub fn new(schema: TableSchema) -> Self {
        // Pull defaults from EngineConfig so there's exactly one
        // place to change production constants.
        let ec = merutable_engine::config::EngineConfig::default();
        Self {
            schema,
            catalog_uri: String::new(),
            object_store_url: String::new(),
            wal_dir: ec.wal_dir,
            memtable_size_mb: ec.memtable_size_bytes / (1024 * 1024),
            max_immutable_count: ec.max_immutable_count,
            row_cache_capacity: ec.row_cache_capacity,
            level_target_bytes: ec.level_target_bytes,
            l0_compaction_trigger: ec.l0_compaction_trigger,
            l0_slowdown_trigger: ec.l0_slowdown_trigger,
            l0_stop_trigger: ec.l0_stop_trigger,
            bloom_bits_per_key: ec.bloom_bits_per_key,
            max_compaction_bytes: ec.max_compaction_bytes,
            flush_parallelism: ec.flush_parallelism,
            compaction_parallelism: ec.compaction_parallelism,
            gc_grace_period_secs: ec.gc_grace_period_secs,
            read_only: ec.read_only,
            dual_format_max_level: ec.dual_format_max_level,
            commit_mode: ec.commit_mode,
            mirror: None,
        }
    }

    /// Issue #31: attach an async mirror to an object-store target.
    /// Valid only with `commit_mode = Posix`. Passing a `MirrorConfig`
    /// when `commit_mode = ObjectStore` is rejected at `open()` —
    /// the two modes target the same layout and mirroring while
    /// writing directly to the object store would double-write every
    /// manifest under different paths.
    ///
    /// Phase 1 (today) validates the combination; phases 2–4 spawn
    /// the worker and surface lag metrics.
    pub fn mirror(mut self, cfg: MirrorConfig) -> Self {
        self.mirror = Some(cfg);
        self
    }

    /// Phase 1 validator for the #31 mirror knob. Returns `Err` if
    /// the combination is incoherent — today, that means
    /// `mirror.is_some() && commit_mode == ObjectStore`. Invoked by
    /// `MeruDB::open` before any I/O so configuration errors fail at
    /// the API boundary, not deep inside the engine.
    pub fn validate_mirror(&self) -> std::result::Result<(), String> {
        if self.mirror.is_some() && matches!(self.commit_mode, CommitMode::ObjectStore) {
            return Err("MirrorConfig requires `commit_mode = Posix`. \
                 `CommitMode::ObjectStore` already writes directly to the object store; \
                 mirroring would double-write every manifest and corrupt the \
                 conditional-PUT chain. Either drop the mirror or switch to Posix."
                .into());
        }
        Ok(())
    }

    /// Issue #26: select the catalog commit mode.
    ///
    /// - [`CommitMode::Posix`] (default): atomic rename-based commits.
    ///   Correct on a local filesystem. Do NOT use on S3 / GCS / Azure
    ///   Blob — a POSIX-emulated layer over those object stores has no
    ///   atomic rename and can silently lose commits when writers race.
    /// - [`CommitMode::ObjectStore`]: single-file conditional-PUT
    ///   commits. Required for S3 / GCS / Azure. The implementation
    ///   lands in phases; selecting this mode currently errors at
    ///   `open` with `Unsupported` until the phase-2 protobuf-manifest
    ///   + put-if-absent plumbing ships.
    pub fn commit_mode(mut self, mode: CommitMode) -> Self {
        self.commit_mode = mode;
        self
    }

    /// Issue #15: highest LSM level whose SSTables carry the
    /// `_merutable_value` row-blob fast path. `Some(0)` (default)
    /// matches the pre-Issue-#15 hard boundary; `Some(N)` pushes the
    /// fast path to L0..=LN for OLTP-heavy workloads; `None` = every
    /// level columnar-only for OLAP / append-only.
    pub fn dual_format_max_level(mut self, max: Option<u8>) -> Self {
        self.dual_format_max_level = max;
        self
    }

    pub fn catalog_uri(mut self, uri: impl Into<String>) -> Self {
        self.catalog_uri = uri.into();
        self
    }

    pub fn object_store(mut self, url: impl Into<String>) -> Self {
        self.object_store_url = url.into();
        self
    }

    pub fn wal_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.wal_dir = dir.into();
        self
    }

    pub fn memtable_size_mb(mut self, mb: usize) -> Self {
        self.memtable_size_mb = mb;
        self
    }

    /// Maximum number of rotated memtables waiting to be flushed
    /// before writes hard-stall. Default: 4.
    pub fn max_immutable_count(mut self, n: usize) -> Self {
        self.max_immutable_count = n;
        self
    }

    pub fn row_cache_capacity(mut self, capacity: usize) -> Self {
        self.row_cache_capacity = capacity;
        self
    }

    /// Per-level byte targets for L1..LN. Index 0 = L1. Default:
    /// `[256 MiB, 2 GiB, 16 GiB, 128 GiB]`.
    pub fn level_target_bytes(mut self, targets: Vec<u64>) -> Self {
        self.level_target_bytes = targets;
        self
    }

    /// L0 file count that triggers a compaction. Default: 4.
    pub fn l0_compaction_trigger(mut self, n: usize) -> Self {
        self.l0_compaction_trigger = n;
        self
    }

    /// L0 file count at which writes begin graduated slowdown.
    /// Default: 20.
    pub fn l0_slowdown_trigger(mut self, n: usize) -> Self {
        self.l0_slowdown_trigger = n;
        self
    }

    /// L0 file count at which writes hard-stop until compaction
    /// drains L0. Default: 36.
    pub fn l0_stop_trigger(mut self, n: usize) -> Self {
        self.l0_stop_trigger = n;
        self
    }

    /// Bits per key for the SIMD bloom filter stored in Parquet footer
    /// KV metadata. Higher = smaller false-positive rate, more bytes.
    /// Default: 10 (~1% FPR).
    pub fn bloom_bits_per_key(mut self, bits: u8) -> Self {
        self.bloom_bits_per_key = bits;
        self
    }

    /// Upper bound on per-compaction input bytes. Prevents a single
    /// deep-level compaction from pulling multi-GiB into memory.
    /// Default: 256 MiB. See Issue #2.
    pub fn max_compaction_bytes(mut self, bytes: u64) -> Self {
        self.max_compaction_bytes = bytes;
        self
    }

    /// Number of background flush workers. Default: 1.
    /// `0` disables the auto-flush background loop (manual
    /// `flush()` calls still work).
    pub fn flush_parallelism(mut self, n: usize) -> Self {
        self.flush_parallelism = n;
        self
    }

    /// Number of background compaction workers. Default: 2.
    /// Workers run on disjoint level sets in parallel. `0`
    /// disables the auto-compaction background loop.
    pub fn compaction_parallelism(mut self, n: usize) -> Self {
        self.compaction_parallelism = n;
        self
    }

    /// Seconds to retain compaction-obsoleted files before GC. Gives
    /// external HTAP readers (DuckDB, Spark) time to finish mid-read.
    /// Default: 300 (5 minutes). Internal readers use version-pin
    /// refcounting and are NOT bounded by this timer.
    pub fn gc_grace_period_secs(mut self, secs: u64) -> Self {
        self.gc_grace_period_secs = secs;
        self
    }

    pub fn read_only(mut self, enabled: bool) -> Self {
        self.read_only = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use merutable_store::local::LocalFileStore;
    use merutable_types::schema::{ColumnDef, ColumnType};

    fn schema() -> TableSchema {
        TableSchema {
            table_name: "mirror-test".into(),
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
    fn mirror_defaults_are_production_sane() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let cfg = MirrorConfig::new(store);
        assert_eq!(cfg.max_lag_alert_secs, 60);
        assert_eq!(cfg.mirror_parallelism, 4);
    }

    #[test]
    fn mirror_parallelism_floored_at_one() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let cfg = MirrorConfig::new(store).mirror_parallelism(0);
        assert_eq!(cfg.mirror_parallelism, 1, "zero coerced to one");
    }

    #[test]
    fn mirror_with_posix_passes_validation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let opts = OpenOptions::new(schema())
            .commit_mode(CommitMode::Posix)
            .mirror(MirrorConfig::new(store));
        assert!(opts.validate_mirror().is_ok());
    }

    #[test]
    fn mirror_with_object_store_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(tmp.path()).unwrap());
        let opts = OpenOptions::new(schema())
            .commit_mode(CommitMode::ObjectStore)
            .mirror(MirrorConfig::new(store));
        let err = opts.validate_mirror().unwrap_err();
        assert!(
            err.contains("Posix") && err.contains("ObjectStore"),
            "error must name both modes: {err}"
        );
    }

    #[test]
    fn no_mirror_no_validation_error() {
        // Both modes should pass validation when no mirror is set.
        let opts_posix = OpenOptions::new(schema()).commit_mode(CommitMode::Posix);
        assert!(opts_posix.validate_mirror().is_ok());
        let opts_os = OpenOptions::new(schema()).commit_mode(CommitMode::ObjectStore);
        assert!(opts_os.validate_mirror().is_ok());
    }
}
