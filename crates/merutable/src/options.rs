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

use merutable_types::schema::TableSchema;
use std::path::PathBuf;

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
        }
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
