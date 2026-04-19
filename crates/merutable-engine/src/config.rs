use merutable_types::level::{FileFormat, Level};
use merutable_types::schema::TableSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Issue #26: how the catalog commits a new metadata version.
///
/// - `Posix` (default, today's behavior): atomic `rename(2)` against a
///   filesystem. Correct on a local FS. Structurally unsafe on S3,
///   GCS, or Azure Blob — those object stores have no atomic rename,
///   and a POSIX-emulated layer can silently lose a commit when two
///   writers race.
/// - `ObjectStore`: single-file conditional-PUT commits against a
///   `MeruStore`-backed object store. Maps to S3 `If-None-Match: *`,
///   GCS `x-goog-if-generation-match: 0`, Azure `If-None-Match: *`.
///   Protobuf-encoded manifests with a backward-pointer chain;
///   HEAD discovered via exponential probe + binary search; no LIST
///   on the commit or refresh path.
///
/// Default is `Posix` — zero behavior change for existing users.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitMode {
    /// POSIX atomic rename. Correct on a local filesystem.
    #[default]
    Posix,
    /// Single-file conditional PUT. Correct on S3 / GCS / Azure Blob.
    ///
    /// Implementation is landing incrementally (Issue #26). Today the
    /// selection is a type-shape freeze pre-0.1-preview: constructing
    /// an engine with `ObjectStore` mode errors with `Unsupported`
    /// until Phase 2 (protobuf manifest + conditional PUT plumbing)
    /// lands.
    ObjectStore,
}

/// All tuning parameters for a `MeruEngine` instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub schema: TableSchema,
    pub catalog_uri: String,
    pub object_store_prefix: String,
    pub wal_dir: PathBuf,

    // Memtable
    /// Flush threshold in bytes. Default: 64 MiB.
    pub memtable_size_bytes: usize,
    /// Max number of immutable memtables before write stall. Default: 4.
    pub max_immutable_count: usize,

    // Row cache
    /// Row cache capacity (number of rows). 0 = disabled. Default: 10_000.
    pub row_cache_capacity: usize,

    // Compaction
    /// Target bytes per level for L1..LN. Index 0 = L1 target.
    /// Default: [256 MiB, 2 GiB, 16 GiB, 128 GiB].
    pub level_target_bytes: Vec<u64>,
    /// Number of L0 files that triggers a compaction. Default: 4.
    pub l0_compaction_trigger: usize,
    /// Number of L0 files that slows writes (1 ms sleep per write). Default: 20.
    pub l0_slowdown_trigger: usize,
    /// Number of L0 files that stops writes entirely. Default: 36.
    pub l0_stop_trigger: usize,

    // Bloom filter
    /// Bits per key for the Parquet-column bloom filter. Default: 10.
    pub bloom_bits_per_key: u8,

    // Compaction I/O
    /// Max bytes written per compaction run before splitting output files. Default: 256 MiB.
    pub max_compaction_bytes: u64,

    /// Issue #30: upper bound on the total ROW count a single
    /// compaction may ingest from its inputs. `0` disables the
    /// cap (back-compat default). Non-zero values bound the
    /// decoded-row memory footprint per compaction — a Parquet
    /// file that compresses ~4× expands on decode, so
    /// `max_compaction_bytes` alone doesn't bound peak memory.
    /// Operators hitting the #30 RSS-2.6x symptom should set
    /// this to cap the pathological case; a reasonable starting
    /// point is `max_compaction_bytes / avg_row_bytes` where
    /// `avg_row_bytes` is measured from the current workload.
    /// The picker enforces this alongside `max_compaction_bytes`;
    /// a compaction that would exceed either cap is skipped.
    pub max_compaction_input_rows: u64,

    // Background parallelism
    pub flush_parallelism: usize,
    pub compaction_parallelism: usize,

    /// Open in read-only mode. No WAL, no memtable writes. Default: false.
    pub read_only: bool,

    /// IMP-12: minimum age (in seconds) before compaction-obsoleted files are
    /// physically deleted. External readers (DuckDB, Spark) that resolved an
    /// older snapshot may still be mid-read of the old files; deleting them
    /// causes read failures. Default: 300 (5 minutes). Set to 0 for tests.
    pub gc_grace_period_secs: u64,

    /// Issue #26: selects the catalog commit strategy. See
    /// [`CommitMode`] for semantics. Default `Posix`.
    pub commit_mode: CommitMode,

    /// Issue #15: highest LSM level (inclusive) whose SSTables carry
    /// the row-blob fast-path (`_merutable_value`) alongside typed
    /// columns. Levels beyond this carry typed columns only.
    ///
    /// - `Some(0)` — L0 dual, L1+ columnar-only. Default; matches
    ///   the pre-Issue-#15 hard-coded behavior (HTAP generic bias).
    /// - `Some(N)` — L0..=LN dual, LN+1+ columnar-only (OLTP-leaning,
    ///   push fast-path deeper so hot keys at L2/L3 resolve in a
    ///   single column-chunk decode).
    /// - `None`    — every level columnar-only (OLAP / append-only;
    ///   saves bytes across the whole tree).
    ///
    /// Changing this at runtime affects NEW compactions only.
    /// Existing files retain their write-time format (stamped in
    /// `ParquetFileMeta::format`).
    pub dual_format_max_level: Option<u8>,
}

impl EngineConfig {
    /// Issue #15: the physical format that a NEWLY-WRITTEN file at
    /// `output_level` should use. Called by flush and compaction
    /// when handing off to `write_sorted_rows`.
    #[inline]
    pub fn file_format_for(&self, output_level: Level) -> FileFormat {
        match self.dual_format_max_level {
            Some(max) if output_level.0 <= max => FileFormat::Dual,
            _ => FileFormat::Columnar,
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            // Issue #25: TableSchema is #[non_exhaustive] — use builder.
            schema: TableSchema::builder(String::new()).build(),
            catalog_uri: String::new(),
            object_store_prefix: String::new(),
            wal_dir: PathBuf::from("./meru-wal"),
            memtable_size_bytes: 64 * 1024 * 1024,
            max_immutable_count: 4,
            row_cache_capacity: 10_000,
            level_target_bytes: vec![
                256 * 1024 * 1024,
                2 * 1024 * 1024 * 1024,
                16 * 1024 * 1024 * 1024,
                128 * 1024 * 1024 * 1024,
            ],
            l0_compaction_trigger: 4,
            l0_slowdown_trigger: 20,
            l0_stop_trigger: 36,
            bloom_bits_per_key: 10,
            max_compaction_bytes: 256 * 1024 * 1024,
            // Issue #30: default 0 (unbounded) preserves back-
            // compat. Operators hitting the RSS-2.6x symptom set
            // this to cap decoded-row memory per compaction.
            max_compaction_input_rows: 0,
            flush_parallelism: 1,
            compaction_parallelism: 2,
            read_only: false,
            gc_grace_period_secs: 300,
            // Issue #26: Posix (today's rename-based commit) is the
            // default; ObjectStore mode opts into conditional-PUT.
            commit_mode: CommitMode::Posix,
            // Default matches the pre-Issue-#15 hard-coded behavior.
            dual_format_max_level: Some(0),
        }
    }
}
