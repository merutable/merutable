//! Engine statistics types for introspection.
//!
//! `MeruEngine::stats()` returns an `EngineStats` snapshot. Construction is
//! lock-free on the version side (ArcSwap load) and takes a brief read lock
//! on the memtable manager — zero overhead on the hot path.

/// Per-file statistics.
#[derive(Debug, Clone)]
pub struct FileStats {
    pub path: String,
    pub file_size: u64,
    pub num_rows: u64,
    pub seq_range: (u64, u64),
    pub has_dv: bool,
}

/// Per-level statistics.
#[derive(Debug, Clone)]
pub struct LevelStats {
    pub level: u8,
    pub file_count: usize,
    pub total_bytes: u64,
    pub total_rows: u64,
    pub files: Vec<FileStats>,
}

/// Memtable statistics.
#[derive(Debug, Clone)]
pub struct MemtableStats {
    pub active_size_bytes: usize,
    pub active_entry_count: u64,
    pub flush_threshold: usize,
    pub immutable_count: usize,
}

/// Row cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub capacity: usize,
    pub size: usize,
    pub hit_count: u64,
    pub miss_count: u64,
}

/// GC-queue statistics. Issue #30 observability hook: under sustained
/// writes with aggressive compaction, the pending-deletions queue can
/// grow faster than `gc_grace_period_secs` drains it if external external analytics
/// readers hold the time-based grace. Operators correlate
/// `pending_count` growth with RSS spikes.
#[derive(Debug, Clone)]
pub struct GcStats {
    /// Number of files currently awaiting deletion (pinned by
    /// snapshot or still within `gc_grace_period_secs`).
    pub pending_count: usize,
    /// Oldest pending entry's age in seconds. Useful signal: a long
    /// tail here means GC keeps deferring the same files every sweep
    /// (likely a long-running snapshot pin).
    pub oldest_pending_age_secs: u64,
}

/// Compaction concurrency statistics. Issue #30 observability:
/// multiple in-flight compactions each buffer decoded rows for
/// their input files until the output Parquet writes complete.
/// `inflight_count` correlates with the ratio of RSS to logical
/// data written — if it spikes past `compaction_parallelism` for
/// extended periods, a compaction is stuck and its row buffer
/// is not being reclaimed.
#[derive(Debug, Clone)]
pub struct CompactionStats {
    /// Number of levels currently reserved by an in-flight
    /// compaction (matches `|compacting_levels|`, where a single
    /// compaction reserves both its input and output levels so
    /// this reaches `2 * compaction_parallelism` at peak).
    pub inflight_levels: usize,
}

/// Top-level engine statistics snapshot.
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub snapshot_id: i64,
    pub current_seq: u64,
    pub levels: Vec<LevelStats>,
    pub memtable: MemtableStats,
    pub cache: CacheStats,
    pub gc: GcStats,
    pub compaction: CompactionStats,
}
