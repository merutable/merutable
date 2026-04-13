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

/// Top-level engine statistics snapshot.
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub snapshot_id: i64,
    pub current_seq: u64,
    pub levels: Vec<LevelStats>,
    pub memtable: MemtableStats,
}
