//! `Version` (immutable LSM state snapshot) and `VersionSet` (lock-free readers).
//!
//! A `Version` is a point-in-time view of which Parquet files exist at each
//! LSM level, along with their metadata and DV state. It maps 1:1 with an
//! Iceberg snapshot.
//!
//! `VersionSet` wraps an `ArcSwap<Version>` so readers can grab a guard
//! without any lock. Writers hold a `Mutex` only during Iceberg snapshot
//! commit + version swap.

use std::{collections::HashMap, sync::Arc};

use arc_swap::{ArcSwap, Guard};
use merutable_types::{
    level::{Level, ParquetFileMeta},
    schema::TableSchema,
};

// ── DataFileMeta ─────────────────────────────────────────────────────────────

/// Metadata for a single Parquet data file in the LSM, as known to the
/// version layer. Combines the Parquet footer metadata with DV info.
#[derive(Clone, Debug)]
pub struct DataFileMeta {
    /// Object-store path of the Parquet file.
    pub path: String,
    /// Decoded from the `"merutable.meta"` KV footer.
    pub meta: ParquetFileMeta,
    /// Path to the `.puffin` DV file, if any.
    pub dv_path: Option<String>,
    /// Byte offset of the DV blob within the `.puffin` file.
    pub dv_offset: Option<i64>,
    /// Byte length of the DV blob.
    pub dv_length: Option<i64>,
}

impl DataFileMeta {
    /// True if this file has an associated Deletion Vector.
    pub fn has_dv(&self) -> bool {
        self.dv_path.is_some()
    }
}

// ── Version ──────────────────────────────────────────────────────────────────

/// Immutable snapshot of the LSM state at one Iceberg snapshot.
///
/// - L0: files may overlap in key range. Sorted by `seq_max` descending
///   (newest first) so that point lookups find the most recent version first.
/// - L1+: files are non-overlapping within each level. Sorted by `key_min`
///   ascending for binary-search point lookup.
#[derive(Clone, Debug)]
pub struct Version {
    /// Iceberg snapshot ID that produced this version.
    pub snapshot_id: i64,
    /// Per-level file list. Keys: all levels that have files.
    pub levels: HashMap<Level, Vec<DataFileMeta>>,
    /// Table schema at this version.
    pub schema: Arc<TableSchema>,
}

impl Version {
    /// Create an empty initial version (no files at any level).
    pub fn empty(schema: Arc<TableSchema>) -> Self {
        Self {
            snapshot_id: 0,
            levels: HashMap::new(),
            schema,
        }
    }

    /// All files at a given level (empty slice if level has no files).
    pub fn files_at(&self, level: Level) -> &[DataFileMeta] {
        self.levels.get(&level).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Number of L0 files. Used for compaction scoring.
    pub fn l0_file_count(&self) -> usize {
        self.files_at(Level(0)).len()
    }

    /// Total bytes at a given level. Used for compaction scoring.
    pub fn level_bytes(&self, level: Level) -> u64 {
        self.files_at(level).iter().map(|f| f.meta.file_size).sum()
    }

    /// Maximum level that has files.
    pub fn max_level(&self) -> Level {
        self.levels.keys().copied().max().unwrap_or(Level(0))
    }

    /// Find the L1+ file whose key range contains `user_key_bytes`.
    /// Returns `None` if no file at this level covers the key.
    /// Precondition: level >= 1 (files are non-overlapping, sorted by key_min).
    pub fn find_file_for_key(&self, level: Level, user_key_bytes: &[u8]) -> Option<&DataFileMeta> {
        let files = self.files_at(level);
        if files.is_empty() {
            return None;
        }
        // Binary search: find rightmost file where key_min <= user_key_bytes.
        let idx = files.partition_point(|f| f.meta.key_min.as_slice() <= user_key_bytes);
        if idx == 0 {
            return None;
        }
        let candidate = &files[idx - 1];
        // Check key_max.
        if user_key_bytes <= candidate.meta.key_max.as_slice() {
            Some(candidate)
        } else {
            None
        }
    }

    /// Total number of files across all levels.
    pub fn total_files(&self) -> usize {
        self.levels.values().map(|v| v.len()).sum()
    }
}

// ── VersionSet ───────────────────────────────────────────────────────────────

/// Lock-free version management. Readers grab an `Arc<Version>` via
/// `ArcSwap::load()`. Writers swap in a new version after committing
/// an Iceberg snapshot transaction.
pub struct VersionSet {
    current: ArcSwap<Version>,
}

impl VersionSet {
    /// Create a new `VersionSet` with an initial version.
    pub fn new(initial: Version) -> Self {
        Self {
            current: ArcSwap::from_pointee(initial),
        }
    }

    /// Get the current version. The returned `Guard` derefs to `Arc<Version>`
    /// and keeps the version alive as long as it's held.
    pub fn current(&self) -> Guard<Arc<Version>> {
        self.current.load()
    }

    /// Atomically install a new version. Called after an Iceberg snapshot
    /// commit succeeds.
    pub fn install(&self, version: Version) {
        self.current.store(Arc::new(version));
    }

    /// Get the current snapshot ID.
    pub fn snapshot_id(&self) -> i64 {
        self.current.load().snapshot_id
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use merutable_types::{
        level::{Level, ParquetFileMeta},
        schema::{ColumnDef, ColumnType, TableSchema},
    };

    fn test_schema() -> Arc<TableSchema> {
        Arc::new(TableSchema {
            table_name: "test".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Int64,
                    nullable: false,

                    ..Default::default()
                },
                ColumnDef {
                    name: "val".into(),
                    col_type: ColumnType::ByteArray,
                    nullable: true,

                    ..Default::default()
                },
            ],
            primary_key: vec![0],

            ..Default::default()
        })
    }

    fn make_file(
        path: &str,
        level: u8,
        key_min: &[u8],
        key_max: &[u8],
        seq_min: u64,
        seq_max: u64,
        num_rows: u64,
    ) -> DataFileMeta {
        DataFileMeta {
            path: path.to_string(),
            meta: ParquetFileMeta {
                level: Level(level),
                seq_min,
                seq_max,
                key_min: key_min.to_vec(),
                key_max: key_max.to_vec(),
                num_rows,
                file_size: num_rows * 100,
                dv_path: None,
                dv_offset: None,
                dv_length: None,
                format: None,
                column_stats: None,
            },
            dv_path: None,
            dv_offset: None,
            dv_length: None,
        }
    }

    #[test]
    fn empty_version() {
        let v = Version::empty(test_schema());
        assert_eq!(v.l0_file_count(), 0);
        assert_eq!(v.total_files(), 0);
        assert_eq!(v.max_level(), Level(0));
    }

    #[test]
    fn l0_file_count() {
        let mut v = Version::empty(test_schema());
        v.levels.insert(
            Level(0),
            vec![
                make_file("f1.parquet", 0, b"\x01", b"\x05", 1, 10, 100),
                make_file("f2.parquet", 0, b"\x03", b"\x08", 11, 20, 200),
            ],
        );
        assert_eq!(v.l0_file_count(), 2);
    }

    #[test]
    fn find_file_for_key_l1() {
        let mut v = Version::empty(test_schema());
        // L1: three non-overlapping files sorted by key_min.
        v.levels.insert(
            Level(1),
            vec![
                make_file("a.parquet", 1, b"\x01", b"\x03", 1, 10, 100),
                make_file("b.parquet", 1, b"\x05", b"\x08", 1, 10, 100),
                make_file("c.parquet", 1, b"\x0A", b"\x0F", 1, 10, 100),
            ],
        );

        // Key 0x02 → file "a"
        assert_eq!(
            v.find_file_for_key(Level(1), &[0x02]).unwrap().path,
            "a.parquet"
        );
        // Key 0x06 → file "b"
        assert_eq!(
            v.find_file_for_key(Level(1), &[0x06]).unwrap().path,
            "b.parquet"
        );
        // Key 0x0C → file "c"
        assert_eq!(
            v.find_file_for_key(Level(1), &[0x0C]).unwrap().path,
            "c.parquet"
        );
        // Key 0x04 → gap between a and b
        assert!(v.find_file_for_key(Level(1), &[0x04]).is_none());
        // Key 0x00 → before all files
        assert!(v.find_file_for_key(Level(1), &[0x00]).is_none());
        // Key 0xFF → after all files
        assert!(v.find_file_for_key(Level(1), &[0xFF]).is_none());
    }

    #[test]
    fn version_set_swap() {
        let schema = test_schema();
        let v1 = Version::empty(schema.clone());
        let vs = VersionSet::new(v1);
        assert_eq!(vs.snapshot_id(), 0);

        let mut v2 = Version::empty(schema);
        v2.snapshot_id = 42;
        v2.levels.insert(
            Level(0),
            vec![make_file("new.parquet", 0, b"\x01", b"\xFF", 1, 50, 500)],
        );
        vs.install(v2);

        assert_eq!(vs.snapshot_id(), 42);
        let guard = vs.current();
        assert_eq!(guard.l0_file_count(), 1);
    }

    #[test]
    fn level_bytes() {
        let mut v = Version::empty(test_schema());
        v.levels.insert(
            Level(1),
            vec![
                make_file("a.parquet", 1, b"\x01", b"\x05", 1, 10, 100),
                make_file("b.parquet", 1, b"\x06", b"\x0A", 1, 10, 200),
            ],
        );
        // Each file: num_rows * 100 bytes.
        assert_eq!(v.level_bytes(Level(1)), 100 * 100 + 200 * 100);
        assert_eq!(v.level_bytes(Level(2)), 0);
    }
}
