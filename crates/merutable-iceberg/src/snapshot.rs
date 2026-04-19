//! `SnapshotTransaction`: builder for an atomic Iceberg snapshot change.
//!
//! Built incrementally during `FlushJob` or `CompactionJob`, then committed
//! atomically via the catalog. Each transaction describes:
//!
//! - **adds**:    new Parquet files produced by this job
//! - **removes**: files fully compacted (DV covers all rows → safe to drop)
//! - **dvs**:     partial compaction DV updates (file path → DV to merge)
//! - **props**:   snapshot summary properties

use std::collections::HashMap;

use merutable_types::level::ParquetFileMeta;

use crate::deletion_vector::DeletionVector;

// ── IcebergDataFile ──────────────────────────────────────────────────────────

/// Describes a new Parquet file to be added to the Iceberg manifest.
#[derive(Clone, Debug)]
pub struct IcebergDataFile {
    /// Object-store path (e.g. `data/L0/abc123.parquet`).
    pub path: String,
    /// Byte size of the Parquet file.
    pub file_size: u64,
    /// Number of rows in the file.
    pub num_rows: u64,
    /// Merutable-level metadata (also embedded in the Parquet KV footer).
    pub meta: ParquetFileMeta,
}

// ── SnapshotTransaction ──────────────────────────────────────────────────────

/// Describes a single atomic change to the Iceberg table state.
///
/// After building, pass to `IcebergCatalog::commit_transaction()` to:
/// 1. Add new `DataFile` entries for `adds`.
/// 2. Remove `DataFile` entries for `removes`.
/// 3. Upload merged `.puffin` files for `dvs` and attach to existing `DataFile`.
/// 4. Commit the Iceberg snapshot with summary `props`.
#[derive(Clone, Debug)]
pub struct SnapshotTransaction {
    /// New Parquet files produced by this flush/compaction.
    pub adds: Vec<IcebergDataFile>,
    /// Paths of fully-compacted files to remove from the manifest.
    /// A file is removable when `dv.cardinality() == file.num_rows`.
    pub removes: Vec<String>,
    /// Partial-compaction DV updates. Key = Parquet file path.
    /// The DV here is the *additional* set of row positions to mark deleted.
    /// The commit path must `union_with()` any pre-existing DV before uploading.
    pub dvs: HashMap<String, DeletionVector>,
    /// Snapshot summary properties (e.g. `"merutable.job" = "flush"`).
    pub props: HashMap<String, String>,
}

impl SnapshotTransaction {
    /// Create an empty transaction.
    pub fn new() -> Self {
        Self {
            adds: Vec::new(),
            removes: Vec::new(),
            dvs: HashMap::new(),
            props: HashMap::new(),
        }
    }

    /// Add a newly written Parquet file to the transaction.
    pub fn add_file(&mut self, file: IcebergDataFile) {
        self.adds.push(file);
    }

    /// Mark a file path for removal from the manifest (fully compacted).
    pub fn remove_file(&mut self, path: String) {
        self.removes.push(path);
    }

    /// Record additional deleted row positions for a Parquet file.
    /// If the file already has pending DV changes in this transaction, merge.
    pub fn add_dv(&mut self, path: String, dv: DeletionVector) {
        self.dvs
            .entry(path)
            .and_modify(|existing| existing.union_with(&dv))
            .or_insert(dv);
    }

    /// Set a snapshot summary property.
    pub fn set_prop(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.props.insert(key.into(), value.into());
    }

    /// True if this transaction makes no changes.
    pub fn is_empty(&self) -> bool {
        self.adds.is_empty() && self.removes.is_empty() && self.dvs.is_empty()
    }

    /// Total number of new files being added.
    pub fn num_adds(&self) -> usize {
        self.adds.len()
    }

    /// Total number of files being removed.
    pub fn num_removes(&self) -> usize {
        self.removes.len()
    }
}

impl Default for SnapshotTransaction {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use merutable_types::level::Level;

    fn test_data_file(path: &str, level: u8) -> IcebergDataFile {
        IcebergDataFile {
            path: path.to_string(),
            file_size: 1024,
            num_rows: 100,
            meta: ParquetFileMeta {
                level: Level(level),
                seq_min: 1,
                seq_max: 10,
                key_min: vec![0x01],
                key_max: vec![0xFF],
                num_rows: 100,
                file_size: 1024,
                dv_path: None,
                dv_offset: None,
                dv_length: None,
                format: None,
                column_stats: None,
            },
        }
    }

    #[test]
    fn empty_txn() {
        let txn = SnapshotTransaction::new();
        assert!(txn.is_empty());
        assert_eq!(txn.num_adds(), 0);
        assert_eq!(txn.num_removes(), 0);
    }

    #[test]
    fn add_and_remove() {
        let mut txn = SnapshotTransaction::new();
        txn.add_file(test_data_file("data/L0/a.parquet", 0));
        txn.add_file(test_data_file("data/L1/b.parquet", 1));
        txn.remove_file("data/L0/old.parquet".into());
        txn.set_prop("merutable.job", "compaction");

        assert!(!txn.is_empty());
        assert_eq!(txn.num_adds(), 2);
        assert_eq!(txn.num_removes(), 1);
        assert_eq!(txn.props.get("merutable.job").unwrap(), "compaction");
    }

    #[test]
    fn dv_merge_in_txn() {
        let mut txn = SnapshotTransaction::new();

        let mut dv1 = DeletionVector::new();
        dv1.mark_deleted(0);
        dv1.mark_deleted(5);
        txn.add_dv("data/L0/a.parquet".into(), dv1);

        let mut dv2 = DeletionVector::new();
        dv2.mark_deleted(5);
        dv2.mark_deleted(10);
        txn.add_dv("data/L0/a.parquet".into(), dv2);

        let merged = txn.dvs.get("data/L0/a.parquet").unwrap();
        assert_eq!(merged.cardinality(), 3); // {0, 5, 10}
    }
}
