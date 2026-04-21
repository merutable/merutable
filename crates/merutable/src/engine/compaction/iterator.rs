//! `CompactionIterator`: wraps a K-way merge iterator with compaction semantics.
//!
//! - Drops stale versions (keeps only the latest seq for each user_key).
//! - Drops tombstones when no older data exists below the output level.
//! - Tracks `row_position` for DV bookkeeping.

use crate::types::{
    key::InternalKey,
    sequence::{OpType, SeqNum},
    value::Row,
};

/// One entry output by the compaction iterator.
#[derive(Clone, Debug)]
pub struct CompactionEntry {
    pub ikey: InternalKey,
    pub row: Row,
    /// Index of the source file this entry came from.
    pub source_file_idx: usize,
    /// Row position within the source file (for DV tracking).
    pub row_position: u32,
}

/// Input to the compaction iterator: entries from one source file.
#[derive(Clone, Debug)]
pub struct FileEntries {
    pub file_idx: usize,
    pub entries: Vec<(InternalKey, Row, u32)>, // (ikey, row, row_position)
}

/// Compaction iterator: merges entries from multiple files, deduplicates by
/// user_key, and optionally drops tombstones.
pub struct CompactionIterator {
    /// All entries merged and sorted by InternalKey.
    entries: Vec<CompactionEntry>,
    pos: usize,
}

impl CompactionIterator {
    /// Build from multiple source files' entries.
    ///
    /// `oldest_snapshot_seq`: the minimum sequence number held by any active
    /// reader. Older versions of a key are only dropped if their seq is
    /// strictly below this watermark — versions at or above it must be
    /// preserved so snapshot readers can still see them.
    ///
    /// `drop_tombstones`: if true, `OpType::Delete` entries are dropped
    /// (safe only when no older data exists below the output level).
    pub fn new(
        file_entries: Vec<FileEntries>,
        oldest_snapshot_seq: SeqNum,
        drop_tombstones: bool,
    ) -> Self {
        // Flatten all entries.
        let mut all: Vec<CompactionEntry> = Vec::new();
        for fe in file_entries {
            for (ikey, row, row_pos) in fe.entries {
                all.push(CompactionEntry {
                    ikey,
                    row,
                    source_file_idx: fe.file_idx,
                    row_position: row_pos,
                });
            }
        }

        // Sort by InternalKey (PK ASC, seq DESC).
        all.sort_by(|a, b| a.ikey.cmp(&b.ikey));

        // IMP-08: snapshot-aware deduplication. For each user_key:
        //   - Always keep the latest version (first in sort order).
        //   - Keep older versions whose seq >= oldest_snapshot_seq (an
        //     active reader may need them).
        //   - Drop older versions whose seq < oldest_snapshot_seq.
        let mut deduped: Vec<CompactionEntry> = Vec::new();
        let mut last_uk: Option<Vec<u8>> = None;
        let mut seen_latest = false;

        for entry in all {
            let uk = entry.ikey.user_key_bytes().to_vec();
            if let Some(ref last) = last_uk {
                if *last == uk {
                    // Older version of the same key — only keep if an
                    // active reader might need it.
                    if entry.ikey.seq >= oldest_snapshot_seq {
                        deduped.push(entry);
                    }
                    continue;
                }
            }
            // New user key — this is the latest version (always kept).
            last_uk = Some(uk);
            seen_latest = true;

            // Drop tombstones if allowed.
            if drop_tombstones && entry.ikey.op_type == OpType::Delete {
                continue;
            }

            deduped.push(entry);
        }
        let _ = seen_latest; // suppress unused warning

        Self {
            entries: deduped,
            pos: 0,
        }
    }

    /// Number of surviving entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Iterator for CompactionIterator {
    type Item = CompactionEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.entries.len() {
            return None;
        }
        let entry = self.entries[self.pos].clone();
        self.pos += 1;
        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        schema::{ColumnDef, ColumnType, TableSchema},
        sequence::OpType,
        value::FieldValue,
    };

    fn schema() -> TableSchema {
        TableSchema {
            table_name: "t".into(),
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

    fn make_ikey(pk: i64, seq: u64, op: OpType) -> InternalKey {
        InternalKey::encode(&[FieldValue::Int64(pk)], SeqNum(seq), op, &schema()).unwrap()
    }

    #[test]
    fn dedup_keeps_latest() {
        let fe = vec![FileEntries {
            file_idx: 0,
            entries: vec![
                (make_ikey(1, 10, OpType::Put), Row::default(), 0),
                (make_ikey(1, 5, OpType::Put), Row::default(), 1),
                (make_ikey(2, 8, OpType::Put), Row::default(), 2),
            ],
        }];
        let iter = CompactionIterator::new(fe, SeqNum(100), false);
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 2); // key=1 (seq=10), key=2 (seq=8)
        assert_eq!(results[0].ikey.seq, SeqNum(10));
        assert_eq!(results[1].ikey.seq, SeqNum(8));
    }

    #[test]
    fn drop_tombstones() {
        let fe = vec![FileEntries {
            file_idx: 0,
            entries: vec![
                (make_ikey(1, 10, OpType::Delete), Row::default(), 0),
                (make_ikey(2, 8, OpType::Put), Row::default(), 1),
            ],
        }];
        let iter = CompactionIterator::new(fe, SeqNum(100), true);
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 1); // only key=2 survives
        assert_eq!(results[0].ikey.seq, SeqNum(8));
    }

    /// IMP-08 regression: when oldest_snapshot_seq is high (no active old
    /// readers), all older versions are dropped. When it's low (an active
    /// reader holds an old snapshot), older versions must be preserved.
    #[test]
    fn snapshot_aware_version_dropping() {
        // key=1 has two versions: seq=10 (newer) and seq=5 (older).
        let fe = vec![FileEntries {
            file_idx: 0,
            entries: vec![
                (make_ikey(1, 10, OpType::Put), Row::default(), 0),
                (make_ikey(1, 5, OpType::Put), Row::default(), 1),
                (make_ikey(2, 8, OpType::Put), Row::default(), 2),
            ],
        }];

        // Case 1: oldest_snapshot_seq is high (100) — no reader needs seq=5.
        // Old version (seq=5) should be dropped.
        let iter = CompactionIterator::new(fe.clone(), SeqNum(100), false);
        let results: Vec<_> = iter.collect();
        assert_eq!(
            results.len(),
            2,
            "old version should be dropped when no reader needs it"
        );

        // Case 2: oldest_snapshot_seq is low (3) — a reader at seq=3 might
        // need the seq=5 version. Both versions must be preserved.
        let iter = CompactionIterator::new(fe, SeqNum(3), false);
        let results: Vec<_> = iter.collect();
        assert_eq!(
            results.len(),
            3,
            "old version must be preserved when oldest_snapshot_seq is below it"
        );
        // Both versions of key=1 survive.
        let key1_versions: Vec<_> = results
            .iter()
            .filter(|e| e.ikey.seq.0 == 10 || e.ikey.seq.0 == 5)
            .collect();
        assert_eq!(
            key1_versions.len(),
            2,
            "both versions of key=1 must survive"
        );
    }

    #[test]
    fn merge_across_files() {
        let fe = vec![
            FileEntries {
                file_idx: 0,
                entries: vec![
                    (make_ikey(1, 10, OpType::Put), Row::default(), 0),
                    (make_ikey(3, 10, OpType::Put), Row::default(), 1),
                ],
            },
            FileEntries {
                file_idx: 1,
                entries: vec![
                    (make_ikey(1, 5, OpType::Put), Row::default(), 0),
                    (make_ikey(2, 8, OpType::Put), Row::default(), 1),
                ],
            },
        ];
        let iter = CompactionIterator::new(fe, SeqNum(100), false);
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 3); // keys 1, 2, 3
                                      // Key 1 should come from file 0 (seq=10, newer).
        assert_eq!(results[0].ikey.seq, SeqNum(10));
        assert_eq!(results[0].source_file_idx, 0);
    }
}
