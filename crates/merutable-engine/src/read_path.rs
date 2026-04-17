//! Read path: point lookup (3-stop) and range scan via K-way merge.
//!
//! ## Point lookup algorithm
//!
//! 1. **Memtable** (active → immutable queue, newest first)
//!    → return immediately if found.
//! 2. **L0 files** (ALL checked, sorted by `seq_max` DESC — can overlap)
//!    → bloom filter gate first; first hit wins (files are already sorted
//!    newest-first by `Manifest::to_version`).
//! 3. **L1..LN** (binary search per level by `key_max` — non-overlapping)
//!    → bloom filter gate first.
//!
//! Deletion Vectors are loaded from their Puffin blob at the offset/length
//! recorded in the manifest and passed through to `ParquetReader::get`.
//!
//! ## Range scan
//!
//! Collect sorted rows from every memtable and every live Parquet file,
//! then do a single-pass dedup by user_key (PK ASC, seq DESC). A tombstone
//! at the top of a user_key group drops the key. File-level DVs filter
//! physically-deleted rows before dedup.

use std::{path::Path, sync::Arc};

use bytes::Bytes;
use merutable_iceberg::{version::DataFileMeta, DeletionVector};
use merutable_memtable::iterator::MemEntry;
use merutable_parquet::reader::ParquetReader;
use merutable_types::{
    key::InternalKey,
    level::Level,
    schema::TableSchema,
    sequence::OpType,
    value::{FieldValue, Row},
    MeruError, Result,
};
use roaring::RoaringBitmap;
use tracing::{debug, instrument, trace};

use crate::engine::MeruEngine;

// ── File open helper ────────────────────────────────────────────────────────

/// Synchronously open a Parquet file from disk and, when the manifest
/// records a Deletion Vector, load the exact DV blob byte range from the
/// companion Puffin file.
///
/// The returned `RoaringBitmap` is the set of **file-global** row positions
/// that were logically deleted by a subsequent partial compaction. Readers
/// MUST pass it to `ParquetReader::get`/`scan` or else deleted rows will
/// silently resurrect on read.
fn open_file(
    base: &Path,
    file: &DataFileMeta,
    schema: Arc<TableSchema>,
) -> Result<(ParquetReader<Bytes>, Option<RoaringBitmap>)> {
    let abs_parquet = base.join(&file.path);
    let parquet_bytes = std::fs::read(&abs_parquet).map_err(MeruError::Io)?;
    let reader = ParquetReader::open(Bytes::from(parquet_bytes), schema)?;

    let dv = match (&file.dv_path, file.dv_offset, file.dv_length) {
        (Some(dv_path), Some(offset), Some(length)) => {
            let abs_dv = base.join(dv_path);
            let puffin_bytes = std::fs::read(&abs_dv).map_err(MeruError::Io)?;
            let start = offset as usize;
            let end = start
                .checked_add(length as usize)
                .ok_or_else(|| MeruError::Corruption("DV offset+length overflow".into()))?;
            if end > puffin_bytes.len() {
                return Err(MeruError::Corruption(format!(
                    "DV blob out of range: path={dv_path} offset={offset} length={length} puffin_len={}",
                    puffin_bytes.len()
                )));
            }
            let dv = DeletionVector::from_puffin_blob(&puffin_bytes[start..end])?;
            Some(dv.bitmap().clone())
        }
        (None, None, None) => None,
        _ => {
            return Err(MeruError::Corruption(format!(
                "inconsistent DV coords on file {}: dv_path={:?} dv_offset={:?} dv_length={:?}",
                file.path, file.dv_path, file.dv_offset, file.dv_length
            )));
        }
    };

    Ok((reader, dv))
}

// ── Point lookup ─────────────────────────────────────────────────────────────

/// 3-stop point lookup: memtable → L0 → L1..LN.
#[instrument(skip(engine), fields(op = "point_lookup"))]
pub fn point_lookup(engine: &MeruEngine, pk_values: &[FieldValue]) -> Result<Option<Row>> {
    let read_seq = engine.read_seq();

    // Encode user key bytes for the lookup.
    let ikey = InternalKey::encode(pk_values, read_seq, OpType::Put, &engine.schema)?;
    let user_key_bytes = ikey.user_key_bytes().to_vec();

    // Stop 1: Memtable.
    if let Some(entry) = engine.memtable.get(&user_key_bytes, read_seq) {
        if entry.op_type == OpType::Delete {
            return Ok(None); // tombstone
        }
        let row = crate::codec::decode_row(&entry.value);
        trace!(source = "memtable", "cache hit");
        return Ok(Some(row));
    }

    // Stop 1.5: Row cache (between memtable and file I/O).
    if let Some(ref cache) = engine.row_cache {
        if let Some(entry) = cache.get(&user_key_bytes) {
            if entry.op_type == OpType::Delete {
                return Ok(None);
            }
            trace!(source = "row_cache", "cache hit");
            return Ok(Some(entry.row));
        }
    }

    // Cache race fix: snapshot the generation BEFORE reading from disk.
    // Any concurrent write that invalidates the cache advances the
    // generation; the `insert_if_fresh` call below refuses to install
    // the disk-sourced value if the generation has moved on — preventing
    // a stale-cache-survives-memtable-flush scenario.
    let cache_gen = engine.row_cache.as_ref().map(|c| c.snapshot_generation());

    let version = engine.version_set.current();
    let base = engine.catalog.base_path();

    // Stop 2: L0 files. `Manifest::to_version` pre-sorts L0 by `seq_max`
    // DESC so the first file that returns a hit is guaranteed to carry the
    // newest visible version of `user_key_bytes`.
    for file in version.files_at(Level(0)) {
        if !range_contains(&file.meta.key_min, &file.meta.key_max, &user_key_bytes) {
            continue;
        }
        let (reader, dv) = open_file(base, file, engine.schema.clone())?;
        if let Some((hit_ikey, row)) = reader.get(&user_key_bytes, read_seq, dv.as_ref())? {
            // Populate cache before returning — only if no concurrent
            // invalidation raced with this read.
            if let (Some(ref cache), Some(gen)) = (&engine.row_cache, cache_gen) {
                cache.insert_if_fresh(
                    user_key_bytes.clone(),
                    crate::cache::CacheEntry {
                        op_type: hit_ikey.op_type,
                        row: row.clone(),
                    },
                    gen,
                );
            }
            if hit_ikey.op_type == OpType::Delete {
                return Ok(None);
            }
            debug!(source = "L0", file = %file.path, "point lookup hit");
            return Ok(Some(row));
        }
    }

    // Stop 3: L1..LN — binary search for the covering file per level.
    let max_level = version.max_level();
    for lvl in 1..=max_level.0 {
        let level = Level(lvl);
        let Some(file) = version.find_file_for_key(level, &user_key_bytes) else {
            continue;
        };
        let (reader, dv) = open_file(base, file, engine.schema.clone())?;
        if let Some((hit_ikey, row)) = reader.get(&user_key_bytes, read_seq, dv.as_ref())? {
            if let (Some(ref cache), Some(gen)) = (&engine.row_cache, cache_gen) {
                cache.insert_if_fresh(
                    user_key_bytes.clone(),
                    crate::cache::CacheEntry {
                        op_type: hit_ikey.op_type,
                        row: row.clone(),
                    },
                    gen,
                );
            }
            if hit_ikey.op_type == OpType::Delete {
                return Ok(None);
            }
            debug!(source = %format!("L{}", lvl), file = %file.path, "point lookup hit");
            return Ok(Some(row));
        }
    }

    trace!("point lookup miss");
    Ok(None)
}

fn range_contains(key_min: &[u8], key_max: &[u8], probe: &[u8]) -> bool {
    if !key_min.is_empty() && probe < key_min {
        return false;
    }
    if !key_max.is_empty() && probe > key_max {
        return false;
    }
    true
}

// ── Range scan ───────────────────────────────────────────────────────────────

/// Range scan with K-way merge across memtables and every live Parquet file.
/// Dedups by `user_key`, drops tombstones, and honors Deletion Vectors.
#[instrument(skip(engine), fields(op = "range_scan"))]
pub fn range_scan(
    engine: &MeruEngine,
    start_pk: Option<&[FieldValue]>,
    end_pk: Option<&[FieldValue]>,
) -> Result<Vec<(InternalKey, Row)>> {
    let read_seq = engine.read_seq();

    // Encode start/end user key bytes.
    let start_bytes = start_pk
        .map(|pk| {
            InternalKey::encode(pk, read_seq, OpType::Put, &engine.schema)
                .map(|ik| ik.user_key_bytes().to_vec())
        })
        .transpose()?;
    let end_bytes = end_pk
        .map(|pk| {
            InternalKey::encode(pk, read_seq, OpType::Put, &engine.schema)
                .map(|ik| ik.user_key_bytes().to_vec())
        })
        .transpose()?;

    // Harvest every candidate `(InternalKey, Row, op_type)` tuple into a
    // single buffer. We do a single sort+dedup pass at the end rather than
    // an incremental k-way merge: simpler to get right, still O(N log N),
    // and N is bounded by the active working set.
    let mut harvest: Vec<(InternalKey, Row, OpType)> = Vec::new();

    // 1. Memtable snapshots.
    let mem_snapshots = engine.memtable.snapshot_entries(read_seq);
    let mut mem_all: Vec<MemEntry> = Vec::new();
    for s in mem_snapshots {
        mem_all.extend(s);
    }
    for entry in &mem_all {
        // Range gate — skip rows outside the requested range early.
        let uk = entry.user_key.as_ref();
        if let Some(ref start) = start_bytes {
            if uk < start.as_slice() {
                continue;
            }
        }
        if let Some(ref end) = end_bytes {
            if uk >= end.as_slice() {
                continue;
            }
        }

        // Rebuild the InternalKey from wire bytes (user_key ++ tag).
        let tag = (merutable_types::sequence::SEQNUM_MAX.0 - entry.seq.0) << 8
            | (entry.entry.op_type as u64);
        let mut wire = Vec::with_capacity(uk.len() + 8);
        wire.extend_from_slice(uk);
        wire.extend_from_slice(&tag.to_be_bytes());
        let ikey = InternalKey::decode(&wire, &engine.schema)?;

        let row: Row = if entry.entry.op_type == OpType::Put && !entry.entry.value.is_empty() {
            crate::codec::decode_row(&entry.entry.value)
        } else {
            Row::default()
        };
        harvest.push((ikey, row, entry.entry.op_type));
    }

    // 2. Every live Parquet file at every level.
    let version = engine.version_set.current();
    let base = engine.catalog.base_path();
    let max_level = version.max_level();
    for lvl in 0..=max_level.0 {
        let level = Level(lvl);
        for file in version.files_at(level) {
            // Skip files whose key range doesn't overlap the scan range.
            if let Some(ref start) = start_bytes {
                if !file.meta.key_max.is_empty() && file.meta.key_max.as_slice() < start.as_slice()
                {
                    continue;
                }
            }
            if let Some(ref end) = end_bytes {
                if !file.meta.key_min.is_empty() && file.meta.key_min.as_slice() >= end.as_slice() {
                    continue;
                }
            }

            let (reader, dv) = open_file(base, file, engine.schema.clone())?;
            // Ask the reader for rows in the requested range, already
            // DV-filtered and MVCC-gated at `read_seq`. `scan` dedups
            // within a single file; cross-file dedup happens below.
            let file_rows = reader.scan(
                start_bytes.as_deref(),
                end_bytes.as_deref(),
                read_seq,
                dv.as_ref(),
            )?;
            for (ikey, row) in file_rows {
                let op = ikey.op_type;
                harvest.push((ikey, row, op));
            }
        }
    }

    // 3. Global sort: (user_key ASC, seq DESC).
    harvest.sort_by(|a, b| a.0.cmp(&b.0));

    // 4. Dedup: for each user_key, keep the topmost entry (highest seq).
    // Drop keys whose topmost entry is a tombstone.
    let mut results: Vec<(InternalKey, Row)> = Vec::new();
    let mut last_uk: Option<Vec<u8>> = None;

    for (ikey, row, op) in harvest {
        let uk = ikey.user_key_bytes().to_vec();
        if let Some(ref last) = last_uk {
            if *last == uk {
                continue; // older version of same key
            }
        }
        last_uk = Some(uk);

        if op == OpType::Delete {
            continue;
        }
        results.push((ikey, row));
    }

    debug!(result_count = results.len(), "range scan complete");
    Ok(results)
}
