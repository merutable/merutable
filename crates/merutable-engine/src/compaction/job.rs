//! `CompactionJob`: orchestrates one compaction run with DV bookkeeping.
//!
//! Sequence:
//! 1. Pick compaction (level, input files).
//! 2. Read input files via `ParquetReader::read_physical_rows_with_positions`,
//!    applying each source file's existing Deletion Vector loaded from its
//!    Puffin blob at the manifest-recorded byte range.
//! 3. Merge via `CompactionIterator` (dedup, tombstone drop).
//! 4. Write output files at the target level.
//! 5. Track promoted row positions for DV updates.
//! 6. Build `SnapshotTransaction` (adds + removes + dvs).
//! 7. Commit Iceberg snapshot.
//! 8. Install new version.

use std::{path::Path, sync::Arc};

use bytes::Bytes;
use merutable_iceberg::{
    version::DataFileMeta, DeletionVector, IcebergDataFile, SnapshotTransaction,
};
use merutable_parquet::reader::ParquetReader;
use merutable_types::{level::ParquetFileMeta, schema::TableSchema, MeruError, Result};
use roaring::RoaringBitmap;
use tracing::{debug, info};

use crate::{
    compaction::{
        iterator::{CompactionIterator, FileEntries},
        picker,
    },
    engine::MeruEngine,
};

/// Open a source Parquet file and load its Deletion Vector (if any) from
/// the companion Puffin blob at the exact manifest-recorded byte range.
/// Kept local to the compaction module so the read path can keep its own
/// equivalent helper without risking circular visibility; both are thin
/// wrappers over `ParquetReader::open` + `DeletionVector::from_puffin_blob`.
fn open_source_file(
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

/// Compute the union `[key_min, key_max]` range across a set of files.
/// Returns `(None, None)` if the set is empty. Files whose own `key_min`/
/// `key_max` are empty (unbounded) are treated as such — `key_min == []`
/// contributes the lexicographically smallest possible value (shrinks
/// `union_min` to `[]`), and `key_max == []` is treated as
/// "no upper bound known" and expands `union_max` to `[0xFF; ...]` by
/// clearing it. In practice every real file carries concrete bounds.
fn compute_union_range(files: &[DataFileMeta]) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let mut union_min: Option<Vec<u8>> = None;
    let mut union_max: Option<Vec<u8>> = None;
    for f in files {
        let km = &f.meta.key_min;
        let kx = &f.meta.key_max;
        match &union_min {
            None => union_min = Some(km.clone()),
            Some(cur) if km.as_slice() < cur.as_slice() => union_min = Some(km.clone()),
            _ => {}
        }
        match &union_max {
            None => union_max = Some(kx.clone()),
            Some(cur) if kx.as_slice() > cur.as_slice() => union_max = Some(kx.clone()),
            _ => {}
        }
    }
    (union_min, union_max)
}

/// Run one compaction job. Picks the best level and compacts.
pub async fn run_compaction(engine: &Arc<MeruEngine>) -> Result<()> {
    let version = engine.version_set.current();
    let pick = match picker::pick_compaction(&version, &engine.config) {
        Some(p) => p,
        None => {
            debug!("no compaction needed");
            return Ok(());
        }
    };

    info!(
        input_level = pick.input_level.0,
        output_level = pick.output_level.0,
        score = pick.score,
        input_files = pick.input_files.len(),
        "starting compaction"
    );

    // Snapshot the version once so every subsequent index into
    // `files_at(input_level)` refers to the same file list used when
    // building `FileEntries` below.
    let version = engine.version_set.current();
    let input_file_metas: Vec<DataFileMeta> = version.files_at(pick.input_level).to_vec();
    if input_file_metas.is_empty() {
        debug!("compaction picked an empty input level");
        return Ok(());
    }

    // ── Overlap pull-in: include every output-level file whose key range
    // intersects the union key range of `input_file_metas`. ──
    //
    // Classic leveled compaction invariant: L1+ must be non-overlapping
    // within each level so `find_file_for_key` can binary-search to a
    // single covering file. If a compaction writes a new L(k+1) file from
    // L(k) inputs WITHOUT rewriting the existing L(k+1) files that cover
    // the same keys, the new file overlaps the old ones and the point
    // lookup path silently returns stale data from the wrong file
    // (regression: `l1_overlap_regression::overlapping_l1_files_serve_correct_version_on_get`).
    //
    // Fix: compute the union key range of the primary inputs, then pull
    // every output-level file whose `[key_min, key_max]` intersects that
    // union into the compaction as additional inputs. They are read,
    // merged with full MVCC+dedup, and rewritten as part of the single
    // output file — leaving L(k+1) non-overlapping by construction.
    let (union_min, union_max) = compute_union_range(&input_file_metas);
    let overlap_output_metas: Vec<DataFileMeta> = if let (Some(umin), Some(umax)) =
        (union_min.as_ref(), union_max.as_ref())
    {
        version
            .files_at(pick.output_level)
            .iter()
            .filter(|f| {
                // Overlap iff `f.key_min <= umax && f.key_max >= umin`.
                (f.meta.key_min.is_empty() || f.meta.key_min.as_slice() <= umax.as_slice())
                    && (f.meta.key_max.is_empty() || f.meta.key_max.as_slice() >= umin.as_slice())
            })
            .cloned()
            .collect()
    } else {
        Vec::new()
    };

    if !overlap_output_metas.is_empty() {
        info!(
            output_level = pick.output_level.0,
            overlap_count = overlap_output_metas.len(),
            "pulling overlapping output-level files into compaction to preserve non-overlap invariant"
        );
    }

    // Read every source file (primary inputs + overlap pull-ins), applying
    // each file's current Deletion Vector so already-promoted rows don't
    // re-enter the output. Row positions are file-global physical positions
    // (u32) that DV stamping expects. `file_idx` is a dense index into the
    // combined list so `CompactionIterator` can disambiguate rows.
    let base = engine.catalog.base_path();
    let all_source_metas: Vec<&DataFileMeta> = input_file_metas
        .iter()
        .chain(overlap_output_metas.iter())
        .collect();
    let mut file_entries: Vec<FileEntries> = Vec::with_capacity(all_source_metas.len());
    for (file_idx, file_meta) in all_source_metas.iter().enumerate() {
        let (reader, dv) = open_source_file(base, file_meta, engine.schema.clone())?;
        let physical = reader.read_physical_rows_with_positions(dv.as_ref())?;
        file_entries.push(FileEntries {
            file_idx,
            entries: physical,
        });
    }

    // Build compaction iterator.
    let read_seq = engine.read_seq();
    let drop_tombstones = picker::should_drop_tombstones(
        pick.output_level,
        engine.config.level_target_bytes.len(),
    );
    let iter = CompactionIterator::new(file_entries, read_seq, drop_tombstones);

    if iter.is_empty() {
        // All inputs were tombstones dropped at the bottom level, or every
        // row was already DV-masked. Still install an empty transaction so
        // the source files can be removed.
        debug!(
            input_level = pick.input_level.0,
            "compaction produced no output rows"
        );
    }

    // Collect surviving entries. Since this path always reads every
    // physical row of every input file (see the `read_physical_rows_*`
    // call above), every input file is fully consumed and will be removed
    // from the manifest below. A future partial-compaction mode could
    // re-introduce per-file DV stamping, but the current model is
    // full-level in → full-level out, so tracking "promoted" positions is
    // redundant — and the old path was buggy because it only marked
    // *winner* positions, leaving deduped-away older duplicates in the
    // source file for the read path to rediscover.
    let mut output_rows: Vec<(
        merutable_types::key::InternalKey,
        merutable_types::value::Row,
    )> = Vec::new();
    let mut seq_min: u64 = u64::MAX;
    let mut seq_max: u64 = 0;
    let mut key_min: Option<Vec<u8>> = None;
    let mut key_max: Option<Vec<u8>> = None;

    for entry in iter {
        let uk = entry.ikey.user_key_bytes().to_vec();
        let s = entry.ikey.seq.0;
        if s < seq_min {
            seq_min = s;
        }
        if s > seq_max {
            seq_max = s;
        }
        match &key_min {
            Some(k) if k.as_slice() <= uk.as_slice() => {}
            _ => key_min = Some(uk.clone()),
        }
        match &key_max {
            Some(k) if k.as_slice() >= uk.as_slice() => {}
            _ => key_max = Some(uk.clone()),
        }
        output_rows.push((entry.ikey, entry.row));
    }

    let num_output_rows = output_rows.len();

    // Build the snapshot transaction up front so we can commit even when
    // the compaction output is empty (pure tombstone drop at the bottom).
    let mut txn = SnapshotTransaction::new();

    if num_output_rows > 0 {
        // Write output Parquet file.
        let file_id = uuid::Uuid::new_v4().to_string();
        let output_path = format!("data/L{}/{file_id}.parquet", pick.output_level.0);

        let (parquet_bytes, _, _) = merutable_parquet::writer::write_sorted_rows(
            output_rows,
            engine.schema.clone(),
            pick.output_level,
            engine.config.bloom_bits_per_key,
        )?;

        if parquet_bytes.is_empty() {
            return Err(MeruError::Parquet(
                "writer returned empty bytes for non-empty row set".into(),
            ));
        }
        let file_size = parquet_bytes.len() as u64;

        engine.catalog.ensure_level_dir(pick.output_level).await?;
        let full_path = engine.catalog.data_file_path(pick.output_level, &file_id);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(MeruError::Io)?;
        }
        tokio::fs::write(&full_path, &parquet_bytes)
            .await
            .map_err(MeruError::Io)?;

        let meta = ParquetFileMeta {
            level: pick.output_level,
            seq_min: if seq_min == u64::MAX { 0 } else { seq_min },
            seq_max,
            key_min: key_min.unwrap_or_default(),
            key_max: key_max.unwrap_or_default(),
            num_rows: num_output_rows as u64,
            file_size,
            dv_path: None,
            dv_offset: None,
            dv_length: None,
        };

        txn.add_file(IcebergDataFile {
            path: output_path,
            file_size,
            num_rows: num_output_rows as u64,
            meta,
        });
    }

    // Remove every fully-consumed input file — primary inputs AND the
    // overlap pull-ins from the output level, which were fully read and
    // rewritten above.
    for file_meta in &input_file_metas {
        txn.remove_file(file_meta.path.clone());
    }
    for file_meta in &overlap_output_metas {
        txn.remove_file(file_meta.path.clone());
    }

    txn.set_prop("merutable.job", "compaction");
    txn.set_prop("merutable.input_level", pick.input_level.0.to_string());
    txn.set_prop("merutable.output_level", pick.output_level.0.to_string());

    // Commit.
    let new_version = engine.catalog.commit(&txn, engine.schema.clone()).await?;
    engine.version_set.install(new_version);

    info!(
        input_level = pick.input_level.0,
        output_level = pick.output_level.0,
        output_rows = num_output_rows,
        "compaction committed"
    );

    Ok(())
}
