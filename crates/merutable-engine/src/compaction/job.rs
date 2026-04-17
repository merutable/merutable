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
use merutable_types::{
    level::Level, level::ParquetFileMeta, schema::TableSchema, MeruError, Result,
};
use roaring::RoaringBitmap;
use tracing::{debug, info, instrument, warn};

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
            if offset < 0 || length < 0 {
                return Err(MeruError::Corruption(format!(
                    "DV has negative offset ({offset}) or length ({length}) on file {}",
                    file.path,
                )));
            }
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

/// RAII guard that releases a level reservation when dropped. Ensures
/// reserved levels are always freed even on error paths — without this,
/// a mid-compaction error would permanently wedge the levels and no
/// future compaction could touch them.
struct LevelReservation {
    engine: Arc<MeruEngine>,
    levels: Vec<Level>,
}

impl Drop for LevelReservation {
    fn drop(&mut self) {
        // We're in a sync drop; `Mutex::blocking_lock` is wrong inside a
        // tokio runtime (deadlocks if the runtime has only one worker).
        // Use `try_lock` — the contention window is microseconds
        // (pick/execute/commit all release naturally), so try_lock
        // virtually always succeeds. If it fails, spawn an async task
        // to free the reservation so levels can't leak permanently.
        let levels = std::mem::take(&mut self.levels);
        if levels.is_empty() {
            return;
        }
        let acquired = {
            let try_result = self.engine.compacting_levels.try_lock();
            match try_result {
                Ok(mut guard) => {
                    for l in &levels {
                        guard.remove(l);
                    }
                    true
                }
                Err(_) => false,
            }
        };
        if !acquired {
            let engine = self.engine.clone();
            tokio::spawn(async move {
                let mut guard = engine.compacting_levels.lock().await;
                for l in &levels {
                    guard.remove(l);
                }
            });
        }
    }
}

/// Run compaction until the LSM tree is healthy or all eligible work is
/// owned by other workers.
///
/// Loops: acquire the state lock → pick a compaction on levels not
/// currently reserved by another worker → reserve those levels
/// (input + output) → release the state lock → execute the merge (no
/// lock held, so concurrent workers can run on disjoint levels) →
/// acquire `commit_lock` for the brief catalog commit → release all
/// reservations via RAII.
///
/// Returns when `pick_compaction()` returns `None`, either because no
/// level needs compaction or because every eligible level is currently
/// being compacted by another worker. The other worker's loop will
/// handle any remaining work as its levels free up.
///
/// Follows Pebble's `inProgressCompactions` / BadgerDB's `compactStatus`
/// pattern. Scaled to per-level granularity because the current picker
/// always picks full levels; refinable to per-file tracking if the
/// picker learns to select subranges.
#[instrument(skip(engine), fields(op = "compaction"))]
pub async fn run_compaction(engine: &Arc<MeruEngine>) -> Result<()> {
    const MAX_ITERATIONS: usize = 128;
    for iter in 0..MAX_ITERATIONS {
        let did_work = run_one_compaction_job(engine).await?;
        if !did_work {
            if iter > 0 {
                debug!(iterations = iter, "compaction drained all pressure");
            }
            return Ok(());
        }
    }
    warn!(
        max = MAX_ITERATIONS,
        "compaction loop hit iteration cap — will resume on next trigger"
    );
    Ok(())
}

/// Reserve the input + output levels for a new compaction. Returns the
/// pick, the version snapshot the pick was made from, and an RAII guard.
/// Returning the version preserves Bug Q's invariant: the compaction
/// reads files from the same version the picker scored — a later
/// `version_set.current()` call could see a different version after a
/// concurrent flush committed.
async fn reserve_next_compaction(
    engine: &Arc<MeruEngine>,
) -> Option<(
    picker::CompactionPick,
    Arc<merutable_iceberg::version::Version>,
    LevelReservation,
)> {
    let mut busy = engine.compacting_levels.lock().await;
    let version_guard = engine.version_set.current();
    let pick = picker::pick_compaction(&version_guard, &engine.config, &busy)?;
    // Clone the Arc out of the ArcSwap guard so the version outlives the
    // reservation (and the caller can read files from it without holding
    // the guard across await points).
    let version: Arc<merutable_iceberg::version::Version> = (*version_guard).clone();
    drop(version_guard);

    // Reserve the picked levels. Inserts must succeed because the
    // picker guaranteed neither is already in `busy`.
    let input_level = pick.input_level;
    let output_level = pick.output_level;
    busy.insert(input_level);
    busy.insert(output_level);
    drop(busy);

    let reservation = LevelReservation {
        engine: engine.clone(),
        levels: vec![input_level, output_level],
    };
    Some((pick, version, reservation))
}

/// Run one compaction job. Returns `true` if a compaction was executed,
/// `false` if no eligible level needed compaction (or all were busy).
async fn run_one_compaction_job(engine: &Arc<MeruEngine>) -> Result<bool> {
    // Phase 1: pick + reserve (brief lock).
    let (pick, version, _reservation) = match reserve_next_compaction(engine).await {
        Some(r) => r,
        None => {
            debug!("no compaction needed (or all candidates busy)");
            return Ok(false);
        }
    };

    info!(
        input_level = pick.input_level.0,
        output_level = pick.output_level.0,
        score = pick.score,
        input_files = pick.input_files.len(),
        "starting compaction"
    );

    // Bug Q fix: reuse the same version snapshot used for picking. The old
    // code re-snapshotted `version_set.current()` here, which could return
    // a different version if a concurrent flush committed between pick and
    // this point — causing the compaction to read a file set inconsistent
    // with what the picker evaluated.
    let input_file_metas: Vec<DataFileMeta> = version.files_at(pick.input_level).to_vec();
    if input_file_metas.is_empty() {
        debug!("compaction picked an empty input level");
        return Ok(false);
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
    let drop_tombstones =
        picker::should_drop_tombstones(pick.output_level, engine.config.level_target_bytes.len());
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

        // IMP-01: fsync the output SST before the manifest references it.
        tokio::fs::File::open(&full_path)
            .await
            .map_err(MeruError::Io)?
            .sync_all()
            .await
            .map_err(MeruError::Io)?;

        // IMP-19: fsync the data directory so the new file's directory
        // entry is durable before the manifest commit.
        if let Some(parent) = full_path.parent() {
            if let Ok(dir) = tokio::fs::File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        }

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

    // Commit. Two concurrent compactions on disjoint levels finish
    // their merges in parallel; their commits must linearize because
    // each computes `next_ver` from the current manifest — without
    // serialization they'd race on the version number and overwrite
    // each other's `v{N+1}.metadata.json`. The commit itself is brief
    // (single fsync chain), so this lock doesn't serialize the hot
    // merge work.
    let new_version = {
        let _commit_guard = engine.commit_lock.lock().await;
        engine.catalog.commit(&txn, engine.schema.clone()).await?
    };
    engine.version_set.install(new_version);

    // IMP-03: clear the row cache after compaction. Compaction rewrites
    // files and resolves MVCC versions — any entry cached from a now-obsolete
    // file could be stale. A full clear is simple and correct; the cache
    // refills on the next read wave.
    if let Some(ref cache) = engine.row_cache {
        cache.clear();
    }

    // IMP-12: enqueue obsoleted files for deferred deletion. External
    // readers (DuckDB, Spark) may still be mid-read of old files; immediate
    // deletion would cause read failures. Files are physically deleted
    // after gc_grace_period_secs has elapsed.
    let mut obsoleted_paths: Vec<std::path::PathBuf> = Vec::new();
    for file_meta in input_file_metas.iter().chain(overlap_output_metas.iter()) {
        obsoleted_paths.push(base.join(&file_meta.path));
        if let Some(ref dv) = file_meta.dv_path {
            obsoleted_paths.push(base.join(dv));
        }
    }
    engine.enqueue_for_deletion(obsoleted_paths).await;

    // Run GC to clean up any files whose grace period has expired.
    engine.gc_pending_deletions().await;

    info!(
        input_level = pick.input_level.0,
        output_level = pick.output_level.0,
        output_rows = num_output_rows,
        "compaction committed"
    );

    Ok(true)
}
