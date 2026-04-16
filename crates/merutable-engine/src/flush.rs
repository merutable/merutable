//! `FlushJob`: immutable memtable → Parquet → Iceberg snapshot commit.
//!
//! Sequence:
//! 1. Get oldest immutable memtable from manager.
//! 2. Iterate all entries in sorted InternalKey order.
//! 3. Convert to `(InternalKey, Row)` pairs.
//! 4. Write via `ParquetWriter` → `Vec<u8>` buffer.
//! 5. Upload Parquet file to data store.
//! 6. Build `SnapshotTransaction { adds: [new L0 file] }`.
//! 7. Commit Iceberg snapshot via catalog.
//! 8. Install new `Version` in `VersionSet`.
//! 9. GC old WAL files.
//! 10. Drop flushed memtable from manager + notify stalled writers.

use std::sync::Arc;

use merutable_iceberg::{IcebergDataFile, SnapshotTransaction};
use merutable_memtable::iterator::MemEntry;
use merutable_types::{
    key::InternalKey,
    level::{Level, ParquetFileMeta},
    sequence::{OpType, SeqNum},
    value::Row,
    MeruError, Result,
};
use tracing::{debug, info, instrument};

use crate::engine::MeruEngine;

/// Run one flush job: takes the oldest immutable memtable and writes it to L0.
///
/// Serialized by `engine.flush_mutex` so that two concurrently-spawned
/// auto-flush tasks don't both observe the same `oldest_immutable()`,
/// double-flush it to two L0 Parquet files with identical data, and
/// double-commit competing Iceberg snapshots (Bug G regression).
#[instrument(skip(engine), fields(op = "flush"))]
pub async fn run_flush(engine: &Arc<MeruEngine>) -> Result<()> {
    let _flush_guard = engine.flush_mutex.lock().await;

    let immutable = match engine.memtable.oldest_immutable() {
        Some(m) => m,
        None => return Ok(()), // nothing to flush
    };

    let first_seq = immutable.first_seq;
    let last_seq = immutable.last_seq();
    let read_seq = SeqNum(u64::MAX >> 8); // read everything
    info!(
        first_seq = first_seq.0,
        last_seq = last_seq.0,
        "starting flush"
    );

    // Collect all entries from the immutable memtable.
    let entries: Vec<MemEntry> = immutable.iter(read_seq).collect();
    if entries.is_empty() {
        debug!("empty memtable, skipping flush");
        engine.memtable.drop_flushed(first_seq);
        return Ok(());
    }

    // Convert MemEntry → (InternalKey, Row).
    // The MemEntry has user_key (PK bytes), seq, and EntryValue.
    // We need to reconstruct InternalKey from the wire bytes.
    let mut rows: Vec<(InternalKey, Row)> = Vec::with_capacity(entries.len());
    let mut key_min: Option<Vec<u8>> = None;
    let mut key_max: Option<Vec<u8>> = None;
    let mut seq_min = u64::MAX;
    let mut seq_max = 0u64;

    for entry in &entries {
        let uk = entry.user_key.to_vec();
        if key_min.is_none() {
            key_min = Some(uk.clone());
        }
        key_max = Some(uk.clone());
        if entry.seq.0 < seq_min {
            seq_min = entry.seq.0;
        }
        if entry.seq.0 > seq_max {
            seq_max = entry.seq.0;
        }

        // Reconstruct an InternalKey from the user_key + seq + op_type.
        // Build the wire bytes: [user_key_bytes][tag:8 BE]
        let tag = (merutable_types::sequence::SEQNUM_MAX.0 - entry.seq.0) << 8
            | (entry.entry.op_type as u64);
        let mut wire = Vec::with_capacity(uk.len() + 8);
        wire.extend_from_slice(&uk);
        wire.extend_from_slice(&tag.to_be_bytes());
        let ikey = InternalKey::decode(&wire, &engine.schema)?;

        // The row data is stored in EntryValue.value as serialized bytes.
        // Deserialize back to Row.
        let row = if entry.entry.op_type == OpType::Put && !entry.entry.value.is_empty() {
            crate::codec::decode_row(&entry.entry.value)
        } else {
            // Bug N fix: tombstone rows must carry the correct PK values in
            // their typed columns so HTAP readers (Spark/DuckDB) can identify
            // which key was deleted. Previously Row::default() produced zero
            // fields, which the codec's Bug K fix filled with sentinel values
            // (session_id=0, turn_id=0, etc.) — making tombstones look like
            // phantom rows with PK (0,0) in external queries.
            //
            // Build a Row with PK columns populated from the InternalKey and
            // non-PK columns set to None (the codec will fill sentinels for
            // non-nullable non-PK columns, which is acceptable since external
            // readers filter tombstones via the _merutable_ikey op_type tag).
            let pk_values = ikey.pk_values();
            let mut fields: Vec<Option<merutable_types::value::FieldValue>> =
                vec![None; engine.schema.columns.len()];
            for (pk_idx, &col_idx) in engine.schema.primary_key.iter().enumerate() {
                if pk_idx < pk_values.len() {
                    fields[col_idx] = Some(pk_values[pk_idx].clone());
                }
            }
            Row::new(fields)
        };

        rows.push((ikey, row));
    }

    let num_rows = rows.len() as u64;

    // Write Parquet file to an in-memory buffer.
    let (parquet_bytes, _bloom_bytes, _meta) = merutable_parquet::writer::write_sorted_rows(
        rows,
        engine.schema.clone(),
        Level(0),
        engine.config.bloom_bits_per_key,
    )?;

    // Generate file path.
    let file_id = uuid::Uuid::new_v4().to_string();
    let parquet_path = format!("data/L0/{file_id}.parquet");

    // Upload Parquet file.
    // For the file-system catalog, we write directly to the catalog's data directory.
    let full_path = engine.catalog.data_file_path(Level(0), &file_id);
    engine.catalog.ensure_level_dir(Level(0)).await?;
    if let Some(parent) = full_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(MeruError::Io)?;
    }
    // If parquet_bytes is empty (current Phase 4 limitation), write a placeholder.
    let file_size = if parquet_bytes.is_empty() {
        // Phase 4 writer doesn't produce real bytes yet; write what we have.
        0u64
    } else {
        tokio::fs::write(&full_path, &parquet_bytes)
            .await
            .map_err(MeruError::Io)?;

        // IMP-01: fsync the SST file so its bytes are durable before the
        // manifest references it.  Without this a crash between write and
        // catalog.commit() leaves a truncated/zero-length Parquet file
        // that the manifest points at.
        tokio::fs::File::open(&full_path)
            .await
            .map_err(MeruError::Io)?
            .sync_all()
            .await
            .map_err(MeruError::Io)?;

        // IMP-19: fsync the data directory so the directory entry for the
        // new file is durable.  POSIX: fsync on a file syncs data+metadata
        // of the file itself but NOT the directory containing the link.
        if let Some(parent) = full_path.parent() {
            if let Ok(dir) = tokio::fs::File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        }

        parquet_bytes.len() as u64
    };

    // Build snapshot transaction.
    let meta = ParquetFileMeta {
        level: Level(0),
        seq_min: if seq_min == u64::MAX { 0 } else { seq_min },
        seq_max,
        key_min: key_min.unwrap_or_default(),
        key_max: key_max.unwrap_or_default(),
        num_rows,
        file_size,
        dv_path: None,
        dv_offset: None,
        dv_length: None,
    };

    let mut txn = SnapshotTransaction::new();
    txn.add_file(IcebergDataFile {
        path: parquet_path.clone(),
        file_size,
        num_rows,
        meta,
    });
    txn.set_prop("merutable.job", "flush");
    txn.set_prop("merutable.first_seq", first_seq.0.to_string());
    txn.set_prop("merutable.last_seq", last_seq.0.to_string());

    // Commit snapshot.
    let new_version = engine.catalog.commit(&txn, engine.schema.clone()).await?;
    engine.version_set.install(new_version);

    info!(
        path = %parquet_path,
        num_rows,
        "flush committed"
    );

    // GC WAL files up to this memtable's last seq.
    engine.wal.lock().await.mark_flushed_seq(last_seq);

    // Drop flushed memtable + notify stalled writers.
    engine.memtable.drop_flushed(first_seq);

    Ok(())
}
