//! Forensic inspection of a real on-disk WAL file.
//!
//! Every test here produces a WAL file via `WalManager` (the real write
//! path, not an in-memory sink), then opens the raw bytes and walks
//! every 7-byte header independently — validating CRC32c, length,
//! record type, fragmentation state machine, block padding, and
//! payload reassembly — without going through `WalReader`.
//!
//! If any of these invariants drift, the parquet/iceberg recovery path
//! silently breaks, so they are pinned byte-for-byte here.

use std::fs;
use std::path::PathBuf;

use bytes::Bytes;
use crc32fast::Hasher as Crc32;
use merutable::types::sequence::SeqNum;
use merutable::wal::batch::WriteBatch;
use merutable::wal::format::{BLOCK_SIZE, HEADER_SIZE};
use merutable::wal::manager::WalManager;

const TYPE_FULL: u8 = 1;
const TYPE_FIRST: u8 = 2;
const TYPE_MIDDLE: u8 = 3;
const TYPE_LAST: u8 = 4;

/// One decoded physical record as it sits on disk.
#[derive(Debug, Clone)]
struct PhysicalRecord {
    file_offset: usize,
    block_index: usize,
    block_offset: usize,
    type_byte: u8,
    length: usize,
    payload: Vec<u8>,
}

/// Walk the raw bytes of a WAL file and return every physical record
/// plus the offsets of every zero-padded tail skip.
///
/// This is the parser the forensic tests use as ground truth; it does
/// NOT share any code with `WalReader` so a bug in the reader cannot
/// hide a bug in the writer.
fn walk_wal_bytes(bytes: &[u8]) -> (Vec<PhysicalRecord>, Vec<(usize, usize)>) {
    let mut records = Vec::new();
    let mut pads = Vec::new(); // (file_offset, len)
    let mut pos = 0usize;

    while pos < bytes.len() {
        let block_index = pos / BLOCK_SIZE;
        let block_offset = pos % BLOCK_SIZE;
        let remaining_in_block = BLOCK_SIZE - block_offset;

        // Block tail too small for a header → must be zero-padded.
        if remaining_in_block < HEADER_SIZE {
            assert!(
                bytes[pos..pos + remaining_in_block].iter().all(|&b| b == 0),
                "block {block_index} tail at file offset {pos} is not zero-padded"
            );
            pads.push((pos, remaining_in_block));
            pos += remaining_in_block;
            continue;
        }

        // Stop if we can't read a full header (truncated tail).
        if pos + HEADER_SIZE > bytes.len() {
            break;
        }

        let header = &bytes[pos..pos + HEADER_SIZE];
        let stored_crc = u32::from_le_bytes(header[..4].try_into().unwrap());
        let length = u16::from_le_bytes(header[4..6].try_into().unwrap()) as usize;
        let type_byte = header[6];

        // Zero-pad sentinel: an all-zero header means "skip to next block".
        if stored_crc == 0 && length == 0 && type_byte == 0 {
            let pad_len = remaining_in_block;
            assert!(
                bytes[pos..pos + pad_len].iter().all(|&b| b == 0),
                "zero-pad sentinel at {pos} has non-zero trailing bytes"
            );
            pads.push((pos, pad_len));
            pos += pad_len;
            continue;
        }

        assert!(
            matches!(type_byte, TYPE_FULL | TYPE_FIRST | TYPE_MIDDLE | TYPE_LAST),
            "unknown record type {type_byte:#x} at file offset {pos}"
        );

        // Payload length must fit in the current block after the header.
        assert!(
            HEADER_SIZE + length <= remaining_in_block,
            "record at {pos} overflows its block: header+len={}, remaining={}",
            HEADER_SIZE + length,
            remaining_in_block
        );

        let payload_start = pos + HEADER_SIZE;
        let payload_end = payload_start + length;
        assert!(
            payload_end <= bytes.len(),
            "record payload at {payload_start}..{payload_end} exceeds file size {}",
            bytes.len()
        );
        let payload = bytes[payload_start..payload_end].to_vec();

        // Verify CRC32c over [type_byte ++ payload] — independent of
        // the reader so any drift in the writer's CRC scope is caught.
        let mut hasher = Crc32::new();
        hasher.update(&[type_byte]);
        hasher.update(&payload);
        let computed = hasher.finalize();
        assert_eq!(
            computed, stored_crc,
            "CRC mismatch at file offset {pos}: stored={stored_crc:#x} computed={computed:#x}"
        );

        records.push(PhysicalRecord {
            file_offset: pos,
            block_index,
            block_offset,
            type_byte,
            length,
            payload,
        });
        pos = payload_end;
    }

    (records, pads)
}

/// Reassemble a stream of physical records into complete logical
/// records, asserting the state machine (Full alone, or First → Middle*
/// → Last) at every step.
fn reassemble(records: &[PhysicalRecord]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut in_fragment = false;
    let mut buf: Vec<u8> = Vec::new();
    let mut last_block: Option<usize> = None;

    for (i, rec) in records.iter().enumerate() {
        match rec.type_byte {
            TYPE_FULL => {
                assert!(
                    !in_fragment,
                    "Full record at index {i} while still assembling a fragment"
                );
                out.push(rec.payload.clone());
            }
            TYPE_FIRST => {
                assert!(
                    !in_fragment,
                    "First record at index {i} while still assembling a fragment"
                );
                in_fragment = true;
                buf = rec.payload.clone();
                last_block = Some(rec.block_index);
            }
            TYPE_MIDDLE => {
                assert!(
                    in_fragment,
                    "Middle record at index {i} without a preceding First"
                );
                let prev = last_block.unwrap();
                assert_eq!(
                    rec.block_index,
                    prev + 1,
                    "Middle record at index {i} is not in the next block (prev={prev}, now={})",
                    rec.block_index
                );
                buf.extend_from_slice(&rec.payload);
                last_block = Some(rec.block_index);
            }
            TYPE_LAST => {
                assert!(
                    in_fragment,
                    "Last record at index {i} without a preceding First"
                );
                let prev = last_block.unwrap();
                assert_eq!(
                    rec.block_index,
                    prev + 1,
                    "Last record at index {i} is not in the next block (prev={prev}, now={})",
                    rec.block_index
                );
                buf.extend_from_slice(&rec.payload);
                out.push(std::mem::take(&mut buf));
                in_fragment = false;
                last_block = None;
            }
            other => panic!("unknown type {other:#x} in reassemble"),
        }
    }
    assert!(!in_fragment, "trailing fragment without Last record");
    out
}

fn wal_path(dir: &std::path::Path, log_number: u64) -> PathBuf {
    dir.join(format!("{log_number:06}.wal"))
}

fn write_batches(dir: &std::path::Path, batches: &[WriteBatch]) {
    let mut mgr = WalManager::open(dir, 1).unwrap();
    for b in batches {
        mgr.append(b).unwrap();
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// A single small batch must land as exactly one `Full` record at file
/// offset 0 in block 0, with a correct CRC32c over `[type_byte, payload]`.
#[test]
fn small_batch_is_one_full_record_in_block_zero() {
    let dir = tempfile::tempdir().unwrap();
    let mut batch = WriteBatch::new(SeqNum(1));
    batch.put(Bytes::from("alpha"), Bytes::from("one"));
    write_batches(dir.path(), &[batch.clone()]);

    let bytes = fs::read(wal_path(dir.path(), 1)).unwrap();
    let (records, pads) = walk_wal_bytes(&bytes);

    assert_eq!(records.len(), 1, "expected exactly one physical record");
    assert_eq!(
        pads.len(),
        0,
        "no padding expected for a single small batch"
    );
    let rec = &records[0];
    assert_eq!(rec.file_offset, 0);
    assert_eq!(rec.block_index, 0);
    assert_eq!(rec.block_offset, 0);
    assert_eq!(rec.type_byte, TYPE_FULL);
    assert_eq!(rec.length, rec.payload.len());

    // The payload must decode back to the exact batch we wrote.
    let decoded = WriteBatch::decode(&rec.payload).unwrap();
    assert_eq!(decoded.sequence, batch.sequence);
    assert_eq!(decoded.records.len(), batch.records.len());
    assert_eq!(decoded.records[0].user_key, batch.records[0].user_key);
    assert_eq!(decoded.records[0].value, batch.records[0].value);
}

/// Multiple small batches must share block 0 without any padding, each
/// becoming its own `Full` record with a fresh valid CRC.
#[test]
fn multiple_small_batches_pack_into_single_block() {
    let dir = tempfile::tempdir().unwrap();
    let batches: Vec<WriteBatch> = (0..16)
        .map(|i| {
            let mut b = WriteBatch::new(SeqNum(100 + i));
            b.put(
                Bytes::from(format!("key-{i:02}")),
                Bytes::from(format!("value-{i:02}")),
            );
            b
        })
        .collect();
    write_batches(dir.path(), &batches);

    let bytes = fs::read(wal_path(dir.path(), 1)).unwrap();
    let (records, pads) = walk_wal_bytes(&bytes);
    assert_eq!(records.len(), batches.len());
    assert_eq!(pads.len(), 0, "small batches must not require padding");

    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.type_byte, TYPE_FULL, "record {i} must be Full");
        assert_eq!(rec.block_index, 0, "record {i} must be in block 0");
        let decoded = WriteBatch::decode(&rec.payload).unwrap();
        assert_eq!(decoded.sequence, batches[i].sequence);
    }
}

/// A batch larger than one block must fragment as `First → Middle* → Last`
/// across consecutive blocks, and reassembly must reproduce the original
/// payload byte-for-byte.
#[test]
fn large_batch_fragments_across_blocks_with_valid_state_machine() {
    let dir = tempfile::tempdir().unwrap();
    // Force at least three blocks of payload → First + Middle + Last.
    let big_val = Bytes::from(vec![0xA5u8; 3 * BLOCK_SIZE]);
    let mut batch = WriteBatch::new(SeqNum(1));
    batch.put(Bytes::from("big"), big_val);
    write_batches(dir.path(), &[batch.clone()]);

    let bytes = fs::read(wal_path(dir.path(), 1)).unwrap();
    let (records, _pads) = walk_wal_bytes(&bytes);

    assert!(
        records.len() >= 3,
        "expected First + Middle* + Last (≥3 physical records), got {}",
        records.len()
    );
    assert_eq!(records.first().unwrap().type_byte, TYPE_FIRST);
    assert_eq!(records.last().unwrap().type_byte, TYPE_LAST);
    for mid in &records[1..records.len() - 1] {
        assert_eq!(mid.type_byte, TYPE_MIDDLE);
    }

    // Every Middle must live in the block immediately after its
    // predecessor — fragmentation never skips a block.
    for pair in records.windows(2) {
        assert_eq!(
            pair[1].block_index,
            pair[0].block_index + 1,
            "fragmented record crossed a non-adjacent block boundary"
        );
    }

    // First must start at block offset 0 of its block (the writer
    // migrated to a fresh block if the previous block couldn't fit).
    // Since this is the first record in the file, block 0 / offset 0.
    assert_eq!(records[0].block_index, 0);
    assert_eq!(records[0].block_offset, 0);

    // Reassembled payload must round-trip the original batch.
    let reassembled = reassemble(&records);
    assert_eq!(reassembled.len(), 1);
    let decoded = WriteBatch::decode(&reassembled[0]).unwrap();
    assert_eq!(decoded.sequence, batch.sequence);
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(
        decoded.records[0].value.as_ref().unwrap().len(),
        3 * BLOCK_SIZE
    );
}

/// When a batch can't fit a 7-byte header at the end of the current
/// block, the writer MUST zero-pad the tail and start a new block.
/// The forensic walker independently verifies every tail byte is zero
/// and the next record starts at block_offset == 0.
#[test]
fn block_tail_is_zero_padded_when_header_does_not_fit() {
    let dir = tempfile::tempdir().unwrap();
    // Craft the first batch so its encoded size is BLOCK_SIZE - HEADER_SIZE - 3.
    // After writing header(7) + payload, block_offset = BLOCK_SIZE - 3,
    // leaving 3 bytes → next record must pad and restart in block 1.
    //
    // WriteBatch encoding overhead (u64 seq + u32 count + op + varint + key
    // + varint + val) isn't fixed, so we build the batch iteratively.
    let target_encoded_len = BLOCK_SIZE - HEADER_SIZE - 3;
    let mut payload_len = target_encoded_len;
    let batch1 = loop {
        let mut b = WriteBatch::new(SeqNum(1));
        b.put(Bytes::from("k"), Bytes::from(vec![0xC3u8; payload_len]));
        let encoded = b.encode();
        if encoded.len() == target_encoded_len {
            break b;
        }
        // Encoding overhead varies by a handful of bytes — nudge down.
        if encoded.len() > target_encoded_len {
            payload_len -= encoded.len() - target_encoded_len;
        } else {
            payload_len += target_encoded_len - encoded.len();
        }
    };
    assert_eq!(batch1.encode().len(), target_encoded_len);

    let mut batch2 = WriteBatch::new(SeqNum(2));
    batch2.put(Bytes::from("next"), Bytes::from("block"));
    write_batches(dir.path(), &[batch1.clone(), batch2.clone()]);

    let bytes = fs::read(wal_path(dir.path(), 1)).unwrap();
    let (records, pads) = walk_wal_bytes(&bytes);

    assert_eq!(records.len(), 2, "expected two physical records");
    assert_eq!(records[0].block_index, 0);
    assert_eq!(records[0].type_byte, TYPE_FULL);
    assert_eq!(records[0].length, target_encoded_len);

    // Second record must be in block 1 at offset 0.
    assert_eq!(
        records[1].block_index, 1,
        "second record must have migrated to block 1"
    );
    assert_eq!(records[1].block_offset, 0);
    assert_eq!(records[1].type_byte, TYPE_FULL);

    // Exactly one padding region of 3 bytes, at the end of block 0.
    assert_eq!(pads.len(), 1);
    assert_eq!(pads[0].0, BLOCK_SIZE - 3);
    assert_eq!(pads[0].1, 3);
}

/// Header CRC must be byte-accurate: flipping a single payload byte in
/// the on-disk file must cause the forensic walker's CRC check to fail
/// at the exact corrupted record.
#[test]
fn crc_check_catches_single_bit_flip_in_payload() {
    let dir = tempfile::tempdir().unwrap();
    let mut batch = WriteBatch::new(SeqNum(42));
    batch.put(Bytes::from("forensic"), Bytes::from("payload"));
    write_batches(dir.path(), &[batch]);

    let path = wal_path(dir.path(), 1);
    let mut bytes = fs::read(&path).unwrap();

    // Clean file must walk cleanly.
    let _ = walk_wal_bytes(&bytes);

    // Flip a bit inside the payload (well past the header).
    let flip_at = HEADER_SIZE + 5;
    bytes[flip_at] ^= 0x80;
    let result = std::panic::catch_unwind(|| walk_wal_bytes(&bytes));
    assert!(
        result.is_err(),
        "walker must reject a bit-flipped payload; instead it accepted the file"
    );
}

/// A fragmented record's First must sit at the start of its owning
/// block (block_offset == 0). Every Middle must fill its block (i.e.
/// header + payload == BLOCK_SIZE) so that no middle block wastes tail
/// space — this is the RocksDB invariant merutable inherits.
#[test]
fn fragmented_records_fill_their_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let big = Bytes::from(vec![0x77u8; 4 * BLOCK_SIZE]);
    let mut batch = WriteBatch::new(SeqNum(1));
    batch.put(Bytes::from("k"), big);
    write_batches(dir.path(), &[batch]);

    let bytes = fs::read(wal_path(dir.path(), 1)).unwrap();
    let (records, _) = walk_wal_bytes(&bytes);

    // First sits at start of block 0.
    assert_eq!(records[0].type_byte, TYPE_FIRST);
    assert_eq!(records[0].block_offset, 0);
    // First must fill block 0 exactly: HEADER_SIZE + payload == BLOCK_SIZE.
    assert_eq!(HEADER_SIZE + records[0].length, BLOCK_SIZE);

    // Every Middle must fill its block exactly.
    let mid_slice = &records[1..records.len() - 1];
    for (i, m) in mid_slice.iter().enumerate() {
        assert_eq!(m.type_byte, TYPE_MIDDLE, "record {i} must be Middle");
        assert_eq!(m.block_offset, 0);
        assert_eq!(
            HEADER_SIZE + m.length,
            BLOCK_SIZE,
            "middle record {i} must fill its block"
        );
    }

    // Last starts at block_offset 0 but may not fill its block.
    let last = records.last().unwrap();
    assert_eq!(last.type_byte, TYPE_LAST);
    assert_eq!(last.block_offset, 0);
}

/// Cross-check: `WalManager::recover_from_dir` must return exactly the
/// logical batches the forensic walker extracted by reassembling the
/// raw bytes. If these ever disagree, either the writer is producing
/// something the reader can't parse or the reader is fabricating data
/// that isn't on disk.
#[test]
fn manager_recovery_matches_raw_byte_reassembly() {
    let dir = tempfile::tempdir().unwrap();
    let batches: Vec<WriteBatch> = (0..8)
        .map(|i| {
            let mut b = WriteBatch::new(SeqNum(1000 + i * 10));
            b.put(
                Bytes::from(format!("rk-{i}")),
                Bytes::from(vec![(i as u8).wrapping_mul(17); 100 + i as usize * 500]),
            );
            b
        })
        .collect();
    write_batches(dir.path(), &batches);

    let bytes = fs::read(wal_path(dir.path(), 1)).unwrap();
    let (records, _) = walk_wal_bytes(&bytes);
    let reassembled = reassemble(&records);
    let raw_decoded: Vec<WriteBatch> = reassembled
        .iter()
        .map(|p| WriteBatch::decode(p).unwrap())
        .collect();

    let (recovered, _max_seq, _max_log) = WalManager::recover_from_dir(dir.path()).unwrap();

    assert_eq!(recovered.len(), raw_decoded.len());
    for (a, b) in recovered.iter().zip(raw_decoded.iter()) {
        assert_eq!(a.sequence, b.sequence);
        assert_eq!(a.records.len(), b.records.len());
        for (ra, rb) in a.records.iter().zip(b.records.iter()) {
            assert_eq!(ra.op_type, rb.op_type);
            assert_eq!(ra.user_key, rb.user_key);
            assert_eq!(ra.value, rb.value);
        }
    }
}
