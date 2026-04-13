//! Deletion Vectors — Puffin format, Iceberg spec v3.
//!
//! A DV is a roaring bitmap of row positions to exclude when reading a Parquet
//! file. Stored as a Puffin blob (`deletion-vector-v1`) alongside the Parquet
//! file. Implemented from the Puffin spec — we do not rely on `iceberg`-rust's
//! Puffin support which may be incomplete.
//!
//! # Puffin wire format (file envelope)
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │ magic: 4 bytes  = "PFA1"                │
//! ├─────────────────────────────────────────┤
//! │ blob data (DV blob body, see below)     │
//! ├─────────────────────────────────────────┤
//! │ footer payload: UTF-8 JSON              │
//! ├─────────────────────────────────────────┤
//! │ footer length: i32 LE                   │
//! ├─────────────────────────────────────────┤
//! │ magic: 4 bytes  = "PFA1"                │
//! └─────────────────────────────────────────┘
//! ```
//!
//! # DV blob body (Iceberg v3 `deletion-vector-v1`)
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │ length: u32 BE  (magic+data+CRC size)   │
//! ├─────────────────────────────────────────┤
//! │ magic: 4 bytes  = D1 D3 39 64           │
//! ├─────────────────────────────────────────┤
//! │ 64-bit Roaring portable:                │
//! │   bitmap_count: u64 LE                  │
//! │   per bitmap:                           │
//! │     key: u32 LE (upper 32 bits)         │
//! │     32-bit Roaring (portable format)    │
//! ├─────────────────────────────────────────┤
//! │ CRC32: u32 BE  (of magic + data)        │
//! └─────────────────────────────────────────┘
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use crc32fast::Hasher as Crc32;
use merutable_types::{MeruError, Result};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

/// Puffin file magic bytes: "PFA1".
const PUFFIN_MAGIC: [u8; 4] = [0x50, 0x46, 0x41, 0x31];

/// Iceberg v3 deletion-vector-v1 blob magic: `D1 D3 39 64`.
const DV_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

/// Minimum valid Puffin file: magic(4) + footer_len(4) + magic(4) = 12 bytes.
const PUFFIN_MIN_SIZE: usize = 12;

// ── PuffinEncoded ────────────────────────────────────────────────────────────

/// Output of [`DeletionVector::encode_puffin`]: the complete Puffin
/// file bytes together with the byte range of the roaring-bitmap blob
/// inside those bytes. Both fields must be persisted to the Iceberg
/// manifest so readers can seek directly to the blob.
#[derive(Clone, Debug)]
pub struct PuffinEncoded {
    /// Full Puffin file, ready to be written to object storage.
    pub bytes: Bytes,
    /// Byte offset of the roaring-bitmap blob inside `bytes`.
    pub blob_offset: i64,
    /// Byte length of the roaring-bitmap blob inside `bytes`.
    pub blob_length: i64,
}

// ── DeletionVector ───────────────────────────────────────────────────────────

/// Roaring bitmap of deleted row positions within a single Parquet file.
#[derive(Clone, Debug)]
pub struct DeletionVector {
    bitmap: RoaringBitmap,
}

impl Default for DeletionVector {
    fn default() -> Self {
        Self::new()
    }
}

impl DeletionVector {
    pub fn new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
        }
    }

    /// Wrap an existing bitmap.
    pub fn from_bitmap(bitmap: RoaringBitmap) -> Self {
        Self { bitmap }
    }

    /// Mark a row position as deleted.
    pub fn mark_deleted(&mut self, row_position: u32) {
        self.bitmap.insert(row_position);
    }

    /// Check whether a row position has been deleted.
    pub fn is_deleted(&self, row_position: u32) -> bool {
        self.bitmap.contains(row_position)
    }

    /// Number of deleted rows.
    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }

    /// True if the bitmap is empty (no deleted rows).
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }

    /// Merge another DV into this one (union).
    pub fn union_with(&mut self, other: &DeletionVector) {
        self.bitmap |= &other.bitmap;
    }

    /// Borrow the underlying bitmap (for passing to `ParquetReader`).
    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    // ── Puffin serialization ─────────────────────────────────────────────

    /// Serialize this DV as a complete Puffin file AND return the exact
    /// `[blob_offset, blob_offset + blob_length)` byte range of the
    /// roaring bitmap blob within those bytes.
    ///
    /// Callers persisting the file to object storage MUST record
    /// `blob_offset` and `blob_length` in the Iceberg manifest so that
    /// readers can seek to the blob without re-parsing the entire
    /// Puffin footer. An earlier version stamped placeholder zeros into
    /// the manifest; on reload the deletion bitmap was silently empty
    /// and previously-deleted rows reappeared.
    ///
    /// `parquet_path`: the referenced Parquet data file (absolute object-store path).
    /// `snapshot_id`: Iceberg snapshot that produced this DV.
    /// `sequence_number`: Iceberg sequence number for this DV blob.
    pub fn encode_puffin(
        &self,
        parquet_path: &str,
        snapshot_id: i64,
        sequence_number: i64,
    ) -> Result<PuffinEncoded> {
        // ── Step 1: Serialize 32-bit Roaring bitmap to portable format ──
        let mut roaring32_bytes = Vec::new();
        self.bitmap
            .serialize_into(&mut roaring32_bytes)
            .map_err(|e| MeruError::Iceberg(format!("DV bitmap serialize: {e}")))?;

        // ── Step 2: Wrap in 64-bit Roaring portable format ──
        // bitmap_count (u64 LE) + key (u32 LE) + 32-bit portable Roaring
        let mut roaring64_data = Vec::with_capacity(12 + roaring32_bytes.len());
        roaring64_data.extend_from_slice(&1u64.to_le_bytes()); // bitmap_count = 1
        roaring64_data.extend_from_slice(&0u32.to_le_bytes()); // key = 0 (all positions < 2^32)
        roaring64_data.extend_from_slice(&roaring32_bytes);

        // ── Step 3: Build DV blob envelope per Iceberg v3 spec ──
        // length(u32 BE) + magic(4) + roaring64_data + CRC32(u32 BE)
        // `length` = size of magic + roaring64_data + CRC32.
        let inner_len = DV_MAGIC.len() + roaring64_data.len() + 4;
        let length = u32::try_from(inner_len).map_err(|_| {
            MeruError::Iceberg(format!("DV blob too large for u32 length: {inner_len}"))
        })?;

        // CRC32 covers magic + roaring64_data.
        let mut hasher = Crc32::new();
        hasher.update(&DV_MAGIC);
        hasher.update(&roaring64_data);
        let crc = hasher.finalize();

        let blob_body_len = 4 + inner_len; // length field + inner_len
        let mut blob_data = Vec::with_capacity(blob_body_len);
        blob_data.extend_from_slice(&length.to_be_bytes());
        blob_data.extend_from_slice(&DV_MAGIC);
        blob_data.extend_from_slice(&roaring64_data);
        blob_data.extend_from_slice(&crc.to_be_bytes());

        // ── Step 4: Assemble Puffin file ──
        // Blob offset = right after the opening Puffin magic.
        let blob_offset = PUFFIN_MAGIC.len() as i64;
        let blob_length = blob_data.len() as i64;

        // Build footer JSON.
        let footer = PuffinFooter {
            blobs: vec![PuffinBlobMeta {
                blob_type: "deletion-vector-v1".to_string(),
                fields: PuffinBlobFields {
                    referenced_data_file: parquet_path.to_string(),
                    cardinality: self.bitmap.len() as i64,
                },
                snapshot_id,
                sequence_number,
                offset: blob_offset,
                length: blob_length,
                compression_codec: None,
            }],
            properties: serde_json::Map::new(),
        };
        let footer_json = serde_json::to_string(&footer)
            .map_err(|e| MeruError::Iceberg(format!("Puffin footer JSON: {e}")))?;
        let footer_bytes = footer_json.as_bytes();
        let footer_len = i32::try_from(footer_bytes.len()).map_err(|_| {
            MeruError::Iceberg(format!(
                "Puffin footer too large for i32: {} bytes",
                footer_bytes.len()
            ))
        })?;

        // Assemble: magic + blob_data + footer_payload + footer_len(i32 LE) + magic
        let total = PUFFIN_MAGIC.len()
            + blob_data.len()
            + footer_bytes.len()
            + 4  // footer_len
            + PUFFIN_MAGIC.len();

        let mut buf = BytesMut::with_capacity(total);
        buf.put_slice(&PUFFIN_MAGIC);
        buf.put_slice(&blob_data);
        buf.put_slice(footer_bytes);
        buf.put_i32_le(footer_len);
        buf.put_slice(&PUFFIN_MAGIC);

        Ok(PuffinEncoded {
            bytes: buf.freeze(),
            blob_offset,
            blob_length,
        })
    }

    /// Legacy wrapper kept for tests that only care about the final
    /// Puffin bytes. Production code MUST use `encode_puffin` so that
    /// the blob offset/length can be persisted into the manifest.
    pub fn to_puffin_bytes(
        &self,
        parquet_path: &str,
        snapshot_id: i64,
        sequence_number: i64,
    ) -> Result<Bytes> {
        self.encode_puffin(parquet_path, snapshot_id, sequence_number)
            .map(|p| p.bytes)
    }

    /// Deserialize a DV from a complete Puffin file.
    pub fn from_puffin_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < PUFFIN_MIN_SIZE {
            return Err(MeruError::Corruption("Puffin file too short".into()));
        }

        // Validate opening magic.
        if data[..4] != PUFFIN_MAGIC {
            return Err(MeruError::Corruption(
                "Puffin file missing opening magic".into(),
            ));
        }
        // Validate closing magic.
        if data[data.len() - 4..] != PUFFIN_MAGIC {
            return Err(MeruError::Corruption(
                "Puffin file missing closing magic".into(),
            ));
        }

        // Read footer length (i32 LE, 4 bytes before closing magic).
        // Bug L: the raw i32 can be negative in a corrupt file, so validate
        // before casting to usize — a negative value cast `as usize` wraps
        // to a huge number and either panics on the subtraction below or
        // reads garbage bytes as the footer JSON.
        let fl_start = data.len() - 8;
        let footer_len_raw = i32::from_le_bytes(data[fl_start..fl_start + 4].try_into().unwrap());
        if footer_len_raw < 0 {
            return Err(MeruError::Corruption(format!(
                "Puffin footer length is negative: {footer_len_raw}"
            )));
        }
        let footer_len = footer_len_raw as usize;

        if footer_len > data.len() - PUFFIN_MIN_SIZE {
            return Err(MeruError::Corruption(format!(
                "Puffin footer length {footer_len} exceeds file size"
            )));
        }

        let footer_start = fl_start - footer_len;
        let footer_json = std::str::from_utf8(&data[footer_start..fl_start])
            .map_err(|e| MeruError::Corruption(format!("Puffin footer not UTF-8: {e}")))?;

        let footer: PuffinFooter = serde_json::from_str(footer_json)
            .map_err(|e| MeruError::Corruption(format!("Puffin footer JSON parse: {e}")))?;

        // We expect exactly one blob of type "deletion-vector-v1".
        let blob_meta = footer
            .blobs
            .iter()
            .find(|b| b.blob_type == "deletion-vector-v1")
            .ok_or_else(|| {
                MeruError::Corruption("Puffin file has no deletion-vector-v1 blob".into())
            })?;

        if blob_meta.offset < 0 || blob_meta.length < 0 {
            return Err(MeruError::Corruption(format!(
                "Puffin blob has negative offset ({}) or length ({})",
                blob_meta.offset, blob_meta.length
            )));
        }
        let offset = blob_meta.offset as usize;
        let length = blob_meta.length as usize;
        let end = offset.checked_add(length).ok_or_else(|| {
            MeruError::Corruption(format!(
                "Puffin blob offset {offset} + length {length} overflows usize"
            ))
        })?;
        if end > data.len() {
            return Err(MeruError::Corruption(format!(
                "Puffin blob at offset {offset} length {length} exceeds file size {}",
                data.len()
            )));
        }

        let blob_bytes = &data[offset..offset + length];
        let bitmap = decode_dv_blob(blob_bytes)?;

        Ok(Self { bitmap })
    }

    /// Deserialize a DV from a slice of a Puffin file (given blob offset + length).
    /// Used when the Puffin file has already been partially read.
    /// The blob bytes must be in Iceberg v3 DV format:
    /// `length(u32 BE) + magic(D1 D3 39 64) + 64-bit Roaring + CRC32(u32 BE)`.
    pub fn from_puffin_blob(blob_bytes: &[u8]) -> Result<Self> {
        let bitmap = decode_dv_blob(blob_bytes)?;
        Ok(Self { bitmap })
    }

    /// Extract the referenced data file path from a Puffin file's footer.
    pub fn referenced_data_file(puffin_data: &[u8]) -> Result<String> {
        let footer = parse_puffin_footer(puffin_data)?;
        let blob_meta = footer
            .blobs
            .iter()
            .find(|b| b.blob_type == "deletion-vector-v1")
            .ok_or_else(|| {
                MeruError::Corruption("Puffin file has no deletion-vector-v1 blob".into())
            })?;
        Ok(blob_meta.fields.referenced_data_file.clone())
    }
}

// ── Puffin footer serde types ────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug)]
struct PuffinFooter {
    blobs: Vec<PuffinBlobMeta>,
    #[serde(default)]
    properties: serde_json::Map<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
struct PuffinBlobMeta {
    #[serde(rename = "type")]
    blob_type: String,
    fields: PuffinBlobFields,
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(rename = "sequence-number")]
    sequence_number: i64,
    offset: i64,
    length: i64,
    #[serde(rename = "compression-codec", skip_serializing_if = "Option::is_none")]
    compression_codec: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct PuffinBlobFields {
    #[serde(rename = "referenced-data-file")]
    referenced_data_file: String,
    /// Number of deleted row positions in this DV (Iceberg v3 spec field).
    #[serde(default)]
    cardinality: i64,
}

/// Decode a DV blob in Iceberg v3 `deletion-vector-v1` format.
///
/// Blob body: `length(u32 BE) + magic(D1 D3 39 64) + 64-bit Roaring + CRC32(u32 BE)`.
/// The `length` field covers magic + roaring data + CRC32.
fn decode_dv_blob(blob_bytes: &[u8]) -> Result<RoaringBitmap> {
    // Minimum: length(4) + magic(4) + CRC(4) = 12 bytes.
    if blob_bytes.len() < 12 {
        return Err(MeruError::Corruption(format!(
            "DV blob too short: {} bytes (minimum 12)",
            blob_bytes.len()
        )));
    }

    // Read length (u32 BE).
    let length = u32::from_be_bytes(blob_bytes[..4].try_into().unwrap()) as usize;
    if 4 + length != blob_bytes.len() {
        return Err(MeruError::Corruption(format!(
            "DV blob length field ({length}) doesn't match actual remaining size ({})",
            blob_bytes.len() - 4
        )));
    }

    // Validate DV magic.
    if blob_bytes[4..8] != DV_MAGIC {
        return Err(MeruError::Corruption(format!(
            "DV blob magic mismatch: expected D1D33964, got {:02X}{:02X}{:02X}{:02X}",
            blob_bytes[4], blob_bytes[5], blob_bytes[6], blob_bytes[7]
        )));
    }

    // Extract vector_data and stored CRC32.
    let crc_offset = blob_bytes.len() - 4;
    let stored_crc = u32::from_be_bytes(blob_bytes[crc_offset..].try_into().unwrap());
    let magic_and_data = &blob_bytes[4..crc_offset]; // magic + roaring64_data

    // Verify CRC32 of (magic + roaring64_data).
    let mut hasher = Crc32::new();
    hasher.update(magic_and_data);
    let computed_crc = hasher.finalize();
    if computed_crc != stored_crc {
        return Err(MeruError::Corruption(format!(
            "DV blob CRC mismatch: stored {stored_crc:#x}, computed {computed_crc:#x}"
        )));
    }

    // Parse 64-bit Roaring: bitmap_count(u64 LE) + per-bitmap(key u32 LE + portable Roaring).
    let roaring64_data = &blob_bytes[8..crc_offset];
    if roaring64_data.len() < 8 {
        return Err(MeruError::Corruption(
            "DV 64-bit Roaring data too short for bitmap_count".into(),
        ));
    }

    let bitmap_count = u64::from_le_bytes(roaring64_data[..8].try_into().unwrap());
    if bitmap_count == 0 {
        return Ok(RoaringBitmap::new());
    }
    if bitmap_count > 1 {
        return Err(MeruError::Corruption(format!(
            "DV 64-bit Roaring has {bitmap_count} bitmaps; \
             merutable only supports row positions < 2^32"
        )));
    }

    // Single bitmap: read key(u32 LE) + 32-bit portable Roaring.
    if roaring64_data.len() < 12 {
        return Err(MeruError::Corruption(
            "DV 64-bit Roaring data too short for key field".into(),
        ));
    }
    let key = u32::from_le_bytes(roaring64_data[8..12].try_into().unwrap());
    if key != 0 {
        return Err(MeruError::Corruption(format!(
            "DV 64-bit Roaring key is {key}; \
             merutable only supports key=0 (row positions < 2^32)"
        )));
    }

    let roaring_bytes = &roaring64_data[12..];
    RoaringBitmap::deserialize_from(roaring_bytes)
        .map_err(|e| MeruError::Corruption(format!("DV 32-bit Roaring deserialize: {e}")))
}

/// Parse the footer from a complete Puffin file (shared by multiple methods).
fn parse_puffin_footer(data: &[u8]) -> Result<PuffinFooter> {
    if data.len() < PUFFIN_MIN_SIZE {
        return Err(MeruError::Corruption("Puffin file too short".into()));
    }
    if data[..4] != PUFFIN_MAGIC || data[data.len() - 4..] != PUFFIN_MAGIC {
        return Err(MeruError::Corruption("Puffin magic mismatch".into()));
    }
    let fl_start = data.len() - 8;
    let footer_len_raw = i32::from_le_bytes(data[fl_start..fl_start + 4].try_into().unwrap());
    if footer_len_raw < 0 {
        return Err(MeruError::Corruption(format!(
            "Puffin footer length is negative: {footer_len_raw}"
        )));
    }
    let footer_len = footer_len_raw as usize;
    if footer_len > fl_start - 4 {
        return Err(MeruError::Corruption(format!(
            "Puffin footer length {footer_len} exceeds available data"
        )));
    }
    let footer_start = fl_start - footer_len;
    let json_str = std::str::from_utf8(&data[footer_start..fl_start])
        .map_err(|e| MeruError::Corruption(format!("Puffin footer UTF-8: {e}")))?;
    serde_json::from_str(json_str)
        .map_err(|e| MeruError::Corruption(format!("Puffin footer JSON: {e}")))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_dv() {
        let dv = DeletionVector::new();
        assert!(dv.is_empty());
        assert_eq!(dv.cardinality(), 0);
        assert!(!dv.is_deleted(0));
    }

    #[test]
    fn mark_and_check() {
        let mut dv = DeletionVector::new();
        dv.mark_deleted(5);
        dv.mark_deleted(100);
        dv.mark_deleted(9999);
        assert!(dv.is_deleted(5));
        assert!(dv.is_deleted(100));
        assert!(dv.is_deleted(9999));
        assert!(!dv.is_deleted(6));
        assert_eq!(dv.cardinality(), 3);
    }

    #[test]
    fn union_merge() {
        let mut dv1 = DeletionVector::new();
        dv1.mark_deleted(1);
        dv1.mark_deleted(3);
        dv1.mark_deleted(5);

        let mut dv2 = DeletionVector::new();
        dv2.mark_deleted(2);
        dv2.mark_deleted(3);
        dv2.mark_deleted(7);

        dv1.union_with(&dv2);
        assert_eq!(dv1.cardinality(), 5); // {1,2,3,5,7}
        for pos in [1, 2, 3, 5, 7] {
            assert!(dv1.is_deleted(pos));
        }
        assert!(!dv1.is_deleted(4));
    }

    #[test]
    fn puffin_roundtrip() {
        let mut dv = DeletionVector::new();
        for i in (0..1000).step_by(3) {
            dv.mark_deleted(i);
        }

        let puffin = dv
            .to_puffin_bytes("s3://bucket/data/L0/abc123.parquet", 42, 7)
            .unwrap();

        // Validate magic bytes.
        assert_eq!(&puffin[..4], &PUFFIN_MAGIC);
        assert_eq!(&puffin[puffin.len() - 4..], &PUFFIN_MAGIC);

        let decoded = DeletionVector::from_puffin_bytes(&puffin).unwrap();
        assert_eq!(decoded.cardinality(), dv.cardinality());
        for i in (0..1000).step_by(3) {
            assert!(decoded.is_deleted(i));
        }
        for i in (1..1000).step_by(3) {
            assert!(!decoded.is_deleted(i));
        }
    }

    #[test]
    fn puffin_referenced_data_file() {
        let mut dv = DeletionVector::new();
        dv.mark_deleted(0);
        let path = "s3://bucket/data/L1/xyz789.parquet";
        let puffin = dv.to_puffin_bytes(path, 100, 1).unwrap();
        let extracted = DeletionVector::referenced_data_file(&puffin).unwrap();
        assert_eq!(extracted, path);
    }

    #[test]
    fn puffin_empty_dv() {
        let dv = DeletionVector::new();
        let puffin = dv
            .to_puffin_bytes("file:///tmp/test.parquet", 1, 1)
            .unwrap();
        let decoded = DeletionVector::from_puffin_bytes(&puffin).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn puffin_bad_magic_rejected() {
        let mut data = vec![0u8; 20];
        // Wrong opening magic.
        data[0..4].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(DeletionVector::from_puffin_bytes(&data).is_err());
    }

    /// Bug L regression: a Puffin file with a negative footer_len i32
    /// must return a clean Corruption error, not panic via usize
    /// wraparound on the subtraction `fl_start - footer_len`.
    #[test]
    fn puffin_negative_footer_len_rejected() {
        // Build a minimal valid-ish Puffin frame: magic + <padding> +
        // footer_len(i32 LE, negative) + magic.
        let neg: i32 = -1;
        let mut data = Vec::new();
        data.extend_from_slice(&PUFFIN_MAGIC); // opening magic
        data.extend_from_slice(&[0u8; 4]); // dummy padding
        data.extend_from_slice(&neg.to_le_bytes()); // negative footer_len
        data.extend_from_slice(&PUFFIN_MAGIC); // closing magic
        let err = DeletionVector::from_puffin_bytes(&data).unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("negative"),
            "expected 'negative' in error: {msg}"
        );
    }

    #[test]
    fn puffin_too_short_rejected() {
        assert!(DeletionVector::from_puffin_bytes(&[0x50, 0x46]).is_err());
    }

    #[test]
    fn from_bitmap_roundtrip() {
        let mut bm = RoaringBitmap::new();
        bm.insert(10);
        bm.insert(20);
        let dv = DeletionVector::from_bitmap(bm);
        assert!(dv.is_deleted(10));
        assert!(dv.is_deleted(20));
        assert!(!dv.is_deleted(15));
    }

    #[test]
    fn large_dv_puffin_roundtrip() {
        let mut dv = DeletionVector::new();
        // 100K deleted positions, simulating a large compaction.
        for i in 0..100_000u32 {
            dv.mark_deleted(i * 2); // even positions
        }
        let puffin = dv.to_puffin_bytes("data/L0/big.parquet", 999, 50).unwrap();
        let decoded = DeletionVector::from_puffin_bytes(&puffin).unwrap();
        assert_eq!(decoded.cardinality(), 100_000);
        assert!(decoded.is_deleted(0));
        assert!(!decoded.is_deleted(1));
        assert!(decoded.is_deleted(199_998));
    }

    // ── Iceberg v3 DV spec compliance tests ─────────────────────────

    /// Verify the DV blob body starts with the Iceberg v3 envelope:
    /// length(u32 BE) + magic(D1 D3 39 64) + 64-bit Roaring + CRC32(u32 BE).
    #[test]
    fn dv_blob_has_iceberg_v3_envelope() {
        let mut dv = DeletionVector::new();
        dv.mark_deleted(7);
        dv.mark_deleted(42);

        let encoded = dv.encode_puffin("data/L0/test.parquet", 1, 1).unwrap();

        // Extract blob from the Puffin file.
        let blob_offset = encoded.blob_offset as usize;
        let blob_length = encoded.blob_length as usize;
        let blob = &encoded.bytes[blob_offset..blob_offset + blob_length];

        // First 4 bytes: length (u32 BE) = total_blob_len - 4.
        let length = u32::from_be_bytes(blob[..4].try_into().unwrap()) as usize;
        assert_eq!(
            length,
            blob.len() - 4,
            "length field must cover magic+data+CRC"
        );

        // Next 4 bytes: DV magic.
        assert_eq!(
            &blob[4..8],
            &DV_MAGIC,
            "DV magic D1D33964 must follow length"
        );

        // Last 4 bytes: CRC32 (u32 BE).
        let crc_offset = blob.len() - 4;
        let stored_crc = u32::from_be_bytes(blob[crc_offset..].try_into().unwrap());
        let mut hasher = Crc32::new();
        hasher.update(&blob[4..crc_offset]); // magic + roaring64_data
        let computed = hasher.finalize();
        assert_eq!(stored_crc, computed, "CRC32 must match");

        // 64-bit Roaring header inside the envelope.
        let roaring64 = &blob[8..crc_offset];
        let bitmap_count = u64::from_le_bytes(roaring64[..8].try_into().unwrap());
        assert_eq!(bitmap_count, 1, "bitmap_count must be 1");
        let key = u32::from_le_bytes(roaring64[8..12].try_into().unwrap());
        assert_eq!(key, 0, "key must be 0 for row positions < 2^32");
    }

    /// Verify CRC corruption in the DV blob body is detected.
    #[test]
    fn dv_blob_crc_corruption_detected() {
        let mut dv = DeletionVector::new();
        dv.mark_deleted(1);

        let encoded = dv.encode_puffin("data/L0/test.parquet", 1, 1).unwrap();
        let mut puffin = encoded.bytes.to_vec();

        // Corrupt one byte inside the blob body (after DV magic, before CRC).
        let blob_offset = encoded.blob_offset as usize;
        puffin[blob_offset + 10] ^= 0xFF;

        let err = DeletionVector::from_puffin_bytes(&puffin).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("CRC"), "expected CRC error: {msg}");
    }

    /// Verify the Puffin footer includes cardinality field.
    #[test]
    fn puffin_footer_has_cardinality() {
        let mut dv = DeletionVector::new();
        for i in 0..25u32 {
            dv.mark_deleted(i);
        }

        let puffin = dv.to_puffin_bytes("data/test.parquet", 1, 1).unwrap();
        let footer = parse_puffin_footer(&puffin).unwrap();
        let blob_meta = footer
            .blobs
            .iter()
            .find(|b| b.blob_type == "deletion-vector-v1")
            .unwrap();
        assert_eq!(blob_meta.fields.cardinality, 25);
    }

    /// The from_puffin_blob path (used by catalog.rs for DV union)
    /// must also correctly decode the v3 envelope.
    #[test]
    fn from_puffin_blob_decodes_v3_envelope() {
        let mut dv = DeletionVector::new();
        dv.mark_deleted(10);
        dv.mark_deleted(20);
        dv.mark_deleted(30);

        let encoded = dv.encode_puffin("data/test.parquet", 1, 1).unwrap();
        let blob_offset = encoded.blob_offset as usize;
        let blob_length = encoded.blob_length as usize;
        let blob_slice = &encoded.bytes[blob_offset..blob_offset + blob_length];

        let decoded = DeletionVector::from_puffin_blob(blob_slice).unwrap();
        assert_eq!(decoded.cardinality(), 3);
        assert!(decoded.is_deleted(10));
        assert!(decoded.is_deleted(20));
        assert!(decoded.is_deleted(30));
        assert!(!decoded.is_deleted(15));
    }

    /// Empty DV still produces valid v3 envelope (bitmap_count=1, key=0,
    /// empty 32-bit Roaring).
    #[test]
    fn empty_dv_v3_envelope_roundtrip() {
        let dv = DeletionVector::new();
        let encoded = dv.encode_puffin("data/test.parquet", 1, 1).unwrap();

        let blob_offset = encoded.blob_offset as usize;
        let blob_length = encoded.blob_length as usize;
        let blob = &encoded.bytes[blob_offset..blob_offset + blob_length];

        // Verify envelope structure.
        assert_eq!(&blob[4..8], &DV_MAGIC);
        let roaring64 = &blob[8..blob.len() - 4];
        let bitmap_count = u64::from_le_bytes(roaring64[..8].try_into().unwrap());
        assert_eq!(bitmap_count, 1);

        // Roundtrip.
        let decoded = DeletionVector::from_puffin_blob(blob).unwrap();
        assert!(decoded.is_empty());
    }
}
