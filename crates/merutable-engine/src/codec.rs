//! Row serialization codec for the WAL value path.
//!
//! Uses `postcard` (compact binary serde) for all new writes. Falls back to
//! `serde_json` for data written before the migration — WAL replay can
//! encounter old JSON-encoded rows during crash recovery.
//!
//! **Discrimination**: JSON-encoded `Row` always starts with `{` (0x7B).
//! `postcard`-encoded `Row` starts with a varint for the `fields` vec length,
//! which for rows with < 123 columns will never be 0x7B. To make the boundary
//! unambiguous regardless of column count, we prepend a single `0x01` marker
//! byte to postcard output.

use merutable_types::{value::Row, MeruError, Result};

/// Marker byte prepended to postcard-encoded rows.
/// JSON never starts with 0x01, so this is a reliable discriminator.
const POSTCARD_MARKER: u8 = 0x01;

/// Serialize a `Row` to bytes using postcard (binary, ~10× faster than JSON).
/// Prepends a 1-byte format marker for backward-compatible decoding.
#[inline]
pub fn encode_row(row: &Row) -> Result<Vec<u8>> {
    let raw = postcard::to_allocvec(row)
        .map_err(|e| MeruError::InvalidArgument(format!("row postcard serialize failed: {e}")))?;
    let mut out = Vec::with_capacity(1 + raw.len());
    out.push(POSTCARD_MARKER);
    out.extend_from_slice(&raw);
    Ok(out)
}

/// Deserialize a `Row` from bytes.
///
/// - If the first byte is `POSTCARD_MARKER` (0x01): strip it and postcard-decode.
/// - Otherwise: fall back to `serde_json` (legacy WAL data).
/// - Empty input → `Row::default()`.
#[inline]
pub fn decode_row(bytes: &[u8]) -> Row {
    if bytes.is_empty() {
        return Row::default();
    }
    if bytes[0] == POSTCARD_MARKER {
        // New format: postcard after the marker byte.
        postcard::from_bytes(&bytes[1..]).unwrap_or_default()
    } else {
        // Legacy format: JSON.
        serde_json::from_slice(bytes).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use merutable_types::value::FieldValue;

    #[test]
    fn roundtrip_postcard() {
        let row = Row::new(vec![
            Some(FieldValue::Int64(42)),
            None,
            Some(FieldValue::Bytes(Bytes::from("hello"))),
        ]);
        let encoded = encode_row(&row).unwrap();
        assert_eq!(encoded[0], POSTCARD_MARKER);
        let decoded = decode_row(&encoded);
        assert_eq!(decoded, row);
    }

    #[test]
    fn decode_legacy_json() {
        let row = Row::new(vec![
            Some(FieldValue::Int64(1)),
            Some(FieldValue::Boolean(true)),
        ]);
        let json = serde_json::to_vec(&row).unwrap();
        // First byte of JSON is '{' (0x7B), not 0x01.
        assert_ne!(json[0], POSTCARD_MARKER);
        let decoded = decode_row(&json);
        assert_eq!(decoded, row);
    }

    #[test]
    fn decode_empty() {
        let decoded = decode_row(&[]);
        assert_eq!(decoded, Row::default());
    }

    #[test]
    fn postcard_smaller_than_json() {
        let row = Row::new(vec![
            Some(FieldValue::Int64(123456789)),
            Some(FieldValue::Double(98.765)),
            Some(FieldValue::Bytes(Bytes::from("test data"))),
            None,
        ]);
        let postcard_bytes = encode_row(&row).unwrap();
        let json_bytes = serde_json::to_vec(&row).unwrap();
        // postcard should be significantly smaller.
        assert!(
            postcard_bytes.len() < json_bytes.len(),
            "postcard={} json={}",
            postcard_bytes.len(),
            json_bytes.len()
        );
    }
}
