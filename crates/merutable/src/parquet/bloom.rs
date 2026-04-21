//! FastLocalBloom: cache-line-aligned bloom filter with AVX2/NEON/scalar dispatch.
//!
//! Design mirrors RocksDB's `FastLocalBloomImpl`. One 64-byte cache-line bucket per
//! probe set, ensuring ≤1 cache miss per query regardless of num_probes.
//!
//! # Hash
//! xxhash3-64 → h1 (high 32 bits) = bucket selector, h2 (low 32 bits) = probe driver.
//!
//! # Serialization
//! `[num_probes: u8][num_buckets: u32 LE][data: num_buckets * 64 bytes]`
//! Stored at `ColumnMetaData.bloom_filter_offset` of the `_merutable_ikey` column.

use crate::types::{MeruError, Result};
use bytes::{BufMut, Bytes, BytesMut};
use once_cell::sync::Lazy;
use xxhash_rust::xxh3::xxh3_64;

// ── Runtime dispatch ──────────────────────────────────────────────────────────

type ProbeFn = fn(data: &[u8], h1: u32, h2: u32, num_probes: u8) -> bool;

static PROBE_FN: Lazy<ProbeFn> = Lazy::new(|| {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        return probe_avx2_dispatch;
    }
    #[cfg(target_arch = "aarch64")]
    {
        // NEON is always available on AArch64; no runtime check needed.
        return probe_neon_dispatch;
    }
    #[allow(unreachable_code)]
    probe_scalar
});

// ── FastLocalBloom ────────────────────────────────────────────────────────────

/// Cache-line-aligned bloom filter.
pub struct FastLocalBloom {
    /// `num_buckets * 64` bytes. The Vec's underlying allocation is not
    /// guaranteed to be 64-byte aligned, but cache-line crossings only affect
    /// performance (not correctness). Phase 10 will use aligned allocation.
    data: Vec<u8>,
    num_probes: u8,
}

impl FastLocalBloom {
    /// Create a new bloom filter for `num_keys` expected entries at `bits_per_key`.
    pub fn new(num_keys: usize, bits_per_key: u8) -> Self {
        let num_probes = choose_num_probes(bits_per_key);
        let total_bits = (num_keys as u64 * bits_per_key as u64).max(512);
        // Round up to a multiple of 512 bits (64 bytes = one cache line).
        let num_buckets = total_bits.div_ceil(512) as usize;
        Self {
            data: vec![0u8; num_buckets * 64],
            num_probes,
        }
    }

    /// Add a key to the filter. `key` = user key bytes (PK without tag).
    #[inline]
    pub fn add(&mut self, key: &[u8]) {
        let hash = xxh3_64(key);
        let h1 = (hash >> 32) as u32;
        let h2 = hash as u32;
        add_inner(&mut self.data, h1, h2, self.num_probes);
    }

    /// Query. Returns `false` = definitely absent; `true` = probably present.
    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let hash = xxh3_64(key);
        let h1 = (hash >> 32) as u32;
        let h2 = hash as u32;
        PROBE_FN(&self.data, h1, h2, self.num_probes)
    }

    /// Serialize: `[num_probes: u8][num_buckets: u32 LE][data]`
    pub fn to_bytes(&self) -> Bytes {
        let num_buckets = (self.data.len() / 64) as u32;
        let mut buf = BytesMut::with_capacity(5 + self.data.len());
        buf.put_u8(self.num_probes);
        buf.put_u32_le(num_buckets);
        buf.put_slice(&self.data);
        buf.freeze()
    }

    /// Deserialize from bytes produced by `to_bytes`.
    ///
    /// Rejects corrupted or semantically-invalid inputs:
    /// - `num_buckets == 0`: an empty bucket table would panic on the
    ///   first `add`/`may_contain` call because `fast_range32(_, 0) = 0`
    ///   then slices `data[0..64]` on an empty backing `Vec`.
    /// - `num_probes == 0`: a zero-probe filter would return `true` for
    ///   every query (the probe loop runs zero iterations and short-
    ///   circuits to the terminal `true`), silently disabling the filter
    ///   and wasting the footer KV bytes.
    /// - `num_buckets * 64` overflow: checked multiply on 32-bit targets.
    pub fn from_bytes(raw: &[u8]) -> Result<Self> {
        if raw.len() < 5 {
            return Err(MeruError::Corruption("bloom filter bytes too short".into()));
        }
        let num_probes = raw[0];
        if num_probes == 0 {
            return Err(MeruError::Corruption(
                "bloom filter num_probes must be ≥ 1".into(),
            ));
        }
        let num_buckets = u32::from_le_bytes(raw[1..5].try_into().unwrap()) as usize;
        if num_buckets == 0 {
            return Err(MeruError::Corruption(
                "bloom filter num_buckets must be ≥ 1".into(),
            ));
        }
        let expected = num_buckets.checked_mul(64).ok_or_else(|| {
            MeruError::Corruption(format!(
                "bloom filter num_buckets {num_buckets} overflows usize when scaled by 64"
            ))
        })?;
        if raw.len() - 5 != expected {
            return Err(MeruError::Corruption(format!(
                "bloom data length mismatch: expected {expected}, got {}",
                raw.len() - 5
            )));
        }
        Ok(Self {
            num_probes,
            data: raw[5..].to_vec(),
        })
    }

    pub fn num_probes(&self) -> u8 {
        self.num_probes
    }
    pub fn num_buckets(&self) -> usize {
        self.data.len() / 64
    }
}

// ── Core bit operations ───────────────────────────────────────────────────────

#[inline]
fn fast_range32(hash: u32, n: u32) -> usize {
    ((hash as u64 * n as u64) >> 32) as usize
}

fn add_inner(data: &mut [u8], h1: u32, h2: u32, num_probes: u8) {
    let num_buckets = (data.len() / 64) as u32;
    let bucket_idx = fast_range32(h1, num_buckets);
    let line = &mut data[bucket_idx * 64..(bucket_idx + 1) * 64];
    let mut h = h2;
    for _ in 0..num_probes {
        let bitpos = ((h >> 23) & 511) as usize;
        line[bitpos >> 3] |= 1u8 << (bitpos & 7);
        h = h.wrapping_mul(0x9e3779b9);
    }
}

// ── Scalar probe ──────────────────────────────────────────────────────────────

fn probe_scalar(data: &[u8], h1: u32, h2: u32, num_probes: u8) -> bool {
    let num_buckets = (data.len() / 64) as u32;
    let bucket_idx = fast_range32(h1, num_buckets);
    let line = &data[bucket_idx * 64..(bucket_idx + 1) * 64];
    let mut h = h2;
    for _ in 0..num_probes {
        let bitpos = ((h >> 23) & 511) as usize;
        if line[bitpos >> 3] & (1u8 << (bitpos & 7)) == 0 {
            return false;
        }
        h = h.wrapping_mul(0x9e3779b9);
    }
    true
}

// ── AVX2 probe ────────────────────────────────────────────────────────────────
//
// Phase 10 (SIMD optimization pass) will replace this with a real AVX2 probe
// using `_mm256_*` intrinsics. For now we delegate to the scalar implementation
// so that the dispatch infrastructure (runtime detection + fn pointer) is in
// place without depending on a working SIMD body.

#[cfg(target_arch = "x86_64")]
fn probe_avx2_dispatch(data: &[u8], h1: u32, h2: u32, num_probes: u8) -> bool {
    probe_scalar(data, h1, h2, num_probes)
}

#[cfg(not(target_arch = "x86_64"))]
#[allow(dead_code)]
fn probe_avx2_dispatch(data: &[u8], h1: u32, h2: u32, num_probes: u8) -> bool {
    probe_scalar(data, h1, h2, num_probes)
}

// ── NEON probe ────────────────────────────────────────────────────────────────

#[cfg(target_arch = "aarch64")]
fn probe_neon_dispatch(data: &[u8], h1: u32, h2: u32, num_probes: u8) -> bool {
    // SAFETY: NEON is always available on AArch64.
    unsafe { probe_neon(data, h1, h2, num_probes) }
}

#[cfg(not(target_arch = "aarch64"))]
#[allow(dead_code)]
fn probe_neon_dispatch(data: &[u8], h1: u32, h2: u32, num_probes: u8) -> bool {
    probe_scalar(data, h1, h2, num_probes)
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn probe_neon(data: &[u8], h1: u32, h2: u32, num_probes: u8) -> bool {
    // NEON implementation: 4-wide uint32x4_t parallel probing.
    // For now, delegate to scalar; Phase 10 fills in the NEON intrinsics.
    probe_scalar(data, h1, h2, num_probes)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn choose_num_probes(bits_per_key: u8) -> u8 {
    let k = (bits_per_key as f64 * std::f64::consts::LN_2).round() as u8;
    k.clamp(1, 30)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let n = 10_000usize;
        let mut bloom = FastLocalBloom::new(n, 10);
        let keys: Vec<Vec<u8>> = (0..n as u64).map(|i| i.to_le_bytes().to_vec()).collect();
        for k in &keys {
            bloom.add(k);
        }
        for k in &keys {
            assert!(bloom.may_contain(k), "false negative for key {:?}", k);
        }
    }

    #[test]
    fn fpr_within_bounds() {
        let n = 10_000usize;
        let mut bloom = FastLocalBloom::new(n, 10);
        for i in 0..n as u64 {
            bloom.add(&i.to_le_bytes());
        }

        let total = 100_000u64;
        let mut fp = 0u64;
        for i in 0..total {
            let k = (i + 1_000_000u64).to_le_bytes();
            if bloom.may_contain(&k) {
                fp += 1;
            }
        }
        let fpr = fp as f64 / total as f64;
        assert!(fpr < 0.015, "FPR {fpr:.4} exceeds 1.5% for 10 bits/key");
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let mut bloom = FastLocalBloom::new(1000, 10);
        bloom.add(b"hello");
        bloom.add(b"world");
        let bytes = bloom.to_bytes();
        let decoded = FastLocalBloom::from_bytes(&bytes).unwrap();
        assert!(decoded.may_contain(b"hello"));
        assert!(decoded.may_contain(b"world"));
        assert_eq!(decoded.num_probes(), bloom.num_probes());
    }

    #[test]
    fn empty_bloom_contains_nothing() {
        let bloom = FastLocalBloom::new(1000, 10);
        let mut fp = 0u64;
        for i in 0..10_000u64 {
            if bloom.may_contain(&i.to_le_bytes()) {
                fp += 1;
            }
        }
        assert_eq!(fp, 0, "empty bloom should have no false positives");
    }

    #[test]
    fn choose_probes_reasonable() {
        assert_eq!(choose_num_probes(10), 7);
        assert_eq!(choose_num_probes(8), 6);
        assert_eq!(choose_num_probes(12), 8);
    }

    /// Corruption: a serialized bloom with `num_buckets = 0` would crash
    /// the first `add`/`may_contain` call by slicing `data[0..64]` on an
    /// empty backing `Vec`. `from_bytes` must reject it up front.
    #[test]
    fn from_bytes_rejects_zero_buckets() {
        let raw = [7u8, 0, 0, 0, 0]; // num_probes=7, num_buckets=0
        let result = FastLocalBloom::from_bytes(&raw);
        assert!(matches!(result, Err(MeruError::Corruption(_))));
    }

    /// Corruption: `num_probes = 0` would make the probe loop iterate
    /// zero times and fall through to the terminal `true`, turning every
    /// query into a false positive and silently disabling the filter.
    #[test]
    fn from_bytes_rejects_zero_probes() {
        // num_probes=0, num_buckets=1, 64 bytes of zeros.
        let mut raw = vec![0u8, 1, 0, 0, 0];
        raw.extend(std::iter::repeat_n(0u8, 64));
        let result = FastLocalBloom::from_bytes(&raw);
        assert!(matches!(result, Err(MeruError::Corruption(_))));
    }

    /// Length/field-count mismatches must be rejected cleanly (not
    /// panic, not silently accept).
    #[test]
    fn from_bytes_rejects_length_mismatch() {
        // Header says num_buckets=2 (→ 128 bytes) but only 64 bytes follow.
        let mut raw = vec![7u8, 2, 0, 0, 0];
        raw.extend(std::iter::repeat_n(0u8, 64));
        let result = FastLocalBloom::from_bytes(&raw);
        assert!(matches!(result, Err(MeruError::Corruption(_))));
    }

    /// Serialized round-trip of the minimum legal filter
    /// (`num_buckets = 1`) must decode and behave correctly end-to-end.
    #[test]
    fn round_trip_minimum_size_filter() {
        // Build the smallest legal bloom (num_keys tiny, floor kicks in).
        let mut bloom = FastLocalBloom::new(1, 10);
        bloom.add(b"present");
        let bytes = bloom.to_bytes();
        let decoded = FastLocalBloom::from_bytes(&bytes).unwrap();
        assert!(decoded.may_contain(b"present"));
        assert_eq!(decoded.num_buckets(), bloom.num_buckets());
        assert!(decoded.num_buckets() >= 1);
    }
}
