//! Issue #26 Phase 2b: HEAD discovery for `CommitMode::ObjectStore`.
//!
//! # Why
//!
//! LIST calls on S3/GCS/Azure are slow, rate-limited, and paginated
//! through 1000 keys at a time. The ObjectStore commit protocol refuses
//! to use them on the commit or refresh path. HEAD discovery instead
//! probes existence keys directly: `v1`, `v2`, `v4`, `v8`, ... doubling
//! until a probe 404s, then binary-searches the gap.
//!
//! # Cost
//!
//! - Cold-open for a catalog with N commits: O(log N) HEADs.
//!   For N = 2^30 (≈ 1 billion commits), that's ≤ 60 probes. In
//!   practice, < 30.
//! - Warm refresh: O(1) — probe `v(HEAD+1)`. If it 404s, no new
//!   work; if it exists, advance one and probe again.
//!
//! # Algorithm
//!
//! Two phases, both using async closures so the probe primitive can be
//! backed by any store (LocalFileStore, S3Store, Azure Blob, etc.) via
//! the trait abstraction.
//!
//! 1. **Exponential scan.** Start at version 1. Probe exists(1). If
//!    no, HEAD is 0 (empty catalog). If yes, double: probe 2, 4, 8,
//!    16, ... until a probe returns false. The last-yes version is
//!    the binary-search lower bound; the first-no is the upper.
//!
//! 2. **Binary search.** Within `(last_yes, first_no)`, probe the
//!    midpoint. If yes, move the lower bound up; if no, move the
//!    upper bound down. Stop when they differ by one. Lower is HEAD.
//!
//! Total probes ≤ `2 * log2(HEAD)` — exponential scan and binary
//! search both contribute one pass each.

use merutable_types::{MeruError, Result};
use std::future::Future;

/// The highest version at which `exists` returned true. Zero means
/// the catalog has never been written to (the caller should emit the
/// genesis v1 manifest).
pub type Head = i64;

/// Discover the HEAD version of an ObjectStore-mode catalog.
///
/// `exists_fn` takes a version number and returns a future resolving
/// to `true` if the on-store key at that version exists. The
/// callback is invoked O(log HEAD) times total.
///
/// Propagates any non-404 error from `exists_fn` immediately — a
/// transient network failure during discovery must NOT be silently
/// treated as a missing key (that would cause the caller to emit a
/// second genesis or worse, clobber an existing chain).
pub async fn discover_head<F, Fut>(exists_fn: F) -> Result<Head>
where
    F: FnMut(i64) -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    discover_head_from(1, exists_fn).await
}

/// Like `discover_head`, but start probing from `start_version`
/// instead of 1. Used after Phase-6 manifest GC, where a low-water
/// pointer file records the lowest surviving version; starting the
/// exponential scan there keeps discovery correct when the pre-cutoff
/// chain has been reclaimed.
///
/// Invariant: the caller must pass a `start_version ≥ 1` that is
/// known to either (a) exist on the store, or (b) be the catalog's
/// genesis position (v1 on a fresh catalog). Passing a start that
/// sits inside a reclaimed gap produces `HEAD = 0` (misinterpreted as
/// empty catalog).
pub async fn discover_head_from<F, Fut>(start_version: i64, mut exists_fn: F) -> Result<Head>
where
    F: FnMut(i64) -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    if start_version < 1 {
        return Err(MeruError::Corruption(format!(
            "discover_head_from: start_version must be ≥ 1 (got {start_version})"
        )));
    }
    // Phase 1: exponential scan. Start at `start_version`.
    if !exists_fn(start_version).await? {
        // Empty catalog (or low-water gap) — caller interprets.
        return Ok(0);
    }
    let mut low = start_version; // confirmed present
                                 // Double from low, matching the classic "probe 1, 2, 4, 8..."
                                 // pattern rooted at the start position. `checked_mul` guards the
                                 // i64-overflow case; the MAX_PROBE cap below still applies.
    let mut probe = start_version
        .checked_mul(2)
        .ok_or_else(|| MeruError::Corruption("HEAD discovery probe overflow".into()))?;
    // Cap at 2^62 to keep arithmetic safe; a catalog larger than
    // that has bigger problems.
    const MAX_PROBE: i64 = 1 << 62;
    let high = loop {
        if probe > MAX_PROBE {
            return Err(MeruError::Corruption(format!(
                "HEAD discovery overflow: probe {probe} exceeded cap {MAX_PROBE}"
            )));
        }
        if exists_fn(probe).await? {
            low = probe;
            probe = probe
                .checked_mul(2)
                .ok_or_else(|| MeruError::Corruption("HEAD discovery probe overflow".into()))?;
        } else {
            break probe;
        }
    };

    // Phase 2: binary search in (low, high). Invariant: exists(low)
    // is true, exists(high) is false.
    let mut lo = low;
    let mut hi = high;
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if exists_fn(mid).await? {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    type ExistsFut = std::pin::Pin<Box<dyn Future<Output = Result<bool>>>>;
    type MockFn = Box<dyn FnMut(i64) -> ExistsFut>;

    /// Build an `exists_fn` that returns true for versions 1..=head
    /// and counts how many times it's called. Lets us prove the
    /// O(log N) contract directly.
    fn mock_store(head: i64) -> (MockFn, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_cl = calls.clone();
        let f: MockFn = Box::new(move |v: i64| -> ExistsFut {
            calls_cl.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(v <= head) })
        });
        (f, calls)
    }

    #[tokio::test]
    async fn empty_catalog_returns_zero() {
        let (f, _) = mock_store(0);
        let head = discover_head(f).await.unwrap();
        assert_eq!(head, 0);
    }

    #[tokio::test]
    async fn genesis_only_returns_one() {
        let (f, _) = mock_store(1);
        assert_eq!(discover_head(f).await.unwrap(), 1);
    }

    /// Probe counts: for HEAD=N, exponential scan takes
    /// ⌈log2(N)⌉ + 1 probes, then binary search takes another
    /// ⌈log2(N)⌉. Total should be ≤ 2*log2(N) + 2.
    #[tokio::test]
    async fn probe_count_is_logarithmic() {
        let test_heads: &[i64] = &[7, 100, 1_000, 10_000, 1_000_000];
        for &head in test_heads {
            let (f, calls) = mock_store(head);
            let found = discover_head(f).await.unwrap();
            assert_eq!(found, head, "discovery must return the true HEAD");
            let probes = calls.load(Ordering::SeqCst);
            let log2 = 64 - (head as u64).leading_zeros() as usize;
            let bound = 2 * log2 + 4; // extra slack for boundary probes
            assert!(
                probes <= bound,
                "probes {probes} exceeded bound {bound} for HEAD {head}"
            );
        }
    }

    /// Exact HEAD at a power of two — exercises the boundary where
    /// the exponential scan's last-yes probe is also the answer.
    #[tokio::test]
    async fn head_at_power_of_two() {
        for head in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
            let (f, _) = mock_store(head);
            assert_eq!(discover_head(f).await.unwrap(), head);
        }
    }

    /// HEAD just below / just above a power of two — catches
    /// off-by-one in the binary-search bounds.
    #[tokio::test]
    async fn head_near_power_of_two() {
        for head in [3, 5, 7, 15, 17, 63, 65, 1023, 1025] {
            let (f, _) = mock_store(head);
            assert_eq!(discover_head(f).await.unwrap(), head, "HEAD={head}");
        }
    }

    /// A transient error from the store aborts discovery — we must
    /// NOT treat an error as a missing key (that silently clobbers
    /// the chain).
    #[tokio::test]
    async fn transient_error_propagates() {
        let mut calls = 0;
        let f = move |_v: i64| -> std::pin::Pin<Box<dyn Future<Output = Result<bool>>>> {
            calls += 1;
            let n = calls;
            Box::pin(async move {
                if n >= 3 {
                    Err(MeruError::ObjectStore("flaky network".into()))
                } else {
                    Ok(true)
                }
            })
        };
        let err = match discover_head(f).await {
            Err(e) => e,
            Ok(v) => panic!("expected error, got HEAD={v}"),
        };
        assert!(format!("{err:?}").contains("flaky network"));
    }
}
