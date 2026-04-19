//! Issue #32: scale-out RO replica for merutable.
//!
//! The replica composes two sources to serve a fresh view of a
//! primary:
//!
//! - **Base**: an object-store layout (#26 / #31 mirror target).
//!   Read via `merutable::OpenOptions::read_only(true) +
//!   CommitMode::ObjectStore`.
//! - **Tail**: a streamed log of ops newer than the base. The log
//!   source is pluggable; see [`LogSource`] for the contract.
//!
//! The replica advances in two modes:
//!
//! 1. **Append-only tail advance** (common case): new log ops
//!    replay into the in-memory tail, `visible_seq` advances.
//!    Cheap; monotonic; no rebase.
//! 2. **Hot-swap rebase** (on mirror advance): a new `ReplicaState`
//!    is warmed up against the new base snapshot in parallel with
//!    the old one continuing to serve reads. When the new state
//!    catches up, an `ArcSwap` pointer atomically retargets.
//!
//! # Phases
//!
//! - **Phase 1 (this crate, today)**: `LogSource` trait, `OpRecord`
//!   type shape, `LogGap` error, placeholder `ChangeFeedLogSource`
//!   stub that returns `LogGap` unconditionally so the v1 impl
//!   contract is pinned ahead of #29 Phase 2 landing. No
//!   `ReplicaState` or rebase worker yet.
//! - **Phase 2 (planned)**: `ChangeFeedLogSource` real impl over a
//!   `merutable_changes` Flight SQL endpoint (requires #29 Phase 2).
//! - **Phase 3 (planned)**: `ReplicaState` + append-only tail
//!   advance. No rebase; growing tail OK for this phase.
//! - **Phase 4 (planned)**: hot-swap rebase worker + drain TTL.
//! - **Phase 5 (planned)**: metrics surface, log-gap recovery,
//!   stress test harness.

use async_trait::async_trait;
use futures::stream::BoxStream;
use merutable_sql::ChangeOp;
use merutable_types::{value::Row, MeruError, Result};

/// A single log op visible to the replica. Same shape as a
/// change-feed record except that the replica needs `op_type`
/// explicitly (so it can apply tombstones) and consumes `row` as
/// owned data (the stream hands it off).
///
/// The seq defines ordering; replicas MUST observe ops in seq-ascending
/// order and MUST reject out-of-order delivery as a corrupted source
/// (the `LogSource` contract guarantees ordering).
#[derive(Clone, Debug)]
pub struct OpRecord {
    pub seq: u64,
    pub op: ChangeOp,
    pub row: Row,
}

/// A log source's view of the replica's starting point was below
/// the source's earliest retained seq. The replica's only recourse
/// is a hard reset: pick a new base snapshot from the object store
/// and rebuild the tail from `mirror_seq` forward.
///
/// Separate from `MeruError::ChangeFeedBelowRetention` (which is
/// the primary's retention-bound error for `merutable_changes`);
/// the replica layer surfaces its own variant so callers can
/// distinguish "primary said below retention" from "my tail source
/// timed out".
#[derive(Clone, Debug)]
pub struct LogGap {
    /// The seq the replica asked the source to stream from.
    pub requested: u64,
    /// The source's earliest available seq (if it can report one).
    pub earliest_available: Option<u64>,
    /// Human-readable reason for the gap (e.g. "Kafka offset
    /// retention exceeded", "change-feed low-water advanced").
    pub reason: String,
}

impl std::fmt::Display for LogGap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "log gap: requested={}, earliest_available={:?}, reason={}",
            self.requested, self.earliest_available, self.reason
        )
    }
}

impl std::error::Error for LogGap {}

/// Pluggable log source. v1 implementation will be
/// [`ChangeFeedLogSource`] consuming `merutable_changes` over
/// Flight SQL. Future implementations — `RaftLogSource`,
/// `KafkaLogSource`, `ObjectStoreLogSource` — plug in by
/// implementing this trait without touching the replica core.
#[async_trait]
pub trait LogSource: Send + Sync + 'static {
    /// Stream ops with `seq > since` in seq-ascending order.
    /// Source-defined retention; returns `Err(LogGap)` if `since`
    /// is below the source's earliest available seq.
    async fn stream(&self, since: u64) -> Result<BoxStream<'static, Result<OpRecord>>>;

    /// Source-side latest-known seq. Best-effort; the stream is
    /// the source of truth. Used by the replica to decide when to
    /// kick a refresh.
    async fn latest_seq(&self) -> Result<u64>;
}

/// Phase 1 placeholder. Returns `LogGap` on every call so the v1
/// impl contract is pinned and callers can exercise their
/// hard-reset recovery path today. Real impl in Phase 2 consumes
/// `merutable_changes(table, since_seq)` over the primary's Flight
/// SQL endpoint.
pub struct ChangeFeedLogSource {
    /// The primary's retention low-water as of the last probe.
    /// Included in `LogGap::earliest_available` so the replica
    /// knows where to restart from.
    pub primary_low_water: u64,
}

impl ChangeFeedLogSource {
    pub fn new(primary_low_water: u64) -> Self {
        Self { primary_low_water }
    }
}

#[async_trait]
impl LogSource for ChangeFeedLogSource {
    async fn stream(&self, since: u64) -> Result<BoxStream<'static, Result<OpRecord>>> {
        Err(MeruError::ChangeFeedBelowRetention {
            requested: since,
            low_water: self.primary_low_water,
        })
    }

    async fn latest_seq(&self) -> Result<u64> {
        Err(MeruError::InvalidArgument(
            "ChangeFeedLogSource::latest_seq: Phase 2 pending (requires #29 Phase 2 \
             Flight SQL endpoint)"
                .into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn phase1_stream_returns_below_retention() {
        let src = ChangeFeedLogSource::new(1000);
        let err = src.stream(500).await.err().unwrap();
        match err {
            MeruError::ChangeFeedBelowRetention {
                requested,
                low_water,
            } => {
                assert_eq!(requested, 500);
                assert_eq!(low_water, 1000);
            }
            other => panic!("unexpected error shape: {other:?}"),
        }
    }

    #[tokio::test]
    async fn phase1_latest_seq_errors_with_pointer() {
        let src = ChangeFeedLogSource::new(0);
        let err = src.latest_seq().await.err().unwrap();
        assert!(format!("{err:?}").contains("Phase 2"));
    }

    #[test]
    fn log_gap_display_is_informative() {
        let gap = LogGap {
            requested: 42,
            earliest_available: Some(100),
            reason: "change-feed retention".into(),
        };
        let s = format!("{gap}");
        assert!(s.contains("42"));
        assert!(s.contains("100"));
        assert!(s.contains("retention"));
    }
}
