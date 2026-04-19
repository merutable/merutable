//! Issue #29 (0.1-preview blocker): SQL surface for merutable.
//!
//! This crate is the home of the `merutable_changes(table, since_seq)`
//! table function — the transaction-granularity change feed served by
//! the RO replica via an embedded DataFusion `SessionContext`.
//!
//! # Phase 1 (this module)
//!
//! API surface only:
//! - [`ChangeOp`] — the three operation kinds a change-feed row can
//!   carry: Insert / Update / Delete.
//! - [`ChangeRecord`] — one row of the feed: (seq, op, row).
//! - [`ChangeFeedCursor`] — type-shape placeholder for the polling
//!   reader contract. Returns `MeruError::ChangeFeedBelowRetention`
//!   today; real iteration plumbing lands in Phase 2.
//!
//! # Phase 2 (planned)
//!
//! - Engine-side merge iterator spanning memtable + L0 + L1..LN with
//!   seq-based filtering + DELETE pre-image reconstruction via LSM
//!   point lookup at `seq - 1`.
//! - DataFusion `TableProvider` wrapper exposing the iterator as
//!   `merutable_changes(table, since_seq)` with:
//!   * tight `statistics()` (rows ≤ `visible_seq - since_seq`)
//!   * `output_ordering` = seq ASC so `ORDER BY seq` elides a sort
//!   * seq-range filter pushdown (`Exact`)
//!   * projection pushdown through row decode
//! - Retention-bound escalation: callers below `low_water` get a
//!   structured error with both the requested seq and current
//!   low-water, so they can escalate to an Iceberg snapshot scan.
//! - Integration tests covering the full mixed-op contract +
//!   DELETE pre-image correctness.
//!
//! # Phase 3 (0.5-beta)
//!
//! - Arrow Flight SQL server binary wrapping the in-process
//!   SessionContext.
//! - Merged overlay view that UNIONs an Iceberg base scan with the
//!   change feed and resolves last-writer-wins per PK.
//! - Streaming subscription API (pushes new ops vs. polling).

use merutable_types::{value::Row, MeruError, Result};

/// The kind of mutation a change-feed row represents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeOp {
    /// New key — no prior row existed at `since_seq`.
    Insert,
    /// Existing key re-written.
    Update,
    /// Key deleted. The accompanying row is the pre-image
    /// reconstructed via LSM point lookup at `seq - 1`.
    Delete,
}

impl ChangeOp {
    /// SQL-compatible text label: `'INSERT' | 'UPDATE' | 'DELETE'`.
    pub fn as_sql_str(self) -> &'static str {
        match self {
            ChangeOp::Insert => "INSERT",
            ChangeOp::Update => "UPDATE",
            ChangeOp::Delete => "DELETE",
        }
    }
}

/// One row of the change feed.
#[derive(Clone, Debug)]
pub struct ChangeRecord {
    /// Sequence number the mutation was committed at.
    pub seq: u64,
    /// Kind of mutation.
    pub op: ChangeOp,
    /// Full row payload. For `Insert`/`Update` this is the post-state;
    /// for `Delete` this is the pre-image at `seq - 1` reconstructed
    /// via a point lookup on the LSM.
    pub row: Row,
}

/// Polling cursor over the change feed.
///
/// Issue #29 Phase 2 will implement `next_batch` against a real LSM
/// iterator. Phase 1 returns the retention-bound error for every
/// call so callers see the stable error shape ahead of time and can
/// wire their escalation logic now.
pub struct ChangeFeedCursor {
    since_seq: u64,
    low_water: u64,
}

impl ChangeFeedCursor {
    /// Open a cursor at `since_seq`. Real impl (Phase 2) takes an
    /// `Arc<MeruDB>` in read-only mode; this placeholder takes just
    /// the low-water so the error is meaningful.
    pub fn new(since_seq: u64, low_water: u64) -> Self {
        Self {
            since_seq,
            low_water,
        }
    }

    /// Pull up to `max_rows` records from the feed. Phase 1 returns
    /// `ChangeFeedBelowRetention` unconditionally — callers can wire
    /// their escalation path today.
    pub fn next_batch(&mut self, _max_rows: usize) -> Result<Vec<ChangeRecord>> {
        // Phase 2 will split into: (a) in-retention case that walks
        // memtable+SSTables in seq order, (b) below-retention case
        // that returns this error.
        Err(MeruError::ChangeFeedBelowRetention {
            requested: self.since_seq,
            low_water: self.low_water,
        })
    }
}
