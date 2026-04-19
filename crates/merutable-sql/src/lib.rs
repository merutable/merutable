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
//! # Phase 2a (shipped): memtable-only change scan
//!
//! The [`ChangeFeedCursor`] in-retention path pulls real records
//! from `MeruEngine::scan_memtable_changes`. Sufficient for
//! low-latency subscribers (RO replicas, audit log tailers) as
//! long as they keep up with the flush cadence.
//!
//! # Phase 2b (this commit): memtable + L0 scan
//!
//! The cursor now calls `MeruEngine::scan_tail_changes`, which
//! extends the scan across L0 Parquet files. Ops that flushed out
//! of the memtable into L0 stay visible as long as the file lives
//! in L0 (i.e., until compacted into L1). This lifts the "must
//! keep up with flush cadence" constraint — subscribers can fall
//! multiple snapshots behind and still recover without escalating.
//!
//! DELETE records carry an empty pre-image `Row` in both 2a and
//! 2b; the `seq - 1` point-lookup reconstruction arrives in
//! Phase 2c.
//!
//! # Phase 2c (planned)
//!
//! - L1..LN scan (seq-range-filtered) + DELETE pre-image
//!   reconstruction via LSM point lookup at `seq - 1`.
//!
//! # Phase 2d (planned)
//!
//! - DataFusion `TableProvider` wrapper exposing the iterator as
//!   `merutable_changes(table, since_seq)` with tight statistics,
//!   seq-ordered output, and filter-pushdown (`Exact`).
//!
//! # Phase 3 (0.5-beta)
//!
//! - Arrow Flight SQL server binary wrapping the in-process
//!   SessionContext.
//! - Merged overlay view that UNIONs an Iceberg base scan with the
//!   change feed and resolves last-writer-wins per PK.
//! - Streaming subscription API (pushes new ops vs. polling).

use std::sync::Arc;

use merutable_engine::engine::MeruEngine;
use merutable_types::{
    sequence::{OpType, SeqNum},
    value::Row,
    MeruError, Result,
};

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
    /// via a point lookup on the LSM. Phase 2a/2b still return an
    /// empty `Row` for deletes — the pre-image reconstruction is
    /// Phase 2c. Consumers that need only the PK (e.g. replica
    /// tails applying tombstones) should use `pk_bytes`.
    pub row: Row,
    /// PK-encoded bytes of the affected key. Populated for every
    /// op (Insert, Update, Delete) — this is the canonical way to
    /// address the mutation across the memtable + SSTable scan
    /// boundary. Replicas key their tail index on these bytes;
    /// tombstones without a pre-image still carry the PK.
    pub pk_bytes: Vec<u8>,
}

/// Polling cursor over the change feed.
///
/// Phase 2a: `next_batch` scans the memtable for ops in
/// `(since_seq, read_seq]` and returns them. Phase 1's retention-
/// bound stub is preserved — constructing a cursor with
/// [`ChangeFeedCursor::new_below_retention`] still returns the
/// stable error shape on every call.
pub struct ChangeFeedCursor {
    inner: CursorInner,
}

enum CursorInner {
    Engine {
        engine: Arc<MeruEngine>,
        since_seq: u64,
    },
    BelowRetention {
        requested: u64,
        low_water: u64,
    },
}

impl ChangeFeedCursor {
    /// Open a cursor that pulls from the running engine's memtable.
    /// Phase 2a scope: memtable only. Rows with seq in
    /// `(since_seq, engine.read_seq()]` are returned.
    pub fn from_engine(engine: Arc<MeruEngine>, since_seq: u64) -> Self {
        Self {
            inner: CursorInner::Engine { engine, since_seq },
        }
    }

    /// Legacy Phase 1 shape — returns `ChangeFeedBelowRetention` on
    /// every `next_batch` so callers wiring escalation paths can
    /// keep exercising them.
    pub fn new_below_retention(requested: u64, low_water: u64) -> Self {
        Self {
            inner: CursorInner::BelowRetention {
                requested,
                low_water,
            },
        }
    }

    /// Pull up to `max_rows` records from the feed.
    ///
    /// Phase 2a scope:
    /// - Engine-backed cursor walks `scan_memtable_changes`, takes
    ///   the first `max_rows` ops by seq, and advances `since_seq`
    ///   past the highest returned seq so the next call continues
    ///   from there.
    /// - Below-retention cursor returns the stable error on every
    ///   call until the caller resets.
    pub fn next_batch(&mut self, max_rows: usize) -> Result<Vec<ChangeRecord>> {
        match &mut self.inner {
            CursorInner::BelowRetention {
                requested,
                low_water,
            } => Err(MeruError::ChangeFeedBelowRetention {
                requested: *requested,
                low_water: *low_water,
            }),
            CursorInner::Engine { engine, since_seq } => {
                let read_seq = engine.read_seq();
                if SeqNum(*since_seq) >= read_seq {
                    return Ok(Vec::new());
                }
                // Phase 2b: include L0 as well as the memtable.
                let raw = engine.scan_tail_changes(*since_seq, read_seq)?;
                let mut out = Vec::with_capacity(raw.len().min(max_rows));
                for tuple in raw.into_iter().take(max_rows) {
                    let op = match tuple.op_type {
                        OpType::Put => {
                            // An Update vs. Insert discrimination requires
                            // knowing whether a prior key existed — that's a
                            // Phase 2c task (pre-image reconstruction). For
                            // Phase 2a/b we tag every Put as Insert. Callers
                            // tracking write-pattern details are expected to
                            // opt into the Phase 2c upgrade when it lands.
                            ChangeOp::Insert
                        }
                        OpType::Delete => ChangeOp::Delete,
                    };
                    *since_seq = tuple.seq;
                    out.push(ChangeRecord {
                        seq: tuple.seq,
                        op,
                        row: tuple.row,
                        pk_bytes: tuple.pk_bytes,
                    });
                }
                Ok(out)
            }
        }
    }

    /// Current `since_seq` — advances past each batch. Readers
    /// persisting a resume point read this after `next_batch`.
    pub fn since_seq(&self) -> u64 {
        match &self.inner {
            CursorInner::Engine { since_seq, .. } => *since_seq,
            CursorInner::BelowRetention { requested, .. } => *requested,
        }
    }
}
