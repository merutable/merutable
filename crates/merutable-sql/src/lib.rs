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
//! # Phase 2b (shipped): memtable + L0 scan
//!
//! The cursor calls `MeruEngine::scan_tail_changes` and sees ops
//! across memtable + L0. Subscribers can fall multiple snapshots
//! behind without escalating.
//!
//! # Phase 2d (this commit): Arrow RecordBatch adapter
//!
//! New `crate::arrow` module converts `Vec<ChangeRecord>` into the
//! Arrow columnar form DataFusion expects. Schema shape:
//! `seq UInt64, op Utf8, pk_bytes Binary, <user columns>`. Phase
//! 2e wires a `TableProvider` that produces these batches on
//! demand from a `ChangeFeedCursor`; landing the adapter
//! separately keeps the dep graph free of DataFusion until the
//! TableProvider actually ships.
//!
//! # Phase 2c (shipped): pre-image reconstruction + INSERT vs UPDATE
//!
//! The cursor now calls `scan_tail_changes_with_pre_image`, which
//! resolves each Delete op's pre-image via a point lookup at
//! `seq - 1`. Records for Delete ops now carry the row that was
//! live immediately before the delete — sufficient for a change
//! consumer to invalidate its own derived state or replay against
//! a sibling system.
//!
//! INSERT vs UPDATE is now distinguished by a pre-image lookup on
//! Puts: a Put with no prior live state at `seq - 1` is an Insert;
//! one with prior state is an Update. The `op` field of the
//! `ChangeRecord` now reflects that distinction. Workloads
//! dominated by pure Inserts pay one extra point-lookup per op;
//! callers that don't need the distinction can set
//! `ChangeFeedCursor::skip_update_discrimination(true)` to keep
//! every Put tagged Insert (the Phase 2a behavior) — cheaper by
//! 1 lookup per op.
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

pub mod arrow;

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
    /// When true, every Put op is tagged `Insert` without the
    /// pre-image lookup that would distinguish Insert from Update.
    /// Defaults to false (full Phase 2c discrimination).
    skip_update_discrimination: bool,
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
            skip_update_discrimination: false,
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
            skip_update_discrimination: false,
        }
    }

    /// Opt out of Insert/Update discrimination. Every Put op is
    /// tagged Insert. Saves one point-lookup per op — useful for
    /// subscribers that only care about the key + op kind at a
    /// coarse level (replicas applying LWW, audit tailers that
    /// don't branch on INSERT vs UPDATE).
    pub fn skip_update_discrimination(mut self, skip: bool) -> Self {
        self.skip_update_discrimination = skip;
        self
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
                // Phase 2c: pre-image reconstruction. Walks the
                // tail AND resolves each Delete's prior live state.
                let raw = engine.scan_tail_changes_with_pre_image(*since_seq, read_seq)?;
                let mut out = Vec::with_capacity(raw.len().min(max_rows));
                for tuple in raw.into_iter().take(max_rows) {
                    let op = match tuple.op_type {
                        OpType::Put => {
                            if self.skip_update_discrimination {
                                ChangeOp::Insert
                            } else {
                                // Phase 2c: distinguish Insert from
                                // Update by probing whether a live
                                // row existed at `seq - 1`.
                                let had_prior = if tuple.seq == 0 {
                                    false
                                } else {
                                    engine
                                        .point_lookup_by_user_key_at_seq(
                                            &tuple.pk_bytes,
                                            SeqNum(tuple.seq - 1),
                                        )?
                                        .is_some()
                                };
                                if had_prior {
                                    ChangeOp::Update
                                } else {
                                    ChangeOp::Insert
                                }
                            }
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
