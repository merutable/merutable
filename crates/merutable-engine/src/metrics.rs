//! Metrics facade for merutable (Issue #14, Phase 1).
//!
//! The underlying crate (`metrics`) is a thin facade that routes
//! `counter!`, `gauge!`, and `histogram!` calls through a globally-
//! registered recorder. When no recorder is registered (the default),
//! the calls are effectively no-ops — a single atomic-pointer load
//! and a null check per emission point. Per-call cost is a few ns
//! and, crucially, remains OFF the hot-path paths by design: Phase 1
//! only instruments control-plane events (WAL rotations, flushes,
//! compactions, GC sweeps, stall transitions, catalog commits, errors).
//!
//! Hot-path counters (`puts_total`, `gets_total`, `scans_total`) and
//! histograms are Phase 2 / Phase 3 respectively and require
//! dedicated benchmarks per the issue's acceptance criteria.
//!
//! # Name conventions
//!
//! Snake_case, suffixed with the unit or cardinality hint
//! (`_total` for counters, `_seconds` for durations, `_bytes` for
//! sizes). Static labels only — no dynamic per-key or per-user
//! labels, as the issue's label policy mandates.
//!
//! # Why a separate module
//!
//! Centralizing the names here means:
//! 1. Renames happen in one file, not across a dozen call sites.
//! 2. The dashboard/alerting side of the contract is visible at a
//!    glance, so operators can reason about what the engine exports
//!    without reading compaction/flush/GC implementations.
//! 3. Phase 2 / 3 additions land here as constants alongside Phase 1,
//!    keeping the metric surface self-documenting.

// ── Counter names ────────────────────────────────────────────────────

// WAL
pub const WAL_APPENDS_TOTAL: &str = "merutable.wal.appends_total";
pub const WAL_SYNCS_TOTAL: &str = "merutable.wal.syncs_total";
pub const WAL_BYTES_TOTAL: &str = "merutable.wal.bytes_total";
pub const WAL_ROTATIONS_TOTAL: &str = "merutable.wal.rotations_total";
pub const WAL_FILES_GCD_TOTAL: &str = "merutable.wal.files_gcd_total";

// Flush
pub const FLUSHES_TOTAL: &str = "merutable.flush.total";
pub const FLUSHES_FAILED_TOTAL: &str = "merutable.flush.failed_total";
pub const MEMTABLE_ROTATIONS_TOTAL: &str = "merutable.memtable.rotations_total";

// Compaction
pub const COMPACTIONS_TOTAL: &str = "merutable.compaction.total";
pub const COMPACTIONS_FAILED_TOTAL: &str = "merutable.compaction.failed_total";
pub const OVERLAP_PULLINS_TOTAL: &str = "merutable.compaction.overlap_pullins_total";

// Stall / backpressure
pub const STALL_EVENTS_TOTAL: &str = "merutable.stall.hard_stops_total";
pub const SLOWDOWN_EVENTS_TOTAL: &str = "merutable.stall.slowdowns_total";

// GC
pub const GC_SWEEPS_TOTAL: &str = "merutable.gc.sweeps_total";
pub const GC_FILES_DELETED_TOTAL: &str = "merutable.gc.files_deleted_total";
pub const GC_FILES_DEFERRED_BY_PIN_TOTAL: &str = "merutable.gc.files_deferred_by_pin_total";
pub const GC_FILES_DEFERRED_BY_GRACE_TOTAL: &str = "merutable.gc.files_deferred_by_grace_total";

// Catalog
pub const SNAPSHOTS_COMMITTED_TOTAL: &str = "merutable.catalog.snapshots_committed_total";

// Errors
pub const IO_ERRORS_TOTAL: &str = "merutable.errors.io_total";
pub const CORRUPTION_DETECTED_TOTAL: &str = "merutable.errors.corruption_total";
pub const SCHEMA_MISMATCH_TOTAL: &str = "merutable.errors.schema_mismatch_total";

// ── Phase 2: hot-path counters ───────────────────────────────────────
//
// These are bumped on every user write / read. They go through the
// `metrics` crate's TLS-cached static registration, which compiles to
// a few ns of overhead when a recorder is registered and a single
// atomic-pointer load + null check when one isn't. The per-op cost is
// bounded by the macro expansion itself (not by any work in this
// module) — we deliberately avoid label allocation on the hot path
// (all labels here are compile-time `&'static str`).
//
// Write path.
pub const PUTS_TOTAL: &str = "merutable.write.puts_total";
pub const DELETES_TOTAL: &str = "merutable.write.deletes_total";
pub const PUT_BATCH_ROWS_TOTAL: &str = "merutable.write.put_batch_rows_total";
pub const PUT_BATCHES_TOTAL: &str = "merutable.write.put_batches_total";

// Read path.
pub const GETS_TOTAL: &str = "merutable.read.gets_total";
pub const GET_HITS_TOTAL: &str = "merutable.read.get_hits_total";
pub const SCANS_TOTAL: &str = "merutable.read.scans_total";
pub const SCAN_ROWS_TOTAL: &str = "merutable.read.scan_rows_total";

// Row cache (the read path sits over this — counting here lets
// operators compute hit ratio without guessing cache behavior).
pub const ROW_CACHE_HITS_TOTAL: &str = "merutable.read.row_cache_hits_total";
pub const ROW_CACHE_MISSES_TOTAL: &str = "merutable.read.row_cache_misses_total";

// ── Phase 3: histograms (sampled, not per-op) ────────────────────────
//
// Histograms are the expensive primitive. These are bumped ONCE per
// flush / compaction / commit — never per row. The issue explicitly
// forbids per-op histograms for cost reasons.
//
// All durations are in seconds (SI-friendly, matches Prometheus
// convention). Operator dashboards can convert to ms/µs as needed.
pub const FLUSH_DURATION_SECONDS: &str = "merutable.flush.duration_seconds";
pub const FLUSH_OUTPUT_BYTES: &str = "merutable.flush.output_bytes";
pub const COMPACTION_DURATION_SECONDS: &str = "merutable.compaction.duration_seconds";
pub const COMPACTION_OUTPUT_BYTES: &str = "merutable.compaction.output_bytes";
pub const COMMIT_DURATION_SECONDS: &str = "merutable.catalog.commit_duration_seconds";

/// Phase 3: record a histogram sample. `value` is the observation
/// (typically seconds for durations, bytes for sizes). Cheap when no
/// recorder is registered — same TLS-cached null-check path as
/// counters.
#[inline]
pub fn record(name: &'static str, value: f64) {
    metrics::histogram!(name).record(value);
}

/// Phase 3: record a histogram sample with a single static label.
/// Used for per-output-level compaction histograms — bounded
/// cardinality (levels 1..5) so no label explosion risk.
#[inline]
pub fn record_labeled(
    name: &'static str,
    label_key: &'static str,
    label_value: String,
    value: f64,
) {
    metrics::histogram!(name, label_key => label_value).record(value);
}

// ── Phase-1 helper ──────────────────────────────────────────────────

/// Increment a counter by 1. Cheaper than constructing a `Counter`
/// handle and calling `.increment(1)` directly — lets call sites
/// read as single expressions.
#[inline]
pub fn inc(name: &'static str) {
    metrics::counter!(name).increment(1);
}

/// Increment a counter by `n`.
#[inline]
pub fn inc_by(name: &'static str, n: u64) {
    metrics::counter!(name).increment(n);
}

/// Increment a counter with a single static label. Common shape for
/// per-level counters (e.g., compactions by input_level).
#[inline]
pub fn inc_labeled(name: &'static str, label_key: &'static str, label_value: String) {
    metrics::counter!(name, label_key => label_value).increment(1);
}
