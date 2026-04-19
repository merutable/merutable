//! Issue #29 Phase 1: API-shape freeze for the change-feed surface.
//!
//! Real behavior lands in Phase 2 (engine-side merge iterator +
//! DataFusion TableProvider). Phase 1 locks the types and the
//! retention-bound error so downstream consumers can build escalation
//! paths ahead of time.

use merutable_sql::{ChangeFeedCursor, ChangeOp};
use merutable_types::MeruError;

#[test]
fn change_op_sql_labels_are_stable() {
    // These strings are part of the SQL contract. Renaming any of
    // them breaks every downstream WHERE-clause filter.
    assert_eq!(ChangeOp::Insert.as_sql_str(), "INSERT");
    assert_eq!(ChangeOp::Update.as_sql_str(), "UPDATE");
    assert_eq!(ChangeOp::Delete.as_sql_str(), "DELETE");
}

#[test]
fn phase1_cursor_returns_retention_error() {
    // Phase 2a renamed `new` → `new_below_retention` to make the
    // intent explicit now that an engine-backed constructor exists.
    let mut cur = ChangeFeedCursor::new_below_retention(100, 500);
    let err = match cur.next_batch(10) {
        Ok(_) => panic!("Phase 1 must return retention error, not rows"),
        Err(e) => e,
    };
    match err {
        MeruError::ChangeFeedBelowRetention {
            requested,
            low_water,
        } => {
            assert_eq!(requested, 100);
            assert_eq!(low_water, 500);
        }
        other => panic!("expected ChangeFeedBelowRetention, got {other:?}"),
    }
}

#[test]
fn retention_error_display_includes_escalation_hint() {
    let err = MeruError::ChangeFeedBelowRetention {
        requested: 42,
        low_water: 100,
    };
    let msg = format!("{err}");
    assert!(msg.contains("42"));
    assert!(msg.contains("100"));
    assert!(
        msg.contains("Iceberg"),
        "error message should hint at escalation path: {msg}"
    );
}
