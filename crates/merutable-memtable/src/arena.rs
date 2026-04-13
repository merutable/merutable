//! Arena allocator backed by `bumpalo::Bump`.
//!
//! Used for arena-allocating raw bytes within a memtable's lifetime.
//! The arena is tied to one `Memtable` instance and is dropped when
//! the memtable is flushed and released.
//!
//! # Note on `alloc_bytes` removal
//!
//! A previous `alloc_bytes(&self, n: usize) -> &mut [u8]` method was unsound:
//! it acquired the inner `Mutex`, allocated from the `Bump`, dropped the
//! `MutexGuard`, then returned a `&mut [u8]` with the lifetime of `&self` via
//! `unsafe` pointer cast. Two concurrent calls could produce overlapping
//! `&mut [u8]` slices, violating Rust's aliasing rules. The method had zero
//! callers in the codebase (the skip-list hot path uses `bytes::Bytes`), so it
//! was removed rather than made safe.
//!
//! If arena-backed allocation is needed in the future, the safe approach is to
//! return an `(offset, len)` pair and provide a separate shared accessor, or to
//! require `&mut self` so the borrow checker enforces exclusivity.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Thread-safe arena allocator. `bumpalo::Bump` itself is not `Sync`, so we
/// wrap it in a `Mutex` for the (rare) concurrent alloc case. The hot path
/// (skip list inserts) does not use the arena directly — values are stored
/// as `bytes::Bytes` (reference-counted). The arena is here for future use
/// (e.g., arena-backed key storage to reduce allocator pressure).
pub struct Arena {
    #[allow(dead_code)]
    inner: std::sync::Mutex<bumpalo::Bump>,
    allocated: AtomicUsize,
}

impl Arena {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(bumpalo::Bump::new()),
            allocated: AtomicUsize::new(0),
        }
    }

    /// Total bytes allocated through this arena.
    pub fn allocated_bytes(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}
