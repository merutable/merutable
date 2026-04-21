use std::sync::atomic::{AtomicU64, Ordering};

/// Monotone sequence number. Top 56 bits = counter; low 8 bits unused (reserved for tag
/// in the internal key wire format). Never constructed from a raw u64 outside this module.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeqNum(pub u64);

/// Operation type tag — 1 byte, OR'd into the low 8 bits of the internal key tag word.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpType {
    Delete = 0x00,
    Put = 0x01,
}

/// Maximum sequence number expressible in 56 bits.
/// Used as the seek-sentinel: encoding SEQNUM_MAX produces the lexicographically smallest
/// tag for a given PK, so a seek lands before all real entries for that PK.
pub const SEQNUM_MAX: SeqNum = SeqNum((1u64 << 56) - 1);

impl SeqNum {
    #[inline]
    pub fn next(self) -> Self {
        SeqNum(self.0 + 1)
    }
}

impl std::fmt::Display for SeqNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Atomic sequence counter. Owned by `MeruEngine`; passed by `Arc` reference to writers.
pub struct GlobalSeq(AtomicU64);

impl GlobalSeq {
    pub fn new(initial: u64) -> Self {
        Self(AtomicU64::new(initial))
    }

    /// Snapshot of the current sequence for read-path use.
    #[inline]
    pub fn current(&self) -> SeqNum {
        SeqNum(self.0.load(Ordering::Acquire))
    }

    /// Atomically fetch-and-increment. Returns the allocated sequence number.
    #[inline]
    pub fn allocate(&self) -> SeqNum {
        SeqNum(self.0.fetch_add(1, Ordering::AcqRel))
    }

    /// Atomically reserve `n` consecutive sequence numbers. Returns the
    /// **first** seq in the reserved range; the caller owns `[base, base+n)`.
    /// Required for multi-record batch writes: the memtable's `apply_batch`
    /// consumes one seq per record, so the global counter must be bumped by
    /// the full record count — not by 1, or else the next allocation collides
    /// with a record already written by the batch and crossbeam_skiplist's
    /// `insert` silently overwrites one of the two entries.
    #[inline]
    pub fn allocate_n(&self, n: u64) -> SeqNum {
        SeqNum(self.0.fetch_add(n, Ordering::AcqRel))
    }

    /// Overwrite (called during WAL recovery to advance past recovered max seq).
    pub fn set_at_least(&self, val: u64) {
        // CAS loop to set only if val > current, avoiding races with in-flight writes.
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            if val <= current {
                break;
            }
            match self
                .0
                .compare_exchange_weak(current, val, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(now) => current = now,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn seqnum_next() {
        let s = SeqNum(5);
        assert_eq!(s.next(), SeqNum(6));
    }

    #[test]
    fn seqnum_ordering() {
        assert!(SeqNum(1) < SeqNum(2));
        assert!(SeqNum(100) > SeqNum(99));
        assert_eq!(SeqNum(42), SeqNum(42));
    }

    #[test]
    fn seqnum_display() {
        assert_eq!(format!("{}", SeqNum(42)), "42");
    }

    #[test]
    fn seqnum_max_is_56_bits() {
        assert_eq!(SEQNUM_MAX.0, (1u64 << 56) - 1);
    }

    #[test]
    fn global_seq_allocate_monotone() {
        let gs = GlobalSeq::new(1);
        let s1 = gs.allocate();
        let s2 = gs.allocate();
        let s3 = gs.allocate();
        assert_eq!(s1, SeqNum(1));
        assert_eq!(s2, SeqNum(2));
        assert_eq!(s3, SeqNum(3));
        assert_eq!(gs.current(), SeqNum(4));
    }

    #[test]
    fn global_seq_allocate_n_reserves_contiguous_range() {
        let gs = GlobalSeq::new(10);
        let base = gs.allocate_n(4);
        // Caller owns [base, base+4) = [10, 11, 12, 13].
        assert_eq!(base, SeqNum(10));
        // Next single allocate must skip over the reserved range.
        assert_eq!(gs.allocate(), SeqNum(14));
        // And a further batch must start where the single left off.
        assert_eq!(gs.allocate_n(2), SeqNum(15));
        assert_eq!(gs.current(), SeqNum(17));
    }

    #[test]
    fn global_seq_allocate_n_zero_is_noop() {
        let gs = GlobalSeq::new(5);
        let base = gs.allocate_n(0);
        assert_eq!(base, SeqNum(5));
        assert_eq!(gs.current(), SeqNum(5));
    }

    #[test]
    fn global_seq_set_at_least() {
        let gs = GlobalSeq::new(5);
        gs.set_at_least(10);
        assert_eq!(gs.current(), SeqNum(10));

        // Should not go backward.
        gs.set_at_least(3);
        assert_eq!(gs.current(), SeqNum(10));
    }

    #[test]
    fn global_seq_concurrent_allocate() {
        let gs = std::sync::Arc::new(GlobalSeq::new(1));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let gs = gs.clone();
            handles.push(thread::spawn(move || {
                let mut seqs = Vec::new();
                for _ in 0..1000 {
                    seqs.push(gs.allocate().0);
                }
                seqs
            }));
        }

        let mut all_seqs: Vec<u64> = Vec::new();
        for h in handles {
            all_seqs.extend(h.join().unwrap());
        }

        // All 8000 allocations should be unique.
        all_seqs.sort();
        all_seqs.dedup();
        assert_eq!(all_seqs.len(), 8000);

        // They should span [1, 8000].
        assert_eq!(*all_seqs.first().unwrap(), 1);
        assert_eq!(*all_seqs.last().unwrap(), 8000);
    }

    #[test]
    fn optype_values() {
        assert_eq!(OpType::Delete as u8, 0x00);
        assert_eq!(OpType::Put as u8, 0x01);
        // Delete < Put in numeric value — used in tag encoding.
        assert!((OpType::Delete as u8) < (OpType::Put as u8));
    }
}
