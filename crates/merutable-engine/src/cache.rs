//! Row-level LRU buffer cache for the read path.
//!
//! Sits between the memtable check and Parquet file I/O in `point_lookup()`.
//! Cache entries are invalidated on every write (put/delete) so the cache
//! never serves stale data after a key leaves the memtable.
//!
//! Thread-safe: wraps `lru::LruCache` in a `std::sync::Mutex`. The lock is
//! held only for the duration of a hash-map get/put — no I/O under the lock.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use lru::LruCache;
use merutable_types::{sequence::OpType, value::Row};

/// A cached read result from a Parquet file lookup.
#[derive(Clone, Debug)]
pub struct CacheEntry {
    pub op_type: OpType,
    pub row: Row,
}

/// Row-level LRU cache. Thread-safe.
pub struct RowCache {
    inner: Mutex<LruCache<Vec<u8>, CacheEntry>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl RowCache {
    /// Create a new cache with the given capacity.
    /// Panics if `capacity` is 0 — use `Option<RowCache>` to represent "no cache".
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).expect("row cache capacity must be > 0"),
            )),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Look up a key. Returns `Some(entry)` on hit, `None` on miss.
    /// Promotes the entry to most-recently-used on hit.
    pub fn get(&self, user_key: &[u8]) -> Option<CacheEntry> {
        let mut guard = self.inner.lock().unwrap();
        match guard.get(user_key) {
            Some(entry) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert or update a cache entry.
    pub fn insert(&self, user_key: Vec<u8>, entry: CacheEntry) {
        let mut guard = self.inner.lock().unwrap();
        guard.put(user_key, entry);
    }

    /// Invalidate a single key. Called on every write (put/delete).
    pub fn invalidate(&self, user_key: &[u8]) {
        let mut guard = self.inner.lock().unwrap();
        guard.pop(user_key);
    }

    /// Current number of cached entries.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Returns `true` if the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Cache capacity.
    pub fn cap(&self) -> usize {
        self.inner.lock().unwrap().cap().into()
    }

    /// Cumulative hit count.
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Cumulative miss count.
    pub fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use merutable_types::value::FieldValue;

    fn make_entry(val: i64) -> CacheEntry {
        CacheEntry {
            op_type: OpType::Put,
            row: Row::new(vec![Some(FieldValue::Int64(val))]),
        }
    }

    #[test]
    fn insert_and_get() {
        let cache = RowCache::new(10);
        cache.insert(b"key1".to_vec(), make_entry(1));
        let hit = cache.get(b"key1");
        assert!(hit.is_some());
        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.miss_count(), 0);
    }

    #[test]
    fn miss_returns_none() {
        let cache = RowCache::new(10);
        assert!(cache.get(b"missing").is_none());
        assert_eq!(cache.miss_count(), 1);
    }

    #[test]
    fn invalidate_removes_entry() {
        let cache = RowCache::new(10);
        cache.insert(b"key1".to_vec(), make_entry(1));
        cache.invalidate(b"key1");
        assert!(cache.get(b"key1").is_none());
    }

    #[test]
    fn lru_eviction() {
        let cache = RowCache::new(2);
        cache.insert(b"a".to_vec(), make_entry(1));
        cache.insert(b"b".to_vec(), make_entry(2));
        cache.insert(b"c".to_vec(), make_entry(3)); // evicts "a"
        assert!(cache.get(b"a").is_none());
        assert!(cache.get(b"b").is_some());
        assert!(cache.get(b"c").is_some());
    }

    #[test]
    fn len_and_cap() {
        let cache = RowCache::new(5);
        assert_eq!(cache.cap(), 5);
        assert_eq!(cache.len(), 0);
        cache.insert(b"x".to_vec(), make_entry(1));
        assert_eq!(cache.len(), 1);
    }
}
