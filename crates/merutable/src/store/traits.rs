//! `MeruStore` trait: abstraction over object stores (local, S3, etc.).
//!
//! The engine writes Parquet files and Puffin DVs through this trait.
//! WAL always uses direct filesystem I/O — it does not go through MeruStore.

use crate::types::Result;
use async_trait::async_trait;
use bytes::Bytes;

/// Unified object-store interface for merutable data files.
#[async_trait]
pub trait MeruStore: Send + Sync + 'static {
    /// Write `data` atomically to `path`. If the path already exists, overwrite.
    async fn put(&self, path: &str, data: Bytes) -> Result<()>;

    /// Read the entire object at `path`.
    async fn get(&self, path: &str) -> Result<Bytes>;

    /// Read a byte range `[offset, offset+length)` from the object at `path`.
    async fn get_range(&self, path: &str, offset: usize, length: usize) -> Result<Bytes>;

    /// Delete the object at `path`. No error if it doesn't exist.
    async fn delete(&self, path: &str) -> Result<()>;

    /// Check whether the object exists.
    async fn exists(&self, path: &str) -> Result<bool>;

    /// List all objects with the given prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Issue #26: atomic create-only write.
    ///
    /// Returns `Ok(())` if this call created the object. Returns
    /// `Err(MeruError::AlreadyExists(path))` if the object already
    /// exists (losing a race is the expected non-error outcome —
    /// callers retry by refetching HEAD and rebuilding on top).
    ///
    /// Implementation contract per backend:
    /// - POSIX: `O_CREAT | O_EXCL` open. Returns AlreadyExists on
    ///   `EEXIST`.
    /// - S3: `PutObject` with `If-None-Match: *` (Nov 2024).
    /// - GCS: upload with `x-goog-if-generation-match: 0`.
    /// - Azure Blob: PUT with `If-None-Match: *`.
    ///
    /// Default impl delegates to `put` after an `exists` check so
    /// existing `MeruStore` implementations compile. The default is
    /// RACY and MUST be overridden by any backend that commits
    /// metadata in `CommitMode::ObjectStore` mode. Override with the
    /// backend-native conditional primitive.
    async fn put_if_absent(&self, path: &str, data: Bytes) -> Result<()> {
        if self.exists(path).await? {
            return Err(crate::types::MeruError::AlreadyExists(path.into()));
        }
        self.put(path, data).await
    }
}
