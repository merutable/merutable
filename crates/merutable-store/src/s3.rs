//! `S3Store`: object-store wrapper for Amazon S3 (via `object_store` crate).
//!
//! Delegates to `object_store::aws::AmazonS3` with multipart upload for
//! large files. Configuration via environment variables or explicit builder.

use async_trait::async_trait;
use bytes::Bytes;
use merutable_types::{MeruError, Result};

use crate::traits::MeruStore;

/// S3-backed object store. Wraps the `object_store` crate's S3 client.
pub struct S3Store {
    inner: object_store::aws::AmazonS3,
    prefix: String,
}

impl S3Store {
    /// Create from an already-configured `AmazonS3` client.
    pub fn new(inner: object_store::aws::AmazonS3, prefix: String) -> Self {
        Self { inner, prefix }
    }

    fn object_path(&self, path: &str) -> object_store::path::Path {
        let full = if self.prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), path)
        };
        object_store::path::Path::from(full)
    }
}

#[async_trait]
impl MeruStore for S3Store {
    async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        use object_store::ObjectStore;
        let loc = self.object_path(path);
        self.inner
            .put(&loc, data.into())
            .await
            .map_err(|e| MeruError::ObjectStore(e.to_string()))?;
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Bytes> {
        use object_store::ObjectStore;
        let loc = self.object_path(path);
        let result = self
            .inner
            .get(&loc)
            .await
            .map_err(|e| MeruError::ObjectStore(e.to_string()))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| MeruError::ObjectStore(e.to_string()))?;
        Ok(bytes)
    }

    async fn get_range(&self, path: &str, offset: usize, length: usize) -> Result<Bytes> {
        use object_store::ObjectStore;
        let loc = self.object_path(path);
        let range = offset..offset + length;
        let bytes = self
            .inner
            .get_range(&loc, range)
            .await
            .map_err(|e| MeruError::ObjectStore(e.to_string()))?;
        Ok(bytes)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        use object_store::ObjectStore;
        let loc = self.object_path(path);
        self.inner
            .delete(&loc)
            .await
            .map_err(|e| MeruError::ObjectStore(e.to_string()))?;
        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        use object_store::ObjectStore;
        let loc = self.object_path(path);
        match self.inner.head(&loc).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(MeruError::ObjectStore(e.to_string())),
        }
    }

    /// Issue #26: S3-native conditional-PUT via `If-None-Match: *`
    /// (GA November 2024). Uses `object_store`'s `PutMode::Create`,
    /// which translates to the `If-None-Match: *` header and makes S3
    /// reject the write with `412 Precondition Failed` when the key
    /// already exists. That rejection surfaces here as
    /// `object_store::Error::AlreadyExists` → mapped to our
    /// `MeruError::AlreadyExists`.
    ///
    /// This override is mandatory for `CommitMode::ObjectStore` — the
    /// racy `exists + put` default would let two concurrent writers
    /// both succeed at committing the same version, corrupting the
    /// backward-pointer chain.
    async fn put_if_absent(&self, path: &str, data: Bytes) -> Result<()> {
        use object_store::{ObjectStore, PutMode, PutOptions};
        let loc = self.object_path(path);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.inner.put_opts(&loc, data.into(), opts).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                Err(MeruError::AlreadyExists(path.into()))
            }
            // S3's If-None-Match failure also arrives as Precondition.
            // Treat it identically — a concurrent writer won.
            Err(object_store::Error::Precondition { .. }) => {
                Err(MeruError::AlreadyExists(path.into()))
            }
            Err(e) => Err(MeruError::ObjectStore(e.to_string())),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        use futures::TryStreamExt;
        use object_store::ObjectStore;
        let loc = self.object_path(prefix);
        let stream = self.inner.list(Some(&loc));
        let objects: Vec<object_store::ObjectMeta> = stream
            .try_collect()
            .await
            .map_err(|e| MeruError::ObjectStore(e.to_string()))?;
        Ok(objects.iter().map(|o| o.location.to_string()).collect())
    }
}
