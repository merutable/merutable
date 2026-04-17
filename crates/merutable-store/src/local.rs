//! `LocalFileStore`: file-system-backed object store using atomic write-then-rename.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use merutable_types::{MeruError, Result};

use crate::traits::MeruStore;

/// Object store backed by a local directory.
pub struct LocalFileStore {
    root: PathBuf,
}

impl LocalFileStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn full_path(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }
}

#[async_trait]
impl MeruStore for LocalFileStore {
    async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let full = self.full_path(path);
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // Atomic: write to tmp, fsync tmp, rename, fsync parent dir.
        //
        // Without the fsync-before-rename, rename might be applied to
        // non-durable data and a crash loses the content. Without the
        // fsync-after-rename, the directory entry itself (the link
        // from `path` to the inode) is not durable on ext4/btrfs and
        // a crash can "roll back" the rename — leaving the caller
        // believing the write succeeded when the file is gone on reboot.
        let tmp = full.with_extension("tmp");
        tokio::fs::write(&tmp, &data).await?;
        // fsync the file contents before the rename so the data is
        // durable under whatever name we link it to.
        if let Ok(f) = tokio::fs::File::open(&tmp).await {
            let _ = f.sync_all().await;
        }
        tokio::fs::rename(&tmp, &full).await?;
        // fsync the parent directory so the rename's directory-entry
        // change is durably committed.
        if let Some(parent) = full.parent() {
            if let Ok(dir) = tokio::fs::File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        }
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Bytes> {
        let full = self.full_path(path);
        let data = tokio::fs::read(&full)
            .await
            .map_err(|e| MeruError::ObjectStore(format!("{}: {e}", full.display())))?;
        Ok(Bytes::from(data))
    }

    async fn get_range(&self, path: &str, offset: usize, length: usize) -> Result<Bytes> {
        let data = self.get(path).await?;
        if offset + length > data.len() {
            return Err(MeruError::ObjectStore(format!(
                "range [{offset}, {}) exceeds file size {}",
                offset + length,
                data.len()
            )));
        }
        Ok(data.slice(offset..offset + length))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let full = self.full_path(path);
        match tokio::fs::remove_file(&full).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(MeruError::Io(e)),
        }
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let full = self.full_path(path);
        Ok(full.exists())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let dir = self.full_path(prefix);
        let mut results = Vec::new();
        if !dir.exists() {
            return Ok(results);
        }
        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if let Ok(rel) = entry.path().strip_prefix(&self.root) {
                results.push(rel.to_string_lossy().to_string());
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_get_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(tmp.path()).unwrap();

        store
            .put("data/test.dat", Bytes::from("hello"))
            .await
            .unwrap();
        let data = store.get("data/test.dat").await.unwrap();
        assert_eq!(data.as_ref(), b"hello");

        assert!(store.exists("data/test.dat").await.unwrap());
        store.delete("data/test.dat").await.unwrap();
        assert!(!store.exists("data/test.dat").await.unwrap());
    }

    #[tokio::test]
    async fn get_range() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(tmp.path()).unwrap();
        store
            .put("range.dat", Bytes::from("abcdefghij"))
            .await
            .unwrap();
        let slice = store.get_range("range.dat", 3, 4).await.unwrap();
        assert_eq!(slice.as_ref(), b"defg");
    }

    #[tokio::test]
    async fn delete_nonexistent_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(tmp.path()).unwrap();
        store.delete("does-not-exist").await.unwrap();
    }
}
