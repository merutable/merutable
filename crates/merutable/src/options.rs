use merutable_types::schema::TableSchema;
use std::path::PathBuf;

/// Builder for opening a `MeruDB` instance.
pub struct OpenOptions {
    pub schema: TableSchema,
    pub catalog_uri: String,
    pub object_store_url: String,
    pub wal_dir: PathBuf,
    pub memtable_size_mb: usize,
    pub row_cache_capacity: usize,
    pub read_only: bool,
}

impl OpenOptions {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            catalog_uri: String::new(),
            object_store_url: String::new(),
            wal_dir: PathBuf::from("./meru-wal"),
            memtable_size_mb: 64,
            row_cache_capacity: 10_000,
            read_only: false,
        }
    }

    pub fn catalog_uri(mut self, uri: impl Into<String>) -> Self {
        self.catalog_uri = uri.into();
        self
    }

    pub fn object_store(mut self, url: impl Into<String>) -> Self {
        self.object_store_url = url.into();
        self
    }

    pub fn wal_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.wal_dir = dir.into();
        self
    }

    pub fn memtable_size_mb(mut self, mb: usize) -> Self {
        self.memtable_size_mb = mb;
        self
    }

    pub fn row_cache_capacity(mut self, capacity: usize) -> Self {
        self.row_cache_capacity = capacity;
        self
    }

    pub fn read_only(mut self, enabled: bool) -> Self {
        self.read_only = enabled;
        self
    }
}
