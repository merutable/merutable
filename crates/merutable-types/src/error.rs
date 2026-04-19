use thiserror::Error;

#[derive(Error, Debug)]
pub enum MeruError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("key not found")]
    NotFound,

    #[error("object store error: {0}")]
    ObjectStore(String),

    #[error("parquet error: {0}")]
    Parquet(String),

    #[error("iceberg error: {0}")]
    Iceberg(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("compaction error: {0}")]
    Compaction(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("operation not permitted: database is read-only")]
    ReadOnly,

    #[error("database is closed")]
    Closed,

    /// Issue #26: create-only PUT lost a race.
    ///
    /// Returned by `MeruStore::put_if_absent` when the target path
    /// already exists. Callers handle this by refetching HEAD,
    /// rebuilding their manifest on top, and retrying. Not an error
    /// condition — the *expected* non-error outcome of losing a race.
    #[error("object already exists: {0}")]
    AlreadyExists(String),
}

pub type Result<T> = std::result::Result<T, MeruError>;
