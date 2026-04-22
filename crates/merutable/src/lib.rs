#![deny(clippy::all)]

// Issue #38: workspace collapsed into a single crate. Internal
// modules below are `pub mod` so legacy callers (and PyO3
// bindings in `merutable-python`) keep their existing import
// paths working while the public re-exports settle into a
// stable surface. A future pass tightens visibility to
// `pub(crate)` for items not part of the supported API.
pub mod engine;
pub mod iceberg;
pub mod memtable;
pub mod parquet;
pub mod store;
pub mod types;
pub mod wal;

#[cfg(feature = "replica")]
pub mod replica;
#[cfg(feature = "sql")]
pub mod sql;

pub mod db;
pub mod error;
pub mod iterator;
pub mod mirror;
pub mod options;

pub use db::MeruDB;
pub use error::Error;
pub use options::{MirrorConfig, OpenOptions};

// Re-export core types so users don't need to dive into the
// internal `types` module.
pub use crate::types::schema;
pub use crate::types::value;

// Re-export stats types for introspection.
pub use crate::engine::{CacheStats, EngineStats, FileStats, LevelStats, MemtableStats};
