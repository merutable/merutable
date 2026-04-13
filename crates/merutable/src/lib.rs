#![deny(clippy::all)]

pub mod db;
pub mod error;
pub mod iterator;
pub mod options;

pub use db::MeruDB;
pub use error::Error;
pub use options::OpenOptions;

// Re-export core types so users don't need to depend on merutable-types directly.
pub use merutable_types::schema;
pub use merutable_types::value;

// Re-export stats types for introspection.
pub use merutable_engine::{CacheStats, EngineStats, FileStats, LevelStats, MemtableStats};
