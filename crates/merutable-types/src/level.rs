use serde::{Deserialize, Serialize};

/// LSM level identifier. L0 = freshly flushed; higher = more compacted.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Level(pub u8);

/// Level 0: freshly flushed Parquet files. Files here may overlap in key range.
pub const L0: Level = Level(0);

impl Level {
    pub fn next(self) -> Self {
        Level(self.0 + 1)
    }
}

impl std::fmt::Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "L{}", self.0)
    }
}

/// Metadata embedded in every Parquet file's KV footer.
/// Key: `"merutable.meta"` (see [`ParquetFileMeta::FOOTER_KEY`]).
/// Serialized as JSON via serde.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParquetFileMeta {
    pub level: Level,
    /// Minimum sequence number of any row in this file.
    pub seq_min: u64,
    /// Maximum sequence number of any row in this file.
    pub seq_max: u64,
    /// PK-encoded user key bytes (without the InternalKey tag) of the
    /// smallest user key in this file. Used for file-level range gating.
    #[serde(with = "hex_bytes")]
    pub key_min: Vec<u8>,
    /// PK-encoded user key bytes (without the InternalKey tag) of the
    /// largest user key in this file.
    #[serde(with = "hex_bytes")]
    pub key_max: Vec<u8>,
    pub num_rows: u64,
    pub file_size: u64,
    /// Object-store path of the associated `.puffin` Deletion Vector file, if any.
    pub dv_path: Option<String>,
    pub dv_offset: Option<i64>,
    pub dv_length: Option<i64>,
    /// Issue #15: per-file physical format stamp. Legacy files
    /// written before the stamp existed omit this field —
    /// deserialization produces `None`, and readers must fall back
    /// to `FileFormat::default_for_level(level)` which matches the
    /// pre-Issue-#15 hard-coded behavior (Dual iff L0).
    #[serde(default)]
    pub format: Option<FileFormat>,
}

/// Physical layout stamp for a Parquet SSTable (Issue #15).
///
/// The cutoff between "blob fast path" and "typed columns only" is
/// now operator-controlled via `EngineConfig::dual_format_max_level`
/// rather than hard-coded to `level == 0`. The format is stamped
/// per file at write time and preserved forever; config changes
/// affect NEW compactions only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    /// Typed columns only. No `_merutable_value` blob. Smaller on
    /// disk at the cost of a reconstruct-from-typed-columns step
    /// on point lookup.
    Columnar,
    /// Typed columns + `_merutable_value` postcard blob. Fast-path
    /// for point lookups at hot tiers.
    Dual,
}

impl FileFormat {
    /// Infer the default format for a file whose manifest entry
    /// lacks the `format` stamp (legacy / pre-Issue-#15 files).
    /// Matches the pre-fix behavior: L0 = Dual, L1+ = Columnar.
    #[inline]
    pub fn default_for_level(level: Level) -> Self {
        if level.0 == 0 {
            FileFormat::Dual
        } else {
            FileFormat::Columnar
        }
    }

    /// Whether this format carries the `_merutable_value` blob
    /// column. Point-lookup strategy switches on this.
    #[inline]
    pub fn has_value_blob(self) -> bool {
        matches!(self, FileFormat::Dual)
    }
}

impl ParquetFileMeta {
    pub const FOOTER_KEY: &'static str = "merutable.meta";
    pub const SCHEMA_KEY: &'static str = "merutable.schema";

    pub fn serialize(&self) -> crate::Result<String> {
        serde_json::to_string(self).map_err(|e| crate::MeruError::Corruption(e.to_string()))
    }

    pub fn deserialize(s: &str) -> crate::Result<Self> {
        serde_json::from_str(s).map_err(|e| crate::MeruError::Corruption(e.to_string()))
    }
}

/// Module-level constant for the footer key (convenience re-export).
pub const FOOTER_KEY: &str = ParquetFileMeta::FOOTER_KEY;
/// Module-level constant for the schema key (convenience re-export).
pub const SCHEMA_KEY: &str = ParquetFileMeta::SCHEMA_KEY;

/// Serde helper: serialize `Vec<u8>` as hex string for human-readable JSON.
mod hex_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(de)?;
        hex::decode(&s).map_err(serde::de::Error::custom)
    }
}
