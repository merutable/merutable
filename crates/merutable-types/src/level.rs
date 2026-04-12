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
