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
    /// Issue #20 Part 2b: per-column statistics hoisted from the
    /// Parquet row-group metadata at write time. Indexed by Iceberg
    /// field id (1..=N, matching `TableSchema::columns`). Projected
    /// into the exported Iceberg `metadata.json` as `lower_bounds`,
    /// `upper_bounds`, `column_sizes`, `value_counts`, and
    /// `null_value_counts`, enabling file pruning and min/max
    /// predicate pushdown for external readers.
    ///
    /// Legacy files (pre-#20 Part 2b) deserialize with `None` —
    /// the Iceberg projection then falls back to empty stat maps,
    /// which is spec-valid.
    #[serde(default)]
    pub column_stats: Option<Vec<ColumnStats>>,
}

/// Per-column statistics for one Parquet file, keyed by `field_id`
/// (1-based Iceberg field id matching `TableSchema::columns[field_id-1]`).
///
/// Emitted by the Parquet writer after finishing the file: the
/// writer reads its own output's row-group metadata and reduces
/// (sum/min/max) per column across row groups. The reduction is
/// bounded by the number of row groups per file (a few dozen at
/// most) so the write-time cost is negligible.
///
/// All bounds fields store raw bytes in Iceberg's single-value
/// serialization format (matching Apache Iceberg v2 spec). Ints are
/// little-endian; booleans are a single byte; floats/doubles are
/// IEEE-754 little-endian; binary is the raw bytes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Iceberg field id. 1-based; matches `TableSchema::columns[id-1]`.
    pub field_id: i32,
    /// Sum of compressed byte sizes for this column across every
    /// row group. Projects to Iceberg `column_sizes[field_id]`.
    pub compressed_bytes: u64,
    /// Row count with a non-null value in this column. Projects to
    /// Iceberg `value_counts[field_id]`.
    pub value_count: u64,
    /// Row count with a NULL value in this column. Projects to
    /// Iceberg `null_value_counts[field_id]`.
    pub null_count: u64,
    /// Serialized min value (Iceberg single-value bytes). Absent
    /// when the Parquet writer did not emit statistics for this
    /// column (e.g., unsupported type, or all-null column).
    /// Projects to Iceberg `lower_bounds[field_id]`.
    #[serde(with = "hex_bytes_opt", default)]
    pub lower_bound: Option<Vec<u8>>,
    /// Serialized max value. Projects to `upper_bounds[field_id]`.
    #[serde(with = "hex_bytes_opt", default)]
    pub upper_bound: Option<Vec<u8>>,
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

    pub fn serialize(&self) -> crate::types::Result<String> {
        serde_json::to_string(self).map_err(|e| crate::types::MeruError::Corruption(e.to_string()))
    }

    pub fn deserialize(s: &str) -> crate::types::Result<Self> {
        serde_json::from_str(s).map_err(|e| crate::types::MeruError::Corruption(e.to_string()))
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

/// Issue #20 Part 2b: hex-encoded `Option<Vec<u8>>` for per-column
/// bound bytes that may be absent (no Parquet statistics emitted for
/// that column, or all-null column).
mod hex_bytes_opt {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Option<Vec<u8>>, ser: S) -> Result<S::Ok, S::Error> {
        match bytes {
            Some(b) => ser.serialize_str(&hex::encode(b)),
            None => ser.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Option<Vec<u8>>, D::Error> {
        let opt = Option::<String>::deserialize(de)?;
        match opt {
            Some(s) => hex::decode(&s).map(Some).map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}
