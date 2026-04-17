//! Leveled compaction picker.
//!
//! Scores each level and picks the one with the highest compaction pressure.
//!
//! - L0 score = `l0_file_count / l0_compaction_trigger`
//! - L1+ score = `level_bytes / level_target_bytes[level]`
//! - Pick highest score > 1.0; output level = input + 1.

use merutable_iceberg::version::Version;
use merutable_types::level::Level;

use crate::config::EngineConfig;

/// Result of compaction picking: which level to compact from, to which level.
#[derive(Debug, Clone)]
pub struct CompactionPick {
    /// Input level (where we read files from).
    pub input_level: Level,
    /// Output level (where we write compacted files).
    pub output_level: Level,
    /// Score that triggered this pick.
    pub score: f64,
    /// Paths of input files to compact.
    pub input_files: Vec<String>,
}

/// Pick the best compaction, or `None` if no level needs compaction.
///
/// L0 priority: when `l0_file_count >= l0_slowdown_trigger`, L0 is picked
/// unconditionally regardless of deeper-level scores. L0 buildup degrades
/// both read latency (more files to probe) and write latency (slowdown
/// sleep + eventual stall). A deep compaction that outpaces flushes will
/// otherwise let L0 accumulate indefinitely — even if its score is 18×
/// the trigger — because `best_score` was previously a single max across
/// all levels. This fix lets the caller (run_compaction loop) keep
/// draining L0 between deep compactions.
pub fn pick_compaction(version: &Version, config: &EngineConfig) -> Option<CompactionPick> {
    let l0_count = version.l0_file_count();
    let l0_score = l0_count as f64 / config.l0_compaction_trigger as f64;

    // L0 priority path.
    let force_l0 = l0_count >= config.l0_slowdown_trigger;

    let mut best_score = 0.0f64;
    let mut best_level = Level(0);

    if force_l0 {
        // L0 is above slowdown — compact it regardless of deeper pressure.
        best_score = l0_score;
        best_level = Level(0);
    } else {
        // Normal scoring: pick the highest-pressure level.
        if l0_score > best_score {
            best_score = l0_score;
            best_level = Level(0);
        }
        for (i, &target) in config.level_target_bytes.iter().enumerate() {
            let level = Level((i + 1) as u8);
            let bytes = version.level_bytes(level);
            if target > 0 {
                let score = bytes as f64 / target as f64;
                if score > best_score {
                    best_score = score;
                    best_level = level;
                }
            }
        }

        if best_score < 1.0 {
            return None; // no level needs compaction
        }
    }

    let output_level = best_level.next();

    // Select input files.
    let input_files: Vec<String> = if best_level == Level(0) {
        // For L0, compact ALL L0 files (they can overlap).
        version
            .files_at(Level(0))
            .iter()
            .map(|f| f.path.clone())
            .collect()
    } else {
        // For L1+, pick files that overlap with the output level's boundary.
        // Simplified: pick all files at this level (full-level compaction).
        // A real implementation would be more selective.
        version
            .files_at(best_level)
            .iter()
            .map(|f| f.path.clone())
            .collect()
    };

    if input_files.is_empty() {
        return None;
    }

    Some(CompactionPick {
        input_level: best_level,
        output_level,
        score: best_score,
        input_files,
    })
}

/// Whether tombstones should be dropped when compacting to `output_level`.
///
/// Safe only when writing to the **true bottom level** — the level beyond
/// all configured `level_target_bytes` entries, which the picker never
/// triggers compaction from (so data accumulates there permanently).
///
/// With default config `level_target_bytes = [L1, L2, L3, L4]` (len=4),
/// the bottom level is L5, so `should_drop_tombstones(Level(5), 4) = true`.
///
/// Bug I fixed an off-by-one (`>=` → `>`) that dropped tombstones one
/// level too early, allowing deleted data to resurrect via a subsequent
/// compaction to the real bottom level.
#[inline]
pub fn should_drop_tombstones(output_level: Level, num_configured_levels: usize) -> bool {
    output_level.0 as usize > num_configured_levels
}

#[cfg(test)]
mod tests {
    use super::*;
    use merutable_iceberg::version::DataFileMeta;
    use merutable_types::{
        level::{Level, ParquetFileMeta},
        schema::{ColumnDef, ColumnType, TableSchema},
    };
    use std::sync::Arc;

    fn test_schema() -> Arc<TableSchema> {
        Arc::new(TableSchema {
            table_name: "test".into(),
            columns: vec![ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
            }],
            primary_key: vec![0],
        })
    }

    fn make_file(path: &str, level: u8, num_rows: u64, file_size: u64) -> DataFileMeta {
        DataFileMeta {
            path: path.to_string(),
            meta: ParquetFileMeta {
                level: Level(level),
                seq_min: 1,
                seq_max: 10,
                key_min: vec![0x01],
                key_max: vec![0xFF],
                num_rows,
                file_size,
                dv_path: None,
                dv_offset: None,
                dv_length: None,
            },
            dv_path: None,
            dv_offset: None,
            dv_length: None,
        }
    }

    #[test]
    fn no_compaction_needed() {
        let v = Version::empty(test_schema());
        let config = EngineConfig::default();
        assert!(pick_compaction(&v, &config).is_none());
    }

    #[test]
    fn l0_compaction_trigger() {
        let mut v = Version::empty(test_schema());
        // Insert enough L0 files to exceed l0_compaction_trigger (default: 4).
        let mut l0_files = Vec::new();
        for i in 0..5 {
            l0_files.push(make_file(&format!("l0_{i}.parquet"), 0, 100, 1024));
        }
        v.levels.insert(Level(0), l0_files);

        let config = EngineConfig::default();
        let pick = pick_compaction(&v, &config).unwrap();
        assert_eq!(pick.input_level, Level(0));
        assert_eq!(pick.output_level, Level(1));
        assert_eq!(pick.input_files.len(), 5);
    }

    /// L0 priority: when L0 is above `l0_slowdown_trigger`, it must be
    /// picked even if a deeper level has a numerically higher score.
    /// Regression: previously a single max-score pass would pick e.g. L2
    /// with score=2.0 over L0 with score=5.0 (because of how score math
    /// can compare), letting L0 grow unbounded while deep compactions
    /// churned. Now L0 wins unconditionally above slowdown.
    #[test]
    fn l0_priority_above_slowdown() {
        let mut v = Version::empty(test_schema());
        let config = EngineConfig::default();

        // L0: 25 files — above slowdown (20), far above trigger (4).
        let mut l0_files = Vec::new();
        for i in 0..25 {
            l0_files.push(make_file(&format!("l0_{i}.parquet"), 0, 100, 1024));
        }
        v.levels.insert(Level(0), l0_files);

        // L2: 10 GiB — score = 10 GiB / 2 GiB = 5.0 (high pressure).
        let mut l2_files = Vec::new();
        for i in 0..10 {
            l2_files.push(make_file(
                &format!("l2_{i}.parquet"),
                2,
                1_000_000,
                1024 * 1024 * 1024, // 1 GiB each → 10 GiB total
            ));
        }
        v.levels.insert(Level(2), l2_files);

        // Even though L2's bytes/target score is high, L0 must win because
        // it's above the slowdown trigger.
        let pick = pick_compaction(&v, &config).unwrap();
        assert_eq!(
            pick.input_level,
            Level(0),
            "L0 above slowdown must be prioritized over deeper-level pressure"
        );
    }

    /// Below the slowdown trigger, the normal max-score picker still
    /// chooses deeper levels when they have higher pressure.
    #[test]
    fn deeper_level_wins_when_l0_below_slowdown() {
        let mut v = Version::empty(test_schema());
        let config = EngineConfig::default();

        // L0: 5 files — above trigger (4) but below slowdown (20).
        let mut l0_files = Vec::new();
        for i in 0..5 {
            l0_files.push(make_file(&format!("l0_{i}.parquet"), 0, 100, 1024));
        }
        v.levels.insert(Level(0), l0_files);

        // L2: 10 GiB → score = 5.0 vs L0 score = 1.25.
        let mut l2_files = Vec::new();
        for i in 0..10 {
            l2_files.push(make_file(
                &format!("l2_{i}.parquet"),
                2,
                1_000_000,
                1024 * 1024 * 1024,
            ));
        }
        v.levels.insert(Level(2), l2_files);

        let pick = pick_compaction(&v, &config).unwrap();
        assert_eq!(
            pick.input_level,
            Level(2),
            "below slowdown, deeper level with higher score should win"
        );
    }

    /// Bug I regression: tombstones must NOT be dropped when writing to a
    /// level that has a configured size target (data can flow deeper).
    /// They should only be dropped at the true bottom level (one beyond
    /// all configured levels).
    #[test]
    fn tombstone_drop_only_at_true_bottom_level() {
        // Default: 4 entries → L1..L4 have targets, L5 is bottom.
        let n = 4;
        // L0→L1 through L3→L4: MUST preserve tombstones.
        for output in 1..=n {
            assert!(
                !should_drop_tombstones(Level(output as u8), n),
                "output L{output} must NOT drop tombstones (L{n} still has a target)"
            );
        }
        // L4→L5: safe to drop.
        assert!(
            should_drop_tombstones(Level((n + 1) as u8), n),
            "output L{} must drop tombstones (true bottom level)",
            n + 1
        );

        // Edge case: no configured levels → L0→L1 is the bottom.
        assert!(!should_drop_tombstones(Level(0), 0));
        assert!(should_drop_tombstones(Level(1), 0));

        // Edge case: 1 level target → L2 is the bottom.
        assert!(!should_drop_tombstones(Level(1), 1));
        assert!(should_drop_tombstones(Level(2), 1));
    }
}
