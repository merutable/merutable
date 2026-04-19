//! Leveled compaction picker.
//!
//! Scores each level and picks the one with the highest compaction pressure.
//!
//! - L0 score = `l0_file_count / l0_compaction_trigger`
//! - L1+ score = `level_bytes / level_target_bytes[level]`
//! - Pick highest score > 1.0; output level = input + 1.
//!
//! Parallelism: the picker accepts a `busy_levels` set; any level whose
//! input or output is already being compacted by another worker is
//! skipped. This lets `run_compaction` run multiple jobs concurrently
//! on disjoint level sets (e.g. L0→L1 in worker A, L2→L3 in worker B).

use std::collections::HashSet;

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

/// Pick the best compaction, or `None` if no level needs compaction —
/// or if every candidate level has its input or output level already
/// reserved by another in-progress compaction.
///
/// L0 priority: when `l0_file_count >= l0_slowdown_trigger`, L0 is picked
/// unconditionally regardless of deeper-level scores. L0 buildup degrades
/// both read latency (more files to probe) and write latency (slowdown
/// sleep + eventual stall).
///
/// Parallelism: `busy_levels` is the set of levels currently being
/// compacted. A candidate level `L` is eligible iff neither `L` nor
/// `L.next()` is in `busy_levels`. This lets a long-running L2→L3
/// compaction coexist with concurrent L0→L1 drainage — the key
/// mechanism that prevents the stress-test L0 buildup when a deep
/// compaction would otherwise hold the worker slot for minutes.
pub fn pick_compaction(
    version: &Version,
    config: &EngineConfig,
    busy_levels: &HashSet<Level>,
) -> Option<CompactionPick> {
    let level_is_free =
        |lvl: Level| -> bool { !busy_levels.contains(&lvl) && !busy_levels.contains(&lvl.next()) };

    let l0_count = version.l0_file_count();
    let l0_score = l0_count as f64 / config.l0_compaction_trigger as f64;

    // L0 priority path.
    let force_l0 = l0_count >= config.l0_slowdown_trigger && level_is_free(Level(0));

    let best_level;
    let best_score;

    if force_l0 {
        // L0 is above slowdown — compact it regardless of deeper pressure.
        best_level = Level(0);
        best_score = l0_score;
    } else {
        // Normal scoring: pick the highest-pressure eligible level.
        let mut cur_best_score = 0.0f64;
        let mut cur_best_level = Level(0);
        let mut found_candidate = false;

        if l0_score > cur_best_score && level_is_free(Level(0)) {
            cur_best_score = l0_score;
            cur_best_level = Level(0);
            found_candidate = true;
        }
        for (i, &target) in config.level_target_bytes.iter().enumerate() {
            let level = Level((i + 1) as u8);
            if !level_is_free(level) {
                continue;
            }
            let bytes = version.level_bytes(level);
            if target > 0 {
                let score = bytes as f64 / target as f64;
                if score > cur_best_score {
                    cur_best_score = score;
                    cur_best_level = level;
                    found_candidate = true;
                }
            }
        }

        if !found_candidate || cur_best_score < 1.0 {
            return None; // no eligible level needs compaction
        }
        best_level = cur_best_level;
        best_score = cur_best_score;
    }

    let output_level = best_level.next();

    // Select input files.
    //
    // Memory cap: `config.max_compaction_bytes` bounds how much input
    // a single compaction loads into memory. The compaction job reads
    // whole Parquet files into `Bytes` buffers before the merge, so
    // unbounded input selection = unbounded RSS. A stress test with
    // a 16 GiB L3 would otherwise pull the entire level into memory
    // per compaction; with parallel workers this multiplies.
    //
    // The strategy differs by level type because the file layout does:
    //
    // - **L0 files can overlap**: any subset is a valid compaction.
    //   Take oldest-first (seq_min ASC) so newer data stays visible
    //   in L0 while older data flows to L1.
    //
    // - **L1+ files are non-overlapping, sorted by key_min**: picking
    //   a non-contiguous subset would force the overlap pull-in at
    //   the output level to span the entire level (breaking the cap).
    //   Instead, take a contiguous prefix from the sorted file list;
    //   the overlap pull-in is then bounded by the selected range's
    //   key span (at most `~10× max_compaction_bytes` given standard
    //   level-size multipliers).
    //
    // Progress guarantee: always take at least one file, even if it
    // exceeds the cap on its own. Remaining files stay at the current
    // level and are drained by the next iteration of the
    // `run_compaction` loop.
    let max_bytes = config.max_compaction_bytes;
    let input_files: Vec<String> = if best_level == Level(0) {
        let mut l0: Vec<&merutable_iceberg::version::DataFileMeta> =
            version.files_at(Level(0)).iter().collect();
        // Sort oldest-first by seq_min so the picked subset is the
        // oldest contiguous batch — the newest L0 files (with the
        // highest seq numbers) remain live and readable while the
        // older ones are rewritten to L1.
        l0.sort_by_key(|f| f.meta.seq_min);
        let mut total = 0u64;
        let mut picked = Vec::new();
        for f in l0 {
            if !picked.is_empty() && total.saturating_add(f.meta.file_size) > max_bytes {
                break;
            }
            total = total.saturating_add(f.meta.file_size);
            picked.push(f.path.clone());
        }
        picked
    } else {
        // L1+: contiguous prefix by key_min (the level's intrinsic
        // order — see Manifest::to_version), capped by total bytes.
        // Issue #2: this prevents a single L2→L3 or L3→L4 compaction
        // from pulling a multi-GiB level entirely into memory. The
        // same range is compacted in the next iteration of the loop
        // with the next prefix, so the level drains predictably.
        //
        // Note: contiguous-from-start always picks the lowest-key
        // range first. For hot-key workloads concentrated at one end
        // of the key space, this produces uneven compaction
        // frequency. A rotating compaction cursor (Pebble's
        // `compact_cursor_`) is the textbook mitigation but is out of
        // scope here — the memory-safety invariant is the priority.
        let files = version.files_at(best_level);
        let mut total = 0u64;
        let mut picked = Vec::new();
        for f in files.iter() {
            if !picked.is_empty() && total.saturating_add(f.meta.file_size) > max_bytes {
                break;
            }
            total = total.saturating_add(f.meta.file_size);
            picked.push(f.path.clone());
        }
        picked
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
                format: None,
                column_stats: None,
            },
            dv_path: None,
            dv_offset: None,
            dv_length: None,
        }
    }

    fn empty_busy() -> HashSet<Level> {
        HashSet::new()
    }

    #[test]
    fn no_compaction_needed() {
        let v = Version::empty(test_schema());
        let config = EngineConfig::default();
        assert!(pick_compaction(&v, &config, &empty_busy()).is_none());
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
        let pick = pick_compaction(&v, &config, &empty_busy()).unwrap();
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
        let pick = pick_compaction(&v, &config, &empty_busy()).unwrap();
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

        let pick = pick_compaction(&v, &config, &empty_busy()).unwrap();
        assert_eq!(
            pick.input_level,
            Level(2),
            "below slowdown, deeper level with higher score should win"
        );
    }

    /// Parallelism: when L0→L1 is already in progress (busy={L0,L1}),
    /// another worker should be able to pick a disjoint deeper compaction
    /// (L2→L3) instead of returning None. This is the mechanism that
    /// unblocks stress tests where a long L0 drainage would otherwise
    /// starve L2/L3 compactions (and vice versa).
    #[test]
    fn picks_disjoint_level_when_l0_busy() {
        let mut v = Version::empty(test_schema());
        let config = EngineConfig::default();

        // L0: above trigger but below slowdown (so not force-picked).
        for i in 0..5 {
            v.levels.entry(Level(0)).or_default().push(make_file(
                &format!("l0_{i}.parquet"),
                0,
                100,
                1024,
            ));
        }
        // L2: above target so score >= 1.
        for i in 0..10 {
            v.levels.entry(Level(2)).or_default().push(make_file(
                &format!("l2_{i}.parquet"),
                2,
                1_000_000,
                1024 * 1024 * 1024,
            ));
        }

        // Simulate worker A doing L0→L1: both levels are reserved.
        let mut busy = HashSet::new();
        busy.insert(Level(0));
        busy.insert(Level(1));

        let pick = pick_compaction(&v, &config, &busy).unwrap();
        assert_eq!(
            pick.input_level,
            Level(2),
            "L0 busy → worker B must be able to pick L2 (disjoint from {{L0,L1}})"
        );
        assert_eq!(pick.output_level, Level(3));
    }

    /// When every eligible level's input or output is reserved, the
    /// picker must return None (don't steal files from an in-progress
    /// compaction). The caller's loop then exits and yields to other
    /// workers.
    #[test]
    fn returns_none_when_all_candidates_busy() {
        let mut v = Version::empty(test_schema());
        let config = EngineConfig::default();

        // L0 over trigger.
        for i in 0..5 {
            v.levels.entry(Level(0)).or_default().push(make_file(
                &format!("l0_{i}.parquet"),
                0,
                100,
                1024,
            ));
        }

        // Busy: L0→L1 in progress.
        let mut busy = HashSet::new();
        busy.insert(Level(0));
        busy.insert(Level(1));

        assert!(
            pick_compaction(&v, &config, &busy).is_none(),
            "no other level needs compaction → must return None when L0 is busy"
        );
    }

    /// BUG-0002 regression: L0 picker must cap total input bytes at
    /// `config.max_compaction_bytes`. Without this, a large L0
    /// (e.g. 75 files × 64 MiB = 4.8 GiB) would load entirely into
    /// memory and cause OOM when multiple workers compact in parallel.
    #[test]
    fn l0_pick_caps_at_max_compaction_bytes() {
        let mut v = Version::empty(test_schema());
        let config = EngineConfig {
            max_compaction_bytes: 256 * 1024 * 1024, // 256 MiB cap.
            ..EngineConfig::default()
        };

        // 20 L0 files × 64 MiB = 1.28 GiB total — well above cap.
        for i in 0..20 {
            let mut f = make_file(&format!("l0_{i}.parquet"), 0, 100, 64 * 1024 * 1024);
            // Make seq_min increase with i so sort is deterministic.
            f.meta.seq_min = (i + 1) as u64;
            f.meta.seq_max = (i + 1) as u64 * 10;
            v.levels.entry(Level(0)).or_default().push(f);
        }

        let pick = pick_compaction(&v, &config, &empty_busy()).unwrap();
        assert_eq!(pick.input_level, Level(0));
        // 256 MiB / 64 MiB = 4 files max before hitting the cap; the
        // loop rule "always take at least one" means we'd take 4 here
        // (at cap, not over).
        assert!(
            pick.input_files.len() <= 5,
            "L0 pick must respect max_compaction_bytes; got {} files \
             (cap=256MiB, 64MiB each)",
            pick.input_files.len()
        );
        assert!(
            !pick.input_files.is_empty(),
            "L0 pick must always take at least one file to make progress"
        );
    }

    /// Issue #2 regression: L1+ picker must also cap total input bytes
    /// at `config.max_compaction_bytes`. A 16 GiB L3 compaction would
    /// otherwise load the whole level into memory (BUG-0002 at 32 GB
    /// RSS / 10 GB data). Fix: contiguous prefix by key_min, bounded
    /// by the cap.
    #[test]
    fn l1plus_pick_caps_at_max_compaction_bytes() {
        let mut v = Version::empty(test_schema());
        let config = EngineConfig {
            max_compaction_bytes: 256 * 1024 * 1024, // 256 MiB cap.
            ..EngineConfig::default()
        };

        // 20 L2 files × 512 MiB = 10 GiB — far over cap, above target.
        for i in 0..20 {
            let mut f = make_file(&format!("l2_{i}.parquet"), 2, 100_000, 512 * 1024 * 1024);
            // Distinct key ranges so they're non-overlapping.
            f.meta.key_min = vec![i as u8];
            f.meta.key_max = vec![i as u8, 0xFF];
            v.levels.entry(Level(2)).or_default().push(f);
        }
        // Manifest::to_version sorts L1+ by key_min ASC; mimic that.
        v.levels
            .get_mut(&Level(2))
            .unwrap()
            .sort_by(|a, b| a.meta.key_min.cmp(&b.meta.key_min));

        let pick = pick_compaction(&v, &config, &empty_busy()).unwrap();
        assert_eq!(pick.input_level, Level(2));
        // 256 MiB cap / 512 MiB per file → at most one file per pick
        // (the first crosses the cap on its own; always picks at least one).
        assert_eq!(
            pick.input_files.len(),
            1,
            "L2 pick must respect max_compaction_bytes; got {} files \
             (cap=256MiB, 512MiB each, should pick exactly 1)",
            pick.input_files.len()
        );
    }

    /// L1+ pick is a CONTIGUOUS prefix by key_min, not a random subset.
    /// Non-contiguous selection would force the overlap pull-in at the
    /// output level to span the whole level, defeating the cap.
    #[test]
    fn l1plus_pick_is_contiguous_prefix() {
        let mut v = Version::empty(test_schema());
        let config = EngineConfig {
            max_compaction_bytes: 300 * 1024 * 1024, // fits 3× 100 MiB files.
            ..EngineConfig::default()
        };

        // 30 files × 100 MiB = 3 GiB — above L2 target (2 GiB) so the
        // picker scores L2 > 1.0 and selects it.
        for i in 0..30 {
            let mut f = make_file(&format!("l2_{i:02}.parquet"), 2, 100_000, 100 * 1024 * 1024);
            f.meta.key_min = vec![i as u8];
            f.meta.key_max = vec![i as u8, 0xFF];
            v.levels.entry(Level(2)).or_default().push(f);
        }
        v.levels
            .get_mut(&Level(2))
            .unwrap()
            .sort_by(|a, b| a.meta.key_min.cmp(&b.meta.key_min));

        let pick = pick_compaction(&v, &config, &empty_busy()).unwrap();
        assert_eq!(pick.input_level, Level(2));
        // 300 MiB cap / 100 MiB per file = 3 files exactly.
        assert_eq!(pick.input_files.len(), 3, "expected 3 files under cap");
        // Must be the FIRST three (lowest key_min), proving contiguous prefix.
        assert_eq!(pick.input_files[0], "l2_00.parquet");
        assert_eq!(pick.input_files[1], "l2_01.parquet");
        assert_eq!(pick.input_files[2], "l2_02.parquet");
    }

    /// Progress guarantee: even if a single L0 file exceeds the cap,
    /// one file is always picked — otherwise the tree would wedge.
    #[test]
    fn l0_pick_always_picks_at_least_one() {
        let mut v = Version::empty(test_schema());
        let config = EngineConfig {
            max_compaction_bytes: 1024, // Absurdly small cap (1 KiB).
            ..EngineConfig::default()
        };

        for i in 0..5 {
            let mut f = make_file(&format!("l0_{i}.parquet"), 0, 100, 64 * 1024 * 1024);
            f.meta.seq_min = (i + 1) as u64;
            v.levels.entry(Level(0)).or_default().push(f);
        }

        let pick = pick_compaction(&v, &config, &empty_busy()).unwrap();
        assert_eq!(pick.input_files.len(), 1, "must take at least one file");
    }

    /// L0 force-pick must yield to an in-progress L0 compaction. A
    /// second worker entering while L0 is already being drained should
    /// not try to steal L0 files — even if L0 is above slowdown.
    /// Otherwise the reservation fails and we waste a picker cycle.
    #[test]
    fn l0_force_pick_respects_busy_l0() {
        let mut v = Version::empty(test_schema());
        let config = EngineConfig::default();

        // L0: 30 files — well above slowdown.
        for i in 0..30 {
            v.levels.entry(Level(0)).or_default().push(make_file(
                &format!("l0_{i}.parquet"),
                0,
                100,
                1024,
            ));
        }
        // L2 also needs work.
        for i in 0..10 {
            v.levels.entry(Level(2)).or_default().push(make_file(
                &format!("l2_{i}.parquet"),
                2,
                1_000_000,
                1024 * 1024 * 1024,
            ));
        }

        // L0 already busy.
        let mut busy = HashSet::new();
        busy.insert(Level(0));
        busy.insert(Level(1));

        // Must fall back to normal scoring and pick L2, not re-pick L0.
        let pick = pick_compaction(&v, &config, &busy).unwrap();
        assert_eq!(
            pick.input_level,
            Level(2),
            "L0 busy → force-pick must defer to normal scoring and pick L2"
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
