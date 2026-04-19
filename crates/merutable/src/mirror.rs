//! Issue #31 Phase 2a: mirror worker scaffold.
//!
//! Spawns a long-lived tokio task that polls the primary's version
//! set and, on observing a new snapshot_id, logs its intent to
//! mirror. Phase 2a does NOT yet upload bytes — that's Phase 2b. The
//! scaffold lands first so:
//!
//! - The worker's shutdown protocol is exercised ahead of the
//!   upload logic.
//! - Operators can verify (via `tracing::info!`) that their mirror
//!   is wired up and seeing snapshot advances, before any real
//!   traffic touches the destination.
//! - Phase 2b can be a pure delta: same shutdown, same polling
//!   loop, just replace the `observed_new_snapshot` log with an
//!   actual `upload_snapshot` call.
//!
//! # Shutdown
//!
//! Mirrors the `BackgroundWorkers` pattern: `AtomicBool` flag set
//! FIRST, then `Notify::notify_waiters()`. The worker checks the
//! flag at the top of every loop iteration, so a shutdown signal
//! arriving between `notified().await` registrations is not lost.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use merutable_engine::engine::MeruEngine;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::options::MirrorConfig;

/// The mirror worker's cadence. Not exposed as a knob yet — 5
/// seconds is short enough to keep mirror_lag bounded to single-
/// digit seconds under sustained writes, long enough to avoid
/// burning CPU on a quiescent primary.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Handle to the spawned mirror worker. Held by `MeruDB` behind a
/// `tokio::sync::Mutex<Option<MirrorWorker>>` so `close()` can
/// `take()` and `shutdown().await` before the engine's final
/// flush.
pub struct MirrorWorker {
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    handle: Option<JoinHandle<()>>,
    /// Highest snapshot_id the worker has OBSERVED (Phase 2a) or
    /// UPLOADED (Phase 2b+). Exposed via `mirror_seq()` so
    /// integration tests + future Phase 3 `stats()` plumbing can
    /// read it without reaching into the worker's internals.
    mirror_seq: Arc<AtomicI64>,
}

impl MirrorWorker {
    /// Spawn a mirror worker. Called by `MeruDB::open` when a
    /// `MirrorConfig` is attached. The worker lives until
    /// `shutdown()` is awaited.
    pub fn spawn(engine: Arc<MeruEngine>, config: MirrorConfig) -> Self {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(Notify::new());
        let mirror_seq = Arc::new(AtomicI64::new(0));
        let flag = shutdown_flag.clone();
        let notify = shutdown_notify.clone();
        let seq = mirror_seq.clone();
        let handle = tokio::spawn(async move {
            mirror_loop(engine, config, flag, notify, seq).await;
        });
        Self {
            shutdown_flag,
            shutdown_notify,
            handle: Some(handle),
            mirror_seq,
        }
    }

    /// Latest snapshot_id the worker has observed (Phase 2a) or
    /// mirrored (Phase 2b+). Synchronously readable from anywhere.
    /// Zero on a freshly-spawned worker that hasn't yet completed a
    /// tick.
    pub fn mirror_seq(&self) -> i64 {
        self.mirror_seq.load(Ordering::Relaxed)
    }

    /// Signal the worker to shut down and await its exit.
    ///
    /// Ordering matches `BackgroundWorkers::shutdown`:
    /// 1. Set the flag (the loop checks it at the top).
    /// 2. Notify (wake any task parked in `notified().await`).
    /// 3. Await the `JoinHandle` (drain the final tick).
    pub async fn shutdown(&mut self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

async fn mirror_loop(
    engine: Arc<MeruEngine>,
    _config: MirrorConfig,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    mirror_seq: Arc<AtomicI64>,
) {
    info!("mirror worker started (Issue #31 Phase 2a — observe-only)");
    let mut last_logged: i64 = 0;
    loop {
        if shutdown_flag.load(Ordering::SeqCst) {
            break;
        }
        let current = engine.current_snapshot_id();
        if current > last_logged {
            // Phase 2a: log the intent to mirror. Phase 2b will
            // replace this with the actual upload call
            // (enumerate new files, put_if_absent them, then
            // put_if_absent the manifest in seq order).
            info!(
                snapshot_id = current,
                previous_mirror_seq = last_logged,
                "mirror worker observed new snapshot (Phase 2a: not yet uploaded)"
            );
            last_logged = current;
            mirror_seq.store(current, Ordering::Relaxed);
        } else {
            debug!(
                snapshot_id = current,
                "mirror worker tick — no new snapshot"
            );
        }

        // Wait for either the poll interval or an explicit shutdown
        // notification. We do NOT register the `notified` future
        // before checking the flag, so a shutdown issued while we
        // were computing the `current` snapshot is caught on the
        // next loop-top flag check rather than being silently lost.
        tokio::select! {
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
            _ = shutdown_notify.notified() => {}
        }
    }
    info!(last_observed_seq = last_logged, "mirror worker shut down");
}

#[cfg(test)]
mod tests {
    use super::*;
    use merutable_engine::config::EngineConfig;
    use merutable_store::local::LocalFileStore;
    use merutable_types::schema::{ColumnDef, ColumnType, TableSchema};

    fn schema() -> TableSchema {
        TableSchema {
            table_name: "mirror-worker-test".into(),
            columns: vec![ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
                ..Default::default()
            }],
            primary_key: vec![0],
            ..Default::default()
        }
    }

    fn engine_config(tmp: &tempfile::TempDir) -> EngineConfig {
        EngineConfig {
            schema: schema(),
            catalog_uri: tmp.path().to_string_lossy().to_string(),
            object_store_prefix: tmp.path().to_string_lossy().to_string(),
            wal_dir: tmp.path().join("wal"),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn spawn_and_shutdown_cleanly() {
        let tmp = tempfile::tempdir().unwrap();
        let mirror_dir = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(engine_config(&tmp)).await.unwrap();
        let store = Arc::new(LocalFileStore::new(mirror_dir.path()).unwrap());
        let cfg = MirrorConfig::new(store);

        let mut worker = MirrorWorker::spawn(engine, cfg);
        // Fresh engine: snapshot_id is 0. Worker's mirror_seq is
        // either 0 (hasn't ticked yet) or 0 (ticked and saw 0).
        assert_eq!(worker.mirror_seq(), 0);
        // Shutdown must return within a bounded wait; no deadlock.
        tokio::time::timeout(Duration::from_secs(5), worker.shutdown())
            .await
            .expect("mirror worker shutdown hung past 5s");
    }

    /// A second shutdown call after the first is a no-op (not a
    /// panic). Mirrors the `close()` contract on `MeruDB`.
    #[tokio::test]
    async fn double_shutdown_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let mirror_dir = tempfile::tempdir().unwrap();
        let engine = MeruEngine::open(engine_config(&tmp)).await.unwrap();
        let store = Arc::new(LocalFileStore::new(mirror_dir.path()).unwrap());
        let mut worker = MirrorWorker::spawn(engine, MirrorConfig::new(store));
        worker.shutdown().await;
        worker.shutdown().await; // must not panic
    }
}
