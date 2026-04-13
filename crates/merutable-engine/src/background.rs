//! Background Tokio task pool for flush and compaction workers.
//!
//! `BackgroundWorkers` spawns long-lived tasks that poll for work:
//! - **Flush worker**: checks for immutable memtables and flushes them.
//! - **Compaction worker**: checks version for compaction triggers and runs jobs.
//!
//! Both workers use `tokio::sync::Notify` to wake up when new work arrives,
//! rather than polling on a timer.

use std::sync::Arc;

use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::engine::MeruEngine;

/// Handles for background flush and compaction workers.
pub struct BackgroundWorkers {
    shutdown: Arc<Notify>,
    flush_handles: Vec<tokio::task::JoinHandle<()>>,
    compaction_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl BackgroundWorkers {
    /// Spawn background workers. Call after `MeruEngine::open()`.
    pub fn spawn(engine: Arc<MeruEngine>) -> Self {
        let shutdown = Arc::new(Notify::new());
        let flush_parallelism = engine.config.flush_parallelism;
        let compaction_parallelism = engine.config.compaction_parallelism;

        let mut flush_handles = Vec::new();
        for i in 0..flush_parallelism {
            let eng = engine.clone();
            let shut = shutdown.clone();
            let handle = tokio::spawn(async move {
                flush_worker(eng, shut, i).await;
            });
            flush_handles.push(handle);
        }

        let mut compaction_handles = Vec::new();
        for i in 0..compaction_parallelism {
            let eng = engine.clone();
            let shut = shutdown.clone();
            let handle = tokio::spawn(async move {
                compaction_worker(eng, shut, i).await;
            });
            compaction_handles.push(handle);
        }

        Self {
            shutdown,
            flush_handles,
            compaction_handles,
        }
    }

    /// Signal all workers to shut down and wait for them to finish.
    pub async fn shutdown(self) {
        self.shutdown.notify_waiters();
        for h in self.flush_handles {
            let _ = h.await;
        }
        for h in self.compaction_handles {
            let _ = h.await;
        }
    }
}

async fn flush_worker(engine: Arc<MeruEngine>, shutdown: Arc<Notify>, id: usize) {
    debug!(worker = id, "flush worker started");
    loop {
        // Bug S fix: wait on `immutable_available` (fired by rotate()),
        // not `flush_complete` (fired by drop_flushed()). The old code
        // created a chicken-and-egg: the flush worker only woke when a
        // prior flush completed, but nobody triggered the first flush.
        tokio::select! {
            _ = shutdown.notified() => {
                info!(worker = id, "flush worker shutting down");
                break;
            }
            _ = engine.memtable.immutable_available.notified() => {}
        }

        // Drain all pending flushes.
        while engine.memtable.oldest_immutable().is_some() {
            match crate::flush::run_flush(&engine).await {
                Ok(_) => debug!(worker = id, "flush completed"),
                Err(e) => {
                    warn!(worker = id, error = %e, "flush failed, will retry");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
}

async fn compaction_worker(engine: Arc<MeruEngine>, shutdown: Arc<Notify>, id: usize) {
    debug!(worker = id, "compaction worker started");
    loop {
        // Wait for a notification or periodic check.
        tokio::select! {
            _ = shutdown.notified() => {
                info!(worker = id, "compaction worker shutting down");
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }

        // Bug Y fix: always call `run_compaction` — it calls
        // `pick_compaction` internally which scores ALL levels (L0 and
        // L1+) and returns `None` if no compaction is needed. The old
        // code only gated on L0 count, so L1+ compactions never
        // triggered from the background worker.
        match crate::compaction::job::run_compaction(&engine).await {
            Ok(_) => debug!(worker = id, "compaction completed"),
            Err(e) => {
                warn!(worker = id, error = %e, "compaction failed, will retry");
            }
        }
    }
}
