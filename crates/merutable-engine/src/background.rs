//! Background Tokio task pool for flush and compaction workers.
//!
//! `BackgroundWorkers` spawns long-lived tasks that poll for work:
//! - **Flush worker**: checks for immutable memtables and flushes them.
//! - **Compaction worker**: checks version for compaction triggers and runs jobs.
//!
//! # Shutdown correctness
//!
//! Shutdown uses a dual signal: an `AtomicBool` flag AND a `Notify`. The
//! `Notify` alone is insufficient — `Notify::notify_waiters()` only
//! wakes tasks that have ALREADY called `notified().await`. Between
//! consecutive iterations of the worker's select, the task is briefly
//! not registered as a waiter; a `shutdown()` call in that window is
//! silently lost, and the worker only exits on the next 1-second
//! timeout (or, if the outer `JoinHandle::await` runs on the same
//! single-threaded runtime as the worker, it never exits at all
//! because the worker never gets scheduled to advance past its next
//! `notified().await` — classic deadlock under `tokio::test`'s
//! `current_thread` runtime).
//!
//! The `AtomicBool` is checked at the top of each loop iteration and
//! is set by `shutdown()` BEFORE notifying; a worker cannot miss both.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::engine::MeruEngine;

/// Handles for background flush and compaction workers.
pub struct BackgroundWorkers {
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    flush_handles: Vec<tokio::task::JoinHandle<()>>,
    compaction_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl BackgroundWorkers {
    /// Spawn background workers. Call after `MeruEngine::open()`.
    pub fn spawn(engine: Arc<MeruEngine>) -> Self {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(Notify::new());
        let flush_parallelism = engine.config.flush_parallelism;
        let compaction_parallelism = engine.config.compaction_parallelism;

        let mut flush_handles = Vec::new();
        for i in 0..flush_parallelism {
            let eng = engine.clone();
            let flag = shutdown_flag.clone();
            let notify = shutdown_notify.clone();
            let handle = tokio::spawn(async move {
                flush_worker(eng, flag, notify, i).await;
            });
            flush_handles.push(handle);
        }

        let mut compaction_handles = Vec::new();
        for i in 0..compaction_parallelism {
            let eng = engine.clone();
            let flag = shutdown_flag.clone();
            let notify = shutdown_notify.clone();
            let handle = tokio::spawn(async move {
                compaction_worker(eng, flag, notify, i).await;
            });
            compaction_handles.push(handle);
        }

        Self {
            shutdown_flag,
            shutdown_notify,
            flush_handles,
            compaction_handles,
        }
    }

    /// Signal all workers to shut down and wait for them to finish.
    ///
    /// Ordering: set the `AtomicBool` flag FIRST, then notify. Any
    /// worker that polls `notified()` between the store and the
    /// notify will return from its select on the notify branch; any
    /// worker that races ahead of the notify will see the flag at
    /// the top of its next iteration. Either path is deterministic.
    pub async fn shutdown(self) {
        self.shutdown_flag.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        for h in self.flush_handles {
            let _ = h.await;
        }
        for h in self.compaction_handles {
            let _ = h.await;
        }
    }
}

async fn flush_worker(
    engine: Arc<MeruEngine>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    id: usize,
) {
    debug!(worker = id, "flush worker started");
    loop {
        if shutdown_flag.load(Ordering::Acquire) {
            info!(worker = id, "flush worker shutting down");
            break;
        }
        // Bug S fix: wait on `immutable_available` (fired by rotate()),
        // not `flush_complete` (fired by drop_flushed()).
        //
        // Added a short timeout alongside the notify so a worker that
        // registered its `notified()` future AFTER `shutdown()` fired
        // still periodically re-checks the `shutdown_flag` and exits.
        // Without this the first-iteration registration window is a
        // real hazard under `current_thread` runtimes.
        tokio::select! {
            _ = shutdown_notify.notified() => {
                info!(worker = id, "flush worker shutting down");
                break;
            }
            _ = engine.memtable.immutable_available.notified() => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
        }

        // Drain all pending flushes.
        while engine.memtable.oldest_immutable().is_some() {
            if shutdown_flag.load(Ordering::Acquire) {
                break;
            }
            match crate::flush::run_flush(&engine).await {
                Ok(_) => debug!(worker = id, "flush completed"),
                Err(e) => {
                    // If the engine has been closed, ReadOnly/Closed
                    // errors are expected — don't spam warnings.
                    if engine.is_closed() {
                        break;
                    }
                    warn!(worker = id, error = %e, "flush failed, will retry");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
}

async fn compaction_worker(
    engine: Arc<MeruEngine>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    id: usize,
) {
    debug!(worker = id, "compaction worker started");
    loop {
        if shutdown_flag.load(Ordering::Acquire) {
            info!(worker = id, "compaction worker shutting down");
            break;
        }
        // Wait for a notification, a short timer (to recheck the
        // shutdown flag in case the notify was missed), or a longer
        // timer as a work heartbeat.
        tokio::select! {
            _ = shutdown_notify.notified() => {
                info!(worker = id, "compaction worker shutting down");
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }

        if shutdown_flag.load(Ordering::Acquire) {
            break;
        }

        // Bug Y fix: always call `run_compaction` — it calls
        // `pick_compaction` internally which scores ALL levels (L0 and
        // L1+) and returns `None` if no compaction is needed. The old
        // code only gated on L0 count, so L1+ compactions never
        // triggered from the background worker.
        match crate::compaction::job::run_compaction(&engine).await {
            Ok(_) => debug!(worker = id, "compaction completed"),
            Err(e) => {
                if engine.is_closed() {
                    break;
                }
                warn!(worker = id, error = %e, "compaction failed, will retry");
            }
        }
    }
}
