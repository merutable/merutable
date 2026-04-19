//! Issue #14 Phase 2: hot-path counters fire on put / delete / get /
//! scan / batch / cache hit / cache miss. Verifies the wiring via a
//! minimal in-memory `metrics::Recorder` implementation; we don't
//! depend on `metrics-util` to keep the test dependency surface tight.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use merutable_engine::{EngineConfig, MeruEngine};
use merutable_types::{
    schema::{ColumnDef, ColumnType, TableSchema},
    value::{FieldValue, Row},
};
use tempfile::TempDir;

// ── Test recorder ─────────────────────────────────────────────────────

#[derive(Default)]
struct CounterSink {
    counts: Mutex<HashMap<String, u64>>,
    histograms: Mutex<HashMap<String, Vec<f64>>>,
}

impl CounterSink {
    fn get(&self, name: &str) -> u64 {
        *self.counts.lock().unwrap().get(name).unwrap_or(&0)
    }
    fn add(&self, name: &str, n: u64) {
        *self
            .counts
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert(0) += n;
    }
    fn histogram_samples(&self, name: &str) -> Vec<f64> {
        self.histograms
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .unwrap_or_default()
    }
    fn record_histogram(&self, name: &str, value: f64) {
        self.histograms
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .push(value);
    }
}

struct SinkHistogram {
    sink: Arc<CounterSink>,
    name: String,
}

impl metrics::HistogramFn for SinkHistogram {
    fn record(&self, value: f64) {
        self.sink.record_histogram(&self.name, value);
    }
}

struct SinkCounter {
    sink: Arc<CounterSink>,
    name: String,
}

impl metrics::CounterFn for SinkCounter {
    fn increment(&self, value: u64) {
        self.sink.add(&self.name, value);
    }
    fn absolute(&self, value: u64) {
        self.sink.add(&self.name, value);
    }
}

struct TestRecorder {
    sink: Arc<CounterSink>,
}

impl metrics::Recorder for TestRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn register_counter(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        let name = key.name().to_string();
        metrics::Counter::from_arc(Arc::new(SinkCounter {
            sink: self.sink.clone(),
            name,
        }))
    }
    fn register_gauge(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        let name = key.name().to_string();
        metrics::Histogram::from_arc(Arc::new(SinkHistogram {
            sink: self.sink.clone(),
            name,
        }))
    }
}

// The `metrics` crate installs exactly one global recorder per process;
// once installed, it cannot be uninstalled. Use a `OnceLock` so multiple
// `#[test]` runs in the same binary share the same sink without
// double-registration panics.
static SINK: std::sync::OnceLock<Arc<CounterSink>> = std::sync::OnceLock::new();
// Tests that read/write the shared sink must serialize; parallel
// execution mixes counter increments. Use a tokio async mutex since
// the test bodies hold this across await points.
static TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn install_once() -> Arc<CounterSink> {
    SINK.get_or_init(|| {
        let sink = Arc::new(CounterSink::default());
        let recorder = TestRecorder { sink: sink.clone() };
        let _ = metrics::set_global_recorder(recorder);
        sink
    })
    .clone()
}

// ── Fixtures ──────────────────────────────────────────────────────────

fn schema() -> TableSchema {
    TableSchema {
        table_name: "m14p2".into(),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                col_type: ColumnType::Int64,
                nullable: false,
            },
            ColumnDef {
                name: "v".into(),
                col_type: ColumnType::ByteArray,
                nullable: true,
            },
        ],
        primary_key: vec![0],
    }
}

fn config(tmp: &TempDir) -> EngineConfig {
    EngineConfig {
        schema: schema(),
        catalog_uri: tmp.path().to_string_lossy().to_string(),
        object_store_prefix: tmp.path().to_string_lossy().to_string(),
        wal_dir: tmp.path().join("wal"),
        flush_parallelism: 0,
        compaction_parallelism: 0,
        row_cache_capacity: 100,
        ..Default::default()
    }
}

fn make_row(id: i64) -> Row {
    Row::new(vec![
        Some(FieldValue::Int64(id)),
        Some(FieldValue::Bytes(Bytes::from(format!("v{id}")))),
    ])
}

// ── Tests ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn put_get_delete_scan_fire_phase2_counters() {
    let _serial = TEST_SERIAL.lock().await;
    let sink = install_once();
    let before_put = sink.get("merutable.write.puts_total");
    let before_del = sink.get("merutable.write.deletes_total");
    let before_get = sink.get("merutable.read.gets_total");
    let before_hit = sink.get("merutable.read.get_hits_total");
    let before_scan = sink.get("merutable.read.scans_total");
    let before_scan_rows = sink.get("merutable.read.scan_rows_total");

    let tmp = tempfile::tempdir().unwrap();
    let engine: Arc<MeruEngine> = MeruEngine::open(config(&tmp)).await.unwrap();

    for i in 0..10i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i))
            .await
            .unwrap();
    }
    engine.delete(vec![FieldValue::Int64(0)]).await.unwrap();

    // Two gets: one hit, one miss (deleted).
    assert!(engine.get(&[FieldValue::Int64(5)]).unwrap().is_some());
    assert!(engine.get(&[FieldValue::Int64(0)]).unwrap().is_none());

    // Scan returning rows.
    let rows = engine.scan(None, None).unwrap();
    assert_eq!(rows.len(), 9, "1 of 10 deleted");

    assert_eq!(
        sink.get("merutable.write.puts_total") - before_put,
        10,
        "10 put() calls"
    );
    assert_eq!(
        sink.get("merutable.write.deletes_total") - before_del,
        1,
        "1 delete() call"
    );
    assert_eq!(
        sink.get("merutable.read.gets_total") - before_get,
        2,
        "2 get() calls"
    );
    assert_eq!(
        sink.get("merutable.read.get_hits_total") - before_hit,
        1,
        "1 get returned Some"
    );
    assert_eq!(
        sink.get("merutable.read.scans_total") - before_scan,
        1,
        "1 scan() call"
    );
    assert_eq!(
        sink.get("merutable.read.scan_rows_total") - before_scan_rows,
        9,
        "scan returned 9 rows"
    );

    engine.close().await.unwrap();
}

/// Issue #14 Phase 3: flush and commit duration histograms record
/// samples once per flush job — sampled post-operation, never per row.
#[tokio::test]
async fn flush_and_commit_duration_histograms_record_samples() {
    let _serial = TEST_SERIAL.lock().await;
    let sink = install_once();
    let before_flush = sink
        .histogram_samples("merutable.flush.duration_seconds")
        .len();
    let before_commit = sink
        .histogram_samples("merutable.catalog.commit_duration_seconds")
        .len();
    let before_bytes = sink.histogram_samples("merutable.flush.output_bytes").len();

    let tmp = tempfile::tempdir().unwrap();
    let engine: Arc<MeruEngine> = MeruEngine::open(config(&tmp)).await.unwrap();

    for i in 0..20i64 {
        engine
            .put(vec![FieldValue::Int64(i)], make_row(i))
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();

    let flush_samples = sink.histogram_samples("merutable.flush.duration_seconds");
    let commit_samples = sink.histogram_samples("merutable.catalog.commit_duration_seconds");
    let bytes_samples = sink.histogram_samples("merutable.flush.output_bytes");

    assert_eq!(
        flush_samples.len() - before_flush,
        1,
        "exactly one flush-duration sample per flush call"
    );
    assert_eq!(
        commit_samples.len() - before_commit,
        1,
        "exactly one commit-duration sample per flush commit"
    );
    assert_eq!(
        bytes_samples.len() - before_bytes,
        1,
        "exactly one flush-output-bytes sample per flush"
    );

    // Durations are positive finite seconds. Output bytes are > 0
    // because 20 rows went to disk.
    let last_flush = flush_samples.last().copied().unwrap();
    let last_bytes = bytes_samples.last().copied().unwrap();
    assert!(last_flush > 0.0 && last_flush.is_finite());
    assert!(last_bytes > 0.0);

    engine.close().await.unwrap();
}
