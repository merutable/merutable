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
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

// The `metrics` crate installs exactly one global recorder per process;
// once installed, it cannot be uninstalled. Use a `OnceLock` so multiple
// `#[test]` runs in the same binary share the same sink without
// double-registration panics.
static SINK: std::sync::OnceLock<Arc<CounterSink>> = std::sync::OnceLock::new();

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
