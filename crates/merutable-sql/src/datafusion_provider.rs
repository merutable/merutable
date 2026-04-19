//! Issue #29 Phase 2e: DataFusion `TableProvider` for the change
//! feed.
//!
//! Wraps a running `MeruEngine` + a `since_seq` watermark behind
//! the standard `datafusion::catalog::TableProvider` trait so
//! analytical consumers run the 0.1-preview headline query:
//!
//! ```sql
//! SELECT * FROM merutable_changes WHERE op = 'DELETE'
//! ```
//!
//! # Scope
//!
//! - **One-shot scan**: `scan()` drains the cursor once and
//!   materializes every record into a single RecordBatch wrapped
//!   in `MemoryExec`. Works well for the blocker's bounded-
//!   watermark query pattern.
//! - **No filter pushdown** in v1 — DataFusion applies
//!   `op = 'DELETE'` / `seq > N` after the scan. Push-down on
//!   `seq > since_seq` is a follow-on (the provider holds the
//!   watermark, so pushing the filter down would just bump
//!   `since_seq` before draining).

use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use merutable_engine::engine::MeruEngine;
use merutable_types::schema::TableSchema;

use crate::arrow::{change_feed_schema, records_to_record_batch};
use crate::ChangeFeedCursor;

/// A DataFusion-shaped view of the change feed. Register with a
/// `SessionContext::register_table("merutable_changes", ..)` and
/// consumers can query the feed with plain SQL.
pub struct ChangeFeedTableProvider {
    engine: Arc<MeruEngine>,
    table_schema: TableSchema,
    since_seq: u64,
    arrow_schema: SchemaRef,
}

impl std::fmt::Debug for ChangeFeedTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeFeedTableProvider")
            .field("table", &self.table_schema.table_name)
            .field("since_seq", &self.since_seq)
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

impl ChangeFeedTableProvider {
    pub fn new(engine: Arc<MeruEngine>, table_schema: TableSchema, since_seq: u64) -> Self {
        let arrow_schema = change_feed_schema(&table_schema);
        Self {
            engine,
            table_schema,
            since_seq,
            arrow_schema,
        }
    }
}

#[async_trait]
impl TableProvider for ChangeFeedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Phase 2e: drain the cursor synchronously into one batch.
        let mut cursor = ChangeFeedCursor::from_engine(self.engine.clone(), self.since_seq);
        let records = cursor
            .next_batch(usize::MAX)
            .map_err(|e| DataFusionError::Execution(format!("change-feed drain: {e}")))?;
        let batch = records_to_record_batch(&records, &self.table_schema)
            .map_err(|e| DataFusionError::Execution(format!("RecordBatch assembly: {e}")))?;
        let partitions = vec![vec![batch]];
        let exec =
            MemoryExec::try_new(&partitions, self.arrow_schema.clone(), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;
    use merutable_engine::config::EngineConfig;
    use merutable_types::{
        schema::{ColumnDef, ColumnType, TableSchema},
        value::{FieldValue, Row},
    };

    fn test_schema() -> TableSchema {
        TableSchema {
            table_name: "df-cf-test".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Int64,
                    nullable: false,
                    ..Default::default()
                },
                ColumnDef {
                    name: "v".into(),
                    col_type: ColumnType::Int64,
                    nullable: true,
                    ..Default::default()
                },
            ],
            primary_key: vec![0],
            ..Default::default()
        }
    }

    async fn open_engine(tmp: &tempfile::TempDir) -> Arc<MeruEngine> {
        let cfg = EngineConfig {
            schema: test_schema(),
            catalog_uri: tmp.path().to_string_lossy().to_string(),
            object_store_prefix: tmp.path().to_string_lossy().to_string(),
            wal_dir: tmp.path().join("wal"),
            ..Default::default()
        };
        MeruEngine::open(cfg).await.unwrap()
    }

    fn row(id: i64, v: i64) -> Row {
        Row::new(vec![
            Some(FieldValue::Int64(id)),
            Some(FieldValue::Int64(v)),
        ])
    }

    #[tokio::test]
    async fn select_star_returns_all_ops() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = open_engine(&tmp).await;
        for i in 1..=3i64 {
            engine
                .put(vec![FieldValue::Int64(i)], row(i, i * 10))
                .await
                .unwrap();
        }
        let ctx = SessionContext::new();
        let provider = Arc::new(ChangeFeedTableProvider::new(engine, test_schema(), 0));
        ctx.register_table("merutable_changes", provider).unwrap();
        let df = ctx
            .sql("SELECT seq, op FROM merutable_changes ORDER BY seq")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn where_clause_on_op_filters_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = open_engine(&tmp).await;
        engine
            .put(vec![FieldValue::Int64(1)], row(1, 10))
            .await
            .unwrap();
        engine
            .put(vec![FieldValue::Int64(2)], row(2, 20))
            .await
            .unwrap();
        engine.delete(vec![FieldValue::Int64(1)]).await.unwrap();
        let ctx = SessionContext::new();
        let provider = Arc::new(ChangeFeedTableProvider::new(engine, test_schema(), 0));
        ctx.register_table("merutable_changes", provider).unwrap();
        let df = ctx
            .sql("SELECT seq FROM merutable_changes WHERE op = 'DELETE'")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn since_seq_watermark_hides_earlier_ops() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = open_engine(&tmp).await;
        engine
            .put(vec![FieldValue::Int64(1)], row(1, 10))
            .await
            .unwrap();
        let boundary = engine.read_seq().0;
        engine
            .put(vec![FieldValue::Int64(2)], row(2, 20))
            .await
            .unwrap();
        engine
            .put(vec![FieldValue::Int64(3)], row(3, 30))
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let provider = Arc::new(ChangeFeedTableProvider::new(
            engine,
            test_schema(),
            boundary,
        ));
        ctx.register_table("merutable_changes", provider).unwrap();
        let df = ctx.sql("SELECT seq FROM merutable_changes").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }
}
