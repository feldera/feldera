use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::catalog::SyncSerBatchReader;
use crate::controller::ConsistentSnapshots;
use crate::RecordFormat;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use serde_arrow::schema::SerdeArrowSchema;
use serde_arrow::ArrayBuilder;
use tokio::sync::mpsc::Sender;

pub struct AdHocTable {
    typ: TableType,
    name: String,
    schema: Arc<Schema>,
    snapshots: ConsistentSnapshots,
}

impl AdHocTable {
    pub fn new(
        typ: TableType,
        name: String,
        schema: Arc<Schema>,
        snapshots: ConsistentSnapshots,
    ) -> Self {
        Self {
            typ,
            name,
            schema,
            snapshots,
        }
    }
}

#[async_trait]
impl TableProvider for AdHocTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        self.typ
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // This holds because we don't enable filter push-down for now.
        assert!(filters.is_empty(), "AdHocTable does not support filters");

        let projected_schema = if let Some(keep_indices) = projection.as_ref() {
            Arc::new(self.schema.project(keep_indices)?)
        } else {
            self.schema.clone()
        };

        Ok(Arc::new(AdHocQueryExecution::new(
            self.schema.clone(),
            projected_schema,
            self.snapshots.lock().await.get(&self.name).unwrap().clone(),
            projection,
            limit,
        )))
    }
}

struct AdHocQueryExecution {
    table_schema: Arc<Schema>,
    projected_schema: Arc<Schema>,
    readers: Vec<Arc<dyn SyncSerBatchReader>>,
    projection: Option<Vec<usize>>,
    limit: usize,
    plan_properties: PlanProperties,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

impl AdHocQueryExecution {
    fn new(
        table_schema: Arc<Schema>,
        projected_schema: Arc<Schema>,
        readers: Vec<Arc<dyn SyncSerBatchReader>>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        // TODO: we could do much better here by encoding our data partitioning schema
        // and using the correct equivalence properties.
        let num_partitions = readers.len();
        let eq_props = EquivalenceProperties::new(projected_schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(num_partitions);
        let plan_properties = PlanProperties::new(eq_props, partitioning, ExecutionMode::Bounded);

        Self {
            table_schema,
            projected_schema,
            readers,
            projection: projection.cloned(),
            limit: limit.unwrap_or(usize::MAX),
            plan_properties,
            children: vec![],
        }
    }
}

impl DisplayAs for AdHocQueryExecution {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "AdHocQueryExecution")
    }
}

impl Debug for AdHocQueryExecution {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AdHocQueryExecution")
    }
}

impl ExecutionPlan for AdHocQueryExecution {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "AdHocQueryExecution"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().map(|c| c as _).collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AdHocQueryExecution {
            table_schema: self.table_schema.clone(),
            projected_schema: self.projected_schema.clone(),
            readers: self.readers.clone(),
            projection: self.projection.clone(),
            limit: self.limit,
            plan_properties: self.plan_properties.clone(),
            children,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        async fn send_batch(
            tx: &Sender<datafusion::common::Result<RecordBatch>>,
            projection: &Option<Vec<usize>>,
            mut batch: RecordBatch,
        ) -> datafusion::common::Result<()> {
            // Apply projection if necessary, ideally we would be able to do this on the
            // fly in the cursor, but that would require a lot of changes.
            if let Some(keep_indices) = projection {
                batch = batch.project(keep_indices).map_err(|e| {
                    DataFusionError::Execution(format!("Unable to project record batch: {}", e))
                })?;
            }

            tx.send(Ok(batch)).await.map_err(|e| {
                DataFusionError::Execution(format!("Unable to send record batch: {}", e))
            })?;

            Ok(())
        }

        let mut builder = RecordBatchReceiverStreamBuilder::new(self.projected_schema.clone(), 10);
        // Returns a single batch when the returned stream is polled
        let batch_reader = self.readers[partition].clone();
        let schema = self.table_schema.clone();
        let tx = builder.tx();
        let projection = self.projection.clone();

        let sas: SerdeArrowSchema = schema.fields().iter().as_slice().try_into().map_err(|e| {
            DataFusionError::Internal(format!(
                "Unable to construct SerdeArrowSchema for the provided schema: {}.",
                e
            ))
        })?;

        builder.spawn(async move {
            const MAX_BATCH_SIZE: usize = 1 << 16;
            let mut cursor = batch_reader
                .cursor(RecordFormat::Parquet(sas.clone()))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let mut insert_builder = SendableArrowBuilder::new(sas)?;

            let mut cur_batch_size = 0;
            while cursor.key_valid() {
                if !cursor.val_valid() {
                    cursor.step_key();
                    continue;
                }
                let mut w = cursor.weight();

                // Skip deleted records.
                if w < 0 {
                    cursor.step_key();
                    continue;
                }

                while w != 0 {
                    cursor
                        .serialize_key_to_arrow(&mut insert_builder.builder)
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Unable to serialize record to arrow: {}",
                                e
                            ))
                        })?;
                    cur_batch_size += 1;
                    w -= 1;

                    if cur_batch_size >= MAX_BATCH_SIZE {
                        let batch = insert_builder.builder.to_record_batch().map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Unable to convert ArrayBuilder to RecordBatch: {}",
                                e
                            ))
                        })?;
                        send_batch(&tx, &projection, batch).await?;
                        cur_batch_size = 0;
                    }
                }
                cursor.step_key();
            }

            let batch = insert_builder.builder.to_record_batch().map_err(|e| {
                DataFusionError::Execution(format!(
                    "Unable to convert ArrayBuilder to RecordBatch: {}",
                    e
                ))
            })?;
            send_batch(&tx, &projection, batch).await?;

            Ok(())
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            builder.build(),
        )))
    }
}

struct SendableArrowBuilder {
    builder: ArrayBuilder,
}

impl SendableArrowBuilder {
    fn new(schema: SerdeArrowSchema) -> datafusion::common::Result<Self> {
        let builder = ArrayBuilder::new(schema).map_err(|e| {
            DataFusionError::Internal(format!(
                "Unable to construct serde_arrow ArrayBuilder for the provided schema: {}.",
                e
            ))
        })?;

        Ok(Self { builder })
    }
}

/// This isn't Send because the underlying Arrow builder has a raw pointer which isn't Send:
/// https://github.com/chmp/serde_arrow/blob/eb8d37a5bdab748251aa983cb1c1517047f28702/serde_arrow/src/internal/serialization/struct_builder.rs#L23C1-L23C55
///
/// But it should be safe to declare this as send because it's not used in a way that breaks Send guarantees.
///
/// I opened an issue about this here: https://github.com/chmp/serde_arrow/issues/225
unsafe impl Send for SendableArrowBuilder {}
