use std::any::Any;
use std::borrow::Cow;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Weak};

use crate::catalog::SyncSerBatchReader;
use crate::controller::{ConsistentSnapshots, ControllerInner};
use crate::transport::adhoc::AdHocInputEndpoint;
use crate::{DeCollectionHandle, RecordFormat, TransportInputEndpoint};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{exec_err, not_impl_err, plan_err, SchemaExt};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use feldera_types::config::{
    default_max_batch_size, default_max_queued_records, ConnectorConfig, FormatConfig,
    InputEndpointConfig, TransportConfig,
};
use feldera_types::program_schema::SqlIdentifier;
use feldera_types::serde_with_context::serde_config::{
    BinaryFormat, DecimalFormat, UuidFormat, VariantFormat,
};
use feldera_types::serde_with_context::{DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat};
use feldera_types::transport::adhoc::AdHocInputConfig;
use serde_arrow::schema::SerdeArrowSchema;
use serde_arrow::ArrayBuilder;
use tokio::sync::mpsc::Sender;
use tracing::{info_span, Instrument};
use uuid::Uuid;

pub const fn input_adhoc_arrow_serde_config() -> &'static SqlSerdeConfig {
    &SqlSerdeConfig {
        timestamp_format: TimestampFormat::String("%FT%T%.f"),
        time_format: TimeFormat::String("%T"),
        date_format: DateFormat::String("%Y-%m-%d"),
        decimal_format: DecimalFormat::String,
        variant_format: VariantFormat::JsonString,
        binary_format: BinaryFormat::Array,
        uuid_format: UuidFormat::String,
    }
}

/// Arrow serde config for encoding result of ad hoc queries.
pub const fn output_adhoc_arrow_serde_config() -> &'static SqlSerdeConfig {
    &SqlSerdeConfig {
        timestamp_format: TimestampFormat::MicrosSinceEpoch,
        time_format: TimeFormat::NanosSigned,
        date_format: DateFormat::String("%Y-%m-%d"),
        decimal_format: DecimalFormat::String,
        variant_format: VariantFormat::JsonString,
        binary_format: BinaryFormat::Array,
        // Datafusion doesn't have a builtin UUID type, so we map UUID columns into strings.
        // Alternatively we can use byte array encoding. I tried it and it works, but requires
        // adjusting uuid type encoding in `columntype_to_datatype`.
        uuid_format: UuidFormat::String,
    }
}

pub struct AdHocTable {
    // We use a weak reference to avoid a reference cycle.
    // e.g., controller owns datafusion SessionContext, which in turn owns
    // the table somewhere underneath.
    controller: Weak<ControllerInner>,
    input_handle: Option<Box<dyn DeCollectionHandle>>,
    name: SqlIdentifier,
    materialized: bool,
    /// True if the table is indexed.
    ///
    /// When true, table records are stored as values; otherwise, they are stored as keys.
    indexed: bool,
    schema: Arc<Schema>,
    /// Contains the current snapshots for tables.
    ///
    /// Note that not finding a snapshot in `snapshots` doesn't imply
    /// that the table isn't materialized just that the table might have
    /// never received any input. One is supposed to check `materialized` to
    /// determine if the table is materialized and return an error on
    /// scans if not.
    snapshots: ConsistentSnapshots,
}

impl Debug for AdHocTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdHocTable")
            .field("controller", &self.controller)
            .field("name", &self.name)
            .field("materialized", &self.materialized)
            .field("schema", &self.schema)
            .finish()
    }
}

impl AdHocTable {
    pub fn new(
        materialized: bool,
        indexed: bool,
        controller: Weak<ControllerInner>,
        input_handle: Option<Box<dyn DeCollectionHandle>>,
        name: SqlIdentifier,
        schema: Arc<Schema>,
        snapshots: ConsistentSnapshots,
    ) -> Self {
        Self {
            materialized,
            indexed,
            controller,
            input_handle,
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
        if self.input_handle.is_some() {
            TableType::Base
        } else {
            TableType::View
        }
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
            self.name.clone(),
            self.materialized,
            self.indexed,
            self.schema.clone(),
            projected_schema,
            self.snapshots.lock().await.get(&self.name).cloned(),
            projection,
            limit,
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if self
            .schema()
            .logically_equivalent_names_and_types(&input.schema())
            .is_err()
        {
            return plan_err!("Inserting query must have the same schema with the table.");
        }

        if overwrite != InsertOp::Append {
            return not_impl_err!("Overwrite not implemented for AdHocTable yet");
        }

        match &self.input_handle {
            Some(ih) => {
                let sink = Arc::new(AdHocTableSink::new(
                    self.controller.clone(),
                    self.name.clone(),
                    self.schema.clone(),
                    ih.fork()));
                Ok(Arc::new(DataSinkExec::new(
                    input,
                    sink,
                    None,
                )))
            }
            None => exec_err!("Called insert_into on a view, this is a bug in the feldera ad-hoc query implementation."),
        }
    }
}

struct AdHocTableSink {
    controller: Weak<ControllerInner>,
    name: SqlIdentifier,
    schema: Arc<Schema>,
    collection_handle: Box<dyn DeCollectionHandle>,
}

impl AdHocTableSink {
    fn new(
        controller: Weak<ControllerInner>,
        name: SqlIdentifier,
        schema: Arc<Schema>,
        collection_handle: Box<dyn DeCollectionHandle>,
    ) -> Self {
        Self {
            controller,
            name,
            schema,
            collection_handle,
        }
    }
}

impl Debug for AdHocTableSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "AdHocTableSink")
    }
}

impl DisplayAs for AdHocTableSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "AdHocTableSink")
            }
        }
    }
}

#[async_trait]
impl DataSink for AdHocTableSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let Some(controller) = self.controller.upgrade() else {
            return Ok(0);
        };

        // Generate endpoint name.
        let endpoint_name = format!("adhoc-ingress-{}-{}", self.name.name(), Uuid::new_v4());

        // Create HTTP endpoint.
        let config = AdHocInputConfig {
            name: endpoint_name.clone(),
        };
        let endpoint = AdHocInputEndpoint::new(config.clone());

        // Create endpoint config.
        let config = InputEndpointConfig {
            stream: Cow::from(self.name.to_string()),
            connector_config: ConnectorConfig {
                transport: TransportConfig::AdHocInput(config),
                format: Some(FormatConfig {
                    name: Cow::from("parquet"),
                    config: serde_json::Value::Null,
                }),
                index: None,
                output_buffer_config: Default::default(),
                max_batch_size: default_max_batch_size(),
                max_queued_records: default_max_queued_records(),
                paused: false,
                labels: vec![],
                start_after: None,
            },
        };

        // Connect endpoint.
        let endpoint_id = controller
            .add_input_endpoint(
                &endpoint_name,
                config,
                Some(Box::new(endpoint.clone()) as Box<dyn TransportInputEndpoint>),
                None,
            )
            .map_err(|e| DataFusionError::External(e.into()))?;

        let arrow_inserter = self
            .collection_handle
            .configure_arrow_deserializer(input_adhoc_arrow_serde_config().clone())
            .map_err(|e| DataFusionError::External(e.into()))?;

        // Call endpoint to complete request.
        let result = endpoint
            .complete_request(data, arrow_inserter, &self.schema)
            .instrument(info_span!("adhoc_output"))
            .await
            .map_err(|e| DataFusionError::External(e.into()));
        drop(endpoint);

        // Delete endpoint on completion/error.
        controller.disconnect_input(&endpoint_id);

        result
    }
}

struct AdHocQueryExecution {
    name: SqlIdentifier,
    materialized: bool,
    indexed: bool,
    table_schema: Arc<Schema>,
    projected_schema: Arc<Schema>,
    readers: Option<Vec<Arc<dyn SyncSerBatchReader>>>,
    projection: Option<Vec<usize>>,
    limit: usize,
    plan_properties: PlanProperties,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

impl AdHocQueryExecution {
    #[allow(clippy::too_many_arguments)]
    fn new(
        name: SqlIdentifier,
        materialized: bool,
        indexed: bool,
        table_schema: Arc<Schema>,
        projected_schema: Arc<Schema>,
        readers: Option<Vec<Arc<dyn SyncSerBatchReader>>>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        // TODO: we could do much better here by encoding our data partitioning schema
        // and using the correct equivalence properties.
        let num_partitions = readers.as_ref().map(|r| r.len()).unwrap_or(1);
        let eq_props = EquivalenceProperties::new(projected_schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(num_partitions);
        let plan_properties = PlanProperties::new(
            eq_props,
            partitioning,
            EmissionType::Both,
            Boundedness::Bounded,
        );

        Self {
            name,
            materialized,
            indexed,
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
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "AdHocQueryExecution")
    }
}

impl Debug for AdHocQueryExecution {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
            name: self.name.clone(),
            materialized: self.materialized,
            indexed: self.indexed,
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
        if !self.materialized {
            return Err(DataFusionError::Execution(
                format!("Tried to SELECT from a non-materialized source. Make sure `{}` is configured as materialized: \
use `with ('materialized' = 'true')` for tables, or `create materialized view` for views", self.name),
            ));
        }

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

        if let Some(readers) = &self.readers {
            let mut builder =
                RecordBatchReceiverStreamBuilder::new(self.projected_schema.clone(), 10);
            // Returns a single batch when the returned stream is polled
            let batch_reader = readers[partition].clone();
            let schema = self.table_schema.clone();
            let tx = builder.tx();
            let projection = self.projection.clone();

            let sas: SerdeArrowSchema =
                schema.fields().iter().as_slice().try_into().map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Unable to construct SerdeArrowSchema for the provided schema: {}.",
                        e
                    ))
                })?;

            let indexed = self.indexed;

            builder.spawn(async move {
                let mut cursor = batch_reader
                    .cursor(RecordFormat::Parquet(
                        output_adhoc_arrow_serde_config().clone(),
                    ))
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let mut insert_builder = SendableArrowBuilder::new(sas)?;

                let mut cur_batch_size = 0;

                while cursor.key_valid() {
                    while cursor.val_valid() {
                        let mut w = cursor.weight();

                        if w < 0 {
                            return Err(datafusion::common::DataFusionError::Execution("Unexpected record with negative weight encountered while processing ad-hoc query.".to_string()));
                        }

                        while w != 0 {
                            let result = if indexed {
                                cursor
                                    .serialize_val_to_arrow(&mut insert_builder.builder)
                            } else {
                                cursor
                                    .serialize_key_to_arrow(&mut insert_builder.builder)
                            };

                            result.map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "Unable to serialize record to arrow: {}",
                                        e
                                    ))
                                })?;
                            cur_batch_size += 1;
                            w -= 1;

                            const MAX_BATCH_SIZE: usize = 256;
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
                        cursor.step_val();
                    }
                    cursor.step_key();
                }

                let batch = insert_builder.builder.to_record_batch().map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Unable to convert ArrayBuilder to RecordBatch: {}",
                        e
                    ))
                })?;
                if batch.num_rows() > 0 {
                    send_batch(&tx, &projection, batch).await?;
                }
                Ok(())
            });

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.projected_schema.clone(),
                builder.build(),
            )))
        } else {
            // The case of no readers can happen if the table has never received any input &
            // the circuit has never stepped so the correct response is to send an empty batch
            let fut =
                futures::future::ready(Ok(RecordBatch::new_empty(self.projected_schema.clone())));
            let stream = futures::stream::once(fut);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.projected_schema.clone(),
                stream,
            )))
        }
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
