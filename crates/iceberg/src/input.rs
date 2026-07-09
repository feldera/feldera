use crate::iceberg_input_serde_config;
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use atomic::Atomic;
use bytemuck::NoUninit;
use chrono::{DateTime, Utc};
use datafusion::common::arrow::array::RecordBatch;
use datafusion::prelude::{DataFrame, SQLOptions, SessionContext};
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::{
    catalog::{ArrowStream, InputCollectionHandle},
    errors::journal::ControllerError,
    format::{InputBuffer, ParseError},
    metrics::{ConnectorMetrics, ValueType},
    transport::{
        InputConsumer, InputEndpoint, InputQueue, InputQueueEntry, InputReader, InputReaderCommand,
        IntegratedInputEndpoint, NonFtInputReaderCommand,
    },
    utils::backoff::calculate_backoff_delay,
    utils::datafusion::{
        array_to_string, create_session_context, execute_query_collect, execute_singleton_query,
        timestamp_to_sql_expression, validate_sql_expression, validate_timestamp_column,
    },
    utils::job_queue::JobQueue,
    PipelineState,
};
use feldera_types::adapter_stats::ConnectorHealth;
use feldera_types::{
    config::{FtModel, PipelineConfig},
    program_schema::Relation,
    transport::iceberg::{IcebergCatalogType, IcebergReaderConfig, IcebergTransactionMode},
};
use futures_util::StreamExt;
use iceberg::CatalogBuilder;
use iceberg::{
    io::{FileIO, FileIOBuilder, StorageFactory},
    spec::SnapshotRef,
    table::{StaticTable, Table as IcebergTable},
    Catalog, TableIdent,
};
use iceberg_catalog_glue::{
    GlueCatalogBuilder, AWS_ACCESS_KEY_ID, AWS_PROFILE_NAME, AWS_REGION_NAME,
    AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, GLUE_CATALOG_PROP_CATALOG_ID, GLUE_CATALOG_PROP_URI,
    GLUE_CATALOG_PROP_WAREHOUSE,
};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_catalog_s3tables::{
    S3TablesCatalogBuilder, S3TABLES_CATALOG_PROP_ENDPOINT_URL,
    S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN,
};
use iceberg_datafusion::IcebergStaticTableProvider;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use log::{debug, info, trace, warn};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{sync::Arc, thread};
use tokio::{
    select,
    sync::{
        mpsc,
        watch::{channel, Receiver, Sender},
    },
    time::sleep,
};
use url::Url;

/// Storage backend for object stores, picked per path from its scheme
/// (`s3`/`s3a`/`gs`/`memory`/...). Used for catalogs and remote tables.
fn storage_factory() -> Arc<dyn StorageFactory> {
    Arc::new(OpenDalResolvingStorageFactory::new())
}

// `iceberg-catalog-s3tables` reads AWS credentials for its S3 Tables API client
// from these property keys but does not re-export them (they live in the crate's
// private `utils` module). Mirror them here. See
// crates/catalog/s3tables/src/utils.rs in the iceberg-rust fork.
const S3TABLES_PROP_ACCESS_KEY_ID: &str = "aws_access_key_id";
const S3TABLES_PROP_SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
const S3TABLES_PROP_SESSION_TOKEN: &str = "aws_session_token";
const S3TABLES_PROP_PROFILE_NAME: &str = "profile_name";
const S3TABLES_PROP_REGION_NAME: &str = "region_name";

/// Current phase of an Iceberg table input connector.
// repr(u64) so the phase can live in an `Atomic<IcebergPhase>` gauge and read
// out as an f64 metric.
#[derive(Copy, Clone, NoUninit)]
#[repr(u64)]
enum IcebergPhase {
    LoadingSnapshot = 0,
    // Reserved so the gauge encoding stays stable when follow mode adds a
    // streaming phase; see #6165.
    #[allow(dead_code)]
    Follow = 1,
    Completed = 2,
}

/// Prometheus-style metrics exported by the Iceberg input connector.
// TODO(#6165): follow-mode metrics land with follow mode.
struct IcebergMetrics {
    /// Current phase of the connector (see [`IcebergPhase`]).
    phase: Atomic<IcebergPhase>,
    /// Unix epoch seconds when the snapshot phase finished; 0 if not yet complete.
    snapshot_completed_ts: AtomicU64,
    /// Total records loaded during the snapshot phase.
    snapshot_records_total: AtomicU64,
    /// Number of Feldera snapshot transactions started by this connector.
    snapshot_transaction_starts: AtomicU64,
    /// Sequence number of the ingested Iceberg snapshot;
    /// [`SEQUENCE_METRIC_UNSET`] until the snapshot has been read.
    last_ingested_sequence_number: AtomicU64,
}

/// Sentinel stored in the sequence-number gauge before a value is available.
const SEQUENCE_METRIC_UNSET: u64 = u64::MAX;

impl IcebergMetrics {
    fn new() -> Self {
        Self {
            phase: Atomic::new(IcebergPhase::LoadingSnapshot),
            snapshot_completed_ts: AtomicU64::new(0),
            snapshot_records_total: AtomicU64::new(0),
            snapshot_transaction_starts: AtomicU64::new(0),
            last_ingested_sequence_number: AtomicU64::new(SEQUENCE_METRIC_UNSET),
        }
    }

    fn set_phase(&self, phase: IcebergPhase) {
        self.phase.store(phase, Ordering::Relaxed);
    }

    fn set_last_ingested_sequence_number(&self, sequence_number: i64) {
        debug_assert!(
            sequence_number >= 0,
            "Iceberg sequence number must be non-negative"
        );
        self.last_ingested_sequence_number
            .store(sequence_number as u64, Ordering::Relaxed);
    }

    fn last_ingested_sequence_number_metric(&self) -> f64 {
        match self.last_ingested_sequence_number.load(Ordering::Relaxed) {
            SEQUENCE_METRIC_UNSET => -1.0,
            sequence_number => sequence_number as f64,
        }
    }
}

impl ConnectorMetrics for IcebergMetrics {
    fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
        vec![
            (
                "input_connector_iceberg_phase",
                "Current phase: 0=loading_snapshot, 2=completed (1 reserved for follow mode).",
                ValueType::Gauge,
                self.phase.load(Ordering::Relaxed) as u64 as f64,
            ),
            (
                "input_connector_iceberg_snapshot_completed_seconds",
                "Unix epoch seconds when the snapshot phase finished (0 if not yet complete).",
                ValueType::Gauge,
                self.snapshot_completed_ts.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_iceberg_snapshot_records_total",
                "Total records loaded during the snapshot phase.",
                ValueType::Counter,
                self.snapshot_records_total.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_iceberg_snapshot_transaction_starts",
                "Number of Feldera snapshot transactions started by this connector.",
                ValueType::Counter,
                self.snapshot_transaction_starts.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_iceberg_last_ingested_sequence_number",
                "Sequence number of the Iceberg snapshot ingested by this connector (-1 if none yet).",
                ValueType::Gauge,
                self.last_ingested_sequence_number_metric(),
            ),
        ]
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

enum SnapshotDescr {
    /// Open the latest snapshot (default)
    Latest,
    /// Open specific snapshot id.
    SnapshotId(i64),
    /// Open
    Timestamp(DateTime<Utc>),
}

/// Integrated input connector that reads from an Iceberg table.
pub struct IcebergInputEndpoint {
    inner: Arc<IcebergInputEndpointInner>,
}

impl IcebergInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &IcebergReaderConfig,
        pipeline_config: &PipelineConfig,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        Self {
            inner: Arc::new(IcebergInputEndpointInner::new(
                endpoint_name,
                config.clone(),
                pipeline_config,
                runtime_env,
                consumer,
            )),
        }
    }
}

impl InputEndpoint for IcebergInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        None
    }
}

impl IntegratedInputEndpoint for IcebergInputEndpoint {
    fn open(
        self: Box<Self>,
        input_handle: &InputCollectionHandle,
        _seek: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(IcebergInputReader::new(
            &self.inner,
            input_handle,
        )?))
    }
}

struct IcebergInputReader {
    sender: Sender<PipelineState>,
    inner: Arc<IcebergInputEndpointInner>,
}

impl IcebergInputReader {
    fn new(
        endpoint: &Arc<IcebergInputEndpointInner>,
        input_handle: &InputCollectionHandle,
    ) -> AnyResult<Self> {
        // TODO: perform validation as part of config deserialization.
        endpoint
            .config
            .validate_catalog_config()
            .map_err(|e| anyhow!(e))?;

        if endpoint.config.num_parsers == 0 {
            bail!("invalid Iceberg connector configuration: 'num_parsers' must be greater than 0");
        }

        if endpoint.config.follow() {
            bail!("'{}' mode is not yet supported", endpoint.config.mode);
        }

        // Register metrics here rather than at endpoint construction: the
        // controller inserts this endpoint's status entry after constructing the
        // endpoint but before calling `open` (which builds this reader), and
        // `set_custom_metrics` is dropped if the status entry does not yet exist.
        endpoint
            .consumer
            .set_custom_metrics(Arc::clone(&endpoint.metrics) as Arc<dyn ConnectorMetrics>);

        let (sender, receiver) = channel(PipelineState::Paused);
        let endpoint_clone = endpoint.clone();
        let receiver_clone = receiver.clone();

        // Used to communicate the status of connector initialization.
        let (init_status_sender, mut init_status_receiver) =
            mpsc::channel::<Result<(), ControllerError>>(1);

        let input_stream = input_handle
            .handle
            .configure_arrow_deserializer(iceberg_input_serde_config())?;
        let schema = input_handle.schema.clone();

        thread::Builder::new()
            .name("iceberg-input-tokio-wrapper".to_string())
            .spawn(move || {
                TOKIO.block_on(async {
                    let _ = endpoint_clone
                        .worker_task(input_stream, schema, receiver_clone, init_status_sender)
                        .await;
                })
            })
            .expect("failed to spawn iceberg-input tokio wrapper thread");

        init_status_receiver.blocking_recv().ok_or_else(|| {
            anyhow!("worker thread terminated unexpectedly during initialization")
        })??;

        Ok(Self {
            sender,
            inner: endpoint.clone(),
        })
    }
}

impl InputReader for IcebergInputReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        match command.as_nonft().unwrap() {
            NonFtInputReaderCommand::Queue => self.inner.queue.queue(),
            NonFtInputReaderCommand::Transition(state) => drop(self.sender.send_replace(state)),
        }
    }

    fn is_closed(&self) -> bool {
        self.inner.queue.is_empty() && self.sender.is_closed()
    }
}

impl Drop for IcebergInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

struct IcebergInputEndpointInner {
    endpoint_name: String,
    config: IcebergReaderConfig,
    consumer: Box<dyn InputConsumer>,
    datafusion: SessionContext,
    queue: Arc<InputQueue>,
    /// Monotonic counter used to label snapshot transactions for observability.
    transaction_index: AtomicUsize,
    metrics: Arc<IcebergMetrics>,
}

impl IcebergInputEndpointInner {
    fn new(
        endpoint_name: &str,
        config: IcebergReaderConfig,
        pipeline_config: &PipelineConfig,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        // Share the pipeline-wide `RuntimeEnv` so that scans against the
        // iceberg table spill to the bounded memory pool and on-disk scratch
        // dir alongside every other datafusion user in the pipeline.
        let datafusion = create_session_context(pipeline_config, runtime_env);

        // Note: metrics are registered with the consumer in `IcebergInputReader::new`
        // (the `open` path), not here. The controller inserts this endpoint's status
        // entry only after constructing the endpoint but before `open`, and
        // `set_custom_metrics` is silently dropped if the status entry does not yet
        // exist.
        let metrics = Arc::new(IcebergMetrics::new());

        Self {
            endpoint_name: endpoint_name.to_string(),
            config,
            consumer,
            datafusion,
            queue,
            transaction_index: AtomicUsize::new(0),
            metrics,
        }
    }

    /// Allocate a transaction for the next snapshot chunk.
    ///
    /// Returns `None` when `transaction_mode` is `none`, meaning the chunk is not
    /// wrapped in a Feldera transaction. Otherwise returns `Some(Some(label))`,
    /// where the label identifies the transaction in logs and metrics.
    fn allocate_snapshot_transaction(&self) -> Option<Option<String>> {
        match self.config.transaction_mode {
            IcebergTransactionMode::None => None,
            IcebergTransactionMode::Snapshot => {
                let index = self.transaction_index.fetch_add(1, Ordering::AcqRel);
                Some(Some(format!("snapshot-{index}")))
            }
        }
    }

    fn table_ident(&self) -> Option<Result<TableIdent, ControllerError>> {
        self.config.table_name.as_ref().map(|table_name| {
            TableIdent::from_strs(table_name.split('.')).map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("'table_name' property specifies an invalid Iceberg table name: {e}"),
                )
            })
        })
    }

    fn snapshot_descr(&self) -> Result<SnapshotDescr, ControllerError> {
        match &self.config {
            IcebergReaderConfig {
                snapshot_id: Some(_),
                datetime: Some(_),
                ..
            } => Err(ControllerError::invalid_transport_configuration(
                &self.endpoint_name,
                "at most one of 'snapshot_id' and 'datetime' options can be specified",
            )),
            IcebergReaderConfig {
                snapshot_id: None,
                datetime: None,
                ..
            } => Ok(SnapshotDescr::Latest),
            IcebergReaderConfig {
                snapshot_id: Some(snapshot_id),
                datetime: None,
                ..
            } => Ok(SnapshotDescr::SnapshotId(*snapshot_id)),
            IcebergReaderConfig {
                snapshot_id: None,
                datetime: Some(datetime),
                ..
            } => {
                let ts = DateTime::parse_from_rfc3339(datetime)
                    .map_err(|e| {
                        ControllerError::invalid_transport_configuration(
                            &self.endpoint_name,
                            &format!(
                        "invalid 'datetime' format (expected ISO-8601/RFC-3339 timestamp): {e}"
                    ),
                        )
                    })?
                    .to_utc();
                Ok(SnapshotDescr::Timestamp(ts))
            }
        }
    }

    async fn worker_task(
        self: Arc<Self>,
        input_stream: Box<dyn ArrowStream>,
        schema: Relation,
        receiver: Receiver<PipelineState>,
        init_status_sender: mpsc::Sender<Result<(), ControllerError>>,
    ) {
        let mut receiver_clone = receiver.clone();
        select! {
            _ = Self::worker_task_inner(self.clone(), input_stream, schema, receiver, init_status_sender) => {
                debug!("iceberg {}: worker task terminated",
                    &self.endpoint_name,
                );
            }
            _ = receiver_clone.wait_for(|state| state == &PipelineState::Terminated) => {
                debug!("iceberg {}: received termination command; worker task canceled",
                    &self.endpoint_name,
                );
            }
        }
    }

    /// Load the entire table snapshot as a single "select * where <filter>" query.
    async fn read_unordered_snapshot(
        &self,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        // Execute the snapshot query; push snapshot data to the circuit.
        info!("iceberg {}: reading initial snapshot", &self.endpoint_name,);

        let mut snapshot_query = "select * from snapshot".to_string();
        if let Some(filter) = &self.config.snapshot_filter {
            snapshot_query = format!("{snapshot_query} where {filter}");
        }

        self.execute_snapshot_query(&snapshot_query, "initial snapshot", input_stream, receiver)
            .await;

        //let _ = self.datafusion.deregister_table("snapshot");
        info!(
            "iceberg {}: finished reading initial snapshot",
            &self.endpoint_name,
        );
    }

    async fn read_ordered_snapshot(
        &self,
        input_stream: &mut dyn ArrowStream,
        schema: &Relation,
        receiver: &mut Receiver<PipelineState>,
    ) {
        self.read_ordered_snapshot_inner(input_stream, schema, receiver)
            .await
            .unwrap_or_else(|e| self.consumer.error(true, e, None));
    }

    async fn read_ordered_snapshot_inner(
        &self,
        input_stream: &mut dyn ArrowStream,
        schema: &Relation,
        receiver: &mut Receiver<PipelineState>,
    ) -> Result<(), AnyError> {
        let timestamp_column = self.config.timestamp_column.as_ref().unwrap();

        let timestamp_field = schema.field(timestamp_column).unwrap();

        // The following unwraps are safe, as validated in `validate_timestamp_column`.
        let lateness = timestamp_field.lateness.as_ref().unwrap();

        // Query the table for min and max values of the timestamp column that satisfy the filter.
        let bounds_query =
            format!("select * from (select cast(min({timestamp_column}) as string) as start_ts, cast(max({timestamp_column}) as string) as end_ts from snapshot {}) where start_ts is not null",
            if let Some(filter) = &self.config.snapshot_filter {
                format!("where {filter}")
            } else {
                String::new()
            });

        let bounds = execute_query_collect(&self.datafusion, &bounds_query).await?;

        info!(
            "iceberg {}: querying the table for min and max timestamp values",
            &self.endpoint_name,
        );

        if bounds.len() != 1 || bounds[0].num_rows() != 1 {
            info!(
                "iceberg {}: initial snapshot is empty; the Delta table contains no records{}",
                &self.endpoint_name,
                if let Some(filter) = &self.config.snapshot_filter {
                    format!(" that satisfy the filter condition '{filter}'")
                } else {
                    String::new()
                }
            );
            return Ok(());
        }

        if bounds[0].num_columns() != 2 {
            // Should never happen.
            return Err(anyhow!(
                    "internal error: query '{bounds_query}' returned a result with {} columns; expected 2 columns",
                    bounds[0].num_columns()
                ));
        }

        let min = array_to_string(bounds[0].column(0)).ok_or_else(|| {
            anyhow!(
                "internal error: cannot retrieve the first column in the output of query '{bounds_query}' as a string"
            )
        })?;

        let max = array_to_string(bounds[0].column(1)).ok_or_else(|| {
            anyhow!(
                "internal error: cannot retrieve the second column in the output of query '{bounds_query}' as a string"
            )
        })?;

        info!(
            "iceberg {}: reading table snapshot in the range '{min} <= {timestamp_column} <= {max}'",
            &self.endpoint_name,
        );

        let min = timestamp_to_sql_expression(&timestamp_field.columntype, &min);
        let max = timestamp_to_sql_expression(&timestamp_field.columntype, &max);

        let mut start = min.clone();
        let mut done = "false".to_string();

        while &done != "true" {
            // Evaluate SQL expression for the new end of the interval.
            let end = execute_singleton_query(
                &self.datafusion,
                &format!("select cast(({start} + {lateness}) as string)"),
            )
            .await?;
            let end = timestamp_to_sql_expression(&timestamp_field.columntype, &end);

            // Query the table for the range.
            let mut range_query =
                format!("select * from snapshot where {timestamp_column} >= {start} and {timestamp_column} < {end}");
            if let Some(filter) = &self.config.snapshot_filter {
                range_query = format!("{range_query} and {filter}");
            }

            self.execute_snapshot_query(&range_query, "range", input_stream, receiver)
                .await;

            start = end.clone();

            done = execute_singleton_query(
                &self.datafusion,
                &format!("select cast({start} > {max} as string)"),
            )
            .await?;
        }

        Ok(())
    }

    async fn worker_task_inner(
        self: Arc<Self>,
        mut input_stream: Box<dyn ArrowStream>,
        schema: Relation,
        mut receiver: Receiver<PipelineState>,
        init_status_sender: mpsc::Sender<Result<(), ControllerError>>,
    ) {
        let table = match self.open_table().await {
            Err(e) => {
                let _ = init_status_sender.send(Err(e)).await;
                return;
            }
            Ok(table) => table,
        };

        let table = Arc::new(table);

        if let Err(e) = self.prepare_snapshot_query(&table, &schema).await {
            let _ = init_status_sender.send(Err(e)).await;
            return;
        };

        // Code before this point is part of endpoint initialization.
        // After this point, the thread should continue running until it receives a
        // shutdown command from the controller.
        let _ = init_status_sender.send(Ok(())).await;

        if self.config.snapshot() && self.config.timestamp_column.is_none() {
            // Read snapshot chunk-by-chunk.
            self.read_unordered_snapshot(input_stream.as_mut(), &mut receiver)
                .await;
        } else if self.config.snapshot() {
            // Read the entire snapshot in one query.
            self.read_ordered_snapshot(input_stream.as_mut(), &schema, &mut receiver)
                .await;
        };

        if self.config.snapshot() {
            self.metrics
                .snapshot_completed_ts
                .store(now_unix_secs(), Ordering::Relaxed);
            if let Some(snapshot) = self.ingested_snapshot(&table) {
                self.metrics
                    .set_last_ingested_sequence_number(snapshot.sequence_number());
                info!(
                    "iceberg {}: ingested snapshot {} (sequence number {})",
                    &self.endpoint_name,
                    snapshot.snapshot_id(),
                    snapshot.sequence_number(),
                );
            }
        }

        // Snapshot-only connector: nothing follows the snapshot, so the
        // connector is done once the snapshot has been read.
        self.metrics.set_phase(IcebergPhase::Completed);

        self.consumer.eoi();
    }

    /// Open existing iceberg table.  Use snapshot id or timestamp specified in the configuration, if any.
    async fn open_table(&self) -> Result<IcebergTable, ControllerError> {
        debug!("iceberg {}: opening iceberg table", &self.endpoint_name);

        match self.config.catalog_type {
            None => self.open_table_no_catalog().await,
            Some(IcebergCatalogType::Glue) => self.open_table_glue().await,
            Some(IcebergCatalogType::Rest) => self.open_table_rest().await,
            Some(IcebergCatalogType::S3Tables) => self.open_table_s3tables().await,
        }

        // // TODO: Validate that table schema matches relation schema

        // // TODO: Validate that timestamp is a valid column.
    }

    async fn open_table_no_catalog(&self) -> Result<IcebergTable, ControllerError> {
        // Safe due to checks in 'validate_catalog_config'.
        let metadata_location = self.config.metadata_location.as_ref().unwrap();

        // Object stores (a URL with a non-`file` scheme) need the
        // scheme-resolving factory and its props (credentials, region).
        let file_io = match Url::parse(metadata_location) {
            Ok(url) if url.scheme() != "file" => FileIOBuilder::new(storage_factory())
                .with_props(&self.config.fileio_config)
                .build(),
            // Local table: a `file://` URL or a bare path. The factory can't
            // read a bare path (it URL-parses every path, and e.g.
            // `/tmp/t/metadata.json` has no scheme), so use the plain
            // filesystem reader, which takes the string as a file path.
            _ => FileIO::new_with_fs(),
        };

        // `StaticTable` loads the metadata read-only and wires up the current
        // tokio runtime for us. (Glue/REST get their table from the catalog.)
        let table_ident = TableIdent::from_strs(["default", "table"]).unwrap();
        let table = StaticTable::from_metadata_file(metadata_location, table_ident, file_io)
            .await
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("error opening Iceberg table at '{metadata_location}': {e}"),
                )
            })?;

        Ok(table.into_table())
    }

    async fn open_table_glue(&self) -> Result<IcebergTable, ControllerError> {
        let mut props = self.config.fileio_config.clone();

        props.insert(
            GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
            self.config
                .glue_catalog_config
                .warehouse
                .as_ref()
                .unwrap()
                .clone(),
        );

        if let Some(id) = self.config.glue_catalog_config.id.as_ref() {
            props.insert(GLUE_CATALOG_PROP_CATALOG_ID.to_string(), id.clone());
        }

        if let Some(endpoint) = self.config.glue_catalog_config.endpoint.as_ref() {
            props.insert(GLUE_CATALOG_PROP_URI.to_string(), endpoint.clone());
        }

        self.config
            .glue_catalog_config
            .access_key_id
            .as_ref()
            .map(|aws_access_key_id| {
                props.insert(AWS_ACCESS_KEY_ID.to_string(), aws_access_key_id.clone())
            });

        self.config
            .glue_catalog_config
            .secret_access_key
            .as_ref()
            .map(|aws_secret_access_key| {
                props.insert(
                    AWS_SECRET_ACCESS_KEY.to_string(),
                    aws_secret_access_key.clone(),
                )
            });

        self.config
            .glue_catalog_config
            .session_token
            .as_ref()
            .map(|session_token| {
                props.insert(AWS_SESSION_TOKEN.to_string(), session_token.clone())
            });

        self.config
            .glue_catalog_config
            .profile_name
            .as_ref()
            .map(|profile_name| props.insert(AWS_PROFILE_NAME.to_string(), profile_name.clone()));

        self.config
            .glue_catalog_config
            .region
            .as_ref()
            .map(|region_name| props.insert(AWS_REGION_NAME.to_string(), region_name.clone()));

        let catalog = GlueCatalogBuilder::default()
            .with_storage_factory(storage_factory())
            .load("glue".to_string(), props)
            .await
            .map_err(|e| {
                ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("error creating Glue catalog client: {e}"),
                )
            })?;

        let table_ident = self.table_ident().unwrap()?;

        catalog.load_table(&table_ident).await.map_err(|e| {
            ControllerError::input_transport_error(
                &self.endpoint_name,
                true,
                anyhow!("error loading Iceberg table: {e}"),
            )
        })
    }

    async fn open_table_rest(&self) -> Result<IcebergTable, ControllerError> {
        let mut props = self.config.fileio_config.clone();

        props.insert(
            REST_CATALOG_PROP_URI.to_string(),
            self.config
                .rest_catalog_config
                .uri
                .as_ref()
                .unwrap()
                .clone(),
        );

        if let Some(warehouse) = self.config.rest_catalog_config.warehouse.as_ref() {
            props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
        }

        self.config
            .rest_catalog_config
            .audience
            .as_ref()
            .map(|audience| props.insert("audience".to_string(), audience.clone()));

        self.config
            .rest_catalog_config
            .resource
            .as_ref()
            .map(|resource| props.insert("resource".to_string(), resource.clone()));

        self.config
            .rest_catalog_config
            .credential
            .as_ref()
            .map(|credential| props.insert("credential".to_string(), credential.clone()));

        self.config
            .rest_catalog_config
            .oauth2_server_uri
            .as_ref()
            .map(|oauth2_server_uri| {
                props.insert("oauth2-server-uri".to_string(), oauth2_server_uri.clone())
            });

        self.config
            .rest_catalog_config
            .prefix
            .as_ref()
            .map(|prefix| props.insert("prefix".to_string(), prefix.clone()));

        self.config
            .rest_catalog_config
            .scope
            .as_ref()
            .map(|scope| props.insert("scope".to_string(), scope.clone()));

        self.config
            .rest_catalog_config
            .token
            .as_ref()
            .map(|token| props.insert("token".to_string(), token.clone()));

        if let Some(headers) = &self.config.rest_catalog_config.headers {
            for (header, val) in headers.iter() {
                props.insert(format!("header.{header}"), val.clone());
            }
        };

        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(storage_factory())
            .load("rest".to_string(), props)
            .await
            .map_err(|e| {
                ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("error creating Rest catalog client: {e}"),
                )
            })?;

        let table_ident = self.table_ident().unwrap()?;

        catalog.load_table(&table_ident).await.map_err(|e| {
            ControllerError::input_transport_error(
                &self.endpoint_name,
                true,
                anyhow!("error loading Iceberg table: {e}"),
            )
        })
    }

    async fn open_table_s3tables(&self) -> Result<IcebergTable, ControllerError> {
        let mut props = self.config.fileio_config.clone();

        // Safe due to checks in 'validate_catalog_config'.
        props.insert(
            S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
            self.config
                .s3tables_catalog_config
                .table_bucket_arn
                .as_ref()
                .unwrap()
                .clone(),
        );

        if let Some(endpoint) = self.config.s3tables_catalog_config.endpoint.as_ref() {
            props.insert(
                S3TABLES_CATALOG_PROP_ENDPOINT_URL.to_string(),
                endpoint.clone(),
            );
        }

        // Credentials for the S3 Tables API client. These use a different key
        // namespace than the `s3.*` `fileio_config` keys that authenticate the
        // FileIO used to read the table's data files, so both can coexist in the
        // same property map (each consumer ignores the other's keys).
        self.config
            .s3tables_catalog_config
            .access_key_id
            .as_ref()
            .map(|aws_access_key_id| {
                props.insert(
                    S3TABLES_PROP_ACCESS_KEY_ID.to_string(),
                    aws_access_key_id.clone(),
                )
            });

        self.config
            .s3tables_catalog_config
            .secret_access_key
            .as_ref()
            .map(|aws_secret_access_key| {
                props.insert(
                    S3TABLES_PROP_SECRET_ACCESS_KEY.to_string(),
                    aws_secret_access_key.clone(),
                )
            });

        self.config
            .s3tables_catalog_config
            .session_token
            .as_ref()
            .map(|session_token| {
                props.insert(
                    S3TABLES_PROP_SESSION_TOKEN.to_string(),
                    session_token.clone(),
                )
            });

        self.config
            .s3tables_catalog_config
            .profile_name
            .as_ref()
            .map(|profile_name| {
                props.insert(S3TABLES_PROP_PROFILE_NAME.to_string(), profile_name.clone())
            });

        self.config
            .s3tables_catalog_config
            .region
            .as_ref()
            .map(|region_name| {
                props.insert(S3TABLES_PROP_REGION_NAME.to_string(), region_name.clone())
            });

        let catalog = S3TablesCatalogBuilder::default()
            .with_storage_factory(storage_factory())
            .load("s3tables".to_string(), props)
            .await
            .map_err(|e| {
                ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("error creating S3 Tables catalog client: {e}"),
                )
            })?;

        let table_ident = self.table_ident().unwrap()?;

        catalog.load_table(&table_ident).await.map_err(|e| {
            ControllerError::input_transport_error(
                &self.endpoint_name,
                true,
                anyhow!("error loading Iceberg table: {e}"),
            )
        })
    }

    /// Validate the filter expression specified in the 'snapshot_filter' parameter.
    fn validate_snapshot_filter(&self) -> Result<(), ControllerError> {
        if let Some(filter) = &self.config.snapshot_filter {
            validate_sql_expression(filter).map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("error parsing 'snapshot_filter' expression '{filter}': {e}"),
                )
            })?;
        }

        Ok(())
    }

    /// Prepare to read initial snapshot, if required by endpoint configuration.
    ///
    /// * register snapshot as a datafusion table
    /// * validate snapshot config: filter condition and timestamp column
    async fn prepare_snapshot_query(
        &self,
        table: &IcebergTable,
        schema: &Relation,
    ) -> Result<(), ControllerError> {
        if !self.config.snapshot() {
            return Ok(());
        }

        trace!(
            "iceberg {}: registering table with Datafusion",
            &self.endpoint_name,
        );

        let snapshot_id = match self.snapshot_descr()? {
            SnapshotDescr::SnapshotId(snapshot_id) => Some(snapshot_id),
            SnapshotDescr::Timestamp(ts) => {
                let ts_ms = ts.timestamp_millis();
                let snapshot_log = table
                    .metadata()
                    .history()
                    .iter()
                    .rev()
                    .find(|log| log.timestamp_ms() <= ts_ms);
                if let Some(snapshot_log) = snapshot_log {
                    Some(snapshot_log.snapshot_id)
                } else {
                    return Err(ControllerError::input_transport_error(
                        &self.endpoint_name,
                        true,
                        anyhow!("Iceberg connector configuration specifies timestamp {ts}; however Iceberg table does not contain a snapshot with the same or earlier timestamp"),
                    ));
                }
            }
            SnapshotDescr::Latest => None,
        };

        let provider = match snapshot_id {
            Some(snapshot_id) => {
                IcebergStaticTableProvider::try_new_from_table_snapshot(table.clone(), snapshot_id)
                    .await
            }
            None => IcebergStaticTableProvider::try_new_from_table(table.clone()).await,
        }
        .map_err(|e| {
            ControllerError::invalid_transport_configuration(
                &self.endpoint_name,
                &format!("error creating Datafusion table provider: {e}"),
            )
        })?;

        self.datafusion
            .register_table("snapshot", Arc::new(provider))
            .map_err(|e| {
                ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("failed to register table snapshot with datafusion: {e}"),
                )
            })?;

        self.validate_snapshot_filter()?;

        if let Some(timestamp_column) = &self.config.timestamp_column {
            validate_timestamp_column(
                &self.endpoint_name,
                timestamp_column,
                &self.datafusion,
                schema,
                "see Iceberg connector documentation for more details: https://docs.feldera.com/connectors/sources/iceberg"
            )
            .await?;
        };

        Ok(())
    }

    /// The Iceberg snapshot selected for ingest, resolved the same way as
    /// [`Self::snapshot_descr`]. `None` if the table has no matching snapshot.
    fn ingested_snapshot(&self, table: &IcebergTable) -> Option<SnapshotRef> {
        let metadata = table.metadata();
        let snapshot = match self.snapshot_descr().ok()? {
            SnapshotDescr::SnapshotId(snapshot_id) => metadata.snapshot_by_id(snapshot_id),
            SnapshotDescr::Timestamp(ts) => {
                let ts_ms = ts.timestamp_millis();
                let snapshot_id = metadata
                    .history()
                    .iter()
                    .rev()
                    .find(|log| log.timestamp_ms() <= ts_ms)?
                    .snapshot_id;
                metadata.snapshot_by_id(snapshot_id)
            }
            SnapshotDescr::Latest => metadata.current_snapshot(),
        };
        snapshot.cloned()
    }

    /// Execute a SQL query to load a complete or partial snapshot of the table.
    async fn execute_snapshot_query(
        &self,
        query: &str,
        descr: &str,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        let descr = format!("{descr} query '{query}'");
        debug!(
            "iceberg {}: retrieving data from the Iceberg table snapshot using {descr}",
            &self.endpoint_name,
        );

        let options: SQLOptions = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false);

        let df = match self.datafusion.sql_with_options(query, options).await {
            Ok(df) => df,
            Err(e) => {
                self.consumer
                    .error(true, anyhow!("error compiling query '{query}': {e}"), None);
                return;
            }
        };

        // Each snapshot chunk is its own Feldera transaction (or none, depending on
        // `transaction_mode`): the whole snapshot for an unordered read, one range
        // for an ordered read.
        let transaction = self.allocate_snapshot_transaction();

        // On terminal failure `execute_df` has already reported the error to the
        // consumer, which stops ingestion; nothing more to do here.
        let _ = self
            .execute_df(df, true, &descr, transaction, input_stream, receiver)
            .await;
    }

    /// Execute a prepared dataframe and push data from it to the circuit,
    /// retrying the whole dataframe on transient failures.
    ///
    /// The object-store reads underlying an Iceberg scan can fail intermittently
    /// (timeouts, throttling). Since a partially consumed dataframe stream cannot
    /// be resumed mid-flight, we retry the entire dataframe with exponential
    /// backoff. On terminal failure the error is reported to the consumer and
    /// returned.
    ///
    /// * `polarity` - determines whether records in the dataframe should be
    ///   inserted to or deleted from the table.
    ///
    /// * `descr` - dataframe description used to construct error message.
    ///
    /// * `transaction` - when `Some`, the dataframe's records are wrapped in a
    ///   Feldera transaction: entries carry the start label and a commit entry is
    ///   pushed once the dataframe completes.
    ///
    /// * `input_stream` - handle to push updates to.
    ///
    /// * `receiver` - used to block the function until the endpoint is unpaused.
    async fn execute_df(
        &self,
        dataframe: DataFrame,
        polarity: bool,
        descr: &str,
        transaction: Option<Option<String>>,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) -> Result<usize, AnyError> {
        let is_transactional = transaction.is_some();
        let max_retries = self.config.max_retries();
        let mut retry_count = 0;
        loop {
            match self
                .execute_df_inner(
                    dataframe.clone(),
                    polarity,
                    transaction.clone(),
                    input_stream,
                    receiver,
                )
                .await
            {
                Ok(total_records) => {
                    self.metrics
                        .snapshot_records_total
                        .fetch_add(total_records as u64, Ordering::Relaxed);

                    // Close the transaction once all records have been queued. The
                    // non-empty-buffer entries above start it lazily at flush time;
                    // this empty entry commits it after the last one flushes.
                    if is_transactional {
                        self.queue.push_entry(
                            InputQueueEntry::new_with_aux(Utc::now(), ())
                                .with_commit_transaction(true),
                            Vec::new(),
                        );
                    }
                    self.consumer
                        .update_connector_health(ConnectorHealth::healthy());
                    return Ok(total_records);
                }
                Err(e) => {
                    retry_count += 1;
                    if retry_count > max_retries {
                        let message =
                            format!("error retrieving {descr} after {retry_count} attempt(s): {e}");
                        self.consumer
                            .update_connector_health(ConnectorHealth::unhealthy(&message));
                        self.consumer
                            .error(true, anyhow!(message.clone()), Some("iceberg-read"));
                        return Err(anyhow!(message));
                    }
                    let backoff_delay = calculate_backoff_delay(retry_count - 1);
                    let message = format!(
                        "error retrieving {descr} after {retry_count} attempt(s): {e}; retrying in {backoff_delay:?}"
                    );
                    self.consumer
                        .update_connector_health(ConnectorHealth::unhealthy(&message));
                    warn!("iceberg {}: {message}", &self.endpoint_name);
                    sleep(backoff_delay).await;
                }
            }
        }
    }

    /// A single attempt of the `execute_df` retry loop.
    ///
    /// Record batches are parsed by a pool of `num_parsers` tasks. Parsing runs
    /// concurrently, but [`JobQueue`] preserves ordering, so parsed buffers reach
    /// the input queue in the same order the batches were read.
    async fn execute_df_inner(
        &self,
        dataframe: DataFrame,
        polarity: bool,
        transaction: Option<Option<String>>,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) -> Result<usize, String> {
        wait_running(receiver).await;

        if transaction.is_some() {
            self.metrics
                .snapshot_transaction_starts
                .fetch_add(1, Ordering::Relaxed);
        }

        let mut stream = dataframe
            .execute_stream()
            .await
            .map_err(|e| format!("{e:?}"))?;

        // The dataframe compiled and started streaming: the connector is healthy.
        self.consumer
            .update_connector_health(ConnectorHealth::healthy());

        let mut num_batches = 0;
        let mut total_records = 0usize;

        let queue = self.queue.clone();
        let num_parsers = self.config.num_parsers as usize;

        // Job queue that parses record batches on a pool of tasks and pushes the
        // resulting buffers to the input queue in enqueue order.
        let job_queue = JobQueue::<
            (RecordBatch, DateTime<Utc>),
            (Option<Box<dyn InputBuffer>>, Vec<ParseError>, DateTime<Utc>),
        >::new(
            num_parsers,
            // Both the worker closure and each per-job future need an owned
            // (`'static`) stream, so each level forks once:
            //   - the outer fork gives every worker its own stream, since the
            //     closure can't capture the borrowed `&mut input_stream`;
            //   - the inner fork produces a fresh stream to move into each job's
            //     future, since an `FnMut` can't move its captured stream out
            //     more than once.
            move || {
                let input_stream = input_stream.fork();
                Box::new(move |(batch, timestamp)| {
                    Box::pin({
                        let mut input_stream = input_stream.fork();
                        async move {
                            let (buffer, errors) =
                                Self::parse_record_batch(batch, polarity, input_stream.as_mut())
                                    .await;
                            (buffer, errors, timestamp)
                        }
                    })
                })
            },
            move |(buffer, errors, timestamp)| {
                // Setting the start label on every entry is idempotent: the input
                // queue starts the transaction on the first flushed entry and
                // ignores the label thereafter.
                queue.push_entry(
                    InputQueueEntry::new_with_aux(timestamp, ())
                        .with_buffer(buffer)
                        .with_start_transaction(transaction.clone()),
                    errors,
                );
            },
        );

        // Use the timestamp when the batch was retrieved as the ingestion timestamp.
        let mut timestamp = Utc::now();

        while let Some(batch) = stream.next().await {
            wait_running(receiver).await;
            let batch =
                batch.map_err(|e| format!("error retrieving batch {num_batches}: {e:?}"))?;
            num_batches += 1;
            total_records += batch.num_rows();
            job_queue.push_job((batch, timestamp)).await;
            timestamp = Utc::now();
        }

        job_queue.flush().await;
        Ok(total_records)
    }

    /// Parse a single record batch into an input buffer.
    async fn parse_record_batch(
        batch: RecordBatch,
        polarity: bool,
        input_stream: &mut dyn ArrowStream,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let result = if polarity {
            input_stream.insert(&batch, &None)
        } else {
            input_stream.delete(&batch, &None)
        };
        let errors = result.map_or_else(
            |e| {
                vec![ParseError::bin_envelope_error(
                    format!("error deserializing records read from the Iceberg table: {e}"),
                    &[],
                    None,
                )]
            },
            |()| Vec::new(),
        );

        (input_stream.take_all(), errors)
    }
}

/// Block until the state is `Running`.
async fn wait_running(receiver: &mut Receiver<PipelineState>) {
    // An error indicates that the channel was closed.  It's ok to ignore
    // the error as this situation will be handled by the top-level select,
    // which will abort the worker thread.
    let _ = receiver
        .wait_for(|state| state == &PipelineState::Running)
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_factory_constructs() {
        // Smoke test; scheme dispatch is covered upstream in iceberg-rust.
        let _factory = storage_factory();
    }
}
