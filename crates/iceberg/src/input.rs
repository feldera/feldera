use crate::iceberg_input_serde_config;
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use chrono::{DateTime, Utc};
use datafusion::{
    arrow::array::AsArray,
    prelude::{DataFrame, SQLOptions, SessionContext},
};
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::{
    catalog::{ArrowStream, InputCollectionHandle},
    errors::journal::ControllerError,
    format::ParseError,
    transport::{
        InputConsumer, InputEndpoint, InputQueue, InputReader, InputReaderCommand,
        IntegratedInputEndpoint, NonFtInputReaderCommand,
    },
    utils::datafusion::{
        execute_query_collect, execute_singleton_query, timestamp_to_sql_expression,
        validate_sql_expression, validate_timestamp_column,
    },
    PipelineState,
};
use feldera_types::{
    config::FtModel,
    program_schema::Relation,
    transport::iceberg::{IcebergCatalogType, IcebergReaderConfig},
};
use futures_util::StreamExt;
use iceberg::{io::FileIO, spec::TableMetadata, table::Table as IcebergTable, Catalog, TableIdent};
use iceberg_catalog_glue::{
    GlueCatalog, GlueCatalogConfig, AWS_ACCESS_KEY_ID, AWS_PROFILE_NAME, AWS_REGION_NAME,
    AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_datafusion::IcebergTableProvider;
use log::{debug, info, trace};
use std::{sync::Arc, thread};
use tokio::{
    select,
    sync::{
        mpsc,
        watch::{channel, Receiver, Sender},
    },
};

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
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        Self {
            inner: Arc::new(IcebergInputEndpointInner::new(
                endpoint_name,
                config.clone(),
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

        if endpoint.config.follow() {
            bail!("'{}' mode is not yet supported", endpoint.config.mode);
        }

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
    queue: InputQueue,
}

impl IcebergInputEndpointInner {
    fn new(
        endpoint_name: &str,
        config: IcebergReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        let queue = InputQueue::new(consumer.clone());
        Self {
            endpoint_name: endpoint_name.to_string(),
            config,
            consumer,
            datafusion: SessionContext::new(),
            queue,
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

        let min = bounds[0]
            .column(0)
            .as_string_opt::<i32>()
            .ok_or_else(|| anyhow!("internal error: cannot retrieve the output of query '{bounds_query}' as a string"))?
            .value(0)
            .to_string();

        let max = bounds[0].column(1).as_string::<i32>().value(0).to_string();

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

        self.consumer.eoi();
    }

    /// Open existing iceberg table.  Use snapshot id or timestamp specified in the configuration, if any.
    async fn open_table(&self) -> Result<IcebergTable, ControllerError> {
        debug!("iceberg {}: opening iceberg table", &self.endpoint_name);

        match self.config.catalog_type {
            None => self.open_table_no_catalog().await,
            Some(IcebergCatalogType::Glue) => self.open_table_glue().await,
            Some(IcebergCatalogType::Rest) => self.open_table_rest().await,
        }

        // // TODO: Validate that table schema matches relation schema

        // // TODO: Validate that timestamp is a valid column.
    }

    async fn open_table_no_catalog(&self) -> Result<IcebergTable, ControllerError> {
        // Safe due to checks in 'validate_catalog_config'.
        let metadata_location = self.config.metadata_location.as_ref().unwrap();

        let file_io = FileIO::from_path(metadata_location)
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("invalid 'metadata_location' value: {e}"),
                )
            })?
            .with_props(&self.config.fileio_config)
            .build()
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("invalid storage configuration: {e}"),
                )
            })?;

        let metadata_file = file_io.new_input(metadata_location).map_err(|e| {
            ControllerError::invalid_transport_configuration(
                &self.endpoint_name,
                &format!("error opening metadata file at '{metadata_location}': {e}"),
            )
        })?;
        let metadata_content = metadata_file.read().await.map_err(|e| {
            ControllerError::invalid_transport_configuration(
                &self.endpoint_name,
                &format!("error reading metadatafile '{metadata_location}': {e}"),
            )
        })?;
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_content).map_err(|e| {
            ControllerError::invalid_transport_configuration(
                &self.endpoint_name,
                &format!("error parsing table metadata: {e}"),
            )
        })?;

        let table_ident = TableIdent::from_strs(["default", "table"]).unwrap();

        IcebergTable::builder()
            .file_io(file_io)
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(table_ident)
            .build()
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("error configuring Iceberg table: {e}"),
                )
            })
    }

    async fn open_table_glue(&self) -> Result<IcebergTable, ControllerError> {
        let builder = GlueCatalogConfig::builder().warehouse(
            self.config
                .glue_catalog_config
                .warehouse
                .as_ref()
                .unwrap()
                .clone(),
        );

        let builder = builder.catalog_id_opt(self.config.glue_catalog_config.id.clone());
        let builder = builder.uri_opt(self.config.glue_catalog_config.endpoint.clone());

        let mut props = self.config.fileio_config.clone();

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

        let builder = builder.props(props);

        let catalog_config = builder.build();

        let catalog = GlueCatalog::new(catalog_config).await.map_err(|e| {
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
        let builder = RestCatalogConfig::builder().uri(
            self.config
                .rest_catalog_config
                .uri
                .as_ref()
                .unwrap()
                .clone(),
        );

        let builder = builder.warehouse_opt(self.config.rest_catalog_config.warehouse.clone());

        let mut props = self.config.fileio_config.clone();

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

        let builder = builder.props(props);

        let catalog_config = builder.build();

        let catalog = RestCatalog::new(catalog_config);

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
                IcebergTableProvider::try_new_from_table_snapshot(table.clone(), snapshot_id).await
            }
            None => IcebergTableProvider::try_new_from_table(table.clone()).await,
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

        self.execute_df(df, true, &descr, input_stream, receiver)
            .await;
    }

    /// Execute a prepared dataframe and push data from it to the circuit.
    ///
    /// * `polarity` - determines whether records in the dataframe should be
    ///   inserted to or deleted from the table.
    ///
    /// * `descr` - dataframe description used to construct error message.
    ///
    /// * `input_stream` - handle to push updates to.
    ///
    /// * `receiver` - used to block the function until the endpoint is unpaused.
    async fn execute_df(
        &self,
        dataframe: DataFrame,
        polarity: bool,
        descr: &str,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        wait_running(receiver).await;

        let mut stream = match dataframe.execute_stream().await {
            Err(e) => {
                self.consumer
                    .error(true, anyhow!("error retrieving {descr}: {e:?}"), None);
                return;
            }
            Ok(stream) => stream,
        };

        let mut num_batches = 0;

        // Use the timestamp when we start retrieving the next batch as the ingestion timestamp.
        let mut timestamp = Utc::now();

        while let Some(batch) = stream.next().await {
            wait_running(receiver).await;

            let batch = match batch {
                Ok(batch) => batch,
                Err(e) => {
                    self.consumer.error(
                        false,
                        anyhow!("error retrieving batch {num_batches} of {descr}: {e:?}"),
                        Some("iceberg-batch"),
                    );
                    continue;
                }
            };
            // info!("schema: {}", batch.schema());
            num_batches += 1;
            let result = if polarity {
                input_stream.insert(&batch, &None)
            } else {
                input_stream.delete(&batch, &None)
            };
            let errors = result.map_or_else(
                |e| {
                    vec![ParseError::bin_envelope_error(
                        format!("error deserializing table records from Parquet data: {e}"),
                        &[],
                        None,
                    )]
                },
                |()| Vec::new(),
            );
            self.queue
                .push((input_stream.take_all(), errors), timestamp);

            timestamp = Utc::now();
        }
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
