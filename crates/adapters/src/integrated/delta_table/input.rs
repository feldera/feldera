use crate::catalog::{ArrowStream, InputCollectionHandle};
use crate::controller::{ControllerInner, EndpointId};
use crate::format::InputBuffer;
use crate::integrated::delta_table::{delta_input_serde_config, register_storage_handlers};
use crate::transport::{
    InputEndpoint, InputQueue, InputReaderCommand, IntegratedInputEndpoint, NonFtInputReaderCommand,
};
use crate::{
    ControllerError, InputConsumer, InputReader, PipelineState, RecordFormat,
    TransportInputEndpoint,
};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use datafusion::common::arrow::array::{AsArray, RecordBatch};
use datafusion::logical_expr::sqlparser::parser::ParserError;
use datafusion::prelude::SessionConfig;
use datafusion::scalar::ScalarValue;
use dbsp::circuit::tokio::TOKIO;
use dbsp::InputHandle;
use deltalake::datafusion::common::Column;
use deltalake::datafusion::dataframe::DataFrame;
use deltalake::datafusion::execution::context::SQLOptions;
use deltalake::datafusion::prelude::{Expr, ParquetReadOptions, SessionContext};
use deltalake::datafusion::sql::sqlparser::dialect::GenericDialect;
use deltalake::datafusion::sql::sqlparser::keywords::Keyword::SQL;
use deltalake::datafusion::sql::sqlparser::parser::Parser;
use deltalake::datafusion::sql::sqlparser::test_utils::table;
use deltalake::delta_datafusion::cdf::FileAction;
use deltalake::kernel::Action;
use deltalake::kernel::Error::Parse;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::table::builder::ensure_table_uri;
use deltalake::table::PeekCommit;
use deltalake::{datafusion, DeltaTable, DeltaTableBuilder, Path};
use feldera_adapterlib::format::ParseError;
use feldera_adapterlib::utils::datafusion::{
    execute_query_collect, execute_singleton_query, timestamp_to_sql_expression,
    validate_sql_expression, validate_timestamp_column,
};
use feldera_types::config::InputEndpointConfig;
use feldera_types::format::json::JsonFlavor;
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlType};
use feldera_types::transport::delta_table::{DeltaTableIngestMode, DeltaTableReaderConfig};
use feldera_types::transport::s3::S3InputConfig;
use futures::TryFutureExt;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt::format;
use std::str::FromStr;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch::{channel, Receiver, Sender};
use tokio::time::sleep;
use tokio_util::time;
use tracing::{debug, error, info, trace};
use tracing_subscriber::fmt::fmt;
use url::Url;
use utoipa::ToSchema;

/// Polling interval when following a delta table.
const POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// Polling delay before retrying an unsuccessful read from a delta log.
const RETRY_INTERVAL: Duration = Duration::from_millis(2000);

/// Integrated input connector that reads from a delta table.
pub struct DeltaTableInputEndpoint {
    inner: Arc<DeltaTableInputEndpointInner>,
}

impl DeltaTableInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &DeltaTableReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        register_storage_handlers();

        Self {
            inner: Arc::new(DeltaTableInputEndpointInner::new(
                endpoint_name,
                config.clone(),
                consumer,
            )),
        }
    }
}

impl InputEndpoint for DeltaTableInputEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        false
    }
}
impl IntegratedInputEndpoint for DeltaTableInputEndpoint {
    fn open(&self, input_handle: &InputCollectionHandle) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(DeltaTableInputReader::new(
            &self.inner,
            input_handle,
        )?))
    }
}

struct DeltaTableInputReader {
    sender: Sender<PipelineState>,
    inner: Arc<DeltaTableInputEndpointInner>,
}

impl DeltaTableInputReader {
    fn new(
        endpoint: &Arc<DeltaTableInputEndpointInner>,
        input_handle: &InputCollectionHandle,
    ) -> AnyResult<Self> {
        let (sender, receiver) = channel(PipelineState::Paused);
        let endpoint_clone = endpoint.clone();
        let receiver_clone = receiver.clone();

        // Used to communicate the status of connector initialization.
        let (init_status_sender, mut init_status_receiver) =
            mpsc::channel::<Result<(), ControllerError>>(1);

        let input_stream = input_handle
            .handle
            .configure_arrow_deserializer(delta_input_serde_config())?;
        let schema = input_handle.schema.clone();

        std::thread::spawn(move || {
            TOKIO.block_on(async {
                let _ = endpoint_clone
                    .worker_task(input_stream, schema, receiver_clone, init_status_sender)
                    .await;
            })
        });

        init_status_receiver.blocking_recv().ok_or_else(|| {
            ControllerError::input_transport_error(
                &endpoint.endpoint_name,
                true,
                anyhow!("worker thread terminated unexpectedly during initialization"),
            )
        })??;

        Ok(Self {
            sender,
            inner: endpoint.clone(),
        })
    }
}

impl InputReader for DeltaTableInputReader {
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

impl Drop for DeltaTableInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

struct DeltaTableInputEndpointInner {
    endpoint_name: String,
    config: DeltaTableReaderConfig,
    consumer: Box<dyn InputConsumer>,
    datafusion: SessionContext,
    queue: InputQueue,
}

impl DeltaTableInputEndpointInner {
    fn new(
        endpoint_name: &str,
        config: DeltaTableReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        let queue = InputQueue::new(consumer.clone());
        // Configure datafusion not to generate Utf8View arrow types, which are not yet
        // supported by the `serde_arrow` crate.
        let session_config = SessionConfig::new().set_bool(
            "datafusion.execution.parquet.schema_force_view_types",
            false,
        );

        Self {
            endpoint_name: endpoint_name.to_string(),
            config,
            consumer,
            datafusion: SessionContext::new_with_config(session_config),
            queue,
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
                debug!("delta_table {}: worker task terminated",
                    &self.endpoint_name,
                );
            }
            _ = receiver_clone.wait_for(|state| state == &PipelineState::Terminated) => {
                debug!("delta_table {}: received termination command; worker task canceled",
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
        info!(
            "delta_table {}: reading initial snapshot",
            &self.endpoint_name,
        );

        let mut snapshot_query = "select * from snapshot".to_string();
        if let Some(filter) = &self.config.snapshot_filter {
            snapshot_query = format!("{snapshot_query} where {filter}");
        }

        self.execute_snapshot_query(&snapshot_query, "initial snapshot", input_stream, receiver)
            .await;

        //let _ = self.datafusion.deregister_table("snapshot");
        info!(
            "delta_table {}: finished reading initial snapshot",
            &self.endpoint_name,
        );
    }

    /// Load the initial snapshot by issuing a sequence of queries for monotonically
    /// increasing timestamp ranges.
    async fn read_ordered_snapshot(
        &self,
        input_stream: &mut dyn ArrowStream,
        schema: &Relation,
        receiver: &mut Receiver<PipelineState>,
    ) {
        self.read_ordered_snapshot_inner(input_stream, schema, receiver)
            .await
            .unwrap_or_else(|e| self.consumer.error(true, e));
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
            "delta_table {}: querying the table for min and max timestamp values",
            &self.endpoint_name,
        );

        if bounds.len() != 1 || bounds[0].num_rows() != 1 {
            info!(
                "delta_table {}: initial snapshot is empty; the Delta table contains no records{}",
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
            "delta_table {}: reading table snapshot in the range '{min} <= {timestamp_column} <= {max}'",
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

        // Start following the table if required by the configuration.
        if self.config.follow() {
            let mut version = table.version();
            loop {
                wait_running(&mut receiver).await;
                match table.log_store().peek_next_commit(version).await {
                    Ok(PeekCommit::UpToDate) => sleep(POLL_INTERVAL).await,
                    Ok(PeekCommit::New(new_version, actions)) => {
                        version = new_version;
                        self.process_log_entry(
                            &actions,
                            &table,
                            input_stream.as_mut(),
                            &mut receiver,
                        )
                        .await;
                    }
                    Err(e) => {
                        self.consumer.error(
                            false,anyhow!("error reading the next log entry (current table version: {version}): {e}"));
                        sleep(RETRY_INTERVAL).await;
                    }
                }
            }
        } else {
            self.consumer.eoi();
        }
    }

    /// Open existing delta table.  Use version or timestamp specified in the configuration, if any.
    async fn open_table(&self) -> Result<DeltaTable, ControllerError> {
        debug!(
            "delta_table {}: opening delta table '{}'",
            &self.endpoint_name, &self.config.uri
        );

        // DeltaTableBuilder::from_uri panics on error, so we use `from_valid_uri` instead.
        let url = ensure_table_uri(&self.config.uri).map_err(|e| {
            ControllerError::invalid_transport_configuration(
                &self.endpoint_name,
                &format!("invalid Delta table uri '{}': {e}", &self.config.uri),
            )
        })?;

        let table_builder = DeltaTableBuilder::from_valid_uri(&url)
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("invalid Delta table URL '{url}': {e}"),
                )
            })?
            .with_storage_options(self.config.object_store_config.clone());

        let table_builder = match &self.config {
            DeltaTableReaderConfig {
                version: Some(_),
                datetime: Some(_),
                ..
            } => {
                return Err(ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    "at most one of 'version' and 'datetime' options can be specified",
                ));
            }
            DeltaTableReaderConfig {
                version: None,
                datetime: None,
                ..
            } => table_builder,
            DeltaTableReaderConfig {
                version: Some(version),
                datetime: None,
                ..
            } => table_builder.with_version(*version),
            DeltaTableReaderConfig {
                version: None,
                datetime: Some(datetime),
                ..
            } => table_builder.with_datestring(datetime).map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!(
                        "invalid 'datetime' format (expected ISO-8601/RFC-3339 timestamp): {e}"
                    ),
                )
            })?,
        };

        let delta_table = table_builder.load().await.map_err(|e| {
            ControllerError::invalid_transport_configuration(
                &self.endpoint_name,
                &format!("error opening delta table '{}': {e}", &self.config.uri),
            )
        })?;

        // Register object store with datafusion, so it will recognize individual parquet
        // file URIs when processing transaction log.  The `object_store_url` function
        // generates a unique URL, which only makes sense to datafusion.  We must append
        // the same string to the relative file path we read from the log below to make
        // sure datafusion links it to this object store.
        let object_store_url = delta_table.log_store().object_store_url();
        let url: &Url = object_store_url.as_ref();
        self.datafusion
            .runtime_env()
            .register_object_store(url, delta_table.log_store().object_store(None));

        info!(
            "delta_table {}: opened delta table '{}' (current table version {})",
            &self.endpoint_name,
            &self.config.uri,
            delta_table.version()
        );

        // TODO: Validate that table schema matches relation schema

        // TODO: Validate that timestamp is a valid column.

        Ok(delta_table)
    }

    /// Register `table` as a Datafusion table named "snapshot".
    async fn register_snapshot_table(
        &self,
        table: &Arc<DeltaTable>,
    ) -> Result<(), ControllerError> {
        trace!(
            "delta_table {}: registering table with Datafusion",
            &self.endpoint_name,
        );

        self.datafusion
            .register_table("snapshot", table.clone())
            .map_err(|e| {
                ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("failed to register table snapshot with datafusion: {e}"),
                )
            })?;

        Ok(())
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
        table: &Arc<DeltaTable>,
        schema: &Relation,
    ) -> Result<(), ControllerError> {
        if !self.config.snapshot() {
            return Ok(());
        }

        self.register_snapshot_table(table).await?;
        self.validate_snapshot_filter()?;

        if let Some(timestamp_column) = &self.config.timestamp_column {
            validate_timestamp_column(
                &self.endpoint_name,
                timestamp_column,
                &self.datafusion,
                schema,
                "see DeltaLake connector documentation for more details: https://docs.feldera.com/connectors/sources/delta"
            )
            .await?;
        };

        Ok(())
    }

    /// Execute a SQL query to load a complete or partial snapshot of the DeltaTable.
    async fn execute_snapshot_query(
        &self,
        query: &str,
        descr: &str,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        let descr = format!("{descr} query '{query}'");
        debug!(
            "delta_table {}: retrieving data from the Delta table snapshot using {descr}",
            &self.endpoint_name,
        );

        let options: SQLOptions = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false);

        let df = match self.datafusion.sql_with_options(query, options).await {
            Ok(df) => df,
            Err(e) => {
                self.consumer
                    .error(true, anyhow!("error compiling query '{query}': {e}"));
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
                    .error(true, anyhow!("error retrieving {descr}: {e}"));
                return;
            }
            Ok(stream) => stream,
        };

        let mut num_batches = 0;
        while let Some(batch) = stream.next().await {
            wait_running(receiver).await;
            let batch = match batch {
                Ok(batch) => batch,
                Err(e) => {
                    self.consumer.error(
                        false,
                        anyhow!("error retrieving batch {num_batches} of {descr}: {e}"),
                    );
                    continue;
                }
            };
            // info!("schema: {}", batch.schema());
            num_batches += 1;
            let bytes = batch.get_array_memory_size();
            let result = if polarity {
                input_stream.insert(&batch)
            } else {
                input_stream.delete(&batch)
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
            self.queue.push((input_stream.take_all(), errors), bytes);
        }
    }

    /// Apply actions from a transaction log entry.
    ///
    /// Only `Add` and `Remove` actions are picked up.
    async fn process_log_entry(
        &self,
        actions: &[Action],
        table: &DeltaTable,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        // TODO: consider processing all Add actions and all Remove actions in one
        // go using `ListingTable`, which understands partitioning and can probably
        // parallelize the load.
        for action in actions {
            self.process_action(action, table, input_stream, receiver)
                .await;
        }
    }

    async fn process_action(
        &self,
        action: &Action,
        table: &DeltaTable,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        match action {
            Action::Add(add) if add.data_change => {
                self.add_with_polarity(&add.path, true, table, input_stream, receiver)
                    .await;
            }
            Action::Remove(remove) if remove.data_change => {
                self.add_with_polarity(&remove.path, false, table, input_stream, receiver)
                    .await;
            }
            _ => (),
        }
    }

    async fn add_with_polarity(
        &self,
        path: &str,
        polarity: bool,
        table: &DeltaTable,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        // See comment about `object_store_url` above.
        let full_path = format!("{}{}", table.log_store().object_store_url().as_str(), path);
        let df = match self
            .datafusion
            .read_parquet(full_path, ParquetReadOptions::default())
            .await
        {
            Err(e) => {
                self.consumer.error(
                    true,
                    anyhow!("error reading Parquet file '{path}' listed in table log: {e}"),
                );
                return;
            }
            Ok(df) => df,
        };

        self.execute_df(
            df,
            polarity,
            &format!("file '{path}'"),
            input_stream,
            receiver,
        )
        .await;
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
