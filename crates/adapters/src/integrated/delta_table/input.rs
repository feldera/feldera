use crate::catalog::{ArrowStream, InputCollectionHandle};
use crate::controller::{ControllerInner, EndpointId};
use crate::integrated::delta_table::{delta_input_serde_config, register_storage_handlers};
use crate::transport::{InputEndpoint, IntegratedInputEndpoint, Step};
use crate::{
    ControllerError, InputConsumer, InputReader, ParseError, PipelineState, RecordFormat,
    TransportInputEndpoint,
};
use anyhow::{anyhow, Result as AnyResult};
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
use env_logger::builder;
use futures_util::StreamExt;
use log::{debug, error, info, trace};
use pipeline_types::config::{InputEndpointConfig, TransportConfigVariant};
use pipeline_types::format::json::JsonFlavor;
use pipeline_types::program_schema::Relation;
use pipeline_types::transport::delta_table::{DeltaTableIngestMode, DeltaTableReaderConfig};
use pipeline_types::transport::s3::{ReadStrategy, S3InputConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::format;
use std::str::FromStr;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch::{channel, Receiver, Sender};
use tokio::time::sleep;
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
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &DeltaTableReaderConfig,
        controller: Weak<ControllerInner>,
    ) -> Self {
        register_storage_handlers();

        Self {
            inner: Arc::new(DeltaTableInputEndpointInner::new(
                endpoint_id,
                endpoint_name,
                config.clone(),
                controller,
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
    fn open(
        &self,
        input_handle: &InputCollectionHandle,
        _start_step: Step,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(DeltaTableInputReader::new(
            &self.inner,
            input_handle,
        )?))
    }
}

struct DeltaTableInputReader {
    sender: Sender<PipelineState>,
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

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("Could not create tokio runtime");
            rt.block_on(async {
                let _ = Self::worker_task(
                    endpoint_clone,
                    input_stream,
                    receiver_clone,
                    init_status_sender,
                )
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

        Ok(Self { sender })
    }

    async fn worker_task(
        endpoint: Arc<DeltaTableInputEndpointInner>,
        input_stream: Box<dyn ArrowStream>,
        receiver: Receiver<PipelineState>,
        init_status_sender: mpsc::Sender<Result<(), ControllerError>>,
    ) {
        let mut receiver_clone = receiver.clone();
        select! {
            _ = Self::worker_task_inner(endpoint.clone(), input_stream, receiver, init_status_sender) => {
                debug!("delta_table {}: worker task terminated",
                    &endpoint.endpoint_name,
                );
            }
            _ = receiver_clone.wait_for(|state| state == &PipelineState::Terminated) => {
                debug!("delta_table {}: received termination command; worker task canceled",
                    &endpoint.endpoint_name,
                );
            }
        }
    }

    async fn worker_task_inner(
        endpoint: Arc<DeltaTableInputEndpointInner>,
        mut input_stream: Box<dyn ArrowStream>,
        mut receiver: Receiver<PipelineState>,
        init_status_sender: mpsc::Sender<Result<(), ControllerError>>,
    ) {
        let table = match endpoint.open_table().await {
            Err(e) => {
                let _ = init_status_sender.send(Err(e)).await;
                return;
            }
            Ok(table) => table,
        };

        let table = Arc::new(table);

        let snapshot_df = match endpoint.prepare_snapshot_query(&table).await {
            Err(e) => {
                let _ = init_status_sender.send(Err(e)).await;
                return;
            }
            Ok(snapshot_df) => snapshot_df,
        };

        // Code before this point is part of endpoint initialization.
        // After this point, the thread should continue running until it receives a
        // shutdown command from the controller.
        let _ = init_status_sender.send(Ok(())).await;

        // Execute the snapshot query; push snapshot data to the circuit.
        if let Some(snapshot_df) = snapshot_df {
            info!(
                "delta_table {}: reading initial snapshot",
                &endpoint.endpoint_name,
            );

            endpoint
                .execute_df(
                    snapshot_df,
                    true,
                    "initial snapshot",
                    input_stream.as_mut(),
                    &mut receiver,
                )
                .await;

            //let _ = endpoint.datafusion.deregister_table("snapshot");
            info!(
                "delta_table {}: finished reading initial snapshot",
                &endpoint.endpoint_name,
            );
        }

        // Start following the table if required by the configuration.
        if endpoint.config.follow() {
            let mut version = table.version();
            loop {
                wait_running(&mut receiver).await;
                match table.peek_next_commit(version).await {
                    Ok(PeekCommit::UpToDate) => sleep(POLL_INTERVAL).await,
                    Ok(PeekCommit::New(new_version, actions)) => {
                        version = new_version;
                        endpoint
                            .process_log_entry(
                                &actions,
                                &table,
                                input_stream.as_mut(),
                                &mut receiver,
                            )
                            .await;
                    }
                    Err(e) => {
                        if let Some(controller) = endpoint.controller.upgrade() {
                            controller.input_transport_error(
                                endpoint.endpoint_id,
                                &endpoint.endpoint_name,
                                false,
                                anyhow!("error reading the next log entry (current table version: {version}): {e}"),
                            )
                        }
                        sleep(RETRY_INTERVAL).await;
                    }
                }
            }
        } else if let Some(controller) = endpoint.controller.upgrade() {
            controller.eoi(endpoint.endpoint_id, 0)
        }
    }
}

impl InputReader for DeltaTableInputReader {
    fn start(&self, _step: Step) -> anyhow::Result<()> {
        self.sender.send_replace(PipelineState::Running);
        Ok(())
    }

    fn pause(&self) -> anyhow::Result<()> {
        self.sender.send_replace(PipelineState::Paused);
        Ok(())
    }

    fn disconnect(&self) {
        self.sender.send_replace(PipelineState::Terminated);
    }
}

impl Drop for DeltaTableInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

struct DeltaTableInputEndpointInner {
    endpoint_id: EndpointId,
    endpoint_name: String,
    config: DeltaTableReaderConfig,
    controller: Weak<ControllerInner>,
    datafusion: SessionContext,
}

impl DeltaTableInputEndpointInner {
    fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: DeltaTableReaderConfig,
        controller: Weak<ControllerInner>,
    ) -> Self {
        Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_string(),
            config,
            controller,
            datafusion: SessionContext::new(),
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
            .register_object_store(url, delta_table.log_store().object_store());

        debug!(
            "delta_table {}: opened delta table '{}' (current table version {})",
            &self.endpoint_name,
            &self.config.uri,
            delta_table.version()
        );

        // TODO: Validate that table schema matches relation schema

        // TODO: Validate that timestamp is a valid column.

        Ok(delta_table)
    }

    /// Prepare a dataframe to read initial snapshot, if required by endpoint configuration.
    ///
    /// This function runs as part of endpoint initialization.  The query will be actually
    /// executed after initialization. The goal is to catch as many configuration errors as
    /// possible during initialization.
    ///
    // FIXME: we rely on `order by` to order the snapshot by timestamp if requested by
    // the configuration.  Datafusion implments this by downloading the entire dataset,
    // storing and sorting it locally.  This is expensive and unnecessary.  A better approach
    // is to instead read the input table in a series of queries, each returning a time range
    // equal to the lateness of the timestamp column.  This is how the
    // [`withEventTimeOrder`](https://docs.databricks.com/en/structured-streaming/delta-lake.html#process-initial-snapshot-without-data-being-dropped)
    // feature works in Databricks.
    async fn prepare_snapshot_query(
        &self,
        table: &Arc<DeltaTable>,
    ) -> Result<Option<DataFrame>, ControllerError> {
        if !self.config.snapshot() {
            return Ok(None);
        }

        trace!(
            "delta_table {}: preparing initial snapshot query",
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

        let mut snapshot_query = "select * from snapshot".to_string();

        if let Some(filter) = &self.config.snapshot_filter {
            // Parse expression only to validate it.
            let mut parser = Parser::new(&GenericDialect)
                .try_with_sql(filter)
                .map_err(|e| {
                    ControllerError::invalid_transport_configuration(
                        &self.endpoint_name,
                        &format!("error parsing filter expression '{filter}': {e}"),
                    )
                })?;
            parser.parse_expr().map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("invalid filter expression '{filter}': {e}"),
                )
            })?;

            snapshot_query = format!("{snapshot_query} where {filter}");
        }

        if let Some(timestamp_column) = &self.config.timestamp_column {
            // Parse expression only to validate it.
            let mut parser = Parser::new(&GenericDialect)
                .try_with_sql(timestamp_column)
                .map_err(|e| {
                    ControllerError::invalid_transport_configuration(
                        &self.endpoint_name,
                        &format!("error parsing timestamp expression '{timestamp_column}': {e}"),
                    )
                })?;
            parser.parse_expr().map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("invalid timestamp expression '{timestamp_column}': {e}"),
                )
            })?;

            snapshot_query = format!("{snapshot_query} order by {timestamp_column}");
        }

        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false);
        let snapshot = self
            .datafusion
            .sql_with_options(&snapshot_query, options)
            .await
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("error compiling snapshot query '{snapshot_query}': {e}"),
                )
            })?;

        Ok(Some(snapshot))
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
                if let Some(controller) = self.controller.upgrade() {
                    controller.input_transport_error(
                        self.endpoint_id,
                        &self.endpoint_name,
                        true,
                        anyhow!("error retrieving {descr}: {e}"),
                    )
                }
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
                    if let Some(controller) = self.controller.upgrade() {
                        controller.input_transport_error(
                            self.endpoint_id,
                            &self.endpoint_name,
                            false,
                            anyhow!("error retrieving batch {num_batches} of {descr}: {e}"),
                        )
                    }
                    continue;
                }
            };
            // info!("schema: {}", batch.schema());
            num_batches += 1;
            let bytes = batch.get_array_memory_size();
            let rows = batch.num_rows();
            let result = if polarity {
                input_stream.insert(&batch)
            } else {
                input_stream.delete(&batch)
            };

            match result {
                Ok(()) => {
                    if let Some(controller) = self.controller.upgrade() {
                        controller.input_batch(self.endpoint_id, bytes, rows)
                    }
                }
                Err(e) => {
                    if let Some(controller) = self.controller.upgrade() {
                        controller.parse_error(
                            self.endpoint_id,
                            &self.endpoint_name,
                            ParseError::bin_envelope_error(
                                format!("error deserializing table records from Parquet data: {e}"),
                                &[],
                                None,
                            ),
                        )
                    }
                }
            }
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
                if let Some(controller) = self.controller.upgrade() {
                    controller.input_transport_error(
                        self.endpoint_id,
                        &self.endpoint_name,
                        true,
                        anyhow!("error reading Parquet file '{path}' listed in table log: {e}"),
                    );
                }
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
