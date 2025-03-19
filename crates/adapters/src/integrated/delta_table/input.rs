use crate::catalog::{ArrowStream, InputCollectionHandle};
use crate::controller::{ControllerInner, EndpointId};
use crate::format::InputBuffer;
use crate::integrated::delta_table::{delta_input_serde_config, register_storage_handlers};
use crate::transport::{
    InputEndpoint, InputQueue, InputReaderCommand, IntegratedInputEndpoint, NonFtInputReaderCommand,
};
use crate::util::{root_cause, JobQueue};
use crate::{
    ControllerError, InputConsumer, InputReader, PipelineState, RecordFormat,
    TransportInputEndpoint,
};
use actix_web::web::Data;
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use arrow::array::BooleanArray;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use datafusion::catalog::TableProvider;
use datafusion::common::arrow::array::{AsArray, RecordBatch};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::sqlparser::parser::ParserError;
use datafusion::logical_expr::{Filter, LogicalPlan, Projection};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionConfig;
use datafusion::scalar::ScalarValue;
use dbsp::circuit::tokio::TOKIO;
use dbsp::operator::input;
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
use deltalake::delta_datafusion::logical;
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
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::error::Error;
use std::fmt::format;
use std::future::Future;
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

/// Default object store timeout. When not explicitly set by the user,
/// we use a large timeout value to avoid this issue:
/// https://github.com/delta-io/delta-rs/issues/2595, which is common for
/// tables with large files.
const DEFAULT_OBJECT_STORE_TIMEOUT: &str = "120s";

const REPORT_ERROR: &str =
    "please report this error to developers (https://github.com/feldera/feldera/issues)";

/// Takes a column name from a DeltaLake schema and returns a qouted string
/// that can be used in datafusion queries like `select "foo""bar" from my_table`.
fn quote_sql_identifier<S: AsRef<str>>(ident: S) -> String {
    format!("\"{}\"", ident.as_ref().replace("\"", "\"\""))
}

/// Integrated input connector that reads from a delta table.
pub struct DeltaTableInputEndpoint {
    endpoint_name: String,
    config: DeltaTableReaderConfig,
    consumer: Box<dyn InputConsumer>,
}

impl DeltaTableInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &DeltaTableReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        register_storage_handlers();

        Self {
            endpoint_name: endpoint_name.to_string(),
            config: config.clone(),
            consumer,
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
        self: Box<Self>,
        input_handle: &InputCollectionHandle,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(DeltaTableInputReader::new(
            self.endpoint_name,
            self.config,
            self.consumer,
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
        endpoint_name: String,
        mut config: DeltaTableReaderConfig,
        consumer: Box<dyn InputConsumer>,
        input_handle: &InputCollectionHandle,
    ) -> AnyResult<Self> {
        let (sender, receiver) = channel(PipelineState::Paused);
        let receiver_clone = receiver.clone();

        // Used to communicate the status of connector initialization.
        let (init_status_sender, mut init_status_receiver) =
            mpsc::channel::<Result<(), ControllerError>>(1);

        if config.num_parsers == 0 {
            bail!("invalid 'num_parsers' value: 'num_parsers' must be greater than 0");
        }

        if !config.object_store_config.contains_key("timeout") {
            config.object_store_config.insert(
                "timeout".to_string(),
                DEFAULT_OBJECT_STORE_TIMEOUT.to_string(),
            );
        }

        let input_stream = input_handle
            .handle
            .configure_arrow_deserializer(delta_input_serde_config())?;
        let schema = input_handle.schema.clone();

        let endpoint = Arc::new(DeltaTableInputEndpointInner::new(
            &endpoint_name,
            config,
            consumer,
            schema,
        ));
        let endpoint_clone = endpoint.clone();

        std::thread::spawn(move || {
            TOKIO.block_on(async {
                let _ = endpoint_clone
                    .worker_task(input_stream, receiver_clone, init_status_sender)
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
    schema: Relation,
    config: DeltaTableReaderConfig,
    consumer: Box<dyn InputConsumer>,
    datafusion: SessionContext,
    queue: Arc<InputQueue>,
}

impl DeltaTableInputEndpointInner {
    fn new(
        endpoint_name: &str,
        config: DeltaTableReaderConfig,
        consumer: Box<dyn InputConsumer>,
        schema: Relation,
    ) -> Self {
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        // Configure datafusion not to generate Utf8View arrow types, which are not yet
        // supported by the `serde_arrow` crate.
        let session_config = SessionConfig::new().set_bool(
            "datafusion.execution.parquet.schema_force_view_types",
            false,
        );

        Self {
            endpoint_name: endpoint_name.to_string(),
            schema,
            config,
            consumer,
            datafusion: SessionContext::new_with_config(session_config),
            queue,
        }
    }

    fn validate_cdc_config(&self) -> Result<(), ControllerError> {
        if self.config.is_cdc() {
            let Some(cdc_order_by) = &self.config.cdc_order_by else {
                return Err(ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    "the DeltaLake connector is configured in 'cdc' mode, but the 'cdc_order_by' property is not set",
                ));
            };

            self.validate_cdc_order_by()?;

            if self.config.cdc_delete_filter.is_none() {
                return Err(ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    "the DeltaLake connector is configured in 'cdc' mode, but the 'cdc_delete_filter' property is not set",
                ));
            }
        } else {
            if self.config.cdc_delete_filter.is_some() {
                return Err(ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    "the 'cdc_delete_filter' property can only be used when 'mode=cdc'",
                ));
            }
            if self.config.cdc_order_by.is_some() {
                return Err(ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    "the 'cdc_order_by' property can only be used with 'mode=cdc'",
                ));
            }
        }

        Ok(())
    }

    async fn worker_task(
        self: Arc<Self>,
        input_stream: Box<dyn ArrowStream>,
        receiver: Receiver<PipelineState>,
        init_status_sender: mpsc::Sender<Result<(), ControllerError>>,
    ) {
        let mut receiver_clone = receiver.clone();
        select! {
            _ = Self::worker_task_inner(self.clone(), input_stream, receiver, init_status_sender) => {
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

    /// Compute the subset of columns in the Delta table schema that occur in the SQL
    /// table declaration.
    fn used_columns(&self, table: &DeltaTable) -> Vec<String> {
        // Column names in the SQL schema.
        let sql_columns = self
            .schema
            .fields
            .iter()
            .map(|field| field.name.name())
            .collect::<Vec<String>>();

        if let Some(table_schema) = table.schema() {
            let delta_columns = table_schema
                .fields()
                .map(|f| f.name().clone())
                .collect::<Vec<String>>();

            // We need to be careful in checking whether a Delta column name occurs in
            // sql_columns.  Delta doesn't seem to include case sensitivity information any
            // any form in its schema.  So we conservatively check whether column name
            // occurs in the SQL tables as is or whether a lowercase column name occurs
            // in the set of SQL columns converted to lowercase.

            let sql_columns = sql_columns.iter().cloned().collect::<BTreeSet<_>>();
            let sql_columns_lowercase = sql_columns
                .iter()
                .map(|c| c.to_lowercase())
                .collect::<BTreeSet<_>>();

            delta_columns
                .into_iter()
                .filter(|c| {
                    sql_columns.contains(c) || sql_columns_lowercase.contains(&c.to_lowercase())
                })
                .collect::<Vec<_>>()
        } else {
            sql_columns
        }
    }

    fn used_column_list(&self, table: &DeltaTable) -> String {
        let columns = self.used_columns(table);

        columns
            .iter()
            .map(quote_sql_identifier)
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Load the entire table snapshot as a single "select * where <filter>" query.
    async fn read_unordered_snapshot(
        &self,
        table: &DeltaTable,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        let column_names = self.used_column_list(table);

        let mut snapshot_query = format!("select {column_names} from snapshot");
        if let Some(filter) = self.effective_snapshot_filter() {
            snapshot_query = format!("{snapshot_query} where {filter}");
        }
        // Execute the snapshot query; push snapshot data to the circuit.
        info!(
            "delta_table {}: reading initial snapshot: {snapshot_query}",
            &self.endpoint_name,
        );

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
        table: &DeltaTable,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        self.read_ordered_snapshot_inner(table, input_stream, receiver)
            .await
            .unwrap_or_else(|e| self.consumer.error(true, e));
    }

    async fn read_ordered_snapshot_inner(
        &self,
        table: &DeltaTable,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) -> Result<(), AnyError> {
        let timestamp_column = self.config.timestamp_column.as_ref().unwrap();

        let timestamp_field = self.schema.field(timestamp_column).unwrap();

        // The following unwraps are safe, as validated in `validate_timestamp_column`.
        let lateness = timestamp_field.lateness.as_ref().unwrap();

        // Query the table for min and max values of the timestamp column that satisfy the filter.
        let bounds_query =
            format!("select * from (select cast(min({timestamp_column}) as string) as start_ts, cast(max({timestamp_column}) as string) as end_ts from snapshot {}) where start_ts is not null",
            if let Some(filter) = &self.effective_snapshot_filter() {
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
                if let Some(filter) = &self.effective_snapshot_filter() {
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

        let columns = self.used_columns(table);
        let column_names = columns
            .iter()
            .map(quote_sql_identifier)
            .collect::<Vec<_>>()
            .join(", ");

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
                format!("select {column_names} from snapshot where {timestamp_column} >= {start} and {timestamp_column} < {end}");
            if let Some(filter) = &self.effective_snapshot_filter() {
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

        if let Err(e) = self.prepare_snapshot_query(&table).await {
            let _ = init_status_sender.send(Err(e)).await;
            return;
        };

        let cdc_delete_filter = match self.prepare_cdc().await {
            Err(e) => {
                let _ = init_status_sender.send(Err(e)).await;
                return;
            }
            Ok(cdc_delete_filter) => cdc_delete_filter,
        };

        // Code before this point is part of endpoint initialization.
        // After this point, the thread should continue running until it receives a
        // shutdown command from the controller.
        let _ = init_status_sender.send(Ok(())).await;

        if self.config.snapshot() && self.config.timestamp_column.is_none() {
            // Read snapshot chunk-by-chunk.
            self.read_unordered_snapshot(&table, input_stream.as_mut(), &mut receiver)
                .await;
        } else if self.config.snapshot() {
            // Read the entire snapshot in one query.
            self.read_ordered_snapshot(&table, input_stream.as_mut(), &mut receiver)
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
                            cdc_delete_filter.clone(),
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

        // if let Some(schema) = delta_table.schema() {
        //     info!("Delta table schema: {schema:?}");
        // }

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

    /// Validate the filter expression specified in the 'filter' parameter.
    fn validate_filter(&self) -> Result<(), ControllerError> {
        if let Some(filter) = &self.config.filter {
            validate_sql_expression(filter).map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("error parsing 'filter' expression '{filter}': {e}"),
                )
            })?;
        }

        Ok(())
    }

    /// SQL expression that combines the snapshot filter and the filter is either or both are specified.
    fn effective_snapshot_filter(&self) -> Option<String> {
        match (&self.config.snapshot_filter, &self.config.filter) {
            (Some(snapshot_filter), Some(filter)) => {
                Some(format!("({snapshot_filter}) and ({filter})"))
            }
            (Some(snapshot_filter), None) => Some(snapshot_filter.to_string()),
            (None, Some(filter)) => Some(filter.to_string()),
            (None, None) => None,
        }
    }

    /// Validate the cdc_order_by expression.
    fn validate_cdc_order_by(&self) -> Result<(), ControllerError> {
        if let Some(order_by) = &self.config.cdc_order_by {
            validate_sql_expression(order_by).map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("error parsing 'cdc_order_by' expression '{order_by}': {e}"),
                )
            })?;
        }

        Ok(())
    }

    /// Convert the delete filter SQL expression into a Datafusion PhysicalExpr.
    ///
    /// To do this, we generate a query plan for the `select * from snapshot where <delete_filter>`
    /// query and extract the logical expression from the plan and then convert it to
    /// a physical expression.
    //
    // FIXME: I wonder if there's a simpler way that doesn't require registering the `snapshot`
    // table just so we can compile this query.
    async fn extract_delete_filter_expr(
        &self,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>, ControllerError> {
        let Some(delete_filter) = &self.config.cdc_delete_filter else {
            return Ok(None);
        };

        let query = format!("SELECT * FROM snapshot WHERE {}", delete_filter);

        let logical_plan = self
            .datafusion
            .sql(&query)
            .await
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("invalid delete filter '{delete_filter}': the 'cdc_delete_filter' expression must be a valid SQL expression that can be used in a 'SELECT * FROM snapshot WHERE <cdc_delete_filter>' query, but the following error was encountered when compiling '{query}': {e}"),
                )
            })?
            .logical_plan()
            .clone();

        let filter_expr = if let LogicalPlan::Projection(filter_plan) = &logical_plan {
            if let LogicalPlan::Filter(filter) = filter_plan.input.as_ref() {
                filter.predicate.clone()
            } else {
                return Err(ControllerError::invalid_encoder_configuration(
                    &self.endpoint_name,
                    &format!("internal error when compiling the 'cdc_delete_filter' expression '{delete_filter}'; {REPORT_ERROR}: unexpected logical plan {logical_plan}"),
                ));
            }
        } else {
            return Err(ControllerError::invalid_encoder_configuration(
                &self.endpoint_name,
                &format!("internal error when compiling the 'cdc_delete_filter' expression '{delete_filter}'; {REPORT_ERROR}: unexpected logical plan {logical_plan}"),
            ));
        };

        let schema = self
            .datafusion
            .table("snapshot")
            .await
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(&self.endpoint_name, &format!("internal error when compiling the 'cdc_delete_filter' expession '{delete_filter}'; {REPORT_ERROR}: table 'snapshot' not found"))
            })?
            .schema()
            .clone();

        let physical_expr = DefaultPhysicalPlanner::default()
            .create_physical_expr(&filter_expr, &schema, &self.datafusion.state())
            .map_err(|e| ControllerError::invalid_transport_configuration(&self.endpoint_name, &format!("internal error when compiling the 'cdc_delete_filter' expession '{delete_filter}'; {REPORT_ERROR}: error generating physical plan: {e}")))?;

        Ok(Some(physical_expr))
    }

    /// Evaluate delete filter expression against a batch of updates; returns a vector of polarities.
    async fn eval_delete_filter(
        delete_filter: &dyn PhysicalExpr,
        batch: &RecordBatch,
    ) -> AnyResult<Vec<bool>> {
        let deletions = delete_filter.evaluate(batch).map_err(|e| {
            anyhow!("internal error evaluating the delete filter expression; {REPORT_ERROR}: {e}")
        })?;

        let array = deletions
            .into_array(batch.num_rows())
            .map_err(|e| {
                anyhow!(
                    "internal error converting the result of the delete filter expressions to array; {REPORT_ERROR}: {e}"
                )
            })?;
        let bitmask = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                anyhow!(
                    "internal error converting the result of the delete filter exptession to BooleanArray; {REPORT_ERROR}: expected Boolean, found {:?}", array.data_type()
                )
            })?;

        let polarities = bitmask
            .into_iter()
            // treat NULLs as inserts.
            .map(|b| !b.unwrap_or(false))
            .collect::<Vec<bool>>();

        Ok(polarities)
    }

    /// Prepare to read initial snapshot, if required by endpoint configuration.
    ///
    /// * register snapshot as a datafusion table
    /// * validate snapshot config: filter condition and timestamp column
    async fn prepare_snapshot_query(&self, table: &Arc<DeltaTable>) -> Result<(), ControllerError> {
        if !self.config.snapshot() && !self.config.is_cdc() {
            return Ok(());
        }

        self.register_snapshot_table(table).await?;
        self.validate_snapshot_filter()?;
        self.validate_filter()?;

        if let Some(timestamp_column) = &self.config.timestamp_column {
            validate_timestamp_column(
                &self.endpoint_name,
                timestamp_column,
                &self.datafusion,
                &self.schema,
                "see DeltaLake connector documentation for more details: https://docs.feldera.com/connectors/sources/delta"
            )
            .await?;
        };

        Ok(())
    }

    /// Prepare to process CDC stream.
    async fn prepare_cdc(&self) -> Result<Option<Arc<dyn PhysicalExpr>>, ControllerError> {
        self.validate_cdc_config()?;

        let delete_expr = self.extract_delete_filter_expr().await?;
        Ok(delete_expr)
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

        self.execute_df(df, true, None, &descr, input_stream, receiver)
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
        cdc_delete_filter: Option<Arc<dyn PhysicalExpr>>,
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

        let queue = self.queue.clone();

        let num_parsers = self.config.num_parsers as usize;

        // Create a job queue to efficiently parse record batches retrieved by the query.
        let job_queue =
            JobQueue::<RecordBatch, (Option<Box<dyn InputBuffer>>, Vec<ParseError>, usize)>::new(
                num_parsers,
                move || {
                    let cdc_delete_filter: Option<Arc<dyn PhysicalExpr>> =
                        cdc_delete_filter.clone();
                    let input_stream = input_stream.fork();

                    Box::new(move |batch| {
                        Box::pin({
                            let cdc_delete_filter: Option<Arc<dyn PhysicalExpr>> =
                                cdc_delete_filter.clone();
                            let mut input_stream = input_stream.fork();

                            async move {
                                Self::parse_record_batch(
                                    batch,
                                    polarity,
                                    &cdc_delete_filter,
                                    input_stream.as_mut(),
                                )
                                .await
                            }
                        })
                    })
                },
                move |(buffer, errors, bytes)| queue.push((buffer, errors), bytes),
            );

        while let Some(batch) = stream.next().await {
            wait_running(receiver).await;
            let batch = match batch {
                Ok(batch) => batch,
                Err(e) => {
                    match e {
                        DataFusionError::ArrowError(ArrowError::ExternalError(error), _) => self.consumer.error(
                            false,
                            anyhow!("error retrieving batch {num_batches} of {descr}: external error: {error} (root cause: {})", root_cause(error.as_ref()),
                        )),
                        e => self.consumer.error(
                            false,
                            anyhow!("error retrieving batch {num_batches} of {descr}: {e}"),
                        ),
                    }

                    continue;
                }
            };
            // info!("schema: {}", batch.schema());
            num_batches += 1;
            job_queue.push_job(batch).await;
        }

        job_queue.flush().await
    }

    async fn parse_record_batch(
        batch: RecordBatch,
        polarity: bool,
        cdc_delete_filter: &Option<Arc<dyn PhysicalExpr>>,
        input_stream: &mut dyn ArrowStream,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>, usize) {
        let bytes = batch.get_array_memory_size();
        let result = if polarity {
            if let Some(delete_filter_expr) = cdc_delete_filter {
                let polarities =
                    match Self::eval_delete_filter(delete_filter_expr.as_ref(), &batch).await {
                        Ok(polarities) => polarities,
                        Err(e) => {
                            return (
                                None,
                                vec![ParseError::bin_envelope_error(e.to_string(), &[], None)],
                                bytes,
                            );
                        }
                    };
                // println!(
                //     "insert_with_polarities: {} updates, {} insertions",
                //     polarities.len(),
                //     polarities.iter().filter(|x| **x == true).count()
                // );
                input_stream.insert_with_polarities(&batch, &polarities)
            } else {
                input_stream.insert(&batch)
            }
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

        (input_stream.take_all(), errors, bytes)
    }

    /// Apply actions from a transaction log entry.
    ///
    /// Only `Add` and `Remove` actions are picked up.
    async fn process_log_entry(
        &self,
        actions: &[Action],
        table: &DeltaTable,
        cdc_delete_filter: Option<Arc<dyn PhysicalExpr>>,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        if self.config.is_cdc() {
            self.process_cdc_transaction(actions, table, cdc_delete_filter, input_stream, receiver)
                .await;
        } else {
            let column_names = self.used_column_list(table);

            // TODO: consider processing all Add actions and all Remove actions in one
            // go using `ListingTable`, which understands partitioning and can probably
            // parallelize the load.
            for action in actions {
                self.process_action(action, table, &column_names, input_stream, receiver)
                    .await;
            }
        }
    }

    /// Process a DeltaLake transaction in CDC mode:
    ///
    /// * Ignore delete actions (the assumption is that CDC tables are append-only,
    ///   where only old records are deleted as part of the table maintenance)
    /// * Order all rows from all insert actions by `cdc_order_by`, compute the
    ///   polarity of each row using `cdc_delete_filter` and insert the row with
    ///   appropriate polarity.
    async fn process_cdc_transaction(
        &self,
        actions: &[Action],
        table: &DeltaTable,
        cdc_delete_filter: Option<Arc<dyn PhysicalExpr>>,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        let result = self
            .do_process_cdc_transaction(actions, table, cdc_delete_filter, input_stream, receiver)
            .await;

        // Deregister the table registered by `do_process_cdc_transaction`.
        // If the table does not exist, there's no harm.
        let _ = self.datafusion.deregister_table("tmp_table");

        if let Err(e) = result {
            self.consumer.error(false, e);
        }
    }

    async fn do_process_cdc_transaction(
        &self,
        actions: &[Action],
        table: &DeltaTable,
        cdc_delete_filter: Option<Arc<dyn PhysicalExpr>>,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) -> AnyResult<()> {
        // List all files that occur in Add actions.
        let files = actions
            .iter()
            .flat_map(|action| match action {
                Action::Add(add) if add.data_change => Some(format!(
                    "{}{}",
                    table.log_store().object_store_url().as_str(),
                    add.path
                )),
                _ => None,
            })
            .collect::<Vec<_>>();

        let description = format!(
            "CDC transaction consisting of {} files {:?}",
            files.len(),
            &files
        );

        // Create a datafusion table backed by these files.
        let table = Arc::new(
            self.create_parquet_table(table, files, &description)
                .await?,
        );

        self.datafusion.register_table("tmp_table", table).map_err(|e| {
            anyhow!("internal error processing {description}; {REPORT_ERROR}; error registering Parquet table: {e}")
        })?;

        let where_clause = if let Some(filter) = &self.config.filter {
            format!("where {filter}")
        } else {
            "".to_string()
        };

        // Order the table by the `cdc_order_by` expression.
        // TODO: We don't use `used_column_list` here, as the resulting dataframe will have a different
        // schema than the original table, and the `cdc_delete_filter` physical expression won't be valid for it.
        let order_by = self.config.cdc_order_by.as_ref().unwrap();
        let query = format!("SELECT * FROM tmp_table {where_clause} ORDER BY {order_by}");

        let df = self.datafusion.sql(&query).await.map_err(|e| {
            anyhow!("invalid 'cdc_order_by' or 'filter' expression: 'cdc_order_by' and 'filter' (when specified) must be valid SQL expressions that can be used in a 'SELECT * FROM <table> WHERE <filter> ORDER BY <cdc_order_by>' query, but the following error was encountered when compiling '{query}': {e}")
        })?;

        self.execute_df(
            df,
            true,
            cdc_delete_filter,
            &description,
            input_stream,
            receiver,
        )
        .await;

        Ok(())
    }

    /// Create a table provider from a list of Parquet files.
    async fn create_parquet_table(
        &self,
        table: &DeltaTable,
        files: Vec<String>,
        description: &str,
    ) -> AnyResult<ListingTable> {
        let Some(schema) = table.schema() else {
            // At this point the table should have a schema, as it's definitely not empty.
            return Err(anyhow!("internal error processing {description}; {REPORT_ERROR}: Delta table has not schema"));
        };

        let schema: Schema = schema.try_into().map_err(|e| anyhow!("internal error processing {description}; {REPORT_ERROR}; error converting Delta schema {schema:?} to arrow schema: {e}"))?;

        let listing_options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");

        let mut urls = Vec::with_capacity(files.len());
        for file in files.iter() {
            urls.push(ListingTableUrl::parse(file).map_err(|e| anyhow!("internal error processing {description}; {REPORT_ERROR}; error converting file path '{file}' to table URL: {e}"))?);
        }

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension_opt(Some(".parquet"));

        let table_config = ListingTableConfig::new_with_multi_paths(urls)
            .with_schema(Arc::new(schema))
            .with_listing_options(listing_options);

        ListingTable::try_new(table_config).map_err(|e| {
            anyhow!("internal error processing {description}; {REPORT_ERROR}; error creating Parquet table: {e}")
        })
    }

    async fn process_action(
        &self,
        action: &Action,
        table: &DeltaTable,
        column_names: &str,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        let result = match action {
            Action::Add(add) if add.data_change => {
                self.add_with_polarity(&add.path, true, table, column_names, input_stream, receiver)
                    .await
            }
            Action::Remove(remove)
                if remove.data_change && self.config.cdc_delete_filter.is_none() =>
            {
                self.add_with_polarity(
                    &remove.path,
                    false,
                    table,
                    column_names,
                    input_stream,
                    receiver,
                )
                .await
            }
            _ => return,
        };

        // Deregister the table registered by `add_with_polarity`.
        // If the table does not exist, there's no harm.
        let _ = self.datafusion.deregister_table("tmp_table");

        if let Err(e) = result {
            self.consumer.error(false, e);
        }
    }

    // TODO: here, as well as in `process_cdc_transaction`, we can get some potential speedup by only reading a subset
    // of columns that occurs in the SQL declaration, similar to how we already do when reading the snapshot.  However,
    // this requires some extra care, since different transactions in the log can have different schemas. The implementation
    // should therefore monitor for schema changes and update the set of relevant columns appropriately.
    async fn add_with_polarity(
        &self,
        path: &str,
        polarity: bool,
        table: &DeltaTable,
        column_names: &str,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) -> AnyResult<()> {
        let description = format!("file '{path}'");

        // See comment about `object_store_url` above.
        let full_path = format!("{}{}", table.log_store().object_store_url().as_str(), path);

        // Create a datafusion table backed by these files.
        let table = Arc::new(
            self.create_parquet_table(table, vec![full_path.clone()], &description)
                .await?,
        );

        self.datafusion.register_table("tmp_table", table).map_err(|e| {
            anyhow!("internal error processing file {full_path}; {REPORT_ERROR}; error registering Parquet table: {e}")
        })?;

        let df = if let Some(filter) = &self.config.filter {
            let query = format!("SELECT {column_names} FROM tmp_table where {filter}");
            self.datafusion.sql(&query).await.map_err(|e| {
                anyhow!("invalid 'cdc_order_by' filter expression '{filter}': 'filter' must be a valid SQL expression that can be used in a 'SELECT * FROM <table> ORDER BY <cdc_order_by>' query, but the following error was encountered when compiling '{query}': {e}")
            })?
        } else {
            let query = format!("SELECT {column_names} FROM tmp_table");
            self.datafusion.sql(&query).await.map_err(|e| {
                anyhow!("internal error processing file {full_path}'; {REPORT_ERROR}; error compiling query '{query}': {e}")
            })?
        };

        self.execute_df(df, polarity, None, &description, input_stream, receiver)
            .await;

        Ok(())
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
