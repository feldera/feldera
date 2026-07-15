use crate::iceberg_input_serde_config;
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::TableProvider;
use datafusion::prelude::{DataFrame, SQLOptions, SessionContext};
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
        array_to_string, columns_referenced_by_expression, create_session_context,
        execute_query_collect, execute_singleton_query, quote_sql_identifier,
        timestamp_to_sql_expression, validate_sql_expression, validate_timestamp_column,
        ColumnNameSet,
    },
    PipelineState,
};
use feldera_types::{
    config::{FtModel, PipelineConfig},
    program_schema::{Field, Relation},
    transport::iceberg::{IcebergCatalogType, IcebergReaderConfig},
};
use futures_util::StreamExt;
use iceberg::CatalogBuilder;
use iceberg::{
    io::{FileIO, FileIOBuilder, StorageFactory},
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
use log::{debug, info, trace};
use std::{collections::BTreeSet, sync::Arc, thread};
use tokio::{
    select,
    sync::{
        mpsc,
        watch::{channel, Receiver, Sender},
    },
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

/// SQL columns named by the connector's own `snapshot_filter` expression,
/// case-folded for matching. These are kept even when marked unused, so the
/// expression is guaranteed to resolve them (mirrors the Delta connector; for
/// the snapshot queries issued here a `where` clause could also reference
/// unprojected columns).
///
/// The filter is validated before columns are computed, so it parses here; a
/// parse error is therefore unreachable and contributes no columns rather
/// than failing the connector a second time.
fn config_referenced_columns(config: &IcebergReaderConfig) -> ColumnNameSet {
    let mut columns = BTreeSet::new();
    if let Some(filter) = &config.snapshot_filter {
        columns.extend(columns_referenced_by_expression(filter).unwrap_or_default());
    }
    ColumnNameSet::from_names(columns)
}

/// True if a column's *shape* allows omitting it: no user-visible result
/// depends on it (`unused`), and omitting it lets us substitute NULL or its
/// default value (it is nullable or has a default).
///
/// This is the shape-only rule; [`can_skip_column`] adds the
/// filter-reference check before a column is actually skipped.
fn is_unused_and_omittable(field: &Field) -> bool {
    field.unused && (field.columntype.nullable || field.default.is_some())
}

/// True if a column may actually be skipped: its shape permits omitting it
/// ([`is_unused_and_omittable`]) *and* the `snapshot_filter` expression does
/// not reference it.
fn can_skip_column(field: &Field, config_referenced: &ColumnNameSet) -> bool {
    is_unused_and_omittable(field) && !config_referenced.contains(&field.name.name())
}

/// SQL columns the connector reads, matched case-insensitively against
/// Iceberg column names. When the SQL table declaration sets the
/// `skip_unused_columns` property, skippable unused columns are removed.
fn used_sql_columns(schema: &Relation, config: &IcebergReaderConfig) -> ColumnNameSet {
    let skip = schema.skip_unused_columns();
    let config_referenced = config_referenced_columns(config);
    ColumnNameSet::from_names(
        schema
            .fields
            .iter()
            .filter(|f| !skip || !can_skip_column(f, &config_referenced))
            .map(|f| f.name.name()),
    )
}

/// Compute the subset of columns in the Iceberg table snapshot schema that
/// occur in the SQL table declaration (minus columns removed by
/// [`used_sql_columns`]), preserving the table schema's column order.
///
/// Snapshot queries select this subset instead of `*`, so the connector never
/// reads table columns the pipeline does not ingest. Besides being wasteful,
/// reading such columns can fail when their Arrow type is not supported by
/// the Feldera deserializer.
///
/// Iceberg schemas carry no case-sensitivity information, so column names are
/// matched in lowercased form (see [`ColumnNameSet`]).
fn used_columns(
    table_schema: &ArrowSchema,
    schema: &Relation,
    config: &IcebergReaderConfig,
) -> Vec<String> {
    let used = used_sql_columns(schema, config);
    table_schema
        .fields()
        .iter()
        .filter(|f| used.contains(f.name()))
        .map(|f| f.name().to_string())
        .collect()
}

/// Quoted, comma-separated column list for `select {} from snapshot` queries.
fn used_column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(quote_sql_identifier)
        .collect::<Vec<_>>()
        .join(", ")
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
    queue: InputQueue,
}

impl IcebergInputEndpointInner {
    fn new(
        endpoint_name: &str,
        config: IcebergReaderConfig,
        pipeline_config: &PipelineConfig,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        let queue = InputQueue::new(consumer.clone());
        // Share the pipeline-wide `RuntimeEnv` so that scans against the
        // iceberg table spill to the bounded memory pool and on-disk scratch
        // dir alongside every other datafusion user in the pipeline.
        let datafusion = create_session_context(pipeline_config, runtime_env);
        Self {
            endpoint_name: endpoint_name.to_string(),
            config,
            consumer,
            datafusion,
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

    /// Load the entire table snapshot as a single
    /// "select <used_columns> where <filter>" query.
    async fn read_unordered_snapshot(
        &self,
        used_columns: &[String],
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) {
        let column_names = used_column_list(used_columns);

        let mut snapshot_query = format!("select {column_names} from snapshot");
        if let Some(filter) = &self.config.snapshot_filter {
            snapshot_query = format!("{snapshot_query} where {filter}");
        }

        // Execute the snapshot query; push snapshot data to the circuit.
        info!(
            "iceberg {}: reading initial snapshot: {snapshot_query}",
            &self.endpoint_name,
        );

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
        used_columns: &[String],
        input_stream: &mut dyn ArrowStream,
        schema: &Relation,
        receiver: &mut Receiver<PipelineState>,
    ) {
        self.read_ordered_snapshot_inner(used_columns, input_stream, schema, receiver)
            .await
            .unwrap_or_else(|e| self.consumer.error(true, e, None));
    }

    async fn read_ordered_snapshot_inner(
        &self,
        used_columns: &[String],
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

        let column_names = used_column_list(used_columns);

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

        let used_columns = match self.prepare_snapshot_query(&table, &schema).await {
            Err(e) => {
                let _ = init_status_sender.send(Err(e)).await;
                return;
            }
            Ok(used_columns) => used_columns,
        };

        // Code before this point is part of endpoint initialization.
        // After this point, the thread should continue running until it receives a
        // shutdown command from the controller.
        let _ = init_status_sender.send(Ok(())).await;

        if self.config.snapshot() && self.config.timestamp_column.is_none() {
            // Read snapshot chunk-by-chunk.
            self.read_unordered_snapshot(&used_columns, input_stream.as_mut(), &mut receiver)
                .await;
        } else if self.config.snapshot() {
            // Read the entire snapshot in one query.
            self.read_ordered_snapshot(
                &used_columns,
                input_stream.as_mut(),
                &schema,
                &mut receiver,
            )
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
    ///
    /// Returns the columns snapshot queries must select (see [`used_columns`]);
    /// empty when the configuration requires no snapshot.
    async fn prepare_snapshot_query(
        &self,
        table: &IcebergTable,
        schema: &Relation,
    ) -> Result<Vec<String>, ControllerError> {
        if !self.config.snapshot() {
            return Ok(Vec::new());
        }

        // Validate the filter before `config_referenced_columns` extracts
        // column names from it, so an invalid filter fails with a parse error
        // rather than being silently ignored during column selection.
        self.validate_snapshot_filter()?;

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

        let used_columns = used_columns(provider.schema().as_ref(), schema, &self.config);
        if used_columns.is_empty() {
            return Err(ControllerError::invalid_transport_configuration(
                &self.endpoint_name,
                "the connector would read no columns: none of the columns declared in the SQL table exist in the Iceberg table (columns skipped via the 'skip_unused_columns' table property don't count); check that the connector points to the correct table",
            ));
        }

        self.datafusion
            .register_table("snapshot", Arc::new(provider))
            .map_err(|e| {
                ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("failed to register table snapshot with datafusion: {e}"),
                )
            })?;

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

        Ok(used_columns)
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field as ArrowField};
    use feldera_types::program_schema::{ColumnType, PropertyValue, SourcePosition, SqlIdentifier};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn storage_factory_constructs() {
        // Smoke test; scheme dispatch is covered upstream in iceberg-rust.
        let _factory = storage_factory();
    }

    fn config(value: serde_json::Value) -> IcebergReaderConfig {
        serde_json::from_value(value).unwrap()
    }

    fn snapshot_config() -> IcebergReaderConfig {
        config(json!({"mode": "snapshot", "metadata_location": "/tmp/metadata.json"}))
    }

    /// SQL field named `name`; `nullable`, `unused`, and `default` control
    /// whether `skip_unused_columns` may skip it.
    fn field(name: &str, nullable: bool, unused: bool, default: Option<&str>) -> Field {
        let mut field = Field::new(SqlIdentifier::new(name, false), ColumnType::int(nullable))
            .with_unused(unused);
        field.default = default.map(String::from);
        field
    }

    fn relation(fields: Vec<Field>, skip_unused_columns: bool) -> Relation {
        let zero = SourcePosition {
            start_line_number: 0,
            start_column: 0,
            end_line_number: 0,
            end_column: 0,
        };
        let mut properties = BTreeMap::new();
        if skip_unused_columns {
            properties.insert(
                "skip_unused_columns".to_string(),
                PropertyValue {
                    value: "true".to_string(),
                    key_position: zero,
                    value_position: zero,
                },
            );
        }
        Relation::new(
            SqlIdentifier::new("test_table", false),
            fields,
            false,
            properties,
        )
    }

    fn arrow_schema(names: &[&str]) -> ArrowSchema {
        ArrowSchema::new(
            names
                .iter()
                .map(|name| ArrowField::new(*name, DataType::Int32, true))
                .collect::<Vec<_>>(),
        )
    }

    /// The projection is the intersection of the Iceberg schema and the SQL
    /// declaration: table-only columns (`b`, `uuid`) are never read, and
    /// SQL-only columns (`missing`) are never selected. Output follows table
    /// schema order, not declaration order.
    #[test]
    fn used_columns_selects_sql_declared_table_columns_in_table_order() {
        let schema = relation(
            vec![
                field("c", true, false, None),
                field("a", true, false, None),
                field("missing", true, false, None),
            ],
            false,
        );
        let table_schema = arrow_schema(&["a", "b", "c", "uuid"]);

        assert_eq!(
            used_columns(&table_schema, &schema, &snapshot_config()),
            vec!["a", "c"]
        );
    }

    /// Names match case-insensitively, and the query uses the table's own
    /// spelling (quoted identifiers resolve case-sensitively in datafusion).
    #[test]
    fn used_columns_matches_case_insensitively() {
        let schema = relation(vec![field("tstz", true, false, None)], false);
        let table_schema = arrow_schema(&["TsTz"]);

        assert_eq!(
            used_columns(&table_schema, &schema, &snapshot_config()),
            vec!["TsTz"]
        );
    }

    #[test]
    fn used_columns_empty_when_nothing_matches() {
        let schema = relation(vec![field("x", true, false, None)], false);
        let table_schema = arrow_schema(&["a", "b"]);

        assert!(used_columns(&table_schema, &schema, &snapshot_config()).is_empty());
    }

    /// With the `skip_unused_columns` table property, a column is dropped only
    /// when it is unused *and* omittable (nullable or defaulted) *and* not
    /// referenced by `snapshot_filter`. Without the property, everything the
    /// SQL table declares is read.
    #[test]
    fn skip_unused_columns_drops_only_omittable_unreferenced_columns() {
        let fields = vec![
            field("used", false, false, None), // read by a view -> kept
            field("unused_nullable", true, true, None), // skippable -> dropped
            field("unused_nonnull", false, true, None), // not omittable -> kept
            field("unused_default", false, true, Some("0")), // defaulted -> dropped
            field("unused_filtered", true, true, None), // filter needs it -> kept
        ];
        let table_schema = arrow_schema(&[
            "used",
            "unused_nullable",
            "unused_nonnull",
            "unused_default",
            "unused_filtered",
        ]);
        let config = config(json!({
            "mode": "snapshot",
            "metadata_location": "/tmp/metadata.json",
            "snapshot_filter": "unused_filtered > 10",
        }));

        assert_eq!(
            used_columns(&table_schema, &relation(fields.clone(), false), &config).len(),
            5
        );
        assert_eq!(
            used_columns(&table_schema, &relation(fields, true), &config),
            vec!["used", "unused_nonnull", "unused_filtered"]
        );
    }

    #[test]
    fn used_column_list_quotes_identifiers() {
        let columns = vec![
            "simple".to_string(),
            "with\"quote".to_string(),
            "With Space".to_string(),
        ];

        assert_eq!(
            used_column_list(&columns),
            r#""simple", "with""quote", "With Space""#
        );
    }
}
