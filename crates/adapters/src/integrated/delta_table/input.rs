use crate::catalog::{ArrowStream, InputCollectionHandle};
use crate::format::InputBuffer;
use crate::integrated::delta_table::{delta_input_serde_config, register_storage_handlers};
use crate::transport::{InputEndpoint, InputQueue, InputReaderCommand, IntegratedInputEndpoint};
use crate::util::JobQueue;
use crate::{ControllerError, InputConsumer, InputReader, PipelineState};
use anyhow::{Error as AnyError, Result as AnyResult, anyhow, bail};
use arrow::array::BooleanArray;
use chrono::{DateTime, Utc};
use datafusion::common::DataFusionError;
use datafusion::common::arrow::array::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::physical_plan::{PhysicalExpr, displayable};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use dbsp::circuit::tokio::TOKIO;
use deltalake::datafusion::dataframe::DataFrame;
use deltalake::datafusion::execution::context::SQLOptions;
use deltalake::datafusion::logical_expr::SortExpr;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::datafusion::sql::sqlparser::dialect::GenericDialect;
use deltalake::datafusion::sql::sqlparser::parser::Parser;
use deltalake::datafusion::sql::sqlparser::tokenizer::Token;
use deltalake::kernel::Action;
use deltalake::logstore::{self, IORuntime};
use deltalake::table::builder::ensure_table_uri;
use deltalake::{DeltaTable, DeltaTableBuilder, datafusion};
use feldera_adapterlib::format::{ParseError, StagedInputBuffer};
use feldera_adapterlib::metrics::{ConnectorMetrics, ValueType};
use feldera_adapterlib::transport::{InputQueueEntry, Resume, Watermark, parse_resume_info};
use feldera_adapterlib::utils::datafusion::{
    array_to_string, columns_referenced_by_expression, columns_referenced_by_order_by,
    create_session_context_with, execute_query_collect, execute_singleton_query,
    timestamp_to_sql_expression, validate_sql_expression, validate_sql_order_by,
    validate_timestamp_column,
};
use feldera_storage::tokio::TOKIO_DEDICATED_IO;
use feldera_types::adapter_stats::ConnectorHealth;
use feldera_types::config::{ConnectorProjection, FtModel, PipelineConfig};
use feldera_types::program_schema::{Field, Relation};
use feldera_types::transport::delta_table::{DeltaTableReaderConfig, DeltaTableTransactionMode};
use futures_util::StreamExt;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::cmp::min;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::select;
use tokio::sync::watch::{Receiver, Sender, channel};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::sleep;
use tracing::{debug, info, trace, warn};

/// Polling interval when following a delta table.
const POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// Calculate exponential backoff delay for retrying delta log reads.
/// Starts at 0.5s, doubles each retry, caps at 32s, plus uniform jitter up to 25% of that delay
/// (capped at `max_delay_ms`) to reduce synchronized retries.
fn calculate_backoff_delay(retry_count: u32) -> Duration {
    let base_delay_ms: u64 = 500; // 0.5 seconds
    let max_delay_ms: u64 = 32_000; // 32 seconds
    let delay_ms = min(
        base_delay_ms.checked_shl(retry_count).unwrap_or(u64::MAX),
        max_delay_ms,
    );
    let jitter_span = (delay_ms / 4).max(1);
    let jitter_ms = rand::thread_rng().gen_range(0..jitter_span);
    Duration::from_millis(min(delay_ms + jitter_ms, max_delay_ms))
}

/// Default object store timeout. When not explicitly set by the user,
/// we use a large timeout value to avoid this issue:
/// https://github.com/delta-io/delta-rs/issues/2595, which is common for
/// tables with large files.
const DEFAULT_OBJECT_STORE_TIMEOUT: &str = "120s";

const REPORT_ERROR: &str =
    "please report this error to developers (https://github.com/feldera/feldera/issues)";

/// Semaphore used to control the number of concurrent reads to the object store
/// as a workaround for https://github.com/apache/arrow-rs-object-store/issues/14.
/// (see `max_concurrent_readers` in `DeltaTableReaderConfig`).
static DELTA_READER_SEMAPHORE: std::sync::LazyLock<Semaphore> =
    std::sync::LazyLock::new(|| Semaphore::new(DEFAULT_MAX_CONCURRENT_READERS));

/// Default cap for concurrent Delta object-store reads, used both as the
/// initial token count for `DELTA_READER_SEMAPHORE` and as the fallback
/// for DataFusion's `target_partitions` when neither
/// `DELTA_DF_TARGET_PARTITIONS` nor `max_concurrent_readers` is set.
const DEFAULT_MAX_CONCURRENT_READERS: usize = 6;

/// Configured `max_concurrent_readers` value (0 = not set by any connector).
/// Used to detect conflicting values of `max_concurrent_readers` during parallel
/// connector initialization.
static MAX_CONCURRENT_READERS: AtomicUsize = AtomicUsize::new(0);

/// Takes a column name from a DeltaLake schema and returns a quoted string
/// that can be used in datafusion queries like `select "foo""bar" from my_table`.
fn quote_sql_identifier<S: AsRef<str>>(ident: S) -> String {
    format!("\"{}\"", ident.as_ref().replace("\"", "\"\""))
}

/// Format a DataFusion error, appending actionable guidance when the
/// underlying variant is `ResourcesExhausted` (the shared memory pool ran
/// out). `find_root` walks past `Context(...)` / `ArrowError(...)` wrappers
/// so the check is robust to the deeply nested errors DataFusion typically
/// produces during sort/merge.
fn format_datafusion_error(prefix: &str, e: &DataFusionError) -> String {
    let base = format!("{prefix}: {e:?}");
    if matches!(e.find_root(), DataFusionError::ResourcesExhausted(_)) {
        format!(
            "{base}\n\
             DataFusion memory pool is exhausted. \
             Consider increasing 'datafusion_memory_mb' in the pipeline runtime config. \
             If raising the budget is not an option, reduce 'io_workers' / 'workers' or \
             set the env var 'DELTA_DF_TARGET_PARTITIONS=1' to lower per-scan parallelism.\n"
        )
    } else {
        base
    }
}

/// Build the `DataFrame` that streams a CDC transaction to the circuit.
///
/// Equivalent (in SQL) to:
///
/// ```sql
/// SELECT * FROM (
///     SELECT * FROM cdc_adds   [WHERE <filter>]
///     EXCEPT ALL
///     SELECT * FROM cdc_removes [WHERE <filter>]
/// ) ORDER BY <order_by>
/// ```
///
/// When `removes_df` is `None`, the set difference is skipped:
///
/// ```sql
/// SELECT * FROM cdc_adds [WHERE <filter>] ORDER BY <order_by>
/// ```
///
/// `EXCEPT ALL` (multiset difference) cancels each Remove row against
/// exactly one matching Add row; plain `EXCEPT` would collapse
/// duplicates and under-count legitimately repeated inserts.
///
/// The optional `filter` is pushed into both sides of the set
/// difference so each Remove only has to cancel against rows that
/// would have been ingested anyway. This also shrinks the inputs to
/// `EXCEPT ALL`, which sorts both relations.
///
/// The filter is parsed against each `DataFrame`'s own schema, so
/// column references resolve to the correct table qualifier
/// (`cdc_adds.col` vs `cdc_removes.col`). The two sides cannot share
/// a single parsed `Expr` because column references would otherwise
/// point at the wrong relation after `except`.
///
/// Caveat: `EXCEPT ALL` relies on `arrow_row::RowConverter`, which in
/// the currently pinned `arrow-row` does not support `Map` columns.
/// Pure-append transactions on tables with `Map` columns are
/// unaffected (the `EXCEPT ALL` branch isn't taken when `removes_df`
/// is `None`); transactions that do produce Removes fail here with
/// `NotImplemented`. See issue:
///   - https://github.com/apache/datafusion/issues/15428
///   - https://github.com/apache/arrow-rs/issues/7879
///
/// Free function (not a method on the connector) so a unit test can
/// inspect the resulting logical plan without standing up the full
/// connector.
pub(super) fn build_cdc_dataframe(
    adds_df: DataFrame,
    removes_df: Option<DataFrame>,
    filter: Option<&str>,
    order_by: &str,
    description: &str,
) -> AnyResult<DataFrame> {
    let apply_filter = |df: DataFrame, side: &'static str| -> AnyResult<DataFrame> {
        let Some(filter) = filter else {
            return Ok(df);
        };
        let expr = df
            .parse_sql_expr(filter)
            .map_err(|e| anyhow!("invalid 'filter' expression '{filter}': {e}"))?;
        df.filter(expr).map_err(|e| {
            anyhow!("internal error processing {description}; {REPORT_ERROR}; error applying 'filter' to '{side}': {e}")
        })
    };

    let adds_df = apply_filter(adds_df, "cdc_adds")?;

    let result_df = match removes_df {
        None => adds_df,
        Some(removes_df) => {
            let removes_df = apply_filter(removes_df, "cdc_removes")?;
            adds_df.except(removes_df).map_err(|e| {
                anyhow!("failed to build the CDC set difference for {description}: {e}. This typically means the Delta table contains a `Map` column, which the CDC deduplication step (`EXCEPT ALL`) does not yet support.")
            })?
        }
    };

    let sort_exprs = parse_cdc_order_by(&result_df, order_by)?;
    result_df.sort(sort_exprs).map_err(|e| {
        anyhow!("internal error processing {description}; {REPORT_ERROR}; error applying 'cdc_order_by': {e}")
    })
}

/// Parse the `cdc_order_by` clause into DataFusion sort expressions resolved
/// against df's schema.
///
/// `cdc_order_by` is the body of an ORDER BY clause: a comma-separated list
/// of keys, each optionally annotated with ASC / DESC and NULLS
/// FIRST / NULLS LAST.
pub(super) fn parse_cdc_order_by(df: &DataFrame, order_by: &str) -> AnyResult<Vec<SortExpr>> {
    let mut parser = Parser::new(&GenericDialect)
        .try_with_sql(order_by)
        .map_err(|e| anyhow!("invalid 'cdc_order_by' expression '{order_by}': {e}"))?;
    let order_exprs = parser
        .parse_comma_separated(Parser::parse_order_by_expr)
        .map_err(|e| anyhow!("invalid 'cdc_order_by' expression '{order_by}': {e}"))?;
    if parser.peek_token().token != Token::EOF {
        bail!(
            "invalid 'cdc_order_by' expression '{order_by}': unexpected trailing input near '{}'",
            parser.peek_token()
        );
    }

    order_exprs
        .into_iter()
        .map(|order_expr| {
            let key = df
                .parse_sql_expr(&order_expr.expr.to_string())
                .map_err(|e| anyhow!("invalid 'cdc_order_by' expression '{order_by}': {e}"))?;
            // Match DataFusion's SQL planner defaults: a missing direction is
            // ASC, and missing NULLS follows nulls_max (last for ASC).
            // - https://datafusion.apache.org/user-guide/sql/select.html#order-by-clause
            // - https://datafusion.apache.org/user-guide/configs.html
            let asc = order_expr.options.asc.unwrap_or(true);
            let nulls_first = order_expr.options.nulls_first.unwrap_or(!asc);
            Ok(key.sort(asc, nulls_first))
        })
        .collect()
}

/// Integrated input connector that reads from a delta table.
pub struct DeltaTableInputEndpoint {
    endpoint_name: String,
    config: DeltaTableReaderConfig,
    projection: Option<ConnectorProjection>,
    datafusion: SessionContext,
    consumer: Box<dyn InputConsumer>,
}

impl DeltaTableInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &DeltaTableReaderConfig,
        projection: Option<ConnectorProjection>,
        pipeline_config: &PipelineConfig,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        register_storage_handlers();

        // if `DELTA_DF_TARGET_PARTITIONS` env var (process-wide override) is set,
        // override default target partitions with that value. Target partitions
        // controls the number of parallel tasks DataFusion uses for scanning during the
        // snapshot phase.
        let env_target_partitions = match std::env::var("DELTA_DF_TARGET_PARTITIONS").ok() {
            None => None,
            Some(s) => match s.parse::<usize>() {
                Ok(n) if n > 0 => Some(n),
                _ => {
                    warn!(
                        "delta_table {endpoint_name}: ignoring DELTA_DF_TARGET_PARTITIONS={s:?}; expected a positive integer"
                    );
                    None
                }
            },
        };
        if let Some(n) = env_target_partitions {
            info!("delta_table {endpoint_name}: DELTA_DF_TARGET_PARTITIONS={n} overriding default");
        }

        let env_batch_size = match std::env::var("DELTA_DF_BATCH_SIZE").ok() {
            None => None,
            Some(s) => match s.parse::<usize>() {
                Ok(n) if n > 0 => {
                    info!("delta_table {endpoint_name}: applying DELTA_DF_BATCH_SIZE={n}");
                    Some(n)
                }
                _ => {
                    warn!(
                        "delta_table {endpoint_name}: ignoring DELTA_DF_BATCH_SIZE={s:?}; expected a positive integer"
                    );
                    None
                }
            },
        };

        // Configure datafusion not to generate Utf8View arrow types, which are
        // not yet supported by the `serde_arrow` crate. The `SessionContext`
        // shares the pipeline-wide `RuntimeEnv` so that the CDC-mode ORDER BY
        // query spills to the same bounded memory pool and on-disk scratch
        // dir as every other datafusion user in the pipeline.
        //
        // `target_partitions` inherits `create_session_context_with`'s
        // worker-derived default; only override if `DELTA_DF_TARGET_PARTITIONS`
        // was set explicitly. Same for `batch_size`.
        let datafusion = create_session_context_with(pipeline_config, runtime_env, |cfg| {
            let mut cfg = cfg.set_bool(
                "datafusion.execution.parquet.schema_force_view_types",
                false,
            );
            if let Some(n) = env_target_partitions {
                cfg = cfg.set_usize("datafusion.execution.target_partitions", n);
            }
            if let Some(n) = env_batch_size {
                cfg = cfg.set_usize("datafusion.execution.batch_size", n);
            }
            cfg
        });

        Self {
            endpoint_name: endpoint_name.to_string(),
            config: config.clone(),
            projection,
            datafusion,
            consumer,
        }
    }
}

impl InputEndpoint for DeltaTableInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::AtLeastOnce)
    }
}
impl IntegratedInputEndpoint for DeltaTableInputEndpoint {
    fn open(
        self: Box<Self>,
        input_handle: &InputCollectionHandle,
        resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(DeltaTableInputReader::new(
            self.endpoint_name,
            self.config,
            self.projection,
            self.datafusion,
            self.consumer,
            input_handle,
            resume_info,
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
        projection: Option<ConnectorProjection>,
        datafusion: SessionContext,
        consumer: Box<dyn InputConsumer>,
        input_handle: &InputCollectionHandle,
        resume_info: Option<JsonValue>,
    ) -> AnyResult<Self> {
        let (sender, receiver) = channel(PipelineState::Paused);
        let receiver_clone = receiver.clone();

        let resume_info = if let Some(resume_info) = resume_info {
            let resume_info = parse_resume_info::<DeltaResumeInfo>(&resume_info)?;
            match &resume_info {
                DeltaResumeInfo { eoi: true, .. } => {
                    info!(
                        "delta_table {endpoint_name}: skipping connector initialization because the connector is already in the end-of-input state"
                    );
                }
                DeltaResumeInfo {
                    version: Some(version),
                    snapshot_timestamp: Some(snapshot_timestamp),
                    ..
                } => {
                    info!(
                        "delta_table {endpoint_name}: resuming to ingest the initial snapshot from table version {version} at timestamp {snapshot_timestamp}"
                    );
                }
                DeltaResumeInfo {
                    version: Some(version),
                    ..
                } => {
                    info!(
                        "delta_table {endpoint_name}: resuming in follow mode from table version {version}"
                    );
                }
                DeltaResumeInfo { version: None, .. } => {
                    info!("delta_table {endpoint_name}: resuming from clean state");
                }
            }

            Some(resume_info)
        } else {
            None
        };

        let eoi = if let Some(resume_info) = &resume_info {
            resume_info.eoi
        } else {
            false
        };

        // Used to communicate the status of connector initialization.
        let (init_status_sender, mut init_status_receiver) =
            mpsc::channel::<Result<(), ControllerError>>(1);

        if config.num_parsers == 0 {
            bail!("invalid 'num_parsers' value: 'num_parsers' must be greater than 0");
        }

        if config.end_version.is_some() && !config.follow() {
            bail!(
                "the 'end_version' property is not valid in '{}' mode; it can only be used when the DeltaLake connector is configured with 'follow', 'snapshot_and_follow', or 'cdc' mode",
                config.mode
            )
        }

        if config.version.is_some()
            && config.end_version.is_some()
            && config.version.unwrap() >= config.end_version.unwrap()
        {
            bail!("'end_version' must be greater than 'version'")
        }

        // If the config specifies max_concurrent_readers, adjust the number of tokens
        // in the semaphore. Connectors are initialized in parallel, so we atomically
        // record the configured value and only adjust the semaphore once.
        if let Some(max_concurrent_readers) = config.max_concurrent_readers {
            let max_concurrent_readers = max_concurrent_readers as usize;

            if max_concurrent_readers == 0 {
                bail!(
                    "invalid 'max_concurrent_readers' value: 'max_concurrent_readers' must be greater than 0"
                );
            }

            let first_setter = match MAX_CONCURRENT_READERS.compare_exchange(
                0,
                max_concurrent_readers,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => true,
                Err(current) if current == max_concurrent_readers => false,
                Err(_) => {
                    bail!(
                        "found conflicting values of the `max_concurrent_readers` attribute: this is a global setting that affects all Delta Lake connectors, and not just the connector where it is specified; if multiple connectors specify `max_concurrent_readers`, they must all use the same value."
                    );
                }
            };

            if first_setter {
                info!(
                    "delta_table {endpoint_name}: adjusting the number of concurrent readers to {max_concurrent_readers}"
                );

                // The semaphore doesn't allow changing the total token count directly, but at this point,
                // while initializing connectors, none of the tokens have been acquired, so we can adjust
                // the total number of tokens by adjusting currently available tokens.
                let available_permits = DELTA_READER_SEMAPHORE.available_permits();
                if max_concurrent_readers > available_permits {
                    DELTA_READER_SEMAPHORE.add_permits(max_concurrent_readers - available_permits);
                } else if max_concurrent_readers < available_permits {
                    DELTA_READER_SEMAPHORE
                        .forget_permits(available_permits - max_concurrent_readers);
                }
            }
        };

        if !config.object_store_config.contains_key("timeout") {
            config.object_store_config.insert(
                "timeout".to_string(),
                DEFAULT_OBJECT_STORE_TIMEOUT.to_string(),
            );
        }

        Self::prepare_unity_config(&config.object_store_config);

        let input_stream = input_handle
            .handle
            .configure_arrow_deserializer(delta_input_serde_config())?;
        let schema = input_handle.schema.clone();

        let endpoint = Arc::new(DeltaTableInputEndpointInner::new(
            &endpoint_name,
            config,
            datafusion,
            consumer,
            schema,
            projection,
            resume_info.clone(),
        ));

        // This is needed to initialize completed_frontier for the connector. It's not ideal, as the completion
        // status for the frontier will be set to the current time instead of whenever the table version in resume_info
        // was actually processed. The right solution is to checkpoint the frontier with the connector.
        if resume_info.is_some() {
            endpoint.queue.push_with_aux(
                (None, Vec::new()),
                Utc::now(),
                QueueEntry::ResumeInfo(resume_info),
            );
        }

        if eoi {
            endpoint
                .metrics
                .phase
                .store(DeltaPhase::Completed as u64, Ordering::Relaxed);
            endpoint.consumer.eoi();
        } else {
            let endpoint_clone = endpoint.clone();
            thread::Builder::new()
                .name(format!("{endpoint_name}-delta-input-tokio-wrapper"))
                .spawn(move || {
                    TOKIO.block_on(async {
                        let _ = endpoint_clone
                            .worker_task(input_stream, receiver_clone, init_status_sender)
                            .await;
                    })
                })
                .expect("failed to spawn delta connector tokio wrapper thread");

            init_status_receiver.blocking_recv().ok_or_else(|| {
                ControllerError::input_transport_error(
                    &endpoint.endpoint_name,
                    true,
                    anyhow!("worker thread terminated unexpectedly during initialization"),
                )
            })??;
        }

        Ok(Self {
            sender,
            inner: endpoint.clone(),
        })
    }

    /// deltalake_catalog_unity expects all configuration settings to be provided as environment variables.
    /// This function sets environment variables for all configuration settings that start with "unity_" or "databricks_".
    fn prepare_unity_config(object_store_config: &HashMap<String, String>) {
        for (key, val) in object_store_config.iter() {
            if key.to_lowercase().starts_with("unity_")
                || key.to_lowercase().starts_with("databricks_")
            {
                // TODO: Audit that the environment access only happens in single-threaded code.
                unsafe { std::env::set_var(key.to_uppercase(), val) };
            }
        }
    }
}

impl InputReader for DeltaTableInputReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        match command {
            InputReaderCommand::Replay { .. } => panic!(
                "replay command is not supported by DeltaTableInputReader; this is a bug, please report it to developers"
            ),
            InputReaderCommand::Extend => {
                let _ = self.sender.send_replace(PipelineState::Running);
            }
            InputReaderCommand::Pause => {
                let _ = self.sender.send_replace(PipelineState::Paused);
            }
            InputReaderCommand::Queue {
                checkpoint_requested,
            } => {
                // When initiating a checkpoint, try to stop at a delta table transaction boundary.
                let stop_at: &dyn Fn(&QueueEntry) -> bool = if checkpoint_requested {
                    &|entry: &QueueEntry| {
                        matches!(
                            entry,
                            QueueEntry::ResumeInfo(Some(_)) | QueueEntry::Rollback
                        )
                    }
                } else {
                    &|_: &QueueEntry| false
                };

                let (total, _, resume_info) = self.inner.queue.flush_with_aux_until(stop_at);
                let resume_status = match resume_info.last() {
                    None => self.inner.last_resume_status.lock().unwrap().clone(),
                    Some((_ts, QueueEntry::ResumeInfo(resume_info))) => resume_info.clone(),
                    Some((_ts, QueueEntry::Rollback)) => Some(
                        self.inner
                            .last_checkpointable_status
                            .lock()
                            .unwrap()
                            .clone(),
                    ),
                };

                *self.inner.last_resume_status.lock().unwrap() = resume_status.clone();
                if let Some(resume_status) = &resume_status {
                    *self.inner.last_checkpointable_status.lock().unwrap() = resume_status.clone();
                }

                let resume = match resume_status {
                    None => Resume::Barrier,
                    Some(delta_resume_info) => delta_resume_info.to_resume(),
                };

                // We use the same format (DeltaResumeInfo) for resume info and watermark metadata.
                self.inner.consumer.extended(
                    total,
                    Some(resume),
                    resume_info
                        .into_iter()
                        .filter_map(|(timestamp, metadata)| match metadata {
                            QueueEntry::ResumeInfo(resume_info) => Some(Watermark::new(
                                timestamp,
                                resume_info.map(|m| serde_json::to_value(m).unwrap()),
                            )),
                            QueueEntry::Rollback => None,
                        })
                        .collect(),
                );
            }
            InputReaderCommand::Disconnect => {
                let _ = self.sender.send_replace(PipelineState::Terminated);
            }
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

/// Resume info stored in each checkpoint for the DeltaTableInputEndpoint.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct DeltaResumeInfo {
    /// Table version where the connector stopped reading before the checkpoint,
    /// `None` if the connector hasn't started reading yet.
    ///
    /// If the connector is still processing the initial snapshot, this is the version of the snapshot.
    version: Option<i64>,

    /// The upper bound of timestamps previously ingested as part of the initial snapshot.
    ///
    /// Only set if the connector is still processing the initial snapshot,
    /// and if `timestamp_column` is set in the configuration.
    ///
    /// The connector will resume loading the snapshot from this timestamp after
    /// resuming from a checkpoint.
    snapshot_timestamp: Option<String>,

    /// True if the connector reached the end-of-input state.
    eoi: bool,
}

impl DeltaResumeInfo {
    /// Checkpoint taken before the connector started ingesting data.
    fn initial() -> Self {
        Self {
            version: None,
            snapshot_timestamp: None,
            eoi: false,
        }
    }

    /// Checkpoint taken after the connector reached the end-of-input state.
    fn eoi() -> Self {
        Self {
            version: None,
            snapshot_timestamp: None,
            eoi: true,
        }
    }

    /// Checkpoint taken in follow mode, after the connector has ingested the initial snapshot
    /// (if any).
    ///
    /// * `version` - the last processed version of the table.
    /// * `eoi` - true if the connector reached the end-of-input state
    ///   (can happen if `end_version` is set in the configuration or if the connector is
    ///   configured to ingest the initial snapshot only).
    fn follow_mode(version: i64, eoi: bool) -> Self {
        Self {
            version: Some(version),
            snapshot_timestamp: None,
            eoi,
        }
    }

    /// Checkpoint taken while the connector is ingesting the initial snapshot.
    ///
    /// * `version` is the version of the table being snapshotted.
    /// * `timestamp` is the timestamp of the last record ingested as part of the initial snapshot.
    fn snapshot_mode(version: i64, timestamp: &str) -> Self {
        Self {
            version: Some(version),
            snapshot_timestamp: Some(timestamp.to_string()),
            eoi: false,
        }
    }

    fn to_resume(&self) -> Resume {
        Resume::Seek {
            seek: serde_json::to_value(self).unwrap(),
        }
    }
}

/// Current phase of a delta table input connector.
// Using repr u64 as we using AtomicU64 to store the phase in metrics
#[repr(u64)]
enum DeltaPhase {
    LoadingSnapshot = 0,
    Follow = 1,
    Completed = 2,
}

struct DeltaTableMetrics {
    /// Current phase of the connector (see [DeltaPhase]).
    phase: AtomicU64,
    /// Unix epoch seconds when snapshot phase finished; 0 if not yet complete.
    snapshot_completed_ts: AtomicU64,
    /// Total records loaded during snapshot phase.
    snapshot_records_total: AtomicU64,
    /// Number of Feldera follow transactions started (`follow-*` labels allocated).
    follow_transaction_starts: AtomicU64,
    /// Number of Feldera snapshot transactions actually started: incremented when a `snapshot-*`
    /// label is propagated to a queue entry, not merely when the label is allocated.
    snapshot_transaction_starts: AtomicU64,
    /// Last ingested Delta table version; [`VERSION_METRIC_UNSET`] until the
    /// first successful ingest.
    last_ingested_version: AtomicU64,
    /// Target Delta table version for the in-flight catchup window;
    /// [`VERSION_METRIC_UNSET`] when no catchup window is active.
    catchup_target_version: AtomicU64,
}

/// Sentinel stored in version gauges before a value is available.
const VERSION_METRIC_UNSET: u64 = u64::MAX;

impl DeltaTableMetrics {
    fn new() -> Self {
        Self {
            phase: AtomicU64::new(DeltaPhase::LoadingSnapshot as u64),
            snapshot_completed_ts: AtomicU64::new(0),
            snapshot_records_total: AtomicU64::new(0),
            follow_transaction_starts: AtomicU64::new(0),
            snapshot_transaction_starts: AtomicU64::new(0),
            last_ingested_version: AtomicU64::new(VERSION_METRIC_UNSET),
            catchup_target_version: AtomicU64::new(VERSION_METRIC_UNSET),
        }
    }

    fn version_metric(value: u64) -> f64 {
        match value {
            VERSION_METRIC_UNSET => -1.0,
            version => version as f64,
        }
    }

    fn set_last_ingested_version(&self, version: i64) {
        debug_assert!(version >= 0, "Delta table version must be non-negative");
        self.last_ingested_version
            .store(version as u64, Ordering::Relaxed);
    }

    fn set_catchup_target_version(&self, version: i64) {
        debug_assert!(version >= 0, "Delta table version must be non-negative");
        self.catchup_target_version
            .store(version as u64, Ordering::Relaxed);
    }

    fn clear_catchup_target_version(&self) {
        self.catchup_target_version
            .store(VERSION_METRIC_UNSET, Ordering::Relaxed);
    }

    fn last_ingested_version_metric(&self) -> f64 {
        Self::version_metric(self.last_ingested_version.load(Ordering::Relaxed))
    }

    fn catchup_target_version_metric(&self) -> f64 {
        Self::version_metric(self.catchup_target_version.load(Ordering::Relaxed))
    }
}

impl ConnectorMetrics for DeltaTableMetrics {
    fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
        vec![
            (
                "input_connector_delta_phase",
                "Current phase: 0=loading_snapshot, 1=follow/streaming, 2=completed.",
                ValueType::Gauge,
                self.phase.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_delta_snapshot_completed_seconds",
                "Unix epoch seconds when the snapshot phase finished (0 if not yet complete).",
                ValueType::Gauge,
                self.snapshot_completed_ts.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_delta_snapshot_records_total",
                "Total records loaded during the snapshot phase.",
                ValueType::Counter,
                self.snapshot_records_total.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_delta_follow_transaction_starts",
                "Number of Feldera follow transactions started by this connector.",
                ValueType::Counter,
                self.follow_transaction_starts.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_delta_snapshot_transaction_starts",
                "Number of Feldera snapshot transactions started by this connector.",
                ValueType::Counter,
                self.snapshot_transaction_starts.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_delta_last_ingested_version",
                "Last Delta table version processed by this connector (-1 if none yet).",
                ValueType::Gauge,
                self.last_ingested_version_metric(),
            ),
            (
                "input_connector_delta_catchup_target_version",
                "Target Delta table version for the in-flight catchup window (-1 if none).",
                ValueType::Gauge,
                self.catchup_target_version_metric(),
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

/// In-flight catchup transaction state for follow/CDC modes.
#[derive(Debug, Default)]
struct CatchupFollowState {
    /// Latest Delta version to include in the current Feldera transaction.
    target: Option<i64>,
    /// Label for the current Feldera transaction, if one has been started.
    transaction: Option<Option<String>>,
}

/// A set of column names compared case-insensitively. Delta schemas carry no
/// case-sensitivity information, so names are stored and probed in lowercased
/// form (mirrors the long-standing matching in [`used_columns`](DeltaTableInputEndpointInner::used_columns)).
///
/// SQL is case-sensitive for quoted column names, but an external table cannot
/// hold two columns with the same lowercase form, so collapsing to a single
/// canonical form is safe here.
#[derive(Default)]
struct ColumnNameSet {
    lowercase: BTreeSet<String>,
}

impl ColumnNameSet {
    fn from_names(names: impl IntoIterator<Item = String>) -> Self {
        let lowercase = names.into_iter().map(|c| c.to_lowercase()).collect();
        Self { lowercase }
    }

    fn contains(&self, name: &str) -> bool {
        self.lowercase.contains(&name.to_lowercase())
    }
}

struct DeltaTableInputEndpointInner {
    endpoint_name: String,
    schema: Relation,
    config: DeltaTableReaderConfig,
    projection: Option<ConnectorProjection>,
    consumer: Box<dyn InputConsumer>,
    datafusion: SessionContext,

    /// Index of the last transaction initiated by this connector.
    ///
    /// This is different from the transaction ID assigned to the transaction by the controller.
    /// Used for debugging to track the number of transactions initiated by this connector.
    transaction_index: AtomicUsize,

    /// The latest resume status of this endpoint:
    /// * Initialized to `None` or `Some(version)` (when resume_info is specified) on initialization.
    /// * Updated to `Some(new_version)` after advancing to the next table version in the transaction log
    ///   in follow mode or after ingesting the initial snapshot.
    last_resume_status: Mutex<Option<DeltaResumeInfo>>,

    /// The latest checkpointable status of this endpoint.
    last_checkpointable_status: Mutex<DeltaResumeInfo>,

    queue: Arc<InputQueue<QueueEntry, StagedInputBuffer>>,
    metrics: Arc<DeltaTableMetrics>,

    /// Active catchup Feldera transaction, shared between the follow loop and `execute_df`.
    catchup_follow_state: Mutex<CatchupFollowState>,

    /// SQL columns the connector actually reads (skippable unused columns
    /// removed when `skip_unused_columns` is set). A delta column is kept iff it
    /// is a member. Derived once from the immutable SQL schema.
    used_sql_columns: OnceLock<ColumnNameSet>,

    /// SQL columns named by the connector's own expressions -- `filter`,
    /// `snapshot_filter`, `cdc_delete_filter`, `cdc_order_by`. These are kept
    /// even when marked unused, so the expressions can resolve them. Derived
    /// once from the immutable config.
    config_referenced_columns: OnceLock<ColumnNameSet>,
}

#[derive(Debug, Clone)]
enum QueueEntry {
    /// Resume info for the connector after processing this queue entry.
    ResumeInfo(Option<DeltaResumeInfo>),

    /// Sent after failing to read a delta log entry, before retrying or
    /// declaring a fatal error. Makes sure that the connector can be checkpointed
    /// between retries.
    ///
    /// Note: this is not the actual transaction rollback: the current transaction,
    /// if any, will be committed.
    Rollback,
}

impl DeltaTableInputEndpointInner {
    fn new(
        endpoint_name: &str,
        config: DeltaTableReaderConfig,
        datafusion: SessionContext,
        consumer: Box<dyn InputConsumer>,
        schema: Relation,
        projection: Option<ConnectorProjection>,
        resume_info: Option<DeltaResumeInfo>,
    ) -> Self {
        let queue = Arc::new(InputQueue::new(consumer.clone()));

        let metrics = Arc::new(DeltaTableMetrics::new());
        consumer.set_custom_metrics(Arc::clone(&metrics) as Arc<dyn ConnectorMetrics>);

        let resume_status = resume_info.unwrap_or_else(DeltaResumeInfo::initial);

        Self {
            endpoint_name: endpoint_name.to_string(),
            schema,
            config,
            projection,
            consumer,
            datafusion,
            transaction_index: AtomicUsize::new(0),

            // Set version to None by default so that the connector is checkpointable in the initial state.
            last_resume_status: Mutex::new(Some(resume_status.clone())),

            last_checkpointable_status: Mutex::new(resume_status),
            queue,
            metrics,
            catchup_follow_state: Mutex::new(CatchupFollowState::default()),
            used_sql_columns: OnceLock::new(),
            config_referenced_columns: OnceLock::new(),
        }
    }

    fn catchup_target(&self) -> Option<i64> {
        self.catchup_follow_state.lock().unwrap().target
    }

    fn begin_catchup_window(&self, target: i64) {
        let mut state = self.catchup_follow_state.lock().unwrap();
        if state.target.is_none() {
            state.target = Some(target);
            state.transaction = self.new_follow_transaction_label();
            self.metrics.set_catchup_target_version(target);
        }
    }

    /// Return the label for the current catchup Feldera transaction, allocating a new one if
    /// the previous transaction was committed (for example after a `Rollback` queue entry).
    fn catchup_follow_start_transaction(&self) -> Option<Option<String>> {
        let mut state = self.catchup_follow_state.lock().unwrap();
        if state.transaction.is_none() {
            state.transaction = self.new_follow_transaction_label();
        }
        state.transaction.clone()
    }

    fn reset_catchup_follow_state(&self) {
        *self.catchup_follow_state.lock().unwrap() = CatchupFollowState::default();
        self.metrics.clear_catchup_target_version();
    }

    /// The current catchup Feldera transaction was committed after a partial failure; drop its
    /// label so the next ingest attempt starts a new transaction for the same catchup window.
    fn abandon_catchup_follow_transaction(&self) {
        self.catchup_follow_state.lock().unwrap().transaction = None;
    }

    fn is_snapshot_transaction_label(label: &Option<Option<String>>) -> bool {
        label
            .as_ref()
            .and_then(|l| l.as_ref())
            .is_some_and(|l| l.starts_with("snapshot-"))
    }

    fn follow_start_transaction(
        &self,
        fallback: &Option<Option<String>>,
    ) -> Option<Option<String>> {
        match self.config.transaction_mode {
            // Snapshot ingest passes a `snapshot-*` label via `fallback`; follow/CDC use catchup
            // state to batch Delta log versions.
            DeltaTableTransactionMode::Catchup => {
                if Self::is_snapshot_transaction_label(fallback) {
                    self.count_snapshot_transaction_start();
                    fallback.clone()
                } else {
                    self.catchup_follow_start_transaction()
                }
            }
            DeltaTableTransactionMode::Always | DeltaTableTransactionMode::Snapshot => {
                if Self::is_snapshot_transaction_label(fallback) {
                    self.count_snapshot_transaction_start();
                }
                fallback.clone()
            }
            DeltaTableTransactionMode::None => None,
        }
    }

    /// Record that a `snapshot-*` Feldera transaction was actually started, i.e., its label was
    /// propagated to a queue entry (and thence to the controller). Counted here rather than where
    /// the label is allocated, so the metric reflects transactions the connector truly started, not
    /// merely intended to start.
    fn count_snapshot_transaction_start(&self) {
        self.metrics
            .snapshot_transaction_starts
            .fetch_add(1, Ordering::Relaxed);
    }

    fn skip_unused_columns(&self) -> bool {
        // Old-style: property in the connector configuration.
        // New-style: property in the table definition.
        self.config.skip_unused_columns || self.schema.skip_unused_columns()
    }

    fn should_project_columns(&self) -> bool {
        self.projection.is_some() || self.skip_unused_columns()
    }

    fn new_follow_transaction_label(&self) -> Option<Option<String>> {
        if matches!(
            self.config.transaction_mode,
            DeltaTableTransactionMode::Always | DeltaTableTransactionMode::Catchup
        ) {
            self.metrics
                .follow_transaction_starts
                .fetch_add(1, Ordering::Relaxed);
            Some(Some(format!(
                "follow-{}",
                self.transaction_index.fetch_add(1, Ordering::Release)
            )))
        } else {
            None
        }
    }

    /// Latest Delta table version to ingest in the current catchup Feldera transaction,
    /// capped by `end_version` when configured.
    async fn catchup_target_version(&self, table: Arc<DeltaTable>) -> Result<i64, AnyError> {
        let latest = self
            .retry(
                "error reading the latest table version for catchup transaction",
                Some("delta-latest-version"),
                move || {
                    let table = Arc::clone(&table);
                    async move { table.get_latest_version().await }
                },
            )
            .await?;
        let latest = latest as i64;
        Ok(match self.config.end_version {
            Some(end_version) => min(latest, end_version),
            None => latest,
        })
    }

    fn allocate_snapshot_transaction_label(&self) -> Option<Option<String>> {
        if matches!(
            self.config.transaction_mode,
            DeltaTableTransactionMode::Always
                | DeltaTableTransactionMode::Snapshot
                | DeltaTableTransactionMode::Catchup
        ) {
            // The metric is incremented in `follow_start_transaction` when the label is actually
            // propagated, not here where it is merely allocated.
            Some(Some(format!(
                "snapshot-{}",
                self.transaction_index.fetch_add(1, Ordering::Release),
            )))
        } else {
            None
        }
    }

    fn validate_cdc_config(&self) -> Result<(), ControllerError> {
        if self.config.is_cdc() {
            if self.config.cdc_order_by.is_none() {
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

    /// SQL columns the connector reads, matched case-insensitively against Delta
    /// column names. When `skip_unused_columns` is set, skippable unused columns
    /// are removed. Derived once from the immutable SQL schema and cached.
    fn used_sql_columns(&self) -> &ColumnNameSet {
        self.used_sql_columns.get_or_init(|| {
            if let Some(projection) = &self.projection {
                let mut columns = projection.columns.clone();
                let referenced = self.config_referenced_columns();
                columns.extend(
                    self.schema
                        .fields
                        .iter()
                        .filter(|field| referenced.contains(&field.name.name()))
                        .map(|field| field.name.name()),
                );
                return ColumnNameSet::from_names(columns);
            }

            ColumnNameSet::from_names(
                self.schema
                    .fields
                    .iter()
                    .filter(|f| !self.skip_unused_columns() || !self.can_skip_column(f))
                    .map(|f| f.name.name()),
            )
        })
    }

    /// Compute the subset of columns in the Delta table schema that occur in the SQL
    /// table declaration.
    ///
    /// Delta schemas carry no case-sensitivity information, so column names are
    /// matched both as-is and lowercased (see [`ColumnNameSet`]).
    fn used_columns(&self, table: &DeltaTable) -> Vec<String> {
        let used = self.used_sql_columns();
        table
            .snapshot()
            .expect("Delta table snapshot must be loaded before computing used columns")
            .schema()
            .fields()
            .filter(|f| used.contains(f.name()))
            .map(|f| f.name().to_string())
            .collect()
    }

    fn used_column_list(&self, table: &DeltaTable) -> String {
        let columns = self.used_columns(table);

        columns
            .iter()
            .map(quote_sql_identifier)
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// True if a column's *shape* allows omitting it: no user-visible result
    /// depends on it (`unused`), and omitting it lets us substitute NULL or its
    /// default value (it is nullable or has a default).
    ///
    /// This is the shape-only rule; [`can_skip_column`](Self::can_skip_column)
    /// adds the connector-config check before a column is actually skipped.
    fn is_unused_and_omittable(field: &Field) -> bool {
        Relation::is_unused_column_omittable(field)
    }

    /// SQL columns named by the connector's own expressions (`filter`,
    /// `snapshot_filter`, `cdc_delete_filter`, `cdc_order_by`), case-folded for
    /// matching. Derived once and cached.
    ///
    /// These strings are validated during configuration, so they parse here; a
    /// parse error is therefore unreachable and contributes no columns rather
    /// than failing the connector a second time.
    fn config_referenced_columns(&self) -> &ColumnNameSet {
        self.config_referenced_columns.get_or_init(|| {
            let mut columns = BTreeSet::new();
            for expr in [
                &self.config.filter,
                &self.config.snapshot_filter,
                &self.config.cdc_delete_filter,
            ]
            .into_iter()
            .flatten()
            {
                columns.extend(columns_referenced_by_expression(expr).unwrap_or_default());
            }
            if let Some(order_by) = &self.config.cdc_order_by {
                columns.extend(columns_referenced_by_order_by(order_by).unwrap_or_default());
            }
            ColumnNameSet::from_names(columns)
        })
    }

    /// True if a column may actually be skipped: its shape permits omitting it
    /// ([`is_unused_and_omittable`](Self::is_unused_and_omittable)) *and* no
    /// connector expression references it. A referenced column must be read and
    /// must survive projection, or expressions like `cdc_delete_filter` cannot
    /// resolve it.
    fn can_skip_column(&self, field: &Field) -> bool {
        Self::is_unused_and_omittable(field)
            && !self
                .config_referenced_columns()
                .contains(&field.name.name())
    }

    /// Project `df` to the columns the connector needs when `skip_unused_columns`
    /// is set, mirroring the snapshot path's `SELECT <used_columns>`: keep a
    /// column iff the circuit reads it (`used_sql_columns`) or a connector
    /// expression names it (`config_referenced_columns`). The latter covers Delta
    /// metadata columns absent from the SQL schema, e.g. `__feldera_op` /
    /// `__feldera_ts`, which `cdc_delete_filter` / `cdc_order_by` test, so those
    /// expressions can resolve. A Delta table may carry far more physical columns
    /// than the SQL table maps; this keeps the connector from reading the rest
    /// off disk.
    fn project_cdc_columns(&self, df: DataFrame) -> AnyResult<DataFrame> {
        if !self.should_project_columns() {
            return Ok(df);
        }

        let used = self.used_sql_columns();
        let referenced = self.config_referenced_columns();

        // Filter `df`'s own field list (not the SQL schema) so the projection
        // tracks the read schema if column mapping ever varies it across
        // versions, and preserves on-disk column order: the order the
        // `cdc_delete_filter` `PhysicalExpr` binds by index in
        // `extract_delete_filter_expr`. Own the kept names before consuming `df`;
        // `select_columns` takes `df` by value, so the `df.schema()` borrow must
        // end first.
        let kept: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .filter(|f| used.contains(f.name()) || referenced.contains(f.name()))
            .map(|f| f.name().to_string())
            .collect();
        let kept: Vec<&str> = kept.iter().map(String::as_str).collect();

        df.select_columns(&kept)
            .map_err(|e| anyhow!("error projecting CDC columns: {e}"))
    }

    /// Load the entire table snapshot as a single "select * where <filter>" query.
    /// Returns the total number of records processed.
    ///
    /// Fails with an error if the function fails to read the snapshot.  This function
    /// doesn't retry (the idea being that the snapshot can be large, and it's better to
    /// fail fast and give the user a chance to restart the pipeline).
    async fn read_unordered_snapshot(
        &self,
        table: &DeltaTable,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) -> AnyResult<usize> {
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

        // Use the time when we started reading the snapshot as the ingestion timestamp for the snapshot.
        let timestamp = Utc::now();

        let record_count = self
            .execute_snapshot_query(
                &snapshot_query,
                "initial snapshot",
                input_stream,
                receiver,
                self.config.max_retries(),
            )
            .await?;
        self.metrics
            .snapshot_records_total
            .fetch_add(record_count as u64, Ordering::Relaxed);
        self.metrics
            .set_last_ingested_version(table.version().unwrap() as i64);

        // Empty buffer to indicate checkpointable state.
        self.queue.push_entry(
            InputQueueEntry::new_with_aux(
                timestamp,
                QueueEntry::ResumeInfo(Some(DeltaResumeInfo::follow_mode(
                    // We verified that the table version is not None in the open_table method.
                    table.version().unwrap() as i64,
                    !self.config.follow(),
                ))),
            )
            // If we started a transaction while processing the snapshot, commit it now.
            .with_commit_transaction(true),
            Vec::new(),
        );

        //let _ = self.datafusion.deregister_table("snapshot");
        info!(
            "delta_table {}: snapshot load completed (records: {}, version: {})",
            &self.endpoint_name,
            record_count,
            table.version().unwrap() as i64
        );
        Ok(record_count)
    }

    /// Load the initial snapshot by issuing a sequence of queries for monotonically
    /// increasing timestamp ranges.
    /// Returns the total number of records processed.
    ///
    /// Fails with an error if the function fails to complete one of the range queries
    /// after retrying `self.config.max_retries` times.
    async fn read_ordered_snapshot(
        &self,
        table: &DeltaTable,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) -> AnyResult<usize> {
        // Use the time when we started reading the snapshot as the ingestion timestamp for the snapshot.
        let timestamp = Utc::now();

        let total_records = self
            .read_ordered_snapshot_inner(table, input_stream, receiver)
            .await?;
        self.metrics
            .set_last_ingested_version(table.version().unwrap() as i64);

        // Empty buffer to indicate checkpointable state.
        self.queue.push_entry(
            InputQueueEntry::new_with_aux(
                timestamp,
                QueueEntry::ResumeInfo(Some(DeltaResumeInfo::follow_mode(
                    // We verified that the table version is not None in the open_table method.
                    table.version().unwrap() as i64,
                    !self.config.follow(),
                ))),
            )
            // If we started a transaction while processing the snapshot, commit it now.
            .with_commit_transaction(true),
            Vec::new(),
        );

        info!(
            "delta_table {}: snapshot load completed (records: {}, version: {})",
            &self.endpoint_name,
            total_records,
            table.version().unwrap() as i64
        );
        Ok(total_records)
    }

    async fn read_ordered_snapshot_inner(
        &self,
        table: &DeltaTable,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
    ) -> Result<usize, AnyError> {
        let timestamp_column = self.config.timestamp_column.as_ref().unwrap();

        let timestamp_field = self.schema.field(timestamp_column).unwrap();

        // The following unwraps are safe, as validated in `validate_timestamp_column`.
        let lateness = timestamp_field.lateness.as_ref().unwrap();

        // Query the table for min and max values of the timestamp column that satisfy the filter.
        let bounds_query = format!(
            "select * from (select cast(min({timestamp_column}) as string) as start_ts, cast(max({timestamp_column}) as string) as end_ts from snapshot {}) where start_ts is not null",
            if let Some(filter) = &self.effective_snapshot_filter() {
                format!("where {filter}")
            } else {
                String::new()
            }
        );

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
            return Ok(0);
        }

        if bounds[0].num_columns() != 2 {
            // Should never happen.
            return Err(anyhow!(
                "internal error: query '{bounds_query}' returned a result with {} columns; expected 2 columns",
                bounds[0].num_columns()
            ));
        }

        // If the connector is restarting from a checkpoint where the initial snapshot has been partially ingested,
        // resume from the checkpointed timestamp value; otherwise use the earliest timestamp in the table.
        let min = match &*self.last_resume_status.lock().unwrap()
        { Some(DeltaResumeInfo {
            snapshot_timestamp: Some(snapshot_timestamp),
            ..
        }) => {
            snapshot_timestamp.clone()
        } _ => {
            array_to_string(bounds[0].column(0))
            .ok_or_else(|| anyhow!("internal error: cannot retrieve the first column in the output of query '{bounds_query}' as a string"))?
        }};

        let max = array_to_string(bounds[0].column(1)).ok_or_else(|| {
            anyhow!(
                "internal error: cannot retrieve the second column in the output of query '{bounds_query}' as a string"
            )
        })?;

        let columns = self.used_columns(table);
        let column_names = columns
            .iter()
            .map(quote_sql_identifier)
            .collect::<Vec<_>>()
            .join(", ");

        info!(
            "delta_table {}: reading table snapshot in the range '{min} <= {timestamp_column} < {max}'",
            &self.endpoint_name,
        );

        let min = timestamp_to_sql_expression(&timestamp_field.columntype, &min);
        let max = timestamp_to_sql_expression(&timestamp_field.columntype, &max);

        let mut start = min.clone();
        let mut total_records = 0usize;

        loop {
            // Evaluate SQL expression for the new end of the interval.
            let end = execute_singleton_query(
                &self.datafusion,
                &format!("select cast(({start} + {lateness}) as string)"),
            )
            .await?;
            let end = timestamp_to_sql_expression(&timestamp_field.columntype, &end);

            // Query the table for the range.
            let mut range_query = format!(
                "select {column_names} from snapshot where {timestamp_column} >= {start} and {timestamp_column} < {end}"
            );
            if let Some(filter) = &self.effective_snapshot_filter() {
                range_query = format!("{range_query} and {filter}");
            }

            let range_record_count = self
                .execute_snapshot_query(
                    &range_query,
                    "range",
                    input_stream,
                    receiver,
                    self.config.max_retries(),
                )
                .await?;
            self.metrics
                .snapshot_records_total
                .fetch_add(range_record_count as u64, Ordering::Relaxed);
            total_records += range_record_count;

            start = end.clone();

            let done = execute_singleton_query(
                &self.datafusion,
                &format!("select cast({start} > {max} as string)"),
            )
            .await?;

            if done == "true" {
                break;
            }

            self.queue.push_entry(
                InputQueueEntry::new_with_aux(
                    Utc::now(),
                    QueueEntry::ResumeInfo(Some(DeltaResumeInfo::snapshot_mode(
                        // We verified that the table version is not None in the open_table method.
                        table.version().unwrap() as i64,
                        &start,
                    ))),
                )
                // If we started a transaction while processing the range query, commit it now.
                .with_commit_transaction(true),
                Vec::new(),
            );
            self.metrics
                .set_last_ingested_version(table.version().unwrap() as i64);
        }

        Ok(total_records)
    }

    /// Runs `operation` until it succeeds or [`DeltaTableReaderConfig::max_retries`] is exhausted.
    ///
    /// On failure before the limit: sets connector health to unhealthy, logs a warning with
    /// `description` as the message prefix, sleeps using [`calculate_backoff_delay`], then retries.
    /// On final failure: updates health, invokes [`InputConsumer::error`], and returns `Err`.
    async fn retry<F, Fut, T, E>(
        &self,
        description: &str,
        error_tag: Option<&'static str>,
        mut operation: F,
    ) -> Result<T, AnyError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: Display,
    {
        let max_retries = self.config.max_retries();
        let mut retry_count = 0u32;
        loop {
            match operation().await {
                Ok(value) => {
                    self.consumer
                        .update_connector_health(ConnectorHealth::healthy());
                    return Ok(value);
                }
                Err(e) => {
                    retry_count += 1;
                    if retry_count - 1 == max_retries {
                        let message = format!("{description} after {retry_count} attempts: {e}");
                        self.consumer
                            .update_connector_health(ConnectorHealth::unhealthy(&message));
                        self.consumer
                            .error(true, anyhow!(message.clone()), error_tag);
                        return Err(anyhow!(message));
                    }
                    let backoff_delay = calculate_backoff_delay(retry_count - 1);
                    let message = format!(
                        "{description} after {retry_count} attempts: {e}; retrying in {:?}",
                        backoff_delay
                    );
                    self.consumer
                        .update_connector_health(ConnectorHealth::unhealthy(&message));
                    warn!("delta_table {}: {message}", &self.endpoint_name);
                    sleep(backoff_delay).await;
                }
            }
        }
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

        // Wait for the pipeline to start before reading.
        wait_running(&mut receiver).await;

        let last_resume_status = self.last_resume_status.lock().unwrap().clone();

        if let Some(DeltaResumeInfo {
            version: Some(version),
            ..
        }) = &last_resume_status
        {
            self.metrics.set_last_ingested_version(*version);
        }

        // We verified that the table version is not None in the open_table method.
        let mut version = table.version().unwrap() as i64;

        // We haven't completed a snapshot before the checkpoint was taken if
        // - there is no checkpoint
        // - the checkpoint was taken in the initial state
        // - the checkpoint was taken mid-snapshot (snapshot_timestamp is set)
        let snapshot_incomplete = matches!(
            last_resume_status,
            None | Some(DeltaResumeInfo { version: None, .. })
                | Some(DeltaResumeInfo {
                    snapshot_timestamp: Some(_),
                    ..
                })
        );

        let snapshot_record_count = if snapshot_incomplete
            && self.config.snapshot()
            && self.config.timestamp_column.is_none()
        {
            // Read snapshot chunk-by-chunk.
            self.read_unordered_snapshot(&table, input_stream.as_mut(), &mut receiver)
                .await
        } else if snapshot_incomplete && self.config.snapshot() {
            // Read the entire snapshot in one query.
            self.read_ordered_snapshot(&table, input_stream.as_mut(), &mut receiver)
                .await
        } else {
            Ok(0)
        };

        let snapshot_record_count = match snapshot_record_count {
            Ok(snapshot_record_count) => snapshot_record_count,
            Err(e) => {
                self.consumer.error(true, e, None);
                return;
            }
        };

        // Start following the table if required by the configuration.
        if self.config.follow() {
            // Log the appropriate message based on whether we just completed a snapshot
            if self.config.snapshot() && snapshot_incomplete {
                // We just completed reading a snapshot, now switching to follow mode
                self.metrics
                    .snapshot_completed_ts
                    .store(now_unix_secs(), Ordering::Relaxed);
                self.metrics
                    .phase
                    .store(DeltaPhase::Follow as u64, Ordering::Relaxed);
                info!(
                    "delta_table {}: snapshot phase completed, switching to follow mode (records: {}, version: {})",
                    &self.endpoint_name, snapshot_record_count, version
                );
            } else if !self.config.snapshot() {
                // Pure CDC follow mode (no snapshot configured)
                self.metrics
                    .phase
                    .store(DeltaPhase::Follow as u64, Ordering::Relaxed);
                info!(
                    "delta_table {}: CDC follow started (from version: {})",
                    &self.endpoint_name, version
                );
            }
            // Note: If self.config.snapshot() && !snapshot_incomplete, we're resuming from a checkpoint
            // where the snapshot was already completed, so no special log needed

            // If we haven't previously read a snapshot of the table, report initial frontier.
            // This makes sure that even if the current version of the table is the final version,
            // we will report the frontier.
            if !self.config.snapshot() {
                self.queue.push_with_aux(
                    (None, Vec::new()),
                    Utc::now(),
                    QueueEntry::ResumeInfo(Some(DeltaResumeInfo::follow_mode(version, false))),
                );
            }

            loop {
                wait_running(&mut receiver).await;
                let new_version = version + 1;

                let table_for_retry = Arc::clone(&table);
                let entry = match self
                    .retry(
                        &format!(
                            "error reading the next log entry (current table version: {version})"
                        ),
                        Some("delta-next-log"),
                        move || {
                            let table = Arc::clone(&table_for_retry);
                            async move {
                                table
                                    .log_store()
                                    .read_commit_entry(new_version as u64)
                                    .await
                            }
                        },
                    )
                    .await
                {
                    Ok(entry) => entry,
                    Err(_) => break,
                };

                match entry {
                    None => sleep(POLL_INTERVAL).await,
                    Some(bytes)
                        if self.config.end_version.is_none()
                            || self.config.end_version.unwrap() >= new_version =>
                    {
                        let actions = match logstore::get_actions(new_version as u64, &bytes) {
                            Ok(actions) => actions,
                            Err(e) => {
                                self.consumer.error(
                                    true,
                                    anyhow!(
                                        "error parsing log entry for table version {new_version}: {e}"
                                    ),
                                    None,
                                );
                                break;
                            }
                        };

                        let (start_transaction, commit_transaction) = match self
                            .config
                            .transaction_mode
                        {
                            DeltaTableTransactionMode::Always => {
                                (self.new_follow_transaction_label(), true)
                            }
                            DeltaTableTransactionMode::Catchup => {
                                if self.catchup_target().is_none() {
                                    let target =
                                        match self.catchup_target_version(Arc::clone(&table)).await
                                        {
                                            Ok(target) => target,
                                            Err(_) => break,
                                        };
                                    debug!(
                                        "delta_table {}: starting catchup transaction (current version: {version}, target version: {target})",
                                        &self.endpoint_name,
                                    );
                                    self.begin_catchup_window(target);
                                }
                                let target = self.catchup_target().unwrap();
                                (
                                    self.catchup_follow_start_transaction(),
                                    new_version >= target,
                                )
                            }
                            _ => (None, false),
                        };

                        version = new_version;

                        if let Err(e) = self
                            .process_log_entry(
                                new_version,
                                &actions,
                                &table,
                                cdc_delete_filter.clone(),
                                input_stream.as_mut(),
                                &mut receiver,
                                start_transaction,
                                commit_transaction,
                            )
                            .await
                        {
                            if self.config.transaction_mode == DeltaTableTransactionMode::Catchup {
                                self.reset_catchup_follow_state();
                            }
                            self.consumer.error(true, e, None);
                            break;
                        };

                        if self.config.transaction_mode == DeltaTableTransactionMode::Catchup
                            && commit_transaction
                        {
                            self.reset_catchup_follow_state();
                        }

                        if let Some(end_version) = self.config.end_version
                            && end_version <= new_version
                        {
                            info!(
                                "delta_table {}: reached table version {} specified as 'end_version' in connector config: stopping the connector",
                                &self.endpoint_name, end_version
                            );
                            self.metrics
                                .phase
                                .store(DeltaPhase::Completed as u64, Ordering::Relaxed);
                            self.consumer.eoi();
                            break;
                        }
                    }
                    Some(_bytes) => {
                        info!(
                            "delta_table {}: reached table version {new_version}, which is greater than the 'end_version' {} specified in connector config: stopping the connector",
                            &self.endpoint_name,
                            self.config.end_version.unwrap()
                        );
                        self.metrics
                            .phase
                            .store(DeltaPhase::Completed as u64, Ordering::Relaxed);
                        self.consumer.eoi();

                        // Empty buffer to indicate eoi.
                        self.queue.push_with_aux(
                            (None, Vec::new()),
                            Utc::now(),
                            QueueEntry::ResumeInfo(Some(DeltaResumeInfo::eoi())),
                        );

                        break;
                    }
                }
            }
        } else {
            // Snapshot-only mode: record completion timestamp and transition to Completed.
            if self.config.snapshot() && snapshot_incomplete {
                self.metrics
                    .snapshot_completed_ts
                    .store(now_unix_secs(), Ordering::Relaxed);
            }
            self.metrics
                .phase
                .store(DeltaPhase::Completed as u64, Ordering::Relaxed);
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

        let delta_table: DeltaTable = {
            let mut retry_count = 0;
            // We don't use config.max_retries here. Do we want unlimited retries opening the table?
            const MAX_RETRIES: u32 = 10;

            // We've seen the table builder get stuck forever in S3 authentication for some configurations
            // (see https://github.com/delta-io/delta-rs/issues/3768). So we add a timeout and retry logic
            // in that case.
            //
            // In other situations, the operation fails returning a timeout error. There is no easy way to
            // distinguish such errors from permanent failures such as incorrect credentials, we therefore
            // resort to checking the returned error message for the word "timeout".
            let mut operation_timeout: Duration = Duration::from_secs(60);

            loop {
                // DeltaTableBuilder doesn't implement Clone, so we need to create a new instance every time.
                let table_builder = DeltaTableBuilder::from_url(url.clone())
                    .map_err(|e| {
                        ControllerError::invalid_transport_configuration(
                            &self.endpoint_name,
                            &format!("invalid Delta table URL '{url}': {e}"),
                        )
                    })?
                    .with_storage_options(self.config.object_store_config.clone())
                    .with_io_runtime(IORuntime::RT(TOKIO_DEDICATED_IO.handle().clone()));

                let table_builder = if let Some(DeltaResumeInfo {
                    version: Some(version),
                    ..
                }) = self.last_resume_status.lock().unwrap().clone()
                {
                    // If we are resuming from a checkpoint, use the version specified in the checkpoint.
                    table_builder.with_version(version as u64)
                } else {
                    match &self.config {
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
                        } => table_builder.with_version(*version as u64),
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
                    }
                };

                match tokio::time::timeout(operation_timeout, table_builder.load()).await {
                    Ok(Ok(table)) => break table,
                    Ok(Err(e)) => {
                        // Timeout errors can originate from multiple transitive dependencies. There is no easy
                        // way to identify them in a strongly-typed fashion. Instead, we check for the "timeout"
                        // substring in a formatter representation of the error.
                        //
                        // Debug-format `e` as the timeout error is often found toward the end of the error chain.
                        let is_timeout = format!("{:?}", e).to_lowercase().contains("timeout");

                        if is_timeout && retry_count < MAX_RETRIES {
                            retry_count += 1;
                            let backoff_ms = min(1000 * (1 << (retry_count - 1)), 10_000);
                            warn!(
                                "delta_table {}: error loading delta table '{}' after {retry_count} attempts (retrying in {backoff_ms} ms): {e:?}",
                                &self.endpoint_name, &self.config.uri,
                            );

                            sleep(Duration::from_millis(backoff_ms)).await;
                        } else {
                            return Err(ControllerError::invalid_transport_configuration(
                                &self.endpoint_name,
                                &format!("error opening delta table '{}': {e:?}", &self.config.uri),
                            ));
                        }
                    }
                    Err(_timeout) => {
                        if retry_count >= MAX_RETRIES {
                            return Err(ControllerError::invalid_transport_configuration(
                                &self.endpoint_name,
                                &format!(
                                    "timeout loading delta table '{}' after {retry_count} attempts",
                                    &self.config.uri,
                                ),
                            ));
                        } else {
                            warn!(
                                "delta_table {}: timeout loading delta table '{}' after {retry_count} attempts, retrying",
                                &self.endpoint_name, &self.config.uri,
                            );
                            retry_count += 1;
                            if operation_timeout < Duration::from_secs(240) {
                                operation_timeout *= 2;
                            }
                        }
                    }
                }
            }
        };

        let version = delta_table.version().ok_or_else(|| {
            ControllerError::invalid_transport_configuration(
                &self.endpoint_name,
                "internal error: table version is not available",
            )
        })? as i64;

        // If we are about to follow the table, set resume state to the current table version, otherwise
        // the connector will remain in the barrier state until at least one transaction is added to the log.
        if !self.config.snapshot() {
            *self.last_resume_status.lock().unwrap() =
                Some(DeltaResumeInfo::follow_mode(version, false));
            *self.last_checkpointable_status.lock().unwrap() =
                DeltaResumeInfo::follow_mode(version, false);
        }

        // Our snapshot scans and CDC listing tables both address files by the
        // table's `root_url()`, so register that store with datafusion.
        // Without it, `select ... from snapshot` fails with "No suitable object
        // store found for <root>". (delta-rs 0.32.x no longer uses the old
        // synthetic `delta-rs://` URL, so we don't register it anymore.)
        let log_store = delta_table.log_store();
        let runtime_env = self.datafusion.runtime_env();
        runtime_env.register_object_store(log_store.root_url(), log_store.root_object_store(None));

        // if let Some(schema) = delta_table.schema() {
        //     info!("Delta table schema: {schema:?}");
        // }

        info!(
            "delta_table {}: opened delta table '{}' (current table version {})",
            &self.endpoint_name, &self.config.uri, version
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

        let provider = table.table_provider().await.map_err(|e| {
            ControllerError::input_transport_error(
                &self.endpoint_name,
                true,
                anyhow!("failed to obtain Delta table provider for snapshot: {e}"),
            )
        })?;
        self.datafusion
            .register_table("snapshot", provider)
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
            validate_sql_order_by(order_by).map_err(|e| {
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
    /// Parses `cdc_delete_filter` directly against the snapshot schema and
    /// lowers the resulting logical expression to a physical expression.
    async fn extract_delete_filter_expr(
        &self,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>, ControllerError> {
        let Some(delete_filter) = &self.config.cdc_delete_filter else {
            return Ok(None);
        };

        let snapshot_df = self
            .datafusion
            .table("snapshot")
            .await
            .map_err(|_e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!(
                        "internal error compiling 'cdc_delete_filter' expression '{delete_filter}'; {REPORT_ERROR}: table 'snapshot' not found"
                    ),
                )
            })?;

        // The compiled `PhysicalExpr` binds columns by index, so it must see the
        // same schema as the batches it will evaluate. When `skip_unused_columns`
        // is set, `do_process_cdc_transaction` projects those batches to the CDC
        // read set via `project_cdc_columns`, so project here through the same
        // helper. Both derive the read set from the same Delta snapshot (this
        // `snapshot` table is registered from it), so the column order matches.
        let snapshot_df = self.project_cdc_columns(snapshot_df).map_err(|e| {
            ControllerError::invalid_transport_configuration(
                &self.endpoint_name,
                &format!(
                    "internal error compiling 'cdc_delete_filter' expression '{delete_filter}'; {REPORT_ERROR}: {e}"
                ),
            )
        })?;

        let schema = snapshot_df.schema().clone();

        let filter_expr = self
            .datafusion
            .parse_sql_expr(delete_filter, &schema)
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!("invalid 'cdc_delete_filter' expression '{delete_filter}': {e}"),
                )
            })?;

        let physical_expr = DefaultPhysicalPlanner::default()
            .create_physical_expr(&filter_expr, &schema, &self.datafusion.state())
            .map_err(|e| {
                ControllerError::invalid_transport_configuration(
                    &self.endpoint_name,
                    &format!(
                        "internal error compiling 'cdc_delete_filter' expression '{delete_filter}'; {REPORT_ERROR}: {e}"
                    ),
                )
            })?;

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
                    "internal error converting the result of the delete filter expression to BooleanArray; {REPORT_ERROR}: expected Boolean, found {:?}", array.data_type()
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
    /// Returns the total number of records processed.
    ///
    /// Fails with an error if the function fails to read the snapshot after retrying
    /// `num_retries` times.
    async fn execute_snapshot_query(
        &self,
        query: &str,
        descr: &str,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
        num_retries: u32,
    ) -> Result<usize, AnyError> {
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
                return Err(anyhow!("error compiling query '{query}': {e}"));
            }
        };

        // One-shot physical-plan dump for debugging ordering / parallelism
        // decisions. Enable with DELTA_SNAPSHOT_EXPLAIN=true. The plan is
        // logged once per snapshot query at INFO level.
        if matches!(
            std::env::var("DELTA_SNAPSHOT_EXPLAIN")
                .ok()
                .as_deref()
                .map(str::to_ascii_lowercase)
                .as_deref(),
            Some("true" | "1")
        ) {
            match df.clone().create_physical_plan().await {
                Ok(plan) => info!(
                    "delta_table {}: physical plan for {descr}:\n{}",
                    &self.endpoint_name,
                    displayable(plan.as_ref()).indent(true)
                ),
                Err(e) => warn!(
                    "delta_table {}: failed to compute physical plan for {descr}: {e}",
                    &self.endpoint_name,
                ),
            }
        }

        self.execute_df(
            df,
            true,
            None,
            &descr,
            input_stream,
            receiver,
            self.allocate_snapshot_transaction_label(),
            num_retries,
            None,
        )
        .await
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
    ///
    /// * `transaction` - execute the dataframe as part of a transaction with the given label (is `Some`).
    ///
    /// * `max_retries` - the maximum number of retries to attempt if the function fails to read the log entry.
    ///
    /// Returns the total number of records processed.
    ///
    /// Returns an error if the function fails to read the log entry after performing the configured
    /// number of retries. Note that errors parsing table records are not reported here; they are
    /// reported by calling `consumer.error`.
    ///
    /// On error, the function commits the current transaction if any. It is possible that some of the
    /// records have been processed and pushed to the circuit before the error.
    ///
    /// If `max_retries` is >0, the function can push duplicate inputs to the circuit as part of the
    /// retry loop.
    #[allow(clippy::too_many_arguments)]
    async fn execute_df(
        &self,
        dataframe: DataFrame,
        polarity: bool,
        cdc_delete_filter: Option<Arc<dyn PhysicalExpr>>,
        descr: &str,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
        transaction: Option<Option<String>>,
        max_retries: u32,
        current_table_version: Option<i64>,
    ) -> Result<usize, AnyError> {
        let mut retry_count = 0;
        loop {
            match self
                .execute_df_inner(
                    dataframe.clone(),
                    polarity,
                    cdc_delete_filter.clone(),
                    input_stream,
                    receiver,
                    &transaction,
                )
                .await
            {
                Ok(total_records) => {
                    self.consumer
                        .update_connector_health(ConnectorHealth::healthy());
                    return Ok(total_records);
                }
                Err(e) => {
                    retry_count += 1;
                    if retry_count - 1 == max_retries {
                        let message = format!(
                            "error retrieving {descr} after {retry_count} attempts{}: {e}",
                            if let Some(version) = current_table_version {
                                format!(" (current table version: {version})")
                            } else {
                                String::new()
                            }
                        );
                        self.consumer
                            .update_connector_health(ConnectorHealth::unhealthy(&message));
                        return Err(anyhow!(message));
                    }
                    let backoff_delay = calculate_backoff_delay(retry_count - 1);

                    let message = format!(
                        "error retrieving {descr} after {retry_count} attempts{}: {e}; retrying in {backoff_delay:?}",
                        if let Some(version) = current_table_version {
                            format!(" (current table version: {version})")
                        } else {
                            String::new()
                        }
                    );
                    self.consumer
                        .update_connector_health(ConnectorHealth::unhealthy(&message));
                    warn!("delta_table {}: {message}", &self.endpoint_name);
                    sleep(backoff_delay).await;
                }
            }
        }
    }

    // A single attempt of the `execute_df` retry loop.
    #[allow(clippy::too_many_arguments)]
    async fn execute_df_inner(
        &self,
        dataframe: DataFrame,
        polarity: bool,
        cdc_delete_filter: Option<Arc<dyn PhysicalExpr>>,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
        transaction: &Option<Option<String>>,
    ) -> Result<usize, String> {
        wait_running(receiver).await;
        let transaction = self.follow_start_transaction(transaction);

        // Limit the number of connectors simultaneously reading from Delta Lake.
        let _token = DELTA_READER_SEMAPHORE.acquire().await.unwrap();

        let mut stream = match dataframe.execute_stream().await {
            Err(e) => {
                return Err(format!("{e:?}"));
            }
            Ok(stream) => stream,
        };

        // We declare the connector healthy at this point.
        self.consumer
            .update_connector_health(ConnectorHealth::healthy());

        let mut num_batches = 0;
        let mut total_records = 0usize;

        let queue = self.queue.clone();

        let num_parsers = self.config.num_parsers as usize;

        // Create a job queue to efficiently parse record batches retrieved by the query.
        let job_queue = JobQueue::<
            (RecordBatch, DateTime<Utc>),
            (Option<StagedInputBuffer>, Vec<ParseError>, DateTime<Utc>),
        >::new(
            num_parsers,
            move || {
                let cdc_delete_filter: Option<Arc<dyn PhysicalExpr>> = cdc_delete_filter.clone();
                let input_stream = input_stream.fork();

                Box::new(move |(batch, timestamp)| {
                    Box::pin({
                        let cdc_delete_filter: Option<Arc<dyn PhysicalExpr>> =
                            cdc_delete_filter.clone();
                        let mut input_stream = input_stream.fork();

                        async move {
                            let (parsed_buffer, errors) = Self::parse_record_batch(
                                batch,
                                polarity,
                                &cdc_delete_filter,
                                input_stream.as_mut(),
                            )
                            .await;
                            let staged_buffer = parsed_buffer.map(|buffer| {
                                let len = buffer.len();
                                StagedInputBuffer::new(input_stream.stage(vec![buffer]), len)
                            });
                            (staged_buffer, errors, timestamp)
                        }
                    })
                })
            },
            move |(buffer, errors, timestamp)| {
                queue.push_entry(
                    InputQueueEntry::new_with_aux(timestamp, QueueEntry::ResumeInfo(None))
                        .with_buffer(buffer)
                        .with_start_transaction(transaction.clone()),
                    errors,
                )
            },
        );

        // Use the timestamp when we start retrieving the next batch as the ingestion timestamp.
        let mut timestamp = Utc::now();

        while let Some(batch) = stream.next().await {
            wait_running(receiver).await;
            let batch = match batch {
                Ok(batch) => batch,
                Err(e) => {
                    drop(job_queue);
                    // We don't have a way to rollback the transaction at this point. The best
                    // we can do is commit the transaction so it doesn't block the pipeline.
                    // This means that the connector will generate duplicate inputs on a retry.
                    //
                    // In catchup mode the committed transaction may cover only part of the
                    // current catchup window. Drop the transaction label so the next retry (or
                    // the next loop iteration for the same Delta version) starts a new Feldera
                    // transaction while the catchup target stays unchanged.
                    if self.config.transaction_mode == DeltaTableTransactionMode::Catchup {
                        self.abandon_catchup_follow_transaction();
                    }
                    self.queue.push_entry(
                        InputQueueEntry::new_with_aux(timestamp, QueueEntry::Rollback)
                            // If we started a transaction while processing the log entry, commit it now.
                            .with_commit_transaction(true),
                        Vec::new(),
                    );

                    return Err(format_datafusion_error(
                        &format!("error retrieving batch {num_batches}"),
                        &e,
                    ));
                }
            };
            // info!("schema: {}", batch.schema());
            num_batches += 1;
            total_records += batch.num_rows();

            // Use the timestamp when the batch was retrieved as the ingestion timestamp.
            job_queue.push_job((batch, timestamp)).await;
            timestamp = Utc::now();
        }

        job_queue.flush().await;
        Ok(total_records)
    }

    async fn parse_record_batch(
        batch: RecordBatch,
        polarity: bool,
        cdc_delete_filter: &Option<Arc<dyn PhysicalExpr>>,
        input_stream: &mut dyn ArrowStream,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let result = if polarity {
            if let Some(delete_filter_expr) = cdc_delete_filter {
                let polarities =
                    match Self::eval_delete_filter(delete_filter_expr.as_ref(), &batch).await {
                        Ok(polarities) => polarities,
                        Err(e) => {
                            return (
                                None,
                                vec![ParseError::bin_envelope_error(e.to_string(), &[], None)],
                            );
                        }
                    };
                // println!(
                //     "insert_with_polarities: {} updates, {} insertions",
                //     polarities.len(),
                //     polarities.iter().filter(|x| **x == true).count()
                // );
                input_stream.insert_with_polarities(&batch, &polarities, &None)
            } else {
                input_stream.insert(&batch, &None)
            }
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

        (input_stream.take_all(), errors)
    }

    /// Apply actions from a transaction log entry.
    ///
    /// Only `Add` and `Remove` actions are picked up.
    ///
    /// Returns an error if the connector failed to read the log entry after performing the `self.config.max_retries`
    /// number of retries. Note that errors parsing table records are not reported here; they are
    /// reported in the `execute_df` method by calling `consumer.error`.
    ///
    /// On error, the function commits the current transaction if any. It is possible that some of the
    /// records have been processed and pushed to the circuit before the error.
    ///
    /// If `self.config.max_retries` is >0, the function can push duplicate inputs to the circuit as part of the
    /// retry loop.
    #[allow(clippy::too_many_arguments)]
    async fn process_log_entry(
        &self,
        new_version: i64,
        actions: &[Action],
        table: &DeltaTable,
        cdc_delete_filter: Option<Arc<dyn PhysicalExpr>>,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
        start_transaction: Option<Option<String>>,
        commit_transaction: bool,
    ) -> AnyResult<()> {
        if self.config.verbose > 0 {
            // Don't log actions we ignore to limit spurious logging. E.g., delta lake
            // optimization passes can generate thousand of noop actions.
            let data_change_actions = actions
                .iter()
                .filter(|action| match action {
                    Action::Add(add) if add.data_change => true,
                    Action::Remove(remove) if remove.data_change => true,
                    _ => false,
                })
                .collect::<Vec<_>>();
            info!(
                "delta_table {}: log entry for table version {new_version}: {data_change_actions:?}{}",
                &self.endpoint_name,
                if actions.len() > data_change_actions.len() {
                    format!(
                        " ({} other actions)",
                        actions.len() - data_change_actions.len()
                    )
                } else {
                    "".to_string()
                }
            );
        }

        // Use the time when we _started_ reading transaction data as the ingestion timestamp.
        let timestamp = Utc::now();

        if self.config.is_cdc() {
            self.process_cdc_transaction(
                actions,
                table,
                cdc_delete_filter,
                input_stream,
                receiver,
                start_transaction,
            )
            .await?;
        } else {
            // Compute the projected read set once for the whole transaction; each
            // `process_action` borrows the `&str` view rather than re-collecting it
            // per Add/Remove. `used_column_names` owns the strings the view points at.
            let used_column_names = self.used_columns(table);
            let used_columns: Vec<&str> = used_column_names.iter().map(String::as_str).collect();

            // TODO: consider processing all Add actions and all Remove actions in one
            // go using `ListingTable`, which understands partitioning and can probably
            // parallelize the load.

            // Process deletes before inserts. Semantically, delete actions in
            // are applied before insert actions; however the delta standard doesn't
            // guarantee that actions occur in any particular order in the transaction log
            // entry.
            for action in actions {
                if matches!(action, Action::Remove(_)) {
                    self.process_action(
                        action,
                        table,
                        &used_columns,
                        input_stream,
                        receiver,
                        start_transaction.clone(),
                    )
                    .await?;
                }
            }

            for action in actions {
                if matches!(action, Action::Add(_)) {
                    self.process_action(
                        action,
                        table,
                        &used_columns,
                        input_stream,
                        receiver,
                        start_transaction.clone(),
                    )
                    .await?;
                }
            }
        }

        // Empty buffer to indicate checkpointable state.
        self.queue.push_entry(
            InputQueueEntry::new_with_aux(
                timestamp,
                QueueEntry::ResumeInfo(Some(DeltaResumeInfo::follow_mode(
                    new_version,
                    self.config.end_version == Some(new_version),
                ))),
            )
            // If we started a transaction while processing the log entry, commit it now.
            .with_commit_transaction(commit_transaction),
            Vec::new(),
        );

        self.metrics.set_last_ingested_version(new_version);

        Ok(())
    }

    /// Process a DeltaLake transaction in CDC mode:
    ///
    /// * Subtract rows in Remove actions from rows in Add actions, so that
    ///   file rewrites that change no logical data (no-op UPDATE, MERGE,
    ///   etc.) don't re-emit unchanged rows as duplicate inserts.
    /// * Order the surviving rows by `cdc_order_by`, compute the polarity of
    ///   each row using `cdc_delete_filter` and insert the row with
    ///   appropriate polarity.
    async fn process_cdc_transaction(
        &self,
        actions: &[Action],
        table: &DeltaTable,
        cdc_delete_filter: Option<Arc<dyn PhysicalExpr>>,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
        start_transaction: Option<Option<String>>,
    ) -> AnyResult<()> {
        let result = self
            .do_process_cdc_transaction(
                actions,
                table,
                cdc_delete_filter,
                input_stream,
                receiver,
                start_transaction,
            )
            .await;

        // Deregister the tables registered by `do_process_cdc_transaction`.
        // If a table does not exist, there's no harm.
        let _ = self.datafusion.deregister_table("cdc_adds");
        let _ = self.datafusion.deregister_table("cdc_removes");

        result
    }

    async fn do_process_cdc_transaction(
        &self,
        actions: &[Action],
        table: &DeltaTable,
        cdc_delete_filter: Option<Arc<dyn PhysicalExpr>>,
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
        start_transaction: Option<Option<String>>,
    ) -> AnyResult<()> {
        // Collect Add and Remove file paths separately. The query below
        // subtracts Removes from Adds via `EXCEPT ALL` to cancel rewrites
        // that don't change logical data.
        //
        // We address files via the table's `root_url()` (e.g. `file:///...` or
        // `s3://bucket/prefix/`) rather than the synthetic `delta-rs://...`
        // URL returned by `object_store_url()`. The synthetic URL encodes the
        // entire table filesystem path into the URL host (slashes become
        // dashes), which works for DataFusion's `register_object_store`
        // routing keyed by scheme+host but produces a malformed listing URL
        // when concatenated with `Add.path`. Using `root_url()` keeps the
        // listing path real, and `register_object_store(root_url, root_store)`
        // (done in `start_input_endpoint`) provides the matching store.
        let log_store = table.log_store();
        let url = log_store.root_url();
        let path_of = |p: &str| format!("{}{}", url.as_str(), p);
        let adds: Vec<String> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Add(x) if x.data_change => Some(path_of(&x.path)),
                _ => None,
            })
            .collect();
        let removes: Vec<String> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Remove(x) if x.data_change => Some(path_of(&x.path)),
                _ => None,
            })
            .collect();

        // No Adds means no new rows to ingest (e.g. OPTIMIZE with
        // `data_change=false` on every action), so there is nothing to do.
        if adds.is_empty() {
            return Ok(());
        }

        let description = format!(
            "CDC transaction with {} adds {:?} and {} removes {:?}",
            adds.len(),
            &adds,
            removes.len(),
            &removes,
        );

        // `self.datafusion` is a per-endpoint `SessionContext` and
        // `process_cdc_transaction` is invoked serially from the single
        // dedicated `worker_task` loop, so the fixed table names
        // `cdc_adds`/`cdc_removes` cannot collide across calls.
        let adds_table = Arc::new(self.create_parquet_table(table, adds, &description).await?);
        self.datafusion.register_table("cdc_adds", adds_table).map_err(|e| {
            anyhow!("internal error processing {description}; {REPORT_ERROR}; error registering 'cdc_adds' table: {e}")
        })?;

        let adds_df = self.datafusion.table("cdc_adds").await.map_err(|e| {
            anyhow!("internal error processing {description}; {REPORT_ERROR}; error reading 'cdc_adds' table: {e}")
        })?;

        // Drop unused columns when `skip_unused_columns` is set, so DataFusion
        // never reads them off disk. Both sides get the same column list so the
        // `EXCEPT ALL` in `build_cdc_dataframe` still lines up, and metadata
        // columns used by `cdc_order_by`/`cdc_delete_filter`/`filter` are kept.
        let adds_df = self
            .project_cdc_columns(adds_df)
            .map_err(|e| anyhow!("internal error processing {description}; {REPORT_ERROR}; {e}"))?;

        let removes_df = if removes.is_empty() {
            None
        } else {
            let removes_table = Arc::new(
                self.create_parquet_table(table, removes, &description)
                    .await?,
            );
            self.datafusion.register_table("cdc_removes", removes_table).map_err(|e| {
                anyhow!("internal error processing {description}; {REPORT_ERROR}; error registering 'cdc_removes' table: {e}")
            })?;
            let removes_df = self.datafusion.table("cdc_removes").await.map_err(|e| {
                anyhow!("internal error processing {description}; {REPORT_ERROR}; error reading 'cdc_removes' table: {e}")
            })?;
            let removes_df = self.project_cdc_columns(removes_df).map_err(|e| {
                anyhow!("internal error processing {description}; {REPORT_ERROR}; {e}")
            })?;
            Some(removes_df)
        };

        // The `cdc_order_by` expression is mandatory in CDC mode (enforced
        // by `validate_cdc_config`), so the unwrap is safe.
        let order_by = self.config.cdc_order_by.as_ref().unwrap();
        let df = build_cdc_dataframe(
            adds_df,
            removes_df,
            self.config.filter.as_deref(),
            order_by,
            &description,
        )?;

        let _record_count = self
            .execute_df(
                df,
                true,
                cdc_delete_filter,
                &description,
                input_stream,
                receiver,
                start_transaction,
                self.config.max_retries(),
                table.version().map(|v| v as i64),
            )
            .await?;

        Ok(())
    }

    /// Create a table provider from a list of Parquet files.
    async fn create_parquet_table(
        &self,
        table: &DeltaTable,
        files: Vec<String>,
        description: &str,
    ) -> AnyResult<ListingTable> {
        // `DeltaTable::snapshot()` returns the table state; calling `.snapshot()`
        // on that state hands back the eager snapshot, which exposes the Arrow
        // schema.
        let schema = table
            .snapshot()
            .map_err(|e| anyhow!("error accessing Delta table snapshot: {e}"))?
            .snapshot()
            .arrow_schema();

        let mut urls = Vec::with_capacity(files.len());
        for file in files.iter() {
            urls.push(ListingTableUrl::parse(file).map_err(|e| anyhow!("internal error processing {description}; {REPORT_ERROR}; error converting file path '{file}' to table URL: {e}"))?);
        }

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension_opt(Some(".parquet"));

        let table_config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(listing_options)
            .with_schema(schema);

        ListingTable::try_new(table_config).map_err(|e| {
            anyhow!("internal error processing {description}; {REPORT_ERROR}; error creating Parquet table: {e}")
        })
    }

    async fn process_action(
        &self,
        action: &Action,
        table: &DeltaTable,
        used_columns: &[&str],
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
        start_transaction: Option<Option<String>>,
    ) -> AnyResult<()> {
        let result = match action {
            Action::Add(add) if add.data_change => {
                self.add_with_polarity(
                    &add.path,
                    true,
                    table,
                    used_columns,
                    input_stream,
                    receiver,
                    start_transaction,
                )
                .await
            }
            Action::Remove(remove)
                if remove.data_change && self.config.cdc_delete_filter.is_none() =>
            {
                self.add_with_polarity(
                    &remove.path,
                    false,
                    table,
                    used_columns,
                    input_stream,
                    receiver,
                    start_transaction,
                )
                .await
            }
            _ => return Ok(()),
        };

        // Deregister the table registered by `add_with_polarity`.
        // If the table does not exist, there's no harm.
        let _ = self.datafusion.deregister_table("tmp_table");

        result
    }

    // NOTE: Column projection (follow mode here, CDC mode in `do_process_cdc_transaction`) projects
    // against the startup snapshot schema, which `create_parquet_table` forces onto every Parquet
    // file we read. This assumes a stable schema across the log versions we follow. DataFusion's
    // schema adapter handles additive evolution (new columns ignored, missing columns read as NULL);
    // a renamed/dropped column also reads as NULL, and an uncastable type change errors at read time.
    // Real per-version evolution would require reloading the table and re-deriving the column set.
    #[allow(clippy::too_many_arguments)]
    async fn add_with_polarity(
        &self,
        path: &str,
        polarity: bool,
        table: &DeltaTable,
        used_columns: &[&str],
        input_stream: &mut dyn ArrowStream,
        receiver: &mut Receiver<PipelineState>,
        start_transaction: Option<Option<String>>,
    ) -> AnyResult<()> {
        let description = format!("file '{path}'");

        // Address files via the table's real `root_url()` (e.g. `file:///...`
        // or `s3://bucket/prefix/`). See `do_process_cdc_transaction` for the
        // full reasoning on why we don't use `object_store_url()` here.
        let full_path = format!("{}{}", table.log_store().root_url().as_str(), path);

        // Create a datafusion table backed by these files.
        let parquet_table = Arc::new(
            self.create_parquet_table(table, vec![full_path.clone()], &description)
                .await?,
        );

        self.datafusion.register_table("tmp_table", parquet_table).map_err(|e| {
            anyhow!("internal error processing file {full_path}; {REPORT_ERROR}; error registering Parquet table: {e}")
        })?;

        let df = self
            .datafusion
            .table("tmp_table")
            .await
            .map_err(|e| {
                anyhow!("internal error processing file {full_path}; {REPORT_ERROR}; error reading 'tmp_table': {e}")
            })?
            .select_columns(used_columns)
            .map_err(|e| {
                anyhow!("internal error processing file {full_path}; {REPORT_ERROR}; error selecting columns: {e}")
            })?;

        let df = if let Some(filter) = &self.config.filter {
            let expr = df
                .parse_sql_expr(filter)
                .map_err(|e| anyhow!("invalid 'filter' expression '{filter}': {e}"))?;
            df.filter(expr).map_err(|e| {
                anyhow!("internal error processing file {full_path}; {REPORT_ERROR}; error applying 'filter': {e}")
            })?
        } else {
            df
        };

        let _record_count = self
            .execute_df(
                df,
                polarity,
                None,
                &description,
                input_stream,
                receiver,
                start_transaction,
                self.config.max_retries(),
                table.version().map(|v| v as i64),
            )
            .await?;

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

#[cfg(test)]
mod format_datafusion_error_tests {
    use super::format_datafusion_error;
    use datafusion::common::DataFusionError;

    #[test]
    fn appends_pool_hint_for_resources_exhausted() {
        let inner = DataFusionError::ResourcesExhausted(
            "Failed to allocate additional 64.0 MB ...".to_string(),
        );
        let wrapped = DataFusionError::Context("external sort".to_string(), Box::new(inner));
        let msg = format_datafusion_error("error retrieving batch 0", &wrapped);
        assert!(
            msg.contains("DataFusion memory pool is exhausted"),
            "missing actionable hint; got: {msg}"
        );
        assert!(
            msg.contains("datafusion_memory_mb"),
            "missing knob name; got: {msg}"
        );
    }

    #[test]
    fn passes_through_unrelated_errors() {
        let other = DataFusionError::Plan("bad column reference".to_string());
        let msg = format_datafusion_error("error retrieving batch 0", &other);
        assert!(
            !msg.contains("memory pool"),
            "spurious pool hint on non-exhaustion error; got: {msg}"
        );
        assert!(
            msg.contains("bad column reference"),
            "lost the original error text; got: {msg}"
        );
    }
}

#[cfg(test)]
mod is_skippable_tests {
    use super::DeltaTableInputEndpointInner;
    use feldera_types::program_schema::{ColumnType, Field};

    fn field(unused: bool, nullable: bool, default: Option<&str>) -> Field {
        let mut field = Field::new("c".into(), ColumnType::varchar(nullable)).with_unused(unused);
        field.default = default.map(str::to_string);
        field
    }

    #[test]
    fn skippable_only_when_unused_and_safely_omittable() {
        // A used column is never omittable, whatever its type.
        assert!(!DeltaTableInputEndpointInner::is_unused_and_omittable(
            &field(false, true, None)
        ));
        assert!(!DeltaTableInputEndpointInner::is_unused_and_omittable(
            &field(false, false, Some("0"))
        ));

        // An unused column is omittable only if substituting for it is
        // well-defined: it is nullable, or it carries a default.
        assert!(DeltaTableInputEndpointInner::is_unused_and_omittable(
            &field(true, true, None)
        ));
        assert!(DeltaTableInputEndpointInner::is_unused_and_omittable(
            &field(true, false, Some("0"))
        ));
        assert!(!DeltaTableInputEndpointInner::is_unused_and_omittable(
            &field(true, false, None)
        ));
    }
}
