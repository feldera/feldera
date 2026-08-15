use crate::errors::journal::ControllerError;
use anyhow::{Error as AnyError, anyhow};
use arrow::array::Array;
use datafusion::common::ScalarValue;
use datafusion::common::arrow::array::{AsArray, RecordBatch};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::{FairSpillPool, MemoryLimit};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::logical_expr::sqlparser::parser::ParserError;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use datafusion::sql::sqlparser::ast::{Expr, visit_expressions};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use feldera_types::config::PipelineConfig;
use feldera_types::constants::DATAFUSION_TEMP_DIR;
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlType};
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::{create_dir_all, read_dir, remove_dir_all, remove_file};
use std::io::Error as IoError;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, warn};

/// Below this much buffered data, a sort partition concatenates its batches
/// and sorts them in place instead of sorting each batch and merging. 64 MiB.
///
/// Powers of two align with page sizes (4 KiB / 2 MiB) the allocator
/// hands back, so a `1 << 26` budget matches what the OS actually
/// reserves rather than a round decimal value the OS rounds up anyway.
const SORT_IN_PLACE_THRESHOLD_BYTES: usize = 1 << 26;

/// Ceiling on the merge headroom reserved for spill-to-disk sorting per sort partition. 64 MiB.
///
/// Normally, we reserve a fraction (SORT_RESERVATION_POOL_DIVISOR) of the
/// datafusion memory pool divided by the number of partitions. Very large values
/// can waste memory, because datafusion will grab the entire reservation even
/// when sorting a few entries. Very large reservations are also unnecessary.
/// This constant bounds the reservation.
///
/// Note: DataFusion 52.x emits noisy `WARN datafusion_physical_plan::spill:
/// Record batch memory usage ... exceeds the expected limit ... by more
/// than the allowed tolerance` lines during spilled sorts. The overage is
/// typically a handful of bytes over a 4 KB tolerance -- upstream
/// accounting drift, tracked at
/// <https://github.com/apache/datafusion/issues/17340> Not a query failure.
const MAX_SORT_SPILL_RESERVATION_BYTES: usize = 1 << 26;

/// A fraction of the datafusion memory pool used for spill sorting.
const SORT_RESERVATION_POOL_DIVISOR: usize = 4;

/// Merge headroom to reserve per sort partition for spill-to-disk merge sorting, in bytes.
///
/// DataFusion pre-books this much for *every* partition of *every* sort, on
/// the partition's first batch and before it compares a single row
/// (`ExternalSorter::reserve_memory_for_merge`).
///
/// We derive it as a fraction of the pool size `pool / SORT_RESERVATION_POOL_DIVISOR`
/// leaving the rest for data.
fn sort_spill_reservation_bytes(pool_bytes: Option<usize>, partitions: usize) -> usize {
    let Some(pool_bytes) = pool_bytes else {
        return MAX_SORT_SPILL_RESERVATION_BYTES;
    };
    (pool_bytes / SORT_RESERVATION_POOL_DIVISOR / partitions.max(1))
        .min(MAX_SORT_SPILL_RESERVATION_BYTES)
}

/// Per-partition merge headroom below which sorting partitions in parallel
/// is not worth its risk. DataFusion's own default
/// `sort_spill_reservation_bytes`, and just above the point where parallel
/// sorts were measured to start failing.
const MIN_PARALLEL_SORT_RESERVATION_BYTES: usize = 10 * 1024 * 1024;

/// Whether the pool can fund a merge sort.
///
/// True if the reservation prescribed by `sort_spill_reservation_bytes` is
/// `>= MIN_PARALLEL_SORT_RESERVATION_BYTES`.
fn parallel_sort_fits_pool(pool_bytes: Option<usize>, partitions: usize) -> bool {
    // One partition has nothing to coalesce, and an unbounded pool has
    // nothing to run out of.
    if partitions <= 1 || pool_bytes.is_none() {
        return true;
    }
    sort_spill_reservation_bytes(pool_bytes, partitions) >= MIN_PARALLEL_SORT_RESERVATION_BYTES
}

/// Size of the pool the sorts will actually charge, or `None` when no memory
/// budget is configured and the pool is unbounded.
///
/// Read from the [`RuntimeEnv`] rather than re-derived from the pipeline
/// config so the number stays right for a caller that built its own env.
fn pool_bytes(runtime_env: &RuntimeEnv) -> Option<usize> {
    match runtime_env.memory_pool.memory_limit() {
        MemoryLimit::Finite(bytes) => Some(bytes),
        MemoryLimit::Infinite | MemoryLimit::Unknown => None,
    }
}

/// Upper bound on the sort partitions a query in this session can run
/// concurrently, and so on the number of merge reservations it books.
///
/// `target_partitions` bounds every scan DataFusion plans itself, which is
/// all a connector session runs. Only the ad-hoc engine escapes it, and it
/// passes its own `partition_floor`; see [`adhoc_partition_floor`].
fn sort_partitions(target_partitions: usize, partition_floor: usize) -> usize {
    target_partitions.max(partition_floor).max(1)
}

/// Sort partitions the ad-hoc engine can open regardless of
/// `target_partitions`.
///
/// `AdHocQueryExecution` reports one partition per reader in the snapshot,
/// and the snapshot holds one batch per *local* DBSP worker
/// (`OutputHandle::take_from_all` walks the local mailboxes). `workers`
/// counts the workers on every host, while the memory pool is per host, so
/// the local share is what a sort here actually runs.
fn adhoc_partition_floor(pipeline_config: &PipelineConfig) -> usize {
    let workers = pipeline_config.global.workers as usize;
    workers.div_ceil(pipeline_config.global.hosts.max(1))
}

/// Build the shared datafusion [`RuntimeEnv`] for a pipeline.
///
/// Build once per pipeline and share the `Arc` across every
/// `SessionContext`. A separate `RuntimeEnv` per context would give each its
/// own pool, multiplying the effective memory budget by `(1 + #connectors)`.
pub fn create_runtime_env(
    pipeline_config: &PipelineConfig,
) -> Result<Arc<RuntimeEnv>, ControllerError> {
    let mut builder = RuntimeEnvBuilder::new();
    if let Some(datafusion_memory_mb) = pipeline_config.global.resolved_datafusion_memory_mb() {
        let memory_bytes_max = datafusion_memory_mb * 1_000_000;
        builder = builder.with_memory_pool(Arc::new(FairSpillPool::new(memory_bytes_max as usize)));
    }
    if let Some(storage) = &pipeline_config.storage_config {
        let path = PathBuf::from(storage.path.clone()).join(DATAFUSION_TEMP_DIR);
        create_dir_all(&path).map_err(|error| {
            ControllerError::io_error(
                format!(
                    "unable to create datafusion scratch space directory '{}'",
                    path.display()
                ),
                error,
            )
        })?;
        clean_stale_scratch_entries(&path);
        builder = builder.with_temp_file_path(path);
    }
    builder.build_arc().map_err(|error| {
        ControllerError::io_error(
            "unable to build datafusion runtime environment",
            IoError::other(error.to_string()),
        )
    })
}

/// Remove leftovers from a previous process inside the scratch directory.
///
/// DataFusion's `DiskManager` leaks its `datafusion-XXXXXX/` subdir if the
/// process is killed before `tempfile::TempDir::drop` runs. The previous
/// process is gone by the time we get here, so anything still in the dir is
/// orphaned. Spill files are per-query and never need to survive a restart.
/// Errors only logged: a stuck file should not block startup.
fn clean_stale_scratch_entries(scratch_dir: &Path) {
    // Tripwire: refuse to recursively delete anything whose final
    // component isn't the well-known scratch dir name. Defends against a
    // future caller accidentally passing `/`, `~`, or the storage root.
    if scratch_dir.file_name() != Some(OsStr::new(DATAFUSION_TEMP_DIR)) {
        warn!(
            "refusing to clean unexpected scratch directory '{}'; expected final component '{DATAFUSION_TEMP_DIR}'",
            scratch_dir.display(),
        );
        return;
    }
    let entries = match read_dir(scratch_dir) {
        Ok(entries) => entries,
        Err(error) => {
            warn!(
                "unable to read datafusion scratch directory '{}' for startup cleanup: {error}",
                scratch_dir.display(),
            );
            return;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(ft) => ft,
            Err(error) => {
                warn!(
                    "unable to stat stale datafusion scratch entry '{}': {error}",
                    path.display(),
                );
                continue;
            }
        };
        let result = if file_type.is_dir() {
            remove_dir_all(&path)
        } else {
            remove_file(&path)
        };
        if let Err(error) = result {
            warn!(
                "unable to remove stale datafusion scratch entry '{}': {error}",
                path.display(),
            );
        }
    }
}

/// `SessionContext` for the ad-hoc query engine, bound to the shared
/// [`RuntimeEnv`] and configured with feldera's sort-spill thresholds.
///
/// Budgets sorts for one partition per local DBSP worker, since that is what
/// the ad-hoc scan opens whatever `target_partitions` says.
pub fn create_session_context(
    pipeline_config: &PipelineConfig,
    runtime_env: Arc<RuntimeEnv>,
) -> SessionContext {
    let floor = adhoc_partition_floor(pipeline_config);
    create_session_context_inner(pipeline_config, runtime_env, floor, |cfg| cfg)
}

/// `SessionContext` for a connector, with a hook to override individual
/// datafusion settings (e.g. parquet decoding) before the context is built.
///
/// Sorts are budgeted for `target_partitions`, which bounds every scan
/// DataFusion plans. Unlike [`create_session_context`] there is no worker
/// floor: a connector session queries external tables, never the ad-hoc
/// snapshot, so the DBSP worker count does not bound its parallelism.
///
/// The hook may raise or lower `target_partitions`. `sort_spill_reservation_bytes`
/// and `repartition_sorts` are derived from whatever partition count the hook
/// settles on and applied afterwards, so a hook that sets either of those two
/// options does not take effect.
pub fn create_session_context_with<F>(
    pipeline_config: &PipelineConfig,
    runtime_env: Arc<RuntimeEnv>,
    customize_config: F,
) -> SessionContext
where
    F: FnOnce(SessionConfig) -> SessionConfig,
{
    create_session_context_inner(pipeline_config, runtime_env, 0, customize_config)
}

fn create_session_context_inner<F>(
    pipeline_config: &PipelineConfig,
    runtime_env: Arc<RuntimeEnv>,
    partition_floor: usize,
    customize_config: F,
) -> SessionContext
where
    F: FnOnce(SessionConfig) -> SessionConfig,
{
    let workers = pipeline_config
        .global
        .io_workers
        .unwrap_or(pipeline_config.global.workers as u64);
    let session_config = SessionConfig::new()
        .with_target_partitions(workers as usize)
        .with_sort_in_place_threshold_bytes(SORT_IN_PLACE_THRESHOLD_BYTES)
        .set(
            "datafusion.execution.planning_concurrency",
            &ScalarValue::UInt64(Some(workers)),
        );
    let session_config = customize_config(session_config);

    let partitions = sort_partitions(session_config.target_partitions(), partition_floor);
    let pool_bytes = pool_bytes(&runtime_env);
    let repartition_sorts = parallel_sort_fits_pool(pool_bytes, partitions);
    if !repartition_sorts {
        info!(
            "DataFusion memory pool is {} MB across {partitions} sort partitions, too little to \
             sort them in parallel; sorts will coalesce their input first. This is slower but \
             lets a sort use the whole pool. Increase 'datafusion_memory_mb' or reduce 'workers' \
             to sort in parallel again.",
            pool_bytes.unwrap_or(0) / 1_000_000,
        );
    }
    // The reservation stays sized for `partitions` even when the `repartition_sorts`
    // flag is off. This flag does not govern sorts that arrive already
    // partitioned (window functions, sort-merge joins), so a plan
    // can still open one sorter per partition.
    let session_config = session_config
        .with_sort_spill_reservation_bytes(sort_spill_reservation_bytes(pool_bytes, partitions))
        .with_repartition_sorts(repartition_sorts);

    let mut state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .build();
    // JSON functions for querying VARIANT columns, which reach DataFusion
    // as JSON-encoded strings. Note: the crate's `->` and `?` operators do
    // not parse in the default (generic) SQL dialect; `->>` works.
    datafusion_functions_json::register_all(&mut state)
        .expect("registering JSON functions on a fresh session state cannot fail");
    SessionContext::from(state)
}

/// Execute a SQL query and collect all results in a vector of `RecordBatch`'s.
pub async fn execute_query_collect(
    datafusion: &SessionContext,
    query: &str,
) -> Result<Vec<RecordBatch>, AnyError> {
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false);

    let df = datafusion
        .sql_with_options(query, options)
        .await
        .map_err(|e| anyhow!("error compiling query '{query}': {e}"))?;

    df.collect()
        .await
        .map_err(|e| anyhow!("error executing query '{query}': {e}"))
}

/// Execute a SQL query that returns a result with exactly one row and column of type `string`.
pub async fn execute_singleton_query(
    datafusion: &SessionContext,
    query: &str,
) -> Result<String, AnyError> {
    let result = execute_query_collect(datafusion, query).await?;
    if result.len() != 1 {
        return Err(anyhow!(
            "internal error: query '{query}' returned {} batches; expected: 1",
            result.len()
        ));
    }

    if result[0].num_rows() != 1 {
        return Err(anyhow!(
            "internal error: query '{query}' returned {} rows; expected: 1",
            result[0].num_rows()
        ));
    }

    if result[0].num_columns() != 1 {
        return Err(anyhow!(
            "internal error: query '{query}' returned {} columns; expected: 1",
            result[0].num_columns()
        ));
    }

    let column0 = result[0].column(0);

    array_to_string(column0).ok_or_else(|| {
        anyhow!("internal error: cannot retrieve the output of query '{query}' as a string")
    })
}

pub fn array_to_string(array: &dyn Array) -> Option<String> {
    if let Some(string_view_array) = array.as_string_view_opt() {
        Some(string_view_array.value(0).to_string())
    } else {
        array
            .as_string_opt::<i32>()
            .map(|array| array.value(0).to_string())
    }
}

/// Parse expression only to validate it.
pub fn validate_sql_expression(expr: &str) -> Result<(), ParserError> {
    let mut parser = Parser::new(&GenericDialect).try_with_sql(expr)?;
    parser.parse_expr()?;

    Ok(())
}

/// Validate the body of an ORDER BY clause (e.g. "ts asc, lsn desc").
///
/// Unlike [`validate_sql_expression`], this accepts the comma-separated,
/// ASC/DESC/NULLS annotated key list a real ORDER BY allows, and
/// requires the whole string to parse so a malformed clause fails here rather
/// than silently dropping every key after the first.
pub fn validate_sql_order_by(order_by: &str) -> Result<(), ParserError> {
    let mut parser = Parser::new(&GenericDialect).try_with_sql(order_by)?;
    parser.parse_comma_separated(Parser::parse_order_by_expr)?;
    parser.expect_token(&Token::EOF)?;

    Ok(())
}

/// Collect into `columns` every column name an expression's AST references,
/// walking nested sub-expressions. Every part of a compound reference `a.b.c` is
/// kept: the AST cannot tell a table qualifier from a column from a struct
/// field, so any part may be the column.
///
/// Over-collecting is harmless for the callers here -- it merely keeps a column
/// from being pruned -- but under-collecting would drop a column the connector
/// needs, so an identifier is kept whenever it could name a column.
fn collect_referenced_columns(expr: &Expr, columns: &mut BTreeSet<String>) {
    let _: ControlFlow<()> = visit_expressions(expr, |e| {
        match e {
            Expr::Identifier(ident) => {
                columns.insert(ident.value.clone());
            }
            Expr::CompoundIdentifier(parts) => {
                columns.extend(parts.iter().map(|part| part.value.clone()));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    });
}

/// Column names referenced by a scalar SQL expression, e.g. a connector's
/// `filter` or `cdc_delete_filter`. Names are returned verbatim; the caller
/// case-folds when matching them against a schema.
///
/// Returns an error if the expression does not parse. Used when pruning
/// "unused" columns, to keep the columns a connector expression depends on.
pub fn columns_referenced_by_expression(expr: &str) -> Result<BTreeSet<String>, ParserError> {
    let mut parser = Parser::new(&GenericDialect).try_with_sql(expr)?;
    let parsed = parser.parse_expr()?;
    let mut columns = BTreeSet::new();
    collect_referenced_columns(&parsed, &mut columns);
    Ok(columns)
}

/// Like [`columns_referenced_by_expression`], but for the comma-separated key
/// list of an ORDER BY clause, e.g. a connector's `cdc_order_by`.
pub fn columns_referenced_by_order_by(order_by: &str) -> Result<BTreeSet<String>, ParserError> {
    let mut parser = Parser::new(&GenericDialect).try_with_sql(order_by)?;
    let keys = parser.parse_comma_separated(Parser::parse_order_by_expr)?;
    parser.expect_token(&Token::EOF)?;
    let mut columns = BTreeSet::new();
    for key in &keys {
        collect_referenced_columns(&key.expr, &mut columns);
    }
    Ok(columns)
}

/// Takes a column name from an external table schema and returns a quoted
/// string that can be used in datafusion queries like `select "foo""bar" from my_table`.
pub fn quote_sql_identifier<S: AsRef<str>>(ident: S) -> String {
    format!("\"{}\"", ident.as_ref().replace("\"", "\"\""))
}

/// A set of column names compared case-insensitively. External table schemas
/// (Delta, Iceberg) carry no case-sensitivity information, so names are stored
/// and probed in lowercased form.
///
/// SQL is case-sensitive for quoted column names, but an external table cannot
/// hold two columns with the same lowercase form, so collapsing to a single
/// canonical form is safe here.
#[derive(Default)]
pub struct ColumnNameSet {
    lowercase: BTreeSet<String>,
}

impl ColumnNameSet {
    pub fn from_names(names: impl IntoIterator<Item = String>) -> Self {
        let lowercase = names.into_iter().map(|c| c.to_lowercase()).collect();
        Self { lowercase }
    }

    pub fn contains(&self, name: &str) -> bool {
        self.lowercase.contains(&name.to_lowercase())
    }
}

/// Convert a value of the timestamp column returned by a SQL query into a valid
/// SQL expression.
pub fn timestamp_to_sql_expression(column_type: &ColumnType, expr: &str) -> String {
    match column_type.typ {
        SqlType::Timestamp => format!("timestamp '{expr}'"),
        SqlType::Date => format!("date '{expr}'"),
        _ => expr.to_string(),
    }
}

/// Check that the `timestamp` field has one of supported types.
pub fn validate_timestamp_type(
    endpoint_name: &str,
    timestamp: &Field,
    docs: &str,
) -> Result<(), ControllerError> {
    if !timestamp.columntype.is_integral_type()
        && !matches!(
            &timestamp.columntype.typ,
            SqlType::Date | SqlType::Timestamp
        )
    {
        return Err(ControllerError::invalid_transport_configuration(
            endpoint_name,
            &format!(
                "timestamp column '{}' has unsupported type {}; supported types for 'timestamp_column' are integer types, DATE, and TIMESTAMP; {docs}",
                timestamp.name,
                serde_json::to_string(&timestamp.columntype).unwrap()
            ),
        ));
    }

    Ok(())
}

/// Validate 'timestamp_column'.
pub async fn validate_timestamp_column(
    endpoint_name: &str,
    timestamp_column: &str,
    datafusion: &SessionContext,
    schema: &Relation,
    docs: &str,
) -> Result<(), ControllerError> {
    // Lookup column in the schema.
    let Some(field) = schema.field(timestamp_column) else {
        return Err(ControllerError::invalid_transport_configuration(
            endpoint_name,
            &format!("timestamp column '{timestamp_column}' not found in table schema"),
        ));
    };

    // Field must have a supported type.
    validate_timestamp_type(endpoint_name, field, docs)?;

    // Column must have lateness.
    let Some(lateness) = &field.lateness else {
        return Err(ControllerError::invalid_transport_configuration(
            endpoint_name,
            &format!(
                "timestamp column '{timestamp_column}' does not have a LATENESS attribute; {docs}"
            ),
        ));
    };

    // Validate lateness expression.
    validate_sql_expression(lateness).map_err(|e|
                ControllerError::invalid_transport_configuration(
                    endpoint_name,
                    &format!("error parsing LATENESS attribute '{lateness}' of the timestamp column '{timestamp_column}': {e}; {docs}"),
                ),
            )?;

    // Lateness has to be >0. Zero would mean that we need to ingest data strictly in order. If we need to support this case in the future,
    // we could revert to our old (and very costly) strategy of issuing a single `select *` query with the 'ORDER BY timestamp_column' clause,
    // which requires storing and sorting the entire collection locally.
    let is_zero = execute_singleton_query(
        datafusion,
        &format!("select cast((({lateness} + {lateness}) = {lateness}) as string)"),
    )
    .await
    .map_err(|e| ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string()))?;

    if &is_zero == "true" {
        return Err(ControllerError::invalid_transport_configuration(
            endpoint_name,
            &format!(
                "invalid LATENESS attribute '{lateness}' of the timestamp column '{timestamp_column}': LATENESS must be greater than zero; {docs}"
            ),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        columns_referenced_by_expression, columns_referenced_by_order_by, create_runtime_env,
        create_session_context,
    };
    use datafusion::execution::memory_pool::MemoryLimit;
    use feldera_types::config::{PipelineConfig, ResourceConfig, RuntimeConfig, StorageConfig};
    use feldera_types::constants::DATAFUSION_TEMP_DIR;
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::{Path, PathBuf};

    /// Drop guard so a failing test does not leak temp directories.
    struct TempStorage {
        path: PathBuf,
    }

    impl TempStorage {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(name);
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempStorage {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn pipeline_config(global: RuntimeConfig, storage: Option<&Path>) -> PipelineConfig {
        PipelineConfig {
            global,
            multihost: None,
            name: None,
            given_name: None,
            storage_config: storage.map(|p| StorageConfig {
                path: p.to_string_lossy().into(),
                cache: Default::default(),
            }),
            secrets_dir: None,
            inputs: Default::default(),
            outputs: Default::default(),
            program_ir: None,
        }
    }

    #[test]
    fn create_runtime_env_creates_tmp_dir_under_storage() {
        let storage = TempStorage::new("feldera-datafusion-create-runtime-env-tmp-dir-test");
        let cfg = pipeline_config(
            RuntimeConfig {
                workers: 1,
                ..Default::default()
            },
            Some(storage.path()),
        );

        create_runtime_env(&cfg).unwrap();

        let expected = storage.path().join(DATAFUSION_TEMP_DIR);
        assert!(
            expected.is_dir(),
            "expected scratch directory at {}",
            expected.display(),
        );
    }

    /// Must match the value `checkpointer::gc_startup` allowlists, or the
    /// scratch dir is wiped on every restart.
    #[test]
    fn scratch_dir_name_matches_gc_allowlist_constant() {
        assert_eq!(DATAFUSION_TEMP_DIR, "datafusion-tmp");
    }

    #[test]
    fn create_runtime_env_without_storage_succeeds() {
        let cfg = pipeline_config(
            RuntimeConfig {
                workers: 1,
                ..Default::default()
            },
            None,
        );
        create_runtime_env(&cfg).unwrap();
    }

    #[test]
    fn create_runtime_env_applies_memory_pool_when_budget_set() {
        // 5% of 16 GB = 800 MB; below the 2 GB ceiling.
        let storage = TempStorage::new("feldera-datafusion-create-runtime-env-pool-test");
        let cfg = pipeline_config(
            RuntimeConfig {
                workers: 1,
                max_rss_mb: Some(16_000),
                ..Default::default()
            },
            Some(storage.path()),
        );

        let env = create_runtime_env(&cfg).unwrap();
        match env.memory_pool.memory_limit() {
            MemoryLimit::Finite(bytes) => assert_eq!(bytes, 800 * 1_000_000),
            MemoryLimit::Infinite => panic!("expected a bounded memory pool, got Infinite"),
            MemoryLimit::Unknown => panic!("expected a bounded memory pool, got Unknown"),
        }
    }

    #[test]
    fn create_runtime_env_no_memory_limit_when_budget_unset() {
        let storage = TempStorage::new("feldera-datafusion-create-runtime-env-unbounded-test");
        let cfg = pipeline_config(
            RuntimeConfig {
                workers: 1,
                ..Default::default()
            },
            Some(storage.path()),
        );

        let env = create_runtime_env(&cfg).unwrap();
        // Anything other than `Finite(_)` proves no FairSpillPool was wired in.
        match env.memory_pool.memory_limit() {
            MemoryLimit::Finite(bytes) => {
                panic!("expected an unbounded pool, got finite limit of {bytes} bytes");
            }
            _ => {}
        }
    }

    #[test]
    fn create_runtime_env_uses_resources_memory_mb_max_fallback() {
        let storage = TempStorage::new("feldera-datafusion-create-runtime-env-resources-test");
        let cfg = pipeline_config(
            RuntimeConfig {
                workers: 1,
                max_rss_mb: None,
                resources: ResourceConfig {
                    memory_mb_max: Some(16_000),
                    ..Default::default()
                },
                ..Default::default()
            },
            Some(storage.path()),
        );

        let env = create_runtime_env(&cfg).unwrap();
        match env.memory_pool.memory_limit() {
            MemoryLimit::Finite(bytes) => assert_eq!(bytes, 800 * 1_000_000),
            MemoryLimit::Infinite => panic!("expected a bounded memory pool, got Infinite"),
            MemoryLimit::Unknown => panic!("expected a bounded memory pool, got Unknown"),
        }
    }

    #[test]
    fn create_runtime_env_wipes_stale_scratch_entries() {
        let storage = TempStorage::new("feldera-datafusion-create-runtime-env-wipe-test");
        let scratch = storage.path().join(DATAFUSION_TEMP_DIR);
        fs::create_dir_all(&scratch).unwrap();

        // Simulate leftovers from a prior crashed process.
        let stale_subdir = scratch.join("datafusion-stale1");
        fs::create_dir_all(&stale_subdir).unwrap();
        fs::write(stale_subdir.join("orphan.arrow"), b"stale").unwrap();
        let stale_file = scratch.join("loose.tmp");
        fs::write(&stale_file, b"stale").unwrap();

        let cfg = pipeline_config(
            RuntimeConfig {
                workers: 1,
                ..Default::default()
            },
            Some(storage.path()),
        );
        create_runtime_env(&cfg).unwrap();

        assert!(
            scratch.is_dir(),
            "scratch root must survive cleanup; gc_startup keeps it on the allowlist",
        );
        assert!(
            !stale_subdir.exists(),
            "stale per-DiskManager subdir should be removed on startup",
        );
        assert!(
            !stale_file.exists(),
            "stale loose file should be removed on startup",
        );
    }

    #[test]
    fn create_session_context_target_partitions_match_workers() {
        let storage = TempStorage::new("feldera-datafusion-create-session-context-workers-test");
        let cfg = pipeline_config(
            RuntimeConfig {
                workers: 7,
                ..Default::default()
            },
            Some(storage.path()),
        );
        let env = create_runtime_env(&cfg).unwrap();
        let ctx = create_session_context(&cfg, env);
        assert_eq!(ctx.copied_config().target_partitions(), 7);
    }

    #[test]
    fn create_session_context_target_partitions_prefer_io_workers() {
        let storage = TempStorage::new("feldera-datafusion-create-session-context-io-workers-test");
        let cfg = pipeline_config(
            RuntimeConfig {
                workers: 4,
                io_workers: Some(12),
                ..Default::default()
            },
            Some(storage.path()),
        );
        let env = create_runtime_env(&cfg).unwrap();
        let ctx = create_session_context(&cfg, env);
        assert_eq!(ctx.copied_config().target_partitions(), 12);
    }

    #[test]
    fn create_session_context_with_customise_overrides_defaults() {
        use super::create_session_context_with;
        let storage = TempStorage::new("feldera-datafusion-create-session-context-override-test");
        let cfg = pipeline_config(
            RuntimeConfig {
                workers: 4,
                ..Default::default()
            },
            Some(storage.path()),
        );
        let env = create_runtime_env(&cfg).unwrap();
        // Customise hook must win over the worker-derived defaults.
        let ctx = create_session_context_with(&cfg, env, |c| {
            c.set_usize("datafusion.execution.target_partitions", 99)
        });
        assert_eq!(ctx.copied_config().target_partitions(), 99);
    }

    /// Every context built here must provide the JSON function family.
    #[test]
    fn create_session_context_registers_json_functions() {
        let storage = TempStorage::new("feldera-datafusion-create-session-context-json-test");
        let cfg = pipeline_config(
            RuntimeConfig {
                workers: 1,
                ..Default::default()
            },
            Some(storage.path()),
        );
        let env = create_runtime_env(&cfg).unwrap();
        let ctx = create_session_context(&cfg, env);
        let state = ctx.state();
        for function in [
            "json_get",
            "json_get_str",
            "json_get_int",
            "json_get_float",
            "json_get_bool",
            "json_get_json",
            "json_get_array",
            "json_as_text",
            "json_contains",
            "json_length",
            "json_object_keys",
        ] {
            assert!(
                state.scalar_functions().contains_key(function),
                "JSON function '{function}' is not registered"
            );
        }
    }

    /// Tripwire: `clean_stale_scratch_entries` refuses to walk a directory
    /// whose final component isn't `DATAFUSION_TEMP_DIR`, so a misuse can't
    /// recursively wipe an arbitrary path.
    #[test]
    fn clean_stale_scratch_entries_refuses_unexpected_paths() {
        use super::clean_stale_scratch_entries;
        let storage = TempStorage::new("feldera-datafusion-clean-scratch-guard-test");
        let bogus = storage.path().join("not-the-scratch-dir");
        fs::create_dir_all(&bogus).unwrap();
        let canary = bogus.join("canary.txt");
        fs::write(&canary, b"do not delete").unwrap();

        clean_stale_scratch_entries(&bogus);

        assert!(
            canary.exists(),
            "guard must not delete contents of a directory whose name != DATAFUSION_TEMP_DIR",
        );
    }

    /// Every partition books its merge headroom before reading a row, so the
    /// aggregate has to fit the pool whatever the partition count is. This is
    /// the property a fixed 64 MiB violated: 8 partitions booked 537 MB
    /// against the 483 MB pool of a 9 GiB pipeline.
    #[test]
    fn sort_reservation_fits_the_pool() {
        use super::{
            MAX_SORT_SPILL_RESERVATION_BYTES, SORT_RESERVATION_POOL_DIVISOR,
            sort_spill_reservation_bytes,
        };

        let mut failures = Vec::new();
        for pool_mb in [12u64, 25, 100, 256, 483, 800, 2_000] {
            for partitions in [1usize, 2, 4, 8, 16, 32, 64, 128] {
                let pool = (pool_mb * 1_000_000) as usize;
                let aggregate = sort_spill_reservation_bytes(Some(pool), partitions) * partitions;
                let budget = pool / SORT_RESERVATION_POOL_DIVISOR;
                if aggregate > budget {
                    failures.push(format!(
                        "pool={pool_mb} MB partitions={partitions}: {aggregate} B booked \
                         exceeds the {budget} B budget"
                    ));
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));

        // Exact values, so an implementation that simply hands out nothing
        // cannot satisfy the inequality above.
        assert_eq!(
            sort_spill_reservation_bytes(Some(483_000_000), 8),
            483_000_000 / 4 / 8,
        );
        // A small pool shared by many partitions thins the reservation rather
        // than clamping it up; clamping up is what would reintroduce
        // `partitions * floor > pool`.
        assert_eq!(sort_spill_reservation_bytes(Some(12_000_000), 64), 46_875);
        // Degenerate partition counts must not divide by zero.
        assert_eq!(sort_spill_reservation_bytes(Some(12_000_000), 0), 3_000_000);
        // A pool that can fund the ceiling gets it, and an unbounded pool has
        // nothing to divide.
        assert_eq!(
            sort_spill_reservation_bytes(Some(4_000_000_000), 8),
            MAX_SORT_SPILL_RESERVATION_BYTES,
        );
        assert_eq!(
            sort_spill_reservation_bytes(None, 64),
            MAX_SORT_SPILL_RESERVATION_BYTES,
        );
    }

    /// The partition count a session budgets for has to be the one its
    /// queries actually run, which differs between the two kinds of session.
    /// Both settings derived from it are checked here, since both are wrong
    /// together when the count is.
    #[test]
    fn sessions_budget_for_the_partitions_they_run() {
        use super::create_session_context_with;

        /// `hosts` and `io_workers` are the two knobs that separate the
        /// partition count from `workers`; `connector` picks the session kind.
        struct Case {
            name: &'static str,
            connector: bool,
            workers: u16,
            hosts: usize,
            io_workers: Option<u64>,
            pool_mb: u64,
            /// `target_partitions`, which bounds a connector's scans.
            target_partitions: usize,
            /// Partitions the session budgets sorts for.
            sort_partitions: usize,
            repartition_sorts: bool,
        }

        let cases = [
            // The reported failure: `io_workers` unset, so `target_partitions`
            // already equals the worker count and 483 MB funds 8 sorters.
            Case {
                name: "adhoc, defaults",
                connector: false,
                workers: 8,
                hosts: 1,
                io_workers: None,
                pool_mb: 483,
                target_partitions: 8,
                sort_partitions: 8,
                repartition_sorts: true,
            },
            // The ad-hoc scan opens one partition per worker whatever
            // `target_partitions` says, so a lower `io_workers` must not
            // shrink the budget.
            Case {
                name: "adhoc, io_workers below workers",
                connector: false,
                workers: 16,
                hosts: 1,
                io_workers: Some(2),
                pool_mb: 480,
                target_partitions: 2,
                sort_partitions: 16,
                repartition_sorts: false,
            },
            // A higher `io_workers` raises what DataFusion may repartition to.
            Case {
                name: "adhoc, io_workers above workers",
                connector: false,
                workers: 4,
                hosts: 1,
                io_workers: Some(12),
                pool_mb: 480,
                target_partitions: 12,
                sort_partitions: 12,
                repartition_sorts: false,
            },
            // `workers` counts every host while the pool and the snapshot are
            // per host, so a host budgets for its own share. Budgeting for all
            // 32 would coalesce a sort this pool funds.
            Case {
                name: "adhoc, four hosts",
                connector: false,
                workers: 32,
                hosts: 4,
                io_workers: Some(4),
                pool_mb: 480,
                target_partitions: 4,
                sort_partitions: 8,
                repartition_sorts: true,
            },
            // A connector queries external tables, never the ad-hoc snapshot,
            // so nothing there opens a partition per worker.
            Case {
                name: "connector, io_workers below workers",
                connector: true,
                workers: 16,
                hosts: 1,
                io_workers: Some(2),
                pool_mb: 480,
                target_partitions: 2,
                sort_partitions: 2,
                repartition_sorts: true,
            },
        ];

        for case in cases {
            let storage = TempStorage::new(&format!("feldera-df-session-{}", case.name));
            let cfg = pipeline_config(
                RuntimeConfig {
                    workers: case.workers,
                    hosts: case.hosts,
                    io_workers: case.io_workers,
                    datafusion_memory_mb: Some(case.pool_mb),
                    max_rss_mb: Some(64_000),
                    ..Default::default()
                },
                Some(storage.path()),
            );
            let env = create_runtime_env(&cfg).unwrap();
            let ctx = if case.connector {
                create_session_context_with(&cfg, env, |c| c)
            } else {
                create_session_context(&cfg, env)
            };

            let config = ctx.copied_config();
            let name = case.name;
            assert_eq!(
                config.target_partitions(),
                case.target_partitions,
                "{name}: target_partitions",
            );
            // The expected partition count is stated, not re-derived from the
            // production helper: deriving it would move expectation and actual
            // together and the assertion could never fail.
            assert_eq!(
                config.options().execution.sort_spill_reservation_bytes,
                case.pool_mb as usize * 1_000_000 / 4 / case.sort_partitions,
                "{name}: reservation must divide by {} partitions",
                case.sort_partitions,
            );
            assert_eq!(
                config.repartition_sorts(),
                case.repartition_sorts,
                "{name}: repartition_sorts",
            );
        }
    }

    /// End to end through a real plan: an `ORDER BY` over one row per worker,
    /// on a pool smaller than a single fixed 64 MiB reservation. Catches a
    /// reservation that is computed but never reaches the session, which the
    /// unit tests above cannot see.
    #[test]
    fn order_by_succeeds_on_a_pool_smaller_than_the_old_reservation() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field as ArrowField, Schema};
        use datafusion::common::arrow::array::RecordBatch;
        use datafusion::datasource::MemTable;
        use std::sync::Arc;

        const WORKERS: usize = 8;
        let storage = TempStorage::new("feldera-datafusion-order-by-small-pool-test");
        let cfg = pipeline_config(
            RuntimeConfig {
                workers: WORKERS as u16,
                // Below one 64 MiB reservation, so the first partition used
                // to fail before comparing a row.
                datafusion_memory_mb: Some(12),
                max_rss_mb: Some(16_000),
                ..Default::default()
            },
            Some(storage.path()),
        );
        let ctx = create_session_context(&cfg, create_runtime_env(&cfg).unwrap());

        // One partition per DBSP worker, mirroring `AdHocQueryExecution`.
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "cnt",
            DataType::Int64,
            false,
        )]));
        let partitions: Vec<Vec<RecordBatch>> = (0..WORKERS)
            .map(|worker| {
                vec![
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int64Array::from(vec![worker as i64]))],
                    )
                    .unwrap(),
                ]
            })
            .collect();
        ctx.register_table(
            "q1",
            Arc::new(MemTable::try_new(schema, partitions).unwrap()),
        )
        .unwrap();

        let batches = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(super::execute_query_collect(
                &ctx,
                "select * from q1 order by cnt desc",
            ))
            .unwrap();
        // Assert the ordering, not just the row count: a plan that dropped
        // the sort would return the same rows.
        let sorted: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("cnt column is Int64")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(sorted, (0..WORKERS as i64).rev().collect::<Vec<_>>());
    }

    /// Make sure random shapes for `filter`, `cdc_delete_filter`, and `cdc_order_by`
    /// are parsed correctly by our connector.
    #[test]
    fn cdc_connector_expr_shapes_validate() {
        use super::{validate_sql_expression, validate_sql_order_by};

        // Set as `filter` or `cdc_delete_filter`; validated as a scalar predicate.
        const FILTER_SHAPES: &[&str] = &[
            "0=0",
            "0=0 AND (a = 's0' AND b = 's1')",
            "0=0 AND (a = 's0')",
            "0=0 AND (a IN ('s0'))",
            "0=0 AND (a IN ('s0','s1'))",
            "0=0 AND (a IN (1,2) OR a IS NULL)",
            "0=0 AND (a IN (1,2) OR a IS NULL) AND (b = false)",
            "0=0 AND (a IN (1,2) OR a IS NULL) AND (b IN ('s0'))",
            "0=0 AND (a IN (1,2) OR a IS NULL) AND (b IN ('s0','s1'))",
            "0=0 AND (a IN (1,2) OR a IS NULL) AND (b IS NOT NULL)",
            "0=0 AND (a IN (1,2) OR a IS NULL) AND (b IS NULL AND c IS NULL)",
            "0=0 AND (a IN (1,2) OR a IS NULL) AND (b IS NULL)",
            "0=0 AND (a IN (1,2) OR a IS NULL) AND (b NOT IN ('s0','s1') AND c IS NOT NULL)",
            "0=0 AND (a IN('s0','s1'))",
            "0=0 AND (a IS NOT NULL AND b IS NOT NULL)",
            "0=0 AND a = false",
            "0=0 AND a = false AND (b = 's0' AND c = 's1')",
            "0=0 AND a = false AND (b = 's0')",
            "0=0 AND a = false AND (b IN ('s0'))",
            "0=0 AND a = false AND (b IN ('s0','s1'))",
            "0=0 AND a = false AND (b IN (1,2) OR b IS NULL)",
            "0=0 AND a = false AND (b IN (1,2) OR b IS NULL) AND (c = false)",
            "0=0 AND a = false AND (b IN (1,2) OR b IS NULL) AND (c IS NOT NULL)",
            "0=0 AND a = false AND (b IS NOT NULL AND c IS NOT NULL)",
            "0=0 AND a = false AND b is null",
            "0=0 AND a = false AND b is null AND (c = 's0')",
            "0=0 AND a = false AND b is null AND (c IN ('s0','s1'))",
            "0=0 AND a = false AND b is null AND (c IN (1,2) OR c IS NULL)",
            "0=0 AND a = false AND b is null AND (c IN (1,2) OR c IS NULL) AND (d NOT IN ('s0','s1') AND e IS NOT NULL)",
            "a > 0",
            "a >= 0 AND a <= 9",
            "a <> 's0'",
            "a != 's0'",
            "a BETWEEN 0 AND 9",
            "a LIKE 's0'",
            "a IS NULL OR b IS NOT NULL",
            "NOT (a = false)",
            "lower(a) = 's0'",
            "cast(a AS bigint) = 0",
            "a + b > 0",
            "coalesce(a, b) = 's0'",
            "a > timestamp '2020-01-02 03:04:05'",
            "a = 's0''s1'",
        ];
        const CDC_DELETE_FILTER_SHAPES: &[&str] = &[
            "a = true",
            "a = true OR b is not null",
            "a = true AND b = false",
            "a IN ('s0','s1')",
            "a IS NOT NULL",
            "NOT a",
        ];
        const CDC_ORDER_BY_SHAPES: &[&str] = &[
            "a",
            "a, b",
            "a asc, b asc",
            "a ASC",
            "a desc",
            "a ASC, b DESC",
            "a NULLS FIRST",
            "a ASC NULLS LAST",
            "a DESC NULLS FIRST",
            "a asc nulls last, b desc nulls first",
            "a asc, b desc, c asc nulls last",
            "a + b asc",
            "a % 2 asc, b desc",
            "lower(a) asc",
            "abs(a) desc, b asc",
            "cast(a AS bigint) asc",
            "coalesce(a, b) asc, c desc",
            "case when a then 0 else 1 end desc",
            // Quoted identifier containing a space.
            "\"a b\" asc",
        ];

        let mut failures = Vec::new();
        for expr in FILTER_SHAPES.iter().chain(CDC_DELETE_FILTER_SHAPES) {
            if let Err(e) = validate_sql_expression(expr) {
                failures.push(format!("predicate '{expr}' failed: {e}"));
            }
        }
        for order_by in CDC_ORDER_BY_SHAPES {
            if let Err(e) = validate_sql_order_by(order_by) {
                failures.push(format!("cdc_order_by '{order_by}' failed: {e}"));
            }
        }

        assert!(
            failures.is_empty(),
            "validation failures:\n{}",
            failures.join("\n")
        );
    }

    fn columns(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn expression_columns_are_collected() {
        for (expr, expected) in [
            ("__is_deleted = true", columns(&["__is_deleted"])),
            ("deleted_at is not null", columns(&["deleted_at"])),
            (
                "__is_deleted = true OR deleted_at is not null",
                columns(&["__is_deleted", "deleted_at"]),
            ),
            // Function arguments are walked; the function name is not a column.
            ("lower(status) = 'gone'", columns(&["status"])),
            // A compound reference is a qualified column (`t.deleted`) or a
            // struct field access (`info.deleted`), so both parts are kept.
            ("info.deleted = true", columns(&["info", "deleted"])),
            // The column is the first part under struct nesting and the second
            // under a qualifier, so deeper nesting keeps every part.
            (
                "t.info.flags.deleted",
                columns(&["t", "info", "flags", "deleted"]),
            ),
            // A predicate over no columns yields the empty set.
            ("1 = 1", columns(&[])),
        ] {
            assert_eq!(
                columns_referenced_by_expression(expr).unwrap(),
                expected,
                "columns of '{expr}'"
            );
        }
    }

    #[test]
    fn order_by_columns_are_collected() {
        assert_eq!(
            columns_referenced_by_order_by("ts asc, lsn desc").unwrap(),
            columns(&["ts", "lsn"]),
        );
        assert_eq!(
            columns_referenced_by_order_by("coalesce(ts, created_at) asc").unwrap(),
            columns(&["ts", "created_at"]),
        );
        // ASC/DESC and NULLS FIRST/LAST modifiers parse but are not columns.
        assert_eq!(
            columns_referenced_by_order_by("ts desc nulls last, lsn asc").unwrap(),
            columns(&["ts", "lsn"]),
        );
    }

    #[test]
    fn malformed_expressions_error() {
        assert!(columns_referenced_by_expression("a =").is_err());
        // A trailing key past the first must still fail rather than be dropped.
        assert!(columns_referenced_by_order_by("ts asc,").is_err());
    }
}
