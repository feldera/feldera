use crate::errors::journal::ControllerError;
use anyhow::{Error as AnyError, anyhow};
use arrow::array::Array;
use datafusion::common::ScalarValue;
use datafusion::common::arrow::array::{AsArray, RecordBatch};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::logical_expr::sqlparser::parser::ParserError;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use feldera_types::config::PipelineConfig;
use feldera_types::constants::DATAFUSION_TEMP_DIR;
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlType};
use std::ffi::OsStr;
use std::fs::{create_dir_all, read_dir, remove_dir_all, remove_file};
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::warn;

/// In-memory sort threshold; above this, sorts spill to disk. 64 MiB.
///
/// Powers of two align with page sizes (4 KiB / 2 MiB) the allocator
/// hands back, so a `1 << 26` budget matches what the OS actually
/// reserves rather than a round decimal value the OS rounds up anyway.
const SORT_IN_PLACE_THRESHOLD_BYTES: usize = 1 << 26;

/// Memory withheld from the sort phase for the merge phase to use. 64 MiB.
///
/// Reserved per partition: a sort with N partitions pre-allocates
/// `N * SORT_SPILL_RESERVATION_BYTES` from the pool.
/// If the pool can't satisfy that, the query fails immediately
/// with `Resources exhausted`. `create_runtime_env` emits a startup warning
/// when the configured pool is below `workers * SORT_SPILL_RESERVATION_BYTES`.
///
/// Note: DataFusion 52.x emits noisy `WARN datafusion_physical_plan::spill:
/// Record batch memory usage ... exceeds the expected limit ... by more
/// than the allowed tolerance` lines during spilled sorts. The overage is
/// typically a handful of bytes over a 4 KB tolerance -- upstream
/// accounting drift, tracked at
/// <https://github.com/apache/datafusion/issues/17340> Not a query failure
const SORT_SPILL_RESERVATION_BYTES: usize = 1 << 26;

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
        warn_if_pool_too_small_for_adhoc_sort(pipeline_config, datafusion_memory_mb);
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

/// Minimum DataFusion pool size, in MB, that can satisfy the ad-hoc
/// engine's per-partition sort reservation given `workers`.
///
/// Ad-hoc sessions set `target_partitions = workers`
/// (see [`create_session_context`]), so an `ORDER BY` (or any other
/// sort-based operator) reserves `workers * SORT_SPILL_RESERVATION_BYTES`
/// from the pool *before* sorting any rows. The reservation is in
/// binary MiB (`1 << 26`); the pool is sized from the user-facing
/// `datafusion_memory_mb` (decimal MB). Compare in bytes, then
/// ceil-divide to MB so the warning's threshold is never lower than
/// the actual byte requirement.
fn min_pool_mb_for_adhoc_sort(workers: u64) -> u64 {
    let needed_bytes = (SORT_SPILL_RESERVATION_BYTES as u64).saturating_mul(workers);
    needed_bytes.div_ceil(1_000_000)
}

/// Warn at startup when the DataFusion pool is too small to satisfy the
/// per-partition sort reservation for the ad-hoc query engine.
///
/// If the pool can't satisfy that, the query fails on the first reservation
/// attempt with `Resources exhausted`. Surface this as a single startup
/// warning so the failure mode isn't silent. Connector sessions can override
/// `target_partitions`; their reservation budget is not checked here.
fn warn_if_pool_too_small_for_adhoc_sort(pipeline_config: &PipelineConfig, pool_mb: u64) {
    let workers = pipeline_config.global.workers as u64;
    // Degenerate configs (tests / synthetic) report `workers == 0`; nothing
    // useful to say in that case and the message would print "0 MB".
    if workers == 0 {
        return;
    }
    let min_pool_mb = min_pool_mb_for_adhoc_sort(workers);
    // `<=` not `<`: at exact equality every partition's reservation sums to
    // the full pool with zero headroom. FairSpillPool's internal accounting
    // takes a few bytes of overhead, so the last partition's reservation
    // fails by a fraction of a MB. Empirically: pool=256 / workers=4
    // fails; 257 succeeds.
    if pool_mb <= min_pool_mb {
        let per_worker_mb = min_pool_mb_for_adhoc_sort(1);
        warn!(
            "DataFusion memory pool is {pool_mb} MB; sort-heavy ad-hoc \
             queries (ORDER BY, EXCEPT, hash joins) need at least \
             {min_pool_mb} MB ({workers} workers x {per_worker_mb} MB \
             reservation per worker). Such queries may fail at first \
             allocation with 'Resources exhausted'. Increase \
             'datafusion_memory_mb' or reduce 'workers'."
        );
    }
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

/// `SessionContext` bound to the shared [`RuntimeEnv`], configured with the
/// pipeline's worker count and feldera's sort-spill thresholds.
pub fn create_session_context(
    pipeline_config: &PipelineConfig,
    runtime_env: Arc<RuntimeEnv>,
) -> SessionContext {
    create_session_context_with(pipeline_config, runtime_env, |cfg| cfg)
}

/// Like [`create_session_context`], with a hook to override individual
/// datafusion settings (e.g. parquet decoding) before the context is built.
pub fn create_session_context_with<F>(
    pipeline_config: &PipelineConfig,
    runtime_env: Arc<RuntimeEnv>,
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
        .with_sort_spill_reservation_bytes(SORT_SPILL_RESERVATION_BYTES)
        .set(
            "datafusion.execution.planning_concurrency",
            &ScalarValue::UInt64(Some(workers)),
        );
    let session_config = customize_config(session_config);

    let state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .build();
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
    use super::{create_runtime_env, create_session_context};
    use datafusion::execution::memory_pool::MemoryLimit;
    use feldera_types::config::{PipelineConfig, ResourceConfig, RuntimeConfig, StorageConfig};
    use feldera_types::constants::DATAFUSION_TEMP_DIR;
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

    /// Pins the boundary that drives the `warn_if_pool_too_small_for_adhoc_sort`
    /// log line. If `SORT_SPILL_RESERVATION_BYTES` changes, the warning
    /// threshold changes with it
    #[test]
    fn min_pool_mb_for_adhoc_sort_matches_reservation_times_workers() {
        use super::min_pool_mb_for_adhoc_sort;
        // SORT_SPILL_RESERVATION_BYTES is 64 MiB = 67_108_864 B; the
        // resolved pool size is reported in decimal MB, so each worker's
        // requirement ceil-divides to 68 MB.
        assert_eq!(min_pool_mb_for_adhoc_sort(0), 0);
        assert_eq!(min_pool_mb_for_adhoc_sort(1), 68);
        assert_eq!(min_pool_mb_for_adhoc_sort(2), 135);
        assert_eq!(min_pool_mb_for_adhoc_sort(8), 537);
    }
}
