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
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlType};
use std::fs::create_dir_all;
use std::io::Error as IoError;
use std::path::PathBuf;
use std::sync::Arc;

/// Subdirectory under `pipeline_config.storage_config.path` where datafusion
/// writes spill files for the ad-hoc query engine and all integrated
/// connectors that use datafusion. Kept as a single pipeline-wide directory
/// (rather than one per connector) so that the on-disk layout is documented
/// in one place and so it can be cleaned up by a single GC.
pub const DATAFUSION_TMP_SUBDIR: &str = "datafusion-tmp";

/// Threshold above which datafusion sort operators consider spilling sorted
/// data to disk, in bytes.
const SORT_IN_PLACE_THRESHOLD_BYTES: usize = 64 * 1024 * 1024;

/// Memory reservation that datafusion sort operators keep available for
/// spilling sorted data to disk, in bytes.
const SORT_SPILL_RESERVATION_BYTES: usize = 64 * 1024 * 1024;

/// Build the shared datafusion [`RuntimeEnv`] for a pipeline.
///
/// The returned env carries a single [`FairSpillPool`] sized to
/// `pipeline_config.global.resources.memory_mb_max` (when set) and a single
/// spill-to-disk path under `{storage.path}/<DATAFUSION_TMP_SUBDIR>/` (when
/// storage is configured). The directory is created if it does not yet exist.
///
/// Build this exactly once per pipeline and share the resulting `Arc` across
/// every `SessionContext` (ad-hoc query engine and all integrated
/// connectors). Building one `RuntimeEnv` per `SessionContext` would give
/// each context its own memory pool sized to the full `memory_mb_max`, so the
/// effective pipeline-wide limit would be `(1 + #connectors) * memory_mb_max`.
pub fn create_runtime_env(
    pipeline_config: &PipelineConfig,
) -> Result<Arc<RuntimeEnv>, ControllerError> {
    let mut builder = RuntimeEnvBuilder::new();
    if let Some(memory_mb_max) = pipeline_config.global.resources.memory_mb_max {
        let memory_bytes_max = memory_mb_max * 1_000_000;
        builder = builder.with_memory_pool(Arc::new(FairSpillPool::new(memory_bytes_max as usize)));
    }
    if let Some(storage) = &pipeline_config.storage_config {
        let path = PathBuf::from(storage.path.clone()).join(DATAFUSION_TMP_SUBDIR);
        create_dir_all(&path).map_err(|error| {
            ControllerError::io_error(
                format!(
                    "unable to create datafusion scratch space directory '{}'",
                    path.display()
                ),
                error,
            )
        })?;
        builder = builder.with_temp_file_path(path);
    }
    builder.build_arc().map_err(|error| {
        ControllerError::io_error(
            "unable to build datafusion runtime environment",
            IoError::other(error.to_string()),
        )
    })
}

/// Create a datafusion `SessionContext` bound to the pipeline-wide
/// [`RuntimeEnv`].
///
/// The session inherits the runtime env's memory pool and spill path, and is
/// configured with the pipeline's worker count and the sort-spill thresholds
/// shared by all of feldera's datafusion users.
pub fn create_session_context(
    pipeline_config: &PipelineConfig,
    runtime_env: Arc<RuntimeEnv>,
) -> SessionContext {
    create_session_context_with(pipeline_config, runtime_env, |cfg| cfg)
}

/// Like [`create_session_context`], but lets the caller override individual
/// datafusion settings (for example, parquet decoding options) before the
/// context is built.
pub fn create_session_context_with<F>(
    pipeline_config: &PipelineConfig,
    runtime_env: Arc<RuntimeEnv>,
    customize_config: F,
) -> SessionContext
where
    F: FnOnce(SessionConfig) -> SessionConfig,
{
    let session_config = SessionConfig::new()
        .with_target_partitions(pipeline_config.global.workers as usize)
        .with_sort_in_place_threshold_bytes(SORT_IN_PLACE_THRESHOLD_BYTES)
        .with_sort_spill_reservation_bytes(SORT_SPILL_RESERVATION_BYTES)
        .set(
            "datafusion.execution.planning_concurrency",
            &ScalarValue::UInt64(Some(pipeline_config.global.workers as u64)),
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
    use super::{DATAFUSION_TMP_SUBDIR, create_runtime_env};
    use feldera_types::config::{PipelineConfig, RuntimeConfig, StorageConfig};
    use std::fs;

    #[test]
    fn create_runtime_env_creates_tmp_dir_under_storage() {
        let storage = std::env::temp_dir().join("feldera-datafusion-create-runtime-env-test");
        let _ = fs::remove_dir_all(&storage);
        fs::create_dir_all(&storage).unwrap();

        let cfg = PipelineConfig {
            global: RuntimeConfig {
                workers: 1,
                ..Default::default()
            },
            multihost: None,
            name: None,
            given_name: None,
            storage_config: Some(StorageConfig {
                path: storage.to_string_lossy().into(),
                cache: Default::default(),
            }),
            secrets_dir: None,
            inputs: Default::default(),
            outputs: Default::default(),
            program_ir: None,
        };
        create_runtime_env(&cfg).unwrap();

        let expected = storage.join(DATAFUSION_TMP_SUBDIR);
        let exists = expected.is_dir();
        let _ = fs::remove_dir_all(&storage);
        assert!(
            exists,
            "expected scratch directory at {}",
            expected.display(),
        );
    }
}
