use crate::controller::ConsistentSnapshot;
use crate::{Controller, PipelineError};
use actix_web::{HttpRequest, HttpResponse, http::header, web::Payload};
use actix_ws::{AggregatedMessage, CloseCode, CloseReason, Closed, Session as WsSession};
use datafusion::common::metadata::ScalarAndMetadata;
use datafusion::common::{DFSchema, ParamValues, ScalarValue};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::{EmptyRelation, Execute, LogicalPlan, Prepare, Statement};
use datafusion::prelude::*;
use datafusion::sql::parser::{DFParserBuilder, Statement as DFStatement};
use datafusion::sql::sqlparser::dialect::dialect_from_str;
use executor::{
    hash_query_result, infallible_from_bytestring, stream_arrow_query, stream_json_query,
    stream_parquet_query, stream_text_query,
};
use feldera_adapterlib::errors::journal::ControllerError;
use feldera_types::config::PipelineConfig;
use feldera_types::query::{AdHocResultFormat, AdhocQueryArgs, MAX_WS_FRAME_SIZE};
use futures_util::StreamExt;
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::warn;

mod executor;
mod format;
pub(crate) mod table;

pub(crate) fn create_session_context(
    config: &PipelineConfig,
) -> Result<SessionContext, ControllerError> {
    const SORT_IN_PLACE_THRESHOLD_BYTES: usize = 64 * 1024 * 1024;
    const SORT_SPILL_RESERVATION_BYTES: usize = 64 * 1024 * 1024;
    let session_config = SessionConfig::new()
        .with_target_partitions(config.global.workers as usize)
        .with_sort_in_place_threshold_bytes(SORT_IN_PLACE_THRESHOLD_BYTES)
        .with_sort_spill_reservation_bytes(SORT_SPILL_RESERVATION_BYTES)
        .set(
            "datafusion.execution.planning_concurrency",
            &ScalarValue::UInt64(Some(config.global.workers as u64)),
        );
    // Initialize datafusion memory limits
    let mut runtime_env_builder = RuntimeEnvBuilder::new();
    if let Some(memory_mb_max) = config.global.resources.memory_mb_max {
        let memory_bytes_max = memory_mb_max * 1_000_000;
        runtime_env_builder = runtime_env_builder
            .with_memory_pool(Arc::new(FairSpillPool::new(memory_bytes_max as usize)));
    }
    // Initialize datafusion spill-to-disk directory
    if let Some(storage) = &config.storage_config {
        let path = PathBuf::from(storage.path.clone()).join("adhoc-tmp");
        if !path.exists() {
            create_dir_all(&path).map_err(|error| {
                ControllerError::io_error(
                    "unable to create ad-hoc scratch space directory during startup",
                    error,
                )
            })?;
        }
        runtime_env_builder = runtime_env_builder.with_temp_file_path(path);
    }

    let runtime_env = runtime_env_builder.build_arc().unwrap();
    let state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .build();
    Ok(SessionContext::from(state))
}

/// Helper for for closing the websocket session
///
/// Note that adding a `description` to the `CloseReason` is currently
/// buggy https://github.com/actix/actix-extras/issues/508
///
/// (It's actually very bad to add it because
/// websocket packets will be corrupted, don't.)
async fn ws_close(ws_session: WsSession, code: CloseCode) {
    let _r = ws_session
        .close(Some(CloseReason {
            code,
            description: None, // Must be None for now!
        }))
        .await;
}

async fn adhoc_query_handler(
    df: DataFrame,
    mut ws_session: WsSession,
    args: AdhocQueryArgs,
) -> Result<(), Closed> {
    match args.format {
        AdHocResultFormat::Text => {
            let mut stream = Box::pin(stream_text_query(df));
            while let Some(res) = stream.next().await {
                match res {
                    Ok(text) => {
                        ws_session.text(text).await?;
                    }
                    Err(e) => {
                        ws_session.text(format!("ERROR: {}", e)).await?;
                        ws_close(ws_session, CloseCode::Error).await;
                        break;
                    }
                }
            }
        }
        AdHocResultFormat::Json => {
            let mut stream = Box::pin(stream_json_query(df));
            while let Some(res) = stream.next().await {
                match res {
                    Ok(byte_string) => {
                        ws_session.text(byte_string).await?;
                    }
                    Err(json_err) => {
                        ws_session
                            .text(serde_json::to_string(&json_err).unwrap())
                            .await?;
                        ws_close(ws_session, CloseCode::Error).await;
                        break;
                    }
                }
            }
        }
        AdHocResultFormat::ArrowIpc => {
            let mut stream = Box::pin(stream_arrow_query(df));
            while let Some(res) = stream.next().await {
                match res {
                    Ok(bytes) => {
                        ws_session.binary(bytes).await?;
                    }
                    Err(err) => {
                        ws_session
                            .text(
                                serde_json::to_string(&PipelineError::AdHocQueryError {
                                    error: err.to_string(),
                                    df: Some(Box::new(err)),
                                })
                                .unwrap(),
                            )
                            .await?;
                        ws_close(ws_session, CloseCode::Error).await;
                        break;
                    }
                }
            }
        }
        AdHocResultFormat::Parquet => {
            let mut stream = Box::pin(stream_parquet_query(df));
            while let Some(res) = stream.next().await {
                match res {
                    Ok(bytes) => ws_session.binary(bytes).await?,
                    Err(err) => {
                        ws_session
                            .text(
                                serde_json::to_string(&PipelineError::AdHocQueryError {
                                    error: err.to_string(),
                                    df: Some(Box::new(err)),
                                })
                                .unwrap(),
                            )
                            .await?;
                        ws_close(ws_session, CloseCode::Error).await;
                        break;
                    }
                }
            }
        }
        AdHocResultFormat::Hash => {
            let hash_result = hash_query_result(df).await;
            match hash_result {
                Ok(hash) => {
                    ws_session.text(hash).await?;
                }
                Err(e) => {
                    ws_session
                        .text(serde_json::to_string(&e).unwrap_or(e.to_string()))
                        .await?;
                    ws_close(ws_session, CloseCode::Error).await;
                }
            }
        }
    }

    Ok(())
}

pub async fn adhoc_websocket(
    controller: Controller,
    req: HttpRequest,
    stream: Payload,
) -> Result<HttpResponse, PipelineError> {
    let (res, mut ws_session, stream) =
        actix_ws::handle(&req, stream).map_err(|e| PipelineError::AdHocQueryError {
            error: format!("Unable to intialize websocket connection: {}", e),
            df: None,
        })?;
    let mut stream = stream
        .max_frame_size(MAX_WS_FRAME_SIZE)
        .aggregate_continuations()
        .max_continuation_size(4 * MAX_WS_FRAME_SIZE);

    actix_web::rt::spawn(async move {
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(AggregatedMessage::Text(text)) => {
                    let sql_request = text.to_string();
                    let maybe_args = serde_json_path_to_error::from_str::<AdhocQueryArgs>(
                        &sql_request,
                    )
                    .map_err(|e| PipelineError::AdHocQueryError {
                        error: format!("Unable to parse adhoc query from the provided JSON: {}", e),
                        df: None,
                    });

                    match maybe_args {
                        Ok(args) => {
                            let df = execute_sql(&controller, &args.sql).await;
                            match df {
                                Ok(df) => {
                                    // If the query is successful, we handle it based on the format.
                                    if adhoc_query_handler(df, ws_session.clone(), args)
                                        .await
                                        .is_err()
                                    {
                                        // Connection was closed, we exit the loop.
                                        return;
                                    } else {
                                        ws_close(ws_session, CloseCode::Normal).await;
                                        return;
                                    }
                                }
                                Err(e) => {
                                    let _r = ws_session
                                        .text(serde_json::to_string(&e).unwrap_or(e.to_string()))
                                        .await;
                                    ws_close(ws_session, CloseCode::Error).await;
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            let _r = ws_session
                                .text(serde_json::to_string(&e).unwrap_or(e.to_string()))
                                .await;
                            ws_close(ws_session, CloseCode::Error).await;
                            return;
                        }
                    }
                }
                Ok(AggregatedMessage::Binary(_)) => {
                    let _r = ws_session
                        .text(json!({
                            "error": "Binary requests are not supported. Please use text messages."
                        }).to_string())
                        .await;
                    ws_close(ws_session, CloseCode::Error).await;
                    break;
                }
                Ok(AggregatedMessage::Ping(msg)) => {
                    if ws_session.pong(&msg).await.is_err() {
                        break;
                    }
                }
                _ => {}
            }
        }
    });

    Ok(res)
}

pub(crate) fn set_snapshot(session_state: &mut SessionState, snapshot: ConsistentSnapshot) {
    session_state.config_mut().set_extension(snapshot);
}

pub(crate) async fn execute_sql(
    controller: &Controller,
    sql: &str,
) -> Result<DataFrame, PipelineError> {
    let mut state = controller.session_context()?.state();
    set_snapshot(
        &mut state,
        controller
            .latest_consistent_snapshot()
            .await
            .ok_or_else(|| PipelineError::Initializing)?,
    );
    execute_sql_with_state(state, sql, Some(controller)).await
}

/// Plan and translate `sql` against `state`, applying `PREPARE`/`EXECUTE`
/// substitution within the scope of a single ad-hoc request.
///
/// Only the final statement returns rows. Earlier statements may be
/// `PREPARE`s or any non-result-producing statement (e.g. `INSERT`),
/// executed for their side effect. After such an intermediate write
/// runs, the per-request snapshot is refreshed so the trailing SELECT
/// sees the just-written rows.
///
/// `controller` is optional so unit tests can drive this function
/// without a running pipeline; in that case the post-write snapshot
/// refresh is skipped.
async fn execute_sql_with_state(
    mut state: SessionState,
    sql: &str,
    controller: Option<&Controller>,
) -> Result<DataFrame, PipelineError> {
    let mut statements = parse_sql_statements(&state, sql)?;
    if statements.is_empty() {
        return Err(PipelineError::AdHocQueryError {
            error: "no SQL statements were provided".to_string(),
            df: None,
        });
    }

    // Per-request prepared-statement cache. PREPARE/EXECUTE pairs only live
    // for the duration of a single ad-hoc query request, matching the way
    // clients submit them over a single HTTP or WebSocket call.
    let mut prepared: HashMap<String, LogicalPlan> = HashMap::new();
    let sql_options = SQLOptions::new().with_allow_ddl(false);

    // Subscribe to `step_watcher` *before* any intermediate writes so the
    // steps that drain those writes accumulate as unseen changes; calling
    // `changed()` after the writes return then completes immediately
    // instead of blocking on a step that may never be triggered.
    let mut step_watcher = controller.map(|c| c.step_watcher());

    let mut intermediate_wrote_data = false;
    while statements.len() > 1 {
        let stmt = statements.pop_front().unwrap();
        let plan = state.statement_to_plan(stmt).await?;
        match plan {
            LogicalPlan::Statement(Statement::Prepare(Prepare { name, input, .. })) => {
                sql_options.verify_plan(&input)?;
                prepared.insert(name, (*input).clone());
            }
            LogicalPlan::Statement(Statement::Execute(Execute { name, parameters })) => {
                // `EXECUTE` of a previously-prepared statement, used here
                // for its side effects (e.g. a prepared INSERT).
                let prepared_plan =
                    prepared
                        .remove(&name)
                        .ok_or_else(|| PipelineError::AdHocQueryError {
                            error: format!(
                                "prepared statement '{name}' is not defined in this request"
                            ),
                            df: None,
                        })?;
                let values = execute_parameters_to_scalars(&parameters)?;
                let bound = prepared_plan.replace_params_with_values(&ParamValues::List(values))?;
                sql_options.verify_plan(&bound)?;
                intermediate_wrote_data |= plan_writes_data(&bound);
                drain_intermediate_plan(&state, bound).await?;
            }
            other if is_result_producing_plan(&other) => {
                return Err(PipelineError::AdHocQueryError {
                    error: "only the final statement in a multi-statement \
                            ad-hoc query may return a result set; \
                            move SELECTs to the end or split into \
                            separate requests"
                        .to_string(),
                    df: None,
                });
            }
            other => {
                // Non-result-producing intermediate statement (INSERT,
                // UPDATE, DELETE, EXPLAIN, ...). Execute it for its side
                // effects and discard the per-statement count row.
                sql_options.verify_plan(&other)?;
                intermediate_wrote_data |= plan_writes_data(&other);
                drain_intermediate_plan(&state, other).await?;
            }
        }
    }

    // The snapshot pinned in `state` was captured at request start, before
    // any intermediate INSERT ran. Refresh it so the trailing SELECT sees
    // the just-written rows. Tracks
    // https://github.com/feldera/feldera/issues/6243.
    if intermediate_wrote_data
        && let Some(controller) = controller
        && let Some(watcher) = step_watcher.as_mut()
    {
        refresh_snapshot_after_writes(controller, watcher, &mut state).await?;
    }

    let stmt = statements.pop_front().unwrap();
    let plan = state.statement_to_plan(stmt).await?;

    let final_plan = match plan {
        LogicalPlan::Statement(Statement::Execute(Execute { name, parameters })) => {
            let prepared_plan =
                prepared
                    .remove(&name)
                    .ok_or_else(|| PipelineError::AdHocQueryError {
                        error: format!(
                            "prepared statement '{name}' is not defined in this request, use `prepare stmt; select stmt;` to write the query"
                        ),
                        df: None,
                    })?;
            let values = execute_parameters_to_scalars(&parameters)?;
            prepared_plan.replace_params_with_values(&ParamValues::List(values))?
        }
        LogicalPlan::Statement(Statement::Prepare(Prepare { input, .. })) => {
            // PREPARE with no matching EXECUTE in the same request has no
            // persistent effect. Validate the inner plan for DDL and return
            // an empty result to the client.
            warn!(
                "PREPARE with no matching EXECUTE in the same request has no persistent effect, returning an empty set"
            );
            sql_options.verify_plan(&input)?;
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::empty()),
            })
        }
        other => other,
    };

    sql_options.verify_plan(&final_plan)?;
    Ok(DataFrame::new(state, final_plan))
}

/// True if executing this plan would surface rows to the caller. Used to
/// reject queries like `SELECT; INSERT` where the early `SELECT` would
/// otherwise be silently dropped.
fn is_result_producing_plan(plan: &LogicalPlan) -> bool {
    !matches!(plan, LogicalPlan::Dml(_) | LogicalPlan::Statement(_))
}

/// Execute an intermediate statement for its side effects and drop the
/// resulting batches. INSERTs produce a one-row count; we keep that
/// count out of the response stream so only the request's final
/// statement contributes rows.
async fn drain_intermediate_plan(
    state: &SessionState,
    plan: LogicalPlan,
) -> Result<(), PipelineError> {
    let df = DataFrame::new(state.clone(), plan);
    let _ = df.collect().await?;
    Ok(())
}

/// True if executing this plan mutates a table (and therefore needs the
/// post-write snapshot refresh below). Today the only mutating plan our
/// SQL options allow is a `LogicalPlan::Dml`.
fn plan_writes_data(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Dml(_))
}

/// Wait for the controller to complete at least one full step after
/// the intermediate writes returned, then update `state`'s pinned
/// snapshot to the freshly produced one. The controller updates
/// `trace_snapshots` at the end of every non-transactional step, so
/// observing the next `Idle` transition is enough to guarantee that
/// our writes are visible.
///
/// `watcher` must have been created before the intermediate writes
/// happened so the steps that drain them are already buffered as
/// unseen changes; otherwise this function would block waiting for
/// a future step that may never be triggered on an idle pipeline.
async fn refresh_snapshot_after_writes(
    controller: &Controller,
    watcher: &mut tokio::sync::watch::Receiver<feldera_types::coordination::StepStatus>,
    state: &mut SessionState,
) -> Result<(), PipelineError> {
    use feldera_types::coordination::StepAction;

    loop {
        let status = *watcher.borrow_and_update();
        if matches!(status.action, StepAction::Idle) {
            break;
        }
        if watcher.changed().await.is_err() {
            break;
        }
    }
    let snapshot = controller
        .latest_consistent_snapshot()
        .await
        .ok_or(PipelineError::Initializing)?;
    set_snapshot(state, snapshot);
    Ok(())
}

/// Convert `EXECUTE` positional parameters to DataFusion's `ScalarAndMetadata`
/// list, rejecting anything that is not a literal value.
fn execute_parameters_to_scalars(params: &[Expr]) -> Result<Vec<ScalarAndMetadata>, PipelineError> {
    params
        .iter()
        .map(|expr| match expr {
            Expr::Literal(value, metadata) => {
                Ok(ScalarAndMetadata::new(value.clone(), metadata.clone()))
            }
            other => Err(PipelineError::AdHocQueryError {
                error: format!(
                    "EXECUTE parameters only support literal values: got {other} instead"
                ),
                df: None,
            }),
        })
        .collect()
}

fn parse_sql_statements(
    state: &SessionState,
    sql: &str,
) -> Result<VecDeque<DFStatement>, PipelineError> {
    let options = state.config_options();
    let dialect_name = &options.sql_parser.dialect;
    let recursion_limit = options.sql_parser.recursion_limit;
    let dialect = dialect_from_str(dialect_name).ok_or_else(|| PipelineError::AdHocQueryError {
        error: format!("unsupported SQL dialect: {dialect_name}"),
        df: None,
    })?;
    let statements = DFParserBuilder::new(sql)
        .with_dialect(dialect.as_ref())
        .with_recursion_limit(recursion_limit)
        .build()?
        .parse_statements()
        .map_err(format_parser_error)?;
    Ok(statements)
}

/// Convert a DataFusion error coming out of the SQL parser into a
/// `PipelineError` whose message is the parser's `Display`, not its
/// `Debug` form. The parser already appends the location ("at Line: X,
/// Column: Y") to its messages; preserving that string gives the user
/// something like
///   `sql parser error: Expected: end of statement, found: in at Line: 1, Column: 30`
/// instead of the wrapped
///   `SQL error: ParserError("Expected: ... at Line: 1, Column: 30")`.
///
/// The DataFusion parser may wrap its `DataFusionError::SQL` in a
/// `DataFusionError::Diagnostic`; unwrap that here so the inner parser
/// message reaches the user.
fn format_parser_error(error: datafusion::error::DataFusionError) -> PipelineError {
    use datafusion::error::DataFusionError;
    let inner = match error {
        DataFusionError::Diagnostic(_, inner) => *inner,
        other => other,
    };
    match inner {
        DataFusionError::SQL(parser_err, _) => PipelineError::AdHocQueryError {
            error: parser_err.to_string(),
            df: None,
        },
        other => PipelineError::from(other),
    }
}

/// Stream the result of an ad-hoc query using a HTTP streaming response.
pub(crate) async fn stream_adhoc_result(
    controller: &Controller,
    args: &AdhocQueryArgs,
) -> Result<HttpResponse, PipelineError> {
    let df = execute_sql(controller, &args.sql).await?;

    // Note that once we are in the stream!{} macros any error that occurs will lead to the connection
    // in the manager being terminated and a 500 error being returned to the client.
    // We can't return an error in a stream that is already Response::Ok.
    //
    // Sometimes things do tend to fail inside the stream!{} macro, e.g., "select 1/0;" will cause a
    // division by zero error during query execution. So we return errors according to the chosen
    // format for text and json, and for parquet we return the 500 error.
    match args.format {
        AdHocResultFormat::Text => Ok(HttpResponse::Ok()
            .content_type(mime::TEXT_PLAIN)
            .streaming::<_, Infallible>(infallible_from_bytestring(stream_text_query(df), |e| {
                format!("ERROR: {}", e).into()
            }))),
        AdHocResultFormat::Json => Ok(HttpResponse::Ok()
            .content_type(mime::APPLICATION_JSON)
            .streaming::<_, Infallible>(infallible_from_bytestring(
            stream_json_query(df),
            |e| serde_json::to_string(&e).unwrap().into(),
        ))),
        AdHocResultFormat::ArrowIpc => Ok(HttpResponse::Ok()
            .content_type(mime::APPLICATION_OCTET_STREAM)
            .streaming(stream_arrow_query(df))),
        AdHocResultFormat::Parquet => {
            let file_name = format!(
                "results_{}.parquet",
                chrono::Utc::now().format("%Y%m%d_%H%M%S")
            );
            Ok(HttpResponse::Ok()
                .insert_header(header::ContentDisposition::attachment(file_name))
                .content_type(mime::APPLICATION_OCTET_STREAM)
                .streaming(stream_parquet_query(df)))
        }
        AdHocResultFormat::Hash => Ok(HttpResponse::Ok()
            .content_type(mime::TEXT_PLAIN)
            .body(hash_query_result(df).await?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow;
    use datafusion::arrow::record_batch::RecordBatch;

    fn test_state() -> SessionState {
        SessionStateBuilder::new().with_default_features().build()
    }

    #[test]
    fn parses_single_statement() {
        let state = test_state();
        let stmts = parse_sql_statements(&state, "SELECT 1").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn trailing_semicolon_parses_as_one_statement() {
        let state = test_state();
        let stmts = parse_sql_statements(&state, "SELECT 1;").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parses_multiple_statements() {
        let state = test_state();
        let stmts = parse_sql_statements(&state, "PREPARE p AS SELECT 1; EXECUTE p").unwrap();
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn empty_input_yields_no_statements() {
        let state = test_state();
        let stmts = parse_sql_statements(&state, "   ").unwrap();
        assert!(stmts.is_empty());
    }

    #[test]
    fn invalid_sql_returns_error() {
        let state = test_state();
        assert!(parse_sql_statements(&state, "SELECT * FROM").is_err());
    }

    /// Parser errors must include the line/column of the offending token so
    /// the user can locate the typo without re-reading the query in their
    /// head.
    #[test]
    fn parse_error_message_carries_location() {
        let state = test_state();
        // 'in' is not a valid statement starter here; the parser stops on the
        // token after the column reference, which is at line 1 / column 30.
        let err = parse_sql_statements(&state, "select * from foo where bar = in baz")
            .expect_err("expected a parser error");
        let msg = format!("{err}");
        assert!(
            msg.contains("Line: 1"),
            "missing line number in error message: {msg}"
        );
        assert!(
            msg.contains("Column:"),
            "missing column number in error message: {msg}"
        );
        // The `Debug`-formatted `ParserError("...")` wrapper from earlier
        // versions of the message should be gone.
        assert!(
            !msg.contains("ParserError(\""),
            "raw Debug wrapper leaked into error message: {msg}"
        );
    }

    #[test]
    fn execute_parameters_to_scalars_rejects_non_literal() {
        let expr = Expr::Column(datafusion::common::Column::new_unqualified("foo"));
        assert!(execute_parameters_to_scalars(&[expr]).is_err());
    }

    #[test]
    fn execute_parameters_to_scalars_accepts_literal() {
        let expr = Expr::Literal(ScalarValue::Int64(Some(42)), None);
        let result = execute_parameters_to_scalars(&[expr]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, ScalarValue::Int64(Some(42)));
    }

    /// A helper that executes a query and returns results.
    async fn collect_rows(state: SessionState, sql: &str) -> Vec<RecordBatch> {
        execute_sql_with_state(state, sql, None)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn prepare_then_execute_binds_parameter() {
        let state = test_state();
        let batches = collect_rows(state, "PREPARE p AS SELECT $1 AS x; EXECUTE p(42)").await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("int64 column");
        assert_eq!(col.value(0), 42);
    }

    #[tokio::test]
    async fn execute_without_prepare_errors() {
        let state = test_state();
        let err = execute_sql_with_state(state, "EXECUTE missing(1)", None)
            .await
            .unwrap_err();
        assert!(
            format!("{err:?}").contains("not defined"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn bare_prepare_returns_empty_relation() {
        let state = test_state();
        let batches = collect_rows(state, "PREPARE p AS SELECT 1").await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    /// An intermediate `SELECT` (or any other result-producing statement)
    /// must be rejected: only one result set comes back per request, so
    /// executing the earlier SELECT silently would discard its rows.
    #[tokio::test]
    async fn intermediate_select_is_rejected() {
        let state = test_state();
        let err = execute_sql_with_state(state, "SELECT 1; SELECT 2", None)
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("final statement"), "unexpected error: {msg}");
    }

    /// Multiple `INSERT`s followed by a `SELECT` must execute in order,
    /// committing each insert's side effect, and only surface the final
    /// `SELECT`'s rows.
    #[tokio::test]
    async fn intermediate_inserts_run_and_final_select_returns_rows() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use std::sync::Arc;

        // Register a writable in-memory table so DML executes for real.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let mem = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
        let ctx = SessionContext::new_with_state(test_state());
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let state = ctx.state();

        let batches = collect_rows(
            state,
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); \
             SELECT SUM(x) AS s FROM t",
        )
        .await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        assert_eq!(col.value(0), 3);
    }

    /// A trailing `INSERT` (no final SELECT) must still execute, and
    /// the final statement's count row is surfaced as today.
    #[tokio::test]
    async fn final_insert_returns_count() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let mem = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
        let ctx = SessionContext::new_with_state(test_state());
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let state = ctx.state();

        let batches = collect_rows(
            state,
            "INSERT INTO t VALUES (10); INSERT INTO t VALUES (20)",
        )
        .await;
        // The final INSERT yields a single-row count batch; check only
        // that one row came back.
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }
}
