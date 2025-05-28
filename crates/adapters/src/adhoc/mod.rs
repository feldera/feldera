use crate::PipelineError;
use actix_web::{http::header, web::Payload, HttpRequest, HttpResponse};
use actix_ws::{AggregatedMessage, CloseCode, CloseReason, Closed, Session as WsSession};
use datafusion::common::ScalarValue;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use executor::{
    infallible_from_bytestring, stream_arrow_query, stream_json_query, stream_parquet_query,
    stream_text_query,
};
use feldera_adapterlib::errors::journal::ControllerError;
use feldera_types::config::PipelineConfig;
use feldera_types::query::{AdHocResultFormat, AdhocQueryArgs};
use futures_util::StreamExt;
use std::convert::Infallible;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Arc;

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
        let memory_bytes_max = memory_mb_max * 1024 * 1024;
        runtime_env_builder = runtime_env_builder
            .with_memory_pool(Arc::new(FairSpillPool::new(memory_bytes_max as usize)));
    }
    // Initialize datafusion spill-to-disk directory
    if let Some(storage) = &config.storage_config {
        let path = PathBuf::from(storage.path.clone()).join("adhoc-tmp");
        if !path.exists() {
            create_dir_all(&path).map_err(|error| {
                ControllerError::io_error(
                    String::from("unable to create ad-hoc scratch space directory during startup"),
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

async fn adhoc_query_handler(
    df: DataFrame,
    ws_session: &mut WsSession,
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
                        // Just end the current client query if the request encountered an error
                        // but keep connection open for further queries.
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
                                    error:
                                        "Error while streaming query results in arrow-ipc format"
                                            .to_string(),
                                    df: Some(err),
                                })
                                .unwrap(),
                            )
                            .await?;
                        break;
                    }
                }
            }
        }
        AdHocResultFormat::Parquet => {
            let mut stream = Box::pin(stream_parquet_query(df));
            while let Some(res) = stream.next().await {
                match res {
                    Ok(bytes) => {
                        ws_session.binary(bytes).await?;
                    }
                    Err(err) => {
                        ws_session
                            .text(
                                serde_json::to_string(&PipelineError::AdHocQueryError {
                                    error: "Error while streaming query results in parquet format"
                                        .to_string(),
                                    df: Some(err),
                                })
                                .unwrap(),
                            )
                            .await?;
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

pub async fn adhoc_websocket(
    df_session: SessionContext,
    req: HttpRequest,
    stream: Payload,
) -> Result<HttpResponse, PipelineError> {
    let (res, mut ws_session, stream) =
        actix_ws::handle(&req, stream).map_err(|e| PipelineError::AdHocQueryError {
            error: format!("Unable to intialize websocket connection: {}", e),
            df: None,
        })?;
    let mut stream = stream
        .aggregate_continuations()
        .max_continuation_size(16 * 1024 * 1024);

    actix_web::rt::spawn(async move {
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(AggregatedMessage::Text(text)) => {
                    let sql_request = text.to_string();
                    let maybe_args =
                        serde_json::from_str::<AdhocQueryArgs>(&sql_request).map_err(|e| {
                            PipelineError::AdHocQueryError {
                                error: format!(
                                    "Unable to parse adhoc query from the provided JSON: {}",
                                    e
                                ),
                                df: None,
                            }
                        });

                    match maybe_args {
                        Ok(args) => {
                            let df = df_session
                                .sql_with_options(
                                    &args.sql,
                                    SQLOptions::new().with_allow_ddl(false),
                                )
                                .await
                                .map_err(|e| PipelineError::AdHocQueryError {
                                    error: format!("Unable to execute SQL query: {}", e),
                                    df: Some(e),
                                });
                            match df {
                                Ok(df) => {
                                    // If the query is successful, we handle it based on the format.
                                    if adhoc_query_handler(df, &mut ws_session, args)
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    if ws_session
                                        .text(serde_json::to_string(&e).unwrap_or(e.to_string()))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let _r = ws_session
                                .close(Some(CloseReason {
                                    code: CloseCode::Error,
                                    description: Some(
                                        serde_json::to_string(&e).unwrap_or(e.to_string()),
                                    ),
                                }))
                                .await;
                            return;
                        }
                    }
                }
                Ok(AggregatedMessage::Binary(_)) => {
                    let _r = ws_session
                        .close(Some(CloseReason {
                            code: CloseCode::Unsupported,
                            description: Some("Binary requests are not supported".into()),
                        }))
                        .await;
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

/// Stream the result of an ad-hoc query using a HTTP streaming response.
pub async fn stream_adhoc_result(
    args: AdhocQueryArgs,
    session: SessionContext,
) -> Result<HttpResponse, PipelineError> {
    let df = session.sql(&args.sql).await?;

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
                format!("ERROR: {}", e.to_string()).into()
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
    }
}
