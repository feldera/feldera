use crate::PipelineError;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    http::{header, StatusCode},
    response::Response,
    body::Body,
};
use futures_util::SinkExt;
use datafusion::common::ScalarValue;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use executor::{
    hash_query_result, infallible_from_bytestring, stream_arrow_query, stream_json_query,
    stream_parquet_query, stream_text_query,
};
use feldera_adapterlib::errors::journal::ControllerError;
use feldera_types::config::PipelineConfig;
use feldera_types::query::{AdHocResultFormat, AdhocQueryArgs};
use futures_util::StreamExt;
use serde_json::json;
use mime;
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

/// Helper for closing the websocket session
async fn ws_close(ws: &mut WebSocket, _code: u16) {
    let _ = ws.close().await;
}

async fn adhoc_query_handler(
    df: DataFrame,
    mut ws: &mut WebSocket,
    args: AdhocQueryArgs,
) -> Result<(), axum::Error> {
    match args.format {
        AdHocResultFormat::Text => {
            let mut stream = Box::pin(stream_text_query(df));
            while let Some(res) = stream.next().await {
                match res {
                    Ok(text) => {
                        ws.send(Message::Text(text.to_string().into())).await?;
                    }
                    Err(e) => {
                        ws.send(Message::Text(format!("ERROR: {}", e).into())).await?;
                        ws_close(ws, 1011).await; // 1011 = Internal Error
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
                        ws.send(Message::Text(byte_string.to_string().into())).await?;
                    }
                    Err(json_err) => {
                        ws.send(Message::Text(serde_json::to_string(&json_err).unwrap().into())).await?;
                        ws_close(ws, 1011).await; // 1011 = Internal Error
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
                        ws.send(Message::Binary(bytes)).await?;
                    }
                    Err(err) => {
                        ws.send(Message::Text(
                            serde_json::to_string(&PipelineError::AdHocQueryError {
                                error: err.to_string(),
                                df: Some(Box::new(err)),
                            })
                            .unwrap().into(),
                        )).await?;
                        ws_close(ws, 1011).await; // 1011 = Internal Error
                        break;
                    }
                }
            }
        }
        AdHocResultFormat::Parquet => {
            let mut stream = Box::pin(stream_parquet_query(df));
            while let Some(res) = stream.next().await {
                match res {
                    Ok(bytes) => ws.send(Message::Binary(bytes)).await?,
                    Err(err) => {
                        ws.send(Message::Text(
                            serde_json::to_string(&PipelineError::AdHocQueryError {
                                error: err.to_string(),
                                df: Some(Box::new(err)),
                            })
                            .unwrap().into(),
                        )).await?;
                        ws_close(ws, 1011).await; // 1011 = Internal Error
                        break;
                    }
                }
            }
        }
        AdHocResultFormat::Hash => {
            let hash_result = hash_query_result(df).await;
            match hash_result {
                Ok(hash) => {
                    ws.send(Message::Text(hash.into())).await?;
                }
                Err(e) => {
                    ws.send(Message::Text(serde_json::to_string(&e).unwrap_or(e.to_string()).into())).await?;
                    ws_close(ws, 1011).await; // 1011 = Internal Error
                }
            }
        }
    }

    Ok(())
}

pub async fn adhoc_websocket(
    df_session: SessionContext,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |mut socket| async move {
        while let Some(msg) = socket.recv().await {
            match msg {
                Ok(Message::Text(text)) => {
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
                            let df = df_session
                                .sql_with_options(
                                    &args.sql,
                                    SQLOptions::new().with_allow_ddl(false),
                                )
                                .await
                                .map_err(|e| PipelineError::AdHocQueryError {
                                    error: format!("Unable to execute SQL query: {}", e),
                                    df: Some(Box::new(e)),
                                });
                            match df {
                                Ok(df) => {
                                    // If the query is successful, we handle it based on the format.
                                    if adhoc_query_handler(df, &mut socket, args)
                                        .await
                                        .is_err()
                                    {
                                        // Connection was closed, we exit the loop.
                                        return;
                                    } else {
                                        ws_close(&mut socket, 1000).await; // 1000 = Normal Closure
                                        return;
                                    }
                                }
                                Err(e) => {
                                    let _ = socket.send(Message::Text(serde_json::to_string(&e).unwrap_or(e.to_string()).into())).await;
                                    ws_close(&mut socket, 1011).await; // 1011 = Internal Error
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = socket.send(Message::Text(serde_json::to_string(&e).unwrap_or(e.to_string()).into())).await;
                            ws_close(&mut socket, 1011).await; // 1011 = Internal Error
                            return;
                        }
                    }
                }
                Ok(Message::Binary(_)) => {
                    let _ = socket.send(Message::Text(json!({
                        "error": "Binary requests are not supported. Please use text messages."
                    }).to_string().into())).await;
                    ws_close(&mut socket, 1011).await; // 1011 = Internal Error
                    return;
                }
                Ok(Message::Ping(_)) => {
                    // Ignore ping messages
                }
                Ok(Message::Pong(_)) => {
                    // Ignore pong messages
                }
                Ok(Message::Close(_)) => {
                    return;
                }
                Err(e) => {
                    let _ = socket.send(Message::Text(serde_json::to_string(&PipelineError::AdHocQueryError {
                        error: format!("WebSocket error: {}", e),
                        df: None,
                    })
                    .unwrap().into())).await;
                    ws_close(&mut socket, 1011).await; // 1011 = Internal Error
                    return;
                }
            }
        }
    })
}

/// Stream the result of an ad-hoc query using a HTTP streaming response.
pub(crate) async fn stream_adhoc_result(
    args: AdhocQueryArgs,
    session: SessionContext,
) -> Result<Response, PipelineError> {
    let df = session.sql(&args.sql).await?;
    // Note that once we are in the stream!{} macros any error that occurs will lead to the connection
    // in the manager being terminated and a 500 error being returned to the client.
    // We can't return an error in a stream that is already Response::Ok.
    //
    // Sometimes things do tend to fail inside the stream!{} macro, e.g., "select 1/0;" will cause a
    // division by zero error during query execution. So we return errors according to the chosen
    // format for text and json, and for parquet we return the 500 error.
    match args.format {
        AdHocResultFormat::Text => {
            let stream = infallible_from_bytestring(stream_text_query(df), |e| {
                format!("ERROR: {}", e).into()
            });
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime::TEXT_PLAIN.as_ref())
                .body(Body::from_stream(stream))
                .unwrap())
        }
        AdHocResultFormat::Json => {
            let stream = infallible_from_bytestring(
                stream_json_query(df),
                |e| serde_json::to_string(&e).unwrap().into(),
            );
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                .body(Body::from_stream(stream))
                .unwrap())
        }
        AdHocResultFormat::ArrowIpc => {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime::APPLICATION_OCTET_STREAM.as_ref())
                .body(Body::from_stream(stream_arrow_query(df)))
                .unwrap())
        }
        AdHocResultFormat::Parquet => {
            let file_name = format!(
                "results_{}.parquet",
                chrono::Utc::now().format("%Y%m%d_%H%M%S")
            );
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", file_name))
                .header(header::CONTENT_TYPE, mime::APPLICATION_OCTET_STREAM.as_ref())
                .body(Body::from_stream(stream_parquet_query(df)))
                .unwrap())
        }
        AdHocResultFormat::Hash => {
            let hash = hash_query_result(df).await?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime::TEXT_PLAIN.as_ref())
                .body(Body::from(hash))
                .unwrap())
        }
    }
}
