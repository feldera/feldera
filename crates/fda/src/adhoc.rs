//! Code that deals with ad-hoc queries.

use std::convert::Infallible;
use std::io::{Read, Write};

use arrow::ipc::reader::StreamReader;
use arrow::util::pretty::pretty_format_batches;
use feldera_rest_api::Client;
use feldera_types::query::{AdHocResultFormat, AdhocQueryArgs};
use futures_util::SinkExt;
use futures_util::StreamExt;
use log::{debug, error, trace, warn};
use reqwest_websocket::{CloseCode, Message, RequestBuilderExt};

use crate::cli::OutputFormat;
use crate::unique_file;
use crate::UPGRADE_NOTICE;

fn handle_ws_errors_fatal(
    server: String,
    msg: &'static str,
    exit_code: i32,
) -> Box<dyn Fn(reqwest_websocket::Error) -> Infallible + Send> {
    assert_ne!(exit_code, 0, "Exit code must not be 0");
    Box::new(move |err: reqwest_websocket::Error| -> Infallible {
        match err {
            reqwest_websocket::Error::Handshake(e) => {
                eprintln!("{}: {e}", msg);
                error!("Failed to establish a websocket connection to {server}.");
            }
            reqwest_websocket::Error::Reqwest(e) => {
                eprintln!("{}: {e}", msg);
            }
            reqwest_websocket::Error::Tungstenite(e) => {
                eprintln!("{}: {e}", msg);
            }
            err => {
                eprintln!("{}", msg);
                error!(
                    "An unexpected error occurred while handling the websocket connection: {err}"
                );
                error!("{}", UPGRADE_NOTICE);
            }
        };
        std::process::exit(exit_code);
    })
}

async fn handle_websocket_message_generic(
    websocket: &mut reqwest_websocket::WebSocket,
    msg: Result<Message, reqwest_websocket::Error>,
) {
    match msg {
        Ok(Message::Binary(chunk)) => {
            eprintln!("ERROR: Received unexpected message type `binary` as part of query execution: {chunk:?}");
            error!("{}", UPGRADE_NOTICE);
            std::process::exit(1);
        }
        Ok(Message::Text(chunk)) => {
            let err_str = match serde_json::from_str::<serde_json::Value>(&chunk) {
                Ok(value) => value
                    .get("error")
                    .map(|e| e.as_str().unwrap().to_string())
                    .unwrap_or_else(|| value.to_string()),
                Err(e) => {
                    eprintln!("ERROR: Unable to parse server response as JSON: {}", e);
                    debug!("Detailed error: {:?}", e);
                    std::process::exit(1);
                }
            };

            eprintln!("ERROR: {}", err_str);
            std::process::exit(1);
        }
        Ok(Message::Ping(payload)) => {
            if let Err(e) = websocket.send(Message::Pong(payload)).await {
                eprintln!(
                    "ERROR: Connection to the pipeline closed unexpectedly: {}",
                    e
                );
                error!("Client was unable to respond to websocket ping message.");
                std::process::exit(1);
            }
        }
        Ok(Message::Pong(_)) => {
            // Ignore Pong messages
        }
        Ok(Message::Close { code, reason }) => {
            if code == CloseCode::Normal {
                trace!("Websocket normal closure.");
            } else if code == CloseCode::Error {
                if !reason.is_empty() {
                    warn!("Error encountered during query processing: {}.", reason);
                } else {
                    warn!("Error encountered during query processing.");
                }
            } else {
                eprint!("Connection unexpectedly closed by pipeline ({})", code);
                if !reason.is_empty() {
                    eprintln!(": {}.", reason);
                } else {
                    eprintln!(".");
                }
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("ERROR: Unable to read server response: {}", e);
            debug!("Detailed error: {:?}", e);
            std::process::exit(1);
        }
    }
}

pub(crate) async fn handle_adhoc_query(
    client: Client,
    format: OutputFormat,
    name: String,
    sql: Option<String>,
    stdin: bool,
) {
    // Because we're doing a websocket connection, we build the request manually
    // (ws isn't supported by progenitor).
    let reqwest_client = client.client();
    let url = format!("{}/v0/pipelines/{}/query", client.baseurl(), &name);

    let response = reqwest_client
        .get(url)
        .upgrade()
        .send()
        .await
        .map_err(handle_ws_errors_fatal(
            client.baseurl().clone(),
            "Failed to connect to pipeline",
            1,
        ))
        .unwrap();
    let mut websocket = response
        .into_websocket()
        .await
        .map_err(handle_ws_errors_fatal(
            client.baseurl().clone(),
            "Failed to connect to pipeline",
            1,
        ))
        .unwrap();

    let format = match format {
        OutputFormat::Text => AdHocResultFormat::Text,
        OutputFormat::Json => {
            warn!("The JSON format is deprecated for ad-hoc queries, see https://github.com/feldera/feldera/issues/4219 for the tracking issue.");
            AdHocResultFormat::Json
        }
        OutputFormat::ArrowIpc => AdHocResultFormat::ArrowIpc,
        OutputFormat::Parquet => AdHocResultFormat::Parquet,
    };
    let sql = sql.unwrap_or_else(|| {
        if stdin {
            let mut program_code = String::new();
            let mut stdin_stream = std::io::stdin();
            if stdin_stream.read_to_string(&mut program_code).is_ok() {
                debug!("Read SQL from stdin");
                program_code
            } else {
                eprintln!("Failed to read SQL from stdin");
                std::process::exit(1);
            }
        } else {
            eprintln!("`query` command expects a SQL query or a pipe from stdin. For example, `fda query p1 'select * from foo'` or `echo 'select * from foo' | fda query p1`");
            std::process::exit(1);
        }
    });

    let query_args = AdhocQueryArgs { sql, format };
    websocket
        .send(Message::Text(serde_json::to_string(&query_args).unwrap()))
        .await
        .map_err(handle_ws_errors_fatal(
            client.baseurl().clone(),
            "Failed to send SQL query arguments to pipeline",
            1,
        ))
        .unwrap();

    match format {
        AdHocResultFormat::Text | AdHocResultFormat::Json => {
            while let Some(chunk) = websocket.next().await {
                let mut text: String = String::new();
                if let Ok(Message::Text(chunk)) = chunk {
                    text.push_str(chunk.as_str());
                    print!("{}", chunk);
                } else {
                    handle_websocket_message_generic(&mut websocket, chunk).await;
                    break;
                }
            }
            println!()
        }
        AdHocResultFormat::ArrowIpc => {
            let mut ipc_bytes: Vec<u8> = Vec::new();
            while let Some(chunk) = websocket.next().await {
                if let Ok(Message::Binary(chunk)) = chunk {
                    ipc_bytes.write_all(chunk.as_ref()).unwrap();
                } else {
                    handle_websocket_message_generic(&mut websocket, chunk).await;
                    break;
                }
            }
            let reader = StreamReader::try_new(ipc_bytes.as_slice(), None).unwrap();
            let results = reader.collect::<Result<Vec<_>, _>>();
            println!("{}", pretty_format_batches(&results.unwrap()).unwrap());
        }
        AdHocResultFormat::Parquet => {
            let (path, mut result_file) =
                unique_file("result", "parquet").expect("Failed to create parquet file");
            while let Some(chunk) = websocket.next().await {
                if let Ok(Message::Binary(chunk)) = chunk {
                    result_file.write_all(chunk.as_ref()).unwrap();
                } else {
                    handle_websocket_message_generic(&mut websocket, chunk).await;
                    break;
                }
            }
            result_file.flush().unwrap();
            println!("Query result saved to '{}'", path.display());
        }
    }
}
