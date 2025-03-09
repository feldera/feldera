use crate::api::error::ApiError;
use crate::api::util::parse_url_parameter;
use crate::config::CommonConfig;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db_notifier::{DbNotification, Operation};
use crate::error::ManagerError;
use crate::probe::Probe;
use crate::runner::error::RunnerError;
use crate::runner::pipeline_automata::PipelineAutomaton;
use crate::runner::pipeline_executor::{LogMessage, PipelineExecutor};
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::{get, web, HttpRequest, HttpServer};
use async_stream::try_stream;
use log::{debug, error, trace};
use reqwest::{Method, StatusCode};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{Mutex, Notify};
use tokio::time::sleep;
use tokio_stream::Stream;
use uuid::Uuid;

/// Maximum number of outstanding log follow requests that have
/// not yet been received by the logging thread of the runner.
pub const MAXIMUM_OUTSTANDING_LOG_FOLLOW_REQUESTS: usize = 100;

/// A follower generates a channel for the runner to send the log line messages over.
/// This is the maximum buffer size of that channel. If the sender of the runner
/// is returned an error upon `try_send` that the buffer is full, the follower will
/// be dropped in order to not slow down others. It should be set to at least the
/// circular buffer size such that catch up will not cause the limit to be hit.
pub const MAXIMUM_BUFFERED_LINES_PER_FOLLOWER: usize = 100_000;

/// Timeout of each request to check if the main HTTP server is ready.
pub const READY_CHECK_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_millis(2_000);

/// Poll period between each check whether the main HTTP server is ready.
pub const READY_CHECK_POLL_PERIOD: Duration = Duration::from_millis(2_000);

/// Number of request tries before giving up.
pub const READY_CHECK_HTTP_RETRIES: u64 = 8;

/// Type alias shorthand for the pipelines state the runner manager maintains and interacts with.
type PipelinesState = BTreeMap<PipelineId, (Arc<Notify>, Sender<Sender<LogMessage>>)>;

/// Returns whether the runner is healthy.
/// The health check consults the continuous probe of database reachability.
#[get("/healthz")]
async fn get_healthz(data: web::Data<Arc<Mutex<Probe>>>) -> Result<impl Responder, ManagerError> {
    data.lock().await.status_as_http_response()
}

/// Produces a stream of logs which it fetches from the receiver.
/// If the channel is closed, the receiver will return and the stream will end.
async fn logs_stream(
    mut receiver: Receiver<LogMessage>,
) -> impl Stream<Item = Result<web::Bytes, actix_web::Error>> {
    try_stream! {
        let mut has_ending = false;
        loop {
            match receiver.recv().await {
                None => {
                    // The corresponding sender was dropped or the channel was closed,
                    // as such there will be no more log lines being sent over
                    break;
                }
                Some(message) => {
                    match message {
                        LogMessage::Line(line) => {
                            yield actix_web::web::Bytes::from(format!("{line}\n"));
                        }
                        LogMessage::End(ending_line) => {
                            has_ending = true;
                            yield actix_web::web::Bytes::from(format!("{ending_line}\n"));
                            // If an ending log message is received, the loop is ended
                            // and the receiver will be dropped at the end of the function.
                            // This will cause the channel to end as it is the only receiver.
                            break;
                        }
                    }
                }
            }
        }
        // Unexpected ending of logs stream generally indicates a panic occurred in the logging thread
        if !has_ending {
            yield actix_web::web::Bytes::from("LOG STREAM ENDED UNEXPECTEDLY\n")
        }
    }
}

/// Retrieves as a stream the logs of a particular pipeline identified by its identifier.
#[get("/logs/{pipeline_id}")]
async fn get_logs(
    data: web::Data<Arc<Mutex<PipelinesState>>>,
    req: HttpRequest,
) -> Result<impl Responder, ManagerError> {
    // Parse pipeline identifier
    let pipeline_id = parse_url_parameter(&req, "pipeline_id")?;
    let pipeline_id = PipelineId(Uuid::from_str(&pipeline_id).map_err(|e| {
        ManagerError::from(ApiError::InvalidUuidParam {
            value: pipeline_id.clone(),
            error: e.to_string(),
        })
    })?);

    // Attempt to follow the logs and return them in a streaming response
    match data.lock().await.get(&pipeline_id) {
        None => Ok(HttpResponse::NotFound().finish()),
        Some((_, follow_request_sender)) => {
            let (sender, receiver) = channel::<LogMessage>(MAXIMUM_BUFFERED_LINES_PER_FOLLOWER);
            match follow_request_sender.try_send(sender) {
                Ok(()) => {
                    // Streaming response with explicit content type of text/plain with UTF-8,
                    // and requesting the browser to abide by it. The reason is to avoid
                    // browsers (in particular, Chrome) not yet displaying the content because
                    // they want more data to infer the content type (even though it was provided).
                    Ok(HttpResponse::Ok()
                        .content_type("text/plain; charset=utf-8")
                        .append_header(("X-Content-Type-Options", "nosniff"))
                        .streaming(logs_stream(receiver).await))
                }
                Err(e) => match e {
                    TrySendError::Full(_) => {
                        error!(
                                "Unable to send the log follow sender to the runner receiver because the channel is full"
                            );
                        Err(ManagerError::from(
                            RunnerError::RunnerInteractionLogFollowRequestChannelFull,
                        ))
                    }
                    TrySendError::Closed(_) => {
                        error!(
                                "Unable to send the log follow sender to the runner receiver because the channel is closed"
                            );
                        Err(ManagerError::from(
                            RunnerError::RunnerInteractionLogFollowRequestChannelClosed,
                        ))
                    }
                },
            }
        }
    }
}

/// Waits for the HTTP server to become ready by polling it.
/// Returns false if after a predefined number of tries with timeouts
/// it was unable to receive an OK response from the health endpoint.
async fn wait_for_http_server_ready(client: &reqwest::Client, port: u16) -> bool {
    let url = format!("http://127.0.0.1:{port}/healthz");
    let mut tries = READY_CHECK_HTTP_RETRIES;
    while tries > 0 {
        if let Ok(response) = client
            .request(Method::GET, &url)
            .timeout(READY_CHECK_HTTP_REQUEST_TIMEOUT)
            .send()
            .await
        {
            if response.status() == StatusCode::OK {
                return true;
            }
        }
        sleep(READY_CHECK_POLL_PERIOD).await;
        tries -= 1;
    }
    false
}

/// Main to start the runner, which consists of starting a web server and
/// a reconciliation loop which matches pipelines with runner automatons.
pub async fn runner_main<E: PipelineExecutor + 'static>(
    // Database handle
    db: Arc<Mutex<StoragePostgres>>,
    // Common configuration
    common_config: CommonConfig,
    // Pipeline executor configuration
    config: E::Config,
    // Main HTTP server port.
    main_http_server_port: u16,
) -> Result<(), ManagerError> {
    // Mapping of the present pipelines to how to reach them:
    // - A notification mechanism for the automata to act quickly on change
    // - A sender channel to request getting logs from the pipeline runner
    let pipelines: Arc<Mutex<PipelinesState>> = Arc::new(Mutex::new(BTreeMap::new()));

    // Setup web server
    let data_healthz = web::Data::new(Probe::new(db.clone()).await);
    let data_logs = web::Data::new(pipelines.clone());
    let server = HttpServer::new(move || {
        actix_web::App::new()
            .app_data(data_healthz.clone())
            .app_data(data_logs.clone())
            .service(get_healthz)
            .service(get_logs)
    });
    spawn(
        server
            .bind(("0.0.0.0", main_http_server_port))
            .expect("Unable to bind runner main HTTP server to port {main_http_server_port}")
            .run(),
    );

    // Reused HTTP client
    let client = reqwest::Client::new();

    if !wait_for_http_server_ready(&client, main_http_server_port).await {
        panic!("Unable to reach runner main HTTP server on port {main_http_server_port}");
    }
    debug!(
        "Runner main HTTP server ready on port {}",
        main_http_server_port
    );

    // Launch the reconciliation loop
    reconcile::<E>(db, client, pipelines, common_config, config).await
}

/// Continuous reconciliation loop between what is stored about the pipelines in the database and
/// the runner managing their deployment.
/// It continuously waits and acts on pipeline database notifications (create, update, delete).
async fn reconcile<E: PipelineExecutor + 'static>(
    db: Arc<Mutex<StoragePostgres>>,
    client: reqwest::Client,
    pipelines: Arc<Mutex<PipelinesState>>,
    common_config: CommonConfig,
    config: E::Config,
) -> Result<(), ManagerError> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    spawn(crate::db_notifier::listen(db.clone(), tx));
    debug!("Runner has started");
    loop {
        trace!("Waiting for pipeline operation notification from database...");
        if let Some(DbNotification::Pipeline(op, tenant_id, pipeline_id)) = rx.recv().await {
            debug!("Received pipeline operation notification: operation={op:?} tenant_id={tenant_id} pipeline_id={pipeline_id}");
            match op {
                Operation::Add | Operation::Update => {
                    pipelines
                        .lock()
                        .await
                        .entry(pipeline_id)
                        .or_insert_with(|| {
                            let notifier = Arc::new(Notify::new());
                            let (follow_request_sender, follow_request_receiver) =
                                channel::<Sender<LogMessage>>(
                                    MAXIMUM_OUTSTANDING_LOG_FOLLOW_REQUESTS,
                                );
                            let pipeline_handle = E::new(
                                pipeline_id,
                                config.clone(),
                                client.clone(),
                                follow_request_receiver,
                            );
                            spawn(
                                PipelineAutomaton::new(
                                    &common_config.platform_version,
                                    pipeline_id,
                                    tenant_id,
                                    db.clone(),
                                    notifier.clone(),
                                    client.clone(),
                                    pipeline_handle,
                                    E::DEFAULT_PROVISIONING_TIMEOUT,
                                )
                                .run(),
                            );
                            (notifier, follow_request_sender)
                        })
                        .0
                        .notify_one();
                }
                Operation::Delete => {
                    if let Some((notifier, _follow_request_sender)) =
                        pipelines.lock().await.remove(&pipeline_id)
                    {
                        // Notify the automaton so it shuts down
                        notifier.notify_one();
                        // The _follow_request_sender will be dropped here automatically
                    }
                }
            };
        }
    }
}
