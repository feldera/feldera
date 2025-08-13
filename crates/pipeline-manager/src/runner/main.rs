use crate::api::error::ApiError;
use crate::api::util::parse_url_parameter;
use crate::config::CommonConfig;
use crate::db::listen_table::{Operation, PIPELINE_NOTIFY_CHANNEL_CAPACITY};
use crate::db::probe::DbProbe;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use crate::runner::pipeline_automata::PipelineAutomaton;
use crate::runner::pipeline_executor::PipelineExecutor;
use crate::runner::pipeline_logs::{LogMessage, LogsSender};
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::{get, web, HttpRequest, HttpServer};
use async_stream::try_stream;
use log::{error, info};
use std::collections::BTreeMap;
use std::net::TcpListener;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout;
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
const MAXIMUM_BUFFERED_LINES_PER_FOLLOWER: usize = 100_000;

/// Interval at which to discover new pipelines in order to start their runners, or to join pipeline
/// runners that have finished.
const PIPELINE_DISCOVERY_INTERVAL: Duration = Duration::from_secs(2);

/// Type alias shorthand for the pipelines state the runner manager maintains and interacts with.
type PipelinesState = BTreeMap<PipelineId, (JoinHandle<()>, Arc<Notify>, Sender<Sender<String>>)>;

/// Returns whether the runner is healthy.
/// The health check consults the continuous probe of database reachability.
#[get("/healthz")]
async fn get_healthz(data: web::Data<Arc<Mutex<DbProbe>>>) -> Result<impl Responder, ManagerError> {
    Ok(data.lock().await.as_http_response())
}

/// Produces a continuous stream of logs which are received from the pipeline runner.
async fn logs_stream(
    mut receiver: Receiver<String>,
) -> impl Stream<Item = Result<web::Bytes, actix_web::Error>> {
    try_stream! {
        loop {
            match receiver.recv().await {
                None => {
                    // The corresponding sender was dropped or the channel was closed.
                    // This can occur when the pipeline is deleted or the runner restarts.
                    break;
                }
                Some(line) => {
                    yield actix_web::web::Bytes::from(format!("{line}\n"));
                }
            }
        }
        yield actix_web::web::Bytes::from("Logs have ended\n")
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
        Some((_, _, follow_request_sender)) => {
            let (sender, receiver) = channel::<String>(MAXIMUM_BUFFERED_LINES_PER_FOLLOWER);
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
                Err(e) => {
                    match e {
                        TrySendError::Full(_) => {
                            error!("Unable to follow pipeline logs because the request channel is full");
                            Err(ManagerError::from(
                                RunnerError::RunnerInteractionLogFollowRequestChannelFull,
                            ))
                        }
                        TrySendError::Closed(_) => {
                            error!("Unable to follow pipeline logs because the request channel is closed");
                            Err(ManagerError::from(
                                RunnerError::RunnerInteractionLogFollowRequestChannelClosed,
                            ))
                        }
                    }
                }
            }
        }
    }
}

/// Main to start the runner, which consists of starting an HTTP(S) server and
/// a reconciliation loop which matches pipelines with runner automatons.
pub async fn runner_main<E: PipelineExecutor + 'static>(
    // Database handle
    db: Arc<Mutex<StoragePostgres>>,
    // Common configuration
    common_config: CommonConfig,
    // Pipeline executor configuration
    config: E::Config,
) {
    // Mapping of the present pipelines to how to reach them:
    // - A notification mechanism for the automata to act quickly on change
    // - A sender channel to request getting logs from the pipeline runner
    let pipelines: Arc<Mutex<PipelinesState>> = Arc::new(Mutex::new(BTreeMap::new()));

    // Setup HTTP(S) server
    let data_healthz = web::Data::new(DbProbe::new(db.clone()).await);
    let data_logs = web::Data::new(pipelines.clone());
    let server = HttpServer::new(move || {
        actix_web::App::new()
            .app_data(data_healthz.clone())
            .app_data(data_logs.clone())
            .service(get_healthz)
            .service(get_logs)
    })
    .workers(common_config.http_workers)
    .worker_max_blocking_threads(std::cmp::max(512 / common_config.http_workers, 1));
    let listener = TcpListener::bind((
        common_config.bind_address.clone(),
        common_config.runner_port,
    ))
    .unwrap_or_else(|_| {
        panic!(
            "runner unable to bind listener to {}:{} -- is the port occupied?",
            common_config.bind_address, common_config.runner_port
        )
    });
    spawn(
        if let Some(server_config) = common_config.https_server_config() {
            server
                .listen_rustls_0_23(listener, server_config)
                .expect("runner HTTPS server unable to listen")
                .run()
        } else {
            server
                .listen(listener)
                .expect("runner HTTP server unable to listen")
                .run()
        },
    );
    info!(
        "Runner {} server: ready on port {} ({} workers)",
        if common_config.enable_https {
            "HTTPS"
        } else {
            "HTTP"
        },
        common_config.runner_port,
        common_config.http_workers,
    );

    // Reused HTTP(S) client
    let client = common_config.reqwest_client().await;

    // Launch the reconciliation loop
    reconcile::<E>(db, client, pipelines, common_config, config).await;
}

/// For each of the rows in the pipeline table, the runner spawns an automaton, also referred to as
/// the "pipeline runner".
///
/// - Periodically, the list of all pipelines is retrieved (identifiers only): for pipelines that do
///   not have a runner, one is spawned for them. For each pipeline runner, its own notifier is
///   generated. The periodic check can be preempted by the main notifier.
/// - Periodically, each pipeline runner fetches its table row and takes action. This can be
///   preempted by its notifier.
/// - If a pipeline runner fetches its table row, but it is not found, it will terminate itself.
///   This termination is detected by the join handle.
/// - In parallel we LISTEN to the pipeline table: if a pipeline is added, updated or deleted, the
///   notifiers are used to have the main and each pipeline runner respond more quickly. This is
///   purely supplemental, as both already have a periodic check anyway.
async fn reconcile<E: PipelineExecutor + 'static>(
    db: Arc<Mutex<StoragePostgres>>,
    client: reqwest::Client,
    pipelines: Arc<Mutex<PipelinesState>>,
    common_config: CommonConfig,
    config: E::Config,
) {
    // Listen to the pipeline table in order to be able to send quick notification to the pipeline
    // runner that something changed in the pipeline, and it might need to take action
    let db_cloned = db.clone();
    let pipelines_cloned = pipelines.clone();
    let main_notifier = Arc::new(Notify::new());
    let main_notifier_cloned = main_notifier.clone();
    spawn(async move {
        // Spawn a separate thread that listens to the table and sends out related notifications
        let (listen_sender, mut listen_receiver) = channel(PIPELINE_NOTIFY_CHANNEL_CAPACITY);
        spawn(crate::db::listen_table::listen_table(
            db_cloned,
            listen_sender,
        ));

        // Receive any table listen notifications and notify the main and its corresponding pipeline
        // runner
        loop {
            match listen_receiver.recv().await {
                Some(notification) => {
                    // Notify the pipeline runner if it already exists
                    if let Some((_, notifier, _)) =
                        pipelines_cloned.lock().await.get(&notification.pipeline_id)
                    {
                        notifier.notify_one();
                    }

                    // If it is an addition, notify the main loop that spawns runner so it can more
                    // quickly spawn the runner
                    if notification.operation == Operation::Add {
                        main_notifier_cloned.notify_one();
                    }
                }
                None => {
                    error!("Runner main: listen notifier sending side has disconnected -- no longer able to send notifications");
                    break;
                }
            }
        }
    });

    // Periodically check for new pipelines and start runners for them, followed by checking whether
    // any existing runners have terminated
    let mut db_error_previously = false;
    loop {
        // Discover new pipelines and delete finished ones at an interval, or it can also be
        // preempted via the listening mechanism
        let _ = timeout(PIPELINE_DISCOVERY_INTERVAL, main_notifier.notified()).await;

        // Retrieve the full list of pipeline identifiers, and start a pipeline runner for each one
        // which is not yet in the state
        match db.lock().await.list_pipeline_ids_across_all_tenants().await {
            Ok(pipeline_ids) => {
                if db_error_previously {
                    info!("Runner main: again able to retrieve pipeline identifiers from the database. Any new pipelines will be retroactively detected.");
                    db_error_previously = false;
                }
                for (tenant_id, pipeline_id) in pipeline_ids {
                    pipelines
                        .lock()
                        .await
                        .entry(pipeline_id)
                        .or_insert_with(|| {
                            let notifier = Arc::new(Notify::new());
                            let (follow_request_sender, follow_request_receiver) =
                                channel::<Sender<String>>(MAXIMUM_OUTSTANDING_LOG_FOLLOW_REQUESTS);
                            let (logs_sender, logs_receiver) =
                                channel::<LogMessage>(MAXIMUM_BUFFERED_LINES_PER_FOLLOWER);
                            let logs_sender = LogsSender::new(logs_sender);
                            let pipeline_handle = E::new(
                                pipeline_id,
                                common_config.clone(),
                                config.clone(),
                                client.clone(),
                                logs_sender.clone(),
                            );
                            let pipeline_runner_handle = spawn(
                                PipelineAutomaton::new(
                                    common_config.clone(),
                                    pipeline_id,
                                    tenant_id,
                                    db.clone(),
                                    notifier.clone(),
                                    client.clone(),
                                    pipeline_handle,
                                    E::DEFAULT_PROVISIONING_TIMEOUT,
                                    follow_request_receiver,
                                    logs_sender,
                                    logs_receiver,
                                )
                                .run(),
                            );
                            (pipeline_runner_handle, notifier, follow_request_sender)
                        });
                }
            }
            Err(e) => {
                error!("Runner main: unable to retrieve pipeline identifiers from the database. Any new pipelines are not detected until again able to. Error: {e}");
                db_error_previously = true;
            }
        }

        // Find the pipeline runners that have finished by checking whether the join handle
        // indicates it has finished
        let mut finished = vec![];
        for (pipeline_id, (join_handle, _, _)) in pipelines.lock().await.iter() {
            if join_handle.is_finished() {
                finished.push(*pipeline_id);
            }
        }
        for pipeline_id in finished {
            if let Some((join_handle, _, _)) = pipelines.lock().await.remove(&pipeline_id) {
                if let Err(e) = join_handle.await {
                    error!("Pipeline {pipeline_id} experienced a join error: {e}")
                }
            } else {
                // Should be unreachable as this loop is the only one removing entries
                error!("Pipeline {pipeline_id} was marked as finished, and as such to be joined and removed. It has however already been removed.");
            }
        }
    }
}
