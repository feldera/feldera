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
use crate::runner::pipeline_logs::{
    FollowMode, FollowRequest, FollowerMessage, LOGS_EPOCH_HEADER, LOGS_GAP_HEADER,
    LOGS_SEQ_HEADER, LogCursor, LogMessage, LogsSender,
};
use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::{HttpRequest, HttpServer, get, web};
use async_stream::try_stream;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::TcpListener;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::Stream;
use tracing::{error, info};
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
type PipelinesState = BTreeMap<PipelineId, (JoinHandle<()>, Arc<Notify>, Sender<FollowRequest>)>;

/// Query parameters accepted by the logs endpoint.
#[derive(Debug, Deserialize)]
struct LogsQuery {
    /// Position to resume the stream from, as reported by a previous response's headers.
    ///
    /// Absent selects the legacy behavior: the whole retained buffer, with the discard
    /// notice in-band and no position headers. Present but empty is a cursor-aware
    /// follower's first connection, which has no position yet.
    cursor: Option<String>,
}

/// Returns whether the runner is healthy.
/// The health check consults the continuous probe of database reachability.
#[get("/healthz")]
async fn get_healthz(data: web::Data<Arc<Mutex<DbProbe>>>) -> Result<impl Responder, ManagerError> {
    Ok(data.lock().await.as_http_response())
}

/// Produces a continuous stream of logs which are received from the pipeline runner.
///
/// `emit_end_notice` appends a closing `Logs have ended` line. It is suppressed for
/// cursor-aware followers, whose body must contain one line per sequence number and
/// nothing else for them to derive their next cursor by counting. Such a follower learns
/// the same fact from the body ending without a transport error.
async fn logs_stream(
    mut receiver: Receiver<FollowerMessage>,
    emit_end_notice: bool,
) -> impl Stream<Item = Result<web::Bytes, actix_web::Error>> {
    try_stream! {
        loop {
            match receiver.recv().await {
                None => {
                    // The corresponding sender was dropped or the channel was closed.
                    // This can occur when the pipeline is deleted or the runner restarts.
                    break;
                }
                Some(FollowerMessage::Line(line)) => {
                    yield actix_web::web::Bytes::from(format!("{line}\n"));
                }
                Some(FollowerMessage::Resume { .. }) => {
                    // Consumed by `get_logs` before the body starts, and sent at most once
                    // per follower, so the stream never encounters one.
                }
            }
        }
        if emit_end_notice {
            yield actix_web::web::Bytes::from("Logs have ended\n")
        }
    }
}

/// Retrieves as a stream the logs of a particular pipeline identified by its identifier.
#[get("/logs/{pipeline_id}")]
async fn get_logs(
    data: web::Data<Arc<Mutex<PipelinesState>>>,
    query: web::Query<LogsQuery>,
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

    // Parse what the follower asks to receive. Malformed syntax is rejected rather than
    // ignored: it can only come from a broken client, whereas a cursor that is merely
    // stale is resolved by the logs thread and degrades to a full catch-up.
    let mode = match &query.cursor {
        None => FollowMode::Full,
        Some(cursor) if cursor.is_empty() => FollowMode::Resume(None),
        Some(cursor) => FollowMode::Resume(Some(LogCursor::from_str(cursor).map_err(|e| {
            ManagerError::from(ApiError::InvalidLogCursorParam {
                value: cursor.clone(),
                error: e,
            })
        })?)),
    };
    let emit_end_notice = matches!(mode, FollowMode::Full);
    // A resuming follower is answered with its position, which the logs thread sends ahead
    // of every line and which has to be in hand before the response head goes out.
    let resolves_position = !emit_end_notice;

    // Take a handle to the logs thread and release the pipelines lock before waiting on it.
    // The reconciliation loop takes the same lock, so holding it across the wait below would
    // stall pipeline discovery for as long as the logs thread takes to answer.
    let follow_request_sender = match data.lock().await.get(&pipeline_id) {
        None => return Ok(HttpResponse::NotFound().finish()),
        Some((_, _, follow_request_sender)) => follow_request_sender.clone(),
    };

    // Attempt to follow the logs and return them in a streaming response
    let (sender, mut receiver) = channel::<FollowerMessage>(MAXIMUM_BUFFERED_LINES_PER_FOLLOWER);
    match follow_request_sender.try_send(FollowRequest { sender, mode }) {
        Ok(()) => {
            // Streaming response with explicit content type of text/plain with UTF-8,
            // and requesting the browser to abide by it. The reason is to avoid
            // browsers (in particular, Chrome) not yet displaying the content because
            // they want more data to infer the content type (even though it was provided).
            let mut builder = HttpResponse::Ok();
            builder
                .content_type("text/plain; charset=utf-8")
                .append_header(("X-Content-Type-Options", "nosniff"));
            if resolves_position {
                // Resolves as soon as the logs thread services the request, which is
                // the same instant the first line would have been produced. A thread
                // that drops the follower closes the channel instead, leaving the
                // headers off and the body empty, as such a follower has always seen.
                if let Some(FollowerMessage::Resume { epoch, seq, gap }) = receiver.recv().await {
                    builder
                        .append_header((LOGS_EPOCH_HEADER, epoch.to_string()))
                        .append_header((LOGS_SEQ_HEADER, seq.to_string()))
                        .append_header((LOGS_GAP_HEADER, gap.to_string()));
                }
            }
            Ok(builder.streaming(logs_stream(receiver, emit_end_notice).await))
        }
        Err(e) => match e {
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
        },
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
                    error!(
                        "Runner main: listen notifier sending side has disconnected -- no longer able to send notifications"
                    );
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
                    info!(
                        "Runner main: again able to retrieve pipeline identifiers from the database. Any new pipelines will be retroactively detected."
                    );
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
                                channel::<FollowRequest>(MAXIMUM_OUTSTANDING_LOG_FOLLOW_REQUESTS);
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
                                    None,
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
                error!(
                    "Runner main: unable to retrieve pipeline identifiers from the database. Any new pipelines are not detected until again able to. Error: {e}"
                );
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
                    error!(
                        pipeline_id = %pipeline_id,
                        pipeline = "N/A",
                        "Pipeline experienced a join error: {e}"
                    )
                }
            } else {
                // Should be unreachable as this loop is the only one removing entries
                error!(
                    pipeline_id = %pipeline_id,
                    pipeline = "N/A",
                    "Pipeline was marked as finished, and as such to be joined and removed. It has however already been removed."
                );
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{
        FollowMode, FollowRequest, FollowerMessage, LOGS_EPOCH_HEADER, LOGS_GAP_HEADER,
        LOGS_SEQ_HEADER, LogMessage, PipelineId, PipelinesState, get_logs, logs_stream,
    };
    use crate::runner::pipeline_logs::start_thread_pipeline_logs;
    use actix_web::http::StatusCode;
    use actix_web::{App, test as actix_test, web};
    use futures_util::StreamExt;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::spawn;
    use tokio::sync::mpsc::{Receiver, Sender, channel};
    use tokio::sync::{Mutex, Notify, oneshot};
    use uuid::Uuid;

    /// Drains a logs stream fed by `lines`, returning the body it produced.
    async fn body_of(lines: &[&str], emit_end_notice: bool) -> String {
        let (sender, receiver) = channel::<FollowerMessage>(10);
        for line in lines {
            sender
                .send(FollowerMessage::Line(line.to_string()))
                .await
                .expect("send failed");
        }
        drop(sender);

        let mut stream = Box::pin(logs_stream(receiver, emit_end_notice).await);
        let mut body = String::new();
        while let Some(chunk) = stream.next().await {
            body.push_str(std::str::from_utf8(&chunk.expect("stream failed")).expect("not UTF-8"));
        }
        body
    }

    /// Followers that do not use a cursor keep the closing notice they have always seen.
    #[tokio::test]
    async fn end_notice_is_kept_without_a_cursor() {
        assert_eq!(
            body_of(&["one", "two"], true).await,
            "one\ntwo\nLogs have ended\n"
        );
    }

    /// A cursor-aware follower derives its next position by counting the lines it receives,
    /// so the body must hold one line per sequence number and nothing else. It learns the
    /// stream ended from the body ending without a transport error.
    #[tokio::test]
    async fn end_notice_is_suppressed_for_a_cursor() {
        assert_eq!(body_of(&["one", "two"], false).await, "one\ntwo\n");
    }

    /// A logs thread wired into the state the endpoint reads, preloaded with lines.
    struct LogsFixture {
        pipeline_id: PipelineId,
        pipelines: Arc<Mutex<PipelinesState>>,
        epoch: Uuid,
        terminate: oneshot::Sender<()>,
        logs_sender: Sender<LogMessage>,
        follow_sender: Sender<FollowRequest>,
    }

    impl LogsFixture {
        /// Waits until the logs thread has serviced every follow request submitted so far.
        /// Requests arrive over one channel in order, so a later one being serviced proves
        /// the earlier ones were. Without this the terminate below can win the thread's
        /// `select!` and the endpoint's follower is dropped before it is caught up.
        async fn settle(&self) {
            let (sender, mut probe) = channel::<FollowerMessage>(10);
            self.follow_sender
                .try_send(FollowRequest {
                    sender,
                    mode: FollowMode::Resume(None),
                })
                .unwrap_or_else(|_| panic!("unable to submit follow request"));
            recv(&mut probe).await;
        }

        /// Ends the stream so a response body can be read to completion.
        fn close(self) {
            drop(self.logs_sender);
            let _ = self.terminate.send(());
        }
    }

    async fn recv(receiver: &mut Receiver<FollowerMessage>) -> FollowerMessage {
        tokio::time::timeout(std::time::Duration::from_secs(10), receiver.recv())
            .await
            .expect("timed out waiting for a follower message")
            .expect("follower channel was closed")
    }

    async fn logs_fixture(lines: &[&str]) -> LogsFixture {
        let pipeline_id = PipelineId(Uuid::now_v7());
        let (follow_sender, follow_receiver) = channel::<FollowRequest>(10);
        let (logs_sender, logs_receiver) = channel::<LogMessage>(1000);
        let (terminate, _join_handle) = start_thread_pipeline_logs(
            pipeline_id.to_string(),
            "test-pipeline",
            follow_receiver,
            logs_receiver,
        );

        // Attach a probe follower before sending anything: draining it is what proves the
        // thread has appended the lines, and its position is where the epoch comes from.
        let (probe_sender, mut probe) = channel::<FollowerMessage>(1000);
        follow_sender
            .try_send(FollowRequest {
                sender: probe_sender,
                mode: FollowMode::Resume(None),
            })
            .unwrap_or_else(|_| panic!("unable to submit follow request"));
        let epoch = match recv(&mut probe).await {
            FollowerMessage::Resume { epoch, .. } => epoch,
            FollowerMessage::Line(line) => panic!("expected a position, received: {line}"),
        };
        recv(&mut probe).await; // The thread's own opening line, which holds sequence 1.

        for line in lines {
            logs_sender
                .send(LogMessage::new_from_pipeline(line))
                .await
                .unwrap_or_else(|_| panic!("unable to send log line"));
            recv(&mut probe).await;
        }

        let mut pipelines = BTreeMap::new();
        pipelines.insert(
            pipeline_id,
            (
                spawn(async {}),
                Arc::new(Notify::new()),
                follow_sender.clone(),
            ),
        );
        LogsFixture {
            pipeline_id,
            pipelines: Arc::new(Mutex::new(pipelines)),
            epoch,
            terminate,
            logs_sender,
            follow_sender,
        }
    }

    /// Reads the endpoint's response, ending the stream so the body can complete.
    ///
    /// The position it reports is all three headers or none. A caller that received only
    /// some of them could not form a cursor, so a partial set is a failure rather than a
    /// response to be interpreted.
    async fn response_at(
        fixture: LogsFixture,
        uri: String,
    ) -> (StatusCode, Option<(Uuid, u64, u64)>, String) {
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(fixture.pipelines.clone()))
                .service(get_logs),
        )
        .await;
        let response =
            actix_test::call_service(&app, actix_test::TestRequest::get().uri(&uri).to_request())
                .await;
        let status = response.status();
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .map(|value| value.to_str().expect("header is not text").to_string())
        };
        let position = match (
            header(LOGS_EPOCH_HEADER),
            header(LOGS_SEQ_HEADER),
            header(LOGS_GAP_HEADER),
        ) {
            (Some(epoch), Some(seq), Some(gap)) => Some((
                Uuid::parse_str(&epoch).expect("epoch header is not a UUID"),
                seq.parse().expect("sequence header is not a number"),
                gap.parse().expect("gap header is not a number"),
            )),
            (None, None, None) => None,
            partial => panic!("response carries only part of a position: {partial:?}"),
        };
        fixture.settle().await;
        fixture.close();
        let body = actix_test::read_body(response).await;
        (
            status,
            position,
            std::str::from_utf8(&body)
                .expect("body is not UTF-8")
                .to_string(),
        )
    }

    /// The cursor survives the trip through the query string. The position is reported in
    /// the headers and the body holds only the lines the caller is missing, with no line
    /// the caller has to tell apart from a log line.
    #[tokio::test]
    async fn get_logs_resumes_from_a_cursor() {
        let fixture = logs_fixture(&["one", "two", "three"]).await;
        let epoch = fixture.epoch;
        // Sequence 1 is the thread's opening line, so "one" holds 2 and a cursor at 2 is
        // owed "two" and "three".
        let uri = format!("/logs/{}?cursor={}:2", fixture.pipeline_id, epoch);
        let (status, position, body) = response_at(fixture, uri).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(position, Some((epoch, 2, 0)));
        assert_eq!(body, "two\nthree\n");
    }

    /// A caller already level with the stream is told where it stands and given an empty
    /// body. The position rides in the headers, so it does not wait on a log line that may
    /// never arrive.
    #[tokio::test]
    async fn get_logs_reports_the_position_with_nothing_to_send() {
        let fixture = logs_fixture(&["one"]).await;
        let epoch = fixture.epoch;
        let uri = format!("/logs/{}?cursor={}:2", fixture.pipeline_id, epoch);
        let (status, position, body) = response_at(fixture, uri).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(position, Some((epoch, 2, 0)));
        assert_eq!(body, "");
    }

    /// The position reaches a caller over a real connection ahead of any log line.
    ///
    /// A caller level with the stream has no line coming, so the response head has to
    /// travel on its own. `init_service` never touches a socket and so cannot show that:
    /// were the head held back until the first body byte, this request would never return.
    #[actix_web::test]
    async fn position_headers_arrive_before_any_log_line() {
        crate::ensure_default_crypto_provider(); // awc's connector wants one installed.
        let fixture = logs_fixture(&["one"]).await;
        let pipelines = fixture.pipelines.clone();
        let server = ::actix_test::start(move || {
            App::new()
                .app_data(web::Data::new(pipelines.clone()))
                .service(get_logs)
        });

        // Sequence 1 is the opening line and "one" holds 2, so this caller is level with
        // the stream and no line follows the head.
        let response = awc::Client::new()
            .get(server.url(&format!(
                "/logs/{}?cursor={}:2",
                fixture.pipeline_id, fixture.epoch
            )))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .expect("no response head arrived");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(LOGS_SEQ_HEADER)
                .expect("no position"),
            "2"
        );
        fixture.close();
    }

    /// Omitting the cursor leaves the stream exactly as callers have always seen it: no
    /// position headers, the whole buffer, and the closing notice.
    #[tokio::test]
    async fn get_logs_without_a_cursor_is_unchanged() {
        let fixture = logs_fixture(&["one", "two"]).await;
        let uri = format!("/logs/{}", fixture.pipeline_id);
        let (status, position, body) = response_at(fixture, uri).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(position, None);
        let lines: Vec<&str> = body.lines().collect();
        assert!(
            lines[0].contains("Fresh start of pipeline logs"),
            "unexpected opening line: {}",
            lines[0]
        );
        assert_eq!(&lines[1..], ["one", "two", "Logs have ended"]);
    }

    /// A malformed cursor can only come from a broken client, so it is rejected rather
    /// than quietly reinterpreted as some other position.
    #[tokio::test]
    async fn get_logs_rejects_a_malformed_cursor() {
        let fixture = logs_fixture(&[]).await;
        let uri = format!("/logs/{}?cursor=nonsense", fixture.pipeline_id);
        let (status, _, _) = response_at(fixture, uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
