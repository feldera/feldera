//! The server: pipelines that a server starts from a circuit factory and that clients reach
//! over HTTP. The cases cover ingress with connector metadata, aborted uploads, and Avro;
//! egress with snapshots; the bootstrap of a changed circuit, silent and concurrent;
//! fault-tolerant replay; checkpoints at exit and on SIGTERM; transactions; profiles; and
//! the server's own record of initialization and suspend.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::thread::{self, sleep};
use std::time::{Duration, Instant};

use actix_test::{PayloadError, TestServer};
use actix_web::http::StatusCode;
use actix_web::web::{self, Bytes, Data as WebData};
use actix_web::{App, middleware::Logger};
use async_stream::stream;
use awc::ClientRequest;
use dbsp::storage::dirlock::LockedDirectory;
use feldera_adapterlib::test::{Updates, ZSet, async_wait};
use feldera_types::adapter_stats::{
    ExternalControllerStatus, ExternalOutputEndpointMetrics, TransactionStatus,
};
use feldera_types::checkpoint::{CheckpointResponse, CheckpointStatus};
use feldera_types::completion_token::{
    CompletionStatus, CompletionStatusResponse, CompletionTokenResponse,
};
use feldera_types::config::ProgramIr;
use feldera_types::program_schema::Relation;
use feldera_types::runtime_status::{
    BootstrapConfig, BootstrapPolicy, ExtendedRuntimeStatus, RuntimeDesiredStatus, RuntimeStatus,
};
use feldera_types::transaction::ConcurrentBootstrapPhase;
use futures::{Stream, StreamExt};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;
use serde::Deserialize;
use serde_json::json;
use tempfile::{NamedTempFile, TempDir};
use tokio::time::timeout;
use tracing::trace;
use uuid::Uuid;

use super::{
    InitializationState, PipelinePhase, ServerArgs, ServerState, bootstrap, build_app,
    error_handler, get_status, parse_config,
};
use crate::controller::ControllerBuilder;
use crate::test::pipeline::{PipelineTestEngine, TestCircuit};
use crate::test::suites::controller::{for_ids, identity, relation};
use crate::test::suites::json_parser;
use crate::test::{read, render, rows};
use crate::transport::http::Chunk;
use crate::{ControllerError, ensure_default_crypto_provider};

/// The Avro schema of the rows of [`relation`].
#[cfg(feature = "with-avro")]
const AVRO_SCHEMA: &str = r#"{
    "type": "record",
    "name": "basic",
    "fields": [
        { "name": "id", "type": "long" },
        { "name": "b", "type": "boolean" },
        { "name": "i", "type": ["null", "long"] },
        { "name": "s", "type": "string" }
    ]
}"#;

/// Returns each batch of `batches` as one message of CSV records.
#[cfg(feature = "with-kafka")]
fn csv_messages(batches: &[Updates]) -> Vec<Vec<u8>> {
    batches
        .iter()
        .map(|batch| render::csv(&relation(), batch.rows()))
        .collect()
}

/// Per-request timeout for the test HTTP client.
///
/// awc defaults to a 5s response timeout, shorter than the harness's own 20s
/// `async_wait`/completion budgets. Under CI load (or a k8s CPU-quota freeze)
/// the in-process server can stall past 5s and trip that deadline, panicking an
/// otherwise-healthy request. The `/ingress` POST is non-idempotent and cannot
/// be retried safely, so we widen the client deadline and let the higher-level
/// waits govern liveness.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Sends batches of rows of [`relation`] as CSV to the server.
struct TestHttpSender;

/// Receives rows of [`relation`] from the server.
struct TestHttpReceiver;

impl TestHttpSender {
    /// Serialize `data` as `csv` and send it as part of HTTP request.
    /// Returns the response as bytes.
    async fn send_stream(req: ClientRequest, data: &[Updates]) -> Bytes {
        let data: Vec<Vec<u8>> = data
            .iter()
            .map(|batch| render::csv(&relation(), batch.rows()))
            .collect();

        let mut response = req
            .timeout(REQUEST_TIMEOUT)
            .send_stream(stream! {
                for batch in data {
                    yield <Result<_, anyhow::Error>>::Ok(Bytes::from(batch));
                }
            })
            .await
            .unwrap();

        response.body().await.unwrap()
    }

    /// Serialize `data` as `csv` and send it as part of HTTP request;
    /// Deserialize JSON response.
    async fn send_stream_deserialize_resp<R>(req: ClientRequest, data: &[Updates]) -> R
    where
        R: for<'de> Deserialize<'de>,
    {
        let resp_bytes = Self::send_stream(req, data).await;
        serde_json::from_slice::<R>(&resp_bytes).unwrap()
    }
}

impl TestHttpReceiver {
    /// Read from `response` until the entire contents of `data` is received.
    async fn wait_for_output_unordered<S>(response: &mut S, data: &[Updates])
    where
        S: Stream<Item = Result<Bytes, PayloadError>> + Unpin,
    {
        let num_records: usize = data.iter().map(Updates::len).sum();
        let (_chunks, received) = collect_output_chunks(response, num_records).await;
        assert_eq!(received, batches_zset(data));
    }
}

/// Writes `config` to a temporary file, as a launcher writes `config.json`.
pub(super) fn json_config_file(config: &serde_json::Value) -> NamedTempFile {
    let mut config_file = NamedTempFile::new().unwrap();
    config_file
        .write_all(config.to_string().as_bytes())
        .unwrap();
    config_file
}

async fn get_stats(server: &TestServer) -> ExternalControllerStatus {
    server
        .get("/stats")
        .send()
        .await
        .unwrap()
        .json::<ExternalControllerStatus>()
        .await
        .unwrap()
}

async fn get_completion_status(server: &TestServer, token: &str) -> CompletionStatusResponse {
    server
        .get(format!("/completion_status?token={token}"))
        .send()
        .await
        .unwrap()
        .json::<CompletionStatusResponse>()
        .await
        .unwrap()
}

async fn print_stats(server: &TestServer) {
    let stats = serde_json::to_string_pretty(&get_stats(server).await).unwrap();

    println!("{stats}")
}

async fn start_test_server(
    engine: &dyn PipelineTestEngine,
    config_json: &serde_json::Value,
    deployment_id: Uuid,
) -> TestServer {
    start_test_server_with_options(
        engine,
        config_json,
        deployment_id,
        BootstrapConfig::from(BootstrapPolicy::Allow),
        &[None],
    )
    .await
}

async fn start_test_server_with_options(
    engine: &dyn PipelineTestEngine,
    config_json: &serde_json::Value,
    deployment_id: Uuid,
    bootstrap_config: BootstrapConfig,
    persistent_output_ids: &[Option<&str>],
) -> TestServer {
    start_test_server_with_state(
        engine,
        config_json,
        deployment_id,
        bootstrap_config,
        persistent_output_ids,
        None,
    )
    .await
    .0
}

/// A `program_ir` describing one input table and one view, the shape of the
/// test circuit. `view_persistent_id` identifies the view's state: giving
/// two restarts different ids makes `compute_pipeline_diff` report the view
/// as modified, exactly as a changed view definition does in a compiled
/// pipeline.
fn test_program_ir(view_persistent_id: &str) -> ProgramIr {
    ProgramIr {
        mir: serde_json::from_value(serde_json::json!({
            "0": {
                "operation": "source",
                "table": "test_input1",
                "persistent_id": "test_input1",
            },
            "1": {
                "operation": "sink",
                "view": "test_output1",
                "persistent_id": view_persistent_id,
            },
        }))
        .unwrap(),
        program_schema: serde_json::json!({ "inputs": [], "outputs": [] }),
        circuit_ir: None,
        udf_library: None,
    }
}

/// Like [start_test_server_with_options], but also returns the
/// [ServerState] so a test can reach the controller directly (e.g. to
/// simulate an ungraceful crash via [crash_pipeline]).
///
/// `program_ir` is injected into the pipeline config. The test circuit is a
/// Rust closure with no compiled program, so its checkpoint normally carries
/// no program info, which makes `compute_pipeline_diff` fail and forces the
/// journal to be discarded on restart (no replay). Injecting an identical
/// `program_ir` makes the diff empty across a same-program restart, so the
/// journal is replayed, as the fault-tolerant replay path needs in-process. Injecting IRs that differ makes the diff report the
/// change, as it does for a recompiled program.
async fn start_test_server_with_state(
    engine: &dyn PipelineTestEngine,
    config_json: &serde_json::Value,
    deployment_id: Uuid,
    bootstrap_config: BootstrapConfig,
    persistent_output_ids: &[Option<&str>],
    program_ir: Option<ProgramIr>,
) -> (TestServer, WebData<ServerState>) {
    start_test_server_for(
        engine,
        config_json,
        deployment_id,
        bootstrap_config,
        persistent_output_ids,
        program_ir,
        relation(),
    )
    .await
}

/// Like [`start_test_server_with_state`], but the tables and views of the
/// circuit have the columns of `relation`.
async fn start_test_server_for(
    engine: &dyn PipelineTestEngine,
    config_json: &serde_json::Value,
    deployment_id: Uuid,
    bootstrap_config: BootstrapConfig,
    persistent_output_ids: &[Option<&str>],
    program_ir: Option<ProgramIr>,
    relation: Relation,
) -> (TestServer, WebData<ServerState>) {
    let config_file = json_config_file(config_json);

    println!("Creating HTTP server");

    let state = WebData::new(ServerState::new(
        PipelinePhase::Initializing(InitializationState::Starting),
        String::default(),
        RuntimeDesiredStatus::Paused,
        bootstrap_config,
        deployment_id,
        None,
        None,
        None,
        None,
    ));
    let state_clone = state.clone();

    let args = ServerArgs {
        config_file: config_file.path().display().to_string(),
        metadata_file: None,
        bind_address: "127.0.0.1".to_string(),
        default_port: None,
        storage_location: None,
        enable_https: false,
        https_tls_cert_path: None,
        https_tls_key_path: None,
        initial: RuntimeDesiredStatus::Paused,
        bootstrap_policy: bootstrap_config.active_bootstrap_policy(),
        silent_bootstrap: bootstrap_config.silent_bootstrap,
        concurrent_bootstrap: bootstrap_config.concurrent_bootstrap,
        deployment_id,
        host_id: None,
    };

    let mut config = parse_config(&args.config_file).unwrap();
    if let Some(program_ir) = program_ir {
        config.program_ir = Some(program_ir);
    }
    let builder = ControllerBuilder::new(&config).unwrap();
    let circuit = TestCircuit::Identity {
        relation,
        persistent_output_ids: persistent_output_ids
            .iter()
            .map(|id| id.map(str::to_string))
            .collect(),
        input_properties: Vec::new(),
        materialized: true,
        persistent_ids: true,
    };
    let factory = engine.circuit_factory(&circuit, None).unwrap();
    thread::spawn(move || bootstrap(builder, factory, state_clone));

    let state_ret = state.clone();
    let server =
        actix_test::start(move || build_app(App::new().wrap(Logger::default()), state.clone()));

    let start = Instant::now();
    while server.get("/stats").send().await.unwrap().status() == StatusCode::SERVICE_UNAVAILABLE {
        assert!(start.elapsed() < Duration::from_millis(20_000));
        sleep(Duration::from_millis(200));
    }

    (server, state_ret)
}

/// Simulate an ungraceful crash of a fault-tolerant pipeline: terminate the
/// circuit without writing a checkpoint (unlike `/suspend`), so the journal
/// tail must be replayed on the next restart, and wait for the storage lock
/// to be released so a new server can reopen the same storage directory.
async fn crash_pipeline(state: &WebData<ServerState>, storage_dir: &Path) {
    let controller = state.abandon_controller().expect("controller present");
    tokio::task::spawn_blocking(move || controller.stop())
        .await
        .unwrap()
        .expect("controller stops cleanly");
    wait_for_storage_unlock(storage_dir).await;
}

/// Returns the rows of `data`, consolidated.
fn batches_zset(data: &[Updates]) -> ZSet {
    let mut zset = ZSet::empty(crate::test::published_schema(&relation())).unwrap();
    for batch in data {
        zset.add(&batch.zset().unwrap()).unwrap();
    }
    zset
}

/// Returns the CSV rows of `chunk`, each of which must have weight 1.
fn decode_chunk_records(chunk: &Chunk) -> Option<Updates> {
    let csv = chunk.text_data.as_ref()?;
    let records = read::csv_updates(&relation(), csv.as_bytes()).unwrap();
    assert!(records.weights().iter().all(|weight| *weight == 1));
    Some(records)
}

/// Reads chunks from `response` until they hold `num_records` rows, and returns the chunks
/// with rows and their rows, each of which must have weight 1.
async fn collect_output_chunks<S>(response: &mut S, num_records: usize) -> (Vec<Chunk>, ZSet)
where
    S: Stream<Item = Result<web::Bytes, PayloadError>> + Unpin,
{
    let mut chunks = Vec::new();
    let mut records = ZSet::empty(crate::test::published_schema(&relation())).unwrap();
    let mut num_received = 0;
    let mut data = Vec::new();

    while num_received < num_records {
        let bytes = response.next().await.unwrap().unwrap();
        data.extend_from_slice(&bytes);

        if data.last() != Some(&b'\n') {
            continue;
        }

        for chunk in serde_json::Deserializer::from_slice(&data).into_iter::<Chunk>() {
            let chunk = chunk.unwrap();
            trace!("collect_output_chunks: chunk {}", chunk.sequence_number);
            let Some(chunk_records) = decode_chunk_records(&chunk) else {
                continue;
            };
            if chunk_records.is_empty() {
                continue;
            }
            num_received += chunk_records.len();
            records.add(&chunk_records.zset().unwrap()).unwrap();
            chunks.push(chunk);
        }

        data.clear();
    }

    (chunks, records)
}

/// Reads chunks up to and including the first one whose `processed_steps` reaches `steps`, and
/// returns them with the records that they carry.
async fn collect_output_chunks_through_step<S>(response: &mut S, steps: u64) -> (Vec<Chunk>, ZSet)
where
    S: Stream<Item = Result<web::Bytes, PayloadError>> + Unpin,
{
    let mut chunks = Vec::new();
    let mut records = ZSet::empty(crate::test::published_schema(&relation())).unwrap();
    let mut data = Vec::new();

    loop {
        let bytes = response.next().await.unwrap().unwrap();
        data.extend_from_slice(&bytes);

        if data.last() != Some(&b'\n') {
            continue;
        }

        for chunk in serde_json::Deserializer::from_slice(&data).into_iter::<Chunk>() {
            let chunk = chunk.unwrap();
            if let Some(chunk_records) = decode_chunk_records(&chunk) {
                records.add(&chunk_records.zset().unwrap()).unwrap();
            }
            let done = chunk.processed_steps.is_some_and(|count| count >= steps);
            chunks.push(chunk);
            if done {
                return (chunks, records);
            }
        }

        data.clear();
    }
}

async fn wait_for_completion(server: &TestServer, token: &str) {
    let completed = async_wait(
        || async {
            let CompletionStatusResponse { status, .. } =
                get_completion_status(server, token).await;
            status == CompletionStatus::Complete
        },
        20_000,
    )
    .await
    .is_ok();

    if !completed {
        // The step the token waits for, next to each connector's
        // `total_processed_steps`, names the endpoint that is holding
        // `total_completed_steps` back.
        let status = get_completion_status(server, token).await;
        print_stats(server).await;
        panic!("completion token '{token}' is still {status:?} after 20 seconds");
    }
}

/// Returns one batch of the rows with ids `start..start + len`.
fn test_batches(start: u32, len: u32) -> Vec<Updates> {
    vec![for_ids(start as usize..(start + len) as usize)]
}

/// Returns the number of rows in `batches`.
fn batch_num_records(batches: &[Updates]) -> u64 {
    batches.iter().map(|batch| batch.len() as u64).sum()
}

/// Returns the rows of the CSV file at `output_path`, each of which must have weight 1, or no
/// rows if the file does not exist.
fn read_file_output(output_path: &Path) -> ZSet {
    if !output_path.exists() {
        return ZSet::empty(crate::test::published_schema(&relation())).unwrap();
    }
    let actual = read::csv_updates(&relation(), &std::fs::read(output_path).unwrap()).unwrap();
    assert!(actual.weights().iter().all(|weight| *weight == 1));
    actual.zset().unwrap()
}

async fn wait_for_status(server: &TestServer, expected: RuntimeStatus) {
    async_wait(
        || async {
            let Ok(status) = server
                .get("/status")
                .send()
                .await
                .unwrap()
                .json::<ExtendedRuntimeStatus>()
                .await
            else {
                return false;
            };
            status.runtime_status == expected
        },
        20_000,
    )
    .await
    .unwrap();
}

async fn start_pipeline(server: &TestServer) {
    println!("/start");
    let resp = server.get("/start").send().await.unwrap();
    assert!(resp.status().is_success());
    wait_for_status(server, RuntimeStatus::Running).await;
}

async fn pause_pipeline(server: &TestServer) {
    println!("/pause");
    let resp = server.get("/pause").send().await.unwrap();
    assert!(resp.status().is_success());
    wait_for_status(server, RuntimeStatus::Paused).await;
}

/// Wait until the storage directory lock (`feldera.pidlock`) can be acquired,
/// indicating that the previous pipeline runtime has released storage.
async fn wait_for_storage_unlock(storage_dir: &Path) {
    let storage_dir = storage_dir.to_path_buf();
    async_wait(
        || {
            let storage_dir = storage_dir.clone();
            async move {
                tokio::task::spawn_blocking(move || {
                    // Probe whether the lock is free; release immediately if so.
                    LockedDirectory::new(&storage_dir).is_ok()
                })
                .await
                .unwrap()
            }
        },
        20_000,
    )
    .await
    .unwrap();
}

/// Suspend the pipeline. When `storage_dir` is set, also wait until the
/// previous runtime releases the storage lock before returning, so a new
/// server can safely reopen the same storage directory.
async fn suspend_pipeline(server: &TestServer, storage_dir: Option<&Path>) {
    println!("/suspend");
    let resp = server.post("/suspend").send().await.unwrap();
    assert!(resp.status().is_success());
    wait_for_status(server, RuntimeStatus::Suspended).await;
    if let Some(storage_dir) = storage_dir {
        wait_for_storage_unlock(storage_dir).await;
    }
}

async fn start_transaction(server: &TestServer) {
    println!("/start_transaction");
    let resp = server.post("/start_transaction").send().await.unwrap();
    assert!(resp.status().is_success());
}

async fn commit_transaction(server: &TestServer) {
    println!("/commit_transaction");
    let resp = server.post("/commit_transaction").send().await.unwrap();
    assert!(resp.status().is_success());
}

#[cfg(feature = "with-kafka")]
async fn start_input_endpoint(server: &TestServer, endpoint_name: &str) {
    println!("/input_endpoints/{endpoint_name}/start");
    let resp = server
        .get(format!("/input_endpoints/{endpoint_name}/start"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
}

#[cfg(feature = "with-kafka")]
async fn pause_input_endpoint(server: &TestServer, endpoint_name: &str) {
    println!("/input_endpoints/{endpoint_name}/pause");
    let resp = server
        .get(format!("/input_endpoints/{endpoint_name}/pause"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
}

async fn send_input_no_wait(server: &TestServer, data: &[Updates]) -> String {
    let CompletionTokenResponse { token } = TestHttpSender::send_stream_deserialize_resp::<
        CompletionTokenResponse,
    >(server.post("/ingress/test_input1"), data)
    .await;
    token
}

async fn send_input(server: &TestServer, data: &[Updates]) {
    let token = send_input_no_wait(server, data).await;
    wait_for_completion(server, &token).await;
}

async fn wait_for_file_output(output_path: &Path, expected: &[Updates]) {
    let expected = batches_zset(expected);

    async_wait(
        || std::future::ready(read_file_output(output_path) == expected),
        20_000,
    )
    .await
    .unwrap();
}

/// Run an ad-hoc query and return the integer value of its single `c`
/// column (used with `SELECT COUNT(*) AS c ...`).
async fn adhoc_query_count(server: &TestServer, sql: &str) -> i64 {
    let qs = form_urlencoded::Serializer::new(String::new())
        .append_pair("sql", sql)
        .append_pair("format", "json")
        .finish();
    let mut resp = server.get(format!("/query?{qs}")).send().await.unwrap();
    assert!(
        resp.status().is_success(),
        "ad-hoc query failed: {}",
        resp.status()
    );
    let body = resp.body().await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    // Newline-delimited JSON; `SELECT COUNT(*) AS c` yields one `{"c":N}` row.
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find_map(|v| v.get("c").and_then(|c| c.as_i64()))
        .unwrap_or(0)
}

/// The rows of an ad-hoc query, as the `/query` endpoint returns them in
/// its JSON format: one JSON object per line.
async fn adhoc_query_rows(server: &TestServer, sql: &str) -> Vec<serde_json::Value> {
    let qs = form_urlencoded::Serializer::new(String::new())
        .append_pair("sql", sql)
        .append_pair("format", "json")
        .finish();
    let mut resp = server.get(format!("/query?{qs}")).send().await.unwrap();
    assert!(
        resp.status().is_success(),
        "ad-hoc query failed: {}",
        resp.status()
    );
    let body = resp.body().await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn output_metrics(stats: &ExternalControllerStatus) -> &ExternalOutputEndpointMetrics {
    stats
        .outputs
        .iter()
        .find(|output| output.endpoint_name == "test_output1")
        .map(|output| &output.metrics)
        .expect("test_output1 output endpoint")
}

/// `(total_processed_input_records, transmitted_records, queued_records)` of
/// the `test_output1` endpoint, with the pipeline's concurrent-bootstrap
/// phase.
async fn output_progress(server: &TestServer) -> (u64, u64, u64, ConcurrentBootstrapPhase) {
    let stats = get_stats(server).await;
    let metrics = output_metrics(&stats);
    (
        metrics.total_processed_input_records,
        metrics.transmitted_records,
        metrics.queued_records,
        stats.global_metrics.concurrent_bootstrap_phase,
    )
}

/// After a silent-bootstrap restart, verify that backfilled records were
/// processed but not written to the output file.
///
/// Waits for bootstrap to finish and for the output connector to catch up
/// with the checkpointed input count before asserting.
async fn assert_no_file_output(
    server: &TestServer,
    output_path: &Path,
    min_processed_records: u64,
    expected_transmitted_records: u64,
) {
    wait_for_status(server, RuntimeStatus::Paused).await;

    async_wait(
        || async {
            output_metrics(&get_stats(server).await).total_processed_input_records
                >= min_processed_records
        },
        20_000,
    )
    .await
    .unwrap();

    let stats = get_stats(server).await;
    let metrics = output_metrics(&stats);
    assert_eq!(
        metrics.transmitted_records,
        expected_transmitted_records,
        "silent bootstrap must not transmit backfilled records, stats: {stats:#?}\nfile contents: {:?}",
        read_file_output(output_path)
    );
    assert!(read_file_output(output_path).is_empty());
}

/// A completed suspend must read back as `Suspended` while the controller is
/// still registered.
///
/// The `/suspend` task records [`PipelinePhase::Suspended`] only after
/// `async_suspend` returns, so a poll in between sees a live controller whose
/// circuit is already `Terminated`. Reporting that as a fatal
/// `PipelineTerminated` is what flaked `suspend_and_resume_demos`.
/// Suspending the controller directly, without going through `/suspend`,
/// leaves the state in exactly that window.
pub fn completed_suspend_is_reported_before_the_phase_is_recorded(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        std::fs::create_dir(&storage_dir).unwrap();
        let config_json = json!({
            "name": "test",
            "workers": 2,
            "storage_config": {
                "path": storage_dir,
            },
            "storage": true,
            "clock_resolution_usecs": null,
            "inputs": {},
            "outputs": {},
        });
        let (_server, state) = start_test_server_with_state(
            engine,
            &config_json,
            Uuid::now_v7(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[Some("v0")],
            None,
        )
        .await;

        state
            .controller()
            .expect("controller present")
            .async_suspend()
            .await
            .expect("suspend succeeds");

        // The window: the controller is still registered and the phase still says
        // the pipeline is initialized.
        assert!(matches!(
            state.phase(),
            PipelinePhase::InitializationComplete
        ));
        assert!(state.controller().is_ok());

        let status = get_status(&state, false).expect("a completed suspend is not an error");
        assert!(
            matches!(status.runtime_status, RuntimeStatus::Suspended),
            "a completed suspend read back as {:?}",
            status.runtime_status
        );
    })
}

/// `/status` must report a fatal controller error that arrives after the
/// controller is published but before the phase is.
///
/// The circuit thread reports initialization complete and then immediately
/// enters its step loop (see `CircuitThread::run`), so a fatal error from its
/// first steps races the tail of `do_bootstrap`, which publishes the
/// controller and the phase in two separate steps.  An error landing between
/// them leaves the controller deallocated, so the phase must stay `Failed`
/// rather than being overwritten with `InitializationComplete`: `/status`
/// reports a controller-less `InitializationComplete` as the fatal
/// `ControllerMissingAfterInitialization`, which masks the real error and
/// makes the pipeline manager record that instead.
pub fn fatal_error_during_bootstrap_tail_is_reported(engine: &dyn PipelineTestEngine) {
    ensure_default_crypto_provider();

    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    std::fs::create_dir(&storage_dir).unwrap();
    let config_json = json!({
        "name": "test",
        "workers": 2,
        "storage_config": {
            "path": storage_dir,
        },
        "storage": true,
        "clock_resolution_usecs": null,
        "inputs": {},
        "outputs": {},
    });
    let config_file = json_config_file(&config_json);
    let config = parse_config(config_file.path().display().to_string()).unwrap();
    let builder = ControllerBuilder::new(&config).unwrap();

    let state = WebData::new(ServerState::new(
        PipelinePhase::Initializing(InitializationState::Starting),
        String::default(),
        RuntimeDesiredStatus::Paused,
        BootstrapConfig::from(BootstrapPolicy::Allow),
        Uuid::now_v7(),
        None,
        None,
        None,
        None,
    ));

    {
        let state = state.clone();
        let factory = engine.circuit_factory(&identity(&[None]), None).unwrap();
        thread::spawn(move || bootstrap(builder, factory, state))
    }
    .join()
    .unwrap();

    // Baseline: a healthy pipeline reports a status, not an error.
    assert!(get_status(&state, false).is_ok());

    // Rewind to just before the bootstrap tail publishes its result.
    let controller = state.abandon_controller().expect("controller present");

    // The circuit thread reports a fatal error, which must be recorded even
    // though there is no controller to deallocate yet.
    let weak = Arc::downgrade(&state.clone().into_inner());
    error_handler(&weak, Arc::new(ControllerError::DbspPanic), None);

    // The bootstrap tail then finishes, and must not claim success.
    assert!(
        state.complete_initialization(controller).is_err(),
        "the bootstrap tail published a controller that a failure invalidated"
    );

    let error = get_status(&state, false)
        .expect_err("a pipeline that hit a fatal error must report an error");
    assert_ne!(
        error.error.error_code.as_ref(),
        "ControllerMissingAfterInitialization",
        "the bootstrap tail masked the fatal error"
    );
    assert_eq!(error.error.error_code.as_ref(), "DbspPanic");
}

/// Initialization whose result is refused must stop the circuit it built.
///
/// The failure that refuses publication was recorded before the controller
/// existed, so `fail` had no controller to stop, and a circuit whose step
/// fails keeps stepping (see `CircuitThread::step_circuit`). The bootstrap() tail
/// holds the only remaining reference to that circuit, so dropping it instead
/// of stopping it leaves the circuit running with nobody able to reach it,
/// still holding the storage lock that the next pipeline process needs.
pub fn a_refused_bootstrap_result_stops_the_circuit(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        std::fs::create_dir(&storage_dir).unwrap();
        let config_json = json!({
            "name": "test",
            "workers": 2,
            "storage_config": {
                "path": storage_dir,
            },
            "storage": true,
            "clock_resolution_usecs": null,
            "inputs": {},
            "outputs": {},
        });
        let config_file = json_config_file(&config_json);
        let config = parse_config(config_file.path().display().to_string()).unwrap();
        let builder = ControllerBuilder::new(&config).unwrap();

        let state = WebData::new(ServerState::new(
            PipelinePhase::Initializing(InitializationState::Starting),
            String::default(),
            RuntimeDesiredStatus::Paused,
            BootstrapConfig::from(BootstrapPolicy::Allow),
            Uuid::now_v7(),
            None,
            None,
            None,
            None,
        ));

        // Record the failure before initialization starts, so the tail is
        // certain to refuse the controller it builds.
        let weak = Arc::downgrade(&state.clone().into_inner());
        error_handler(&weak, Arc::new(ControllerError::DbspPanic), None);

        let bootstrap_state = state.clone();
        let factory = engine.circuit_factory(&identity(&[None]), None).unwrap();
        tokio::task::spawn_blocking(move || bootstrap(builder, factory, bootstrap_state))
            .await
            .unwrap();

        // Nothing was published and the failure stands.
        assert!(state.controller().is_err());
        assert!(matches!(state.phase(), PipelinePhase::Failed(_)));

        // The circuit was stopped, so it has let go of the storage lock.
        wait_for_storage_unlock(&storage_dir).await;
    })
}

/// Downloads both circuit profiles from a running pipeline.
pub fn profiles_stream_from_running_pipeline(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        use std::io::Read;
        crate::ensure_default_crypto_provider();

        let server = start_test_server(
            engine,
            &json!({"name": "test", "inputs": {}, "outputs": {}}),
            Uuid::new_v4(),
        )
        .await;
        start_pipeline(&server).await;

        // Without `Accept-Encoding` the JSON profile streams uncompressed.
        let mut plain = server
            .get("/dump_json_profile")
            .no_decompress()
            .send()
            .await
            .unwrap();
        assert_eq!(plain.status(), StatusCode::OK);
        assert!(plain.headers().get("content-encoding").is_none());
        assert_eq!(
            plain.headers().get("transfer-encoding").unwrap(),
            "chunked",
            "the profile must stream rather than be buffered"
        );
        let plain_body = plain.body().limit(64 << 20).await.unwrap();
        let plain_json: serde_json::Value = serde_json::from_slice(&plain_body).unwrap();
        let workers = plain_json["worker_profiles"].as_array().unwrap().len();
        assert!(workers > 0);

        // With `Accept-Encoding: gzip` it arrives gzip-compressed, with the same content.
        let mut gz = server
            .get("/dump_json_profile")
            .insert_header(("accept-encoding", "gzip"))
            .no_decompress()
            .send()
            .await
            .unwrap();
        assert_eq!(gz.status(), StatusCode::OK);
        assert_eq!(gz.headers().get("content-encoding").unwrap(), "gzip");
        let gz_body = gz.body().limit(64 << 20).await.unwrap();
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(&gz_body[..])
            .read_to_end(&mut decoded)
            .unwrap();
        let gz_json: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(
            gz_json["worker_profiles"].as_array().unwrap().len(),
            workers
        );
        assert_eq!(gz_json["graph"], plain_json["graph"]);

        // The dot profile is a zip with one `.dot` and `.txt` per worker plus a Makefile.
        let mut dot = server.get("/dump_profile").send().await.unwrap();
        assert_eq!(dot.status(), StatusCode::OK);
        assert_eq!(
            dot.headers().get("content-type").unwrap(),
            "application/zip"
        );
        let dot_body = dot.body().limit(64 << 20).await.unwrap();
        // Zip local headers carry entry names uncompressed, so they are visible as bytes.
        assert!(dot_body.starts_with(b"PK\x03\x04"));
        let contains = |name: &[u8]| dot_body.windows(name.len()).any(|w| w == name);
        assert!(contains(b"0.dot") && contains(b"0.txt") && contains(b"Makefile"));
    })
}

/// Connector metadata that the `connector_metadata` tests attach to a
/// request, with one attribute for each metadata column of
/// [`json_parser::metadata_relation`].
const CONNECTOR_METADATA: &str = r#"{"kafka_headers":{"h":"v"},"kafka_topic":"events","kafka_timestamp":"2025-01-01 10:00:00","kafka_partition":3,"kafka_offset":42}"#;

/// Posts `body` to `/ingress/test_input1` as raw JSON with the given
/// `connector_metadata`; returns the response status and body.
async fn post_with_connector_metadata(
    server: &TestServer,
    connector_metadata: &str,
    body: &'static str,
) -> (StatusCode, String) {
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("format", "json")
        .append_pair("update_format", "raw")
        .append_pair("connector_metadata", connector_metadata)
        .finish();
    let mut response = server
        .post(format!("/ingress/test_input1?{query}"))
        .send_body(body)
        .await
        .unwrap();
    let status = response.status();
    let body = response.body().await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// Checks that `test_output1` holds exactly the records with the given
/// `ids`, each with the metadata columns from `CONNECTOR_METADATA`.
async fn assert_records_carry_connector_metadata(server: &TestServer, ids: &[i64]) {
    let rows = adhoc_query_rows(
        server,
        "SELECT i, kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, kafka_headers \
         FROM test_output1 ORDER BY i",
    )
    .await;
    assert_eq!(rows.len(), ids.len(), "{rows:?}");
    for (row, id) in rows.iter().zip(ids) {
        assert_eq!(row["i"], serde_json::json!(id), "{row}");
        assert_eq!(row["kafka_topic"], serde_json::json!("events"), "{row}");
        assert_eq!(row["kafka_partition"], serde_json::json!(3), "{row}");
        assert_eq!(row["kafka_offset"], serde_json::json!(42), "{row}");
        let timestamp = row["kafka_timestamp"].as_str().expect("timestamp string");
        assert!(
            timestamp.starts_with("2025-01-01") && timestamp.contains("10:00:00"),
            "{row}"
        );
        // The query engine returns a VARIANT either as a JSON value or as
        // its JSON text.
        let headers = &row["kafka_headers"];
        let headers = headers
            .as_str()
            .map(|text| serde_json::from_str::<serde_json::Value>(text).unwrap())
            .unwrap_or_else(|| headers.clone());
        assert_eq!(headers, serde_json::json!({"h": "v"}), "{row}");
    }
}

/// The `connector_metadata` query parameter of `/ingress` reaches every
/// record of the request through `CONNECTOR_METADATA()`, and an invalid
/// parameter is rejected before any record is ingested.
pub fn ingress_connector_metadata(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let (server, _state) = start_test_server_for(
            engine,
            &json!({"name": "test", "inputs": {}, "outputs": {}}),
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[None],
            None,
            json_parser::metadata_relation(),
        )
        .await;
        start_pipeline(&server).await;

        let (status, body) =
            post_with_connector_metadata(&server, CONNECTOR_METADATA, "{\"i\": 1}\n{\"i\": 2}")
                .await;
        assert!(status.is_success(), "{body}");
        let CompletionTokenResponse { token } = serde_json::from_str(&body).unwrap();
        wait_for_completion(&server, &token).await;
        assert_records_carry_connector_metadata(&server, &[1, 2]).await;

        for invalid in ["not json", "[1, 2]", "\"text\""] {
            let (status, body) = post_with_connector_metadata(&server, invalid, "{\"i\": 3}").await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}: {body}");
            assert!(body.contains("connector_metadata"), "{body}");
        }
        assert_eq!(
            adhoc_query_count(&server, "SELECT COUNT(*) AS c FROM test_output1").await,
            2
        );
    })
}

/// `/ingress` rejects JSON update formats that input does not support.
/// It used to accept `redis` and `weighted` and panic in the parser while
/// holding the endpoint's lock, after which no step could complete.
/// `weighted` is no longer a format at all.
pub fn ingress_rejects_unsupported_update_formats(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();
        let server = start_test_server(
            engine,
            &json!({"name": "test", "inputs": {}, "outputs": {}}),
            Uuid::new_v4(),
        )
        .await;
        start_pipeline(&server).await;

        for update_format in ["redis", "weighted"] {
            let mut response = timeout(
                Duration::from_secs(20),
                server
                    .post(format!(
                        "/ingress/test_input1?format=json&update_format={update_format}"
                    ))
                    .send_body(r#"{"weight": 1, "data": {}}"#),
            )
            .await
            .unwrap_or_else(|_| {
                panic!("/ingress with update_format={update_format} did not return")
            })
            .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{update_format}"
            );
            let body = response.body().await.unwrap();
            let body = String::from_utf8_lossy(&body);
            let expected = match update_format {
                "redis" => "not supported for JSON input",
                _ => "unknown variant `weighted`",
            };
            assert!(body.contains(expected), "{update_format}: {body}");
        }

        let token = timeout(
            Duration::from_secs(20),
            send_input_no_wait(&server, &test_batches(0, 10)),
        )
        .await
        .expect("a later /ingress did not return");
        wait_for_completion(&server, &token).await;
    })
}

/// A client that hangs up in the middle of an `/ingress` upload fails
/// only its own request. The table's ingress endpoint is shared by every
/// later request, so it keeps accepting data and `/stats` must not report
/// it as failed.
pub fn ingress_aborted_upload_is_not_fatal(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        use tokio::io::AsyncWriteExt;

        ensure_default_crypto_provider();

        let server = start_test_server(
            engine,
            &json!({"name": "test", "inputs": {}, "outputs": {}}),
            Uuid::new_v4(),
        )
        .await;
        start_pipeline(&server).await;

        // Announce a larger body than is sent, then close the connection.
        let row = r#"{"id": 1, "b": true, "i": null, "s": "a"}"#;
        let request = format!(
            "POST /ingress/test_input1?format=json&update_format=raw HTTP/1.1\r\n\
         Host: localhost\r\nContent-Type: application/json\r\n\
         Content-Length: 1000000\r\n\r\n{row}\n"
        );
        let mut connection = tokio::net::TcpStream::connect(server.addr()).await.unwrap();
        connection.write_all(request.as_bytes()).await.unwrap();
        drop(connection);

        // Wait until the server has read the broken body.
        let errors_seen = async_wait(
            || async {
                get_stats(&server)
                    .await
                    .inputs
                    .iter()
                    .any(|input| input.metrics.num_transport_errors > 0)
            },
            20_000,
        )
        .await;
        assert!(errors_seen.is_ok(), "the aborted upload was never noticed");

        let mut response = server
            .post("/ingress/test_input1?format=json&update_format=raw")
            .send_body(r#"{"id": 2, "b": false, "i": 2, "s": "b"}"#)
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{:?}",
            response.body().await
        );

        for input in get_stats(&server).await.inputs {
            assert_eq!(
                input.fatal_error, None,
                "input endpoint '{}' is reported as failed",
                input.endpoint_name
            );
        }
    })
}

/// A fault-tolerant replay parses the journaled chunks with the
/// `connector_metadata` of their requests, so the replayed records carry
/// the same metadata columns as the original ones.
pub fn ft_replay_connector_metadata(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        std::fs::create_dir(&storage_dir).unwrap();
        let config_json = json!({
            "name": "test",
            "workers": 1,
            "storage_config": {
                "path": storage_dir,
            },
            "storage": true,
            "fault_tolerance": "latest_checkpoint",
            "clock_resolution_usecs": null,
            "inputs": {},
            "outputs": {},
        });

        let (server, state) = start_test_server_for(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[Some("v0")],
            Some(test_program_ir("v0")),
            json_parser::metadata_relation(),
        )
        .await;
        start_pipeline(&server).await;
        let (status, body) =
            post_with_connector_metadata(&server, CONNECTOR_METADATA, "{\"i\": 1}").await;
        assert!(status.is_success(), "{body}");
        let CompletionTokenResponse { token } = serde_json::from_str(&body).unwrap();
        wait_for_completion(&server, &token).await;
        assert_records_carry_connector_metadata(&server, &[1]).await;

        // Crash without a checkpoint, so that the restart replays the journal.
        crash_pipeline(&state, &storage_dir).await;
        drop(server);

        let (server, _state) = start_test_server_for(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[Some("v0")],
            Some(test_program_ir("v0")),
            json_parser::metadata_relation(),
        )
        .await;
        start_pipeline(&server).await;
        assert_records_carry_connector_metadata(&server, &[1]).await;

        suspend_pipeline(&server, Some(&storage_dir)).await;
    })
}

/// Avro over `/ingress`: the schema travels in the query string, and each
/// request body is one datum (issue 7397).
#[cfg(feature = "with-avro")]
pub fn http_ingress_avro(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        use apache_avro::{Schema as AvroSchema, to_avro_datum, types::Value as AvroValue};

        ensure_default_crypto_provider();

        let ids = [1, 2];
        let data = vec![for_ids(ids)];
        let schema = AvroSchema::parse_str(AVRO_SCHEMA).unwrap();
        let query = form_urlencoded::Serializer::new(String::new())
            .append_pair("format", "avro")
            .append_pair("update_format", "raw")
            .append_pair("skip_schema_id", "true")
            .append_pair("schema", AVRO_SCHEMA)
            .finish();

        let server = start_test_server(
            engine,
            &json!({"name": "test", "inputs": {}, "outputs": {}}),
            Uuid::new_v4(),
        )
        .await;
        start_pipeline(&server).await;

        // The rows of `for_ids`: `b` false, `i` NULL, and `s` empty.
        let datums: Vec<Vec<u8>> = ids
            .iter()
            .map(|id| {
                let value = AvroValue::Record(vec![
                    ("id".to_string(), AvroValue::Long(*id as i64)),
                    ("b".to_string(), AvroValue::Boolean(false)),
                    (
                        "i".to_string(),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    ),
                    ("s".to_string(), AvroValue::String(String::new())),
                ]);
                to_avro_datum(&schema, value).unwrap()
            })
            .collect();
        for datum in &datums {
            let mut response = server
                .post(format!("/ingress/test_input1?{query}"))
                .send_body(datum.clone())
                .await
                .unwrap();
            let status = response.status();
            let body = response.body().await.unwrap();
            assert!(status.is_success(), "{}", String::from_utf8_lossy(&body));
            let CompletionTokenResponse { token } = serde_json::from_slice(&body).unwrap();
            wait_for_completion(&server, &token).await;
        }

        // A body that holds two datums is rejected instead of silently
        // dropping the second datum.
        let two_datums = [datums[0].as_slice(), datums[1].as_slice()].concat();
        let mut response = server
            .post(format!("/ingress/test_input1?{query}"))
            .send_body(two_datums)
            .await
            .unwrap();
        let status = response.status();
        let body = String::from_utf8_lossy(&response.body().await.unwrap()).into_owned();
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("trailing bytes"), "{body}");

        let mut response = server
            .post("/egress/test_output1?format=csv&backpressure=true&send_snapshot=true")
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let (_chunks, records) = timeout(
            Duration::from_secs(10),
            collect_output_chunks(&mut response, 2),
        )
        .await
        .unwrap();
        assert_eq!(records, batches_zset(&data));
    })
}

/// Verifies the `send_snapshot` HTTP egress mode:
/// 1. Pushes initial data, connects with `send_snapshot=true`, and
///    verifies the snapshot chunks carry `snapshot: true`.
/// 2. Pushes more data and verifies incremental delta chunks arrive
///    with `snapshot: false`.
/// 3. Opens a second connection and verifies it receives a full
///    snapshot of all data (initial + follow).
pub fn http_egress_send_snapshot(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let initial_data = vec![for_ids([1, 2]), for_ids([3])];
        let follow_data = vec![for_ids([10, 11])];

        let server = start_test_server(
            engine,
            &json!({"name": "test", "inputs": {}, "outputs": {}}),
            Uuid::new_v4(),
        )
        .await;

        start_pipeline(&server).await;

        let CompletionTokenResponse { token } =
            TestHttpSender::send_stream_deserialize_resp::<CompletionTokenResponse>(
                server.post("/ingress/test_input1"),
                &initial_data,
            )
            .await;
        wait_for_completion(&server, &token).await;

        let mut response = server
            .post("/egress/test_output1?format=csv&backpressure=true&send_snapshot=true")
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());

        let (snapshot_chunks, snapshot_records) = match timeout(
            Duration::from_secs(10),
            collect_output_chunks(&mut response, initial_data.iter().map(Updates::len).sum()),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                print_stats(&server).await;
                panic!("timed out waiting for snapshot output: {error:?}");
            }
        };
        assert_eq!(snapshot_records, batches_zset(&initial_data));
        assert!(!snapshot_chunks.is_empty());
        assert!(snapshot_chunks.iter().all(|chunk| chunk.snapshot));

        let CompletionTokenResponse { token } = TestHttpSender::send_stream_deserialize_resp::<
            CompletionTokenResponse,
        >(
            server.post("/ingress/test_input1"), &follow_data
        )
        .await;
        wait_for_completion(&server, &token).await;

        let (delta_chunks, delta_records) = timeout(
            Duration::from_secs(10),
            collect_output_chunks(&mut response, follow_data.iter().map(Updates::len).sum()),
        )
        .await
        .expect("timed out waiting for follow output");
        assert_eq!(delta_records, batches_zset(&follow_data));
        assert!(!delta_chunks.is_empty());
        assert!(delta_chunks.iter().all(|chunk| !chunk.snapshot));

        // Open a second connection: it should receive a snapshot of all data
        // (initial + follow) since the view is materialized.
        let all_data_len: usize = initial_data.iter().map(Updates::len).sum::<usize>()
            + follow_data.iter().map(Updates::len).sum::<usize>();

        let mut response2 = server
            .post("/egress/test_output1?format=csv&backpressure=true&send_snapshot=true")
            .send()
            .await
            .unwrap();
        assert!(response2.status().is_success());

        let (snapshot2_chunks, snapshot2_records) = timeout(
            Duration::from_secs(10),
            collect_output_chunks(&mut response2, all_data_len),
        )
        .await
        .expect("timed out waiting for second connection snapshot");

        let mut all_expected = batches_zset(&initial_data);
        all_expected.add(&batches_zset(&follow_data)).unwrap();

        assert_eq!(snapshot2_records, all_expected);
        assert!(!snapshot2_chunks.is_empty());
        assert!(snapshot2_chunks.iter().all(|chunk| chunk.snapshot));
    })
}

/// Connecting to `/egress?send_snapshot=true` while the pipeline is
/// Paused must still deliver the cached materialized-view snapshot.
///
/// Without the fix, the snapshot stalls indefinitely: registration calls
/// `request_snapshot_delivery`, which short-circuits when the pipeline is
/// not `Running`, so no step fires and `push_output` never runs the
/// `is_snapshot_pending` path. The HTTP client receives only the 3-second
/// keepalive empties and times out.
pub fn http_egress_send_snapshot_while_paused(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let initial_data = vec![for_ids([1, 2]), for_ids([3])];

        let server = start_test_server(
            engine,
            &json!({"name": "test", "inputs": {}, "outputs": {}}),
            Uuid::new_v4(),
        )
        .await;

        // Start, push data, wait for it to land in the materialized view so
        // `trace_snapshots` is populated before we pause.
        start_pipeline(&server).await;

        let CompletionTokenResponse { token } =
            TestHttpSender::send_stream_deserialize_resp::<CompletionTokenResponse>(
                server.post("/ingress/test_input1"),
                &initial_data,
            )
            .await;
        wait_for_completion(&server, &token).await;

        // Pause; subsequent egress request must still see the snapshot.
        // `pause_pipeline` waits until the circuit thread actually stops
        // stepping, so this exercises the truly-paused path.
        pause_pipeline(&server).await;

        let mut response = server
            .post("/egress/test_output1?format=csv&backpressure=true&send_snapshot=true")
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());

        let (snapshot_chunks, snapshot_records) = match timeout(
            Duration::from_secs(10),
            collect_output_chunks(&mut response, initial_data.iter().map(Updates::len).sum()),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                print_stats(&server).await;
                panic!(
                    "timed out waiting for snapshot output while paused: {error:?} \
                 (expected the snapshot to be delivered without resuming)"
                );
            }
        };
        assert_eq!(snapshot_records, batches_zset(&initial_data));
        assert!(
            !snapshot_chunks.is_empty(),
            "expected at least one chunk with payload"
        );
        assert!(
            snapshot_chunks.iter().all(|chunk| chunk.snapshot),
            "expected all data chunks to be marked snapshot=true while paused, got: {:?}",
            snapshot_chunks
                .iter()
                .map(|c| c.snapshot)
                .collect::<Vec<_>>()
        );
    })
}

/// A step that changes nothing in the view still ends with its chunk, so a client can wait for
/// a token whose input changed nothing.
pub fn egress_step_chunk_without_output(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let config_json = json!({"name": "test", "inputs": {}, "outputs": {}});
        let server = start_test_server(engine, &config_json, Uuid::default()).await;
        start_pipeline(&server).await;

        let mut egress = server
            .post("/egress/test_output1?format=csv")
            .send()
            .await
            .unwrap();
        assert!(egress.status().is_success());

        // An insert and a delete of the same record in one transaction leave `test_output1`,
        // the identity of `test_input1`, as it was.
        start_transaction(&server).await;
        let record = r#"{"id": 1, "b": true, "i": 5, "s": "x"}"#;
        let mut response = server
            .post("/ingress/test_input1?format=json&update_format=insert_delete")
            .send_body(format!(
                "{{\"insert\": {record}}}\n{{\"delete\": {record}}}"
            ))
            .await
            .unwrap();
        let status = response.status();
        let body = response.body().await.unwrap();
        assert!(status.is_success(), "{}", String::from_utf8_lossy(&body));
        let CompletionTokenResponse { token } = serde_json::from_slice(&body).unwrap();
        commit_transaction(&server).await;
        wait_for_completion(&server, &token).await;

        let step = get_completion_status(&server, &token).await.step.unwrap();
        let (chunks, records) = timeout(
            Duration::from_secs(10),
            collect_output_chunks_through_step(&mut egress, step),
        )
        .await
        .expect("the chunk that ends the step arrives although the step changed nothing");
        assert!(records.is_empty(), "{records:?}");
        assert!(chunks.iter().all(|chunk| chunk.text_data.is_none()));
    })
}

/// An `/egress` stream marks the end of each step's output with an empty chunk whose
/// `processed_steps` is in the unit of the `step` that `/completion_status` reports, after the
/// step's output; the steps of a transaction close only with the commit, after its output.
pub fn egress_step_chunks(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let config_json = json!({"name": "test", "inputs": {}, "outputs": {}});
        let server = start_test_server(engine, &config_json, Uuid::default()).await;
        start_pipeline(&server).await;

        let mut egress = server
            .post("/egress/test_output1?format=csv")
            .send()
            .await
            .unwrap();
        assert!(egress.status().is_success());

        let data = test_batches(0, 10);
        let token = send_input_no_wait(&server, &data).await;
        wait_for_completion(&server, &token).await;
        let step = get_completion_status(&server, &token).await.step.unwrap();
        let (chunks, records) = collect_output_chunks_through_step(&mut egress, step).await;
        assert_eq!(records, batches_zset(&data));
        assert!(chunks.last().unwrap().text_data.is_none());

        start_transaction(&server).await;
        let data = test_batches(10, 10);
        let token = send_input_no_wait(&server, &data).await;
        commit_transaction(&server).await;
        wait_for_completion(&server, &token).await;
        let step = get_completion_status(&server, &token).await.step.unwrap();
        let (_chunks, records) = collect_output_chunks_through_step(&mut egress, step).await;
        assert_eq!(records, batches_zset(&data));
    })
}

pub fn silent_bootstrap(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        let output_path = tempdir.path().join("output.csv");
        std::fs::create_dir(&storage_dir).unwrap();

        let config_json = json!({
            "name": "test",
            "workers": 4,
            "storage_config": {
                "path": storage_dir,
            },
            "storage": true,
            "clock_resolution_usecs": null,
            "inputs": {},
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "file_output",
                        "config": {
                            "path": output_path,
                        },
                    },
                    "format": {
                        "name": "csv",
                        "config": {},
                    },
                },
            },
        });

        let first_batch = test_batches(0, 10);
        let second_batch = test_batches(10, 10);
        let third_batch = test_batches(20, 10);

        let server = start_test_server_with_options(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[Some("v0")],
        )
        .await;
        start_pipeline(&server).await;
        send_input(&server, &first_batch).await;
        wait_for_file_output(&output_path, &first_batch).await;

        let stats = get_stats(&server).await;
        println!("stats before suspend: {stats:#?}");

        suspend_pipeline(&server, Some(&storage_dir)).await;
        drop(server);

        let server = start_test_server_with_options(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow).with_silent_bootstrap(true),
            &[Some("v1")],
        )
        .await;
        assert_no_file_output(
            &server,
            &output_path,
            batch_num_records(&first_batch),
            batch_num_records(&first_batch),
        )
        .await;
        start_pipeline(&server).await;
        send_input(&server, &second_batch).await;
        wait_for_file_output(&output_path, &second_batch).await;
        suspend_pipeline(&server, Some(&storage_dir)).await;
        drop(server);

        let server = start_test_server_with_options(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow).with_silent_bootstrap(true),
            &[Some("v2")],
        )
        .await;
        let cumulative_records = batch_num_records(&first_batch) + batch_num_records(&second_batch);
        assert_no_file_output(
            &server,
            &output_path,
            cumulative_records,
            cumulative_records,
        )
        .await;
        start_pipeline(&server).await;
        send_input(&server, &third_batch).await;
        wait_for_file_output(&output_path, &third_batch).await;
        suspend_pipeline(&server, Some(&storage_dir)).await;
    })
}

/// Concurrent bootstrap: on restart with a changed view and
/// `concurrent_bootstrap=true`, the new view backfills from the materialized
/// input in the background. Unlike a silent bootstrap, the new view's output
/// IS emitted at cutover, and it reflects the full input state (the
/// backfilled records plus any live records ingested during the backfill).
///
pub fn concurrent_bootstrap(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        let output_path = tempdir.path().join("output.csv");
        std::fs::create_dir(&storage_dir).unwrap();

        let config_json = json!({
            "name": "test",
            "workers": 4,
            "storage_config": {
                "path": storage_dir,
            },
            "storage": true,
            "clock_resolution_usecs": null,
            "inputs": {},
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "file_output",
                        "config": {
                            "path": output_path,
                        },
                    },
                    "format": {
                        "name": "csv",
                        "config": {},
                    },
                },
            },
        });

        let first_batch = test_batches(0, 10);
        let second_batch = test_batches(10, 10);

        // Initial deployment (view `v0`): ingest `first_batch`, then checkpoint.
        let server = start_test_server_with_options(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[Some("v0")],
        )
        .await;
        start_pipeline(&server).await;
        send_input(&server, &first_batch).await;
        wait_for_file_output(&output_path, &first_batch).await;
        suspend_pipeline(&server, Some(&storage_dir)).await;
        drop(server);

        // Restart with a changed view (`v1`) and concurrent bootstrap. The new
        // view backfills from the materialized input; its output is emitted at
        // cutover (not suppressed) and equals the full input state.
        let server = start_test_server_with_options(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow).with_concurrent_bootstrap(true),
            &[Some("v1")],
        )
        .await;
        start_pipeline(&server).await;
        send_input(&server, &second_batch).await;
        // The new view holds the full input state: the backfilled `first_batch`
        // (emitted as its first output, not suppressed) plus the live
        // `second_batch`.
        let all = [first_batch.clone(), second_batch.clone()].concat();
        wait_for_file_output(&output_path, &all).await;
        suspend_pipeline(&server, Some(&storage_dir)).await;
    })
}

/// `total_processed_input_records` must not reach the pipeline's record
/// count while the concurrent-bootstrap cutover still owes the endpoint the
/// re-emission of its view.
///
/// The counter promises that the endpoint's output equals the circuit's
/// output after that many input records, so a reader waiting on it concludes
/// the sink is up to date. During a concurrent bootstrap the pre-existing
/// view stays live, and the empty batches those steps push to the endpoint
/// carry the pipeline's full record count, which advances the counter
/// before the cutover re-emission reaches the transport.
///
/// `send_snapshot` picks the path the endpoint takes to that same promise:
/// the ordinary delta path, or `enqueue_latest_snapshot`, which hands the
/// endpoint the pre-cutover snapshot of a view the bootstrap has yet to
/// rebuild.
async fn concurrent_bootstrap_output_progress_case(
    engine: &dyn PipelineTestEngine,
    send_snapshot: bool,
) {
    ensure_default_crypto_provider();

    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    let output_path = tempdir.path().join("output.csv");
    std::fs::create_dir(&storage_dir).unwrap();

    // A one-record replay chunk stretches the background backfill over many
    // pumps, so the pre-cutover window spans several samples of the
    // endpoint's counters.
    let config_json = json!({
        "name": "test",
        "workers": 4,
        "storage_config": {
            "path": storage_dir,
        },
        "storage": true,
        "clock_resolution_usecs": null,
        "dev_tweaks": {
            "splitter_chunk_size_records": 1,
        },
        "inputs": {},
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "send_snapshot": send_snapshot,
                "transport": {
                    "name": "file_output",
                    "config": {
                        "path": output_path,
                    },
                },
                "format": {
                    "name": "csv",
                    "config": {},
                },
            },
        },
    });

    let first_batch = test_batches(0, 2_000);
    let ingested = batch_num_records(&first_batch);

    let (server, _) = start_test_server_with_state(
        engine,
        &config_json,
        Uuid::new_v4(),
        BootstrapConfig::from(BootstrapPolicy::Allow),
        &[Some("v0")],
        Some(test_program_ir("v0")),
    )
    .await;
    start_pipeline(&server).await;
    send_input(&server, &first_batch).await;
    wait_for_file_output(&output_path, &first_batch).await;
    let (_, transmitted_before, _, _) = output_progress(&server).await;
    assert_eq!(transmitted_before, ingested);
    suspend_pipeline(&server, Some(&storage_dir)).await;
    drop(server);

    // Restart with a changed view and concurrent bootstrap, feeding no new
    // input: every record the endpoint still owes is derived from the
    // `ingested` records the pipeline processed before the restart.
    let (server, _) = start_test_server_with_state(
        engine,
        &config_json,
        Uuid::new_v4(),
        BootstrapConfig::from(BootstrapPolicy::Allow).with_concurrent_bootstrap(true),
        &[Some("v1")],
        Some(test_program_ir("v1")),
    )
    .await;

    // Sample until the endpoint reports the pipeline's full progress. That
    // reading promises the re-emission has reached the transport, so the
    // transmitted count must already include it. Every sample before it
    // must report less than full progress: the endpoint transmits the
    // re-emission and takes its new progress reading in one move, leaving no
    // sample in between.
    let expected_transmitted = 2 * ingested;
    let mut trace = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let sample = output_progress(&server).await;
        trace.push(sample);
        let (processed, transmitted, _queued, _phase) = sample;
        if processed >= ingested {
            assert_eq!(
                transmitted, expected_transmitted,
                "the endpoint reported {processed} processed records with the cutover \
                 re-emission still owed; trace = {trace:?}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the endpoint never caught up with the cutover re-emission; trace = {trace:?}"
        );
    }

    // The re-emission is also on disk: the modified connector re-truncates
    // the file, so it holds the new view's full contents.
    wait_for_file_output(&output_path, &first_batch).await;
}

pub fn concurrent_bootstrap_output_progress_waits_for_cutover(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        concurrent_bootstrap_output_progress_case(engine, false).await;
    })
}

/// The `send_snapshot` variant: `enqueue_latest_snapshot` must not report
/// progress the endpoint has not earned either.
pub fn concurrent_bootstrap_snapshot_progress_waits_for_cutover(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        concurrent_bootstrap_output_progress_case(engine, true).await;
    })
}

/// Regression test: after a concurrent-bootstrap cutover, an ad-hoc query of
/// the backfilled view must observe its full contents without any
/// post-cutover input.
///
/// During the background backfill the new view is excluded from the live
/// schedule, so its queryable integral is never read into the ad-hoc
/// snapshot. Cutover swaps the backfilled integral into the live circuit,
/// but the snapshot is refreshed only by a circuit step (`update_snapshot`
/// runs inside `step`). Without the post-cutover snapshot-refresh step the
/// query keeps reading the empty pre-cutover snapshot and returns 0. This
/// exercises the ad-hoc path specifically (no input is fed after cutover:
/// feeding any would trigger a step and mask the bug).
pub fn concurrent_bootstrap_adhoc_snapshot(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        let output_path = tempdir.path().join("output.csv");
        std::fs::create_dir(&storage_dir).unwrap();

        let config_json = json!({
            "name": "test",
            "workers": 4,
            "storage_config": {
                "path": storage_dir,
            },
            "storage": true,
            "clock_resolution_usecs": null,
            "inputs": {},
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "file_output",
                        "config": {
                            "path": output_path,
                        },
                    },
                    "format": {
                        "name": "csv",
                        "config": {},
                    },
                },
            },
        });

        let first_batch = test_batches(0, 10);

        // Initial deployment (view `v0`): ingest 10 rows, then checkpoint.
        let server = start_test_server_with_options(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[Some("v0")],
        )
        .await;
        start_pipeline(&server).await;
        send_input(&server, &first_batch).await;
        wait_for_file_output(&output_path, &first_batch).await;
        suspend_pipeline(&server, Some(&storage_dir)).await;
        drop(server);

        // Restart with a changed view (`v1`) and concurrent bootstrap. The new
        // view backfills the 10 pre-existing rows. `start_pipeline` waits for
        // `Running`, which is reached only after cutover.
        let server = start_test_server_with_options(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow).with_concurrent_bootstrap(true),
            &[Some("v1")],
        )
        .await;
        // `start_pipeline` waits for `Running`, which a concurrent bootstrap
        // reaches only after cutover AND the post-cutover snapshot-refresh step
        // (the `Finalizing` phase). So once the pipeline is `Running` the ad-hoc
        // snapshot must already hold the 10 backfilled rows, queried once, with
        // no polling and no post-cutover input. This pins the invariant that the
        // pipeline is not reported `Running` until view snapshots are
        // initialized; reporting `Running` at cutover (before the refresh step)
        // would let this query observe the stale empty snapshot and return 0.
        start_pipeline(&server).await;
        let count = adhoc_query_count(&server, "SELECT COUNT(*) AS c FROM test_output1").await;
        assert_eq!(
            count, 10,
            "ad-hoc query observed a stale snapshot after the pipeline reported Running"
        );

        suspend_pipeline(&server, Some(&storage_dir)).await;
    })
}

/// Regression test for the fault-tolerant replay path, the analogue of
/// [concurrent_bootstrap_adhoc_snapshot]: after a restart that replays
/// the journal, an ad-hoc query must observe the replayed state.
///
/// The ad-hoc snapshot is refreshed only by a circuit step (`update_snapshot`
/// inside `step`) and is not part of the checkpoint. If the pipeline reports
/// `Running` (clears `restoring`) as soon as the journal is exhausted, before
/// a step has refreshed the snapshot with the replayed state, an ad-hoc query
/// reads the stale (empty) pre-replay snapshot and returns 0.
///
/// We crash the pipeline without a checkpoint so the restart must replay,
/// then query once right after `Running`, with no polling and no post-restart
/// input (feeding any would trigger a step and mask the bug).
pub fn ft_replay_adhoc_snapshot(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        let output_path = tempdir.path().join("output.csv");
        std::fs::create_dir(&storage_dir).unwrap();

        let config_json = json!({
            "name": "test",
            "workers": 1,
            "storage_config": {
                "path": storage_dir,
            },
            "storage": true,
            "fault_tolerance": "latest_checkpoint",
            "clock_resolution_usecs": null,
            "inputs": {},
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "file_output",
                        "config": {
                            "path": output_path,
                        },
                    },
                    "format": {
                        "name": "csv",
                        "config": {},
                    },
                },
            },
        });

        let batch = test_batches(0, 10);

        // Start the fault-tolerant pipeline and ingest 10 rows. The input step is
        // journaled; `send_input` waits for it to complete.
        let (server, state) = start_test_server_with_state(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[Some("v0")],
            Some(test_program_ir("v0")),
        )
        .await;
        start_pipeline(&server).await;
        send_input(&server, &batch).await;
        wait_for_file_output(&output_path, &batch).await;

        // Crash without a checkpoint: the journaled input survives and must be
        // replayed on restart (the `checkpoint_before = false` case).
        crash_pipeline(&state, &storage_dir).await;
        drop(server);

        // Restart: reopen the initial (step 0) checkpoint and REPLAY the journal.
        // `start_pipeline` waits for `Running`, reached only after `restoring`
        // clears. Once `Running`, the ad-hoc snapshot must already hold the 10
        // replayed rows, queried once, with no polling and no new input.
        let (server, _state) = start_test_server_with_state(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[Some("v0")],
            Some(test_program_ir("v0")),
        )
        .await;
        start_pipeline(&server).await;
        let count = adhoc_query_count(&server, "SELECT COUNT(*) AS c FROM test_output1").await;
        assert_eq!(
            count, 10,
            "ad-hoc query observed a stale snapshot after fault-tolerant replay"
        );

        suspend_pipeline(&server, Some(&storage_dir)).await;
    })
}

/// Reproduces feldera/cloud#1927: a checkpoint that spans a pipeline
/// process restart must not be mistaken for one still in progress.
/// `success: None` looks identical whether a process is still working
/// on the checkpoint or has simply never heard of it; `/checkpoint`
/// hands back an `incarnation_uuid` alongside the sequence number so
/// `/checkpoint_status` can tell the two apart.
pub fn checkpoint_status_detects_restart(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        std::fs::create_dir(&storage_dir).unwrap();

        let config_json = json!({
            "name": "test",
            "workers": 1,
            "storage_config": {
                "path": storage_dir,
            },
            "storage": true,
            "fault_tolerance": "latest_checkpoint",
            "clock_resolution_usecs": null,
            "inputs": {},
            "outputs": {},
        });

        let (server, state) = start_test_server_with_state(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[Some("v0")],
            Some(test_program_ir("v0")),
        )
        .await;
        start_pipeline(&server).await;

        let resp = server
            .post("/checkpoint")
            .send()
            .await
            .unwrap()
            .json::<CheckpointResponse>()
            .await
            .unwrap();
        let seq = resp.checkpoint_sequence_number;
        let stale_incarnation_uuid = resp
            .incarnation_uuid
            .expect("a current-version server always stamps a real incarnation on /checkpoint");

        // Wait for the checkpoint to actually complete before crashing, so
        // the crash below can't race the in-flight checkpoint write.
        let start = Instant::now();
        loop {
            let status = server
                .get("/checkpoint_status")
                .send()
                .await
                .unwrap()
                .json::<CheckpointStatus>()
                .await
                .unwrap();
            if status.success == Some(seq) {
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(20));
            sleep(Duration::from_millis(50));
        }

        // Simulate the pipeline process dying and a new one starting in its
        // place, e.g. a pod eviction; the new process gets a fresh
        // incarnation UUID.
        crash_pipeline(&state, &storage_dir).await;
        drop(server);

        let (server, _state) = start_test_server_with_state(
            engine,
            &config_json,
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[Some("v0")],
            Some(test_program_ir("v0")),
        )
        .await;
        start_pipeline(&server).await;

        // A status check carrying the OLD incarnation UUID must be
        // rejected outright, not answered with an ambiguous status that
        // reads identically to "still in progress".
        let resp = server
            .get(format!(
                "/checkpoint_status?incarnation_uuid={stale_incarnation_uuid}"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Omitting the incarnation UUID preserves the old, ambiguous but
        // backward-compatible behavior for clients that don't send it.
        let status = server
            .get("/checkpoint_status")
            .send()
            .await
            .unwrap()
            .json::<CheckpointStatus>()
            .await
            .unwrap();
        assert_eq!(status.success, None);

        suspend_pipeline(&server, Some(&storage_dir)).await;
    })
}

/// A failed SIGTERM checkpoint must not hang or lose the previous checkpoint.
#[cfg(unix)]
pub fn checkpoint_before_exit(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        use super::{Checkpointer, ControllerError, ServerState, checkpoint_before_exit};
        use dbsp::{circuit::StorageCacheConfig, storage::backend::posixio_impl::PosixBackend};
        use feldera_types::config::FileBackendConfig;
        use std::{os::unix::fs::PermissionsExt, sync::Arc};

        async fn checkpoint(state: &ServerState) -> Result<bool, Arc<ControllerError>> {
            timeout(Duration::from_secs(20), checkpoint_before_exit(state))
                .await
                .expect("the SIGTERM checkpoint must not hang")
        }

        ensure_default_crypto_provider();

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        std::fs::create_dir(&storage_dir).unwrap();
        let checkpoints = || {
            let backend = PosixBackend::new(
                &storage_dir,
                StorageCacheConfig::default(),
                &FileBackendConfig::default(),
            );
            Checkpointer::read_checkpoints(&backend).unwrap()
        };

        let config_json = json!({
            "name": "test",
            "workers": 1,
            "storage_config": {
                "path": storage_dir,
            },
            "storage": true,
            "fault_tolerance": {"model": "exactly_once"},
            "clock_resolution_usecs": null,
            "inputs": {},
            "outputs": {},
        });
        let start = || {
            start_test_server_with_state(
                engine,
                &config_json,
                Uuid::new_v4(),
                BootstrapConfig::from(BootstrapPolicy::Allow),
                &[Some("v0")],
                Some(test_program_ir("v0")),
            )
        };

        let (server, state) = start().await;
        start_pipeline(&server).await;
        send_input(&server, &test_batches(0, 10)).await;
        let before = checkpoints();
        assert!(checkpoint(&state).await.unwrap());
        let saved = checkpoints();
        assert_eq!(saved.len(), before.len() + 1, "no checkpoint written");

        // Root can write to read-only storage, so skip the failure case.
        start_pipeline(&server).await;
        send_input(&server, &test_batches(10, 10)).await;
        let mode = std::fs::Permissions::from_mode;
        std::fs::set_permissions(&storage_dir, mode(0o555)).unwrap();
        if std::fs::write(storage_dir.join("probe"), b"").is_err() {
            let result = checkpoint(&state).await;
            assert!(
                result.is_err(),
                "checkpoint into read-only storage succeeded"
            );
        }
        std::fs::set_permissions(&storage_dir, mode(0o755)).unwrap();

        crash_pipeline(&state, &storage_dir).await;
        drop(server);

        // The journal replays rows 10..20, so only the list shows the checkpoint.
        let (server, _state) = start().await;
        start_pipeline(&server).await;
        assert_eq!(checkpoints(), saved);
        let count = adhoc_query_count(&server, "SELECT COUNT(*) AS c FROM test_output1").await;
        assert_eq!(count, 20);

        suspend_pipeline(&server, Some(&storage_dir)).await;
    })
}

/// Without fault tolerance, SIGTERM must not write a checkpoint: on
/// Kubernetes, Force Stop also sends SIGTERM.
pub fn checkpoint_before_exit_without_ft(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        use super::checkpoint_before_exit;

        ensure_default_crypto_provider();

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        std::fs::create_dir(&storage_dir).unwrap();
        let config_json = json!({
            "name": "test",
            "workers": 1,
            "storage_config": {"path": storage_dir},
            "storage": true,
            "clock_resolution_usecs": null,
            "inputs": {},
            "outputs": {},
        });
        let start = || {
            start_test_server_with_state(
                engine,
                &config_json,
                Uuid::new_v4(),
                BootstrapConfig::from(BootstrapPolicy::Allow),
                &[Some("v0")],
                Some(test_program_ir("v0")),
            )
        };

        let (server, state) = start().await;
        start_pipeline(&server).await;
        send_input(&server, &test_batches(0, 10)).await;
        assert!(!checkpoint_before_exit(&state).await.unwrap());
        crash_pipeline(&state, &storage_dir).await;
        drop(server);

        let (server, _state) = start().await;
        start_pipeline(&server).await;
        let count = adhoc_query_count(&server, "SELECT COUNT(*) AS c FROM test_output1").await;
        assert_eq!(
            count, 0,
            "SIGTERM wrote a checkpoint without fault tolerance"
        );

        suspend_pipeline(&server, Some(&storage_dir)).await;
    })
}

/// SIGTERM must not wait without storage or a circuit.
pub fn checkpoint_before_exit_without_storage(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        use super::{ControllerError, ServerState, checkpoint_before_exit};

        ensure_default_crypto_provider();

        let (server, state) = start_test_server_with_state(
            engine,
            &json!({"name": "test", "inputs": {}, "outputs": {}}),
            Uuid::new_v4(),
            BootstrapConfig::from(BootstrapPolicy::Allow),
            &[None],
            None,
        )
        .await;
        start_pipeline(&server).await;
        let result = timeout(Duration::from_secs(20), checkpoint_before_exit(&state))
            .await
            .expect("the SIGTERM checkpoint must not hang");
        assert!(!result.unwrap());

        let error = ControllerError::io_error("test", std::io::Error::other("test"));
        let state = ServerState::for_error(error, Uuid::new_v4());
        assert!(!checkpoint_before_exit(&state).await.unwrap());
    })
}

/// A real SIGTERM, sent to a pipeline without fault tolerance in a child
/// process, must stop the process with SIGTERM and write no checkpoint.
#[cfg(unix)]
pub fn sigterm_process_without_ft(engine: &dyn PipelineTestEngine) {
    use super::{SERVER_PORT_FILE, ServerArgs, run_server};
    use feldera_types::{config::PipelineConfig, runtime_status::RuntimeDesiredStatus};
    use nix::{sys::signal, unistd::Pid};
    use std::{
        os::unix::process::ExitStatusExt,
        path::Path,
        process::{Child, Command},
    };

    const DIR: &str = "FELDERA_SIGTERM_TEST_DIR";

    // The child process runs the pipeline.
    if let Ok(dir) = std::env::var(DIR) {
        let args = ServerArgs {
            config_file: Path::new(&dir).join("config.json").display().to_string(),
            metadata_file: None,
            bind_address: "127.0.0.1".to_string(),
            default_port: None,
            storage_location: None,
            enable_https: false,
            https_tls_cert_path: None,
            https_tls_key_path: None,
            initial: RuntimeDesiredStatus::Running,
            bootstrap_policy: BootstrapPolicy::Allow,
            silent_bootstrap: false,
            concurrent_bootstrap: false,
            deployment_id: Uuid::new_v4(),
            host_id: None,
        };
        run_server(
            args,
            engine
                .circuit_factory(&identity(&[Some("v0")]), None)
                .unwrap(),
        )
        .unwrap();
        return;
    }

    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    std::fs::create_dir(&storage_dir).unwrap();
    let mut config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 1,
        "storage_config": {"path": storage_dir},
        "storage": true,
        "clock_resolution_usecs": null,
        "inputs": {},
        "outputs": {},
    }))
    .unwrap();
    config.program_ir = Some(test_program_ir("v0"));
    std::fs::write(
        tempdir.path().join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();

    // The test harness names the thread of each test after the test.
    let test_name = thread::current()
        .name()
        .expect("the test thread has a name")
        .to_string();
    // Kills the child process if the test fails.
    struct KillOnDrop(Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let port_file = tempdir.path().join(SERVER_PORT_FILE);
    let start = || -> (KillOnDrop, String) {
        let _ = std::fs::remove_file(&port_file);
        let child = KillOnDrop(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &test_name, "--nocapture"])
                .env(DIR, tempdir.path())
                .current_dir(tempdir.path())
                .spawn()
                .unwrap(),
        );
        let start = Instant::now();
        let port = loop {
            if let Ok(port) = std::fs::read_to_string(&port_file) {
                break port.trim().to_string();
            }
            assert!(start.elapsed() < Duration::from_secs(60), "no port file");
            sleep(Duration::from_millis(100));
        };
        (child, format!("http://127.0.0.1:{port}"))
    };
    let client = reqwest::blocking::Client::new();
    let count = |url: &str| -> Option<i64> {
        let qs = form_urlencoded::Serializer::new(String::new())
            .append_pair("sql", "SELECT COUNT(*) AS c FROM test_output1")
            .append_pair("format", "json")
            .finish();
        let response = client.get(format!("{url}/query?{qs}")).send().ok()?;
        let text = response.error_for_status().ok()?.text().ok()?;
        let row: serde_json::Value = serde_json::from_str(text.lines().next()?).ok()?;
        row["c"].as_i64()
    };
    let wait_for = |what: &str, mut done: Box<dyn FnMut() -> bool + '_>| {
        let start = Instant::now();
        while !done() {
            assert!(start.elapsed() < Duration::from_secs(60), "{what}");
            sleep(Duration::from_millis(100));
        }
    };
    let sigterm = |mut child: KillOnDrop| {
        signal::kill(Pid::from_raw(child.0.id() as i32), signal::SIGTERM).unwrap();
        let start = Instant::now();
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "the pipeline did not exit on SIGTERM"
            );
            sleep(Duration::from_millis(100));
        };
        assert_eq!(status.signal(), Some(signal::SIGTERM as i32), "{status}");
    };

    let (child, url) = start();
    wait_for(
        "the pipeline did not take the input",
        Box::new(|| {
            let rows: String = (0..10).map(|id| format!("{id},false,,\n")).collect();
            client
                .post(format!("{url}/ingress/test_input1"))
                .body(rows)
                .send()
                .is_ok_and(|r| r.status().is_success())
        }),
    );
    wait_for(
        "the input did not reach the view",
        Box::new(|| count(&url) == Some(10)),
    );
    sigterm(child);

    // No checkpoint holds the rows.
    let (child, url) = start();
    let mut restored = None;
    wait_for(
        "the restarted pipeline did not answer",
        Box::new(|| {
            restored = count(&url);
            restored.is_some()
        }),
    );
    sigterm(child);
    assert_eq!(restored, Some(0));
}

pub fn transactions(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = rows::batches(&relation(), "id", 0..=100, 1000)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        // Config string
        let config_json = json!({"name": "test", "inputs": {}, "outputs": {}});
        let server = start_test_server(engine, &config_json, Uuid::default()).await;

        // Start pipeline.
        start_pipeline(&server).await;

        start_transaction(&server).await;

        println!("Connecting to HTTP output endpoint");
        let mut egress_resp = server
            .post("/egress/test_output1?backpressure=true")
            .send()
            .await
            .unwrap();

        let token = send_input_no_wait(&server, &data).await;
        println!("data sent");

        commit_transaction(&server).await;

        // Receive output first, otherwise the token never gets marked as complete.
        TestHttpReceiver::wait_for_output_unordered(&mut egress_resp, &data).await;
        wait_for_completion(&server, &token).await;
    })
}

/// Regression test for issue #6231.
/// In a pipeline without output connectors, the number of completed records,
/// completed steps, and the `pipeline_complete` flag are updated before the current transaction
/// completes.
pub fn transaction_completion_without_output_connectors(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        ensure_default_crypto_provider();

        let data = vec![for_ids([1, 2, 3])];
        let input_records = data.iter().map(Updates::len).sum::<usize>() as u64;

        // Keep the controller free of configured output connectors.  This
        // exercises the no-output path in completion accounting instead of
        // waiting for any egress connector to report progress.
        let config_json = json!({"name": "test", "inputs": {}, "outputs": {}});
        let server = start_test_server(engine, &config_json, Uuid::default()).await;

        start_pipeline(&server).await;

        start_transaction(&server).await;

        // Push input through HTTP ingress while the transaction is open.  The
        // endpoint returns a completion token for exactly the records that this
        // request placed into the pipeline.
        let req = server.post("/ingress/test_input1");
        let CompletionTokenResponse { token } =
            TestHttpSender::send_stream_deserialize_resp::<CompletionTokenResponse>(req, &data)
                .await;
        println!("completion token: {token}");

        // Give the pipeline time to process the input inside the transaction.
        // Before commit, records must not be reported as completed.
        sleep(Duration::from_millis(5000));
        let stats = get_stats(&server).await;
        assert!(!stats.global_metrics.pipeline_complete);
        assert_eq!(stats.global_metrics.total_completed_records, 0);

        let CompletionStatusResponse { status, .. } = get_completion_status(&server, &token).await;
        assert_eq!(status, CompletionStatus::InProgress);

        commit_transaction(&server).await;

        // Wait for the transaction to commit.
        async_wait(
            || async {
                let stats = get_stats(&server).await;
                stats.global_metrics.transaction_status == TransactionStatus::NoTransaction
            },
            20_000,
        )
        .await
        .unwrap();

        async_wait(
            || async {
                let CompletionStatusResponse { status, .. } =
                    get_completion_status(&server, &token).await;
                status == CompletionStatus::Complete
            },
            20_000,
        )
        .await
        .unwrap();

        let stats = get_stats(&server).await;
        assert!(stats.global_metrics.pipeline_complete);
        assert_eq!(stats.global_metrics.total_completed_records, input_records,);
    })
}

/// A pipeline between Kafka topics, with HTTP ingress and egress on the side: paused and
/// started inputs, invalid input, many short-lived egress connections, and input forced
/// into a paused pipeline.
#[cfg(feature = "with-kafka")]
pub fn kafka_server(engine: &dyn PipelineTestEngine) {
    actix_web::rt::System::new().block_on(async {
        use crate::controller::MAX_API_CONNECTIONS;
        use crate::test::kafka::{BufferConsumer, KafkaResources, TestProducer};

        ensure_default_crypto_provider();

        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = rows::batches(&relation(), "id", 0..=100, 1000)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        //let _ = log::set_logger(&TEST_LOGGER);
        //log::set_max_level(LevelFilter::Debug);

        // Create topics.
        let kafka_resources = KafkaResources::create_topics(&[
            ("test_server_input_topic", 1),
            ("test_server_output_topic", 1),
        ]);

        // Create buffer consumer
        let buffer_consumer = BufferConsumer::new(
            engine,
            &relation(),
            "test_server_output_topic",
            "csv",
            json!({}),
            None,
        );

        // Config string
        let config_json = json!({
            "name": "test",
            "inputs": {
                "test_input1": {
                    "stream": "test_input1",
                    "paused": true,
                    "transport": {
                        "name": "kafka_input",
                        "config": {
                            "auto.offset.reset": "earliest",
                            "topics": ["test_server_input_topic"],
                            "log_level": "debug",
                        },
                    },
                    "format": {"name": "csv"},
                },
            },
            "outputs": {
                "test_output2": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "kafka_output",
                        "config": {"topic": "test_server_output_topic"},
                    },
                    "format": {"name": "csv"},
                },
            },
        });
        let server = start_test_server(engine, &config_json, Uuid::new_v4()).await;

        // Write data to Kafka.
        println!("Send test data");
        let producer = TestProducer::new();
        producer.send_to_topic(&csv_messages(&data), "test_server_input_topic");

        sleep(Duration::from_millis(2000));
        assert!(buffer_consumer.is_empty());

        // Start pipeline.
        start_pipeline(&server).await;

        sleep(Duration::from_millis(3000));

        // Unpause input endpoint.
        start_input_endpoint(&server, "test_input1").await;

        // Wait for data.
        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("/stats");
        get_stats(&server).await;

        println!("/metadata");
        let resp = server.get("/metadata").send().await.unwrap();
        assert!(resp.status().is_success());

        // Pause input endpoint.
        pause_input_endpoint(&server, "test_input1").await;

        // Pause pipeline.
        pause_pipeline(&server).await;
        sleep(Duration::from_millis(1000));

        // Send more data, receive none
        producer.send_to_topic(&csv_messages(&data), "test_server_input_topic");
        sleep(Duration::from_millis(2000));
        assert_eq!(buffer_consumer.len(), 0);

        // Start pipeline; still no data because the endpoint is paused.
        start_pipeline(&server).await;

        sleep(Duration::from_millis(2000));
        assert_eq!(buffer_consumer.len(), 0);

        // Resume input endpoint, receive data.
        start_input_endpoint(&server, "test_input1").await;

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("Testing invalid input");
        producer.send_string("invalid\n", "test_server_input_topic");
        loop {
            let stats = get_stats(&server).await;
            // println!("stats: {stats:#}");
            let num_errors = stats.inputs[0].metrics.num_parse_errors;
            if num_errors == 1 {
                break;
            }
        }

        // Make sure that HTTP connections get dropped on client disconnect
        // (see comment in `HttpOutputEndpoint::request`).  We create 2x the
        // number of supported simultaneous API connections and drop the client
        // side instantly, which should cause the server side to close within
        // 6 seconds.  If everything works as intended, this should _not_
        // trigger the API connection limit error.
        for _ in 0..2 * MAX_API_CONNECTIONS {
            assert!(
                server
                    .post("/egress/test_output1")
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .is_success()
            );
            sleep(Duration::from_millis(150));
        }

        println!("Connecting to HTTP output endpoint");
        let mut resp1 = server
            .post("/egress/test_output1?backpressure=true")
            .send()
            .await
            .unwrap();

        let mut resp2 = server
            .post("/egress/test_output1?backpressure=true")
            .send()
            .await
            .unwrap();

        println!("Streaming test");
        let token = send_input_no_wait(&server, &data).await;
        println!("data sent");

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        TestHttpReceiver::wait_for_output_unordered(&mut resp1, &data).await;
        TestHttpReceiver::wait_for_output_unordered(&mut resp2, &data).await;
        wait_for_completion(&server, &token).await;

        // Force-push data in paused state.
        pause_pipeline(&server).await;
        sleep(Duration::from_millis(1000));

        println!("Force-push data via HTTP");
        let req = server.post("/ingress/test_input1?force=true");

        let CompletionTokenResponse { token } =
            TestHttpSender::send_stream_deserialize_resp::<CompletionTokenResponse>(req, &data)
                .await;
        println!("completion token: {token}");

        tokio::join!(
            // Wait for completion.
            async {
                async_wait(
                    || async {
                        print_stats(&server).await;

                        let status = get_completion_status(&server, &token).await.status;
                        println!("completion status: {status:?}");

                        // println!("stats {}", stats.to_str_lossy());
                        status == CompletionStatus::Complete
                    },
                    20_000,
                )
                .await
                .unwrap()
            },
            // In parallel, run the HTTP client to receive outputs from the pipeline, otherwise the
            // HTTP output connector can get stuck, and the /completion_status check above will timeout.
            async {
                TestHttpReceiver::wait_for_output_unordered(&mut resp1, &data).await;
                TestHttpReceiver::wait_for_output_unordered(&mut resp2, &data).await;
            }
        );

        drop(resp1);
        drop(resp2);

        // Even though we checked completion status of the token, it only means that the connector
        // has sent data to Kafka, not that it has been received by the consumer.
        buffer_consumer.wait_for_output_unordered(&data);
        print_stats(&server).await;

        buffer_consumer.clear();

        start_pipeline(&server).await;

        sleep(Duration::from_millis(5000));

        pause_pipeline(&server).await;
        sleep(Duration::from_millis(1000));

        drop(buffer_consumer);
        drop(kafka_resources);
    })
}
