//! Tests of the exactly-once Kafka sink's commit loop against a librdkafka
//! mock cluster, which supports the transaction API, can take its broker
//! down and up, delay its responses, and answer a request with an error.
//!
//! The mock broker does not apply commit and abort visibility to consumers,
//! so these tests check what reached the broker, not isolation. The tests in
//! `real` below checks recovery and visibility against the Kafka test broker.

use super::*;
use crate::transport::kafka::nonft::test::RecordingCallbacks;
use feldera_adapterlib::test::wait;
use feldera_types::adapter_stats::ConnectorHealthStatus;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use serde_json::json;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

mod proxy;
mod real;

const TOPIC: &str = "ft_test_topic";

/// A connected sink on `cluster` with a one second transaction timeout, so
/// that a delivery failure turns into an abortable transaction quickly.
fn endpoint(
    cluster: &MockCluster<'_, DefaultProducerContext>,
) -> (
    KafkaOutputEndpoint,
    Arc<RecordingCallbacks>,
    CancellationToken,
) {
    let config: KafkaOutputConfig = serde_json::from_value(json!({
        "topic": TOPIC,
        "bootstrap.servers": cluster.bootstrap_servers(),
        "fault_tolerance": {
            "producer_options": {
                "transaction.timeout.ms": "1000",
            }
        }
    }))
    .unwrap();
    let shutdown = CancellationToken::new();
    let mut endpoint = KafkaOutputEndpoint::new(config, shutdown.clone(), None).unwrap();
    let callbacks = Arc::new(RecordingCallbacks::default());
    endpoint.connect(callbacks.clone()).unwrap();
    (endpoint, callbacks, shutdown)
}

fn cluster() -> MockCluster<'static, DefaultProducerContext> {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic(TOPIC, 1, 1).unwrap();
    cluster
}

/// Reads through the broker's current high watermark, including any records
/// beyond the expected count. With read_committed, aborted records and control
/// records are filtered by Kafka; the consumer still advances past them.
fn read_records(
    bootstrap: &str,
    topic: &str,
    partition_count: i32,
    isolation: &str,
) -> Vec<SpooledRecord> {
    use rdkafka::message::Headers;
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "verify-output")
        .set("enable.partition.eof", "true")
        .set("isolation.level", isolation)
        .create()
        .unwrap();
    let mut received = Vec::new();
    for partition in 0..partition_count {
        let (low, high) = consumer
            .fetch_watermarks(topic, partition, Duration::from_secs(10))
            .unwrap();
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(topic, partition, Offset::Offset(low))
            .unwrap();
        consumer.assign(&assignment).unwrap();
        wait(
            || {
                match consumer.poll(Duration::from_millis(100)) {
                    Some(Ok(message)) => received.push(SpooledRecord {
                        partition: message.partition(),
                        key: message.key().map(<[u8]>::to_vec),
                        payload: message.payload().map(<[u8]>::to_vec),
                        headers: message
                            .headers()
                            .map(|headers| {
                                headers
                                    .iter()
                                    .map(|h| (h.key.to_owned(), h.value.map(<[u8]>::to_vec)))
                                    .collect()
                            })
                            .unwrap_or_default(),
                    }),
                    Some(Err(KafkaError::PartitionEOF(_))) => {
                        return consumer
                            .position()
                            .unwrap()
                            .find_partition(topic, partition)
                            .unwrap()
                            .offset()
                            == Offset::Offset(high);
                    }
                    Some(Err(error)) => panic!("reading {topic}: {error}"),
                    None => {}
                }
                false
            },
            60_000,
        )
        .expect("the consumer must reach the high watermark");
    }
    received
}

fn read_topic(cluster: &MockCluster<'_, DefaultProducerContext>) -> Vec<(OutputPosition, String)> {
    read_records(&cluster.bootstrap_servers(), TOPIC, 1, "read_uncommitted")
        .into_iter()
        .map(|r| {
            (
                serde_json::from_slice(r.key.as_ref().unwrap()).unwrap(),
                String::from_utf8(r.payload.unwrap()).unwrap(),
            )
        })
        .collect()
}

fn statuses(callbacks: &RecordingCallbacks) -> Vec<ConnectorHealthStatus> {
    callbacks
        .health_history()
        .into_iter()
        .map(|health| health.status)
        .collect()
}

/// Runs one call on the sink in a thread, since a call that recovers or
/// holds does not return on its own. Cancels and joins the thread even when
/// an assertion fails.
struct BatchWorker {
    shutdown: CancellationToken,
    finished: mpsc::Receiver<()>,
    handle: Option<thread::JoinHandle<(AnyResult<()>, KafkaOutputEndpoint)>>,
}

impl BatchWorker {
    /// Runs `batch_end`.
    fn new(endpoint: KafkaOutputEndpoint) -> Self {
        Self::run(endpoint, |endpoint| endpoint.batch_end())
    }

    fn run(
        mut endpoint: KafkaOutputEndpoint,
        call: impl FnOnce(&mut KafkaOutputEndpoint) -> AnyResult<()> + Send + 'static,
    ) -> Self {
        let shutdown = endpoint.shutdown.clone();
        let (done, finished) = mpsc::channel();
        let handle = thread::spawn(move || {
            let result = call(&mut endpoint);
            let _ = done.send(());
            (result, endpoint)
        });
        Self {
            shutdown,
            finished,
            handle: Some(handle),
        }
    }

    fn finish(mut self) -> (AnyResult<()>, KafkaOutputEndpoint) {
        self.finished
            .recv_timeout(Duration::from_secs(60))
            .expect("batch_end must finish");
        self.handle.take().unwrap().join().unwrap()
    }
}

impl Drop for BatchWorker {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.shutdown.cancel();
            let result = handle.join();
            if !thread::panicking() {
                let _ = result.unwrap();
            }
        }
    }
}

/// A broker outage during a transaction breaks its deliveries. The sink
/// aborts the transaction once the broker is back, sends every record again
/// and commits, so the broker holds each record exactly once.
#[test]
fn an_outage_aborts_and_replays_the_transaction() {
    let cluster = cluster();
    let (mut endpoint, callbacks, _shutdown) = endpoint(&cluster);

    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    cluster.broker_down(1).unwrap();
    for i in 0..4 {
        endpoint
            .push_buffer(format!("message {i}").as_bytes())
            .unwrap();
    }
    // The one second transaction timeout expires the deliveries, which puts
    // the transaction in an error state. librdkafka still accepts sends in
    // that state; the commit reports it.
    wait(
        || callbacks.metric("kafka_output_delivery_errors_total") >= 1.0,
        30_000,
    )
    .expect("the deliveries must time out while the broker is down");
    endpoint.push_buffer(b"message 4").unwrap();
    assert!(
        callbacks.errors().is_empty(),
        "a delivery failure that the replay repairs is not an output error: {:?}",
        callbacks.errors()
    );

    // `batch_end` recovers once the broker is back. Bring it back after the
    // unhealthy report, so the commit loop waited on it.
    let worker = BatchWorker::new(endpoint);
    wait(
        || {
            matches!(
                callbacks.health_status(),
                Some(ConnectorHealthStatus::Unhealthy)
            )
        },
        30_000,
    )
    .expect("the sink must report itself unhealthy while the broker is down");
    thread::sleep(Duration::from_millis(500));
    cluster.broker_up(1).unwrap();
    let (result, mut endpoint) = worker.finish();
    result.expect("the commit loop must succeed once the broker is back");

    assert!(callbacks.metric("kafka_output_transaction_aborts_total") >= 1.0);
    assert!(matches!(
        callbacks.health_status(),
        Some(ConnectorHealthStatus::Healthy)
    ));
    // Timed out deliveries are an outage, not a refusal to report.
    assert!(
        callbacks
            .errors()
            .iter()
            .all(|(fatal, tag, _)| !fatal && *tag != Some("kafka_ft_replay")),
        "{:?}",
        callbacks.errors()
    );
    assert_eq!(endpoint.memory(), 0, "the spool must be released at commit");
    assert_eq!(callbacks.metric("kafka_output_replay_spool_bytes"), 0.0);

    let received = read_topic(&cluster);
    assert_eq!(
        received,
        (0..5)
            .map(|i| (
                OutputPosition {
                    transaction: 0,
                    substep: i
                },
                format!("message {i}")
            ))
            .collect::<Vec<_>>()
    );

    // The next transaction works as usual.
    endpoint.batch_start(1, OutputBatchType::Delta).unwrap();
    endpoint.push_buffer(b"after recovery").unwrap();
    endpoint.batch_end().unwrap();
    assert_eq!(read_topic(&cluster).len(), 6);
}

/// A commit whose response is late times out with a retriable error. The
/// sink repeats the commit, which completes without a replay.
#[test]
fn a_late_commit_response_is_retried_without_replay() {
    let cluster = cluster();
    let (mut endpoint, callbacks, _shutdown) = endpoint(&cluster);

    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    endpoint.push_buffer(b"slow commit").unwrap();
    wait(
        || callbacks.metric("kafka_output_delivered_messages_total") == 1.0,
        30_000,
    )
    .unwrap();
    // A round trip longer than the API timeout makes the first commit call
    // time out. The broker answers at its normal speed again once the sink
    // has reported the retry, so the repeated call completes.
    cluster
        .broker_round_trip_time(1, TRANSACTION_API_TIMEOUT + Duration::from_secs(2))
        .unwrap();
    let start = Instant::now();
    let worker = BatchWorker::new(endpoint);
    wait(
        || {
            matches!(
                callbacks.health_status(),
                Some(ConnectorHealthStatus::Unhealthy)
            )
        },
        30_000,
    )
    .expect("the timed-out commit must make the sink unhealthy");
    assert!(
        start.elapsed() >= TRANSACTION_API_TIMEOUT,
        "the first commit call must have timed out"
    );
    cluster.broker_round_trip_time(1, Duration::ZERO).unwrap();
    let (result, _endpoint) = worker.finish();
    result.expect("the repeated commit must succeed");
    assert!(callbacks.metric("kafka_output_commit_retries_total") >= 1.0);
    assert_eq!(
        callbacks.metric("kafka_output_transaction_aborts_total"),
        0.0
    );
    assert!(
        matches!(
            statuses(&callbacks)[..],
            [
                ConnectorHealthStatus::Unhealthy,
                ConnectorHealthStatus::Healthy
            ]
        ),
        "{:?}",
        statuses(&callbacks)
    );
    assert_eq!(read_topic(&cluster).len(), 1);
}

/// A fenced producer is unusable. The sink reports a fatal error, turns
/// unhealthy, and holds the output thread until the pipeline shuts down
/// instead of dropping later output.
#[test]
fn a_fenced_producer_holds_the_sink_until_shutdown() {
    let cluster = cluster();
    let (mut endpoint, callbacks, shutdown) = endpoint(&cluster);

    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    endpoint.push_buffer(b"fenced").unwrap();
    cluster.request_errors(
        RDKafkaApiKey::EndTxn,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_PRODUCER_FENCED],
    );

    let worker = BatchWorker::new(endpoint);
    wait(
        || {
            callbacks.errors().iter().any(|(fatal, ..)| *fatal)
                && matches!(
                    callbacks.health_status(),
                    Some(ConnectorHealthStatus::Unhealthy)
                )
        },
        30_000,
    )
    .expect("the fence must be reported as a fatal error");
    assert!(matches!(
        callbacks.health_status(),
        Some(ConnectorHealthStatus::Unhealthy)
    ));
    assert!(
        worker
            .finished
            .recv_timeout(Duration::from_secs(2))
            .is_err(),
        "the sink must hold the output thread after a fatal error"
    );
    shutdown.cancel();
    let (result, _endpoint) = worker.finish();
    let error = result.expect_err("a fatal error must end the batch with an error");
    assert!(error.to_string().contains("fenced"), "{error}");
    let errors = callbacks.errors();
    let (fatal, tag, text) = errors.iter().find(|(fatal, ..)| *fatal).unwrap();
    assert!(*fatal);
    assert_eq!(*tag, Some("kafka_ft_fatal"));
    assert!(text.contains(TOPIC), "{text}");
}

/// Shutdown ends a commit loop that waits on a down broker within one
/// transaction API timeout.
#[test]
fn shutdown_ends_a_stalled_commit() {
    let cluster = cluster();
    let (mut endpoint, callbacks, shutdown) = endpoint(&cluster);

    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    endpoint.push_buffer(b"never committed").unwrap();
    cluster.broker_down(1).unwrap();

    let start = Instant::now();
    let worker = BatchWorker::new(endpoint);
    thread::sleep(Duration::from_secs(1));
    shutdown.cancel();
    let (result, _endpoint) = worker.finish();
    let elapsed = start.elapsed();
    let error = result.expect_err("a shutdown must end the commit loop with an error");
    assert!(error.to_string().contains("shutting down"), "{error}");
    // One timed-out call plus the poll interval; the margin is for a loaded
    // CI runner.
    assert!(
        elapsed < 2 * TRANSACTION_API_TIMEOUT,
        "shutdown took {elapsed:?}"
    );
    assert!(
        callbacks.errors().iter().all(|(fatal, ..)| !fatal),
        "{:?}",
        callbacks.errors()
    );
}

#[test]
fn an_unclassified_error_retains_the_batch_until_shutdown() {
    let cluster = cluster();
    let (mut endpoint, callbacks, shutdown) = endpoint(&cluster);
    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    endpoint.push_buffer(b"kept for recovery").unwrap();
    // Force an unclassified State error at commit, without setting the native
    // fatal or abortable flags. It must follow the same policy as a fatal error.
    endpoint
        .kafka_producer
        .abort_transaction(TRANSACTION_API_TIMEOUT)
        .unwrap();
    let worker = BatchWorker::new(endpoint);
    wait(
        || callbacks.errors().iter().any(|(fatal, ..)| *fatal),
        30_000,
    )
    .unwrap();
    assert!(
        worker
            .finished
            .recv_timeout(Duration::from_millis(200))
            .is_err()
    );
    shutdown.cancel();
    let (result, endpoint) = worker.finish();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("outcome is unknown")
    );
    assert_eq!(endpoint.spool.len(), 1);
    assert_eq!(endpoint.next_transaction, 0);
}

/// A record appended after the transaction broke is first sent by the
/// replay. When that send refuses it, the sink reports it once, even when a
/// second abort replays the spool again.
#[test]
fn replay_reports_a_record_first_refused_after_an_abort() {
    let cluster = cluster();
    let (mut endpoint, callbacks, _shutdown) = endpoint(&cluster);
    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    endpoint.push_buffer(b"first").unwrap();
    // While `needs_abort` is set, as after a send that found the transaction
    // in an error state, records go to the spool unsent. Clear it again so
    // that `batch_end` commits, and the replays come from the commit errors.
    endpoint.needs_abort = true;
    endpoint
        .push_buffer(&vec![b'x'; DEFAULT_MAX_MESSAGE_SIZE + 1])
        .unwrap();
    endpoint.push_buffer(b"last").unwrap();
    endpoint.needs_abort = false;
    assert_eq!(endpoint.spool.len(), 3);
    wait(
        || callbacks.metric("kafka_output_delivered_messages_total") == 1.0,
        30_000,
    )
    .unwrap();
    // The first commit fails with an abortable error, and the produce of the
    // first replay fails, so the spool is replayed twice. (An abort is an
    // EndTxn request too, so a second EndTxn error would hit the abort.)
    cluster.request_errors(
        RDKafkaApiKey::EndTxn,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
    );
    cluster.request_errors(
        RDKafkaApiKey::Produce,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS],
    );
    let (result, endpoint) = BatchWorker::new(endpoint).finish();
    result.unwrap();
    assert!(
        callbacks.metric("kafka_output_transaction_aborts_total") >= 2.0,
        "aborts {} errors {:?}",
        callbacks.metric("kafka_output_transaction_aborts_total"),
        callbacks.errors()
    );
    let errors = callbacks.errors();
    let replay_errors: Vec<_> = errors
        .iter()
        .filter(|(_, tag, _)| *tag == Some("kafka_ft_replay"))
        .collect();
    let [(_, _, error)] = replay_errors[..] else {
        panic!("one replay error expected, got {replay_errors:?}");
    };
    assert!(error.contains("being dropped"), "{error}");
    assert!(error.contains(TOPIC), "{error}");
    assert!(endpoint.spool.is_empty());
    // The mock broker keeps the aborted attempts visible. The committed
    // attempt is the last one, and no attempt carried the refused record.
    let payloads: Vec<_> = read_topic(&cluster)
        .into_iter()
        .map(|(_, payload)| payload)
        .collect();
    assert!(
        payloads.ends_with(&["first".into(), "last".into()]),
        "{payloads:?}"
    );
    assert!(payloads.iter().all(|p| p.len() < 100), "{payloads:?}");
}

/// A produce error about the topic rather than the record, which librdkafka
/// keeps answering from its cache for a while, does not drop the record: the
/// sink aborts and replays until the topic takes records again.
#[test]
fn a_topic_error_during_replay_is_retried_not_dropped() {
    let cluster = cluster();
    let (mut endpoint, callbacks, _shutdown) = endpoint(&cluster);
    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    cluster.request_errors(
        RDKafkaApiKey::Produce,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
    );
    endpoint.push_buffer(b"first").unwrap();
    wait(
        || callbacks.metric("kafka_output_delivery_errors_total") > 0.0,
        30_000,
    )
    .unwrap();
    // librdkafka now refuses the topic from its cache, so this send and the
    // replays that follow are refused until the cache clears.
    endpoint.push_buffer(b"second").unwrap();
    assert!(endpoint.needs_abort);
    assert_eq!(endpoint.spool.len(), 2);
    let (result, endpoint) = BatchWorker::new(endpoint).finish();
    result.unwrap();
    assert!(
        !callbacks
            .errors()
            .iter()
            .any(|(.., e)| e.contains("being dropped")),
        "no record is dropped: {:?}",
        callbacks.errors()
    );
    assert!(callbacks.metric("kafka_output_transaction_aborts_total") >= 1.0);
    assert!(endpoint.spool.is_empty());
    assert_eq!(
        read_topic(&cluster)
            .into_iter()
            .map(|(_, payload)| payload)
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
}

/// A record that the first send refuses is reported to the caller there and
/// is not kept, so a later replay does not report it again.
#[test]
fn a_record_refused_on_first_send_is_not_reported_again_on_replay() {
    let cluster = cluster();
    let (mut endpoint, callbacks, _shutdown) = endpoint(&cluster);
    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    let error = endpoint
        .push_buffer(&vec![b'x'; DEFAULT_MAX_MESSAGE_SIZE + 1])
        .unwrap_err();
    assert!(error.to_string().contains(TOPIC), "{error}");
    assert_eq!(endpoint.spool.len(), 0, "a refused record is not kept");
    cluster.request_errors(
        RDKafkaApiKey::Produce,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
    );
    endpoint.push_buffer(b"forces an abort").unwrap();
    wait(
        || callbacks.metric("kafka_output_delivery_errors_total") > 0.0,
        30_000,
    )
    .unwrap();
    let (result, _endpoint) = BatchWorker::new(endpoint).finish();
    result.unwrap();
    assert!(callbacks.metric("kafka_output_transaction_aborts_total") >= 1.0);
    assert!(
        !callbacks
            .errors()
            .iter()
            .any(|(.., e)| e.contains("being dropped")),
        "the replay must not report the refused record: {:?}",
        callbacks.errors()
    );
    assert_eq!(
        read_topic(&cluster)
            .into_iter()
            .map(|(_, payload)| payload)
            .collect::<Vec<_>>(),
        ["forces an abort"]
    );
}

/// The broker refuses a batch, as it does when the topic's `max.message.bytes`
/// is below the connector's `message.max.bytes`. librdkafka fails every record
/// of the batch with that error, so the sink cannot single out a record: it
/// aborts and replays, and once the refusals have gone on for a while it
/// reports a connector error once, dropping nothing. The transaction commits
/// when the broker takes the batch.
#[test]
fn a_broker_refusal_is_reported_once_and_drops_nothing() {
    // One record, so that each replay is one produce request and consumes one
    // injected error. The backoff doubles from 100 ms, so the streak reaches
    // the report delay at the seventh abort.
    const REFUSALS: usize = 7;
    let cluster = cluster();
    let (mut endpoint, callbacks, _shutdown) = endpoint(&cluster);
    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    cluster.request_errors(
        RDKafkaApiKey::Produce,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE; REFUSALS],
    );
    endpoint.push_buffer(b"refused by the broker").unwrap();
    wait(
        || callbacks.metric("kafka_output_delivery_errors_total") > 0.0,
        30_000,
    )
    .unwrap();
    let (result, endpoint) = BatchWorker::new(endpoint).finish();
    result.unwrap();
    // librdkafka can answer one more produce from its cached topic error.
    let aborts = callbacks.metric("kafka_output_transaction_aborts_total");
    assert!(
        (REFUSALS as f64..=REFUSALS as f64 + 1.0).contains(&aborts),
        "{aborts}"
    );
    let errors = callbacks.errors();
    let [(fatal, tag, error)] = &errors[..] else {
        panic!("one refusal report expected, got {errors:?}");
    };
    assert!(!fatal);
    assert_eq!(*tag, Some("kafka_ft_replay"));
    assert!(
        error.contains("aborted") && error.contains(TOPIC),
        "{error}"
    );
    assert!(error.contains("max.message.bytes"), "{error}");
    assert!(error.contains("no record is dropped"), "{error}");
    assert!(endpoint.spool.is_empty());
    assert!(matches!(
        callbacks.health_status(),
        Some(ConnectorHealthStatus::Healthy)
    ));
    // The mock broker keeps the refused attempts visible; the committed
    // attempt is the last one.
    let payloads: Vec<_> = read_topic(&cluster)
        .into_iter()
        .map(|(_, payload)| payload)
        .collect();
    assert!(
        !payloads.is_empty() && payloads.iter().all(|p| p == "refused by the broker"),
        "{payloads:?}"
    );
}

/// A spool that cannot take a record is a fatal error: the sink holds the
/// output thread, and the transaction is not committed.
#[test]
fn a_spool_write_failure_holds_the_sink_without_a_commit() {
    let cluster = cluster();
    let (mut endpoint, callbacks, shutdown) = endpoint(&cluster);
    let missing = std::env::temp_dir().join(format!("missing-spool-dir-{}", std::process::id()));
    endpoint.spool = ReplaySpool::new(1, Some(missing));
    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();

    let worker = BatchWorker::run(endpoint, |endpoint| endpoint.push_buffer(b"not kept"));
    wait(
        || callbacks.errors().iter().any(|(fatal, ..)| *fatal),
        30_000,
    )
    .expect("the spool failure must be reported as a fatal error");
    assert!(
        worker
            .finished
            .recv_timeout(Duration::from_millis(500))
            .is_err(),
        "the sink must hold the output thread"
    );
    shutdown.cancel();
    let (result, endpoint) = worker.finish();
    let error = result.unwrap_err().to_string();
    assert!(error.contains("cannot keep output for replay"), "{error}");
    assert!(error.contains("spool directory"), "{error}");
    assert_eq!(endpoint.next_transaction, 0, "nothing was committed");
    assert!(matches!(
        callbacks.health_status(),
        Some(ConnectorHealthStatus::Unhealthy)
    ));
}

/// A begin that finds the producer in an abortable state aborts, begins
/// again, and stops there: it neither replays nor commits the new
/// transaction, which the batch that follows owns.
#[test]
fn an_abortable_error_at_begin_leaves_a_fresh_transaction_open() {
    let cluster = cluster();
    let (mut endpoint, callbacks, _shutdown) = endpoint(&cluster);
    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    cluster.request_errors(
        RDKafkaApiKey::Produce,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
    );
    endpoint.push_buffer(b"breaks the transaction").unwrap();
    wait(
        || callbacks.metric("kafka_output_delivery_errors_total") > 0.0,
        30_000,
    )
    .unwrap();

    let worker = BatchWorker::run(endpoint, |endpoint| {
        endpoint.drive(1, Phase::Begin { replay: false })
    });
    let (result, endpoint) = worker.finish();
    result.expect("the begin must recover from the abortable state");
    assert!(callbacks.metric("kafka_output_transaction_aborts_total") >= 1.0);
    // The new transaction is open and empty: a commit succeeds and delivers
    // nothing. Without a transaction open, this call fails with a state
    // error; after a replay, the commit delivers the spooled record.
    endpoint
        .kafka_producer
        .commit_transaction(TRANSACTION_API_TIMEOUT)
        .expect("a transaction must be open after the begin");
    assert_eq!(
        callbacks.metric("kafka_output_delivered_messages_total"),
        0.0,
        "the begin must not replay"
    );
}

#[test]
fn cancellation_stops_replay_with_room_in_the_queue() {
    for limit in [SPOOL_MEMORY_LIMIT, 1] {
        let cluster = cluster();
        let (mut endpoint, _callbacks, shutdown) = endpoint(&cluster);
        endpoint.spool = ReplaySpool::new(limit, None);
        endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
        endpoint.push_buffer(b"kept for replay").unwrap();
        endpoint
            .kafka_producer
            .abort_transaction(TRANSACTION_API_TIMEOUT)
            .unwrap();
        endpoint.kafka_producer.begin_transaction().unwrap();
        shutdown.cancel();
        let start = Instant::now();
        assert!(
            endpoint
                .replay()
                .unwrap_err()
                .to_string()
                .contains("shutting down")
        );
        assert!(start.elapsed() < Duration::from_secs(1));
        assert_eq!(endpoint.spool.len(), 1);
    }
}
