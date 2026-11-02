//! Recovery tests using the normal Kafka test service and a per-test proxy.
//! The proxy changes connectivity and response timing; the real broker decides
//! transaction outcomes and read_committed visibility.

use super::proxy::BrokerProxy;
use super::*;
use crate::test::kafka::KafkaResources;
use feldera_types::transport::kafka::default_redpanda_server;

fn endpoint(
    topic: &str,
    proxy: &BrokerProxy,
    transaction_timeout_ms: u64,
    message_timeout_ms: u64,
) -> (KafkaOutputEndpoint, Arc<RecordingCallbacks>) {
    let config = serde_json::from_value(json!({
        "topic": topic,
        "bootstrap.servers": proxy.address,
        "initialization_timeout": "30s",
        "fault_tolerance": {
            "producer_options": {
                "transaction.timeout.ms": transaction_timeout_ms.to_string(),
                "message.timeout.ms": message_timeout_ms.to_string(),
            }
        }
    }))
    .unwrap();
    let mut endpoint = KafkaOutputEndpoint::new(config, CancellationToken::new(), None).unwrap();
    let callbacks = Arc::new(RecordingCallbacks::default());
    endpoint.connect(callbacks.clone()).unwrap();
    (endpoint, callbacks)
}

fn committed(topic: &str, partitions: i32) -> Vec<SpooledRecord> {
    read_records(
        &default_redpanda_server(),
        topic,
        partitions,
        "read_committed",
    )
}

#[test]
fn an_outage_replays_a_spilled_transaction_exactly_once() {
    let topic = "ft_recovery_spilled_outage";
    let _topics = KafkaResources::create_topics(&[(topic, 3)]);
    let proxy = BrokerProxy::new(default_redpanda_server());
    let (mut endpoint, callbacks) = endpoint(topic, &proxy, 60_000, 1000);
    // Exercise the endpoint's disk path without making the test allocate 64 MiB.
    endpoint.spool = ReplaySpool::new(1024, None);
    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    for _ in 0..3 {
        endpoint
            .push_key(None, Some(&[b'a'; 1024]), &[("header", Some(b"value"))])
            .unwrap();
    }
    wait(
        || callbacks.metric("kafka_output_delivered_messages_total") == 3.0,
        30_000,
    )
    .unwrap();
    proxy.disconnect();
    let outage = Instant::now();
    endpoint.push_key(None, None, &[("null", None)]).unwrap();
    endpoint.push_buffer(b"after disconnect").unwrap();
    wait(
        || {
            outage.elapsed() >= Duration::from_secs(3)
                && callbacks.metric("kafka_output_delivery_errors_total") > 0.0
        },
        30_000,
    )
    .unwrap();
    assert!(endpoint.spool.is_spilled());
    let mut expected = Vec::new();
    endpoint
        .spool
        .replay(|_, record| {
            expected.push(record.clone());
            Ok(())
        })
        .unwrap();
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
    .unwrap();
    proxy.reconnect();
    wait(
        || {
            worker.handle.as_ref().unwrap().is_finished()
                || callbacks.errors().iter().any(|(fatal, ..)| *fatal)
        },
        30_000,
    )
    .unwrap_or_else(|_| {
        panic!(
            "recovery stalled: {:?}; {:?}",
            callbacks.errors(),
            callbacks.health_history()
        )
    });
    assert!(
        !callbacks.errors().iter().any(|(fatal, ..)| *fatal),
        "{:?}",
        callbacks.errors()
    );
    let (result, mut endpoint) = worker.finish();
    result.unwrap();
    assert!(callbacks.metric("kafka_output_transaction_aborts_total") >= 1.0);
    assert!(matches!(
        callbacks.health_status(),
        Some(ConnectorHealthStatus::Healthy)
    ));
    assert!(endpoint.spool.is_empty());
    assert!(!endpoint.spool.is_spilled());
    expected.sort_by_key(|r| (r.partition, r.key.clone()));
    let mut actual = committed(topic, 3);
    actual.sort_by_key(|r| (r.partition, r.key.clone()));
    assert_eq!(actual, expected);
    // At least the three acknowledged records also exist in an aborted attempt.
    // They must be present in the log but absent from read_committed output.
    assert!(
        read_records(&default_redpanda_server(), topic, 3, "read_uncommitted").len() > actual.len()
    );
    endpoint.batch_start(1, OutputBatchType::Delta).unwrap();
    endpoint.push_buffer(b"next transaction").unwrap();
    endpoint.batch_end().unwrap();
    assert_eq!(committed(topic, 3).len(), expected.len() + 1);
}

#[test]
fn a_committed_transaction_with_a_delayed_reply_is_not_replayed() {
    let topic = "ft_recovery_delayed_commit";
    let _topics = KafkaResources::create_topics(&[(topic, 1)]);
    let proxy = BrokerProxy::new(default_redpanda_server());
    let (mut endpoint, callbacks) = endpoint(topic, &proxy, 60_000, 60_000);
    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    endpoint.push_buffer(b"committed before its reply").unwrap();
    proxy.hold_commit(true);
    let worker = BatchWorker::new(endpoint);
    wait(|| proxy.held_commits() > 0, 30_000).unwrap();
    let before = committed(topic, 1);
    assert_eq!(before.len(), 1);
    wait(
        || callbacks.metric("kafka_output_commit_retries_total") > 0.0,
        30_000,
    )
    .unwrap();
    proxy.hold_commit(false);
    let (result, _endpoint) = worker.finish();
    result.unwrap();
    assert_eq!(
        callbacks.metric("kafka_output_transaction_aborts_total"),
        0.0
    );
    assert_eq!(committed(topic, 1), before);
}

#[test]
fn restart_skips_a_commit_whose_reply_was_lost() {
    let topic = "ft_recovery_lost_commit";
    let _topics = KafkaResources::create_topics(&[(topic, 1)]);
    let proxy = BrokerProxy::new(default_redpanda_server());
    let (mut output, _) = endpoint(topic, &proxy, 60_000, 60_000);
    output.batch_start(0, OutputBatchType::Delta).unwrap();
    output.push_buffer(b"committed before shutdown").unwrap();
    proxy.hold_commit(true);
    let worker = BatchWorker::new(output);
    wait(|| proxy.held_commits() > 0, 30_000).unwrap();
    assert_eq!(committed(topic, 1).len(), 1);
    worker.shutdown.cancel();
    let (result, output) = worker.finish();
    assert!(result.is_err());
    assert_eq!(output.spool.len(), 1);
    drop(output);
    proxy.hold_commit(false);

    let (mut output, _) = endpoint(topic, &proxy, 60_000, 60_000);
    assert_eq!(output.next_transaction, 1);
    output.batch_start(0, OutputBatchType::Delta).unwrap();
    output.push_buffer(b"committed before shutdown").unwrap();
    output.batch_end().unwrap();
    output.batch_start(1, OutputBatchType::Delta).unwrap();
    output.push_buffer(b"after restart").unwrap();
    output.batch_end().unwrap();
    let messages = committed(topic, 1);
    assert_eq!(messages.len(), 2);
    assert_eq!(
        messages[0].payload.as_deref(),
        Some(b"committed before shutdown".as_slice())
    );
    assert_eq!(
        messages[1].payload.as_deref(),
        Some(b"after restart".as_slice())
    );
}

#[test]
fn a_replacement_producer_keeps_ownership_after_fencing() {
    let topic = "ft_recovery_real_fence";
    let _topics = KafkaResources::create_topics(&[(topic, 1)]);
    let proxy = BrokerProxy::new(default_redpanda_server());
    let (mut old, callbacks) = endpoint(topic, &proxy, 60_000, 60_000);
    old.batch_start(0, OutputBatchType::Delta).unwrap();
    old.push_buffer(b"must be aborted").unwrap();
    wait(
        || callbacks.metric("kafka_output_delivered_messages_total") == 1.0,
        30_000,
    )
    .unwrap();
    let (mut replacement, _) = endpoint(topic, &proxy, 60_000, 60_000);
    let worker = BatchWorker::new(old);
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
    replacement.batch_start(0, OutputBatchType::Delta).unwrap();
    replacement.push_buffer(b"new owner").unwrap();
    replacement.batch_end().unwrap();
    worker.shutdown.cancel();
    let (result, _) = worker.finish();
    assert!(result.is_err());
    replacement.batch_start(1, OutputBatchType::Delta).unwrap();
    replacement.push_buffer(b"still the owner").unwrap();
    replacement.batch_end().unwrap();
    let messages = committed(topic, 1);
    assert_eq!(messages.len(), 2);
    assert_eq!(
        messages[0].payload.as_deref(),
        Some(b"new owner".as_slice())
    );
    assert_eq!(
        messages[1].payload.as_deref(),
        Some(b"still the owner".as_slice())
    );
}

/// Redpanda can fence a producer after expiring its transaction. A fatal
/// outcome must retain the batch and require a restart, just like fencing by
/// another producer; guessing that it is safe to reclaim ownership is unsafe.
#[test]
fn a_broker_timeout_keeps_uncommitted_output_until_restart() {
    let topic = "ft_recovery_expired_transaction";
    let _topics = KafkaResources::create_topics(&[(topic, 1)]);
    let proxy = BrokerProxy::new(default_redpanda_server());
    let (mut output, callbacks) = endpoint(topic, &proxy, 3000, 3000);
    output.batch_start(0, OutputBatchType::Delta).unwrap();
    output.push_buffer(b"before outage").unwrap();
    wait(
        || callbacks.metric("kafka_output_delivered_messages_total") == 1.0,
        30_000,
    )
    .unwrap();
    proxy.disconnect();
    let outage = Instant::now();
    output.push_buffer(b"during outage").unwrap();
    wait(
        || {
            outage.elapsed() >= Duration::from_secs(3)
                && callbacks.metric("kafka_output_delivery_errors_total") > 0.0
        },
        30_000,
    )
    .unwrap();
    let worker = BatchWorker::new(output);
    proxy.reconnect();
    // A broker may permit abort/replay after expiry or require a restart by
    // returning a fatal fence. Both must leave exactly one committed copy.
    wait(
        || {
            worker.handle.as_ref().unwrap().is_finished()
                || callbacks.errors().iter().any(|(fatal, ..)| *fatal)
        },
        30_000,
    )
    .unwrap();
    if callbacks.errors().iter().any(|(fatal, ..)| *fatal) {
        assert!(
            worker
                .finished
                .recv_timeout(Duration::from_millis(200))
                .is_err()
        );
        worker.shutdown.cancel();
        let (result, output) = worker.finish();
        assert!(result.is_err());
        assert_eq!(output.spool.len(), 2);
        drop(output);
        let (mut output, _) = endpoint(topic, &proxy, 60_000, 60_000);
        assert_eq!(output.next_transaction, 0);
        output.batch_start(0, OutputBatchType::Delta).unwrap();
        output.push_buffer(b"before outage").unwrap();
        output.push_buffer(b"during outage").unwrap();
        output.batch_end().unwrap();
    } else {
        let (result, _) = worker.finish();
        result.unwrap();
    }
    let messages = committed(topic, 1);
    assert_eq!(messages.len(), 2);
    assert_eq!(
        messages[0].payload.as_deref(),
        Some(b"before outage".as_slice())
    );
    assert_eq!(
        messages[1].payload.as_deref(),
        Some(b"during outage".as_slice())
    );
}
