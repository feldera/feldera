//! Tests of the DynamoDB output connector that reach no engine: its metrics, the conversion of
//! items, and the retry loop. The `dynamodb` connector suite covers the connector with an
//! engine.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::sleep;
use std::time::Duration;

use aws_sdk_dynamodb::types::AttributeValue;
use feldera_adapterlib::metrics::ConnectorMetrics;
use feldera_adapterlib::transport::OutputBatchType;
use feldera_storage::tokio::TOKIO;
use tokio_util::sync::CancellationToken;

use crate::format::Encoder;

use super::helpers::{to_transact_item, write_transact_chunk};
use super::metrics::DynamoDBOutputMetrics;
use super::tests::{
    TempDynamoTable, attr_n, attr_s, dummy_client, dynamodb_client, dynamodb_endpoint,
    dynamodb_endpoint_url, dynamodb_metric, empty_endpoint_with_counter, records_written,
    test_table_name, wait_for_dynamodb,
};

#[test]
fn connector_metrics_accumulate_transact_write_failures() {
    let metrics = DynamoDBOutputMetrics::default();

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_transact_write_failures_total"),
        0.0
    );

    metrics.record_transact_write_failure();
    metrics.record_transact_write_failure();

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_transact_write_failures_total"),
        2.0
    );
}

#[test]
fn connector_metrics_accumulate_failed_items() {
    let metrics = DynamoDBOutputMetrics::default();

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_failed_items_total"),
        0.0
    );

    metrics.record_failed_items(3);
    metrics.record_failed_items(2);

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_failed_items_total"),
        5.0
    );
}

#[test]
fn connector_metrics_accumulate_throttled_items() {
    let metrics = DynamoDBOutputMetrics::default();

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_throttled_items_total"),
        0.0
    );

    metrics.record_throttled_items(10);
    metrics.record_throttled_items(5);

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_throttled_items_total"),
        15.0
    );
}

#[test]
fn connector_metrics_record_semaphore_wait_histogram() {
    let metrics = DynamoDBOutputMetrics::default();

    metrics.record_semaphore_wait(Duration::from_micros(400));
    metrics.record_semaphore_wait(Duration::from_micros(600));

    let histograms = metrics.histograms();
    let wait = histograms
        .iter()
        .find(|h| h.name == "dynamodb_output_semaphore_wait_microseconds")
        .expect("missing semaphore-wait histogram");

    // 400us + 600us recorded; the snapshot sum is in microseconds.
    assert_eq!(wait.snapshot.sum(), 1000);

    let count: u64 = wait
        .snapshot
        .iter_buckets()
        .map(|bucket| bucket.count)
        .sum();
    assert_eq!(count, 2);
}

#[test]
fn connector_metrics_record_write_call_latency_histogram() {
    let metrics = DynamoDBOutputMetrics::default();

    metrics.record_write_call_latency(Duration::from_micros(1_500));
    metrics.record_write_call_latency(Duration::from_micros(2_500));

    let histograms = metrics.histograms();
    let latency = histograms
        .iter()
        .find(|h| h.name == "dynamodb_output_write_call_latency_microseconds")
        .expect("missing write-call latency histogram");

    // 1,500us + 2,500us recorded; the snapshot sum is in microseconds.
    assert_eq!(latency.snapshot.sum(), 4_000);

    let count: u64 = latency
        .snapshot
        .iter_buckets()
        .map(|bucket| bucket.count)
        .sum();
    assert_eq!(count, 2);
}

#[test]
fn encoder_progress_counter_resets_at_batch_start_and_end() {
    let records_written = Arc::new(AtomicU64::new(99));
    let mut endpoint = empty_endpoint_with_counter(records_written.clone());

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    assert_eq!(records_written.load(Ordering::Relaxed), 0);

    records_written.store(88, Ordering::Relaxed);
    endpoint.consumer().batch_end();
    assert_eq!(records_written.load(Ordering::Relaxed), 0);
}

#[test]
fn encoder_json_to_attribute_value_encodes_nested_values() {
    let item = HashMap::from([
        ("n".to_string(), AttributeValue::N("1.25".to_string())),
        ("s".to_string(), AttributeValue::S("x".to_string())),
        ("b".to_string(), AttributeValue::Bool(true)),
        ("null".to_string(), AttributeValue::Null(true)),
        (
            "list".to_string(),
            AttributeValue::L(vec![
                AttributeValue::N("1".to_string()),
                AttributeValue::S("two".to_string()),
            ]),
        ),
        (
            "map".to_string(),
            AttributeValue::M(HashMap::from([(
                "nested".to_string(),
                AttributeValue::Bool(false),
            )])),
        ),
    ]);

    assert_eq!(attr_n(&item, "n"), "1.25");
    assert_eq!(attr_s(&item, "s"), "x");
    assert_eq!(item.get("b").unwrap().as_bool().unwrap(), &true);
    assert_eq!(item.get("null").unwrap().as_null().unwrap(), &true);
    assert!(item.get("list").unwrap().is_l());
    assert!(item.get("map").unwrap().is_m());
}

#[test]
fn to_transact_item_attaches_conditions_per_operation() {
    use aws_sdk_dynamodb::types::{DeleteRequest, PutRequest, WriteRequest};

    let item = HashMap::from([("id".to_string(), AttributeValue::N("1".to_string()))]);
    let put = WriteRequest::builder()
        .put_request(PutRequest::builder().set_item(Some(item)).build().unwrap())
        .build();
    let delete = WriteRequest::builder()
        .delete_request(
            DeleteRequest::builder()
                .set_key(Some(HashMap::from([(
                    "id".to_string(),
                    AttributeValue::N("1".to_string()),
                )])))
                .build()
                .unwrap(),
        )
        .build();

    // A put carries the put condition; a delete carries the delete condition.
    let transact_put =
        to_transact_item("t", &put, Some("attribute_not_exists(id)"), Some("bogus")).unwrap();
    assert_eq!(
        transact_put.put().unwrap().condition_expression(),
        Some("attribute_not_exists(id)")
    );

    let transact_delete =
        to_transact_item("t", &delete, Some("bogus"), Some("attribute_exists(id)")).unwrap();
    assert_eq!(
        transact_delete.delete().unwrap().condition_expression(),
        Some("attribute_exists(id)")
    );

    // No condition attached when none is configured.
    let unconditional = to_transact_item("t", &put, None, None).unwrap();
    assert_eq!(unconditional.put().unwrap().condition_expression(), None);
}

#[test]
fn dynamodb_progress_counter_batch_start_resets() {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("progress_reset");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

    // Simulate a stale counter left over from a previous batch.
    endpoint.records_written.store(99, Ordering::Relaxed);
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    assert_eq!(records_written(&endpoint), 0);

    // Close cleanly so the worker does not leak an open state.
    endpoint.consumer().batch_end();
}

/// A shutdown ends `write_transact_chunk`'s unbounded retry loop.
///
/// `max_retries: None` makes the loop unbounded, and
/// `DynamoDBWorker::drain_pending_writes` blocks the controller's output
/// thread on these tasks, a DBSP aux thread that `RuntimeHandle::kill` joins.
///
/// Nothing listens at the client's endpoint, so every attempt fails and the
/// loop cannot end on its own.
#[test]
fn a_shutdown_ends_an_unbounded_transact_retry_loop() {
    use aws_sdk_dynamodb::types::{PutRequest, WriteRequest};

    let item = HashMap::from([("id".to_string(), AttributeValue::N("1".to_string()))]);
    let put = WriteRequest::builder()
        .put_request(PutRequest::builder().set_item(Some(item)).build().unwrap())
        .build();
    let items = vec![to_transact_item("test_table", &put, None, None).unwrap()];

    let shutdown = CancellationToken::new();
    let client = dummy_client();

    // Write on a thread of its own, so that a regression fails this test
    // rather than hanging the whole run.
    let writing_shutdown = shutdown.clone();
    let (done, write_returned) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let metrics = DynamoDBOutputMetrics::default();
        let result = TOKIO.block_on(write_transact_chunk(
            client,
            "test_endpoint".to_string(),
            items,
            None,
            &writing_shutdown,
            &metrics,
        ));
        let _ = done.send(result.err());
    });

    // Long enough for the first attempt to fail and the retry to back off.
    sleep(Duration::from_millis(200));
    shutdown.cancel();

    let error = write_returned
        .recv_timeout(Duration::from_secs(30))
        .expect("a shutdown must end the retry loop")
        .expect("a chunk that gave up must report an error");
    writer.join().unwrap();

    assert!(
        error.to_string().contains("shutting down"),
        "unexpected error: {error}"
    );
}
