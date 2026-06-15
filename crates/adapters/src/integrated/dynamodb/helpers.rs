use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Result as AnyResult, anyhow, bail};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::DisplayErrorContext;
use aws_sdk_dynamodb::types::{AttributeValue, Delete, Put, TransactWriteItem, WriteRequest};
use aws_types::region::Region;
use dbsp::circuit::tokio::TOKIO;
use feldera_types::transport::dynamodb::DynamoDBWriterConfig;
use rand::Rng;
use tracing::warn;

use super::metrics::DynamoDBOutputMetrics;

/// Sends one pre-chunked `BatchWriteItem` request, retrying unprocessed items with backoff.
pub(crate) async fn write_batch_chunk(
    client: Client,
    endpoint_name: String,
    table: String,
    requests: Vec<WriteRequest>,
    max_retries: Option<usize>,
    metrics: &DynamoDBOutputMetrics,
) -> AnyResult<u64> {
    let mut request_items = HashMap::from([(table, requests)]);
    let mut retry = 0usize;

    loop {
        let call_start = Instant::now();
        let output_result = client
            .batch_write_item()
            .set_request_items(Some(request_items.clone()))
            .send()
            .await;
        metrics.record_write_call_latency(call_start.elapsed());

        let output = output_result
            .map_err(|error| {
                // `SdkError` display can hide the underlying DynamoDB service error.
                anyhow!(
                    "dynamodb output connector '{endpoint_name}' failed to write batch \
                     chunk: {}",
                    DisplayErrorContext(&error)
                )
            })
            .inspect_err(|error| warn!("{error:#}"))?;

        let Some(unprocessed) = output.unprocessed_items() else {
            return Ok(retry as u64);
        };
        if unprocessed.is_empty() {
            return Ok(retry as u64);
        }

        let count = unprocessed.values().map(Vec::len).sum::<usize>();
        // Unprocessed items are writes DynamoDB rejected for lack of capacity.
        metrics.record_throttled_items(count as u64);
        if max_retries.is_some_and(|max| retry >= max) {
            metrics.record_failed_items(count as u64);
            bail!(
                "dynamodb output connector '{endpoint_name}' failed to write \
                 {count} unprocessed item(s) after {retry} retries"
            );
        }

        request_items = unprocessed.clone();
        warn!(
            "dynamodb output connector '{endpoint_name}' retrying \
             {count} unprocessed batch item(s) (attempt {})",
            retry + 1,
        );

        retry += 1;
        tokio::time::sleep(backoff_delay(retry)).await;
    }
}

/// Converts a `WriteRequest` (put or delete) into a `TransactWriteItem` for use in `TransactWriteItems`.
pub(crate) fn to_transact_item(
    table: &str,
    request: &WriteRequest,
) -> AnyResult<TransactWriteItem> {
    match (request.put_request(), request.delete_request()) {
        (Some(put_request), None) => Ok(TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name(table)
                    .set_item(Some(put_request.item().clone()))
                    .build()?,
            )
            .build()),
        (None, Some(delete_request)) => Ok(TransactWriteItem::builder()
            .delete(
                Delete::builder()
                    .table_name(table)
                    .set_key(Some(delete_request.key().clone()))
                    .build()?,
            )
            .build()),
        _ => bail!("expected exactly one put or delete request"),
    }
}

/// Sends one pre-chunked `TransactWriteItems` request, retrying on failure with backoff.
pub(crate) async fn write_transact_chunk(
    client: Client,
    endpoint_name: String,
    transact_items: Vec<TransactWriteItem>,
    max_retries: Option<usize>,
    metrics: &DynamoDBOutputMetrics,
) -> AnyResult<u64> {
    let mut retry = 0usize;

    loop {
        let call_start = Instant::now();
        let result = client
            .transact_write_items()
            .set_transact_items(Some(transact_items.clone()))
            .send()
            .await;
        metrics.record_write_call_latency(call_start.elapsed());

        match result {
            Ok(_) => return Ok(retry as u64),
            Err(error) => {
                // Count every failed TransactWriteItems attempt; these are
                // retried below (or dropped once retries are exhausted).
                metrics.record_transact_write_failure();
                let exhausted = max_retries.is_some_and(|max| retry >= max);
                // Render the underlying DynamoDB error into the message via
                // `DisplayErrorContext`; the bare `SdkError` `Display`  would
                // otherwise hide the real cause when shown with `{}`.
                let error = anyhow!(
                    "dynamodb output connector '{endpoint_name}' TransactWriteItems request \
                     with {} item(s) failed (attempt {}), {}: {}",
                    transact_items.len(),
                    retry + 1,
                    if exhausted { "giving up" } else { "retrying" },
                    DisplayErrorContext(&error),
                );
                warn!("{error:#}");
                if exhausted {
                    // A transaction is all-or-nothing, so every item is dropped.
                    metrics.record_failed_items(transact_items.len() as u64);
                    return Err(error);
                }
            }
        }

        retry += 1;
        tokio::time::sleep(backoff_delay(retry)).await;
    }
}

/// Returns the exponential backoff delay for retry attempt `retry`, with full jitter.
///
/// The max delay would be ~12.8s, which happens after 8 retries.
fn backoff_delay(retry: usize) -> Duration {
    let ceiling_ms = 50 * (1u64 << retry.min(8));
    Duration::from_millis(rand::thread_rng().gen_range(0..=ceiling_ms))
}

pub(crate) fn item_size(item: &HashMap<String, AttributeValue>) -> usize {
    item.iter()
        .map(|(key, value)| key.len() + attribute_value_size(value))
        .sum()
}

fn attribute_value_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) => value.len(),
        AttributeValue::B(value) => value.as_ref().len(),
        AttributeValue::Bool(_) | AttributeValue::Null(_) => 1,
        AttributeValue::M(values) => item_size(values),
        AttributeValue::L(values) => values.iter().map(attribute_value_size).sum(),
        AttributeValue::Ss(values) | AttributeValue::Ns(values) => {
            values.iter().map(String::len).sum()
        }
        AttributeValue::Bs(values) => values.iter().map(|value| value.as_ref().len()).sum(),
        // `AttributeValue` is `#[non_exhaustive]`, so a future SDK version may
        // add a variant we do not size here, silently undercounting
        // `bytes_written`. Fail loudly in debug/test builds so we notice and
        // extend this match; in release, fall back to 0.
        other => {
            debug_assert!(
                false,
                "unhandled DynamoDB AttributeValue variant in size estimation: {other:?}"
            );
            0
        }
    }
}

pub(crate) fn make_client(config: &DynamoDBWriterConfig) -> Client {
    let mut config_builder =
        aws_sdk_dynamodb::Config::builder().region(Region::new(config.region.clone()));

    if let Some(endpoint_url) = &config.endpoint_url {
        config_builder = config_builder.endpoint_url(endpoint_url);
    }

    if let (Some(access_key), Some(secret_key)) =
        (&config.aws_access_key_id, &config.aws_secret_access_key)
    {
        let credentials = aws_sdk_dynamodb::config::Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "credential-provider",
        );
        Client::from_conf(config_builder.credentials_provider(credentials).build())
    } else {
        let provider = TOKIO.block_on(async {
            aws_config::default_provider::credentials::default_provider().await
        });
        Client::from_conf(config_builder.credentials_provider(provider).build())
    }
}
