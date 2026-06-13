use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result as AnyResult, bail};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{AttributeValue, Delete, Put, TransactWriteItem, WriteRequest};
use aws_types::region::Region;
use dbsp::circuit::tokio::TOKIO;
use feldera_types::transport::dynamodb::DynamoDBWriterConfig;
use rand::Rng;
use tracing::warn;

/// Sends one pre-chunked `BatchWriteItem` request, retrying unprocessed items with backoff.
pub(crate) async fn write_batch_chunk(
    client: Client,
    endpoint_name: String,
    table: String,
    requests: Vec<WriteRequest>,
    max_retries: Option<usize>,
) -> AnyResult<u64> {
    let mut request_items = HashMap::from([(table, requests)]);
    let mut retry = 0usize;

    loop {
        let output = client
            .batch_write_item()
            .set_request_items(Some(request_items.clone()))
            .send()
            .await
            .with_context(|| {
                format!("dynamodb output connector '{endpoint_name}' failed to write batch chunk")
            })?;

        let Some(unprocessed) = output.unprocessed_items() else {
            return Ok(retry as u64);
        };
        if unprocessed.is_empty() {
            return Ok(retry as u64);
        }

        let count = unprocessed.values().map(Vec::len).sum::<usize>();
        if max_retries.is_some_and(|max| retry >= max) {
            bail!(
                "dynamodb output connector '{endpoint_name}' failed to write \
                 {count} unprocessed item(s) after {retry} retries"
            );
        }

        request_items = unprocessed.clone();
        if should_log(retry + 1, max_retries) {
            warn!(
                "dynamodb output connector '{endpoint_name}' retrying \
                 {count} unprocessed batch item(s) {}",
                retry_label(retry + 1, max_retries),
            );
        }

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
///
/// Transient errors are first retried transparently by the AWS SDK. If the SDK exhausts its
/// retries and returns an error, this function retries the whole chunk.
pub(crate) async fn write_transact_chunk(
    client: Client,
    endpoint_name: String,
    transact_items: Vec<TransactWriteItem>,
    max_retries: Option<usize>,
) -> AnyResult<u64> {
    let mut retry = 0usize;

    loop {
        match client
            .transact_write_items()
            .set_transact_items(Some(transact_items.clone()))
            .send()
            .await
        {
            Ok(_) => return Ok(retry as u64),
            Err(error) => {
                if max_retries.is_some_and(|max| retry >= max) {
                    return Err(error).with_context(|| {
                        format!(
                            "dynamodb output connector '{endpoint_name}' failed to commit \
                             transaction chunk after {retry} retries"
                        )
                    });
                }
                if should_log(retry + 1, max_retries) {
                    warn!(
                        "dynamodb output connector '{endpoint_name}' retrying \
                         transaction chunk with {} item(s) {}: {error}",
                        transact_items.len(),
                        retry_label(retry + 1, max_retries),
                    );
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

/// Whether this retry attempt should be logged.
///
/// Bounded retries are always logged (there are at most `max` of them).
/// Unbounded retries are logged for the first 10 instances, and then
/// every 10 attempts to avoid flooding the logs.
fn should_log(retry: usize, max_retries: Option<usize>) -> bool {
    match max_retries {
        Some(_) => true,
        None => retry % 10 == 0 || retry < 10,
    }
}

/// Formats the retry position for log messages.
fn retry_label(retry: usize, max_retries: Option<usize>) -> String {
    match max_retries {
        Some(max) => format!("({retry}/{max})"),
        None => format!("(attempt {retry}, unbounded)"),
    }
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
        _ => 0,
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
