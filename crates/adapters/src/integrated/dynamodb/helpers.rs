use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Result as AnyResult, anyhow, bail};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::DisplayErrorContext;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeValue, CancellationReason, Delete, DeleteRequest, Put, PutRequest, TransactWriteItem,
    WriteRequest,
};
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
///
/// `put_condition`/`delete_condition` are optional DynamoDB condition expressions
/// attached to the put and delete respectively. A put covers both inserts and
/// upserts, so `put_condition` gates every value write.
pub(crate) fn to_transact_item(
    table: &str,
    request: &WriteRequest,
    put_condition: Option<&str>,
    delete_condition: Option<&str>,
) -> AnyResult<TransactWriteItem> {
    match (request.put_request(), request.delete_request()) {
        (Some(request), None) => transact_put(table, request, put_condition),
        (None, Some(request)) => transact_delete(table, request, delete_condition),
        _ => bail!("expected exactly one put or delete request"),
    }
}

fn transact_put(
    table: &str,
    request: &PutRequest,
    condition: Option<&str>,
) -> AnyResult<TransactWriteItem> {
    let put = Put::builder()
        .table_name(table)
        .set_item(Some(request.item().clone()));
    let put = match condition {
        Some(condition) => put.condition_expression(condition),
        None => put,
    };
    Ok(TransactWriteItem::builder().put(put.build()?).build())
}

fn transact_delete(
    table: &str,
    request: &DeleteRequest,
    condition: Option<&str>,
) -> AnyResult<TransactWriteItem> {
    let delete = Delete::builder()
        .table_name(table)
        .set_key(Some(request.key().clone()));
    let delete = match condition {
        Some(condition) => delete.condition_expression(condition),
        None => delete,
    };
    Ok(TransactWriteItem::builder().delete(delete.build()?).build())
}

/// DynamoDB `CancellationReason` code for an item whose condition expression was
/// not met. Such an item is a permanent, expected rejection: retrying it is
/// pointless, so the connector drops it and retries the rest of the transaction.
const CONDITION_CHECK_FAILED: &str = "ConditionalCheckFailed";

/// `CancellationReason` code for an item that had no error of its own. It was
/// rolled back only because a sibling item cancelled the transaction, so it is
/// kept and retried.
const NO_ERROR: &str = "None";

/// Classifies each item of a cancelled transaction from its `CancellationReason`
/// and decides what to retry.
struct CancellationOutcome {
    /// Items to resubmit: those with no error of their own, plus those rejected
    /// for a transient reason (throttling / conflict).
    kept: Vec<TransactWriteItem>,
    /// Count of items dropped because their condition was not met.
    condition_failed: usize,
    /// A kept item was rejected for a transient reason, so the resubmission is a
    /// genuine retry that should back off and count against `max_retries`.
    has_retryable: bool,
    /// Codes of items that failed permanently for a reason other than a condition
    /// check (for example `ValidationError`). Their presence makes the whole
    /// transaction unrecoverable.
    hard_failures: Vec<String>,
}

/// Splits the items of a cancelled transaction by their per-item cancellation
/// reason. `reasons[i]` describes `items[i]` (DynamoDB returns them in request
/// order). Returns `None` when the counts disagree, in which case the caller
/// falls back to retrying the whole transaction with `items` left unchanged.
fn partition_by_cancellation_reason(
    items: &mut Vec<TransactWriteItem>,
    reasons: &[CancellationReason],
) -> Option<CancellationOutcome> {
    if reasons.len() != items.len() {
        return None;
    }

    let items = std::mem::take(items);
    let mut outcome = CancellationOutcome {
        kept: Vec::with_capacity(items.len()),
        condition_failed: 0,
        has_retryable: false,
        hard_failures: Vec::new(),
    };

    // DynamoDB returns cancellation reasons in the same order as the submitted
    // transaction items, so each reason can be paired by position.
    for (item, reason) in items.into_iter().zip(reasons) {
        match reason.code() {
            Some(CONDITION_CHECK_FAILED) => outcome.condition_failed += 1,
            None | Some(NO_ERROR) => outcome.kept.push(item),
            Some(code) if is_retryable_cancellation_code(code) => {
                outcome.has_retryable = true;
                outcome.kept.push(item);
            }
            Some(code) => outcome.hard_failures.push(code.to_string()),
        }
    }

    Some(outcome)
}

/// Whether a per-item cancellation code denotes a transient failure worth
/// retrying, as opposed to a permanent one.
fn is_retryable_cancellation_code(code: &str) -> bool {
    matches!(
        code,
        "TransactionConflict" | "ThrottlingError" | "ProvisionedThroughputExceeded"
    )
}

/// Outcome of a transactional chunk that DynamoDB accepted (possibly after
/// dropping items that failed their condition).
pub(crate) struct TransactChunkOutcome {
    /// Number of retry attempts the chunk cost.
    pub retries: u64,
    /// Items dropped because their condition expression was not met. These were
    /// never written, so the caller must exclude them from records-written.
    pub suppressed: u64,
}

/// Records and bytes actually written for a chunk of `rows` items totalling
/// `bytes`, after excluding the `suppressed` items that failed their condition.
///
/// Per-item byte sizes are not retained past the flush, so the written byte
/// count is the chunk's `bytes` scaled by the fraction of rows that landed.
pub(crate) fn written_totals(rows: usize, bytes: usize, suppressed: u64) -> (u64, u64) {
    let written = rows.saturating_sub(suppressed as usize);
    let written_bytes = if rows > 0 {
        (bytes as u128 * written as u128 / rows as u128) as u64
    } else {
        0
    };
    (written as u64, written_bytes)
}

/// Sends one pre-chunked `TransactWriteItems` request, retrying on failure with backoff.
///
/// On cancellation, items that failed their condition are dropped and the rest
/// are resubmitted. Only transient failures back off and count toward
/// `max_retries`. The returned [`TransactChunkOutcome`] reports how many items
/// the condition suppressed so the caller can avoid counting them as written.
pub(crate) async fn write_transact_chunk(
    client: Client,
    endpoint_name: String,
    mut transact_items: Vec<TransactWriteItem>,
    max_retries: Option<usize>,
    metrics: &DynamoDBOutputMetrics,
) -> AnyResult<TransactChunkOutcome> {
    let mut retry = 0usize;
    let mut suppressed = 0u64;

    loop {
        let call_start = Instant::now();
        let result = client
            .transact_write_items()
            .set_transact_items(Some(transact_items.clone()))
            .send()
            .await;
        metrics.record_write_call_latency(call_start.elapsed());

        let error = match result {
            Ok(_) => {
                return Ok(TransactChunkOutcome {
                    retries: retry as u64,
                    suppressed,
                });
            }
            Err(error) => error,
        };

        // A cancelled transaction carries a per-item reason list. Use it to drop
        // items whose condition was not met and retry only the rest. Any other
        // error (or a cancellation without reasons) falls through to the
        // whole-transaction retry below.
        if let Some(TransactWriteItemsError::TransactionCanceledException(cancelled)) =
            error.as_service_error()
        {
            let reasons = cancelled.cancellation_reasons();
            if !reasons.is_empty()
                && let Some(outcome) =
                    partition_by_cancellation_reason(&mut transact_items, reasons)
            {
                if !outcome.hard_failures.is_empty() {
                    // A permanent, non-condition failure (for example a
                    // validation error) cancels the transaction.
                    // Every item is dropped, including those whose condition happened
                    // to fail.
                    let failed =
                        outcome.kept.len() + outcome.hard_failures.len() + outcome.condition_failed;
                    metrics.record_transact_write_failure();
                    metrics.record_failed_items(failed as u64);
                    bail!(
                        "dynamodb output connector '{endpoint_name}' TransactWriteItems request \
                         cancelled by unrecoverable item error(s) [{}], dropping {failed} item(s): \
                         {}",
                        outcome.hard_failures.join(", "),
                        DisplayErrorContext(&error),
                    );
                }

                // Past the unrecoverable check the surviving items will be
                // written, so the condition failures are genuine suppressions.
                if outcome.condition_failed > 0 {
                    suppressed += outcome.condition_failed as u64;
                    metrics.record_condition_check_failures(outcome.condition_failed as u64);
                }

                if outcome.kept.is_empty() {
                    // Every remaining item failed its condition: nothing left to write.
                    return Ok(TransactChunkOutcome {
                        retries: retry as u64,
                        suppressed,
                    });
                }

                let dropped_any = outcome.condition_failed > 0;
                transact_items = outcome.kept;

                if outcome.has_retryable {
                    // Transient contention on a kept item; back off, retry and count it.
                    metrics.record_transact_write_failure();
                    if max_retries.is_some_and(|max| retry >= max) {
                        metrics.record_failed_items(transact_items.len() as u64);
                        bail!(
                            "dynamodb output connector '{endpoint_name}' gave up on {} \
                             transaction item(s) after {retry} retries: {}",
                            transact_items.len(),
                            DisplayErrorContext(&error),
                        );
                    }
                    retry += 1;
                    warn!(
                        "dynamodb output connector '{endpoint_name}' retrying {} transaction \
                         item(s) after transient cancellation (attempt {retry})",
                        transact_items.len(),
                    );
                    tokio::time::sleep(backoff_delay(retry)).await;
                    continue;
                }

                if dropped_any {
                    // Sibling items were dropped for failing their condition;  so resubmit
                    // the now-smaller set immediately.
                    continue;
                }
            }
        }

        // Whole-transaction failure: no usable per-item reasons, so retry the
        // entire request with backoff.
        metrics.record_transact_write_failure();
        let exhausted = max_retries.is_some_and(|max| retry >= max);
        // Render the underlying DynamoDB error into the message via
        // `DisplayErrorContext`; the bare `SdkError` `Display` would otherwise
        // hide the real cause when shown with `{}`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::Put;

    /// A throwaway transact item; the partition logic never inspects its contents.
    fn put_item(id: &str) -> TransactWriteItem {
        TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name("t")
                    .set_item(Some(HashMap::from([(
                        "id".to_string(),
                        AttributeValue::N(id.to_string()),
                    )])))
                    .build()
                    .unwrap(),
            )
            .build()
    }

    fn reason(code: &str) -> CancellationReason {
        CancellationReason::builder().code(code).build()
    }

    #[test]
    fn condition_failures_are_dropped_and_counted() {
        let mut items = vec![put_item("1"), put_item("2")];
        let reasons = vec![reason(CONDITION_CHECK_FAILED), reason(NO_ERROR)];

        let outcome = partition_by_cancellation_reason(&mut items, &reasons).unwrap();
        assert_eq!(outcome.condition_failed, 1);
        assert_eq!(outcome.kept.len(), 1);
        assert!(!outcome.has_retryable);
        assert!(outcome.hard_failures.is_empty());
    }

    #[test]
    fn all_condition_failures_leave_nothing_to_retry() {
        let mut items = vec![put_item("1"), put_item("2")];
        let reasons = vec![
            reason(CONDITION_CHECK_FAILED),
            reason(CONDITION_CHECK_FAILED),
        ];

        let outcome = partition_by_cancellation_reason(&mut items, &reasons).unwrap();
        assert_eq!(outcome.condition_failed, 2);
        assert!(outcome.kept.is_empty());
    }

    #[test]
    fn transient_reason_marks_retry_and_keeps_item() {
        let mut items = vec![put_item("1")];
        let reasons = vec![reason("TransactionConflict")];

        let outcome = partition_by_cancellation_reason(&mut items, &reasons).unwrap();
        assert!(outcome.has_retryable);
        assert_eq!(outcome.kept.len(), 1);
        assert_eq!(outcome.condition_failed, 0);
    }

    #[test]
    fn permanent_non_condition_reason_is_a_hard_failure() {
        let mut items = vec![put_item("1"), put_item("2")];
        let reasons = vec![reason("ValidationError"), reason(CONDITION_CHECK_FAILED)];

        let outcome = partition_by_cancellation_reason(&mut items, &reasons).unwrap();
        assert_eq!(outcome.hard_failures, vec!["ValidationError".to_string()]);
        assert_eq!(outcome.condition_failed, 1);
    }

    #[test]
    fn reason_count_mismatch_falls_back_to_whole_transaction_retry() {
        let mut items = vec![put_item("1"), put_item("2")];
        let reasons = vec![reason(NO_ERROR)];

        assert!(partition_by_cancellation_reason(&mut items, &reasons).is_none());
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn written_totals_exclude_suppressed_items() {
        // Nothing suppressed: the whole chunk counts.
        assert_eq!(written_totals(5, 100, 0), (5, 100));
        // Half the rows suppressed: records exact, bytes scaled by written fraction.
        assert_eq!(written_totals(2, 100, 1), (1, 50));
        // Every row suppressed: nothing written.
        assert_eq!(written_totals(2, 100, 2), (0, 0));
        // Defensive: suppressed exceeding rows saturates to zero rather than wrapping.
        assert_eq!(written_totals(2, 100, 3), (0, 0));
        // Empty chunk: no division by zero.
        assert_eq!(written_totals(0, 0, 0), (0, 0));
    }
}
