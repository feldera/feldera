use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result as AnyResult, anyhow, bail};
use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::{
    ConnectorError, DisplayErrorContext, ProvideErrorMetadata, SdkError,
};
use aws_sdk_dynamodb::operation::RequestId;
use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeValue, CancellationReason, Delete, DeleteRequest, Put, PutRequest, ThrottlingReason,
    TransactWriteItem, WriteRequest,
};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_types::retry::ProvideErrorKind;
use aws_types::region::Region;
use feldera_storage::tokio::TOKIO;
use feldera_types::transport::dynamodb::DynamoDBWriterConfig;
use rand::Rng;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::health::{ChunkGuard, RetryHealth};
use super::metrics::DynamoDBOutputMetrics;

/// Whether a failed DynamoDB request is worth sending again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteErrorKind {
    /// The condition can pass: throttling, a timeout, a network failure, a
    /// server error, or a failure the connector cannot tell apart from one.
    Retryable,
    /// A retry sends the same request to the same answer: a validation error,
    /// a missing table, denied access, or an invalid configuration.
    Permanent,
}

/// AWS error codes the SDK retries as throttling. Copied from
/// `aws-runtime` 1.7.2, `src/retries/classifiers.rs`, `THROTTLING_ERRORS`,
/// which is not a direct dependency of this crate.
const THROTTLING_ERROR_CODES: &[&str] = &[
    "Throttling",
    "ThrottlingException",
    "ThrottledException",
    "RequestThrottledException",
    "TooManyRequestsException",
    "ProvisionedThroughputExceededException",
    "TransactionInProgressException",
    "RequestLimitExceeded",
    "BandwidthLimitExceeded",
    "LimitExceededException",
    "RequestThrottled",
    "SlowDown",
    "PriorRequestNotComplete",
    "EC2ThrottledException",
];

/// AWS error codes the SDK retries as transient. Same source as
/// [`THROTTLING_ERROR_CODES`], `TRANSIENT_ERRORS`.
const TRANSIENT_ERROR_CODES: &[&str] = &["RequestTimeout", "RequestTimeoutException"];

/// DynamoDB error codes that the AWS error-handling guide labels retryable
/// and that the SDK lists above omit. An item collection over its size limit
/// can shrink through deletes, smaller updates or TTL expiry, so its code is
/// retryable too, with the configured retry budget as the bound.
const DYNAMODB_RETRYABLE_CODES: &[&str] = &[
    "ReplicatedWriteConflictException",
    "InternalServerError",
    "TransactionConflictException",
    "ItemCollectionSizeLimitExceededException",
    // Retryable as a whole; the transactional writer reads the per-item
    // reasons first and gives up on a permanent one.
    "TransactionCanceledException",
];

/// HTTP statuses the SDK's default classifier retries.
const RETRYABLE_HTTP_STATUSES: &[u16] = &[500, 502, 503, 504];

fn is_retryable_error_code(code: &str) -> bool {
    THROTTLING_ERROR_CODES.contains(&code)
        || TRANSIENT_ERROR_CODES.contains(&code)
        || DYNAMODB_RETRYABLE_CODES.contains(&code)
}

/// Classifies a failed DynamoDB request.
pub(crate) fn classify_sdk_error<E>(error: &SdkError<E, HttpResponse>) -> WriteErrorKind
where
    E: ProvideErrorMetadata + ProvideErrorKind,
{
    use WriteErrorKind::*;
    match error {
        SdkError::TimeoutError(_) | SdkError::ResponseError(_) => Retryable,
        SdkError::ConstructionFailure(_) => Permanent,
        SdkError::DispatchFailure(failure) => match failure.as_connector_error() {
            Some(error) => classify_connector_error(error),
            None => Retryable,
        },
        SdkError::ServiceError(service) => {
            if RETRYABLE_HTTP_STATUSES.contains(&service.raw().status().as_u16()) {
                return Retryable;
            }
            let error = service.err();
            if ProvideErrorMetadata::code(error).is_some_and(is_retryable_error_code) {
                return Retryable;
            }
            // The SDK retries every error its model marks retryable, whatever
            // the kind; `ReplicatedWriteConflictException` is a `ClientError`.
            match error.retryable_error_kind() {
                Some(_) => Retryable,
                None => Permanent,
            }
        }
        // `SdkError` is `non_exhaustive`. An unknown failure is not known to be permanent.
        _ => Retryable,
    }
}

/// Classifies a request that never got a response.
///
/// The SDK marks I/O and timeouts; it leaves a credential-provider failure as
/// `other` with no retry kind. A temporary STS outage during a credential
/// refresh looks the same as an invalid configuration there, so the source
/// chain decides: only a positively identified configuration error is permanent.
fn classify_connector_error(error: &ConnectorError) -> WriteErrorKind {
    use WriteErrorKind::*;
    if error.is_timeout() || error.is_io() {
        return Retryable;
    }
    if error.is_user() || has_invalid_credentials_configuration(error) {
        return Permanent;
    }
    // The SDK retries any kind the connector error names, and the connector
    // retries an unnamed one too: it is not known to be permanent.
    Retryable
}

/// Whether `error` or a cause of it is `CredentialsError::InvalidConfiguration`.
/// The identity cache wraps the provider's error before the SDK boxes it, so
/// the immediate source is not enough.
fn has_invalid_credentials_configuration(error: &(dyn StdError + 'static)) -> bool {
    let mut current: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(error) = current {
        if matches!(
            error.downcast_ref::<CredentialsError>(),
            Some(CredentialsError::InvalidConfiguration(_))
        ) {
            return true;
        }
        current = error.source();
    }
    false
}

/// The write operation errors, with the throttling reasons some of them carry.
pub(crate) trait WriteOperationError:
    ProvideErrorMetadata + ProvideErrorKind + RequestId + StdError + 'static
{
    fn throttling_reasons(&self) -> &[ThrottlingReason];
}

impl WriteOperationError for BatchWriteItemError {
    fn throttling_reasons(&self) -> &[ThrottlingReason] {
        match self {
            Self::ThrottlingException(error) => error.throttling_reasons(),
            Self::ProvisionedThroughputExceededException(error) => error.throttling_reasons(),
            Self::RequestLimitExceeded(error) => error.throttling_reasons(),
            _ => &[],
        }
    }
}

impl WriteOperationError for TransactWriteItemsError {
    fn throttling_reasons(&self) -> &[ThrottlingReason] {
        match self {
            Self::ThrottlingException(error) => error.throttling_reasons(),
            Self::ProvisionedThroughputExceededException(error) => error.throttling_reasons(),
            Self::RequestLimitExceeded(error) => error.throttling_reasons(),
            _ => &[],
        }
    }
}

/// One line about a failed request: the service's code, message, throttling
/// reasons and request id, without the raw HTTP response that
/// [`DisplayErrorContext`] prints for a service error. A failure with no
/// response keeps its source chain, which is all it has.
pub(crate) fn describe_sdk_error<E: WriteOperationError>(
    error: &SdkError<E, HttpResponse>,
) -> String {
    let SdkError::ServiceError(service) = error else {
        return DisplayErrorContext(error).to_string();
    };
    let error = service.err();
    let mut text = ProvideErrorMetadata::code(error)
        .unwrap_or("unknown service error")
        .to_string();
    if let Some(message) = error.message() {
        text.push_str(": ");
        text.push_str(message);
    }
    let reasons = error
        .throttling_reasons()
        .iter()
        .map(|reason| {
            format!(
                "{} on {}",
                reason.reason().unwrap_or("unknown reason"),
                reason.resource().unwrap_or("unknown resource")
            )
        })
        .collect::<Vec<_>>();
    if !reasons.is_empty() {
        text.push_str(" (throttling reasons: ");
        text.push_str(&reasons.join(", "));
        text.push(')');
    }
    if let Some(request_id) = error.request_id() {
        text.push_str(" (request id ");
        text.push_str(request_id);
        text.push(')');
    }
    text
}

/// How a chunk writer waits before retry number `retry`.
pub(crate) type RetryDelay =
    Arc<dyn Fn(usize) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// The retry budget of a chunk writer and how it waits between retries.
#[derive(Clone)]
pub(crate) struct RetryPolicy {
    /// Retries after the first request. `None` is unbounded.
    pub(crate) max_retries: Option<usize>,
    pub(crate) delay: RetryDelay,
}

impl RetryPolicy {
    /// The production policy: exponential backoff with full jitter.
    pub(crate) fn new(max_retries: Option<u8>) -> Self {
        Self::with_delay(
            max_retries,
            Arc::new(|retry| Box::pin(tokio::time::sleep(backoff_delay(retry)))),
        )
    }

    pub(crate) fn with_delay(max_retries: Option<u8>, delay: RetryDelay) -> Self {
        Self {
            max_retries: max_retries.map(usize::from),
            delay,
        }
    }

    fn exhausted(&self, retries: usize) -> bool {
        self.max_retries.is_some_and(|max| retries >= max)
    }

    fn budget(&self) -> String {
        match self.max_retries {
            Some(max) => format!(" of {max}"),
            None => String::new(),
        }
    }
}

/// The retries of one chunk: its budget, its count, and its health and metrics.
struct ChunkRetries<'a> {
    policy: &'a RetryPolicy,
    metrics: &'a DynamoDBOutputMetrics,
    guard: ChunkGuard<'a>,
    /// `BatchWriteItem` or `TransactWriteItems`, for the diagnostics.
    operation: &'static str,
    endpoint_name: &'a str,
    /// Retries so far.
    retries: usize,
}

impl<'a> ChunkRetries<'a> {
    fn new(
        policy: &'a RetryPolicy,
        metrics: &'a DynamoDBOutputMetrics,
        health: &'a RetryHealth,
        operation: &'static str,
        endpoint_name: &'a str,
    ) -> Self {
        Self {
            policy,
            metrics,
            guard: health.chunk(),
            operation,
            endpoint_name,
            retries: 0,
        }
    }

    /// Waits for the next retry and counts it, or returns the error of a
    /// chunk that must give up now. `cause` is why the request did not land
    /// and `outstanding` how many items it still carries.
    async fn retry_or_give_up(
        &mut self,
        outstanding: usize,
        cause: &str,
    ) -> Result<(), anyhow::Error> {
        if self.policy.exhausted(self.retries) {
            self.metrics.record_failed_items(outstanding as u64);
            let error = anyhow!(
                "dynamodb output connector '{}' dropped {outstanding} item(s) after {} {} \
                 retries; raise `max_retries`, or set it to null to retry until DynamoDB \
                 recovers: {cause}",
                self.endpoint_name,
                self.retries,
                self.operation,
            );
            warn!("{error:#}");
            return Err(error);
        }
        self.retries += 1;
        let description = format!(
            "retrying {} for {outstanding} item(s) (retry {}{}): {cause}",
            self.operation,
            self.retries,
            self.policy.budget()
        );
        warn!(
            "dynamodb output connector '{}' {description}",
            self.endpoint_name
        );
        self.guard.retrying(&description);
        (self.policy.delay)(self.retries).await;
        // A retry that the shutdown cancelled during the wait never started.
        self.metrics.record_retries(1);
        Ok(())
    }

    /// Returns the error of a chunk whose request failed in a way a retry cannot fix.
    fn give_up_now(&self, outstanding: usize, cause: &str) -> anyhow::Error {
        self.metrics.record_failed_items(outstanding as u64);
        let error = anyhow!(
            "dynamodb output connector '{}' dropped {outstanding} item(s): {} failed with an \
             error that a retry cannot fix, check the connector configuration and the table: \
             {cause}",
            self.endpoint_name,
            self.operation,
        );
        warn!("{error:#}");
        error
    }

    fn succeeded(self) -> u64 {
        self.guard.succeeded();
        self.retries as u64
    }

    fn failed(self, error: anyhow::Error) -> anyhow::Error {
        self.guard.failed(&error.to_string());
        error
    }
}

/// Sends one pre-chunked `BatchWriteItem` request, retrying unprocessed items
/// and retryable errors with backoff.
///
/// The request set shrinks to the unprocessed items after a partial success
/// and stays that way through later failures, so a retry never resends an
/// item that landed. A response lost to the network may still have been
/// applied: this is not exactly-once delivery.
///
/// Gives up on `shutdown`: the loop is unbounded when the policy has no
/// retry limit, and `DynamoDBWorker::drain_pending_writes` blocks the
/// controller's output thread on these tasks, a DBSP aux thread that
/// `RuntimeHandle::kill` joins. The loop itself is shutdown-unaware;
/// dropping it is what ends it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_batch_chunk(
    client: Client,
    endpoint_name: String,
    table: String,
    requests: Vec<WriteRequest>,
    policy: &RetryPolicy,
    shutdown: &CancellationToken,
    metrics: &DynamoDBOutputMetrics,
    health: &RetryHealth,
) -> AnyResult<u64> {
    let item_count = requests.len();
    let attempts = async {
        let mut retries =
            ChunkRetries::new(policy, metrics, health, "BatchWriteItem", &endpoint_name);
        let mut request_items = HashMap::from([(table, requests)]);

        loop {
            let outstanding = request_items.values().map(Vec::len).sum::<usize>();
            let call_start = Instant::now();
            let result = client
                .batch_write_item()
                .set_request_items(Some(request_items.clone()))
                .send()
                .await;
            metrics.record_write_call_latency(call_start.elapsed());

            let cause = match result {
                Ok(output) => {
                    let unprocessed = output
                        .unprocessed_items()
                        .filter(|unprocessed| !unprocessed.is_empty());
                    let Some(unprocessed) = unprocessed else {
                        return Ok(retries.succeeded());
                    };
                    let count = unprocessed.values().map(Vec::len).sum::<usize>();
                    // Unprocessed items are writes DynamoDB rejected for lack of capacity.
                    metrics.record_throttled_items(count as u64);
                    request_items = unprocessed.clone();
                    format!("DynamoDB returned {count} of {outstanding} item(s) unprocessed")
                }
                Err(error) => {
                    let cause = describe_sdk_error(&error);
                    if classify_sdk_error(&error) == WriteErrorKind::Permanent {
                        let error = retries.give_up_now(outstanding, &cause);
                        return Err(retries.failed(error));
                    }
                    cause
                }
            };

            let outstanding = request_items.values().map(Vec::len).sum::<usize>();
            if let Err(error) = retries.retry_or_give_up(outstanding, &cause).await {
                return Err(retries.failed(error));
            }
        }
    };

    // One shutdown consult for the whole loop, so that no await inside it can
    // outlive the teardown, whether or not anyone remembers to say so.
    shutdown
        .run_until_cancelled(attempts)
        .await
        .unwrap_or_else(|| {
            Err(anyhow!(
                "dynamodb output connector '{endpoint_name}' gave up on a chunk of \
                 {item_count} item(s): the pipeline is shutting down"
            ))
        })
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
        "TransactionConflict"
            | "ThrottlingError"
            | "ProvisionedThroughputExceeded"
            | "ItemCollectionSizeLimitExceeded"
    )
}

/// Outcome of a transactional chunk that DynamoDB accepted (possibly after
/// dropping items that failed their condition).
#[derive(Debug)]
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
/// are resubmitted at once. A transient item error, a cancellation without
/// usable per-item reasons, and every other retryable error back off and count
/// toward the retry budget. A permanent item error, and every other permanent
/// error, drops the whole transaction at once. The returned
/// [`TransactChunkOutcome`] reports how many items the condition suppressed so
/// the caller can avoid counting them as written.
///
/// Each request gets a fresh `ClientRequestToken` from the SDK, so a retry is a
/// new transaction to DynamoDB, not an idempotent repeat.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_transact_chunk(
    client: Client,
    endpoint_name: String,
    mut transact_items: Vec<TransactWriteItem>,
    policy: &RetryPolicy,
    shutdown: &CancellationToken,
    metrics: &DynamoDBOutputMetrics,
    health: &RetryHealth,
) -> AnyResult<TransactChunkOutcome> {
    let item_count = transact_items.len();
    let attempts = async {
        let mut retries = ChunkRetries::new(
            policy,
            metrics,
            health,
            "TransactWriteItems",
            &endpoint_name,
        );
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
                        retries: retries.succeeded(),
                        suppressed,
                    });
                }
                Err(error) => error,
            };
            let description = describe_sdk_error(&error);

            // A cancelled transaction carries a per-item reason list. Use it to
            // drop items whose condition was not met and retry only the rest.
            // Any other error, and a cancellation without usable reasons, goes
            // through the whole-transaction retry below.
            let cancelled = match error.as_service_error() {
                Some(TransactWriteItemsError::TransactionCanceledException(cancelled)) => {
                    Some(cancelled)
                }
                _ => None,
            };
            let cause = 'classify: {
                let Some(cancelled) = cancelled else {
                    metrics.record_transact_write_failure();
                    if classify_sdk_error(&error) == WriteErrorKind::Permanent {
                        let error = retries.give_up_now(transact_items.len(), &description);
                        return Err(retries.failed(error));
                    }
                    break 'classify Some(description);
                };

                let reasons = cancelled.cancellation_reasons();
                let outcome = if reasons.is_empty() {
                    None
                } else {
                    partition_by_cancellation_reason(&mut transact_items, reasons)
                };
                let Some(outcome) = outcome else {
                    metrics.record_transact_write_failure();
                    break 'classify Some(format!(
                        "transaction cancelled without usable per-item reasons: {description}"
                    ));
                };

                if !outcome.hard_failures.is_empty() {
                    // A permanent, non-condition failure (for example a
                    // validation error) cancels the transaction. Every item is
                    // dropped, including those whose condition happened to fail.
                    let failed =
                        outcome.kept.len() + outcome.hard_failures.len() + outcome.condition_failed;
                    metrics.record_transact_write_failure();
                    let error = retries.give_up_now(
                        failed,
                        &format!(
                            "transaction cancelled by item error(s) [{}]: {description}",
                            outcome.hard_failures.join(", ")
                        ),
                    );
                    return Err(retries.failed(error));
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
                        retries: retries.succeeded(),
                        suppressed,
                    });
                }

                let dropped_any = outcome.condition_failed > 0;
                let has_retryable = outcome.has_retryable;
                transact_items = outcome.kept;

                if has_retryable {
                    // Transient contention on a kept item: back off, retry and count it.
                    metrics.record_transact_write_failure();
                    break 'classify Some(format!(
                        "transaction cancelled by transient item error(s): {description}"
                    ));
                }
                if dropped_any {
                    // Sibling items were dropped for failing their condition, so
                    // resubmit the now-smaller set at once. That is no retry.
                    break 'classify None;
                }
                // Every item was rolled back with no error of its own.
                metrics.record_transact_write_failure();
                Some(format!(
                    "transaction cancelled with no item error: {description}"
                ))
            };
            let Some(cause) = cause else {
                continue;
            };

            if let Err(error) = retries.retry_or_give_up(transact_items.len(), &cause).await {
                return Err(retries.failed(error));
            }
        }
    };

    // One shutdown consult for the whole loop, so that no await inside it can
    // outlive the teardown, whether or not anyone remembers to say so.
    shutdown
        .run_until_cancelled(attempts)
        .await
        .unwrap_or_else(|| {
            Err(anyhow!(
                "dynamodb output connector '{endpoint_name}' gave up on a transaction of \
                 {item_count} item(s): the pipeline is shutting down"
            ))
        })
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
    Client::from_conf(client_config(config).build())
}

/// The SDK configuration for `config`: region, endpoint and credentials.
pub(crate) fn client_config(config: &DynamoDBWriterConfig) -> aws_sdk_dynamodb::config::Builder {
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
        config_builder.credentials_provider(credentials)
    } else {
        let provider = TOKIO.block_on(async {
            aws_config::default_provider::credentials::default_provider().await
        });
        config_builder.credentials_provider(provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::error::ErrorMetadata;
    use aws_sdk_dynamodb::types::Put;
    use aws_sdk_dynamodb::types::error::{
        ReplicatedWriteConflictException, ResourceNotFoundException, ThrottlingException,
        TransactionCanceledException,
    };
    use aws_smithy_runtime_api::http::{Response, StatusCode};
    use aws_smithy_types::body::SdkBody;
    use aws_smithy_types::retry::ErrorKind;
    use std::fmt;

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

    fn response(status: u16) -> HttpResponse {
        Response::new(StatusCode::try_from(status).unwrap(), SdkBody::empty())
    }

    /// A service error whose metadata carries only `code`, as the SDK builds
    /// one for a code it does not model.
    fn generic(code: Option<&str>, status: u16) -> SdkError<BatchWriteItemError, HttpResponse> {
        let mut meta = ErrorMetadata::builder();
        if let Some(code) = code {
            meta = meta.code(code);
        }
        SdkError::service_error(BatchWriteItemError::generic(meta.build()), response(status))
    }

    fn dispatch(error: ConnectorError) -> SdkError<BatchWriteItemError, HttpResponse> {
        SdkError::dispatch_failure(error)
    }

    fn boxed(text: &str) -> aws_sdk_dynamodb::error::BoxError {
        text.into()
    }

    /// An error with a source, to put a `CredentialsError` one level down.
    #[derive(Debug)]
    struct Wrapped(CredentialsError);

    impl fmt::Display for Wrapped {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "wrapped")
        }
    }

    impl StdError for Wrapped {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn every_listed_code_is_retryable_on_a_400() {
        for code in THROTTLING_ERROR_CODES
            .iter()
            .chain(TRANSIENT_ERROR_CODES)
            .chain(DYNAMODB_RETRYABLE_CODES)
        {
            assert_eq!(
                classify_sdk_error(&generic(Some(code), 400)),
                WriteErrorKind::Retryable,
                "{code}"
            );
        }
    }

    #[test]
    fn unlisted_codes_are_permanent_on_a_400() {
        for code in [
            "ValidationException",
            "ResourceNotFoundException",
            "AccessDeniedException",
            "ConditionalCheckFailedException",
            "IdempotentParameterMismatchException",
        ] {
            assert_eq!(
                classify_sdk_error(&generic(Some(code), 400)),
                WriteErrorKind::Permanent,
                "{code}"
            );
        }
    }

    #[test]
    fn server_statuses_are_retryable_whatever_the_code() {
        for status in RETRYABLE_HTTP_STATUSES {
            assert_eq!(
                classify_sdk_error(&generic(Some("ValidationException"), *status)),
                WriteErrorKind::Retryable,
                "{status}"
            );
            assert_eq!(
                classify_sdk_error(&generic(None, *status)),
                WriteErrorKind::Retryable,
                "{status}"
            );
        }
        for status in [400, 403, 499, 501, 505] {
            assert_eq!(
                classify_sdk_error(&generic(Some("ValidationException"), status)),
                WriteErrorKind::Permanent,
                "{status}"
            );
        }
    }

    #[test]
    fn absent_metadata_is_permanent() {
        assert_eq!(
            classify_sdk_error(&generic(None, 400)),
            WriteErrorKind::Permanent
        );
    }

    #[test]
    fn modeled_errors_classify_by_code_and_retry_kind() {
        // The variant the SDK models as retryable, with no code in its metadata.
        let conflict = SdkError::service_error(
            BatchWriteItemError::ReplicatedWriteConflictException(
                ReplicatedWriteConflictException::builder().build(),
            ),
            response(400),
        );
        assert_eq!(classify_sdk_error(&conflict), WriteErrorKind::Retryable);

        let throttled = SdkError::service_error(
            BatchWriteItemError::ThrottlingException(
                ThrottlingException::builder()
                    .meta(ErrorMetadata::builder().code("ThrottlingException").build())
                    .build(),
            ),
            response(400),
        );
        assert_eq!(classify_sdk_error(&throttled), WriteErrorKind::Retryable);

        let missing = SdkError::service_error(
            BatchWriteItemError::ResourceNotFoundException(
                ResourceNotFoundException::builder()
                    .meta(
                        ErrorMetadata::builder()
                            .code("ResourceNotFoundException")
                            .build(),
                    )
                    .build(),
            ),
            response(400),
        );
        assert_eq!(classify_sdk_error(&missing), WriteErrorKind::Permanent);

        let cancelled = SdkError::service_error(
            TransactWriteItemsError::TransactionCanceledException(
                TransactionCanceledException::builder()
                    .meta(
                        ErrorMetadata::builder()
                            .code("TransactionCanceledException")
                            .build(),
                    )
                    .build(),
            ),
            response(400),
        );
        assert_eq!(classify_sdk_error(&cancelled), WriteErrorKind::Retryable);
    }

    #[test]
    fn requests_without_a_response_classify_by_their_cause() {
        use WriteErrorKind::*;
        let cases: Vec<(
            &str,
            SdkError<BatchWriteItemError, HttpResponse>,
            WriteErrorKind,
        )> = vec![
            (
                "timeout",
                SdkError::timeout_error(boxed("timed out")),
                Retryable,
            ),
            (
                "response",
                SdkError::response_error(boxed("bad response"), response(200)),
                Retryable,
            ),
            (
                "construction",
                SdkError::construction_failure(boxed("bad input")),
                Permanent,
            ),
            (
                "io",
                dispatch(ConnectorError::io(boxed("connection reset"))),
                Retryable,
            ),
            (
                "connect timeout",
                dispatch(ConnectorError::timeout(boxed("connect timeout"))),
                Retryable,
            ),
            (
                "user",
                dispatch(ConnectorError::user(boxed("bad uri"))),
                Permanent,
            ),
            (
                "other transient",
                dispatch(ConnectorError::other(
                    boxed("x"),
                    Some(ErrorKind::TransientError),
                )),
                Retryable,
            ),
            (
                "other client",
                dispatch(ConnectorError::other(
                    boxed("x"),
                    Some(ErrorKind::ClientError),
                )),
                Retryable,
            ),
            (
                "other unknown",
                dispatch(ConnectorError::other(boxed("x"), None)),
                Retryable,
            ),
        ];
        for (name, error, expected) in cases {
            assert_eq!(classify_sdk_error(&error), expected, "{name}");
        }
    }

    #[test]
    fn credential_failures_are_permanent_only_when_the_configuration_is_invalid() {
        use WriteErrorKind::*;
        let invalid = || CredentialsError::invalid_configuration("no region");
        let cases: Vec<(&str, CredentialsError, WriteErrorKind)> = vec![
            ("invalid configuration", invalid(), Permanent),
            (
                "provider error",
                CredentialsError::provider_error("STS is down"),
                Retryable,
            ),
            (
                "provider timed out",
                CredentialsError::provider_timed_out(Duration::from_secs(1)),
                Retryable,
            ),
            (
                "not loaded",
                CredentialsError::not_loaded("no provider answered"),
                Retryable,
            ),
        ];
        for (name, error, expected) in cases {
            let error = dispatch(ConnectorError::other(Box::new(error), None));
            assert_eq!(classify_sdk_error(&error), expected, "{name}");
        }
        // The identity cache wraps the provider's error: the chain is walked.
        let wrapped = dispatch(ConnectorError::other(Box::new(Wrapped(invalid())), None));
        assert_eq!(classify_sdk_error(&wrapped), Permanent);
        let wrapped = dispatch(ConnectorError::other(
            Box::new(Wrapped(CredentialsError::provider_error("STS is down"))),
            None,
        ));
        assert_eq!(classify_sdk_error(&wrapped), Retryable);
    }

    #[test]
    fn a_service_error_is_described_without_its_raw_response() {
        let throttled = SdkError::service_error(
            BatchWriteItemError::ThrottlingException(
                ThrottlingException::builder()
                    .message("Throughput exceeds the current capacity")
                    .throttling_reasons(
                        ThrottlingReason::builder()
                            .reason("TableWriteKeyRangeThroughputExceeded")
                            .resource("arn:aws:dynamodb:us-east-1:1:table/t")
                            .build(),
                    )
                    .meta(
                        ErrorMetadata::builder()
                            .code("ThrottlingException")
                            .message("Throughput exceeds the current capacity")
                            .custom("aws_request_id", "req-1")
                            .build(),
                    )
                    .build(),
            ),
            response(400),
        );
        assert_eq!(
            describe_sdk_error(&throttled),
            "ThrottlingException: Throughput exceeds the current capacity (throttling reasons: \
             TableWriteKeyRangeThroughputExceeded on arn:aws:dynamodb:us-east-1:1:table/t) \
             (request id req-1)"
        );
        assert_eq!(
            describe_sdk_error(&generic(None, 400)),
            "unknown service error"
        );
    }

    #[test]
    fn a_failure_without_a_response_keeps_its_source_chain() {
        let error = dispatch(ConnectorError::other(
            Box::new(Wrapped(CredentialsError::provider_error("STS is down"))),
            None,
        ));
        let text = describe_sdk_error(&error);
        assert!(text.contains("wrapped"), "{text}");
        assert!(text.contains("STS is down"), "{text}");
    }

    #[test]
    fn the_retry_budget_counts_retries_after_the_first_request() {
        let none = RetryPolicy::new(Some(0));
        assert!(none.exhausted(0));
        let two = RetryPolicy::new(Some(2));
        assert!(!two.exhausted(0));
        assert!(!two.exhausted(1));
        assert!(two.exhausted(2));
        assert_eq!(two.budget(), " of 2");
        let unbounded = RetryPolicy::new(None);
        assert!(!unbounded.exhausted(1_000_000));
        assert_eq!(unbounded.budget(), "");
    }

    #[test]
    fn backoff_grows_to_its_ceiling_with_jitter() {
        for retry in 1..=12 {
            let ceiling = Duration::from_millis(50 * (1 << retry.min(8)));
            for _ in 0..20 {
                assert!(backoff_delay(retry) <= ceiling, "retry {retry}");
            }
        }
    }
}
