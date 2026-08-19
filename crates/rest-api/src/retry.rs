//! Retry layer for the generated client.
//!
//! Every request the generated `Client` sends passes through
//! [`execute_with_retry`] (wired up by the `ClientHooks` impl in `lib.rs`),
//! which resends requests that failed for reasons known to be transient:
//! transport failures and the HTTP statuses in [`RETRYABLE_STATUSES`]. Waits
//! between attempts grow exponentially; a `Retry-After` header overrides the
//! computed wait, and a 502 waits based on a cluster-health probe (see
//! [`cluster_is_healthy`]).
//!
//! Repeating a request is only safe when the first attempt provably caused no
//! server-side effect, or when the operation yields the same state however
//! often it runs. [`is_idempotent`] classifies operations; non-idempotent
//! ones retry only failures where the request never reached its target.

use std::time::Duration;

use reqwest::header::HeaderMap;
use reqwest::{Method, Request, Response, StatusCode};

/// Retry behavior for transient request failures.
///
/// Carried by `Client` as its inner state: construct one and pass it to
/// `Client::new` / `Client::new_with_client`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Retries after the initial attempt. `3` means up to 4 attempts total;
    /// `0` disables retrying.
    pub max_retries: u32,
    /// Wait before the first retry. Each further retry doubles the wait.
    pub initial_backoff: Duration,
    /// Upper bound on the wait between attempts.
    pub max_backoff: Duration,
    /// Flat wait between 502 retries while the cluster reports unhealthy on
    /// `/v0/cluster_healthz`: the cluster is likely upgrading or restarting,
    /// so a flat pause beats an exponential ramp. Not capped by
    /// `max_backoff`.
    pub unhealthy_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(60),
            unhealthy_backoff: Duration::from_secs(90),
        }
    }
}

impl RetryPolicy {
    /// A policy that never retries.
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            ..Self::default()
        }
    }

    /// Wait before retry number `retry_index` (zero-based).
    fn backoff(&self, retry_index: u32) -> Duration {
        self.initial_backoff
            .saturating_mul(2u32.saturating_pow(retry_index))
            .min(self.max_backoff)
    }
}

/// Statuses that signal a transient condition worth retrying (for idempotent
/// operations): request timeout, rate limit, and gateway/service failures.
const RETRYABLE_STATUSES: [StatusCode; 5] = [
    StatusCode::REQUEST_TIMEOUT,
    StatusCode::TOO_MANY_REQUESTS,
    StatusCode::BAD_GATEWAY,
    StatusCode::SERVICE_UNAVAILABLE,
    StatusCode::GATEWAY_TIMEOUT,
];

/// Mutating operations that are nonetheless safe to repeat: desired-state
/// setters (repeating yields the same desired state) and pure functions of
/// their input. Operations absent from this list retry only failures where
/// the request never reached its target.
const IDEMPOTENT_OPERATIONS: &[&str] = &[
    "post_pipeline_start",
    "post_pipeline_pause",
    "post_pipeline_resume",
    "post_pipeline_stop",
    "post_pipeline_clear",
    "post_pipeline_activate",
    "post_pipeline_approve",
    "post_pipeline_dismiss_error",
    "post_pipeline_input_connector_action",
    "post_pipeline_diff",
    "post_validate_program",
    // Set-based updates: repeating writes the same field values.
    "patch_pipeline",
    "patch_tenant",
];

/// GET/HEAD/PUT/DELETE are idempotent by HTTP semantics; POST and PATCH only
/// when the operation is known to be safe to repeat.
fn is_idempotent(method: &Method, operation_id: &str) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::PUT | Method::DELETE
    ) || IDEMPOTENT_OPERATIONS.contains(&operation_id)
}

/// Whether a 503 body proves the request never reached the pipeline, making a
/// retry safe even for non-idempotent operations.
///
/// The api-server proxies pipeline-interaction endpoints and answers 503
/// `PipelineInteractionUnreachable` for every proxy failure. Only the
/// connect-phase failure ("Failed to connect to host", the awc connect error
/// prefix) guarantees the pipeline never saw the request; an exchange that
/// timed out or disconnected mid-flight carries different wording and may
/// already have been applied. If the manager ever rewords the message, this
/// check fails closed: the error surfaces as before, nothing double-applies.
fn is_never_dispatched_503(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body).is_ok_and(|v| {
        v.get("error_code").and_then(|c| c.as_str()) == Some("PipelineInteractionUnreachable")
            && v.get("message")
                .and_then(|m| m.as_str())
                .is_some_and(|m| m.contains("Failed to connect to host"))
    })
}

/// Rebuild a response consumed while inspecting its body, so the caller can
/// parse the error as if the response arrived untouched. The rebuilt response
/// loses request metadata such as the URL, which error handling never reads.
fn rebuild_response(status: StatusCode, headers: HeaderMap, body: bytes::Bytes) -> Response {
    let mut rebuilt = http::Response::new(body);
    *rebuilt.status_mut() = status;
    *rebuilt.headers_mut() = headers;
    Response::from(rebuilt)
}

/// Server-requested wait from a `Retry-After` header (seconds form only; the
/// HTTP-date form is rare and falls back to the computed backoff).
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let secs = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(secs))
}

/// How long the `/v0/cluster_healthz` probe may take before the cluster
/// counts as unhealthy.
const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Probe `/v0/cluster_healthz` to tell a spurious 502 (retry immediately)
/// from an unhealthy cluster, e.g. one whose upgrade is in progress (flat
/// long wait). The endpoint answers 200 only when every service is healthy;
/// any other status or a probe failure counts as unhealthy.
async fn cluster_is_healthy(client: &reqwest::Client, baseurl: &str) -> bool {
    let url = format!("{}/v0/cluster_healthz", baseurl.trim_end_matches('/'));
    match client.get(url).timeout(HEALTH_PROBE_TIMEOUT).send().await {
        Ok(response) => response.status().is_success(),
        Err(_) => false,
    }
}

/// Pick the wait before the next retry:
/// a `Retry-After` value from the server wins (capped at `max_backoff`);
/// a 502 from a healthy cluster was spurious, so retry immediately;
/// a 502 from an unhealthy cluster (e.g. an upgrade in progress) waits the
/// flat `unhealthy_backoff`; everything else backs off exponentially.
fn next_wait(
    policy: &RetryPolicy,
    retry_index: u32,
    server_wait: Option<Duration>,
    cluster_healthy_after_502: Option<bool>,
) -> Duration {
    match (server_wait, cluster_healthy_after_502) {
        (Some(server_wait), _) => server_wait.min(policy.max_backoff),
        (None, Some(true)) => Duration::ZERO,
        (None, Some(false)) => policy.unhealthy_backoff,
        (None, None) => policy.backoff(retry_index),
    }
}

enum Verdict {
    Return(Response),
    Retry {
        reason: &'static str,
        /// Wait requested by the server via `Retry-After`; overrides the
        /// computed backoff (still capped at `max_backoff`).
        server_wait: Option<Duration>,
        status: StatusCode,
    },
}

/// Decide whether a completed exchange warrants a retry. Consumes the
/// response only when a body inspection is needed; hands it back otherwise.
async fn judge_response(response: Response, idempotent: bool) -> reqwest::Result<Verdict> {
    let status = response.status();
    if !RETRYABLE_STATUSES.contains(&status) {
        return Ok(Verdict::Return(response));
    }
    let server_wait = retry_after(response.headers());
    if idempotent {
        return Ok(Verdict::Retry {
            reason: "transient HTTP status",
            server_wait,
            status,
        });
    }
    if status != StatusCode::SERVICE_UNAVAILABLE {
        return Ok(Verdict::Return(response));
    }
    let headers = response.headers().clone();
    let body = response.bytes().await?;
    if is_never_dispatched_503(&body) {
        Ok(Verdict::Retry {
            reason: "pipeline unreachable, request never sent",
            server_wait,
            status,
        })
    } else {
        Ok(Verdict::Return(rebuild_response(status, headers, body)))
    }
}

/// Execute `request`, retrying transient failures per `policy`.
///
/// Requests with streaming bodies cannot be cloned and get a single attempt.
pub(crate) async fn execute_with_retry(
    client: &reqwest::Client,
    policy: &RetryPolicy,
    baseurl: &str,
    mut request: Request,
    operation_id: &str,
) -> reqwest::Result<Response> {
    let idempotent = is_idempotent(request.method(), operation_id);
    let mut retry_index = 0u32;
    loop {
        // Clone before executing: execute() consumes the request, and a
        // failed attempt leaves nothing to resend.
        let retry_request = if retry_index < policy.max_retries {
            request.try_clone()
        } else {
            None
        };

        let retries_remain = retry_index < policy.max_retries;
        let outcome = client.execute(request).await;

        let Some(retry_request) = retry_request else {
            // Final attempt: retries exhausted, or a streaming body that
            // cannot be resent.
            let failed_transiently = match &outcome {
                Ok(response) => RETRYABLE_STATUSES.contains(&response.status()),
                Err(_) => true,
            };
            if retries_remain && failed_transiently {
                log::info!(
                    "{operation_id}: not retrying a transient failure because the request body is a stream and cannot be resent"
                );
            }
            return outcome;
        };

        let (reason, detail, server_wait, retried_status) = match outcome {
            Ok(response) => match judge_response(response, idempotent).await? {
                Verdict::Return(response) => return Ok(response),
                Verdict::Retry {
                    reason,
                    server_wait,
                    status,
                } => (reason, String::new(), server_wait, Some(status)),
            },
            Err(error) => {
                // Idempotent operations repeat on any transport failure
                // (timeouts, resets mid-exchange). A connect failure never
                // reached the server, so it is safe for every operation;
                // other failures may already have been applied.
                if !(idempotent || error.is_connect()) {
                    return Err(error);
                }
                ("transport error", format!(": {error}"), None, None)
            }
        };

        request = retry_request;
        // A 502 comes from in front of the api-server; probe cluster health
        // to pick the right wait (see `next_wait`). Skipped when the server
        // already prescribed a wait via `Retry-After`.
        let cluster_healthy_after_502 =
            if server_wait.is_none() && retried_status == Some(StatusCode::BAD_GATEWAY) {
                Some(cluster_is_healthy(client, baseurl).await)
            } else {
                None
            };
        let wait = next_wait(policy, retry_index, server_wait, cluster_healthy_after_502);
        retry_index += 1;
        log::debug!(
            "{operation_id}: {reason}{detail} - retrying in {}s (attempt {} of {})",
            wait.as_secs(),
            retry_index + 1,
            policy.max_retries + 1,
        );
        tokio::time::sleep(wait).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Backoff doubles per retry and is capped at `max_backoff`.
    #[test]
    fn backoff_doubles_and_caps() {
        let policy = RetryPolicy {
            max_retries: 5,
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(6),
            ..RetryPolicy::default()
        };
        assert_eq!(policy.backoff(0), Duration::from_secs(2));
        assert_eq!(policy.backoff(1), Duration::from_secs(4));
        assert_eq!(policy.backoff(2), Duration::from_secs(6));
        assert_eq!(policy.backoff(30), Duration::from_secs(6));
    }

    /// Idempotency follows the HTTP method except for allowlisted operations.
    #[test]
    fn idempotency_classification() {
        assert!(is_idempotent(&Method::GET, "get_pipeline"));
        assert!(is_idempotent(&Method::PUT, "put_pipeline"));
        assert!(is_idempotent(&Method::DELETE, "delete_pipeline"));
        assert!(is_idempotent(&Method::POST, "post_pipeline_start"));
        assert!(!is_idempotent(&Method::POST, "clock_advance"));
        assert!(!is_idempotent(&Method::POST, "http_input"));
        assert!(!is_idempotent(&Method::PATCH, "patch_something_new"));
    }

    /// Wait selection: `Retry-After` wins (capped), a 502 waits per cluster
    /// health, everything else backs off exponentially.
    #[test]
    fn next_wait_selection() {
        let policy = RetryPolicy {
            max_retries: 3,
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(60),
            unhealthy_backoff: Duration::from_secs(90),
        };
        // Server-prescribed wait wins, capped at max_backoff.
        assert_eq!(
            next_wait(&policy, 0, Some(Duration::from_secs(7)), None),
            Duration::from_secs(7)
        );
        assert_eq!(
            next_wait(&policy, 0, Some(Duration::from_secs(600)), None),
            Duration::from_secs(60)
        );
        // 502 with healthy cluster: immediate; unhealthy: flat, uncapped.
        assert_eq!(next_wait(&policy, 0, None, Some(true)), Duration::ZERO);
        assert_eq!(
            next_wait(&policy, 0, None, Some(false)),
            Duration::from_secs(90)
        );
        // Otherwise exponential.
        assert_eq!(next_wait(&policy, 1, None, None), Duration::from_secs(4));
    }

    /// Only the seconds form of `Retry-After` yields a wait.
    #[test]
    fn retry_after_parses_seconds_only() {
        let with = |v: &str| {
            let mut h = HeaderMap::new();
            h.insert(reqwest::header::RETRY_AFTER, v.parse().unwrap());
            h
        };
        assert_eq!(retry_after(&with("7")), Some(Duration::from_secs(7)));
        assert_eq!(retry_after(&with("Wed, 21 Oct 2026 07:28:00 GMT")), None);
        assert_eq!(retry_after(&with("-3")), None);
        assert_eq!(retry_after(&HeaderMap::new()), None);
    }

    /// Only the connect-phase wording marks a 503 as safe for any operation.
    #[test]
    fn never_dispatched_detection() {
        let connect = br#"{"message": "Failed to connect to host: Timeout while establishing connection", "error_code": "PipelineInteractionUnreachable", "details": {}}"#;
        let timeout = br#"{"message": "timeout (5s) was reached", "error_code": "PipelineInteractionUnreachable", "details": {}}"#;
        let other_code = br#"{"message": "Failed to connect to host", "error_code": "PipelineInteractionNotDeployed", "details": {}}"#;
        assert!(is_never_dispatched_503(connect));
        assert!(!is_never_dispatched_503(timeout));
        assert!(!is_never_dispatched_503(other_code));
        assert!(!is_never_dispatched_503(b"not json"));
    }
}
