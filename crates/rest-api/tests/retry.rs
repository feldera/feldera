//! End-to-end tests for the client's retry layer: requests go to a local mock
//! server that scripts one HTTP response per connection.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use feldera_rest_api::{Client, RetryPolicy};
use feldera_types::transport::clock::ClockAdvanceRequest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Serve one scripted response per connection, counting requests. Closes each
/// connection after responding so every attempt opens a fresh one.
async fn mock_server(responses: Vec<String>) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let baseurl = format!("http://{}", listener.local_addr().unwrap());
    let request_count = Arc::new(AtomicUsize::new(0));
    let counter = request_count.clone();
    tokio::spawn(async move {
        for response in responses {
            let (mut stream, _) = listener.accept().await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            // Drain the request head; tests never send bodies large enough to
            // block the client on an unread request.
            let mut buf = [0u8; 4096];
            let mut head = Vec::new();
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                head.extend_from_slice(&buf[..n]);
                if n == 0 || head.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    (baseurl, request_count)
}

fn http_response(status: &str, body: &str) -> String {
    http_response_with(status, "", body)
}

/// `extra_headers` must be zero or more "Name: value\r\n" lines.
fn http_response_with(status: &str, extra_headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra_headers}Connection: close\r\n\r\n{body}",
        body.len()
    )
}

/// 503 whose wording proves the api-server never connected to the pipeline.
fn unreachable_connect_503() -> String {
    http_response(
        "503 Service Unavailable",
        r#"{"message": "Error sending HTTP request to pipeline: Failed to connect to host: Timeout while establishing connection", "error_code": "PipelineInteractionUnreachable", "details": {}}"#,
    )
}

/// 503 where the exchange timed out mid-flight: possibly already applied.
fn unreachable_timeout_503() -> String {
    http_response(
        "503 Service Unavailable",
        r#"{"message": "Error sending HTTP request to pipeline: timeout (5s) was reached", "error_code": "PipelineInteractionUnreachable", "details": {}}"#,
    )
}

fn fast_policy(max_retries: u32) -> RetryPolicy {
    RetryPolicy {
        max_retries,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(40),
    }
}

fn client(baseurl: &str, policy: RetryPolicy) -> Client {
    // reqwest is built without a rustls provider, so the process default
    // decides which one it uses.
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    Client::new_with_client(baseurl, reqwest::Client::new(), policy)
}

/// A GET is retried past a transient 503 and succeeds on the second attempt.
#[tokio::test]
async fn get_retries_transient_503_then_succeeds() {
    let (baseurl, requests) = mock_server(vec![
        http_response("503 Service Unavailable", "{}"),
        http_response("200 OK", "[]"),
    ])
    .await;
    let response = client(&baseurl, fast_policy(3))
        .list_api_keys()
        .send()
        .await;
    assert!(response.is_ok(), "{response:?}");
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

/// The final failure surfaces once retries are exhausted.
#[tokio::test]
async fn get_exhausts_retries_and_surfaces_error() {
    let (baseurl, requests) = mock_server(vec![
        http_response("503 Service Unavailable", "{}"),
        http_response("503 Service Unavailable", "{}"),
    ])
    .await;
    let response = client(&baseurl, fast_policy(1))
        .list_api_keys()
        .send()
        .await;
    assert!(response.is_err());
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

/// `max_retries: 0` disables retrying entirely.
#[tokio::test]
async fn zero_retries_sends_exactly_one_request() {
    let (baseurl, requests) =
        mock_server(vec![http_response("503 Service Unavailable", "{}")]).await;
    let response = client(&baseurl, RetryPolicy::none())
        .list_api_keys()
        .send()
        .await;
    assert!(response.is_err());
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

/// An idempotent GET is retried after the connection drops mid-exchange
/// (the server closes without sending a response).
#[tokio::test]
async fn get_retries_dropped_connection() {
    let (baseurl, requests) = mock_server(vec![
        String::new(), // accept, then close without responding
        http_response("200 OK", "[]"),
    ])
    .await;
    let response = client(&baseurl, fast_policy(3))
        .list_api_keys()
        .send()
        .await;
    assert!(response.is_ok(), "{response:?}");
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

/// A `Retry-After` header overrides the computed backoff: with a large
/// configured backoff and `Retry-After: 0`, the retry happens immediately.
#[tokio::test]
async fn retry_after_header_overrides_backoff() {
    let (baseurl, requests) = mock_server(vec![
        http_response_with("503 Service Unavailable", "Retry-After: 0\r\n", "{}"),
        http_response("200 OK", "[]"),
    ])
    .await;
    let slow_policy = RetryPolicy {
        max_retries: 3,
        initial_backoff: Duration::from_secs(30),
        max_backoff: Duration::from_secs(60),
    };
    let started = std::time::Instant::now();
    let response = client(&baseurl, slow_policy).list_api_keys().send().await;
    assert!(response.is_ok(), "{response:?}");
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "Retry-After: 0 was ignored; waited {:?}",
        started.elapsed()
    );
}

/// A non-idempotent POST must not repeat a 503 that may already have been
/// applied; the inspected response is rebuilt and still parses.
#[tokio::test]
async fn non_idempotent_post_returns_ambiguous_503_unretried() {
    let (baseurl, requests) = mock_server(vec![unreachable_timeout_503()]).await;
    let response = client(&baseurl, fast_policy(3))
        .clock_advance()
        .pipeline_name("p")
        .body(ClockAdvanceRequest {
            delta_ms: Some(100),
        })
        .send()
        .await;
    match response {
        Err(feldera_rest_api::Error::ErrorResponse(e)) => {
            assert_eq!(e.error_code, "PipelineInteractionUnreachable");
        }
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

/// A non-idempotent POST is retried when the 503 proves the request never
/// reached the pipeline.
#[tokio::test]
async fn non_idempotent_post_retries_connect_phase_503() {
    let (baseurl, requests) = mock_server(vec![
        unreachable_connect_503(),
        http_response(
            "200 OK",
            r#"{"now_ms": 1000, "now": "1970-01-01 00:00:01"}"#,
        ),
    ])
    .await;
    let response = client(&baseurl, fast_policy(3))
        .clock_advance()
        .pipeline_name("p")
        .body(ClockAdvanceRequest {
            delta_ms: Some(100),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.now_ms, 1000);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}
