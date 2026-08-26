//! Raw JSON transport for API reads.
//!
//! Reads bypass the generated OpenAPI types on purpose: a monitoring console
//! must render whatever any server version returns, so payloads stay
//! `serde_json::Value` until the tolerant mappers in [`crate::model`] shape
//! them.

use serde_json::Value;
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::options::ConnectionOptions;

/// A failed API call, reduced to what the status line can show.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApiError {
    /// The server answered with a non-success status.
    Status { status: u16, message: String },
    /// The request never completed (DNS, TCP, TLS, timeout).
    Transport(String),
    /// The response body was not JSON.
    Decode(String),
    /// A multi-step operation gave up waiting for a state change.
    Timeout(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status { status, message } => write!(formatter, "HTTP {status}: {message}"),
            Self::Transport(message) => write!(formatter, "connection failed: {message}"),
            Self::Decode(message) => write!(formatter, "unreadable response: {message}"),
            Self::Timeout(message) => write!(formatter, "timed out: {message}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    /// Whether the instance itself is unreachable, as opposed to one endpoint
    /// rejecting one request.
    pub fn is_connection_loss(&self) -> bool {
        matches!(self, Self::Transport(_))
    }
}

/// A body the server produces asynchronously, such as a Samply profile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Download {
    Ready(Vec<u8>),
    /// HTTP 204: ask again after this many seconds.
    Pending {
        retry_after_secs: u64,
    },
}

impl Download {
    /// Poll cadence when a 204 carries no usable `Retry-After` header.
    pub const DEFAULT_RETRY_SECS: u64 = 2;
}

/// Shared raw HTTP client. Cheap to clone; the tenant override is shared
/// between clones so a tenant switch applies everywhere at once.
#[derive(Clone)]
pub struct Http {
    base_url: String,
    client: reqwest::Client,
    tenant: Arc<RwLock<Option<String>>>,
    minter: Option<TokenMinter>,
}

/// Runs the configured auth command and caches its short-lived token.
#[derive(Clone)]
pub struct TokenMinter {
    command: String,
    cache: Arc<RwLock<Option<(String, std::time::Instant)>>>,
}

impl TokenMinter {
    /// tsidp guarantees at least a minute of validity on whatever the mint
    /// script prints, so a shorter in-process cache is always safe.
    const CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(45);

    pub fn new(command: String) -> Self {
        Self {
            command,
            cache: Arc::new(RwLock::new(None)),
        }
    }

    /// The cached token if it is still fresh.
    pub fn current(&self) -> Option<String> {
        let cache = self.cache.read().expect("token cache lock");
        cache.as_ref().and_then(|(token, minted_at)| {
            (minted_at.elapsed() < Self::CACHE_TTL).then(|| token.clone())
        })
    }

    /// A fresh token, minting through the auth command when the cache aged.
    pub async fn token(&self) -> Result<String, ApiError> {
        if let Some(token) = self.current() {
            return Ok(token);
        }
        let token = mint_token(&self.command).await?;
        *self.cache.write().expect("token cache lock") =
            Some((token.clone(), std::time::Instant::now()));
        Ok(token)
    }
}

/// Run the auth command and return its stdout as the bearer token.
async fn mint_token(command: &str) -> Result<String, ApiError> {
    let auth_error = |message: String| ApiError::Transport(format!("auth command: {message}"));
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        tokio::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .output(),
    )
    .await
    .map_err(|_| auth_error("timed out after 20s".to_string()))?
    .map_err(|error| auth_error(error.to_string()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(auth_error(format!(
            "exited with {}: {}",
            output.status,
            crate::model::text::terminal_safe(stderr.trim())
        )));
    }
    let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if token.is_empty() {
        return Err(auth_error("printed no token".to_string()));
    }
    Ok(token)
}

impl Http {
    pub fn new(options: &ConnectionOptions) -> anyhow::Result<Self> {
        Ok(Self {
            base_url: options.host.clone(),
            client: options.build_http_client()?,
            tenant: Arc::new(RwLock::new(options.tenant.clone())),
            minter: options.auth_command.clone().map(TokenMinter::new),
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn tenant(&self) -> Option<String> {
        self.tenant.read().expect("tenant lock").clone()
    }

    pub fn set_tenant(&self, tenant: Option<String>) {
        *self.tenant.write().expect("tenant lock") = tenant;
    }

    /// The token minter, when `--auth-command` is configured; the typed
    /// client uses it to keep its default headers fresh.
    pub fn minter(&self) -> Option<&TokenMinter> {
        self.minter.as_ref()
    }

    /// GET a JSON document.
    pub async fn get_json(&self, path: &str) -> Result<Value, ApiError> {
        let response = self.send(self.client.get(self.url(path))).await?;
        response
            .json()
            .await
            .map_err(|error| ApiError::Decode(error.to_string()))
    }

    /// Downloads get their own limit: `--timeout-secs` is sized for API
    /// calls, and a profile of a busy pipeline runs to tens of megabytes.
    const DOWNLOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15 * 60);

    /// GET a raw body that the server may still be producing: a 204 with
    /// `Retry-After` means "not yet", anything else successful is the body.
    pub async fn get_download(&self, path: &str) -> Result<Download, ApiError> {
        let request = self
            .client
            .get(self.url(path))
            .timeout(Self::DOWNLOAD_TIMEOUT);
        let response = self.send(request).await?;
        if response.status() == reqwest::StatusCode::NO_CONTENT {
            let retry_after_secs = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|text| text.trim().parse::<u64>().ok())
                .unwrap_or(Download::DEFAULT_RETRY_SECS);
            return Ok(Download::Pending { retry_after_secs });
        }
        response
            .bytes()
            .await
            .map(|bytes| Download::Ready(bytes.to_vec()))
            .map_err(|error| {
                ApiError::Transport(format!(
                    "download interrupted: {}",
                    concise_reqwest_error(&error)
                ))
            })
    }

    /// POST with an empty body, for lifecycle actions; the response body is
    /// only consulted for error messages.
    pub async fn post(&self, path: &str) -> Result<(), ApiError> {
        self.send(self.client.post(self.url(path))).await?;
        Ok(())
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// GET a body as a live line stream, forwarding each line into `sink`
    /// until the stream ends or the receiver is dropped. Used for `/logs`.
    pub async fn stream_lines(
        &self,
        path: &str,
        sink: &tokio::sync::mpsc::UnboundedSender<String>,
    ) -> Result<(), ApiError> {
        // The client-level timeout would sever a long-lived stream.
        let request = self
            .client
            .get(self.url(path))
            .timeout(std::time::Duration::from_secs(60 * 60 * 24));
        let mut response = self.send(request).await?;
        let mut pending = Vec::new();
        loop {
            let chunk = match response.chunk().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => {
                    if !pending.is_empty() {
                        let _ = sink.send(String::from_utf8_lossy(&pending).into_owned());
                    }
                    return Ok(());
                }
                Err(error) => return Err(ApiError::Transport(concise_reqwest_error(&error))),
            };
            pending.extend_from_slice(&chunk);
            while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                let line: Vec<u8> = pending.drain(..=newline).collect();
                let mut end = line.len() - 1;
                if end > 0 && line[end - 1] == b'\r' {
                    end -= 1;
                }
                let text = String::from_utf8_lossy(&line[..end]).into_owned();
                if sink.send(text).is_err() {
                    // The viewer went away; stop pulling.
                    return Ok(());
                }
            }
        }
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<reqwest::Response, ApiError> {
        let request = match self.tenant() {
            Some(tenant) => request.header("Feldera-Tenant", tenant),
            None => request,
        };
        let request = match &self.minter {
            Some(minter) => request.bearer_auth(minter.token().await?),
            None => request,
        };
        let response = request
            .send()
            .await
            .map_err(|error| ApiError::Transport(concise_reqwest_error(&error)))?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        Err(ApiError::Status {
            status: status.as_u16(),
            message: error_message(&body),
        })
    }
}

/// Prefer the API's own `message` field over a raw error body.
fn error_message(body: &str) -> String {
    let message = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|json| {
            json.get("message")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| body.trim().to_string());
    let message = crate::model::text::terminal_safe(message.trim());
    if message.is_empty() {
        "no further detail from the server".to_string()
    } else {
        const LIMIT: usize = 300;
        message.chars().take(LIMIT).collect()
    }
}

/// Reqwest error chains repeat the URL several times; keep the root cause.
fn concise_reqwest_error(error: &reqwest::Error) -> String {
    let mut source: &dyn std::error::Error = error;
    while let Some(inner) = source.source() {
        source = inner;
    }
    if error.is_timeout() {
        "request timed out".to_string()
    } else {
        source.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{ApiError, Download, Http, error_message};
    use crate::options::ConnectionOptions;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn http(server: &MockServer) -> Http {
        Http::new(&ConnectionOptions {
            host: server.uri(),
            ..Default::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn get_json_returns_the_document() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v0/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"a": 1})))
            .mount(&server)
            .await;
        let value = http(&server).await.get_json("/v0/config").await.unwrap();
        assert_eq!(value["a"], 1);
    }

    #[tokio::test]
    async fn error_statuses_carry_the_api_message() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(serde_json::json!({"message": "Unknown pipeline"})),
            )
            .mount(&server)
            .await;
        let error = http(&server).await.get_json("/nope").await.unwrap_err();
        assert_eq!(
            error,
            ApiError::Status {
                status: 404,
                message: "Unknown pipeline".to_string()
            }
        );
        assert!(!error.is_connection_loss());
        assert_eq!(error.to_string(), "HTTP 404: Unknown pipeline");
    }

    #[tokio::test]
    async fn tenant_switches_apply_to_every_clone() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(header("Feldera-Tenant", "acme"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let original = http(&server).await;
        let clone = original.clone();
        original.set_tenant(Some("acme".to_string()));
        clone.get_json("/v0/pipelines").await.unwrap();
        assert_eq!(clone.tenant().as_deref(), Some("acme"));
    }

    #[tokio::test]
    async fn downloads_outlive_the_per_request_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/slow"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"late".to_vec())
                    .set_delay(std::time::Duration::from_millis(1_500)),
            )
            .mount(&server)
            .await;
        let http = Http::new(&ConnectionOptions {
            host: server.uri(),
            timeout_secs: 1,
            ..Default::default()
        })
        .unwrap();
        assert!(
            http.get_json("/slow").await.is_err(),
            "API calls keep the short limit"
        );
        assert_eq!(
            http.get_download("/slow").await.unwrap(),
            Download::Ready(b"late".to_vec())
        );
    }

    /// A one-shot HTTP/1.1 server running `script` on the accepted socket.
    /// wiremock delays or ends a response as a whole; these tests need a body
    /// that stalls or breaks after the headers went out.
    async fn one_shot_server<F, Fut>(script: F) -> String
    where
        F: FnOnce(tokio::net::TcpStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            script(socket).await;
        });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn slow_download_bodies_outlive_the_per_request_timeout() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let host = one_shot_server(|mut socket| async move {
            let mut head = [0u8; 1024];
            let _ = socket.read(&mut head).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/gzip\r\nContent-Length: 4\r\n\r\n")
                .await
                .unwrap();
            // The headers are out; the body arrives after the 1 s API limit.
            tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
            socket.write_all(b"late").await.unwrap();
        })
        .await;
        let http = Http::new(&ConnectionOptions {
            host,
            timeout_secs: 1,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            http.get_download("/blob").await.unwrap(),
            Download::Ready(b"late".to_vec())
        );
    }

    #[tokio::test]
    async fn interrupted_downloads_name_the_cause() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let host = one_shot_server(|mut socket| async move {
            let mut head = [0u8; 1024];
            let _ = socket.read(&mut head).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/gzip\r\nContent-Length: 64\r\n\r\npartial")
                .await
                .unwrap();
            // Dropping the socket cuts the body short of the promised length.
        })
        .await;
        let http = Http::new(&ConnectionOptions {
            host,
            ..Default::default()
        })
        .unwrap();
        let error = http.get_download("/blob").await.unwrap_err();
        assert!(error.is_connection_loss(), "{error}");
        let text = error.to_string();
        assert!(text.contains("download interrupted"), "{text}");
        assert!(!text.contains("error decoding response body"), "{text}");
    }

    #[tokio::test]
    async fn post_discards_success_bodies_and_downloads_distinguish_pending() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/start"))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/blob"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"abc".to_vec()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/later"))
            .respond_with(ResponseTemplate::new(204).insert_header("Retry-After", "7"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/later-unspecified"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let http = http(&server).await;
        http.post("/start").await.unwrap();
        assert_eq!(
            http.get_download("/blob").await.unwrap(),
            Download::Ready(b"abc".to_vec())
        );
        assert_eq!(
            http.get_download("/later").await.unwrap(),
            Download::Pending {
                retry_after_secs: 7
            }
        );
        assert_eq!(
            http.get_download("/later-unspecified").await.unwrap(),
            Download::Pending {
                retry_after_secs: Download::DEFAULT_RETRY_SECS
            }
        );
    }

    #[tokio::test]
    async fn auth_commands_mint_cache_and_attach_bearer_tokens() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(header("Authorization", "Bearer minted-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(2)
            .mount(&server)
            .await;
        // The command counts its invocations through a side-effect file.
        let counter =
            std::env::temp_dir().join(format!("f5a-mint-count-{}-{}", std::process::id(), line!()));
        let _ = std::fs::remove_file(&counter);
        let http = Http::new(&ConnectionOptions {
            host: server.uri(),
            auth_command: Some(format!(
                "echo x >> {}; echo minted-token",
                counter.display()
            )),
            ..Default::default()
        })
        .unwrap();
        http.get_json("/a").await.unwrap();
        http.get_json("/b").await.unwrap();
        let mints = std::fs::read_to_string(&counter).unwrap();
        assert_eq!(
            mints.lines().count(),
            1,
            "the second request hits the cache"
        );
        let _ = std::fs::remove_file(&counter);
    }

    #[tokio::test]
    async fn failing_auth_commands_surface_clearly() {
        let http = Http::new(&ConnectionOptions {
            host: "http://127.0.0.1:9".to_string(),
            auth_command: Some("echo broken >&2; exit 3".to_string()),
            ..Default::default()
        })
        .unwrap();
        let error = http.get_json("/x").await.unwrap_err();
        assert!(error.to_string().contains("auth command"), "{error}");
        assert!(error.to_string().contains("broken"), "{error}");

        let silent = Http::new(&ConnectionOptions {
            host: "http://127.0.0.1:9".to_string(),
            auth_command: Some("true".to_string()),
            ..Default::default()
        })
        .unwrap();
        let error = silent.get_json("/x").await.unwrap_err();
        assert!(error.to_string().contains("no token"), "{error}");
    }

    #[tokio::test]
    async fn line_streams_split_chunks_and_strip_carriage_returns() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/logs"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string("first\r\nsecond\nlast without newline"),
            )
            .mount(&server)
            .await;
        let http = http(&server).await;
        let (line_tx, mut line_rx) = tokio::sync::mpsc::unbounded_channel();
        http.stream_lines("/logs", &line_tx).await.unwrap();
        drop(line_tx);
        let mut lines = Vec::new();
        while let Some(line) = line_rx.recv().await {
            lines.push(line);
        }
        assert_eq!(lines, vec!["first", "second", "last without newline"]);
    }

    #[tokio::test]
    async fn line_streams_report_http_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(serde_json::json!({"message": "no such pipeline"})),
            )
            .mount(&server)
            .await;
        let http = http(&server).await;
        let (line_tx, _line_rx) = tokio::sync::mpsc::unbounded_channel();
        let error = http.stream_lines("/logs", &line_tx).await.unwrap_err();
        assert!(error.to_string().contains("no such pipeline"));
    }

    #[tokio::test]
    async fn unreachable_hosts_are_connection_loss() {
        let http = Http::new(&ConnectionOptions {
            // Reserved TEST-NET address: nothing listens there.
            host: "http://192.0.2.1:9".to_string(),
            timeout_secs: 1,
            ..Default::default()
        })
        .unwrap();
        let error = http.get_json("/v0/config").await.unwrap_err();
        assert!(error.is_connection_loss());
    }

    #[test]
    fn error_messages_are_extracted_sanitized_and_bounded() {
        assert_eq!(error_message(r#"{"message": "boom"}"#), "boom");
        assert_eq!(error_message("plain text"), "plain text");
        assert_eq!(error_message("  "), "no further detail from the server");
        assert_eq!(error_message("bad\u{1b}[2Jesc"), "bad�[2Jesc");
        assert_eq!(error_message(&"x".repeat(500)).chars().count(), 300);
    }
}
