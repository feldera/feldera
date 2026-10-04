//! Raw JSON transport for API reads.
//!
//! Reads bypass the generated OpenAPI types on purpose: a monitoring console
//! must render whatever any server version returns, so payloads stay
//! `serde_json::Value` until the tolerant mappers in [`crate::model`] shape
//! them.

use serde_json::Value;
use std::fmt;
use std::sync::{Arc, RwLock};

use std::path::PathBuf;

use crate::options::{ConnectionOptions, check_oidc_token};

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
    /// Whether `--header` sets `Feldera-Tenant`, which then wins over the
    /// tenant picker, as `--header` wins over everything f5a sends.
    is_tenant_overridden: bool,
    token_file: Option<TokenFile>,
}

/// A bearer token kept current by whatever writes the file (a projected
/// volume, a refresh loop). Read before every request, so a rotation
/// applies at once and f5a never holds a stale copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenFile {
    path: PathBuf,
}

impl TokenFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The file's token, with `fda`'s checks.
    pub async fn token(&self) -> Result<String, ApiError> {
        let contents = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|error| {
                ApiError::Transport(format!(
                    "failed to read OIDC token file `{}`: {error}",
                    self.path.display()
                ))
            })?;
        check_oidc_token(&self.path, &contents).map_err(ApiError::Transport)
    }
}

impl Http {
    pub fn new(options: &ConnectionOptions) -> anyhow::Result<Self> {
        Ok(Self {
            base_url: options.host.clone(),
            client: options.build_http_client()?,
            tenant: Arc::new(RwLock::new(options.tenant.clone())),
            is_tenant_overridden: options.overrides_header("Feldera-Tenant"),
            token_file: options
                .bearer_token_file()
                .map(|path| TokenFile::new(path.to_path_buf())),
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

    /// The token file, when one authorizes requests; the typed client
    /// reads it too, to keep its default headers fresh.
    pub fn token_file(&self) -> Option<&TokenFile> {
        self.token_file.as_ref()
    }

    /// GET a JSON document.
    pub async fn get_json(&self, path: &str) -> Result<Value, ApiError> {
        self.fetch_json(self.client.get(self.url(path))).await
    }

    /// GET a JSON document that can run to tens of megabytes, such as a
    /// circuit profile, which needs longer than `--timeout`.
    pub async fn get_large_json(&self, path: &str) -> Result<Value, ApiError> {
        let request = self
            .client
            .get(self.url(path))
            .timeout(Self::LARGE_JSON_TIMEOUT);
        self.fetch_json(request).await
    }

    /// The gzipped circuit profile of a large pipeline (about 60 MB) takes
    /// 15-25 s to load.
    const LARGE_JSON_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

    async fn fetch_json(&self, request: reqwest::RequestBuilder) -> Result<Value, ApiError> {
        let body = read_body(self.send(request).await?).await?;
        serde_json::from_slice(&body).map_err(|error| ApiError::Decode(error.to_string()))
    }

    /// Downloads get their own limit: `--timeout` is sized for API
    /// calls, and a profile of a busy pipeline runs to tens of megabytes.
    const DOWNLOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15 * 60);

    /// GET a raw body that the server may still be producing: a 204 with
    /// `Retry-After` means "not yet", anything else successful is the body.
    pub async fn get_download(&self, path: &str) -> Result<Download, ApiError> {
        // Bundles and recordings are archives already; gzip only costs server CPU.
        let request = self
            .client
            .get(self.url(path))
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
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
        read_body(response)
            .await
            .map(Download::Ready)
            .map_err(|error| match error {
                ApiError::Transport(cause) => {
                    ApiError::Transport(format!("download interrupted: {cause}"))
                }
                other => other,
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
        // The client-level timeout would sever a long-lived stream, and the
        // server's gzip encoder holds lines back until its buffer fills.
        let request = self
            .client
            .get(self.url(path))
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
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

    /// Add what changes while f5a runs: the tenant and the token file's
    /// bearer token.
    async fn authorize(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, ApiError> {
        let request = match self.tenant().filter(|_| !self.is_tenant_overridden) {
            Some(tenant) => request.header("Feldera-Tenant", tenant),
            None => request,
        };
        Ok(match &self.token_file {
            Some(file) => request.bearer_auth(file.token().await?),
            None => request,
        })
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<reqwest::Response, ApiError> {
        let response = self
            .authorize(request)
            .await?
            .send()
            .await
            .map_err(|error| ApiError::Transport(concise_reqwest_error(&error)))?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = read_body(response).await.unwrap_or_default();
        Err(ApiError::Status {
            status: status.as_u16(),
            message: error_message(&String::from_utf8_lossy(&body)),
        })
    }
}

/// The response body, gunzipped when the server answered the client's
/// `Accept-Encoding: gzip` with `Content-Encoding: gzip`.
async fn read_body(response: reqwest::Response) -> Result<Vec<u8>, ApiError> {
    use std::io::Read;
    let is_gzip = response
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .is_some_and(|encoding| encoding.as_bytes().eq_ignore_ascii_case(b"gzip"));
    let body = response
        .bytes()
        .await
        .map_err(|error| ApiError::Transport(concise_reqwest_error(&error)))?;
    if !is_gzip {
        return Ok(body.to_vec());
    }
    let mut decoded = Vec::with_capacity(body.len() * 8);
    flate2::read::GzDecoder::new(&body[..])
        .read_to_end(&mut decoded)
        .map_err(|error| ApiError::Decode(format!("corrupt gzip body: {error}")))?;
    Ok(decoded)
}

/// Prefer the API's own `message` field over a raw error body.
pub(crate) fn error_message(body: &str) -> String {
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

    /// The headers a request leaves with, without sending it.
    async fn authorized_headers(http: &Http) -> Result<reqwest::header::HeaderMap, ApiError> {
        let request = http.authorize(http.client.get(http.url("/x"))).await?;
        Ok(request.build().expect("a valid request").headers().clone())
    }

    #[tokio::test]
    async fn token_files_authorize_https_requests_and_rotate() {
        let file = std::env::temp_dir().join(format!("f5a-http-token-{}", std::process::id()));
        std::fs::write(&file, "first\n").unwrap();
        let options = |host: &str, headers: Vec<String>| ConnectionOptions {
            host: host.to_string(),
            oidc_token_file: Some(file.clone()),
            headers,
            tenant: Some("acme".to_string()),
            ..Default::default()
        };
        let http = Http::new(&options("https://feldera.test", Vec::new())).unwrap();
        let headers = authorized_headers(&http).await.unwrap();
        assert_eq!(headers["authorization"], "Bearer first");
        assert_eq!(headers["feldera-tenant"], "acme");
        std::fs::write(&file, "second").unwrap();
        let headers = authorized_headers(&http).await.unwrap();
        assert_eq!(
            headers["authorization"], "Bearer second",
            "re-read per request"
        );

        let plain = Http::new(&options("http://feldera.test", Vec::new())).unwrap();
        let headers = authorized_headers(&plain).await.unwrap();
        assert!(
            !headers.contains_key("authorization"),
            "never over plain HTTP"
        );

        let overridden = Http::new(&options(
            "https://feldera.test",
            vec!["Feldera-Tenant: other".to_string()],
        ))
        .unwrap();
        let headers = authorized_headers(&overridden).await.unwrap();
        assert!(
            !headers.contains_key("feldera-tenant"),
            "the --header default applies instead"
        );

        std::fs::write(&file, "").unwrap();
        let error = authorized_headers(&http).await.unwrap_err();
        assert!(error.to_string().contains("is empty"), "{error}");
        std::fs::remove_file(&file).unwrap();
        let error = authorized_headers(&http).await.unwrap_err();
        assert!(
            error.to_string().contains("failed to read OIDC token file"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn api_keys_and_headers_follow_fdas_rules_on_the_wire() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/keyed"))
            .and(header("Authorization", "Bearer from-header"))
            .and(header("X-Trace", "on"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/plain"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        // Over plain HTTP the API key stays home, but --header still goes.
        let http = Http::new(&ConnectionOptions {
            host: server.uri(),
            api_key: Some("apikey:secret".to_string()),
            headers: vec![
                "Authorization: Bearer from-header".to_string(),
                "X-Trace: on".to_string(),
            ],
            ..Default::default()
        })
        .unwrap();
        http.get_json("/keyed").await.unwrap();
        let keyless = Http::new(&ConnectionOptions {
            host: server.uri(),
            api_key: Some("apikey:secret".to_string()),
            ..Default::default()
        })
        .unwrap();
        keyless.get_json("/plain").await.unwrap();
        let requests = server.received_requests().await.unwrap();
        let plain = requests
            .iter()
            .find(|request| request.url.path() == "/plain")
            .unwrap();
        assert!(!plain.headers.contains_key("authorization"));
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

    /// A JSON body that stalls past the API timeout reads as a timeout,
    /// not as a malformed document.
    #[tokio::test]
    async fn stalled_json_bodies_report_a_timeout() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let host = one_shot_server(|mut socket| async move {
            let mut head = [0u8; 1024];
            let _ = socket.read(&mut head).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 8\r\n\r\n{\"a\":")
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
            let _ = socket.write_all(b" 1}").await;
        })
        .await;
        let http = Http::new(&ConnectionOptions {
            host,
            timeout_secs: 1,
            ..Default::default()
        })
        .unwrap();
        let error = http.get_json("/v0/doc").await.unwrap_err();
        assert!(matches!(error, ApiError::Transport(_)), "{error:?}");
        assert!(error.to_string().contains("timed out"), "{error}");
    }

    /// Large documents ask for gzip, decompress it, and outlive the API timeout.
    #[tokio::test]
    async fn large_json_is_gzipped_and_outlives_the_api_timeout() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let compressed = gzip(br#"{"graph": [1, 2, 3]}"#);
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        let host = one_shot_server(move |mut socket| async move {
            let mut head = [0u8; 1024];
            let read = socket.read(&mut head).await.unwrap();
            let _ = request_tx.send(String::from_utf8_lossy(&head[..read]).to_lowercase());
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\n\r\n",
                compressed.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            let (first, rest) = compressed.split_at(compressed.len() / 2);
            socket.write_all(first).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
            socket.write_all(rest).await.unwrap();
        })
        .await;
        let http = Http::new(&ConnectionOptions {
            host,
            timeout_secs: 1,
            ..Default::default()
        })
        .unwrap();
        let document = http.get_large_json("/v0/profile").await.unwrap();
        assert_eq!(document, serde_json::json!({"graph": [1, 2, 3]}));
        assert!(request_rx.await.unwrap().contains("accept-encoding: gzip"));
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    /// Every API request offers gzip, and gzipped answers decode, errors included.
    #[tokio::test]
    async fn api_requests_offer_gzip_and_decode_it() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v0/config"))
            .and(header("accept-encoding", "gzip"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-encoding", "gzip")
                    .set_body_raw(gzip(br#"{"a": 1}"#), "application/json"),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v0/pipelines/p1/pause"))
            .and(header("accept-encoding", "gzip"))
            .respond_with(
                ResponseTemplate::new(400)
                    .insert_header("content-encoding", "gzip")
                    .set_body_raw(gzip(br#"{"message": "zipped no"}"#), "application/json"),
            )
            .mount(&server)
            .await;
        let http = http(&server).await;
        assert_eq!(http.get_json("/v0/config").await.unwrap()["a"], 1);
        let error = http.post("/v0/pipelines/p1/pause").await.unwrap_err();
        assert!(error.to_string().contains("zipped no"), "{error}");
    }

    /// Downloads and the live log stream opt out of gzip.
    #[tokio::test]
    async fn downloads_and_log_streams_ask_for_identity() {
        let server = MockServer::start().await;
        for route in ["/blob", "/logs"] {
            Mock::given(method("GET"))
                .and(path(route))
                .and(header("accept-encoding", "identity"))
                .respond_with(ResponseTemplate::new(200).set_body_string("raw\n"))
                .expect(1)
                .mount(&server)
                .await;
        }
        let http = http(&server).await;
        assert_eq!(
            http.get_download("/blob").await.unwrap(),
            Download::Ready(b"raw\n".to_vec())
        );
        let (line_tx, mut line_rx) = tokio::sync::mpsc::unbounded_channel();
        http.stream_lines("/logs", &line_tx).await.unwrap();
        assert_eq!(line_rx.recv().await.unwrap(), "raw");
    }

    /// A large document that stops arriving fails after one minute.
    #[tokio::test(start_paused = true)]
    async fn large_json_gives_up_after_a_minute() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let host = one_shot_server(|mut socket| async move {
            let mut head = [0u8; 1024];
            let _ = socket.read(&mut head).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n{")
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(3_600)).await;
        })
        .await;
        let http = Http::new(&ConnectionOptions {
            host,
            timeout_secs: 1,
            ..Default::default()
        })
        .unwrap();
        let started = tokio::time::Instant::now();
        let error = http.get_large_json("/v0/profile").await.unwrap_err();
        let waited = started.elapsed();
        assert!(error.to_string().contains("timed out"), "{error}");
        assert!(
            (60..61).contains(&waited.as_secs()),
            "gave up after {waited:?}"
        );
    }

    #[tokio::test]
    async fn large_json_accepts_an_uncompressed_answer() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v0/profile"))
            .and(header("accept-encoding", "gzip"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"a": 1})))
            .mount(&server)
            .await;
        let document = http(&server)
            .await
            .get_large_json("/v0/profile")
            .await
            .unwrap();
        assert_eq!(document["a"], 1);
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
