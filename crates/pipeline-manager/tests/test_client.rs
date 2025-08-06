use actix_http::{encoding::Decoder, Payload, StatusCode};
use anyhow::{bail, Result as AnyResult};
use awc::error::SendRequestError;
use awc::{ClientRequest, ClientResponse};
use feldera_types::transport::http::Chunk;
use futures_util::StreamExt;
use pipeline_manager::db::types::pipeline::PipelineStatus;
use pipeline_manager::db::types::program::ProgramStatus;
use pipeline_manager::db::types::storage::StorageStatus;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};
use serde_json::json;
use std::env::VarError;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

/// Client to convenient test the Feldera API.
/// It includes functions for direct HTTP calls and (multistep) API calls.
pub struct TestClient {
    /// URL where the Feldera instance is accessible (e.g., http://localhost:8080)
    feldera_url: String,
    /// Authentication bearer token (optional)
    bearer_token: Option<String>,
    /// Reused HTTP client
    client: awc::Client,
}

impl TestClient {
    // Timeouts
    const PIPELINE_COMPILATION_TIMEOUT: Duration = Duration::from_secs(600);
    const PIPELINE_START_TIMEOUT: Duration = Duration::from_secs(30);
    const PIPELINE_PAUSE_TIMEOUT: Duration = Duration::from_secs(30);
    const PIPELINE_STOP_TIMEOUT: Duration = Duration::from_secs(30);
    const PIPELINE_CLEAR_TIMEOUT: Duration = Duration::from_secs(30);

    /// Sets up a test client
    pub async fn setup() -> TestClient {
        // Cryptography provider
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );

        // Feldera API URL
        let feldera_url = std::env::var("TEST_FELDERA_URL")
            .expect("Set TEST_FELDERA_URL to run integration tests");
        // println!("Running integration test against TEST_FELDERA_URL: {}", feldera_url);
        assert!(feldera_url.starts_with("http://") || feldera_url.starts_with("https://"));

        // Feldera authentication bearer token (optional)
        let bearer_token = match std::env::var("TEST_FELDERA_BEARER_TOKEN") {
            Ok(bearer_token) => {
                // println!("Running integration test with authentication bearer token");
                Some(bearer_token)
            }
            Err(e) => match e {
                VarError::NotPresent => None,
                VarError::NotUnicode(_) => panic!("Invalid TEST_FELDERA_BEARER_TOKEN: not Unicode"),
            },
        };

        // HTTP(S) client which is reused
        let client = match std::env::var("TEST_FELDERA_HTTPS_TLS_CERT_PATH") {
            Ok(https_tls_cert_path) => {
                assert!(feldera_url.starts_with("https://"), "TEST_FELDERA_HTTPS_TLS_CERT_PATH should only be set for a Feldera URL starting with https://");
                let cert_chain: Vec<_> = CertificateDer::pem_file_iter(https_tls_cert_path)
                    .expect("HTTPS TLS certificate should be read")
                    .flatten()
                    .collect();
                let mut root_cert_store = RootCertStore::empty();
                for certificate in cert_chain {
                    root_cert_store.add(certificate).unwrap();
                }
                let config = ClientConfig::builder()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth();
                awc::Client::builder()
                    .connector(awc::Connector::new().rustls_0_23(Arc::new(config)))
                    .finish()
            }
            Err(_) => {
                assert!(feldera_url.starts_with("http://"), "TEST_FELDERA_HTTPS_TLS_CERT_PATH should be set for a Feldera URL starting with https://");
                awc::Client::default()
            }
        };

        TestClient {
            feldera_url,
            bearer_token,
            client,
        }
    }

    /// Sets up a test client and cleans up all pipelines.
    pub async fn setup_and_full_cleanup() -> TestClient {
        let test_client = TestClient::setup().await;
        let pipelines = test_client.get_pipeline_statuses().await;
        for pipeline in pipelines {
            test_client
                .cleanup_pipeline(pipeline["name"].as_str().unwrap())
                .await;
        }
        test_client
    }

    /// Sets up a test client and cleans up a specific pipeline.
    pub async fn setup_and_cleanup_pipeline(pipeline_name: &str) -> TestClient {
        let test_client = TestClient::setup().await;
        test_client.cleanup_pipeline(pipeline_name).await;
        test_client
    }

    /// Formats endpoint URL using the Feldera base URL and the endpoint.
    fn endpoint_url<S: AsRef<str>>(&self, endpoint: S) -> String {
        format!("{}{}", self.feldera_url, endpoint.as_ref())
    }

    /// If one is defined, attaches the authentication bearer token to the request.
    fn maybe_attach_bearer_token(&self, req: ClientRequest) -> ClientRequest {
        match &self.bearer_token {
            Some(token) => req.insert_header((
                actix_http::header::AUTHORIZATION,
                format!("Bearer {}", token),
            )),
            None => req,
        }
    }

    pub async fn try_get<S: AsRef<str>>(
        &self,
        endpoint: S,
    ) -> Result<ClientResponse<Decoder<Payload>>, SendRequestError> {
        self.maybe_attach_bearer_token(self.client.get(self.endpoint_url(endpoint)))
            .send()
            .await
    }

    pub async fn get<S: AsRef<str>>(&self, endpoint: S) -> ClientResponse<Decoder<Payload>> {
        self.try_get(endpoint).await.unwrap()
    }

    pub async fn post<S: AsRef<str>>(
        &self,
        endpoint: S,
        content: String,
    ) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.post(self.endpoint_url(endpoint)))
            .send_body(content)
            .await
            .expect("POST request should return a response")
    }

    pub async fn post_no_body<S: AsRef<str>>(
        &self,
        endpoint: S,
    ) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.post(self.endpoint_url(endpoint)))
            .send()
            .await
            .expect("POST request with empty body should return a response")
    }

    pub async fn post_json<S: AsRef<str>>(
        &self,
        endpoint: S,
        json: &serde_json::Value,
    ) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.post(self.endpoint_url(endpoint)))
            .send_json(json)
            .await
            .expect("POST JSON request should return a response")
    }

    pub async fn put_json<S: AsRef<str>>(
        &self,
        endpoint: S,
        json: &serde_json::Value,
    ) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.put(self.endpoint_url(endpoint)))
            .send_json(json)
            .await
            .expect("PUT JSON request should return a response")
    }

    pub async fn patch_json<S: AsRef<str>>(
        &self,
        endpoint: S,
        json: &serde_json::Value,
    ) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.patch(self.endpoint_url(endpoint)))
            .send_json(&json)
            .await
            .expect("PATCH JSON request should return a response")
    }

    pub async fn delete<S: AsRef<str>>(&self, endpoint: S) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.delete(self.endpoint_url(endpoint)))
            .send()
            .await
            .expect("DELETE request should return a response")
    }

    /// Parses the response body as UTF-8 and returns it alongside the response HTTP status code.
    pub async fn parse_response(
        mut response: ClientResponse<Decoder<Payload>>,
    ) -> (StatusCode, String) {
        let bytes = response
            .body()
            .await
            .expect("Body should be retrievable as bytes");
        let utf8_string = std::str::from_utf8(&bytes).expect("Bytes should be valid UTF-8 string");
        (response.status(), utf8_string.to_string())
    }

    /// Parses the response body as JSON and returns it alongside the response HTTP status code.
    pub async fn parse_json_response(
        response: ClientResponse<Decoder<Payload>>,
    ) -> (StatusCode, serde_json::Value) {
        let (status_code, utf8_string) = Self::parse_response(response).await;
        let value = serde_json::from_str::<serde_json::Value>(&utf8_string)
            .expect("UTF-8 string should be convertible to JSON");
        (status_code, value)
    }

    pub async fn post_pipeline(&self, value: &serde_json::Value) -> serde_json::Value {
        let response = self.post_json("/v0/pipelines", value).await;
        let (status_code, value) = Self::parse_json_response(response).await;
        assert_eq!(status_code, StatusCode::CREATED);
        value
    }

    pub async fn put_pipeline(
        &self,
        pipeline_name: &str,
        value: &serde_json::Value,
    ) -> (bool, serde_json::Value) {
        let response = self
            .put_json(&format!("/v0/pipelines/{pipeline_name}"), value)
            .await;
        let (status_code, value) = Self::parse_json_response(response).await;
        if status_code == StatusCode::CREATED {
            (true, value)
        } else if status_code == StatusCode::OK {
            (false, value)
        } else {
            panic!("PUT pipeline response status code was not CREATED or OK but: {status_code}");
        }
    }

    pub async fn patch_pipeline(
        &self,
        pipeline_name: &str,
        value: &serde_json::Value,
    ) -> serde_json::Value {
        let response = self
            .patch_json(&format!("/v0/pipelines/{pipeline_name}"), value)
            .await;
        let (status_code, value) = Self::parse_json_response(response).await;
        assert_eq!(status_code, StatusCode::OK);
        value
    }

    pub async fn get_pipeline(&self, pipeline_name: &str) -> serde_json::Value {
        let response = self.get(&format!("/v0/pipelines/{pipeline_name}")).await;
        let (status_code, value) = Self::parse_json_response(response).await;
        assert_eq!(status_code, StatusCode::OK);
        value
    }

    pub async fn get_pipeline_status(&self, pipeline_name: &str) -> serde_json::Value {
        let response = self
            .get(&format!("/v0/pipelines/{pipeline_name}?selector=status"))
            .await;
        let (status_code, value) = Self::parse_json_response(response).await;
        assert_eq!(status_code, StatusCode::OK);
        value
    }

    pub async fn get_pipelines(&self) -> Vec<serde_json::Value> {
        let response = self.get("/v0/pipelines").await;
        let (status_code, value) = Self::parse_json_response(response).await;
        assert_eq!(status_code, StatusCode::OK);
        value
            .as_array()
            .expect("response should be a JSON array")
            .to_vec()
    }

    pub async fn get_pipeline_statuses(&self) -> Vec<serde_json::Value> {
        let response = self.get("/v0/pipelines?selector=status").await;
        let (status_code, value) = Self::parse_json_response(response).await;
        assert_eq!(status_code, StatusCode::OK);
        value
            .as_array()
            .expect("response should be a JSON array")
            .to_vec()
    }

    pub async fn pipeline_exists(&self, pipeline_name: &str) -> bool {
        let status_code = self
            .get(&format!("/v0/pipelines/{pipeline_name}"))
            .await
            .status();
        if status_code == StatusCode::OK {
            true
        } else if status_code == StatusCode::NOT_FOUND {
            false
        } else {
            panic!("GET pipeline response status code was not OK or NOT_FOUND but: {status_code}");
        }
    }

    pub async fn wait_for_stopped_with_error(&self, pipeline_name: &str) -> serde_json::Value {
        self.wait_for_pipeline_status(
            pipeline_name,
            PipelineStatus::Stopped,
            Self::PIPELINE_STOP_TIMEOUT,
        )
        .await;
        let value = self.get_pipeline(pipeline_name).await;
        value["deployment_error"].clone()
    }

    pub async fn start_pipeline(&self, pipeline_name: &str, wait: bool) {
        assert_eq!(
            self.post_no_body(&format!("/v0/pipelines/{pipeline_name}/start"))
                .await
                .status(),
            StatusCode::ACCEPTED
        );
        if wait {
            self.wait_for_pipeline_status(
                pipeline_name,
                PipelineStatus::Running,
                Self::PIPELINE_START_TIMEOUT,
            )
            .await;
        }
    }

    pub async fn pause_pipeline(&self, pipeline_name: &str, wait: bool) {
        assert_eq!(
            self.post_no_body(&format!("/v0/pipelines/{pipeline_name}/pause"))
                .await
                .status(),
            StatusCode::ACCEPTED
        );
        if wait {
            self.wait_for_pipeline_status(
                pipeline_name,
                PipelineStatus::Paused,
                Self::PIPELINE_PAUSE_TIMEOUT,
            )
            .await;
        }
    }

    #[cfg(feature = "feldera-enterprise")]
    pub async fn stop_pipeline(&self, pipeline_name: &str, wait: bool) {
        assert_eq!(
            self.post_no_body(&format!("/v0/pipelines/{pipeline_name}/stop?force=false"))
                .await
                .status(),
            StatusCode::ACCEPTED
        );
        if wait {
            self.wait_for_pipeline_status(
                pipeline_name,
                PipelineStatus::Stopped,
                Self::PIPELINE_STOP_TIMEOUT,
            )
            .await;
        }
    }

    pub async fn stop_force_pipeline(&self, pipeline_name: &str, wait: bool) {
        assert_eq!(
            self.post_no_body(&format!("/v0/pipelines/{pipeline_name}/stop?force=true"))
                .await
                .status(),
            StatusCode::ACCEPTED
        );
        if wait {
            self.wait_for_pipeline_status(
                pipeline_name,
                PipelineStatus::Stopped,
                Self::PIPELINE_STOP_TIMEOUT,
            )
            .await;
        }
    }

    pub async fn stop_force_and_clear_pipeline(&self, pipeline_name: &str) {
        self.stop_force_pipeline(pipeline_name, true).await;
        self.clear_pipeline(pipeline_name, true).await;
    }

    pub async fn clear_pipeline(&self, pipeline_name: &str, wait: bool) {
        assert_eq!(
            self.post_no_body(&format!("/v0/pipelines/{pipeline_name}/clear"))
                .await
                .status(),
            StatusCode::ACCEPTED
        );
        if wait {
            self.wait_for_cleared_storage(pipeline_name, Self::PIPELINE_CLEAR_TIMEOUT)
                .await;
        }
    }

    pub async fn delete_pipeline(&self, pipeline_name: &str) {
        assert_eq!(
            self.delete(&format!("/v0/pipelines/{pipeline_name}"))
                .await
                .status(),
            StatusCode::OK
        );
    }

    /// Cleans up the pipeline if it exists: stops forceful, clears storage and deletes it.
    pub async fn cleanup_pipeline(&self, pipeline_name: &str) {
        if self.pipeline_exists(pipeline_name).await {
            self.stop_force_pipeline(pipeline_name, true).await;
            self.clear_pipeline(pipeline_name, true).await;
            self.delete_pipeline(pipeline_name).await;
        }
    }

    /// Waits for the pipeline to reach a status with a timeout.
    pub async fn wait_for_pipeline_status(
        &self,
        pipeline_name: &str,
        status: PipelineStatus,
        timeout: Duration,
    ) -> serde_json::Value {
        let start = Instant::now();
        let mut last_update = Instant::now();
        loop {
            let pipeline = self.get_pipeline(pipeline_name).await;
            if pipeline["deployment_status"] == json!(status) {
                return pipeline;
            }
            if last_update.elapsed().as_secs() >= 5 {
                println!(
                    "Pipeline '{pipeline_name}': waiting for status {status:?} since {} seconds (current: {})",
                    start.elapsed().as_secs(),
                    pipeline["deployment_status"].as_str().unwrap()
                );
                last_update = Instant::now();
            }
            if start.elapsed() >= timeout {
                panic!(
                    "Pipeline '{pipeline_name}': timeout reached ({timeout:?}) waiting for status {status:?} (current: '{}') -- pipeline error (if any):\n{:#}",
                    pipeline["deployment_status"],
                    pipeline["deployment_error"]
                );
            }
            sleep(Duration::from_millis(250)).await;
        }
    }

    /// Waits for the pipeline program to be successfully compiled.
    pub async fn wait_for_compiled_program(&self, pipeline_name: &str, version: i64) {
        let start = Instant::now();
        let mut last_update = Instant::now();
        loop {
            let pipeline = self.get_pipeline_status(pipeline_name).await;

            // Program version must match
            let found_program_version = pipeline["program_version"].as_i64().unwrap();
            if found_program_version != version {
                panic!(
                    "Program version ({}) does not match expected ({})",
                    found_program_version, version
                );
            }

            if pipeline["program_status"] == json!(ProgramStatus::SqlError)
                || pipeline["program_status"] == json!(ProgramStatus::RustError)
                || pipeline["program_status"] == json!(ProgramStatus::SystemError)
            {
                // If an error occurred, it will not become successful anymore
                panic!(
                    "Pipeline compilation failed ({}): {}",
                    pipeline["program_status"], pipeline["program_error"]
                );
            } else if pipeline["program_status"] == json!(ProgramStatus::Success) {
                // Compilation is successful
                return;
            }

            // Print an update every 60 seconds approximately
            if last_update.elapsed().as_secs() >= 60 {
                println!(
                    "Pipeline '{pipeline_name}': waiting for compilation since {} seconds (currently: '{}')",
                    start.elapsed().as_secs(),
                    pipeline["program_status"]
                );
                last_update = Instant::now();
            }

            // Timeout
            if start.elapsed() > Self::PIPELINE_COMPILATION_TIMEOUT {
                panic!(
                    "Pipeline '{pipeline_name}': compilation did not complete within timeout ({:?}) -- current status: {}",
                    Self::PIPELINE_COMPILATION_TIMEOUT,
                    pipeline["program_status"]
                );
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Waits for the pipeline to reach cleared storage status with a timeout.
    pub async fn wait_for_cleared_storage(
        &self,
        pipeline_name: &str,
        timeout: Duration,
    ) -> serde_json::Value {
        let start = Instant::now();
        let mut last_update = Instant::now();
        loop {
            let pipeline = self.get_pipeline(pipeline_name).await;
            if pipeline["storage_status"] == json!(StorageStatus::Cleared) {
                return pipeline;
            }
            if last_update.elapsed().as_secs() >= 5 {
                println!(
                    "Pipeline '{pipeline_name}': waiting for storage status {:?} since {} seconds (current: '{}')",
                    StorageStatus::Cleared,
                    start.elapsed().as_secs(),
                    pipeline["storage_status"]
                );
                last_update = Instant::now();
            }
            if start.elapsed() >= timeout {
                panic!(
                    "Pipeline '{pipeline_name}': timeout reached ({timeout:?}) waiting for storage status {:?} (current: {:?})",
                    StorageStatus::Cleared,
                    pipeline["storage_status"].as_str().unwrap()
                );
            }
            sleep(Duration::from_millis(250)).await;
        }
    }

    /// Wait for the pipeline to reach the point where all input is processed.
    ///
    /// This is necessary before we e.g., send ad-hoc queries to ensure the
    /// query will compute on the latest state.
    pub async fn wait_for_processing(&self, pipeline_name: &str) -> bool {
        let start = Instant::now();
        loop {
            let stats = self.stats_json(pipeline_name).await;
            let num_processed = stats["global_metrics"]["total_processed_records"]
                .as_u64()
                .unwrap();
            let num_ingested = stats["global_metrics"]["total_input_records"]
                .as_u64()
                .unwrap();

            if num_processed == num_ingested {
                return true;
            } else {
                sleep(Duration::from_millis(20)).await;
                if start.elapsed() > Duration::from_secs(60) {
                    panic!("Pipeline did not process test records within 60 seconds");
                }
            }
        }
    }

    /// Creates, compiles and starts paused a pipeline named "test".
    pub async fn prepare_test_pipeline(&self, sql: &str) {
        self.prepare_pipeline("test", sql).await
    }

    /// Creates, compiles and starts paused a pipeline.
    pub async fn prepare_pipeline(&self, pipeline_name: &str, sql: &str) {
        let (is_created, _) = self
            .put_pipeline(
                pipeline_name,
                &json!({
                    "name": pipeline_name,
                    "program_code": sql,
                }),
            )
            .await;
        assert!(is_created);
        self.wait_for_compiled_program(pipeline_name, 1).await;
        self.pause_pipeline(pipeline_name, true).await;
    }

    pub async fn adhoc_query<S: AsRef<str>>(
        &self,
        pipeline_name: S,
        query: S,
        format: S,
    ) -> ClientResponse<Decoder<Payload>> {
        self.wait_for_processing(pipeline_name.as_ref()).await;
        let endpoint = format!("/v0/pipelines/{}/query", pipeline_name.as_ref());

        let r = self
            .maybe_attach_bearer_token(self.client.get(self.endpoint_url(endpoint)))
            .query(&[("sql", query.as_ref()), ("format", format.as_ref())])
            .expect("query parameters are valid");
        r.send().await.expect("request is successful")
    }

    /// Return the result of an ad hoc query as a JSON array.
    ///
    /// Doesn't sort the array; use `order by` to ensure deterministic results.
    pub async fn adhoc_query_json(&self, pipeline_name: &str, query: &str) -> serde_json::Value {
        let mut r = self.adhoc_query(pipeline_name, query, "json").await;
        assert_eq!(r.status(), StatusCode::OK);

        let body = r.body().await.unwrap();
        let ret = std::str::from_utf8(body.as_ref()).unwrap();
        let lines: Vec<String> = ret.split('\n').map(|s| s.to_string()).collect();

        serde_json::Value::Array(
            lines
                .iter()
                .filter(|s| !s.is_empty())
                .map(|s| {
                    serde_json::from_str::<serde_json::Value>(s)
                        .map_err(|e| {
                            format!(
                            "ad hoc query returned an invalid JSON string: '{s}' (parse error: {e})"
                        )
                        })
                        .unwrap()
                })
                .collect(),
        )
    }

    /// Retrieve the stats of a pipeline as JSON value.
    pub async fn stats_json(&self, name: &str) -> serde_json::Value {
        let endpoint = format!("/v0/pipelines/{name}/stats");
        let response = self.get(endpoint).await;
        assert_eq!(response.status(), StatusCode::OK);
        Self::parse_json_response(response).await.1
    }

    /// Retrieve the stats of an input connector as a JSON value.
    pub async fn input_connector_stats_json(
        &self,
        pipeline_name: &str,
        table_name: &str,
        connector_name: &str,
    ) -> serde_json::Value {
        let encoded_table_name = urlencoding::encode(table_name).to_string();

        let endpoint = format!(
            "/v0/pipelines/{pipeline_name}/tables/{encoded_table_name}/connectors/{connector_name}/stats"
        );
        let response = self.get(endpoint).await;
        assert_eq!(response.status(), StatusCode::OK);
        Self::parse_json_response(response).await.1
    }

    pub async fn delta_stream_request_json(
        &self,
        name: &str,
        table: &str,
    ) -> ClientResponse<Decoder<Payload>> {
        let resp = self
            .post_no_body(format!("/v0/pipelines/{name}/egress/{table}?format=json"))
            .await;
        assert!(resp.status().is_success());
        resp
    }

    /// Pause or unpause a connector.
    pub async fn connector_action(
        &self,
        pipeline_name: &str,
        table_name: &str,
        connector_name: &str,
        action: &str,
    ) {
        let encoded_table_name = urlencoding::encode(table_name).to_string();

        // Pause the connector
        assert_eq!(
                    self
                        .post_no_body(format!(
                            "/v0/pipelines/{pipeline_name}/tables/{encoded_table_name}/connectors/{connector_name}/{action}"
                        ))
                        .await
                        .status(),
                    StatusCode::OK
                );
    }

    pub async fn read_response_json(
        &self,
        response: &mut ClientResponse<Decoder<Payload>>,
        max_timeout: Duration,
    ) -> AnyResult<Option<serde_json::Value>> {
        let start = Instant::now();

        loop {
            match timeout(Duration::from_millis(1_000), response.next()).await {
                Err(_) => (),
                Ok(Some(Ok(bytes))) => {
                    let chunk = serde_json::from_reader::<_, Chunk>(&bytes[..])?;
                    if let Some(json) = chunk.json_data {
                        return Ok(Some(json));
                    }
                }
                Ok(Some(Err(e))) => bail!(e.to_string()),
                Ok(None) => return Ok(None),
            }
            if start.elapsed() >= max_timeout {
                return Ok(None);
            }
        }
    }

    /// Wait for the exact output, potentially split across multiple chunks.
    pub async fn read_expected_response_json(
        &self,
        response: &mut ClientResponse<Decoder<Payload>>,
        max_timeout: Duration,
        expected_response: &[serde_json::Value],
    ) {
        let start = Instant::now();
        let mut received = Vec::new();

        while received.len() < expected_response.len() {
            let mut new_received = self
                .read_response_json(response, Duration::from_millis(1_000))
                .await
                .unwrap()
                .unwrap_or_else(|| json!([]))
                .as_array()
                .unwrap()
                .clone();
            received.append(&mut new_received);

            assert!(start.elapsed() <= max_timeout);
        }

        assert_eq!(expected_response, received);
    }

    pub async fn check_cluster_health(&self) -> (StatusCode, serde_json::Value) {
        let response = self.get("/v0/cluster_healthz").await;
        let (status_code, value) = Self::parse_json_response(response).await;
        (status_code, value)
    }
}

/// Wait for a condition to be true by periodically checking it.
pub async fn wait_for_condition<F, Fut>(description: &str, mut check: F, timeout: Duration)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();

    while start.elapsed() < timeout {
        if check().await {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }

    panic!("Timeout waiting for {description}");
}
