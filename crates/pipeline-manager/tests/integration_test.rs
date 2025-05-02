//! Pipeline manager integration tests that test the end-to-end
//! compiler->run->feed inputs->receive outputs workflow.
//!
//! There are two ways to run these tests:
//!
//! 1. Using an external pipeline manager instance.
//!
//! Start the pipeline manager by running `scripts/start_manager.sh` or using
//! the following command line:
//!
//! ```text
//! cargo run --bin=pipeline-manager
//! ```
//!
//! or as a container
//!
//! ```text
//! docker compose -f deploy/docker-compose.yml -f deploy/docker-compose-dev.yml --profile demo up --build --renew-anon-volumes --force-recreate
//! ```
//!
//! Run the tests in a different terminal:
//!
//! ```text
//! TEST_DBSP_URL=http://localhost:8080 cargo test --test integration_test --package=pipeline-manager  -- --nocapture
//! ```
use std::time::{Duration, Instant};

use actix_http::{encoding::Decoder, Payload, StatusCode};
use awc::error::SendRequestError;
use awc::{http, ClientRequest, ClientResponse};
use aws_sdk_cognitoidentityprovider::config::Region;
use feldera_types::transport::http::Chunk;
use futures_util::StreamExt;
use serde_json::{json, Value};
use serial_test::serial;
use tokio::time::{sleep, timeout};

use anyhow::{bail, Result as AnyResult};
use feldera_types::config::{ResourceConfig, RuntimeConfig, StorageOptions};

use pipeline_manager::db::types::pipeline::PipelineStatus;
use pipeline_manager::db::types::program::CompilationProfile;
use pipeline_manager::db::types::program::ProgramConfig;
use pipeline_manager::db::types::program::ProgramStatus;
use pipeline_manager::runner::pipeline_executor::LOGS_END_MESSAGE;

const TEST_DBSP_URL_VAR: &str = "TEST_DBSP_URL";
const MANAGER_INITIALIZATION_TIMEOUT: Duration = Duration::from_secs(100);

struct TestConfig {
    dbsp_url: String,
    client: awc::Client,
    bearer_token: Option<String>,
    compilation_timeout: Duration,
    start_timeout: Duration,
    pause_timeout: Duration,
    resume_timeout: Duration,
    shutdown_timeout: Duration,
    failed_timeout: Duration,
}

impl TestConfig {
    fn endpoint_url<S: AsRef<str>>(&self, endpoint: S) -> String {
        format!("{}{}", self.dbsp_url, endpoint.as_ref())
    }

    async fn get<S: AsRef<str>>(&self, endpoint: S) -> ClientResponse<Decoder<Payload>> {
        self.try_get(endpoint).await.unwrap()
    }

    async fn try_get<S: AsRef<str>>(
        &self,
        endpoint: S,
    ) -> Result<ClientResponse<Decoder<Payload>>, SendRequestError> {
        self.maybe_attach_bearer_token(self.client.get(self.endpoint_url(endpoint)))
            .send()
            .await
    }

    /// Wait for the pipeline to reach the point where all input is processed.
    ///
    /// This is necessary before we e.g., send ad-hoc queries to ensure the
    /// query will compute on the latest state.
    async fn wait_for_processing(&self, pipeline_name: &str) -> bool {
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

    async fn adhoc_query<S: AsRef<str>>(
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
    async fn adhoc_query_json(&self, pipeline_name: &str, query: &str) -> Value {
        let mut r = self.adhoc_query(pipeline_name, query, "json").await;
        assert_eq!(r.status(), StatusCode::OK);

        let body = r.body().await.unwrap();
        let ret = std::str::from_utf8(body.as_ref()).unwrap();
        let lines: Vec<String> = ret.split('\n').map(|s| s.to_string()).collect();

        Value::Array(
            lines
                .iter()
                .filter(|s| !s.is_empty())
                .map(|s| {
                    serde_json::from_str::<Value>(s)
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

    // TODO: currently unused
    // /// Performs GET request, asserts the status code is OK, and returns result.
    // async fn get_ok<S: AsRef<str>>(&self, endpoint: S) -> ClientResponse<Decoder<Payload>> {
    //     let result = self.get(endpoint).await;
    //     assert_eq!(result.status(), StatusCode::OK);
    //     result
    // }

    fn maybe_attach_bearer_token(&self, req: ClientRequest) -> ClientRequest {
        match &self.bearer_token {
            Some(token) => {
                req.insert_header((http::header::AUTHORIZATION, format!("Bearer {}", token)))
            }
            None => req,
        }
    }

    async fn post_no_body<S: AsRef<str>>(&self, endpoint: S) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.post(self.endpoint_url(endpoint)))
            .send()
            .await
            .unwrap()
    }

    async fn put<S: AsRef<str>>(
        &self,
        endpoint: S,
        json: &Value,
    ) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.put(self.endpoint_url(endpoint)))
            .send_json(&json)
            .await
            .unwrap()
    }

    async fn post<S: AsRef<str>>(
        &self,
        endpoint: S,
        json: &Value,
    ) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.post(self.endpoint_url(endpoint)))
            .send_json(&json)
            .await
            .unwrap()
    }

    async fn post_csv<S: AsRef<str>>(
        &self,
        endpoint: S,
        csv: String,
    ) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.post(self.endpoint_url(endpoint)))
            .send_body(csv)
            .await
            .unwrap()
    }

    async fn post_json<S: AsRef<str>>(
        &self,
        endpoint: S,
        json: String,
    ) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.post(self.endpoint_url(endpoint)))
            .send_body(json)
            .await
            .unwrap()
    }

    async fn patch<S: AsRef<str>>(
        &self,
        endpoint: S,
        json: &Value,
    ) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.patch(self.endpoint_url(endpoint)))
            .send_json(&json)
            .await
            .unwrap()
    }

    /// Decodes the response body as JSON.
    async fn decode_json(mut response: ClientResponse<Decoder<Payload>>) -> Value {
        let bytes = response
            .body()
            .await
            .expect("Body should be retrievable as bytes");
        let utf8_str = std::str::from_utf8(&bytes).expect("Bytes should be valid UTF-8 string");
        serde_json::from_str::<Value>(utf8_str)
            .expect("UTF-8 string should be convertible to JSON value")
    }

    /// Checks status and decodes the response body as JSON.
    pub async fn check_status_and_decode_json(
        response: ClientResponse<Decoder<Payload>>,
        expected_status: StatusCode,
    ) -> Value {
        assert_eq!(response.status(), expected_status);
        Self::decode_json(response).await
    }

    /// Retrieve the stats of a pipeline as JSON value.
    async fn stats_json(&self, name: &str) -> Value {
        let endpoint = format!("/v0/pipelines/{name}/stats");
        let response = self.get(endpoint).await;
        assert_eq!(response.status(), StatusCode::OK);
        Self::decode_json(response).await
    }

    /// Retrieve the stats of an input connector as a JSON value.
    async fn input_connector_stats_json(
        &self,
        pipeline_name: &str,
        table_name: &str,
        connector_name: &str,
    ) -> Value {
        let encoded_table_name = urlencoding::encode(table_name).to_string();

        let endpoint = format!(
            "/v0/pipelines/{pipeline_name}/tables/{encoded_table_name}/connectors/{connector_name}/stats"
        );
        let response = self.get(endpoint).await;
        assert_eq!(response.status(), StatusCode::OK);
        Self::decode_json(response).await
    }

    async fn delta_stream_request_json(
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

    async fn read_response_json(
        &self,
        response: &mut ClientResponse<Decoder<Payload>>,
        max_timeout: Duration,
    ) -> AnyResult<Option<Value>> {
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
    async fn read_expected_response_json(
        &self,
        response: &mut ClientResponse<Decoder<Payload>>,
        max_timeout: Duration,
        expected_response: &[Value],
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

    async fn delete<S: AsRef<str>>(&self, endpoint: S) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.delete(self.endpoint_url(endpoint)))
            .send()
            .await
            .unwrap()
    }

    /// Waits for pipeline program status to indicate it is fully compiled.
    /// Panics after `timeout`.
    async fn wait_for_compilation(&self, pipeline_name: &str, version: i64, timeout: Duration) {
        println!("Waiting for program of pipeline {pipeline_name} to finish compilation...");
        let start = Instant::now();
        let mut last_update = Instant::now();
        loop {
            // Retrieve pipeline
            let mut response = self.get(format!("/v0/pipelines/{pipeline_name}")).await;
            let pipeline = response.json::<Value>().await.unwrap();

            // Program version must match
            let found_program_version = pipeline["program_version"].as_i64().unwrap();
            if found_program_version != version {
                panic!(
                    "Program version {} does not match expected {}",
                    found_program_version, version
                );
            }

            // Check program status
            if pipeline["program_status"] == json!(ProgramStatus::Pending)
                || pipeline["program_status"] == json!(ProgramStatus::CompilingSql)
                || pipeline["program_status"] == json!(ProgramStatus::SqlCompiled)
                || pipeline["program_status"] == json!(ProgramStatus::CompilingRust)
            {
                // Continue waiting
            } else if pipeline["program_status"] == json!(ProgramStatus::Success) {
                return;
            } else {
                println!("Pipeline:\n{pipeline:#?}");
                panic!(
                    "Compilation failed in status: {:?}",
                    pipeline["program_status"]
                );
            }

            // Print an update every 60 seconds approximately
            if last_update.elapsed().as_secs() >= 60 {
                // println!("Pipeline:\n{pipeline:#?}");
                println!(
                    "Waiting for compilation since {} seconds, current program status: {:?}",
                    start.elapsed().as_secs(),
                    pipeline["program_status"]
                );
                last_update = Instant::now();
            }

            // Timeout
            if start.elapsed() > timeout {
                println!("Pipeline:\n{pipeline:#?}");
                panic!("Compilation timeout ({timeout:?})");
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Waits for the pipeline deployment to reach the specified status.
    /// Panics after `timeout`.
    async fn wait_for_deployment_status(
        &self,
        pipeline_name: &str,
        status: PipelineStatus,
        timeout: Duration,
    ) -> Value {
        println!("Waiting for pipeline {pipeline_name} to reach deployment status {status:?}...");
        let start = Instant::now();
        let mut last_update = Instant::now();
        loop {
            // Retrieve pipeline
            let mut response = self.get(format!("/v0/pipelines/{pipeline_name}")).await;
            let pipeline = response.json::<Value>().await.unwrap();

            // Reached the status
            if pipeline["deployment_status"] == json!(status) {
                return pipeline;
            }

            // Print an update every 60 seconds approximately
            if last_update.elapsed().as_secs() >= 60 {
                println!(
                    "Pipeline status: {}",
                    pipeline.as_object().unwrap()["deployment_status"]
                );
                println!(
                    "Waiting for pipeline status {status:?} since {} seconds",
                    start.elapsed().as_secs()
                );
                last_update = Instant::now();
            }

            // Timeout
            if start.elapsed() >= timeout {
                println!("Pipeline:\n{pipeline:#?}");
                panic!("Timeout ({timeout:?}) waiting for pipeline status {status:?}");
            }

            sleep(Duration::from_millis(300)).await;
        }
    }

    /// Cleanup by shutting down and removing all pipelines.
    async fn cleanup(&self) {
        let config = self;

        // Retrieve list of pipelines
        let start = Instant::now();
        let mut response = loop {
            match config.try_get("/v0/pipelines").await {
                Ok(response) => {
                    break response;
                }
                Err(e) => {
                    if start.elapsed() > MANAGER_INITIALIZATION_TIMEOUT {
                        panic!("Timeout waiting for the pipeline manager");
                    }
                    println!("Could not reach API due to error: {e}");
                    println!("Retrying...");
                    sleep(Duration::from_millis(1000)).await;
                }
            }
        };
        let pipelines = response
            .json::<Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .clone();
        println!("Found {} pipeline(s) to clean up", pipelines.len());

        // Shutdown the pipelines
        for pipeline in &pipelines {
            let pipeline_name = pipeline["name"].as_str().unwrap();
            println!("Shutting down pipeline: {pipeline_name}");
            let response = config
                .post_no_body(format!("/v0/pipelines/{pipeline_name}/shutdown"))
                .await;
            assert_eq!(
                StatusCode::ACCEPTED,
                response.status(),
                "Unexpected response to pipeline shutdown: {:?}",
                response
            )
        }

        // Delete each pipeline once it is confirmed it is shutdown
        for pipeline in &pipelines {
            let pipeline_name = pipeline["name"].as_str().unwrap();
            self.wait_for_deployment_status(
                pipeline_name,
                PipelineStatus::Shutdown,
                config.shutdown_timeout,
            )
            .await;
            let response = config
                .delete(format!("/v0/pipelines/{pipeline_name}"))
                .await;
            assert_eq!(
                StatusCode::OK,
                response.status(),
                "Unexpected response to pipeline deletion: {:?}",
                response
            )
        }
    }
}

async fn bearer_token() -> Option<String> {
    let client_id = std::env::var("TEST_CLIENT_ID");
    match client_id {
        Ok(client_id) => {
            let test_user = std::env::var("TEST_USER")
                .expect("If TEST_CLIENT_ID is set, TEST_USER should be as well");
            let test_password = std::env::var("TEST_PASSWORD")
                .expect("If TEST_CLIENT_ID is set, TEST_PASSWORD should be as well");
            let test_region = std::env::var("TEST_REGION")
                .expect("If TEST_CLIENT_ID is set, TEST_REGION should be as well");
            let config = aws_config::from_env()
                .region(Region::new(test_region))
                .load()
                .await;
            let cognito_idp = aws_sdk_cognitoidentityprovider::Client::new(&config);
            let res = cognito_idp
                .initiate_auth()
                .set_client_id(Some(client_id))
                .set_auth_flow(Some(
                    aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth,
                ))
                .auth_parameters("USERNAME", test_user)
                .auth_parameters("PASSWORD", test_password)
                .send()
                .await
                .unwrap();
            Some(res.authentication_result()?.access_token()?.to_string())
        }
        Err(e) => {
            println!(
                "TEST_CLIENT_ID is unset (reason: {}). Test will not use authentication",
                e
            );
            None
        }
    }
}

async fn setup() -> TestConfig {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let dbsp_url = match std::env::var(TEST_DBSP_URL_VAR) {
        Ok(val) => {
            println!("Running integration test against TEST_DBSP_URL: {}", val);
            val
        }
        Err(e) => {
            panic!("Set TEST_DBSP_URL to run this test (reason: {})", e);
        }
    };
    let client = awc::ClientBuilder::new()
        .timeout(Duration::from_secs(10))
        .finish();
    let bearer_token = bearer_token().await;
    let compilation_timeout = Duration::from_secs(
        std::env::var("TEST_COMPILATION_TIMEOUT")
            .unwrap_or("480".to_string())
            .parse::<u64>()
            .unwrap(),
    );
    let start_timeout = Duration::from_secs(
        std::env::var("TEST_START_TIMEOUT")
            .unwrap_or("60".to_string())
            .parse::<u64>()
            .unwrap(),
    );
    let pause_timeout = Duration::from_secs(
        std::env::var("TEST_PAUSE_TIMEOUT")
            .unwrap_or("10".to_string())
            .parse::<u64>()
            .unwrap(),
    );
    let resume_timeout = Duration::from_secs(
        std::env::var("TEST_RESUME_TIMEOUT")
            .unwrap_or("10".to_string())
            .parse::<u64>()
            .unwrap(),
    );
    let shutdown_timeout = Duration::from_secs(
        std::env::var("TEST_SHUTDOWN_TIMEOUT")
            .unwrap_or("120".to_string())
            .parse::<u64>()
            .unwrap(),
    );
    let failed_timeout = Duration::from_secs(
        std::env::var("TEST_FAILED_TIMEOUT")
            .unwrap_or("120".to_string())
            .parse::<u64>()
            .unwrap(),
    );
    let config = TestConfig {
        dbsp_url,
        client,
        bearer_token,
        compilation_timeout,
        start_timeout,
        pause_timeout,
        resume_timeout,
        shutdown_timeout,
        failed_timeout,
    };
    config.cleanup().await;
    config
}

/// Creates and deploys a basic pipeline named `test` with the provided SQL in paused state.
async fn create_and_deploy_test_pipeline(config: &TestConfig, sql: &str) {
    prepare_pipeline(config, "test", sql).await
}

/// Creates, compiles and deploys a pipeline.
async fn prepare_pipeline(config: &TestConfig, name: &str, sql: &str) {
    // Create
    let request_body = json!({
        "name": name,
        "description": "Description of the test pipeline",
        "runtime_config": {},
        "program_code": sql,
        "program_config": {}
    });
    let response = config.post("/v0/pipelines", &request_body).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Wait for compilation
    config
        .wait_for_compilation(name, 1, config.compilation_timeout)
        .await;

    // Start the pipeline in Paused state
    let response = config
        .post_no_body(format!("/v0/pipelines/{name}/pause"))
        .await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status(name, PipelineStatus::Paused, config.start_timeout)
        .await;
}

/// Tests the creation of pipelines using its POST endpoint.
#[actix_web::test]
#[serial]
async fn pipeline_post() {
    let config = setup().await;

    // Empty body
    assert_eq!(
        config.post("/v0/pipelines", &json!({})).await.status(),
        StatusCode::BAD_REQUEST
    );

    // Name is missing
    assert_eq!(
        config
            .post("/v0/pipelines", &json!({ "program-code": "" }))
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );

    // Program SQL code is missing
    assert_eq!(
        config
            .post("/v0/pipelines", &json!({ "name": "test-1" }))
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );

    // Minimum body
    let pipeline = TestConfig::check_status_and_decode_json(
        config
            .post(
                "/v0/pipelines",
                &json!({
                    "name": "test-1",
                    "program_code": "",
                }),
            )
            .await,
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(pipeline["name"], json!("test-1"));
    assert_eq!(pipeline["description"], json!(""));
    assert_eq!(
        pipeline["runtime_config"],
        serde_json::to_value(RuntimeConfig::default()).unwrap()
    );
    assert_eq!(pipeline["program_code"], json!(""));
    assert_eq!(pipeline["udf_rust"], json!(""));
    assert_eq!(pipeline["udf_toml"], json!(""));
    assert_eq!(
        pipeline["program_config"],
        serde_json::to_value(ProgramConfig::default()).unwrap()
    );

    // Body with SQL
    let pipeline = TestConfig::check_status_and_decode_json(
        config
            .post(
                "/v0/pipelines",
                &json!({
                    "name": "test-2",
                    "program_code": "sql-2",
                }),
            )
            .await,
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(pipeline["name"], json!("test-2"));
    assert_eq!(pipeline["description"], json!(""));
    assert_eq!(
        pipeline["runtime_config"],
        serde_json::to_value(RuntimeConfig::default()).unwrap()
    );
    assert_eq!(pipeline["program_code"], json!("sql-2"));
    assert_eq!(pipeline["udf_rust"], json!(""));
    assert_eq!(pipeline["udf_toml"], json!(""));
    assert_eq!(
        pipeline["program_config"],
        serde_json::to_value(ProgramConfig::default()).unwrap()
    );

    // All fields
    let pipeline = TestConfig::check_status_and_decode_json(
        config
            .post(
                "/v0/pipelines",
                &json!({
                    "name": "test-3",
                    "description": "description-3",
                    "runtime_config": {
                        "workers": 123
                    },
                    "program_code": "sql-3",
                    "udf_rust": "rust-3",
                    "udf_toml": "toml-3",
                    "program_config": {
                        "profile": "dev"
                    }
                }),
            )
            .await,
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(pipeline["name"], json!("test-3"));
    assert_eq!(pipeline["description"], json!("description-3"));
    assert_eq!(
        pipeline["runtime_config"],
        serde_json::to_value(RuntimeConfig {
            workers: 123,
            ..Default::default()
        })
        .unwrap()
    );
    assert_eq!(pipeline["program_code"], json!("sql-3"));
    assert_eq!(pipeline["udf_rust"], json!("rust-3"));
    assert_eq!(pipeline["udf_toml"], json!("toml-3"));
    assert_eq!(
        pipeline["program_config"],
        serde_json::to_value(ProgramConfig {
            profile: Some(CompilationProfile::Dev),
            ..Default::default()
        })
        .unwrap()
    );
    assert_eq!(pipeline["program_config"]["profile"], json!("dev"));
}

/// Tests the retrieval of a pipeline and list of pipelines.
#[actix_web::test]
#[serial]
async fn pipeline_get() {
    let config = setup().await;

    // Not found
    let response = config.get("/v0/pipelines/test-1").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // List is initially empty
    let mut response = config.get("/v0/pipelines").await;
    let value: Value = response.json().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(value, json!([]));

    // Create first pipeline
    let sql1 = "CREATE TABLE t1(c1 INT);";
    prepare_pipeline(&config, "test-1", sql1).await;

    // Retrieve list of one
    let mut response = config.get("/v0/pipelines").await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    let list1 = value.as_array().unwrap();
    assert_eq!(list1.len(), 1);

    // Retrieve first pipeline
    let mut response = config.get("/v0/pipelines/test-1").await;
    assert_eq!(response.status(), StatusCode::OK);
    let object1_1: Value = response.json().await.unwrap();

    // Create second pipeline
    let sql2 = "CREATE TABLE t2(c2 INT);";
    prepare_pipeline(&config, "test-2", sql2).await;

    // Retrieve list of two
    let mut response = config.get("/v0/pipelines").await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    let list2 = value.as_array().unwrap();
    assert_eq!(list2.len(), 2);

    // Retrieve first pipeline again
    let mut response = config.get("/v0/pipelines/test-1").await;
    assert_eq!(response.status(), StatusCode::OK);
    let object1_2: Value = response.json().await.unwrap();

    // Retrieve second pipeline
    let mut response = config.get("/v0/pipelines/test-2").await;
    assert_eq!(response.status(), StatusCode::OK);
    let object2_2: Value = response.json().await.unwrap();

    // Check first pipeline
    for object1 in [list1[0].clone(), object1_1, object1_2, list2[0].clone()] {
        assert_eq!(object1["name"].as_str().unwrap(), "test-1");
        assert_eq!(object1["program_code"].as_str().unwrap(), sql1);
    }

    // Check second pipeline
    for object2 in [object2_2, list2[1].clone()] {
        assert_eq!(object2["name"].as_str().unwrap(), "test-2");
        assert_eq!(object2["program_code"].as_str().unwrap(), sql2);
    }
}

/// Fields included in the `all` selector.
const PIPELINE_FIELD_SELECTOR_ALL_FIELDS: [&str; 21] = [
    "id",
    "name",
    "description",
    "created_at",
    "version",
    "platform_version",
    "runtime_config",
    "program_code",
    "udf_rust",
    "udf_toml",
    "program_config",
    "program_version",
    "program_status",
    "program_status_since",
    "program_error",
    "program_info",
    "deployment_status",
    "deployment_status_since",
    "deployment_desired_status",
    "deployment_error",
    "refresh_version",
];

/// Fields included in the `status` selector.
const PIPELINE_FIELD_SELECTOR_STATUS_FIELDS: [&str; 14] = [
    "id",
    "name",
    "description",
    "created_at",
    "version",
    "platform_version",
    "program_version",
    "program_status",
    "program_status_since",
    "deployment_status",
    "deployment_status_since",
    "deployment_desired_status",
    "deployment_error",
    "refresh_version",
];

/// Tests the retrieval of a pipeline and list of pipelines with a field selector.
#[actix_web::test]
#[serial]
async fn pipeline_get_selector() {
    let config = setup().await;
    prepare_pipeline(&config, "test-1", "CREATE TABLE t1(c1 INT);").await;
    for base_endpoint in ["/v0/pipelines", "/v0/pipelines/test-1"] {
        for (selector_value, expected_fields) in [
            ("", PIPELINE_FIELD_SELECTOR_ALL_FIELDS.to_vec()),
            ("all", PIPELINE_FIELD_SELECTOR_ALL_FIELDS.to_vec()),
            ("status", PIPELINE_FIELD_SELECTOR_STATUS_FIELDS.to_vec()),
        ] {
            // Perform request
            let endpoint = if selector_value.is_empty() {
                base_endpoint.to_string()
            } else {
                format!("{base_endpoint}?selector={selector_value}")
            };
            let mut response = config.get(&endpoint).await;
            assert_eq!(response.status(), StatusCode::OK);

            // Parse response body as object
            let value: Value = response.json().await.unwrap();
            let object = if value.is_array() {
                let array = value.as_array().unwrap();
                assert_eq!(array.len(), 1);
                array[0].as_object().unwrap().clone()
            } else {
                value.as_object().unwrap().clone()
            };

            // Check that the pipeline object has exactly the expected fields
            let mut expected_fields_sorted = expected_fields;
            expected_fields_sorted.sort();
            let mut actual_fields_sorted: Vec<&String> = object.keys().collect();
            actual_fields_sorted.sort();
            assert_eq!(expected_fields_sorted, actual_fields_sorted);
        }
    }
}

/// Tests creating a pipeline, waiting for it to compile and delete afterward.
#[actix_web::test]
#[serial]
async fn pipeline_create_compile_delete() {
    let config = setup().await;
    let request_body = json!({
        "name":  "test",
        "description": "desc",
        "runtime_config": {},
        "program_code": "CREATE TABLE t1(c1 INTEGER);",
        "program_config": {}
    });
    let mut response = config.post("/v0/pipelines", &request_body).await;
    assert_eq!(StatusCode::CREATED, response.status());
    let resp: Value = response.json().await.unwrap();
    let version = resp["program_version"].as_i64().unwrap();
    config
        .wait_for_compilation("test", version, config.compilation_timeout)
        .await;
    let response = config.delete("/v0/pipelines/test").await;
    assert_eq!(StatusCode::OK, response.status());
    let response = config.get("/v0/programs/test").await;
    assert_eq!(StatusCode::NOT_FOUND, response.status());
}

/// Tests that an error is thrown if a pipeline with the same name already exists.
#[actix_web::test]
#[serial]
async fn pipeline_name_conflict() {
    let config = setup().await;
    let request_body = json!({
        "name":  "test",
        "description": "desc",
        "runtime_config": {},
        "program_code": "CREATE TABLE t1(c1 INTEGER);",
        "program_config": {}
    });
    let response = config.post("/v0/pipelines", &request_body).await;
    assert_eq!(StatusCode::CREATED, response.status());

    // Same name, different description and program code.
    let request_body = json!({
        "name":  "test",
        "description": "a different description",
        "runtime_config": {},
        "program_code": "CREATE TABLE t2(c2 VARCHAR);",
        "program_config": {}
    });
    let response = config.post("/v0/pipelines", &request_body).await;
    assert_eq!(StatusCode::CONFLICT, response.status());
}

/// Tests pausing and resuming a pipeline, pushing ingress data, and validating the contents of the output view.
#[actix_web::test]
#[serial]
async fn deploy_pipeline() {
    let config = setup().await;

    // Basic test pipeline with a SQL program which is known to panic
    create_and_deploy_test_pipeline(
        &config,
        "CREATE TABLE t1(c1 INTEGER) with ('materialized' = 'true'); CREATE VIEW v1 AS SELECT * FROM t1;",
    ).await;

    // Again pause the pipeline before it is started
    let response = config.post_no_body("/v0/pipelines/test/pause").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Running, config.resume_timeout)
        .await;

    // Push some data
    let response = config
        .post_csv("/v0/pipelines/test/ingress/t1", "1\n2\n3\n".to_string())
        .await;
    assert!(response.status().is_success());

    // Push more data with Windows-style newlines
    let response = config
        .post_csv("/v0/pipelines/test/ingress/t1", "4\r\n5\r\n6".to_string())
        .await;
    assert!(response.status().is_success());

    // Pause a pipeline after it is started
    let response = config.post_no_body("/v0/pipelines/test/pause").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Paused, config.pause_timeout)
        .await;

    // Querying should work in Paused state
    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1;")
            .await,
        json!([{"c1": 1}, {"c1": 2}, {"c1": 3}, {"c1": 4}, {"c1": 5}, {"c1": 6}])
    );

    // Start the pipeline again
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Running, config.resume_timeout)
        .await;

    // Querying should work in Running state
    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1;")
            .await,
        json!([{"c1": 1}, {"c1": 2}, {"c1": 3}, {"c1": 4}, {"c1": 5}, {"c1": 6}])
    );

    // Shutdown the pipeline
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;
}

/// Tests that pipeline panics are correctly reported by providing a SQL program
/// which will panic when data is pushed to it.
#[actix_web::test]
#[serial]
async fn pipeline_panic() {
    let config = setup().await;

    // Basic test pipeline with a SQL program which is known to panic
    create_and_deploy_test_pipeline(
        &config,
        "CREATE TABLE t1(c1 INTEGER); CREATE VIEW v1 AS SELECT ELEMENT(ARRAY [2, 3]) FROM t1;",
    )
    .await;

    // Start the pipeline
    let resp = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Wait until it is running
    config
        .wait_for_deployment_status(
            "test",
            PipelineStatus::Running,
            Duration::from_millis(1_000),
        )
        .await;

    // Push some data, which should cause a panic
    let _ = config
        .post_csv("/v0/pipelines/test/ingress/t1", "1\n2\n3\n".to_string())
        .await;

    // Ignore failures
    // assert_eq!(response.status(), StatusCode::OK, "Val: {:?}", response);

    // Should discover the error next time it polls the pipeline.
    let pipeline = config
        .wait_for_deployment_status("test", PipelineStatus::Failed, config.failed_timeout)
        .await;
    assert_eq!(
        pipeline["deployment_error"]["error_code"],
        "RuntimeError.WorkerPanic"
    );

    // Shutdown the pipeline
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;
}

/// Tests starting a pipeline, shutting it down, starting it again, and then shutting it down again.
#[actix_web::test]
#[serial]
async fn pipeline_restart() {
    let config = setup().await;

    // Basic test pipeline
    create_and_deploy_test_pipeline(
        &config,
        "CREATE TABLE t1(c1 INTEGER); CREATE VIEW v1 AS SELECT * FROM t1;",
    )
    .await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Running, config.start_timeout)
        .await;

    // Shutdown the pipeline
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Running, config.start_timeout)
        .await;

    // Shutdown the pipeline
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;
}

/// Tests that the pipeline runtime configuration is validated and stored correctly,
/// and that patching works on the field as whole.
#[actix_web::test]
#[serial]
async fn pipeline_runtime_config() {
    let config = setup().await;

    // Valid JSON for runtime_config
    for (runtime_config, expected) in [
        (
            None,
            serde_json::to_value(RuntimeConfig::default()).unwrap(),
        ),
        (
            Some(json!(null)),
            serde_json::to_value(RuntimeConfig::default()).unwrap(),
        ),
        (
            Some(json!({})),
            serde_json::to_value(RuntimeConfig::default()).unwrap(),
        ),
        (
            Some(json!({ "workers": 12 })),
            serde_json::to_value(RuntimeConfig {
                workers: 12,
                ..Default::default()
            })
            .unwrap(),
        ),
        (
            Some(json!({
                "workers": 100,
                "resources": {
                    "cpu_cores_min": 5,
                    "storage_mb_max": 2000,
                    "storage_class": "normal"
                }
            })),
            serde_json::to_value(RuntimeConfig {
                workers: 100,
                resources: ResourceConfig {
                    cpu_cores_min: Some(5),
                    storage_mb_max: Some(2000),
                    storage_class: Some("normal".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            })
            .unwrap(),
        ),
    ] {
        let mut body = json!({
            "name":  "test-1",
            "program_code": "sql-1"
        });
        if let Some(json) = runtime_config {
            body["runtime_config"] = json;
        }
        let mut response = config.put("/v0/pipelines/test-1", &body).await;
        assert!(response.status() == StatusCode::CREATED || response.status() == StatusCode::OK);
        let value: Value = response.json().await.unwrap();
        assert_eq!(value["runtime_config"], expected);
    }

    // Invalid JSON for runtime_config
    for runtime_config in [
        json!({ "workers": "not-a-number" }),
        json!({ "resources": { "storage_mb_max": "not-a-number" } }),
    ] {
        let body = json!({
            "name":  "test-1",
            "program_code": "sql-1",
            "runtime_config": runtime_config
        });
        let mut response = config.put("/v0/pipelines/test-1", &body).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let value: Value = response.json().await.unwrap();
        assert_eq!(value["error_code"], json!("InvalidRuntimeConfig"));
    }

    // Patching: original
    let body = json!({
        "name":  "test-2",
        "program_code": "sql-2",
        "runtime_config": {
            "workers": 100,
            "storage": true,
            "resources": {
                "cpu_cores_min": 2,
                "storage_mb_max": 500,
                "storage_class": "fast"
            }
        }
    });
    let response = config.post("/v0/pipelines", &body).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Patching: apply patch which does not affect runtime_config
    let mut response = config.patch("/v0/pipelines/test-2", &json!({})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert_eq!(
        value["runtime_config"],
        serde_json::to_value(RuntimeConfig {
            workers: 100,
            storage: Some(StorageOptions::default()),
            resources: ResourceConfig {
                cpu_cores_min: Some(2),
                storage_mb_max: Some(500),
                storage_class: Some("fast".to_string()),
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap()
    );

    // Patching: apply patch which affects runtime_config
    let body = json!({
        "runtime_config": {
            "workers": 1,
            "resources": {
                "storage_mb_max": 123,
            }
        }
    });
    let mut response = config.patch("/v0/pipelines/test-2", &body).await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert_eq!(
        value["runtime_config"],
        serde_json::to_value(RuntimeConfig {
            workers: 1,
            resources: ResourceConfig {
                storage_mb_max: Some(123),
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap()
    );
}

/// Tests that the pipeline program configuration is validated and stored correctly,
/// and that patching works on the field as whole.
#[actix_web::test]
#[serial]
async fn pipeline_program_config() {
    let config = setup().await;

    // Valid JSON for program_config
    for (program_config, expected) in [
        (
            None,
            serde_json::to_value(ProgramConfig::default()).unwrap(),
        ),
        (
            Some(json!(null)),
            serde_json::to_value(ProgramConfig::default()).unwrap(),
        ),
        (
            Some(json!({})),
            serde_json::to_value(ProgramConfig::default()).unwrap(),
        ),
        (
            Some(json!({ "profile": "dev" })),
            serde_json::to_value(ProgramConfig {
                profile: Some(CompilationProfile::Dev),
                ..Default::default()
            })
            .unwrap(),
        ),
        (
            Some(json!({ "cache": true })),
            serde_json::to_value(ProgramConfig {
                cache: true,
                ..Default::default()
            })
            .unwrap(),
        ),
        (
            Some(json!({ "profile": "dev", "cache": false })),
            serde_json::to_value(ProgramConfig {
                profile: Some(CompilationProfile::Dev),
                cache: false,
            })
            .unwrap(),
        ),
    ] {
        let mut body = json!({
            "name":  "test-1",
            "program_code": "sql-1"
        });
        if let Some(json) = program_config {
            body["program_config"] = json;
        }
        let mut response = config.put("/v0/pipelines/test-1", &body).await;
        assert!(response.status() == StatusCode::CREATED || response.status() == StatusCode::OK);
        let value: Value = response.json().await.unwrap();
        assert_eq!(value["program_config"], expected);
    }

    // Invalid JSON for program_config
    for program_config in [
        json!({ "profile": "does-not-exist" }),
        json!({ "cache": 123 }),
        json!({ "profile": 123 }),
        json!({ "profile": "unknown", "cache": "a" }),
    ] {
        let body = json!({
            "name":  "test-1",
            "program_code": "sql-1",
            "program_config": program_config
        });
        let mut response = config.put("/v0/pipelines/test-1", &body).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let value: Value = response.json().await.unwrap();
        assert_eq!(value["error_code"], json!("InvalidProgramConfig"));
    }

    // Patching: original
    let body = json!({
        "name":  "test-2",
        "program_code": "sql-2",
        "program_config": {
            "profile": "unoptimized",
            "cache": false
        }
    });
    let response = config.post("/v0/pipelines", &body).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Patching: apply patch which does not affect program_config
    let mut response = config.patch("/v0/pipelines/test-2", &json!({})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert_eq!(
        value["program_config"],
        serde_json::to_value(ProgramConfig {
            profile: Some(CompilationProfile::Unoptimized),
            cache: false,
        })
        .unwrap()
    );

    // Patching: apply patch which affects program_config
    let body = json!({
        "program_config": {
            "cache": true
        }
    });
    let mut response = config.patch("/v0/pipelines/test-2", &body).await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert_eq!(
        value["program_config"],
        serde_json::to_value(ProgramConfig {
            cache: true,
            ..Default::default()
        })
        .unwrap()
    );
}

/// Attempt to start a pipeline without it having finished its compilation fully.
/// This tests the early start mechanism.
#[actix_web::test]
#[serial]
async fn pipeline_start_without_compiling() {
    let config = setup().await;
    let body = json!({
        "name":  "test",
        "description": "desc",
        "runtime_config": {},
        "program_code": "CREATE TABLE foo (bar INTEGER);",
        "program_config": {}
    });
    let response = config.post("/v0/pipelines", &body).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Try starting the program when it is in the CompilingRust state.
    // There is a possibility that rust compilation is so fast that we don't poll
    // at that moment but that's unlikely.
    let now = Instant::now();
    println!("Waiting till program compilation state is in past the CompilingSql state...");
    loop {
        std::thread::sleep(Duration::from_millis(50));
        if now.elapsed().as_secs() > 30 {
            panic!("Took longer than 30 seconds to be past CompilingSql");
        }
        let mut response = config.get("/v0/pipelines/test").await;
        let val: Value = response.json().await.unwrap();
        let status = val["program_status"].clone();
        if status != json!("Pending") && status != json!("CompilingSql") {
            break;
        }
    }

    // Attempt to start the pipeline
    let resp = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Wait for it to actually start
    config
        .wait_for_deployment_status("test", PipelineStatus::Running, config.compilation_timeout)
        .await;
}

#[actix_web::test]
#[serial]
async fn json_ingress() {
    let config = setup().await;

    // Basic test pipeline
    create_and_deploy_test_pipeline(
        &config,
        "create table t1(c1 integer, c2 bool, c3 varchar) with ('materialized' = 'true'); create materialized view v1 as select * from t1;",
    )
        .await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status(
            "test",
            PipelineStatus::Running,
            Duration::from_millis(1_000),
        )
        .await;

    // Push some data using default json config.
    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=raw",
            r#"{"c1": 10, "c2": true}
            {"c1": 20, "c3": "foo"}"#
                .to_string(),
        )
        .await;
    assert!(response.status().is_success());

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1, c2, c3;")
            .await,
        json!([{"c1": 10, "c2": true, "c3": null}, {"c1": 20, "c2": null, "c3": "foo"}])
    );

    // Push more data using insert/delete format.
    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/t1?format=json&update_format=insert_delete",
            r#"{"delete": {"c1": 10, "c2": true}}
            {"insert": {"c1": 30, "c3": "bar"}}"#
                .to_string(),
        )
        .await;
    assert!(response.status().is_success());

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1, c2, c3;")
            .await,
        json!([{"c1": 20, "c2": null, "c3": "foo"}, {"c1": 30, "c2": null, "c3": "bar"}])
    );

    // Format data as json array.
    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",
            r#"{"insert": [40, true, "buzz"]}"#.to_string(),
        )
        .await;
    assert!(response.status().is_success());

    // Use array of updates instead of newline-delimited JSON
    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/t1?format=json&update_format=insert_delete&array=true",
            r#"[{"delete": [40, true, "buzz"]}, {"insert": [50, true, ""]}]"#.to_string(),
        )
        .await;
    assert!(response.status().is_success());

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from T1 order by c1, c2, c3;")
            .await,
        json!([{"c1": 20, "c2": null, "c3": "foo"}, {"c1": 30, "c2": null, "c3": "bar"}, {"c1": 50, "c2": true, "c3": ""}])
    );

    // Trigger parse errors.
    let mut response = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete&array=true",
            r#"[{"insert": [35, true, ""]}, {"delete": [40, "foo", "buzz"]}, {"insert": [true, true, ""]}]"#.to_string(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.body().await.unwrap();
    let error = std::str::from_utf8(&body).unwrap();
    assert_eq!(error, "{\"message\":\"Errors parsing input data (2 errors):\\n    Parse error (event #2): failed to deserialize JSON record: error parsing field 'c2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\\nInvalid fragment: '[40, \\\"foo\\\", \\\"buzz\\\"]'\\n    Parse error (event #3): failed to deserialize JSON record: error parsing field 'c1': invalid type: boolean `true`, expected i32 at line 1 column 5\\nInvalid fragment: '[true, true, \\\"\\\"]'\",\"error_code\":\"ParseErrors\",\"details\":{\"errors\":[{\"description\":\"failed to deserialize JSON record: error parsing field 'c2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\",\"event_number\":2,\"field\":\"c2\",\"invalid_bytes\":null,\"invalid_text\":\"[40, \\\"foo\\\", \\\"buzz\\\"]\",\"suggestion\":null},{\"description\":\"failed to deserialize JSON record: error parsing field 'c1': invalid type: boolean `true`, expected i32 at line 1 column 5\",\"event_number\":3,\"field\":\"c1\",\"invalid_bytes\":null,\"invalid_text\":\"[true, true, \\\"\\\"]\",\"suggestion\":null}],\"num_errors\":2}}");

    // Even records that are parsed successfully don't get ingested when
    // using array format.
    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1, c2, c3;")
            .await,
        json!([{"c1": 20, "c2": null, "c3": "foo"}, {"c1": 30, "c2": null, "c3": "bar"}, {"c1": 50, "c2": true, "c3": ""}])
    );

    let mut response = config
        .post_json(
            "/v0/pipelines/test/ingress/t1?format=json&update_format=insert_delete",
            r#"{"insert": [25, true, ""]}{"delete": [40, "foo", "buzz"]}{"insert": [true, true, ""]}"#.to_string(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.body().await.unwrap();
    let error = std::str::from_utf8(&body).unwrap();
    pretty_assertions::assert_eq!(error, "{\"message\":\"Errors parsing input data (2 errors):\\n    Parse error (event #4): failed to deserialize JSON record: error parsing field 'c2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\\nInvalid fragment: '[40, \\\"foo\\\", \\\"buzz\\\"]'\\n    Parse error (event #5): failed to deserialize JSON record: error parsing field 'c1': invalid type: boolean `true`, expected i32 at line 1 column 5\\nInvalid fragment: '[true, true, \\\"\\\"]'\",\"error_code\":\"ParseErrors\",\"details\":{\"errors\":[{\"description\":\"failed to deserialize JSON record: error parsing field 'c2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\",\"event_number\":4,\"field\":\"c2\",\"invalid_bytes\":null,\"invalid_text\":\"[40, \\\"foo\\\", \\\"buzz\\\"]\",\"suggestion\":null},{\"description\":\"failed to deserialize JSON record: error parsing field 'c1': invalid type: boolean `true`, expected i32 at line 1 column 5\",\"event_number\":5,\"field\":\"c1\",\"invalid_bytes\":null,\"invalid_text\":\"[true, true, \\\"\\\"]\",\"suggestion\":null}],\"num_errors\":2}}");

    // Even records that are parsed successfully don't get ingested when
    // using array format.
    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1, c2, c3;")
            .await,
        json!([{"c1": 20, "c2": null, "c3": "foo"}, {"c1": 25, "c2": true, "c3": ""}, {"c1": 30, "c2": null, "c3": "bar"}, {"c1": 50, "c2": true, "c3": ""}])
    );

    // Debezium CDC format
    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=debezium",
            r#"{"payload": {"op": "u", "before": [50, true, ""], "after": [60, true, "hello"]}}"#
                .to_string(),
        )
        .await;
    assert!(response.status().is_success());

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1, c2, c3;")
            .await,
        json!([{"c1": 20, "c2": null, "c3": "foo"}, {"c1": 25, "c2": true, "c3": ""}, {"c1": 30, "c2": null, "c3": "bar"}, {"c1": 60, "c2": true, "c3": "hello"}])
    );

    // Push some CSV data (the second record is invalid, but the other two should
    // get ingested).
    let mut response = config
        .post_csv(
            "/v0/pipelines/test/ingress/t1?format=csv",
            r#"15,true,foo
not_a_number,true,ΑαΒβΓγΔδ
16,false,unicode🚲"#
                .to_string(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.body().await.unwrap();
    let error = std::str::from_utf8(&body).unwrap();
    assert_eq!(error, "{\"message\":\"Errors parsing input data (1 errors):\\n    Parse error (event #2): failed to deserialize CSV record: error parsing field 'c1': field 0: invalid digit found in string\\nInvalid fragment: 'not_a_number,true,ΑαΒβΓγΔδ\\n'\",\"error_code\":\"ParseErrors\",\"details\":{\"errors\":[{\"description\":\"failed to deserialize CSV record: error parsing field 'c1': field 0: invalid digit found in string\",\"event_number\":2,\"field\":\"c1\",\"invalid_bytes\":null,\"invalid_text\":\"not_a_number,true,ΑαΒβΓγΔδ\\n\",\"suggestion\":null}],\"num_errors\":1}}");

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1, c2, c3;")
            .await,
        json!([{"c1": 15, "c2": true, "c3": "foo"}, {"c1": 16, "c2": false, "c3": "unicode🚲"}, {"c1": 20, "c2": null, "c3": "foo"}, {"c1": 25, "c2": true, "c3": ""}, {"c1": 30, "c2": null, "c3": "bar"}, {"c1": 60, "c2": true, "c3": "hello"}])
    );

    // Shutdown the pipeline
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;
}

/// Table with column of type MAP.
#[actix_web::test]
#[serial]
async fn map_column() {
    let config = setup().await;

    // Basic test pipeline
    create_and_deploy_test_pipeline(
        &config,
        "create table t1(c1 integer, c2 bool, c3 MAP<varchar, varchar>) with ('materialized' = 'true'); create view v1 as select * from t1;",
    )
        .await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status(
            "test",
            PipelineStatus::Running,
            Duration::from_millis(1_000),
        )
        .await;

    // Push some data using default json config.
    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=raw",
            r#"{"c1": 10, "c2": true, "c3": {"foo": "1", "bar": "2"}}
            {"c1": 20}"#
                .to_string(),
        )
        .await;
    assert!(response.status().is_success());

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1;")
            .await,
        json!([{"c1": 10, "c2": true, "c3": {"bar": "2", "foo": "1"}}, {"c1": 20, "c2": null, "c3": null}])
    );

    // Shutdown the pipeline
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;
}

#[actix_web::test]
#[serial]
async fn parse_datetime() {
    let config = setup().await;

    // Basic test pipeline
    create_and_deploy_test_pipeline(
        &config,
        "create table t1(t TIME, ts TIMESTAMP, d DATE) with ('materialized' = 'true');",
    )
    .await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status(
            "test",
            PipelineStatus::Running,
            Duration::from_millis(1_000),
        )
        .await;

    // The parser should trim leading and trailing white space when parsing
    // dates/times.
    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/t1?format=json&update_format=raw",
            r#"{"t":"13:22:00","ts": "2021-05-20 12:12:33","d": "2021-05-20"}
            {"t":" 11:12:33.483221092 ","ts": " 2024-02-25 12:12:33 ","d": " 2024-02-25 "}"#
                .to_string(),
        )
        .await;
    assert!(response.status().is_success());

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by t, ts, d;")
            .await,
        json!([{"d": "2024-02-25", "t": "11:12:33.483221092", "ts": "2024-02-25T12:12:33"}, {"d": "2021-05-20", "t": "13:22:00", "ts": "2021-05-20T12:12:33"}])
    );

    // Shutdown the pipeline
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;
}

#[actix_web::test]
#[serial]
async fn quoted_columns() {
    let config = setup().await;

    // Basic test pipeline
    create_and_deploy_test_pipeline(
        &config,
        r#"create table t1("c1" integer not null, "C2" bool not null, "😁❤" varchar not null, "αβγ" boolean not null, ΔΘ boolean not null) with ('materialized' = 'true')"#,
    )
        .await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status(
            "test",
            PipelineStatus::Running,
            Duration::from_millis(1_000),
        )
        .await;

    // Push some data using default json config.
    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=raw",
            r#"{"c1": 10, "C2": true, "😁❤": "foo", "αβγ": true, "δθ": false}"#.to_string(),
        )
        .await;
    assert!(response.status().is_success());

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by \"c1\";")
            .await,
        json!([{"C2": true, "c1": 10, "αβγ": true, "δθ": false, "😁❤": "foo"}])
    );

    // Shutdown the pipeline
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;
}

#[actix_web::test]
#[serial]
async fn primary_keys() {
    let config = setup().await;

    // Basic test pipeline
    create_and_deploy_test_pipeline(
        &config,
        r#"create table t1(id bigint not null, s varchar not null, primary key (id)) with ('materialized' = 'true')"#,
    )
        .await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status(
            "test",
            PipelineStatus::Running,
            Duration::from_millis(1_000),
        )
        .await;

    // Push some data using default json config.
    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",
            r#"{"insert":{"id":1, "s": "1"}}
{"insert":{"id":2, "s": "2"}}"#
                .to_string(),
        )
        .await;
    assert!(response.status().is_success());

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by id;")
            .await,
        json!([{"id": 1, "s": "1"}, {"id": 2, "s": "2"}])
    );

    // Make some changes.
    let req = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",
            r#"{"insert":{"id":1, "s": "1-modified"}}
{"update":{"id":2, "s": "2-modified"}}"#
                .to_string(),
        )
        .await;
    assert!(req.status().is_success());

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by id;")
            .await,
        json!([{"id": 1, "s": "1-modified"}, {"id": 2, "s": "2-modified"}])
    );

    // Delete a key
    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",
            r#"{"delete":{"id":2}}"#.to_string(),
        )
        .await;
    assert!(response.status().is_success());

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by id;")
            .await,
        json!([ {"id": 1, "s": "1-modified"}])
    );

    // Shutdown the pipeline
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;
}

/// Test case-sensitive table ingress/egress behavior.
#[actix_web::test]
#[serial]
async fn case_sensitive_tables() {
    let config = setup().await;

    // Table "TaBle1" and view "V1" are case-sensitive and can only be accessed
    // by quoting their name.
    // Table "v1" is also case-sensitive, but since its name is lowercase, it
    // can be accessed as both "v1" and "\"v1\""
    create_and_deploy_test_pipeline(
        &config,
        r#"create table "TaBle1"(id bigint not null);
create table table1(id bigint);
create materialized view "V1" as select * from "TaBle1";
create materialized view "v1" as select * from table1;"#,
    )
    .await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status(
            "test",
            PipelineStatus::Running,
            Duration::from_millis(1_000),
        )
        .await;

    let mut response1 = config.delta_stream_request_json("test", "\"V1\"").await;
    let mut response2 = config.delta_stream_request_json("test", "\"v1\"").await;

    // Push some data using default json config.
    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/\"TaBle1\"?format=json&update_format=insert_delete",
            r#"{"insert":{"id":1}}"#.to_string(),
        )
        .await;
    assert!(response.status().is_success());

    let response = config
        .post_json(
            "/v0/pipelines/test/ingress/table1?format=json&update_format=insert_delete",
            r#"{"insert":{"id":2}}"#.to_string(),
        )
        .await;
    assert!(response.status().is_success());

    config
        .read_expected_response_json(
            &mut response1,
            Duration::from_millis(10_000),
            &[json!({"insert": {"id":1}})],
        )
        .await;

    config
        .read_expected_response_json(
            &mut response2,
            Duration::from_millis(10_000),
            &[json!({"insert": {"id":2}})],
        )
        .await;

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from \"V1\";")
            .await,
        json!([{ "id": 1 }])
    );

    assert_eq!(
        config.adhoc_query_json("test", "select * from v1;").await,
        json!([{ "id": 2 }])
    );

    // Shutdown the pipeline
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;
}

#[actix_web::test]
#[serial]
async fn duplicate_outputs() {
    let config = setup().await;

    // Basic test pipeline
    create_and_deploy_test_pipeline(
        &config,
        r#"create table t1(id bigint not null, s varchar not null); create view v1 as select s from t1;"#,
    )
        .await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status(
            "test",
            PipelineStatus::Running,
            Duration::from_millis(1_000),
        )
        .await;

    let mut response = config.delta_stream_request_json("test", "V1").await;

    // Push some data using default json config.
    let response2 = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",
            r#"{"insert":{"id":1, "s": "1"}}
{"insert":{"id":2, "s": "2"}}"#
                .to_string(),
        )
        .await;
    assert!(response2.status().is_success());

    config
        .read_expected_response_json(
            &mut response,
            Duration::from_millis(10_000),
            &[json!({"insert": {"s":"1"}}), json!({"insert":{"s":"2"}})],
        )
        .await;

    // Push some more data
    let response2 = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",
            r#"{"insert":{"id":3, "s": "3"}}
{"insert":{"id":4, "s": "4"}}"#
                .to_string(),
        )
        .await;
    assert!(response2.status().is_success());

    config
        .read_expected_response_json(
            &mut response,
            Duration::from_millis(10_000),
            &[json!({"insert": {"s":"3"}}), json!({"insert":{"s":"4"}})],
        )
        .await;

    // Push more records that will create duplicate outputs.
    let response2 = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",
            r#"{"insert":{"id":5, "s": "1"}}
{"insert":{"id":6, "s": "2"}}"#
                .to_string(),
        )
        .await;
    assert!(response2.status().is_success());

    config
        .read_expected_response_json(
            &mut response,
            Duration::from_millis(10_000),
            &[json!({"insert": {"s":"1"}}), json!({"insert":{"s":"2"}})],
        )
        .await;

    // Shutdown the pipeline
    let response2 = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response2.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;
}

#[actix_web::test]
#[serial]
async fn upsert() {
    let config = setup().await;

    // Basic test pipeline
    create_and_deploy_test_pipeline(
        &config,
        r#"create table t1(
            id1 bigint not null,
            id2 bigint not null,
            str1 varchar not null,
            str2 varchar,
            int1 bigint not null,
            int2 bigint,
            primary key(id1, id2));"#,
    )
    .await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status(
            "test",
            PipelineStatus::Running,
            Duration::from_millis(1_000),
        )
        .await;

    let mut response = config.delta_stream_request_json("test", "T1").await;

    // Push some data.
    //
    // NOTE: we use `array=true` to push data in this test to make sure that all updates are
    // delivered atomically and all outputs are produced in a single chunk. It is still
    // theoretically possible that inputs are split across multiple `step`'s due to the
    // `ZSetHandle::append` method not being atomic.  This is highly improbable, but if it
    // happens, increasing buffering delay in DBSP should solve that.
    let response2 = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete&array=true",
            // Add several identical records with different id's
            r#"[{"insert":{"id1":1, "id2":1, "str1": "1", "int1": 1}},{"insert":{"id1":2, "id2":1, "str1": "1", "int1": 1}},{"insert":{"id1":3, "id2":1, "str1": "1", "int1": 1}}]"#
                .to_string(),
        )
        .await;
    assert!(response2.status().is_success());

    config
        .read_expected_response_json(
            &mut response,
            Duration::from_millis(10_000),
            &[
                json!({"insert": {"id1":1,"id2":1,"str1":"1","str2":null,"int1":1,"int2":null}}),
                json!({"insert": {"id1":2,"id2":1,"str1":"1","str2":null,"int1":1,"int2":null}}),
                json!({"insert": {"id1":3,"id2":1,"str1":"1","str2":null,"int1":1,"int2":null}}),
            ],
        )
        .await;

    let response2 = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete&array=true",
            // 1: Update 'str1'.
            // 2: Update 'str2'.
            // 3: Overwrite entire record.
            r#"[{"update":{"id1":1, "id2":1, "str1": "2"}},{"update":{"id1":2, "id2":1, "str2": "foo"}},{"insert":{"id1":3, "id2":1, "str1": "1", "str2": "2", "int1":3, "int2":33}}]"#
                .to_string(),
        )
        .await;
    assert!(response2.status().is_success());

    config
        .read_expected_response_json(
            &mut response,
            Duration::from_millis(10_000),
            &[
                json!({"delete": {"id1":1,"id2":1,"str1":"1","str2":null,"int1":1,"int2":null}}),
                json!({"delete": {"id1":2,"id2":1,"str1":"1","str2":null,"int1":1,"int2":null}}),
                json!({"delete": {"id1":3,"id2":1,"str1":"1","str2":null,"int1":1,"int2":null}}),
                json!({"insert": {"id1":1,"id2":1,"str1":"2","str2":null,"int1":1,"int2":null}}),
                json!({"insert": {"id1":2,"id2":1,"str1":"1","str2":"foo","int1":1,"int2":null}}),
                json!({"insert": {"id1":3,"id2":1,"str1":"1","str2":"2","int1":3,"int2":33}}),
            ],
        )
        .await;

    let response2 = config
        .post_json(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete&array=true",
            // 1: Update command that doesn't modify any fields - noop.
            // 2: Clear 'str2' to null.
            // 3: Delete record.
            // 4: Delete non-existing key - noop.
            // 5: Update non-existing key - noop.
            r#"[{"update":{"id1":1, "id2":1}},{"update":{"id1":2, "id2":1, "str2": null}},{"delete":{"id1":3, "id2":1}},{"delete":{"id1":4, "id2":1}},{"update":{"id1":4, "id2":1, "int1":0, "str1":""}}]"#
                .to_string(),
        )
        .await;
    assert!(response2.status().is_success());

    config
        .read_expected_response_json(
            &mut response,
            Duration::from_millis(10_000),
            &[
                json!({"delete": {"id1":2,"id2":1,"str1":"1","str2":"foo","int1":1,"int2":null}}),
                json!({"delete": {"id1":3,"id2":1,"str1":"1","str2":"2","int1":3,"int2":33}}),
                json!({"insert": {"id1":2,"id2":1,"str1":"1","str2":null,"int1":1,"int2":null}}),
            ],
        )
        .await;

    // Shutdown the pipeline
    let response2 = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response2.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;
}

#[actix_web::test]
#[serial]
async fn pipeline_name_invalid() {
    let config = setup().await;
    // Empty
    let request_body = json!({
        "name": "",
        "description": "", "runtime_config": {}, "program_code": "", "program_config": {}
    });
    let response = config.post("/v0/pipelines", &request_body).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Too long
    let request_body = json!({
        "name": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "description": "", "runtime_config": {}, "program_code": "", "program_config": {}
    });
    let response = config.post("/v0/pipelines", &request_body).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Invalid characters
    let request_body = json!({
        "name": "%abc",
        "description": "", "runtime_config": {}, "program_code": "", "program_config": {}
    });
    let response = config.post("/v0/pipelines", &request_body).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[actix_web::test]
#[serial]
async fn pipeline_adhoc_query() {
    const PROGRAM: &str = r#"
CREATE TABLE not_materialized(id bigint not null);

CREATE TABLE "TaBle1"(id bigint not null) with ('materialized' = 'true');

CREATE TABLE t1 (
    id INT NOT NULL,
    dt DATE NOT NULL,
    uid UUID NOT NULL
) with (
  'materialized' = 'true',
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 5
        }]
      }
    }
  }]'
);
CREATE TABLE t2 (
    id INT NOT NULL,
    st VARCHAR NOT NULL
) with (
  'materialized' = 'true',
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 5
        }]
      }
    }
  }]'
);
CREATE MATERIALIZED VIEW joined AS ( SELECT t1.dt AS c1, t2.st AS c2, t1.uid as c3 FROM t1, t2 WHERE t1.id = t2.id );
CREATE MATERIALIZED VIEW view_of_not_materialized AS ( SELECT * FROM not_materialized );
"#;
    const ADHOC_SQL_A: &str = "SELECT * FROM joined";
    const ADHOC_SQL_B: &str =
        "SELECT t1.dt AS c1, t2.st AS c2, t1.uid as c3 FROM t1, t2 WHERE t1.id = t2.id";

    let config = setup().await;
    create_and_deploy_test_pipeline(&config, PROGRAM).await;
    let resp = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    config
        .wait_for_deployment_status(
            "test",
            PipelineStatus::Running,
            Duration::from_millis(1_000),
        )
        .await;

    for format in &["text", "json"] {
        let mut r0 = config
            .adhoc_query("test", "SELECT * FROM \"TaBle1\"", format)
            .await;
        assert_eq!(r0.status(), StatusCode::OK);
        let r0_body = r0.body().await.unwrap();
        let r0 = std::str::from_utf8(r0_body.as_ref()).unwrap();
        let r0_sorted: Vec<String> = r0.split('\n').map(|s| s.to_string()).collect();
        if format == &"text" {
            // Empty table prints header in text format
            assert_eq!(r0_sorted.len(), 4);
        } else if format == &"json" {
            assert_eq!(r0_sorted.len(), 1);
        }

        let mut r1 = config.adhoc_query("test", ADHOC_SQL_A, format).await;
        assert_eq!(r1.status(), StatusCode::OK);
        let b1_body = r1.body().await.unwrap();
        let b1 = std::str::from_utf8(b1_body.as_ref()).unwrap();
        let mut b1_sorted: Vec<String> = b1.split('\n').map(|s| s.to_string()).collect();
        b1_sorted.sort();

        let mut r2 = config.adhoc_query("test", ADHOC_SQL_B, format).await;
        assert_eq!(r2.status(), StatusCode::OK);
        let b2_body = r2.body().await.unwrap();
        let b2 = std::str::from_utf8(b2_body.as_ref()).unwrap();
        let mut b2_sorted: Vec<String> = b2.split('\n').map(|s| s.to_string()).collect();
        b2_sorted.sort();
        assert_eq!(b1_sorted, b2_sorted);
    }

    // Test parquet format, here we can't just sort it, so we ensure that view and adhoc join have the same order
    let q1 = format!("{} ORDER BY c1, c2, c3", ADHOC_SQL_A);
    let mut r1 = config.adhoc_query("test", q1.as_str(), "parquet").await;
    assert_eq!(r1.status(), StatusCode::OK);
    let b1_body = r1.body().await.unwrap();

    let q2 = format!("{} ORDER BY c1, c2, c3", ADHOC_SQL_B);
    let mut r2 = config.adhoc_query("test", q2.as_str(), "parquet").await;
    assert_eq!(r2.status(), StatusCode::OK);
    let b2_body = r2.body().await.unwrap();
    assert!(!b1_body.is_empty());
    assert_eq!(b1_body, b2_body);

    // Handle table casing
    let mut r = config
        .adhoc_query("test", "SELECT * FROM \"TaBle1\"", "text")
        .await;
    assert_eq!(r.status(), StatusCode::OK);
    let b = r.body().await.unwrap();
    assert!(!b.is_empty());

    // Invalid SQL returns 400
    let r = config
        .adhoc_query("test", "SELECT * FROM invalid_table", "text")
        .await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    let mut r = config.adhoc_query("test", "SELECT 1/0", "text").await;
    assert_eq!(r.status(), StatusCode::OK);
    assert!(String::from_utf8_lossy(r.body().await.unwrap().as_ref()).contains("ERROR"));
    let r = config
        .adhoc_query("test", "SELECT * FROM table1", "text")
        .await;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    // Test insert statements for materialized tables
    const ADHOC_SQL_COUNT: &str = "SELECT COUNT(*) from t1";
    const ADHOC_SQL_INSERT: &str = "INSERT INTO t1 VALUES (99, '2020-01-01', 'c32d330f-5757-4ada-bcf6-1fac2d54e37f'), (100, '2020-01-01', '00000000-0000-0000-0000-000000000000')";

    let mut r = config.adhoc_query("test", ADHOC_SQL_COUNT, "json").await;
    assert_eq!(r.status(), StatusCode::OK);
    let cnt_body = r.body().await.unwrap();
    let cnt_ret = std::str::from_utf8(cnt_body.as_ref()).unwrap();
    let cnt_ret_json = serde_json::from_str::<Value>(cnt_ret).unwrap();
    assert_eq!(cnt_ret_json, json!({"count(*)": 5}));

    let mut r = config.adhoc_query("test", ADHOC_SQL_INSERT, "json").await;
    assert_eq!(r.status(), StatusCode::OK);
    let ins_body = r.body().await.unwrap();
    let ins_ret = std::str::from_utf8(ins_body.as_ref()).unwrap();
    let ins_ret_json = serde_json::from_str::<Value>(ins_ret).unwrap();
    assert_eq!(ins_ret_json, json!({"count": 2}));

    let mut r = config.adhoc_query("test", ADHOC_SQL_COUNT, "json").await;
    assert_eq!(r.status(), StatusCode::OK);
    let cnt_body = r.body().await.unwrap();
    let cnt_ret = std::str::from_utf8(cnt_body.as_ref()).unwrap();
    let cnt_ret_json = serde_json::from_str::<Value>(cnt_ret).unwrap();
    // This needs to ensure we step() the circuit before we return from insert
    // for the assert to hold
    assert_eq!(cnt_ret_json, json!({"count(*)": 7}));

    // Make sure we can insert into non-materialized tables too
    let mut r = config
        .adhoc_query(
            "test",
            "SELECT COUNT(*) from view_of_not_materialized",
            "json",
        )
        .await;
    assert_eq!(r.status(), StatusCode::OK);
    let cnt_body = r.body().await.unwrap();
    let cnt_ret = std::str::from_utf8(cnt_body.as_ref()).unwrap();
    let cnt_ret_json = serde_json::from_str::<Value>(cnt_ret).unwrap();
    assert_eq!(cnt_ret_json, json!({"count(*)": 0}));

    let mut r = config
        .adhoc_query(
            "test",
            "INSERT INTO not_materialized VALUES (99), (100)",
            "json",
        )
        .await;
    assert_eq!(r.status(), StatusCode::OK);
    let ins_body = r.body().await.unwrap();
    let ins_ret = std::str::from_utf8(ins_body.as_ref()).unwrap();
    let ins_ret_json = serde_json::from_str::<Value>(ins_ret).unwrap();
    assert_eq!(ins_ret_json, json!({"count": 2}));

    let mut r = config
        .adhoc_query(
            "test",
            "SELECT COUNT(*) from view_of_not_materialized",
            "json",
        )
        .await;
    assert_eq!(r.status(), StatusCode::OK);
    let cnt_body = r.body().await.unwrap();
    let cnt_ret = std::str::from_utf8(cnt_body.as_ref()).unwrap();
    let cnt_ret_json = serde_json::from_str::<Value>(cnt_ret).unwrap();
    assert_eq!(cnt_ret_json, json!({"count(*)": 2}));
}

/// We should be able to query a table that never received any input.
///
/// This is a regression test for a bug where we called unwrap on the persistent snapshots,
/// which were not yet initialized/set because the circuit has never stepped.
#[actix_web::test]
#[serial]
async fn pipeline_adhoc_query_empty() {
    const PROGRAM: &str = r#"
CREATE TABLE "TaBle1"(id bigint not null) with ('materialized' = 'true');
"#;
    let config = setup().await;
    create_and_deploy_test_pipeline(&config, PROGRAM).await;
    let resp = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    const ADHOC_SQL_EMPTY: &str = "SELECT COUNT(*) from \"TaBle1\"";
    let mut r = config.adhoc_query("test", ADHOC_SQL_EMPTY, "json").await;
    assert_eq!(r.status(), StatusCode::OK);
    let cnt_body = r.body().await.unwrap();
    let cnt_ret = std::str::from_utf8(cnt_body.as_ref()).unwrap();
    let cnt_ret_json = serde_json::from_str::<Value>(cnt_ret).unwrap();
    assert_eq!(cnt_ret_json, json!({"count(*)": 0}));
}

/// The pipeline should transition to Shutdown status when being shutdown after starting.
/// This test will take at least 20 seconds due to various waiting times after starting.
#[actix_web::test]
#[serial]
async fn pipeline_shutdown_after_start() {
    let config = setup().await;
    create_and_deploy_test_pipeline(&config, "CREATE TABLE t1(c1 INTEGER);").await;

    // Test a variety of waiting before shutdown to capture the various states
    for duration_ms in [
        0, 50, 100, 250, 500, 750, 1000, 1250, 1500, 1750, 2000, 5000, 10000,
    ] {
        // Start pipeline in paused state
        let response = config.post_no_body("/v0/pipelines/test/start").await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // Shortly wait for the pipeline to transition to next state(s)
        sleep(Duration::from_millis(duration_ms)).await;

        // Shut it down
        let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // Check that the pipeline becomes Shutdown
        config
            .wait_for_deployment_status("test", PipelineStatus::Shutdown, Duration::from_secs(10))
            .await;
    }
}

#[actix_web::test]
#[serial]
async fn test_get_metrics() {
    let config = setup().await;
    // Basic test pipeline with a SQL program which is known to panic
    create_and_deploy_test_pipeline(
        &config,
        "CREATE TABLE t1(c1 INTEGER) with ('materialized' = 'true'); CREATE VIEW v1 AS SELECT * FROM t1;",
    ).await;

    // Again pause the pipeline before it is started
    let response = config.post_no_body("/v0/pipelines/test/pause").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Running, config.resume_timeout)
        .await;

    // Push some data
    let response = config
        .post_csv("/v0/pipelines/test/ingress/t1", "1\n2\n3\n".to_string())
        .await;
    assert!(response.status().is_success());

    let mut response = config.get("/v0/metrics").await;

    let resp = response.body().await.unwrap_or_default();
    let s = String::from_utf8(resp.to_vec()).unwrap_or_default();

    assert!(s.contains("# TYPE"));

    // Shutdown the pipeline
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;

    // Delete the pipeline
    let response = config.delete("/v0/pipelines/test").await;
    assert_eq!(StatusCode::OK, response.status());
}

/// Tests that logs can be retrieved from the pipeline.
/// TODO: test in the other deployment statuses whether logs can be retrieved
#[actix_web::test]
#[serial]
async fn pipeline_logs() {
    let config = setup().await;

    // Retrieve logs of non-existent pipeline
    let response = config.get("/v0/pipelines/test/logs").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Create pipeline
    let request_body = json!({
        "name": "test",
        "description": "Description of the test pipeline",
        "runtime_config": {},
        "program_code": "CREATE TABLE t1(c1 INTEGER) with ('materialized' = 'true');",
        "program_config": {}
    });
    let response = config.post("/v0/pipelines", &request_body).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Retrieve logs (shutdown)
    let response_logs = config.get("/v0/pipelines/test/logs").await;
    assert_eq!(response_logs.status(), StatusCode::SERVICE_UNAVAILABLE);

    // Wait for its program compilation completion
    config
        .wait_for_compilation("test", 1, config.compilation_timeout)
        .await;

    // Start the pipeline in Paused state
    let response = config.post_no_body("/v0/pipelines/test/pause").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    // Await it reaching paused status
    config
        .wait_for_deployment_status("test", PipelineStatus::Paused, config.start_timeout)
        .await;

    // Retrieve logs (paused)
    let mut response_logs_paused = config.get("/v0/pipelines/test/logs").await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Running, config.resume_timeout)
        .await;

    // Retrieve logs (running)
    let mut response_logs_running = config.get("/v0/pipelines/test/logs").await;

    // Shut the pipeline down
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;

    // Check the logs
    assert_eq!(response_logs_paused.status(), StatusCode::OK);
    assert_eq!(response_logs_running.status(), StatusCode::OK);
    let logs1 = String::from_utf8(response_logs_paused.body().await.unwrap().to_vec()).unwrap();
    let logs2 = String::from_utf8(response_logs_running.body().await.unwrap().to_vec()).unwrap();
    let normal_ending = format!("{}\n", LOGS_END_MESSAGE);
    assert!(
        logs1.ends_with(&normal_ending),
        "Unexpected logs ending: {logs1}"
    );
    assert!(
        logs2.ends_with(&normal_ending),
        "Unexpected logs ending: {logs2}"
    );

    // Retrieve logs (shutdown)
    let response_logs = config.get("/v0/pipelines/test/logs").await;
    assert_eq!(response_logs.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// The compiler needs to handle and continue to function when the pipeline is deleted
/// during program compilation.
#[actix_web::test]
#[serial]
async fn pipeline_deleted_during_program_compilation() {
    let config = setup().await;

    // Test a variety of waiting before deletion
    for duration_ms in [0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000] {
        // Create pipeline
        let request_body = json!({
            "name": "test-pddpc",
            "description": "Description of the test pipeline",
            "runtime_config": {},
            "program_code": "",
            "program_config": {}
        });
        let response = config.post("/v0/pipelines", &request_body).await;
        assert_eq!(response.status(), StatusCode::CREATED);

        // Wait for compilation to progress
        sleep(Duration::from_millis(duration_ms)).await;

        // Delete the pipeline
        let response = config.delete("/v0/pipelines/test-pddpc").await;
        assert_eq!(StatusCode::OK, response.status());
    }

    // Validate the compiler still works correctly by fully compiling a program
    create_and_deploy_test_pipeline(&config, "").await;
}

/// Retrieves whether pipeline is paused, connector is paused, and number of processed
/// records, which is used by the basic orchestration test.
async fn basic_orchestration_info(
    config: &TestConfig,
    pipeline_name: &str,
    table_name: &str,
    connector_name: &str,
) -> (bool, bool, u64) {
    let stats = config.stats_json(pipeline_name).await;
    let pipeline_paused = stats["global_metrics"]["state"].as_str() == Some("Paused");
    let num_processed = stats["global_metrics"]["total_processed_records"]
        .as_u64()
        .unwrap();

    let connector_paused = config
        .input_connector_stats_json(pipeline_name, table_name, connector_name)
        .await["paused"]
        .as_bool()
        .unwrap();
    (pipeline_paused, connector_paused, num_processed)
}

/// Tests the orchestration of the pipeline, which means the starting and pausing of the
/// pipeline itself as well as its connectors individually. This tests the basic processing
/// of data and handling of case sensitivity and special characters.
#[actix_web::test]
#[serial]
async fn pipeline_orchestration_basic() {
    for (table_name, connector_name) in [
        // Case-insensitive table name
        ("numbers", "c1"),
        // Case-insensitive table name (with some non-alphanumeric characters that do not need to be encoded)
        ("numbersC0_", "aA0_-"),
        // Case-sensitive table name
        ("\"Numbers\"", "c1"),
        // Case-sensitive table name with special characters that need to be encoded
        ("\"numbers +C0_-,.!%()&/\"", "aA0_-"),
    ] {
        let encoded_table_name = urlencoding::encode(table_name).to_string();

        // One table with one connector
        let config = setup().await;
        let sql = format!("
            CREATE TABLE {table_name} (
                num DOUBLE
            ) WITH (
                'connectors' = '[{{
                    \"name\": \"{connector_name}\",
                    \"transport\": {{
                        \"name\": \"datagen\",
                        \"config\": {{\"plan\": [{{ \"rate\": 100, \"fields\": {{ \"num\": {{ \"range\": [0, 1000], \"strategy\": \"uniform\" }} }} }}]}}
                    }}
                }}]'
            );
        ");
        create_and_deploy_test_pipeline(&config, &sql).await;

        // Pipeline is paused, connector is running
        sleep(Duration::from_millis(500)).await;
        let (pipeline_paused, connector_paused, num_processed) =
            basic_orchestration_info(&config, "test", table_name, connector_name).await;
        assert!(pipeline_paused);
        assert!(!connector_paused);
        assert_eq!(num_processed, 0);

        // Pause the connector
        assert_eq!(
            config
                .post_no_body(format!(
                    "/v0/pipelines/test/tables/{encoded_table_name}/connectors/{connector_name}/pause"
                ))
                .await
                .status(),
            StatusCode::OK
        );

        // Pipeline is paused, connector is paused
        sleep(Duration::from_millis(500)).await;
        let (pipeline_paused, connector_paused, num_processed) =
            basic_orchestration_info(&config, "test", table_name, connector_name).await;
        assert!(pipeline_paused);
        assert!(connector_paused);
        assert_eq!(num_processed, 0);

        // Start the pipeline
        let response = config.post_no_body("/v0/pipelines/test/start").await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        config
            .wait_for_deployment_status(
                "test",
                PipelineStatus::Running,
                Duration::from_millis(1_000),
            )
            .await;

        // Pipeline is running, connector is paused
        sleep(Duration::from_millis(500)).await;
        let (pipeline_paused, connector_paused, num_processed) =
            basic_orchestration_info(&config, "test", table_name, connector_name).await;
        assert!(!pipeline_paused);
        assert!(connector_paused);
        assert_eq!(num_processed, 0);

        // Start the connector
        assert_eq!(
            config
                .post_no_body(format!(
                    "/v0/pipelines/test/tables/{encoded_table_name}/connectors/{connector_name}/start"
                ))
                .await
                .status(),
            StatusCode::OK
        );

        // Pipeline is running, connector is running
        sleep(Duration::from_millis(500)).await;
        let (pipeline_paused, connector_paused, num_processed) =
            basic_orchestration_info(&config, "test", table_name, connector_name).await;
        assert!(!pipeline_paused);
        assert!(!connector_paused);
        assert!(num_processed > 0);
    }
}

/// Tests for orchestration the cases where errors should be returned.
#[actix_web::test]
#[serial]
async fn pipeline_orchestration_errors() {
    let config = setup().await;
    let sql = r#"
        CREATE TABLE numbers1 (
            num DOUBLE
        ) WITH (
            'connectors' = '[{
                "name": "c1",
                "transport": {
                    "name": "datagen",
                    "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [0, 1000], "strategy": "uniform" } } }]}
                }
            }]'
        );
    "#;
    create_and_deploy_test_pipeline(&config, sql).await;

    // ACCEPTED
    for endpoint in ["/v0/pipelines/test/start", "/v0/pipelines/test/pause"] {
        assert_eq!(
            config.post_no_body(endpoint).await.status(),
            StatusCode::ACCEPTED,
            "POST {endpoint}"
        );
    }

    // OK
    for endpoint in [
        "/v0/pipelines/test/tables/numbers1/connectors/c1/start",
        "/v0/pipelines/test/tables/numbers1/connectors/c1/pause",
        "/v0/pipelines/test/tables/Numbers1/connectors/c1/pause",
        "/v0/pipelines/test/tables/NUMBERS1/connectors/c1/pause",
        "/v0/pipelines/test/tables/%22numbers1%22/connectors/c1/pause",
    ] {
        assert_eq!(
            config.post_no_body(endpoint).await.status(),
            StatusCode::OK,
            "POST {endpoint}"
        );
    }

    // BAD REQUEST
    for endpoint in [
        "/v0/pipelines/test/action2", // Invalid pipeline action
        "/v0/pipelines/test/Start",   // Invalid pipeline action (case-sensitive)
        "/v0/pipelines/test/tables/numbers1/connectors/c1/action2", // Invalid connector action
        "/v0/pipelines/test/tables/numbers1/connectors/c1/START", // Invalid connector action (case-sensitive)
    ] {
        assert_eq!(
            config.post_no_body(endpoint).await.status(),
            StatusCode::BAD_REQUEST,
            "POST {endpoint}"
        );
    }

    // NOT FOUND
    for endpoint in [
        "/v0/pipelines/test2/start", // Pipeline not found
        "/v0/pipelines/test2/tables/numbers1/connectors/c1/start", // Pipeline not found
        "/v0/pipelines/test/tables/numbers1/connectors/c2/start", // Connector not found
        "/v0/pipelines/test/tables/numbers1/connectors/C1/start", // Connector not found (case-sensitive)
        "/v0/pipelines/test/tables/numbers2/connectors/c1/start", // Table not found
        "/v0/pipelines/test/tables/numbers2/connectors/c2/start", // Table and connector not found
        "/v0/pipelines/test/tables/%22Numbers1%22/connectors/c1/pause", // Table not found (case-sensitive due to double quotes)
    ] {
        assert_eq!(
            config.post_no_body(endpoint).await.status(),
            StatusCode::NOT_FOUND,
            "POST {endpoint}"
        );
    }
}

#[derive(Debug)]
enum OrchestrationTestStep {
    StartPipeline,
    PausePipeline,
    StartConnector(u32),
    PauseConnector(u32),
}

/// Tests for orchestration that the effects (i.e., pipeline and connector state) are
/// indeed as expected after each scenario consisting of various start and pause steps.
#[actix_web::test]
#[serial]
async fn pipeline_orchestration_scenarios() {
    let config = setup().await;

    // Pipeline with SQL of a table with two connectors
    let sql = r#"
        CREATE TABLE numbers (
            num DOUBLE
        ) WITH (
            'connectors' = '[
                {
                    "name": "c1",
                    "transport": {
                        "name": "datagen",
                        "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [0, 1000], "strategy": "uniform" } } }]}
                    }
                },
                {
                    "name": "c2",
                    "transport": {
                        "name": "datagen",
                        "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [1000, 2000], "strategy": "uniform" } } }]}
                    }
                }
            ]'
        );
    "#;
    create_and_deploy_test_pipeline(&config, sql).await;

    // Shutdown for the first scenario
    assert_eq!(
        config
            .post_no_body("/v0/pipelines/test/shutdown")
            .await
            .status(),
        StatusCode::ACCEPTED
    );
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
        .await;

    // Various scenarios (steps) and the expected outcome (what is paused)
    for (steps, expected_pipeline_paused, expected_c1_paused, expected_c2_paused) in [
        // All four combination when the pipeline is paused
        (
            vec![OrchestrationTestStep::PausePipeline],
            true,
            false,
            false,
        ),
        (
            vec![
                OrchestrationTestStep::PausePipeline,
                OrchestrationTestStep::PauseConnector(1),
            ],
            true,
            true,
            false,
        ),
        (
            vec![
                OrchestrationTestStep::PausePipeline,
                OrchestrationTestStep::PauseConnector(2),
            ],
            true,
            false,
            true,
        ),
        (
            vec![
                OrchestrationTestStep::PausePipeline,
                OrchestrationTestStep::PauseConnector(1),
                OrchestrationTestStep::PauseConnector(2),
            ],
            true,
            true,
            true,
        ),
        // All four combinations when the pipeline is running
        (
            vec![OrchestrationTestStep::StartPipeline],
            false,
            false,
            false,
        ),
        (
            vec![
                OrchestrationTestStep::StartPipeline,
                OrchestrationTestStep::PauseConnector(1),
            ],
            false,
            true,
            false,
        ),
        (
            vec![
                OrchestrationTestStep::StartPipeline,
                OrchestrationTestStep::PauseConnector(2),
            ],
            false,
            false,
            true,
        ),
        (
            vec![
                OrchestrationTestStep::StartPipeline,
                OrchestrationTestStep::PauseConnector(1),
                OrchestrationTestStep::PauseConnector(2),
            ],
            false,
            true,
            true,
        ),
        // Start then pause the pipeline
        (
            vec![
                OrchestrationTestStep::StartPipeline,
                OrchestrationTestStep::PausePipeline,
            ],
            true,
            false,
            false,
        ),
        // Pause then start again the connector
        (
            vec![
                OrchestrationTestStep::StartPipeline,
                OrchestrationTestStep::PauseConnector(1),
                OrchestrationTestStep::StartConnector(1),
            ],
            false,
            false,
            false,
        ),
    ] {
        // Perform the steps
        for (i, step) in steps.iter().enumerate() {
            match step {
                OrchestrationTestStep::StartPipeline => {
                    let response = config.post_no_body("/v0/pipelines/test/start").await;
                    assert_eq!(
                        response.status(),
                        StatusCode::ACCEPTED,
                        "during step {i} of steps {steps:?}"
                    );
                    config
                        .wait_for_deployment_status(
                            "test",
                            PipelineStatus::Running,
                            config.start_timeout,
                        )
                        .await;
                }
                OrchestrationTestStep::PausePipeline => {
                    let response = config.post_no_body("/v0/pipelines/test/pause").await;
                    assert_eq!(
                        response.status(),
                        StatusCode::ACCEPTED,
                        "during step {i} of steps {steps:?}"
                    );
                    config
                        .wait_for_deployment_status(
                            "test",
                            PipelineStatus::Paused,
                            config.start_timeout,
                        )
                        .await;
                }
                OrchestrationTestStep::StartConnector(connector) => {
                    assert_eq!(
                        config
                            .post_no_body(format!(
                                "/v0/pipelines/test/tables/numbers/connectors/c{connector}/start"
                            ))
                            .await
                            .status(),
                        StatusCode::OK,
                        "during step {i} of steps {steps:?}"
                    );
                }
                OrchestrationTestStep::PauseConnector(connector) => {
                    assert_eq!(
                        config
                            .post_no_body(format!(
                                "/v0/pipelines/test/tables/numbers/connectors/c{connector}/pause"
                            ))
                            .await
                            .status(),
                        StatusCode::OK,
                        "during step {i} of steps {steps:?}"
                    );
                }
            }
        }

        // Check expected outcome
        let stats = config.stats_json("test").await;
        let inputs = stats["inputs"].as_array().unwrap();
        let pipeline_paused = stats["global_metrics"]["state"].as_str() == Some("Paused");
        let c1_paused = inputs
            .iter()
            .find(|input| input["endpoint_name"] == "numbers.c1")
            .unwrap()
            .clone()["paused"]
            .as_bool()
            .unwrap();
        let c2_paused = inputs
            .iter()
            .find(|input| input["endpoint_name"] == "numbers.c2")
            .unwrap()
            .clone()["paused"]
            .as_bool()
            .unwrap();
        let actual = (pipeline_paused, c1_paused, c2_paused);
        let expected = (
            expected_pipeline_paused,
            expected_c1_paused,
            expected_c2_paused,
        );
        assert_eq!(
            actual, expected,
            "Got {actual:?} but expected {expected:?} for steps {steps:?}"
        );

        // Shutdown for the next scenario
        assert_eq!(
            config
                .post_no_body("/v0/pipelines/test/shutdown")
                .await
                .status(),
            StatusCode::ACCEPTED
        );
        config
            .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
            .await;
    }
}

/// Checkpoint should return NOT_IMPLEMENTED status if `feldera-enterprise` feature is not set.
#[actix_web::test]
#[serial]
async fn checkpoint() {
    let config = setup().await;
    create_and_deploy_test_pipeline(&config, "").await;
    let mut response = config.post_no_body("/v0/pipelines/test/checkpoint").await;
    let value: Value = response.json().await.unwrap();
    assert_ne!(value["error_code"], json!("InvalidPipelineAction"));

    #[cfg(not(feature = "feldera-enterprise"))]
    {
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(value["error_code"], json!("EnterpriseFeature"));
    }

    #[cfg(feature = "feldera-enterprise")]
    {
        assert_ne!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }
}

/// Incrementing of `refresh_version`.
#[actix_web::test]
#[serial]
async fn refresh_version() {
    let config = setup().await;

    // Initial refresh version should be 1
    let request_body = json!({
        "name": "test",
        "program_code": "",
    });
    let mut response = config.post("/v0/pipelines", &request_body).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["refresh_version"], json!(1));

    // After compilation, refresh version should be 3
    config
        .wait_for_compilation("test", 1, config.compilation_timeout)
        .await;
    let mut response = config.get("/v0/pipelines/test").await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["refresh_version"], json!(3));

    // Starting and shutting down should have no effect on the refresh version
    let response = config.post_no_body("/v0/pipelines/test/pause").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Paused, config.start_timeout)
        .await;
    let response = config.post_no_body("/v0/pipelines/test/shutdown").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Shutdown, config.start_timeout)
        .await;
    let mut response = config.get("/v0/pipelines/test").await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["refresh_version"], json!(3));

    // Edits should have an impact, incrementing refresh version to 4
    let mut response = config
        .patch(
            "/v0/pipelines/test",
            &json!({ "program_code": "CREATE TABLE t1 ( v1 INT );" }),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["refresh_version"], json!(4));

    // After compilation, refresh version should be 6
    config
        .wait_for_compilation("test", 2, config.compilation_timeout)
        .await;
    let mut response = config.get("/v0/pipelines/test").await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["refresh_version"], json!(6));
}

/// Tests that circuit metrics can be retrieved from the pipeline.
#[actix_web::test]
#[serial]
async fn pipeline_metrics() {
    let config = setup().await;
    create_and_deploy_test_pipeline(&config, "").await;

    // Retrieve metrics in default format
    let mut response = config.get("/v0/pipelines/test/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.body().await.unwrap();
    let metrics_default = std::str::from_utf8(&body).unwrap();

    // Retrieve metrics in Prometheus format
    let mut response = config
        .get("/v0/pipelines/test/metrics?format=prometheus")
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.body().await.unwrap();
    let metrics_prometheus = std::str::from_utf8(&body).unwrap();

    // Retrieve metrics in JSON format
    let mut response = config.get("/v0/pipelines/test/metrics?format=json").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.body().await.unwrap();
    let metrics_json = std::str::from_utf8(&body).unwrap();
    let _metrics_json_value: Value = serde_json::from_str(metrics_json).unwrap();

    // Unknown format
    let response = config
        .get("/v0/pipelines/test/metrics?format=does-not-exist")
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Minimally check formats
    assert!(metrics_default.contains("# TYPE total_processed_records gauge"));
    assert!(metrics_prometheus.contains("# TYPE total_processed_records gauge"));
    assert!(metrics_json.contains("\"key\":\"total_input_records\""));
}

/// Tests retrieving pipeline statistics via `/stats`.
#[actix_web::test]
#[serial]
async fn pipeline_stats() {
    let config = setup().await;

    // Basic test pipeline
    create_and_deploy_test_pipeline(
        &config,
        r#"
        CREATE TABLE t1(c1 INT) WITH (
            'materialized' = 'true',
            'connectors' = '[{
                "transport": {
                   "name": "datagen",
                   "config": {
                       "plan": [{
                           "limit": 5,
                           "rate": 1000
                        }]
                    }
                }
            }]'
        );
        CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;
        "#,
    )
    .await;

    // Start the pipeline
    let response = config.post_no_body("/v0/pipelines/test/start").await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    config
        .wait_for_deployment_status("test", PipelineStatus::Running, config.start_timeout)
        .await;

    // Create output connector
    let response = config.post_no_body("/v0/pipelines/test/egress/v1").await;
    assert_eq!(response.status(), StatusCode::OK);

    // Give it some seconds to process
    sleep(Duration::from_secs(4)).await;

    // Check the output of `/stats`
    let mut response = config.get("/v0/pipelines/test/stats").await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();

    // Keys of main object
    let mut keys: Vec<String> = value.as_object().unwrap().keys().cloned().collect();
    keys.sort();
    assert_eq!(
        keys,
        vec!["global_metrics", "inputs", "outputs", "suspend_error"]
    );

    // Check global_metrics
    assert_eq!(value["global_metrics"]["state"], json!("Running"));
    assert_eq!(value["global_metrics"]["buffered_input_records"], json!(0));
    assert_eq!(value["global_metrics"]["pipeline_complete"], json!(true));
    assert_eq!(value["global_metrics"]["total_input_records"], json!(5));
    assert_eq!(value["global_metrics"]["total_processed_records"], json!(5));

    // Check inputs
    let inputs = value["inputs"].as_array().unwrap();
    assert_eq!(inputs.len(), 1);
    assert_eq!(
        value["inputs"][0]["config"],
        json!({
            "stream": "t1"
        })
    );
    assert_eq!(
        value["inputs"][0]["metrics"],
        json!({
            "buffered_records": 0,
            "end_of_input": true,
            "num_parse_errors": 0,
            "num_transport_errors": 0,
            "total_bytes": 0,
            "total_records": 5
        })
    );

    // Check outputs
    let outputs = value["outputs"].as_array().unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        value["outputs"][0]["config"],
        json!({
            "stream": "v1"
        })
    );
    assert_eq!(
        value["outputs"][0]["metrics"],
        json!({
            "buffered_batches": 0,
            "buffered_records": 0,
            "num_encode_errors": 0,
            "num_transport_errors": 0,
            "queued_batches": 0,
            "queued_records": 0,
            "total_processed_input_records": 5,
            "transmitted_bytes": 0,
            "transmitted_records": 0
        })
    );
}
