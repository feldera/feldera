//! Pipeline manager integration tests that test the end-to-end
//! compiler->run->feed iinputs->receive outputs workflow.
//!
//! There are two ways to run these tests:
//!
//! 1. Self-contained mode, spinning up a pipeline manager instance on each run.
//! This is good for running tests from a clean state, but is very slow, as it
//! involves pre-compiling all dependencies from scratch:
//!
//! ```text
//! cargo test --features integration-test --features=pg-embed integration_test::
//! ```
//!
//! 2. Using an external pipeline manager instance.
//!
//! Start the pipeline manager by running `scripts/start_manager.sh` or using
//! the following command line:
//!
//! ```text
//! RUST_LOG=debug,tokio_postgres=info cargo run --bin=pipeline-manager --features pg-embed -- --db-connection-string=postgres-embed \
//!    --bind-address=0.0.0.0 \
//!    --compiler-working-directory=$HOME/.dbsp \
//!    --runner-working-directory=$HOME/.dbsp \
//!    --sql-compiler-home=sql-to-dbsp-compiler \
//!    --dbsp-override-path=.
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
//! TEST_DBSP_URL=http://localhost:8080 cargo test integration_test:: --package=pipeline-manager --features integration-test  -- --nocapture
//! ```
use std::collections::BTreeMap;
use std::{
    process::Command,
    time::{self, Duration, Instant},
};

use actix_http::{encoding::Decoder, Payload, StatusCode};
use awc::error::SendRequestError;
use awc::{http, ClientRequest, ClientResponse};
use aws_sdk_cognitoidentityprovider::config::Region;
use colored::Colorize;
use futures_util::StreamExt;
use pipeline_types::transport::http::Chunk;
use serde_json::{json, Value};
use serial_test::serial;
use tempfile::TempDir;
use tokio::{
    sync::OnceCell,
    time::{sleep, timeout},
};

use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::program::CompilationProfile;
use crate::{
    compiler::Compiler,
    config::{ApiServerConfig, CompilerConfig, DatabaseConfig, LocalRunnerConfig},
    db::types::pipeline::PipelineStatus,
};
use anyhow::{bail, Result as AnyResult};
use pipeline_types::service::KafkaService;
use std::sync::Arc;
use tokio::sync::Mutex;

const TEST_DBSP_URL_VAR: &str = "TEST_DBSP_URL";
const TEST_DBSP_DEFAULT_PORT: u16 = 8089;
const MANAGER_INITIALIZATION_TIMEOUT: Duration = Duration::from_secs(100);

// Used if we are testing against a local DBSP instance
// whose lifecycle is managed by this test file
static LOCAL_DBSP_INSTANCE: OnceCell<TempDir> = OnceCell::const_new();

async fn initialize_local_pipeline_manager_instance() -> TempDir {
    crate::logging::init_logging("[manager]".cyan());
    println!("Performing one time initialization for integration tests.");
    println!("Initializing a postgres container");
    let _output = Command::new("docker")
        .args([
            "compose",
            "-f",
            "../../deploy/docker-compose.yml",
            "-f",
            "../../deploy/docker-compose-dev.yml",
            "up",
            "--renew-anon-volumes",
            "--force-recreate",
            "-d",
            "db", // run only the DB service
        ])
        .output()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5000)).await;
    let tmp_dir = TempDir::new().unwrap();
    let workdir = tmp_dir.path().to_str().unwrap();
    let database_config = DatabaseConfig {
        db_connection_string: "postgresql://postgres:postgres@localhost:6666".to_owned(),
    };
    let api_config = ApiServerConfig {
        port: TEST_DBSP_DEFAULT_PORT,
        bind_address: "0.0.0.0".to_owned(),
        api_server_working_directory: workdir.to_owned(),
        auth_provider: crate::config::AuthProviderType::None,
        dev_mode: false,
        dump_openapi: false,
        config_file: None,
        allowed_origins: None,
        demos_dir: None,
    }
    .canonicalize()
    .unwrap();
    let compiler_config = CompilerConfig {
        compiler_working_directory: workdir.to_owned(),
        sql_compiler_home: "../../sql-to-dbsp-compiler".to_owned(),
        dbsp_override_path: "../../".to_owned(),
        compilation_profile: CompilationProfile::Unoptimized,
        precompile: true,
        binary_ref_host: "127.0.0.1".to_string(),
        binary_ref_port: 8085,
    }
    .canonicalize()
    .unwrap();
    let local_runner_config = LocalRunnerConfig {
        runner_working_directory: workdir.to_owned(),
        pipeline_host: "127.0.0.1".to_owned(),
    }
    .canonicalize()
    .unwrap();
    println!("Using ApiServerConfig: {:?}", api_config);
    println!("Issuing Compiler::precompile_dependencies(). This will be slow.");
    Compiler::precompile_dependencies(&compiler_config)
        .await
        .unwrap();
    println!("Completed Compiler::precompile_dependencies().");

    // We cannot reuse the tokio runtime instance created by the test (e.g., the one
    // implicitly created via [actix_web::test]) to create the compiler, local
    // runner and api futures below. The reason is that when that first test
    // completes, these futures will get cancelled.
    //
    // To avoid that problem, and for general integration test hygiene, we force
    // another tokio runtime to be created here to run the server processes. We
    // obviously can't create one runtime within another, so the easiest way to
    // work around that is to do so within an std::thread::spawn().
    std::thread::spawn(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let db = StoragePostgres::connect(
                    &database_config,
                    #[cfg(feature = "pg-embed")]
                    Some(&api_config),
                )
                .await
                .unwrap();
                db.run_migrations().await.unwrap();
                let db = Arc::new(Mutex::new(db));
                let db_clone = db.clone();
                let _compiler = tokio::spawn(async move {
                    crate::compiler::Compiler::run(&compiler_config.clone(), db_clone)
                        .await
                        .unwrap();
                });
                let db_clone = db.clone();
                let _local_runner = tokio::spawn(async move {
                    crate::local_runner::run(db_clone, &local_runner_config.clone()).await;
                });
                // The api-server blocks forever
                crate::api::run(db, api_config).await.unwrap();
            })
    });
    tokio::time::sleep(Duration::from_millis(3000)).await;
    tmp_dir
}

struct TestConfig {
    dbsp_url: String,
    client: awc::Client,
    bearer_token: Option<String>,
    start_timeout: Duration,
    shutdown_timeout: Duration,
    failed_timeout: Duration,
}

impl TestConfig {
    fn endpoint_url<S: AsRef<str>>(&self, endpoint: S) -> String {
        format!("{}{}", self.dbsp_url, endpoint.as_ref())
    }

    async fn cleanup(&self) {
        let config = self;

        // Cleanup pipelines..
        let start = Instant::now();
        let mut req;

        loop {
            match config.try_get("/v0/pipelines").await {
                Ok(r) => {
                    req = r;
                    break;
                }
                Err(e) => {
                    if start.elapsed() > MANAGER_INITIALIZATION_TIMEOUT {
                        panic!("Timeout waiting for the pipeline manager");
                    }
                    println!("Couldn't reach pipeline manager, retrying: {e}");
                    sleep(Duration::from_millis(1000)).await;
                }
            }
        }
        let pipelines: Value = req.json().await.unwrap();
        // First, shutdown the pipelines
        for pipeline in pipelines.as_array().unwrap() {
            let name = pipeline["descriptor"]["name"].as_str().unwrap();
            println!("shutting down pipeline {name}");
            let req = config
                .post_no_body(format!("/v0/pipelines/{name}/shutdown"))
                .await;
            assert_eq!(StatusCode::ACCEPTED, req.status(), "Response {:?}", req)
        }
        // Once we can confirm pipelines are shutdown, delete them
        for pipeline in pipelines.as_array().unwrap() {
            let name = pipeline["descriptor"]["name"].as_str().unwrap();
            self.wait_for_pipeline_status(
                name,
                PipelineStatus::Shutdown,
                time::Duration::from_secs(30),
            )
            .await;
            let req = config.delete(format!("/v0/pipelines/{name}")).await;
            assert_eq!(StatusCode::OK, req.status(), "Response {:?}", req)
        }
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

    /// Performs GET request, asserts the status code is OK, and returns result.
    async fn get_ok<S: AsRef<str>>(&self, endpoint: S) -> ClientResponse<Decoder<Payload>> {
        let result = self.get(endpoint).await;
        assert_eq!(result.status(), StatusCode::OK);
        result
    }

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

    async fn quantiles_csv(&self, name: &str, table: &str) -> String {
        // this is a workaround for the fact that the server may not have processed the
        // request before the response is returned and may return
        // wrong/inconsistent results for future queries e.g., post data then do
        // quantiles
        tokio::time::sleep(Duration::from_millis(800)).await;

        let mut resp = self
            .post_no_body(format!(
                "/v0/pipelines/{name}/egress/{table}?query=quantiles&mode=snapshot&format=csv"
            ))
            .await;
        assert!(resp.status().is_success());
        let resp: Value = resp.json().await.unwrap();
        resp.get("text_data").unwrap().as_str().unwrap().to_string()
    }

    async fn quantiles_json(&self, name: &str, table: &str) -> String {
        // this is a workaround for the fact that the server may not have processed the
        // request before the response is returned and may return
        // wrong/inconsistent results for future queries e.g., post data then do
        // quantiles
        tokio::time::sleep(Duration::from_millis(800)).await;

        let mut resp = self
            .post_no_body(format!(
                "/v0/pipelines/{name}/egress/{table}?query=quantiles&mode=snapshot&format=json"
            ))
            .await;
        assert!(resp.status().is_success());
        let resp: Value = resp.json().await.unwrap();
        resp.get("json_data").unwrap().to_string()
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

    async fn neighborhood_json(
        &self,
        name: &str,
        table: &str,
        anchor: Option<Value>,
        before: u64,
        after: u64,
    ) -> String {
        let mut resp = self
            .post(
                format!(
                "/v0/pipelines/{name}/egress/{table}?query=neighborhood&mode=snapshot&format=json"
            ),
                &json!({"before": before, "after": after, "anchor": anchor}),
            )
            .await;
        assert!(resp.status().is_success());
        let resp: Value = resp.json().await.unwrap();
        resp.get("json_data").unwrap().to_string()
    }

    async fn delete<S: AsRef<str>>(&self, endpoint: S) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.delete(self.endpoint_url(endpoint)))
            .send()
            .await
            .unwrap()
    }

    // TODO: refactor function
    // async fn compile(&self, program_name: &str, version: i64) {
    //     let compilation_request = json!({ "version": version });
    //
    //     let resp = self
    //         .post(
    //             format!("/v0/programs/{program_name}/compile"),
    //             &compilation_request,
    //         )
    //         .await;
    //     assert_eq!(StatusCode::ACCEPTED, resp.status());
    //
    //     let start = Instant::now();
    //     println!("Waiting for compilation");
    //     let mut last_wait_println = Instant::now();
    //     loop {
    //         std::thread::sleep(time::Duration::from_secs(1));
    //         if start.elapsed().as_secs() > 480 {
    //             panic!("Compilation timeout");
    //         }
    //         let mut resp = self.get(format!("/v0/programs/{program_name}")).await;
    //         let val: Value = resp.json().await.unwrap();
    //
    //         let status = val["status"].clone();
    //         if status != json!("CompilingSql")
    //             && status != json!("CompilingRust")
    //             && status != json!("Pending")
    //         {
    //             if status == json!("Success") {
    //                 break;
    //             } else {
    //                 // This makes long multiline error messages readable.
    //                 let status = status.to_string().replace("\\n", "\n");
    //                 panic!("Compilation failed with status {}", status);
    //             }
    //         }
    //         if last_wait_println.elapsed().as_secs() >= 60 {
    //             println!(
    //                 "Waiting for compilation since {} seconds, status: {}",
    //                 start.elapsed().as_secs(),
    //                 status
    //             );
    //             last_wait_println = Instant::now();
    //         }
    //     }
    // }

    // TODO: refactor function
    // /// Wait for the pipeline to reach the specified status.
    // /// Panic after `timeout`.
    // async fn wait_for_pipeline_status(
    //     &self,
    //     name: &str,
    //     status: PipelineStatus,
    //     timeout: Duration,
    // ) -> Pipeline {
    //     let start = Instant::now();
    //     println!("Waiting for pipeline status {status:?}...");
    //     let mut last_wait_println = Instant::now();
    //     loop {
    //         let mut response = self.get(format!("/v0/pipelines/{name}")).await;
    //
    //         let pipeline = response.json::<Pipeline>().await.unwrap();
    //
    //         if last_wait_println.elapsed().as_secs() >= 60 {
    //             println!("Pipeline:\n{pipeline:#?}");
    //             println!(
    //                 "Waiting for pipeline status {status:?} since {} seconds",
    //                 start.elapsed().as_secs()
    //             );
    //             last_wait_println = Instant::now();
    //         }
    //
    //         if pipeline.state.current_status == status {
    //             return pipeline;
    //         }
    //         if start.elapsed() >= timeout {
    //             println!("Pipeline:\n{pipeline:#?}");
    //             panic!("Timeout waiting for pipeline status {status:?}");
    //         }
    //         sleep(Duration::from_millis(300)).await;
    //     }
    // }
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
            Some(
                res.authentication_result()
                    .unwrap()
                    .access_token()
                    .unwrap()
                    .to_string(),
            )
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
    let dbsp_url = match std::env::var(TEST_DBSP_URL_VAR) {
        Ok(val) => {
            println!("Running integration test against TEST_DBSP_URL: {}", val);
            val
        }
        Err(e) => {
            println!(
                "TEST_DBSP_URL is unset (reason: {}). Running integration test against: localhost:{}", e, TEST_DBSP_DEFAULT_PORT
            );
            LOCAL_DBSP_INSTANCE
                .get_or_init(initialize_local_pipeline_manager_instance)
                .await;
            format!("http://localhost:{}", TEST_DBSP_DEFAULT_PORT).to_owned()
        }
    };
    let client = awc::ClientBuilder::new()
        .timeout(Duration::from_secs(10))
        .finish();
    let bearer_token = bearer_token().await;
    let start_timeout = Duration::from_secs(
        std::env::var("TEST_START_TIMEOUT")
            .unwrap_or("60".to_string())
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
        start_timeout,
        shutdown_timeout,
        failed_timeout,
    };
    config.cleanup().await;
    config
}

// TODO: refactor test
// async fn deploy_pipeline_without_connectors(config: &TestConfig, sql: &str) -> String {
//     let program_request = json!({
//         "name":  "test",
//         "description": "desc",
//         "code": sql,
//     });
//     let mut req = config.post("/v0/programs", &program_request).await;
//     assert_eq!(StatusCode::CREATED, req.status());
//     let resp: Value = req.json().await.unwrap();
//     let version = resp["version"].as_i64().unwrap();
//     config.compile("test", version).await;
//
//     let pipeline_request = json!({
//         "name":  "test",
//         "description": "desc",
//         "program_name": Some("test".to_string()),
//         "config": {
//             "storage": true,
//         },
//         "connectors": null
//     });
//     let req = config.post("/v0/pipelines", &pipeline_request).await;
//     assert_eq!(StatusCode::OK, req.status());
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/pause"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Paused,
//             Duration::from_millis(100_000),
//         )
//         .await;
//     "test".to_string()
// }

#[actix_web::test]
#[serial]
async fn lists_at_initialization_are_empty() {
    let config = setup().await;
    for endpoint in &["/v0/pipelines"] {
        let mut req = config.get(endpoint).await;
        let ret: Value = req.json().await.unwrap();
        assert_eq!(ret, json!([]));
    }
}

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// async fn program_create_compile_delete() {
//     let config = setup().await;
//     let program_request = json!({
//         "name":  "test",
//         "description": "desc",
//         "code": "create table t1(c1 integer);"
//     });
//     let mut req = config.post("/v0/programs", &program_request).await;
//     assert_eq!(StatusCode::CREATED, req.status());
//     let resp: Value = req.json().await.unwrap();
//     let version = resp["version"].as_i64().unwrap();
//     config.compile("test", version).await;
//     let resp = config.delete(format!("/v0/programs/test")).await;
//     assert_eq!(StatusCode::OK, resp.status());
//     let resp = config.get(format!("/v0/programs/test").as_str()).await;
//     assert_eq!(StatusCode::NOT_FOUND, resp.status());
// }

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// async fn program_create_twice() {
//     let config = setup().await;
//     let program_request = json!({
//         "name":  "test",
//         "description": "desc",
//         "code": "create table t1(c1 integer);"
//     });
//     let req = config.post("/v0/programs", &program_request).await;
//     assert_eq!(StatusCode::CREATED, req.status());
//
//     // Same name, different desc/program.
//     let program_request = json!({
//         "name":  "test",
//         "description": "desc1",
//         "code": "create table t2(c2 integer);"
//     });
//     let req = config.post("/v0/programs", &program_request).await;
//     assert_eq!(StatusCode::CONFLICT, req.status());
// }

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// async fn deploy_pipeline() {
//     let config = setup().await;
//     let _ = deploy_pipeline_without_connectors(
//         &config,
//         "create table t1(c1 integer) with ('materialized' = 'true'); create view v1 as select * from t1;",
//     )
//     .await;
//
//     // Pause a pipeline before it is started
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/pause"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Running,
//             Duration::from_millis(1_000),
//         )
//         .await;
//     // Push some data.
//     let req = config
//         .post_csv(
//             format!("/v0/pipelines/test/ingress/T1"),
//             "1\n2\n3\n".to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     // Push more data without Windows-style newlines.
//     let req = config
//         .post_csv(
//             format!("/v0/pipelines/test/ingress/T1"),
//             "4\r\n5\r\n6".to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     // Pause a pipeline after it is started
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/pause"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Paused, Duration::from_millis(1_000))
//         .await;
//
//     // Querying quantiles should work in paused state.
//     let quantiles = config.quantiles_csv("test", "T1").await;
//     assert_eq!(&quantiles, "1,1\n2,1\n3,1\n4,1\n5,1\n6,1\n");
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Running,
//             Duration::from_millis(1_000),
//         )
//         .await;
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
// }

// TODO: refactor test
// // Correctly report pipeline panic to the user.
// #[actix_web::test]
// #[serial]
// async fn pipeline_panic() {
//     let config = setup().await;
//
//     // This SQL program is known to panic.
//     let _ = deploy_pipeline_without_connectors(
//         &config,
//         "create table t1(c1 integer); create view v1 as select element(array [2, 3]) from t1;",
//     )
//     .await;
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Running,
//             Duration::from_millis(1_000),
//         )
//         .await;
//
//     // Push some data.  This should cause a panic.
//     let req = config
//         .post_csv(
//             format!("/v0/pipelines/test/ingress/T1"),
//             "1\n2\n3\n".to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     // The manager should discover the error next time it polls the pipeline.
//     let pipeline = config
//         .wait_for_pipeline_status("test", PipelineStatus::Failed, config.failed_timeout)
//         .await;
//
//     assert_eq!(
//         pipeline.state.error.as_ref().unwrap().error_code,
//         "RuntimeError.WorkerPanic"
//     );
//     println!("status: {pipeline:?}");
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
// }

// TODO: refactor test
// // Program deletes must not cause pipeline deletes.
// #[actix_web::test]
// #[serial]
// async fn program_delete_with_pipeline() {
//     let config = setup().await;
//     let program_request = json!({
//         "name":  "test",
//         "description": "desc",
//         "code": "create table t1(c1 integer); create view v1 as select * from t1;"
//     });
//     let req = config.post("/v0/programs", &program_request).await;
//     assert_eq!(StatusCode::CREATED, req.status());
//
//     let pipeline_request = json!({
//         "name":  "test",
//         "description": "desc",
//         "program_name": Some("test".to_string()),
//         "config": {},
//         "connectors": null
//     });
//     let req = config.post("/v0/pipelines", &pipeline_request).await;
//     assert_eq!(StatusCode::OK, req.status());
//     let req = config.get(format!("/v0/pipelines/test")).await;
//     assert_eq!(StatusCode::OK, req.status());
//
//     // Now delete the program and check that the pipeline still exists
//     let req = config.delete(format!("/v0/programs/test")).await;
//     assert_eq!(StatusCode::BAD_REQUEST, req.status());
//
//     let req = config.get(format!("/v0/programs/test")).await;
//     assert_eq!(StatusCode::OK, req.status());
//
//     let req = config.get(format!("/v0/pipelines/test")).await;
//     assert_eq!(StatusCode::OK, req.status());
// }

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// async fn json_ingress() {
//     let config = setup().await;
//     let id = deploy_pipeline_without_connectors(
//         &config,
//         "create table t1(c1 integer, c2 bool, c3 varchar) with ('materialized' = 'true'); create materialized view v1 as select * from t1;",
//     )
//     .await;
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Running,
//             Duration::from_millis(1_000),
//         )
//         .await;
//
//     // Push some data using default json config.
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/T1?format=json&update_format=raw",),
//             r#"{"c1": 10, "c2": true}
//             {"c1": 20, "c3": "foo"}"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     let quantiles = config.quantiles_json("test", "T1").await;
//     assert_eq!(quantiles, "[{\"insert\":{\"c1\":10,\"c2\":true,\"c3\":null}},{\"insert\":{\"c1\":20,\"c2\":null,\"c3\":\"foo\"}}]");
//
//     let hood = config
//         .neighborhood_json(
//             &id,
//             "T1",
//             Some(json!({"c1":10,"c2":true,"c3":null})),
//             10,
//             10,
//         )
//         .await;
//     assert_eq!(hood, "[{\"insert\":{\"index\":0,\"key\":{\"c1\":10,\"c2\":true,\"c3\":null}}},{\"insert\":{\"index\":1,\"key\":{\"c1\":20,\"c2\":null,\"c3\":\"foo\"}}}]");
//
//     // Push more data using insert/delete format.
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/t1?format=json&update_format=insert_delete",),
//             r#"{"delete": {"c1": 10, "c2": true}}
//             {"insert": {"c1": 30, "c3": "bar"}}"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     let quantiles = config.quantiles_json("test", "t1").await;
//     assert_eq!(quantiles, "[{\"insert\":{\"c1\":20,\"c2\":null,\"c3\":\"foo\"}},{\"insert\":{\"c1\":30,\"c2\":null,\"c3\":\"bar\"}}]");
//
//     // Format data as json array.
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",),
//             r#"{"insert": [40, true, "buzz"]}"#.to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     // Use array of updates instead of newline-delimited JSON
//     let req = config
//         .post_json(
//             format!(
//                 "/v0/pipelines/test/ingress/t1?format=json&update_format=insert_delete&array=true",
//             ),
//             r#"[{"delete": [40, true, "buzz"]}, {"insert": [50, true, ""]}]"#.to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     let quantiles = config.quantiles_json("test", "T1").await;
//     assert_eq!(quantiles, "[{\"insert\":{\"c1\":20,\"c2\":null,\"c3\":\"foo\"}},{\"insert\":{\"c1\":30,\"c2\":null,\"c3\":\"bar\"}},{\"insert\":{\"c1\":50,\"c2\":true,\"c3\":\"\"}}]");
//
//     // Trigger parse errors.
//     let mut req = config
//         .post_json(
//             format!(
//                 "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete&array=true",
//             ),
//             r#"[{"insert": [35, true, ""]}, {"delete": [40, "foo", "buzz"]}, {"insert": [true, true, ""]}]"#.to_string(),
//         )
//         .await;
//     assert_eq!(req.status(), StatusCode::BAD_REQUEST);
//     let body = req.body().await.unwrap();
//     let error = std::str::from_utf8(&body).unwrap();
//     assert_eq!(error, "{\"message\":\"Errors parsing input data (2 errors):\\n    Parse error (event #2): failed to deserialize JSON record: error parsing field 'c2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\\nInvalid fragment: '[40, \\\"foo\\\", \\\"buzz\\\"]'\\n    Parse error (event #3): failed to deserialize JSON record: error parsing field 'c1': invalid type: boolean `true`, expected i32 at line 1 column 5\\nInvalid fragment: '[true, true, \\\"\\\"]'\",\"error_code\":\"ParseErrors\",\"details\":{\"errors\":[{\"description\":\"failed to deserialize JSON record: error parsing field 'c2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\",\"event_number\":2,\"field\":\"c2\",\"invalid_bytes\":null,\"invalid_text\":\"[40, \\\"foo\\\", \\\"buzz\\\"]\",\"suggestion\":null},{\"description\":\"failed to deserialize JSON record: error parsing field 'c1': invalid type: boolean `true`, expected i32 at line 1 column 5\",\"event_number\":3,\"field\":\"c1\",\"invalid_bytes\":null,\"invalid_text\":\"[true, true, \\\"\\\"]\",\"suggestion\":null}],\"num_errors\":2}}");
//
//     // Even records that are parsed successfully don't get ingested when
//     // using array format.
//     let quantiles = config.quantiles_json("test", "T1").await;
//     assert_eq!(quantiles, "[{\"insert\":{\"c1\":20,\"c2\":null,\"c3\":\"foo\"}},{\"insert\":{\"c1\":30,\"c2\":null,\"c3\":\"bar\"}},{\"insert\":{\"c1\":50,\"c2\":true,\"c3\":\"\"}}]");
//
//     let mut req = config
//         .post_json(
//             format!(
//                 "/v0/pipelines/test/ingress/t1?format=json&update_format=insert_delete",
//             ),
//             r#"{"insert": [25, true, ""]}{"delete": [40, "foo", "buzz"]}{"insert": [true, true, ""]}"#.to_string(),
//         )
//         .await;
//     assert_eq!(req.status(), StatusCode::BAD_REQUEST);
//     let body = req.body().await.unwrap();
//     let error = std::str::from_utf8(&body).unwrap();
//     assert_eq!(error, "{\"message\":\"Errors parsing input data (2 errors):\\n    Parse error (event #2): failed to deserialize JSON record: error parsing field 'c2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\\nInvalid fragment: '[40, \\\"foo\\\", \\\"buzz\\\"]'\\n    Parse error (event #3): failed to deserialize JSON record: error parsing field 'c1': invalid type: boolean `true`, expected i32 at line 1 column 5\\nInvalid fragment: '[true, true, \\\"\\\"]'\",\"error_code\":\"ParseErrors\",\"details\":{\"errors\":[{\"description\":\"failed to deserialize JSON record: error parsing field 'c2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\",\"event_number\":2,\"field\":\"c2\",\"invalid_bytes\":null,\"invalid_text\":\"[40, \\\"foo\\\", \\\"buzz\\\"]\",\"suggestion\":null},{\"description\":\"failed to deserialize JSON record: error parsing field 'c1': invalid type: boolean `true`, expected i32 at line 1 column 5\",\"event_number\":3,\"field\":\"c1\",\"invalid_bytes\":null,\"invalid_text\":\"[true, true, \\\"\\\"]\",\"suggestion\":null}],\"num_errors\":2}}");
//
//     // Even records that are parsed successfully don't get ingested when
//     // using array format.
//     let quantiles = config.quantiles_csv(&id, "T1").await;
//     assert_eq!(quantiles, "20,,foo,1\n25,true,,1\n30,,bar,1\n50,true,,1\n");
//
//     // Debezium CDC format
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/T1?format=json&update_format=debezium",),
//             r#"{"payload": {"op": "u", "before": [50, true, ""], "after": [60, true, "hello"]}}"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     let quantiles = config.quantiles_csv(&id, "t1").await;
//     assert_eq!(
//         quantiles,
//         "20,,foo,1\n25,true,,1\n30,,bar,1\n60,true,hello,1\n"
//     );
//
//     // Push some CSV data (the second record is invalid, but the other two should
//     // get ingested).
//     let mut req = config
//         .post_csv(
//             format!("/v0/pipelines/test/ingress/t1?format=csv"),
//             r#"15,true,foo
// not_a_number,true,ΑαΒβΓγΔδ
// 16,false,unicode🚲"#
//                 .to_string(),
//         )
//         .await;
//     assert_eq!(req.status(), StatusCode::BAD_REQUEST);
//     let body = req.body().await.unwrap();
//     let error = std::str::from_utf8(&body).unwrap();
//     assert_eq!(error, "{\"message\":\"Errors parsing input data (1 errors):\\n    Parse error (event #2): failed to deserialize CSV record: error parsing field 'c1': field 0: invalid digit found in string\\nInvalid fragment: 'not_a_number,true,ΑαΒβΓγΔδ\\n'\",\"error_code\":\"ParseErrors\",\"details\":{\"errors\":[{\"description\":\"failed to deserialize CSV record: error parsing field 'c1': field 0: invalid digit found in string\",\"event_number\":2,\"field\":\"c1\",\"invalid_bytes\":null,\"invalid_text\":\"not_a_number,true,ΑαΒβΓγΔδ\\n\",\"suggestion\":null}],\"num_errors\":1}}");
//
//     let quantiles = config.quantiles_json("test", "t1").await;
//     assert_eq!(
//         quantiles,
//         "[{\"insert\":{\"c1\":15,\"c2\":true,\"c3\":\"foo\"}},{\"insert\":{\"c1\":16,\"c2\":false,\"c3\":\"unicode🚲\"}},{\"insert\":{\"c1\":20,\"c2\":null,\"c3\":\"foo\"}},{\"insert\":{\"c1\":25,\"c2\":true,\"c3\":\"\"}},{\"insert\":{\"c1\":30,\"c2\":null,\"c3\":\"bar\"}},{\"insert\":{\"c1\":60,\"c2\":true,\"c3\":\"hello\"}}]"
//     );
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
// }

// TODO: refactor test
// // Table with column of type MAP.
// #[actix_web::test]
// #[serial]
// async fn map_column() {
//     let config = setup().await;
//     let id = deploy_pipeline_without_connectors(
//         &config,
//         "create table t1(c1 integer, c2 bool, c3 MAP<varchar, varchar>) with ('materialized' = 'true'); create view v1 as select * from t1;",
//     )
//     .await;
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Running,
//             Duration::from_millis(1_000),
//         )
//         .await;
//
//     // Push some data using default json config.
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/T1?format=json&update_format=raw",),
//             r#"{"c1": 10, "c2": true, "c3": {"foo": "1", "bar": "2"}}
//             {"c1": 20}"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     let quantiles = config.quantiles_json("test", "T1").await;
//     assert_eq!(quantiles, "[{\"insert\":{\"c1\":10,\"c2\":true,\"c3\":{\"bar\":\"2\",\"foo\":\"1\"}}},{\"insert\":{\"c1\":20,\"c2\":null,\"c3\":null}}]");
//
//     let hood = config
//         .neighborhood_json(
//             &id,
//             "T1",
//             Some(json!({"c1":10,"c2":true,"c3": {"foo": "1", "bar": "2"}})),
//             10,
//             10,
//         )
//         .await;
//     assert_eq!(hood, "[{\"insert\":{\"index\":0,\"key\":{\"c1\":10,\"c2\":true,\"c3\":{\"bar\":\"2\",\"foo\":\"1\"}}}},{\"insert\":{\"index\":1,\"key\":{\"c1\":20,\"c2\":null,\"c3\":null}}}]");
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
// }

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// async fn parse_datetime() {
//     let config = setup().await;
//     let _ = deploy_pipeline_without_connectors(
//         &config,
//         "create table t1(t TIME, ts TIMESTAMP, d DATE) with ('materialized' = 'true');",
//     )
//     .await;
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Running,
//             Duration::from_millis(1_000),
//         )
//         .await;
//
//     // The parser should trim leading and trailing white space when parsing
//     // dates/times.
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/t1?format=json&update_format=raw",),
//             r#"{"t":"13:22:00","ts": "2021-05-20 12:12:33","d": "2021-05-20"}
//             {"t":" 11:12:33.483221092 ","ts": " 2024-02-25 12:12:33 ","d": " 2024-02-25 "}"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     let quantiles = config.quantiles_json("test", "T1").await;
//     assert_eq!(quantiles.parse::<Value>().unwrap(),
//                "[{\"insert\":{\"d\":\"2024-02-25\",\"t\":\"11:12:33.483221092\",\"ts\":\"2024-02-25 12:12:33\"}},{\"insert\":{\"d\":\"2021-05-20\",\"t\":\"13:22:00\",\"ts\":\"2021-05-20 12:12:33\"}}]".parse::<Value>().unwrap());
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
// }

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// async fn quoted_columns() {
//     let config = setup().await;
//     let _ = deploy_pipeline_without_connectors(
//         &config,
//         r#"create table t1("c1" integer not null, "C2" bool not null, "😁❤" varchar not null, "αβγ" boolean not null, ΔΘ boolean not null) with ('materialized' = 'true')"#,
//     )
//     .await;
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Running,
//             Duration::from_millis(1_000),
//         )
//         .await;
//
//     // Push some data using default json config.
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/T1?format=json&update_format=raw",),
//             r#"{"c1": 10, "C2": true, "😁❤": "foo", "αβγ": true, "δθ": false}"#.to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     let quantiles = config.quantiles_json("test", "T1").await;
//     assert_eq!(
//         quantiles.parse::<Value>().unwrap(),
//         "[{\"insert\":{\"C2\":true,\"c1\":10,\"αβγ\":true,\"δθ\":false,\"😁❤\":\"foo\"}}]"
//             .parse::<Value>()
//             .unwrap()
//     );
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
// }

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// async fn primary_keys() {
//     let config = setup().await;
//     let _ = deploy_pipeline_without_connectors(
//         &config,
//         r#"create table t1(id bigint not null, s varchar not null, primary key (id)) with ('materialized' = 'true')"#,
//     )
//     .await;
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Running,
//             Duration::from_millis(1_000),
//         )
//         .await;
//
//     // Push some data using default json config.
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",),
//             r#"{"insert":{"id":1, "s": "1"}}
// {"insert":{"id":2, "s": "2"}}"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     let quantiles = config.quantiles_json("test", "T1").await;
//     assert_eq!(
//         quantiles.parse::<Value>().unwrap(),
//         "[{\"insert\":{\"id\":1,\"s\":\"1\"}},{\"insert\":{\"id\":2,\"s\":\"2\"}}]"
//             .parse::<Value>()
//             .unwrap()
//     );
//
//     let hood = config
//         .neighborhood_json("test", "T1", Some(json!({"id":2,"s":"1"})), 10, 10)
//         .await;
//     assert_eq!(
//         hood.parse::<Value>().unwrap(),
//         "[{\"insert\":{\"index\":-1,\"key\":{\"id\":1,\"s\":\"1\"}}},{\"insert\":{\"index\":0,\"key\":{\"id\":2,\"s\":\"2\"}}}]"
//             .parse::<Value>()
//             .unwrap()
//     );
//
//     // Make some changes.
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",),
//             r#"{"insert":{"id":1, "s": "1-modified"}}
// {"update":{"id":2, "s": "2-modified"}}"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     let quantiles = config.quantiles_json("test", "T1").await;
//     assert_eq!(
//         quantiles.parse::<Value>().unwrap(),
//         "[{\"insert\":{\"id\":1,\"s\":\"1-modified\"}},{\"insert\":{\"id\":2,\"s\":\"2-modified\"}}]"
//             .parse::<Value>()
//             .unwrap()
//     );
//
//     let hood = config.neighborhood_json("test", "T1", None, 10, 10).await;
//     assert_eq!(
//         hood.parse::<Value>().unwrap(),
//         "[{\"insert\":{\"index\":0,\"key\":{\"id\":1,\"s\":\"1-modified\"}}},{\"insert\":{\"index\":1,\"key\":{\"id\":2,\"s\":\"2-modified\"}}}]"
//             .parse::<Value>()
//             .unwrap()
//     );
//
//     // Delete a key
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",),
//             r#"{"delete":{"id":2}}"#.to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     let quantiles = config.quantiles_json("test", "T1").await;
//     assert_eq!(
//         quantiles.parse::<Value>().unwrap(),
//         "[{\"insert\":{\"id\":1,\"s\":\"1-modified\"}}]"
//             .parse::<Value>()
//             .unwrap()
//     );
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
// }

// TODO: refactor test
// /// Test case-sensitive table ingress/egress behavior.
// #[actix_web::test]
// #[serial]
// async fn case_sensitive_tables() {
//     let config = setup().await;
//     // Table "TaBle1" and view "V1" are case-sensitive and can only be accessed
//     // by quoting their name.
//     // Table "v1" is also case-sensitive, but since its name is lowercase, it
//     // can be accessed as both "v1" and "\"v1\""
//     let _ = deploy_pipeline_without_connectors(
//         &config,
//         r#"create table "TaBle1"(id bigint not null);
// create table table1(id bigint);
// create materialized view "V1" as select * from "TaBle1";
// create materialized view "v1" as select * from table1;"#,
//     )
//     .await;
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Running,
//             Duration::from_millis(1_000),
//         )
//         .await;
//
//     let mut response1 = config.delta_stream_request_json("test", "\"V1\"").await;
//     let mut response2 = config.delta_stream_request_json("test", "\"v1\"").await;
//
//     // Push some data using default json config.
//     let req = config
//         .post_json(
//             format!(
//                 "/v0/pipelines/test/ingress/\"TaBle1\"?format=json&update_format=insert_delete",
//             ),
//             r#"{"insert":{"id":1}}"#.to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/table1?format=json&update_format=insert_delete",),
//             r#"{"insert":{"id":2}}"#.to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     config
//         .read_expected_response_json(
//             &mut response1,
//             Duration::from_millis(10_000),
//             &[json!({"insert": {"id":1}})],
//         )
//         .await;
//
//     config
//         .read_expected_response_json(
//             &mut response2,
//             Duration::from_millis(10_000),
//             &[json!({"insert": {"id":2}})],
//         )
//         .await;
//
//     let quantiles = config.quantiles_json("test", "\"V1\"").await;
//     assert_eq!(
//         quantiles.parse::<Value>().unwrap(),
//         "[{\"insert\":{\"id\":1}}]".parse::<Value>().unwrap()
//     );
//
//     let quantiles = config.quantiles_json("test", "\"v1\"").await;
//     assert_eq!(
//         quantiles.parse::<Value>().unwrap(),
//         "[{\"insert\":{\"id\":2}}]".parse::<Value>().unwrap()
//     );
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
// }

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// async fn duplicate_outputs() {
//     let config = setup().await;
//     let _ = deploy_pipeline_without_connectors(
//         &config,
//         r#"create table t1(id bigint not null, s varchar not null); create view v1 as select s from t1;"#,
//     )
//     .await;
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Running,
//             Duration::from_millis(1_000),
//         )
//         .await;
//
//     let mut response = config.delta_stream_request_json("test", "V1").await;
//
//     // Push some data using default json config.
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",),
//             r#"{"insert":{"id":1, "s": "1"}}
// {"insert":{"id":2, "s": "2"}}"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     config
//         .read_expected_response_json(
//             &mut response,
//             Duration::from_millis(10_000),
//             &[json!({"insert": {"s":"1"}}), json!({"insert":{"s":"2"}})],
//         )
//         .await;
//
//     // Push some more data
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete"),
//             r#"{"insert":{"id":3, "s": "3"}}
// {"insert":{"id":4, "s": "4"}}"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     config
//         .read_expected_response_json(
//             &mut response,
//             Duration::from_millis(10_000),
//             &[json!({"insert": {"s":"3"}}), json!({"insert":{"s":"4"}})],
//         )
//         .await;
//
//     // Push more records that will create duplicate outputs.
//     let req = config
//         .post_json(
//             format!("/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",),
//             r#"{"insert":{"id":5, "s": "1"}}
// {"insert":{"id":6, "s": "2"}}"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     config
//         .read_expected_response_json(
//             &mut response,
//             Duration::from_millis(10_000),
//             &[json!({"insert": {"s":"1"}}), json!({"insert":{"s":"2"}})],
//         )
//         .await;
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
// }

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// async fn upsert() {
//     let config = setup().await;
//     let _ = deploy_pipeline_without_connectors(
//         &config,
//         r#"create table t1(
//             id1 bigint not null,
//             id2 bigint,
//             str1 varchar not null,
//             str2 varchar,
//             int1 bigint not null,
//             int2 bigint,
//             primary key(id1, id2));"#,
//     )
//     .await;
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status(
//             "test",
//             PipelineStatus::Running,
//             Duration::from_millis(1_000),
//         )
//         .await;
//
//     let mut response = config.delta_stream_request_json("test", "T1").await;
//
//     // Push some data.
//     //
//     // NOTE: we use `array=true` to push data in this test to make sure that all updates are
//     // delivered atomically and all outputs are produced in a single chunk. It is still
//     // theoretically possible that inputs are split across multiple `step`'s due to the
//     // `ZSetHandle::append` method not being atomic.  This is highly improbable, but if it
//     // happens, increasing buffering delay in DBSP should solve that.
//     let req = config
//         .post_json(
//             format!(
//                 "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete&array=true",
//             ),
//             // Add several identical records with different id's
//             r#"[{"insert":{"id1":1, "str1": "1", "int1": 1}},{"insert":{"id1":2, "str1": "1", "int1": 1}},{"insert":{"id1":3, "str1": "1", "int1": 1}}]"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     config
//         .read_expected_response_json(
//             &mut response,
//             Duration::from_millis(10_000),
//             &[
//                 json!({"insert": {"id1":1,"id2":null,"str1":"1","str2":null,"int1":1,"int2":null}}),
//                 json!({"insert": {"id1":2,"id2":null,"str1":"1","str2":null,"int1":1,"int2":null}}),
//                 json!({"insert": {"id1":3,"id2":null,"str1":"1","str2":null,"int1":1,"int2":null}}),
//             ],
//         )
//         .await;
//
//     let req = config
//         .post_json(
//             format!(
//                 "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete&array=true",
//             ),
//             // 1: Update 'str1'.
//             // 2: Update 'str2'.
//             // 3: Overwrite entire record.
//             r#"[{"update":{"id1":1, "str1": "2"}},{"update":{"id1":2, "str2": "foo"}},{"insert":{"id1":3, "str1": "1", "str2": "2", "int1":3, "int2":33}}]"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     config
//         .read_expected_response_json(
//             &mut response,
//             Duration::from_millis(10_000),
//             &[json!({"delete": {"id1":1,"id2":null,"str1":"1","str2":null,"int1":1,"int2":null}}),
//                 json!({"delete": {"id1":2,"id2":null,"str1":"1","str2":null,"int1":1,"int2":null}}),
//                 json!({"delete": {"id1":3,"id2":null,"str1":"1","str2":null,"int1":1,"int2":null}}),
//                 json!({"insert": {"id1":1,"id2":null,"str1":"2","str2":null,"int1":1,"int2":null}}),
//                 json!({"insert": {"id1":2,"id2":null,"str1":"1","str2":"foo","int1":1,"int2":null}}),
//                 json!({"insert": {"id1":3,"id2":null,"str1":"1","str2":"2","int1":3,"int2":33}})]
//         )
//         .await;
//
//     let req = config
//         .post_json(
//             format!(
//                 "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete&array=true",
//             ),
//             // 1: Update command that doesn't modify any fields - noop.
//             // 2: Clear 'str2' to null.
//             // 3: Delete record.
//             // 4: Delete non-existing key - noop.
//             // 5: Update non-existing key - noop.
//             r#"[{"update":{"id1":1}},{"update":{"id1":2, "str2": null}},{"delete":{"id1":3}},{"delete":{"id1":4}},{"update":{"id1":4, "int1":0, "str1":""}}]"#
//                 .to_string(),
//         )
//         .await;
//     assert!(req.status().is_success());
//
//     config
//         .read_expected_response_json(
//             &mut response,
//             Duration::from_millis(10_000),
//             &[json!({"delete": {"id1":2,"id2":null,"str1":"1","str2":"foo","int1":1,"int2":null}}),
//                 json!({"delete": {"id1":3,"id2":null,"str1":"1","str2":"2","int1":3,"int2":33}}),
//                 json!({"insert": {"id1":2,"id2":null,"str1":"1","str2":null,"int1":1,"int2":null}})]
//         )
//         .await;
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
// }

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// async fn pipeline_restart() {
//     let config = setup().await;
//     let _ = deploy_pipeline_without_connectors(
//         &config,
//         "create table t1(c1 integer); create view v1 as select * from t1;",
//     )
//     .await;
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Running, config.start_timeout)
//         .await;
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
//
//     // Start the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Running, config.start_timeout)
//         .await;
//
//     // Shutdown the pipeline
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/shutdown"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::ACCEPTED);
//     config
//         .wait_for_pipeline_status("test", PipelineStatus::Shutdown, config.shutdown_timeout)
//         .await;
// }

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// async fn pipeline_runtime_configuration() {
//     let config = setup().await;
//     let pipeline_request = json!({
//         "name":  "pipeline_runtime_configuration",
//         "description": "desc",
//         "program_name": null,
//         "config": {
//             "workers": 100,
//             "resources": {
//                 "cpu_cores_min": 5,
//                 "storage_mb_max": 2000,
//                 "storage_class": "normal"
//             }
//         },
//         "connectors": null
//     });
//     // Create the pipeline
//     let resp = config.post("/v0/pipelines", &pipeline_request).await;
//     assert_eq!(resp.status(), StatusCode::OK);
//
//     // Get config
//     let mut resp = config
//         .get(format!(
//             "/v0/pipelines/pipeline_runtime_configuration/config"
//         ))
//         .await;
//     assert_eq!(resp.status(), StatusCode::OK);
//     let resp: Value = resp.json().await.unwrap();
//     let workers = resp["workers"].as_i64().unwrap();
//     let resources = &resp["resources"];
//     assert_eq!(100, workers);
//     assert_eq!(
//         json!({ "cpu_cores_min": 5, "cpu_cores_max": null, "memory_mb_min": null, "memory_mb_max": null, "storage_mb_max": 2000, "storage_class": "normal" }),
//         *resources
//     );
//
//     // Update config
//     let patch = json!({
//         "name": "pipeline_runtime_configuration",
//         "description": "desc",
//         "config": {
//             "workers": 5,
//             "resources": {
//                 "memory_mb_max": 100
//             }
//         },
//     });
//     let resp = config
//         .patch(
//             format!("/v0/pipelines/pipeline_runtime_configuration"),
//             &patch,
//         )
//         .await;
//     assert_eq!(resp.status(), StatusCode::OK);
//
//     // Get config
//     let mut resp = config
//         .get(format!(
//             "/v0/pipelines/pipeline_runtime_configuration/config"
//         ))
//         .await;
//     assert_eq!(resp.status(), StatusCode::OK);
//     let resp: Value = resp.json().await.unwrap();
//     let workers = resp["workers"].as_i64().unwrap();
//     let resources = &resp["resources"];
//     assert_eq!(5, workers);
//     assert_eq!(
//         json!({ "cpu_cores_min": null, "cpu_cores_max": null, "memory_mb_min": null, "memory_mb_max": 100, "storage_mb_max": null, "storage_class": null }),
//         *resources
//     );
// }

// TODO: refactor test
// #[actix_web::test]
// #[serial]
// // https://github.com/feldera/feldera/issues/1057
// async fn pipeline_start_without_compiling() {
//     let config = setup().await;
//     let program_request = json!({
//         "name":  "test",
//         "description": "desc",
//         "code": "create table foo(bar integer);",
//     });
//     let mut req = config.post("/v0/programs", &program_request).await;
//     assert_eq!(StatusCode::CREATED, req.status());
//     let resp: Value = req.json().await.unwrap();
//     let version = resp["version"].as_i64().unwrap();
//
//     let pipeline_request = json!({
//         "name":  "test",
//         "description": "desc",
//         "program_name": Some("test".to_string()),
//         "config": {},
//         "connectors": null
//     });
//     let req = config.post("/v0/pipelines", &pipeline_request).await;
//     assert_eq!(req.status(), StatusCode::OK);
//
//     // Start compiling the new program but don't wait till completion
//     let compilation_request = json!({ "version": version });
//     let resp = config
//         .post(format!("/v0/programs/test/compile"), &compilation_request)
//         .await;
//     assert_eq!(StatusCode::ACCEPTED, resp.status());
//
//     // Try starting the program when it is in the CompilingRust state.
//     // There is a possibility that rust compilation is so fast that we don't poll
//     // at that moment but that's unlikely.
//     let now = Instant::now();
//     loop {
//         println!("Waiting till program compilation state is in past the CompilingSql state");
//         std::thread::sleep(time::Duration::from_millis(30));
//         if now.elapsed().as_secs() > 200 {
//             panic!("Compilation timeout");
//         }
//         let mut resp = config.get(format!("/v0/programs/test")).await;
//         let val: Value = resp.json().await.unwrap();
//
//         let status = val["status"].clone();
//         println!("Program status is: {status:?}");
//
//         if status == json!("None") || status == json!("Pending") || status == json!("CompilingSql")
//         {
//             continue;
//         }
//         break;
//     }
//     // Start the program
//     let resp = config
//         .post_no_body(format!("/v0/pipelines/test/start"))
//         .await;
//     assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
// }
