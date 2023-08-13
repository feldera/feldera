use std::{
    process::Command,
    time::{self, Duration, Instant},
};

use actix_http::{encoding::Decoder, Payload, StatusCode};
use awc::{http, ClientRequest, ClientResponse};
use aws_sdk_cognitoidentityprovider::config::Region;
use colored::Colorize;
use serde_json::{json, Value};
use serial_test::serial;
use tempfile::TempDir;
use tokio::{sync::OnceCell, time::sleep};

use crate::{
    compiler::Compiler,
    config::{ApiServerConfig, CompilerConfig, DatabaseConfig, LocalRunnerConfig},
    db::{Pipeline, PipelineStatus},
};
use std::sync::Arc;
use tokio::sync::Mutex;

const TEST_DBSP_URL_VAR: &str = "TEST_DBSP_URL";
const TEST_DBSP_DEFAULT_PORT: u16 = 8089;

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
        initial_sql: None,
    };
    let api_config = ApiServerConfig {
        port: TEST_DBSP_DEFAULT_PORT,
        bind_address: "0.0.0.0".to_owned(),
        api_server_working_directory: workdir.to_owned(),
        use_auth: false,
        dev_mode: false,
        dump_openapi: false,
        config_file: None,
    }
    .canonicalize()
    .unwrap();
    let compiler_config = CompilerConfig {
        compiler_working_directory: workdir.to_owned(),
        sql_compiler_home: "../../sql-to-dbsp-compiler".to_owned(),
        dbsp_override_path: Some("../../".to_owned()),
        debug: false,
        precompile: true,
    }
    .canonicalize()
    .unwrap();
    let local_runner_config = LocalRunnerConfig {
        runner_working_directory: workdir.to_owned(),
    }
    .canonicalize()
    .unwrap();
    println!("Using ApiServerConfig: {:?}", api_config);
    println!("Issuing Compiler::precompile_dependencies(). This will be slow.");
    Compiler::precompile_dependencies(&compiler_config)
        .await
        .unwrap();
    println!("Completed Compiler::precompile_dependencies().");

    // We cannot reuse the tokio runtime instance created by the test (e.g., the one implicitly
    // created via [actix_web::test]) to create the compiler, local runner and api futures below.
    // The reason is that when that first test completes, these futures will get cancelled.
    //
    // To avoid that problem, and for general integration test hygiene, we force another tokio
    // runtime to be created here to run the server processes. We obviously can't create one
    // runtime within another, so the easiest way to work around that is to do so within an
    // std::thread::spawn().
    std::thread::spawn(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let db = crate::db::ProjectDB::connect(
                    &database_config,
                    #[cfg(feature = "pg-embed")]
                    Some(&api_config),
                )
                .await
                .unwrap();
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
                let _api_server = crate::api::run(db, api_config).await.unwrap();
            })
    });
    tokio::time::sleep(Duration::from_millis(1000)).await;
    tmp_dir
}

struct TestConfig {
    dbsp_url: String,
    client: awc::Client,
    bearer_token: Option<String>,
}

impl TestConfig {
    fn endpoint_url<S: AsRef<str>>(&self, endpoint: S) -> String {
        format!("{}{}", self.dbsp_url, endpoint.as_ref())
    }

    async fn cleanup(&self) {
        let config = self;

        // Cleanup pipelines..
        let mut req = config.get("/v0/pipelines").await;
        let pipelines: Value = req.json().await.unwrap();
        // First, shutdown the pipelines
        for pipeline in pipelines.as_array().unwrap() {
            let id = pipeline["descriptor"]["pipeline_id"].as_str().unwrap();
            let req = config
                .post_no_body(format!("/v0/pipelines/{}/shutdown", id))
                .await;
            assert_eq!(StatusCode::ACCEPTED, req.status(), "Response {:?}", req)
        }
        // Once we can confirm pipelines are shutdown, delete them
        for pipeline in pipelines.as_array().unwrap() {
            let id = pipeline["descriptor"]["pipeline_id"].as_str().unwrap();
            self.wait_for_pipeline_status(
                id,
                PipelineStatus::Shutdown,
                time::Duration::from_secs(20),
            )
            .await;
            let req = config.delete(format!("/v0/pipelines/{}", id)).await;
            assert_eq!(StatusCode::OK, req.status(), "Response {:?}", req)
        }

        // .. programs
        let mut req = config.get("/v0/programs").await;
        let programs: Value = req.json().await.unwrap();
        for program in programs.as_array().unwrap() {
            let id = program["program_id"].as_str().unwrap();
            let req = config.delete(format!("/v0/programs/{}", id)).await;
            assert_eq!(StatusCode::OK, req.status(), "Response {:?}", req)
        }

        // .. connectors
        let mut req = config.get("/v0/connectors").await;
        let programs: Value = req.json().await.unwrap();
        for program in programs.as_array().unwrap() {
            let id = program["connector_id"].as_str().unwrap();
            let req = config.delete(format!("/v0/connectors/{}", id)).await;
            assert_eq!(StatusCode::OK, req.status(), "Response {:?}", req)
        }
    }

    async fn get<S: AsRef<str>>(&self, endpoint: S) -> ClientResponse<Decoder<Payload>> {
        self.maybe_attach_bearer_token(self.client.get(self.endpoint_url(endpoint)))
            .send()
            .await
            .unwrap()
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

    async fn quantiles_csv(&self, id: &str, table: &str) -> String {
        let mut resp = self
            .post_no_body(format!(
                "/v0/pipelines/{id}/egress/{table}?query=quantiles&mode=snapshot&format=csv"
            ))
            .await;
        assert!(resp.status().is_success());
        let resp: Value = resp.json().await.unwrap();
        resp.get("text_data").unwrap().as_str().unwrap().to_string()
    }

    async fn quantiles_json(&self, id: &str, table: &str) -> String {
        let mut resp = self
            .post_no_body(format!(
                "/v0/pipelines/{id}/egress/{table}?query=quantiles&mode=snapshot&format=json"
            ))
            .await;
        assert!(resp.status().is_success());
        let resp: Value = resp.json().await.unwrap();
        resp.get("json_data").unwrap().to_string()
    }

    async fn neighborhood_json(
        &self,
        id: &str,
        table: &str,
        anchor: Option<Value>,
        before: u64,
        after: u64,
    ) -> String {
        let mut resp = self
            .post(
                format!(
                "/v0/pipelines/{id}/egress/{table}?query=neighborhood&mode=snapshot&format=json"
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

    async fn compile(&self, program_id: &str, version: i64) {
        let compilation_request = json!({ "version": version });

        let resp = self
            .post(
                format!("/v0/programs/{program_id}/compile"),
                &compilation_request,
            )
            .await;
        assert_eq!(StatusCode::ACCEPTED, resp.status());

        let now = Instant::now();
        loop {
            println!("Waiting for compilation");
            std::thread::sleep(time::Duration::from_secs(1));
            if now.elapsed().as_secs() > 100 {
                panic!("Compilation timeout");
            }
            let mut resp = self.get(format!("/v0/programs/{}", program_id)).await;
            let val: Value = resp.json().await.unwrap();
            let status = val["status"].as_str().unwrap();
            if status != "CompilingSql" && status != "CompilingRust" && status != "Pending" {
                if status == "Success" {
                    break;
                } else {
                    panic!("Compilation failed with status {}", status);
                }
            }
        }
    }

    /// Wait for the pipeline to reach the specified status.
    /// Panic afrer `timeout`.
    async fn wait_for_pipeline_status(
        &self,
        id: &str,
        status: PipelineStatus,
        timeout: Duration,
    ) -> Pipeline {
        let start = Instant::now();
        loop {
            let mut response = self.get(format!("/v0/pipelines/{}", id)).await;

            let pipeline = response.json::<Pipeline>().await.unwrap();

            println!("Pipeline:\n{pipeline:#?}");
            if pipeline.state.current_status == status {
                return pipeline;
            }
            if start.elapsed() >= timeout {
                panic!("Timeout waiting for pipeline status {status:?}");
            }
            sleep(Duration::from_millis(300)).await;
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
    let client = awc::Client::default();
    let bearer_token = bearer_token().await;
    let config = TestConfig {
        dbsp_url,
        client,
        bearer_token,
    };
    config.cleanup().await;
    config
}

async fn deploy_pipeline_without_connectors(config: &TestConfig, sql: &str) -> String {
    let program_request = json!({
        "name":  "test",
        "description": "desc",
        "code": sql,
    });
    let mut req = config.post("/v0/programs", &program_request).await;
    assert_eq!(StatusCode::CREATED, req.status());
    let resp: Value = req.json().await.unwrap();
    let id = resp["program_id"].as_str().unwrap();
    let version = resp["version"].as_i64().unwrap();
    config.compile(id, version).await;

    let pipeline_request = json!({
        "name":  "test",
        "description": "desc",
        "program_id": Some(id.to_string()),
        "config": {},
        "connectors": null
    });
    let mut req = config.post("/v0/pipelines", &pipeline_request).await;
    let resp: Value = req.json().await.unwrap();
    let id = resp["pipeline_id"].as_str().unwrap();

    let resp = config
        .post_no_body(format!("/v0/pipelines/{}/pause", id))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    config
        .wait_for_pipeline_status(id, PipelineStatus::Paused, Duration::from_millis(100_000))
        .await;

    id.to_string()
}

#[actix_web::test]
#[serial]
async fn lists_at_initialization_are_empty() {
    let config = setup().await;
    for endpoint in vec!["/v0/pipelines", "/v0/programs", "/v0/connectors"] {
        let mut req = config.get(endpoint).await;
        let ret: Value = req.json().await.unwrap();
        assert_eq!(ret, json!([]));
    }
}

#[actix_web::test]
#[serial]
async fn program_create_compile_delete() {
    let config = setup().await;
    let program_request = json!({
        "name":  "test",
        "description": "desc",
        "code": "create table t1(c1 integer);"
    });
    let mut req = config.post("/v0/programs", &program_request).await;
    assert_eq!(StatusCode::CREATED, req.status());
    let resp: Value = req.json().await.unwrap();
    let id = resp["program_id"].as_str().unwrap();
    let version = resp["version"].as_i64().unwrap();
    config.compile(id, version).await;
    let resp = config.delete(format!("/v0/programs/{}", id)).await;
    assert_eq!(StatusCode::OK, resp.status());
    let resp = config.get(format!("/v0/programs/{}", id).as_str()).await;
    assert_eq!(StatusCode::NOT_FOUND, resp.status());
}

#[actix_web::test]
#[serial]
async fn program_create_twice() {
    let config = setup().await;
    let program_request = json!({
        "name":  "test",
        "description": "desc",
        "code": "create table t1(c1 integer);"
    });
    let req = config.post("/v0/programs", &program_request).await;
    assert_eq!(StatusCode::CREATED, req.status());

    // Same name, different desc/program.
    let program_request = json!({
        "name":  "test",
        "description": "desc1",
        "code": "create table t2(c2 integer);"
    });
    let req = config.post("/v0/programs", &program_request).await;
    assert_eq!(StatusCode::CONFLICT, req.status());
}

#[actix_web::test]
#[serial]
async fn deploy_pipeline() {
    let config = setup().await;
    let id = deploy_pipeline_without_connectors(
        &config,
        "create table t1(c1 integer); create view v1 as select * from t1;",
    )
    .await;

    // Pause a pipeline before it is started
    let resp = config
        .post_no_body(format!("/v0/pipelines/{}/pause", id))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Start the pipeline
    let resp = config
        .post_no_body(format!("/v0/pipelines/{}/start", id))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    config
        .wait_for_pipeline_status(&id, PipelineStatus::Running, Duration::from_millis(1_000))
        .await;
    // Push some data.
    let req = config
        .post_csv(
            format!("/v0/pipelines/{}/ingress/T1", id),
            "1\n2\n3\n".to_string(),
        )
        .await;
    assert!(req.status().is_success());

    // Pause a pipeline after it is started
    let resp = config
        .post_no_body(format!("/v0/pipelines/{}/pause", id))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    config
        .wait_for_pipeline_status(&id, PipelineStatus::Paused, Duration::from_millis(1_000))
        .await;

    // Querying quantiles should work in paused state.
    let quantiles = config.quantiles_csv(&id, "T1").await;
    assert_eq!(&quantiles, "1,1\n2,1\n3,1\n");

    // Start the pipeline
    let resp = config
        .post_no_body(format!("/v0/pipelines/{}/start", id))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    config
        .wait_for_pipeline_status(&id, PipelineStatus::Running, Duration::from_millis(1_000))
        .await;

    // Shutdown the pipeline
    let resp = config
        .post_no_body(format!("/v0/pipelines/{}/shutdown", id))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    config
        .wait_for_pipeline_status(&id, PipelineStatus::Shutdown, Duration::from_millis(10_000))
        .await;
}

// Correctly report pipeline panic to the user.
#[actix_web::test]
#[serial]
async fn pipeline_panic() {
    let config = setup().await;

    // This SQL program is known to panic.
    let id = deploy_pipeline_without_connectors(
        &config,
        "create table t1(c1 integer); create view v1 as select element(array [2, 3]) from t1;",
    )
    .await;

    // Start the pipeline
    let resp = config
        .post_no_body(format!("/v0/pipelines/{}/start", id))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    config
        .wait_for_pipeline_status(&id, PipelineStatus::Running, Duration::from_millis(1_000))
        .await;

    // Push some data.  This should cause a panic.
    let req = config
        .post_csv(
            format!("/v0/pipelines/{}/ingress/T1", id),
            "1\n2\n3\n".to_string(),
        )
        .await;
    assert!(req.status().is_success());

    // The manager should discover the error next time it polls the pipeline.
    let pipeline = config
        .wait_for_pipeline_status(&id, PipelineStatus::Failed, Duration::from_millis(20_000))
        .await;

    assert_eq!(
        pipeline.state.error.as_ref().unwrap().error_code,
        "RuntimeError.WorkerPanic"
    );
    println!("status: {pipeline:?}");

    // Shutdown the pipeline
    let resp = config
        .post_no_body(format!("/v0/pipelines/{}/shutdown", id))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    config
        .wait_for_pipeline_status(&id, PipelineStatus::Shutdown, Duration::from_millis(10_000))
        .await;
}

// Program deletes must not cause pipeline deletes.
#[actix_web::test]
#[serial]
async fn program_delete_with_pipeline() {
    let config = setup().await;
    let program_request = json!({
        "name":  "test",
        "description": "desc",
        "code": "create table t1(c1 integer); create view v1 as select * from t1;"
    });
    let mut req = config.post("/v0/programs", &program_request).await;
    assert_eq!(StatusCode::CREATED, req.status());
    let resp: Value = req.json().await.unwrap();
    let program_id = resp["program_id"].as_str().unwrap();

    let pipeline_request = json!({
        "name":  "test",
        "description": "desc",
        "program_id": Some(program_id.to_string()),
        "config": {},
        "connectors": null
    });
    let mut req = config.post("/v0/pipelines", &pipeline_request).await;
    assert_eq!(StatusCode::OK, req.status());
    let resp: Value = req.json().await.unwrap();
    let pipeline_id = resp["pipeline_id"].as_str().unwrap();
    let req = config.get(format!("/v0/pipelines/{pipeline_id}")).await;
    assert_eq!(StatusCode::OK, req.status());

    // Now delete the program and check that the pipeline still exists
    let req = config.delete(format!("/v0/programs/{program_id}")).await;
    assert_eq!(StatusCode::BAD_REQUEST, req.status());

    let req = config.get(format!("/v0/programs/{program_id}")).await;
    assert_eq!(StatusCode::OK, req.status());

    let req = config.get(format!("/v0/pipelines/{pipeline_id}")).await;
    assert_eq!(StatusCode::OK, req.status());
}

#[actix_web::test]
#[serial]
async fn json_ingress() {
    let config = setup().await;
    let id = deploy_pipeline_without_connectors(
        &config,
        "create table t1(c1 integer, c2 bool, c3 varchar); create view v1 as select * from t1;",
    )
    .await;

    // Start the pipeline
    let resp = config
        .post_no_body(format!("/v0/pipelines/{}/start", id))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    config
        .wait_for_pipeline_status(&id, PipelineStatus::Running, Duration::from_millis(1_000))
        .await;

    // Push some data using default json config.
    let req = config
        .post_json(
            format!("/v0/pipelines/{}/ingress/T1?format=json&update_format=raw", id),
            r#"{"C1": 10, "C2": true}
            {"C1": 20, "C3": "foo"}"#
                .to_string(),
        )
        .await;
    assert!(req.status().is_success());

    let quantiles = config.quantiles_json(&id, "T1").await;
    assert_eq!(quantiles, "[{\"data\":{\"C1\":10,\"C2\":true,\"C3\":null},\"table\":null,\"weight\":1},{\"data\":{\"C1\":20,\"C2\":null,\"C3\":\"foo\"},\"table\":null,\"weight\":1}]");

    let hood = config
        .neighborhood_json(
            &id,
            "T1",
            Some(json!({"C1":10,"C2":true,"C3":null})),
            10,
            10,
        )
        .await;
    assert_eq!(hood, "[{\"data\":{\"index\":0,\"key\":{\"C1\":10,\"C2\":true,\"C3\":null}},\"table\":null,\"weight\":1},{\"data\":{\"index\":1,\"key\":{\"C1\":20,\"C2\":null,\"C3\":\"foo\"}},\"table\":null,\"weight\":1}]");

    // Push more data using insert/delete format.
    let req = config
        .post_json(
            format!(
                "/v0/pipelines/{}/ingress/T1?format=json&update_format=insert_delete",
                id
            ),
            r#"{"delete": {"C1": 10, "C2": true}}
            {"insert": {"C1": 30, "C3": "bar"}}"#
                .to_string(),
        )
        .await;
    assert!(req.status().is_success());

    let quantiles = config.quantiles_json(&id, "T1").await;
    assert_eq!(quantiles, "[{\"data\":{\"C1\":20,\"C2\":null,\"C3\":\"foo\"},\"table\":null,\"weight\":1},{\"data\":{\"C1\":30,\"C2\":null,\"C3\":\"bar\"},\"table\":null,\"weight\":1}]");

    // Format data as json array.
    let req = config
        .post_json(
            format!(
                "/v0/pipelines/{}/ingress/T1?format=json&update_format=insert_delete",
                id
            ),
            r#"{"insert": [40, true, "buzz"]}"#.to_string(),
        )
        .await;
    assert!(req.status().is_success());

    // Use array of updates instead of newline-delimited JSON
    let req = config
        .post_json(
            format!(
                "/v0/pipelines/{}/ingress/T1?format=json&update_format=insert_delete&array=true",
                id
            ),
            r#"[{"delete": [40, true, "buzz"]}, {"insert": [50, true, ""]}]"#.to_string(),
        )
        .await;
    assert!(req.status().is_success());

    let quantiles = config.quantiles_json(&id, "T1").await;
    assert_eq!(quantiles, "[{\"data\":{\"C1\":20,\"C2\":null,\"C3\":\"foo\"},\"table\":null,\"weight\":1},{\"data\":{\"C1\":30,\"C2\":null,\"C3\":\"bar\"},\"table\":null,\"weight\":1},{\"data\":{\"C1\":50,\"C2\":true,\"C3\":\"\"},\"table\":null,\"weight\":1}]");

    // Trigger parse errors.
    let mut req = config
        .post_json(
            format!(
                "/v0/pipelines/{}/ingress/T1?format=json&update_format=insert_delete&array=true",
                id
            ),
            r#"[{"insert": [35, true, ""]}, {"delete": [40, "foo", "buzz"]}, {"insert": [true, true, ""]}]"#.to_string(),
        )
        .await;
    assert_eq!(req.status(), StatusCode::BAD_REQUEST);
    let body = req.body().await.unwrap();
    let error = std::str::from_utf8(&body).unwrap();
    assert_eq!(error, "{\"message\":\"Errors parsing input data (2 errors):\\n    Parse error (event #2): failed to deserialize JSON record: error parsing field 'C2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\\nInvalid fragment: [40, \\\"foo\\\", \\\"buzz\\\"]\\n    Parse error (event #3): failed to deserialize JSON record: error parsing field 'C1': invalid type: boolean `true`, expected i32 at line 1 column 5\\nInvalid fragment: [true, true, \\\"\\\"]\",\"error_code\":\"ParseErrors\",\"details\":{\"errors\":[{\"description\":\"failed to deserialize JSON record: error parsing field 'C2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\",\"event_number\":2,\"field\":\"C2\",\"invalid_bytes\":null,\"invalid_text\":\"[40, \\\"foo\\\", \\\"buzz\\\"]\",\"suggestion\":null},{\"description\":\"failed to deserialize JSON record: error parsing field 'C1': invalid type: boolean `true`, expected i32 at line 1 column 5\",\"event_number\":3,\"field\":\"C1\",\"invalid_bytes\":null,\"invalid_text\":\"[true, true, \\\"\\\"]\",\"suggestion\":null}],\"num_errors\":2}}");

    // Even records that are parsed successfully don't get ingested when
    // using array format.
    let quantiles = config.quantiles_json(&id, "T1").await;
    assert_eq!(quantiles, "[{\"data\":{\"C1\":20,\"C2\":null,\"C3\":\"foo\"},\"table\":null,\"weight\":1},{\"data\":{\"C1\":30,\"C2\":null,\"C3\":\"bar\"},\"table\":null,\"weight\":1},{\"data\":{\"C1\":50,\"C2\":true,\"C3\":\"\"},\"table\":null,\"weight\":1}]");

    let mut req = config
        .post_json(
            format!(
                "/v0/pipelines/{}/ingress/T1?format=json&update_format=insert_delete",
                id
            ),
            r#"{"insert": [25, true, ""]}{"delete": [40, "foo", "buzz"]}{"insert": [true, true, ""]}"#.to_string(),
        )
        .await;
    assert_eq!(req.status(), StatusCode::BAD_REQUEST);
    let body = req.body().await.unwrap();
    let error = std::str::from_utf8(&body).unwrap();
    assert_eq!(error, "{\"message\":\"Errors parsing input data (2 errors):\\n    Parse error (event #2): failed to deserialize JSON record: error parsing field 'C2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\\nInvalid fragment: [40, \\\"foo\\\", \\\"buzz\\\"]\\n    Parse error (event #3): failed to deserialize JSON record: error parsing field 'C1': invalid type: boolean `true`, expected i32 at line 1 column 5\\nInvalid fragment: [true, true, \\\"\\\"]\",\"error_code\":\"ParseErrors\",\"details\":{\"errors\":[{\"description\":\"failed to deserialize JSON record: error parsing field 'C2': invalid type: string \\\"foo\\\", expected a boolean at line 1 column 10\",\"event_number\":2,\"field\":\"C2\",\"invalid_bytes\":null,\"invalid_text\":\"[40, \\\"foo\\\", \\\"buzz\\\"]\",\"suggestion\":null},{\"description\":\"failed to deserialize JSON record: error parsing field 'C1': invalid type: boolean `true`, expected i32 at line 1 column 5\",\"event_number\":3,\"field\":\"C1\",\"invalid_bytes\":null,\"invalid_text\":\"[true, true, \\\"\\\"]\",\"suggestion\":null}],\"num_errors\":2}}");

    // Even records that are parsed successfully don't get ingested when
    // using array format.
    let quantiles = config.quantiles_csv(&id, "T1").await;
    assert_eq!(quantiles, "20,,foo,1\n25,true,,1\n30,,bar,1\n50,true,,1\n");

    // Debezium CDC format
    let req = config
        .post_json(
            format!(
                "/v0/pipelines/{}/ingress/T1?format=json&update_format=debezium",
                id
            ),
            r#"{"payload": {"op": "u", "before": [50, true, ""], "after": [60, true, "hello"]}}"#
                .to_string(),
        )
        .await;
    assert!(req.status().is_success());

    let quantiles = config.quantiles_csv(&id, "T1").await;
    assert_eq!(
        quantiles,
        "20,,foo,1\n25,true,,1\n30,,bar,1\n60,true,hello,1\n"
    );

    // Push some CSV data (the second record is invalid, but the other two should get ingested).
    let mut req = config
        .post_json(
            format!("/v0/pipelines/{}/ingress/T1?format=csv", id),
            r#"15,true,foo
not_a_number,true,ΑαΒβΓγΔδ
16,false,unicode🚲"#
                .to_string(),
        )
        .await;
    assert_eq!(req.status(), StatusCode::BAD_REQUEST);
    let body = req.body().await.unwrap();
    let error = std::str::from_utf8(&body).unwrap();
    assert_eq!(error, "{\"message\":\"Errors parsing input data (1 errors):\\n    Parse error (event #2): failed to deserialize CSV record: error parsing field 'C1': field 0: invalid digit found in string\\nInvalid fragment: ByteRecord([\\\"not_a_number\\\", \\\"true\\\", \\\"ΑαΒβΓγΔδ\\\"])\",\"error_code\":\"ParseErrors\",\"details\":{\"errors\":[{\"description\":\"failed to deserialize CSV record: error parsing field 'C1': field 0: invalid digit found in string\",\"event_number\":2,\"field\":\"C1\",\"invalid_bytes\":null,\"invalid_text\":\"ByteRecord([\\\"not_a_number\\\", \\\"true\\\", \\\"ΑαΒβΓγΔδ\\\"])\",\"suggestion\":null}],\"num_errors\":1}}");

    let quantiles = config.quantiles_json(&id, "T1").await;
    assert_eq!(
        quantiles,
        "[{\"data\":{\"C1\":15,\"C2\":true,\"C3\":\"foo\"},\"table\":null,\"weight\":1},{\"data\":{\"C1\":16,\"C2\":false,\"C3\":\"unicode🚲\"},\"table\":null,\"weight\":1},{\"data\":{\"C1\":20,\"C2\":null,\"C3\":\"foo\"},\"table\":null,\"weight\":1},{\"data\":{\"C1\":25,\"C2\":true,\"C3\":\"\"},\"table\":null,\"weight\":1},{\"data\":{\"C1\":30,\"C2\":null,\"C3\":\"bar\"},\"table\":null,\"weight\":1},{\"data\":{\"C1\":60,\"C2\":true,\"C3\":\"hello\"},\"table\":null,\"weight\":1}]"
    );

    // Shutdown the pipeline
    let resp = config
        .post_no_body(format!("/v0/pipelines/{}/shutdown", id))
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    config
        .wait_for_pipeline_status(&id, PipelineStatus::Shutdown, Duration::from_millis(10_000))
        .await;
}
