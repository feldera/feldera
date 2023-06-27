use std::{
    process::Command,
    time::{self, Duration, Instant},
};

use actix_http::{encoding::Decoder, Payload, StatusCode};
use awc::ClientResponse;
use serde_json::{json, Value};
use serial_test::serial;
use tempfile::TempDir;
use tokio::sync::OnceCell;

use crate::{compiler::Compiler, config::ManagerConfig};

const TEST_DBSP_URL_VAR: &str = "TEST_DBSP_URL";
const TEST_DBSP_DEFAULT_PORT: u16 = 8089;

// Used if we are testing against a local DBSP instance
// whose lifecycle is managed by this test file
static LOCAL_DBSP_INSTANCE: OnceCell<TempDir> = OnceCell::const_new();

async fn initialize_local_dbsp_instance() -> TempDir {
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
    let manager_config = ManagerConfig {
        port: TEST_DBSP_DEFAULT_PORT,
        bind_address: "0.0.0.0".to_owned(),
        logfile: None,
        working_directory: tmp_dir.path().to_str().unwrap().to_owned(),
        sql_compiler_home: "../../sql-to-dbsp-compiler".to_owned(),
        dbsp_override_path: Some("../../".to_owned()),
        debug: false,
        unix_daemon: false,
        use_auth: false,
        db_connection_string: "postgresql://postgres:postgres@localhost:6666".to_owned(),
        dump_openapi: false,
        precompile: true,
        config_file: None,
        initial_sql: None,
        dev_mode: false,
    };
    let manager_config = manager_config.canonicalize().unwrap();
    println!("Using ManagerConfig: {:?}", manager_config);
    println!("Issuing Compiler::precompile_dependencies(). This will be slow.");
    Compiler::precompile_dependencies(&manager_config)
        .await
        .unwrap();
    println!("Completed Compiler::precompile_dependencies().");

    // We can't use tokio::spawn because super::run() creates its own Tokio Runtime
    let _ = std::thread::spawn(|| {
        super::run(manager_config).unwrap();
    });
    tokio::time::sleep(Duration::from_millis(1000)).await;
    tmp_dir
}

struct TestConfig {
    dbsp_url: String,
    client: awc::Client,
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
        for pipeline in pipelines.as_array().unwrap() {
            let id = pipeline.get("pipeline_id").unwrap().as_str().unwrap();
            let req = config.delete(format!("/v0/pipelines/{}", id)).await;
            assert_eq!(StatusCode::OK, req.status())
        }

        // .. programs
        let mut req = config.get("/v0/programs").await;
        let programs: Value = req.json().await.unwrap();
        for program in programs.as_array().unwrap() {
            let id = program.get("program_id").unwrap().as_str().unwrap();
            let req = config.delete(format!("/v0/programs/{}", id)).await;
            assert_eq!(StatusCode::OK, req.status())
        }

        // .. connectors
        let mut req = config.get("/v0/connectors").await;
        let programs: Value = req.json().await.unwrap();
        for program in programs.as_array().unwrap() {
            let id = program.get("connector_id").unwrap().as_str().unwrap();
            let req = config.delete(format!("/v0/connectors/{}", id)).await;
            assert_eq!(StatusCode::OK, req.status())
        }
    }

    async fn get<S: AsRef<str>>(&self, endpoint: S) -> ClientResponse<Decoder<Payload>> {
        self.client
            .get(self.endpoint_url(endpoint))
            .send()
            .await
            .unwrap()
    }

    async fn post<S: AsRef<str>>(
        &self,
        endpoint: S,
        json: &Value,
    ) -> ClientResponse<Decoder<Payload>> {
        self.client
            .post(self.endpoint_url(endpoint))
            .send_json(&json)
            .await
            .unwrap()
    }

    async fn delete<S: AsRef<str>>(&self, endpoint: S) -> ClientResponse<Decoder<Payload>> {
        self.client
            .delete(self.endpoint_url(endpoint))
            .send()
            .await
            .unwrap()
    }

    async fn compile(&self, program_id: &str, version: i64) {
        let compilation_request = json!({
            "program_id":  program_id,
            "version": version
        });

        let resp = self
            .post("/v0/programs/compile", &compilation_request)
            .await;
        assert_eq!(StatusCode::ACCEPTED, resp.status());

        let now = Instant::now();
        loop {
            println!("Waiting for compilation");
            std::thread::sleep(time::Duration::from_secs(1));
            if now.elapsed().as_secs() > 100 {
                panic!("Compilation timeout");
            }
            let mut resp = self.get(format!("/v0/program?id={}", program_id)).await;
            let val: Value = resp.json().await.unwrap();
            let status = val.get("status").unwrap().as_str().unwrap();
            if status != "CompilingSql" && status != "CompilingRust" && status != "Pending" {
                if status == "Success" {
                    break;
                } else {
                    panic!("Compilation failed with status {}", status);
                }
            }
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
                .get_or_init(initialize_local_dbsp_instance)
                .await;
            format!("http://localhost:{}", TEST_DBSP_DEFAULT_PORT).to_owned()
        }
    };
    let client = awc::Client::default();
    let config = TestConfig { dbsp_url, client };
    config.cleanup().await;
    config
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
    let id = resp.get("program_id").unwrap().as_str().unwrap();
    let version = resp.get("version").unwrap().as_i64().unwrap();
    config.compile(id, version).await;
    let resp = config.delete(format!("/v0/programs/{}", id)).await;
    assert_eq!(StatusCode::OK, resp.status());
    let resp = config.get(format!("/v0/program?id={}", id).as_str()).await;
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
