use std::time::{self, Instant};

use actix_http::{encoding::Decoder, Payload, StatusCode};
use awc::ClientResponse;
use log::info;
use serde_json::{json, Value};
use serial_test::serial;

const TEST_DBSP_URL_VAR: &str = "TEST_DBSP_URL";
const TEST_DBSP_URL_DEFAULT: &str = "http://localhost:8085";

struct TestConfig {
    dbsp_url: String,
    client: awc::Client,
}

impl TestConfig {
    fn with_endpoint(&self, endpoint: &str) -> String {
        format!("{}{}", self.dbsp_url, endpoint)
    }

    async fn cleanup(&self) {
        let config = self;

        // Cleanup pipelines
        let mut req = config.get("/v0/pipelines").await;
        let pipelines: Value = req.json().await.unwrap();
        for pipeline in pipelines.as_array().unwrap() {
            let id = pipeline.get("pipeline_id").unwrap().as_str().unwrap();
            let req = config
                .delete(format!("/v0/pipelines/{}", id).as_str())
                .await;
            assert_eq!(StatusCode::OK, req.status())
        }

        // programs
        let mut req = config.get("/v0/programs").await;
        let programs: Value = req.json().await.unwrap();
        for program in programs.as_array().unwrap() {
            let id = program.get("program_id").unwrap().as_str().unwrap();
            let req = config.delete(format!("/v0/programs/{}", id).as_str()).await;
            assert_eq!(StatusCode::OK, req.status())
        }
    }

    async fn get(&self, endpoint: &str) -> ClientResponse<Decoder<Payload>> {
        self.client
            .get(self.with_endpoint(endpoint))
            .send()
            .await
            .unwrap()
    }

    async fn post(&self, endpoint: &str, json: &Value) -> ClientResponse<Decoder<Payload>> {
        self.client
            .post(self.with_endpoint(endpoint))
            .send_json(&json)
            .await
            .unwrap()
    }

    async fn delete(&self, endpoint: &str) -> ClientResponse<Decoder<Payload>> {
        self.client
            .delete(self.with_endpoint(endpoint))
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
            std::thread::sleep(time::Duration::from_secs(1));
            if now.elapsed().as_secs() > 100 {
                panic!("Compilation timeout");
            }
            let mut resp = self
                .get(format!("/v0/program?id={}", program_id).as_str())
                .await;
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
            info!("Running integration tests against TEST_DBSP_URL: {}", val);
            val
        }
        Err(e) => {
            info!(
                "Could not get TEST_DBSP_URL environment variable (reason: {}).
                Running integration tests against: localhost:8085",
                e
            );
            TEST_DBSP_URL_DEFAULT.to_owned()
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

    let resp = config.delete(format!("/v0/programs/{}", id).as_str()).await;
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
