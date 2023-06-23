use actix_web::http::header::ContentType;
use log::info;
use serde_json::{json, Value};

const TEST_DBSP_URL_VAR: &str = "TEST_DBSP_URL";
const TEST_DBSP_URL_DEFAULT: &str = "http://localhost:8085";

struct TestConfig {
    dbsp_url: String,
}

impl TestConfig {
    fn with_endpoint(&self, endpoint: &str) -> String {
        format!("{}{}", self.dbsp_url, endpoint)
    }
}

fn setup() -> TestConfig {
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
    TestConfig { dbsp_url }
}

#[actix_web::test]
async fn cleanup() {
    let config = setup();
    let client = awc::Client::default();
    // construct request
    let mut req = client
        .get(config.with_endpoint("/v0/pipelines"))
        .send()
        .await
        .unwrap();
    let x: Value = req.json().await.unwrap();
    assert_eq!(x, json!([]));
}

#[actix_web::test]
async fn lists_are_empty() {
    let config = setup();
    let client = awc::Client::default();
    for endpoint in vec!["/v0/pipelines", "/v0/programs", "/v0/connectors"] {
        let mut req = client
            .get(config.with_endpoint(endpoint))
            .send()
            .await
            .unwrap();
        let ret: Value = req.json().await.unwrap();
        assert_eq!(ret, json!([]));
    }
}

#[actix_web::test]
async fn program_create_get_delete() {
    let config = setup();
    let client = awc::Client::default();
    let body = json!({
        "name":  "test1",
        "description": "test",
        "code": "create table t1(c1 integer);"
    });
    println!("json {}", body.to_string());
    let mut req = client
        .post(config.with_endpoint("/v0/programs"))
        .send_json(&body)
        .await
        .unwrap();
    println!("{}", req.status());
    // let ret = req.status();
    // assert_eq!(ret, json!([]));
}
