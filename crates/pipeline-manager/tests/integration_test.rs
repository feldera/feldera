pub mod test_client;

#[cfg(feature = "feldera-enterprise")]
use crate::test_client::wait_for_condition;
use crate::test_client::TestClient;
use actix_http::StatusCode;
#[cfg(feature = "feldera-enterprise")]
use feldera_types::checkpoint::{CheckpointResponse, CheckpointStatus};
use feldera_types::completion_token::{
    CompletionStatus, CompletionStatusResponse, CompletionTokenResponse,
};
use feldera_types::config::{ResourceConfig, RuntimeConfig, StorageOptions};
use feldera_types::time_series::TimeSeries;
use pipeline_manager::db::types::combined_status::CombinedStatus;
use pipeline_manager::db::types::program::CompilationProfile;
use pipeline_manager::db::types::program::ProgramConfig;
use serde_json::{json, Value};
use serial_test::serial;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use tokio::time::{interval, sleep};

/// Tests the cluster health endpoint.
#[actix_web::test]
#[serial]
async fn cluster_health_check() {
    let config = TestClient::setup_and_full_cleanup().await;

    let mut ticker = interval(Duration::from_secs(2)); // check every 2 seconds
    let start = Instant::now();
    let timeout = Duration::from_secs(300); // 5 minutes

    loop {
        ticker.tick().await;
        let (status, health) = config.check_cluster_health().await;

        // Safely extract and check "healthy" for both runner and compiler
        let runner_healthy = match health.get("runner").and_then(|h| h.get("healthy")) {
            Some(val) => val.as_bool().unwrap_or(false),
            None => false,
        };
        let compiler_healthy = match health.get("compiler").and_then(|h| h.get("healthy")) {
            Some(val) => val.as_bool().unwrap_or(false),
            None => false,
        };

        if runner_healthy && compiler_healthy {
            assert_eq!(StatusCode::OK, status);
            // we can stop the test are both are healthy
            break;
        } else {
            // if either of the service is unhealthy,
            // status code must be 503
            assert_eq!(StatusCode::SERVICE_UNAVAILABLE, status)
        }

        if start.elapsed() > timeout {
            panic!(
                "Timed out waiting for both runner and compiler to become healthy. Last health status: {health:#?}",
            );
        }
    }
}

/// Tests the creation of pipelines using its POST endpoint.
#[actix_web::test]
#[serial]
async fn pipeline_post() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Empty body
    assert_eq!(
        config.post_json("/v0/pipelines", &json!({})).await.status(),
        StatusCode::BAD_REQUEST
    );

    // Name is missing
    assert_eq!(
        config
            .post_json("/v0/pipelines", &json!({ "program-code": "" }))
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );

    // Program SQL code is missing
    assert_eq!(
        config
            .post_json("/v0/pipelines", &json!({ "name": "test-1" }))
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );

    // Minimum body
    let pipeline = config
        .post_pipeline(&json!({
            "name": "test-1",
            "program_code": "",
        }))
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
    let pipeline = config
        .post_pipeline(&json!({
            "name": "test-2",
            "program_code": "sql-2",
        }))
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
    let pipeline = config
        .post_pipeline(&json!({
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
        }))
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
    let config = TestClient::setup_and_full_cleanup().await;

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
    config.prepare_pipeline("test-1", sql1).await;

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
    config.prepare_pipeline("test-2", sql2).await;

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
const PIPELINE_FIELD_SELECTOR_ALL_FIELDS: [&str; 33] = [
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
    "deployment_error",
    "refresh_version",
    "storage_status",
    "deployment_id",
    "deployment_initial",
    "deployment_status",
    "deployment_status_since",
    "deployment_desired_status",
    "deployment_desired_status_since",
    "deployment_resources_status",
    "deployment_resources_status_since",
    "deployment_resources_desired_status",
    "deployment_resources_desired_status_since",
    "deployment_runtime_status",
    "deployment_runtime_status_since",
    "deployment_runtime_desired_status",
    "deployment_runtime_desired_status_since",
];

/// Fields included in the `status` selector.
const PIPELINE_FIELD_SELECTOR_STATUS_FIELDS: [&str; 26] = [
    "id",
    "name",
    "description",
    "created_at",
    "version",
    "platform_version",
    "program_version",
    "program_status",
    "program_status_since",
    "deployment_error",
    "refresh_version",
    "storage_status",
    "deployment_id",
    "deployment_initial",
    "deployment_status",
    "deployment_status_since",
    "deployment_desired_status",
    "deployment_desired_status_since",
    "deployment_resources_status",
    "deployment_resources_status_since",
    "deployment_resources_desired_status",
    "deployment_resources_desired_status_since",
    "deployment_runtime_status",
    "deployment_runtime_status_since",
    "deployment_runtime_desired_status",
    "deployment_runtime_desired_status_since",
];

/// Tests the retrieval of a pipeline and list of pipelines with a field selector.
#[actix_web::test]
#[serial]
async fn pipeline_get_selector() {
    let config = TestClient::setup_and_full_cleanup().await;
    config
        .prepare_pipeline("test-1", "CREATE TABLE t1(c1 INT);")
        .await;
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

/// Tests creating a pipeline, waiting for it to compile and deleting it afterward.
#[actix_web::test]
#[serial]
async fn pipeline_create_compile_delete() {
    let client = TestClient::setup_and_full_cleanup().await;
    client
        .post_pipeline(&json!({
            "name":  "test",
            "description": "desc",
            "runtime_config": {},
            "program_code": "CREATE TABLE t1(c1 INTEGER);",
            "program_config": {}
        }))
        .await;
    let _ = client.get_pipeline("test").await;
    client.wait_for_compiled_program("test", 1).await;
    client.delete_pipeline("test").await;
    assert!(!client.pipeline_exists("test").await);
}

/// Tests that an error is thrown if a pipeline with the same name already exists.
#[actix_web::test]
#[serial]
async fn pipeline_name_conflict() {
    let config = TestClient::setup_and_full_cleanup().await;
    let request_body = json!({
        "name":  "test",
        "description": "desc",
        "runtime_config": {},
        "program_code": "CREATE TABLE t1(c1 INTEGER);",
        "program_config": {}
    });
    let response = config.post_json("/v0/pipelines", &request_body).await;
    assert_eq!(StatusCode::CREATED, response.status());

    // Same name, different description and program code.
    let request_body = json!({
        "name":  "test",
        "description": "a different description",
        "runtime_config": {},
        "program_code": "CREATE TABLE t2(c2 VARCHAR);",
        "program_config": {}
    });
    let response = config.post_json("/v0/pipelines", &request_body).await;
    assert_eq!(StatusCode::CONFLICT, response.status());
}

/// Tests pausing and resuming a pipeline, pushing ingress data, and validating the contents of the output view.
#[actix_web::test]
#[serial]
async fn deploy_pipeline() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Basic test pipeline
    config.prepare_pipeline(
        "test",
        "CREATE TABLE t1(c1 INTEGER) with ('materialized' = 'true'); CREATE VIEW v1 AS SELECT * FROM t1;",
    ).await;
    config.resume_pipeline("test", true).await;

    // Push some data
    let response = config
        .post("/v0/pipelines/test/ingress/t1", "1\n2\n3\n".to_string())
        .await;
    assert!(response.status().is_success());

    // Push more data with Windows-style newlines
    let response = config
        .post("/v0/pipelines/test/ingress/t1", "4\r\n5\r\n6".to_string())
        .await;
    assert!(response.status().is_success());

    // Pause a pipeline after it is started
    config.pause_pipeline("test", true).await;

    // Querying should work in Paused state
    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1;")
            .await,
        json!([{"c1": 1}, {"c1": 2}, {"c1": 3}, {"c1": 4}, {"c1": 5}, {"c1": 6}])
    );

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    // Querying should work in Running state
    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1;")
            .await,
        json!([{"c1": 1}, {"c1": 2}, {"c1": 3}, {"c1": 4}, {"c1": 5}, {"c1": 6}])
    );

    // Stop force and clear the pipeline
    config.stop_force_and_clear_pipeline("test").await;
}

/// Tests that pipeline panics are correctly reported by providing a SQL program
/// which will panic when data is pushed to it.
#[actix_web::test]
#[serial]
async fn pipeline_panic() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Basic test pipeline with a SQL program which is known to panic
    config
        .prepare_pipeline(
            "test",
            "CREATE TABLE t1(c1 INTEGER); CREATE VIEW v1 AS SELECT ELEMENT(ARRAY [2, 3]) FROM t1;",
        )
        .await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    // Push some data, which should cause a panic
    let _ = config
        .post("/v0/pipelines/test/ingress/t1", "1\n2\n3\n".to_string())
        .await;

    // Should discover the error next time it polls the pipeline.
    let error = config.wait_for_stopped_with_error("test").await;
    assert_eq!(error["error_code"], "RuntimeError.WorkerPanic");

    // Stop force and clear the pipeline
    config.stop_force_and_clear_pipeline("test").await;
}

/// Tests starting, stopping, starting and stopping again.
#[actix_web::test]
#[serial]
async fn pipeline_restart() {
    let config = TestClient::setup_and_full_cleanup().await;
    config
        .prepare_pipeline(
            "test",
            "CREATE TABLE t1(c1 INTEGER); CREATE VIEW v1 AS SELECT * FROM t1;",
        )
        .await;
    config.resume_pipeline("test", true).await;
    config.stop_force_pipeline("test", true).await;
    config.start_pipeline("test", true).await;
    config.stop_force_and_clear_pipeline("test").await;
}

/// Tests that the pipeline runtime configuration is validated and stored correctly,
/// and that patching works on the field as whole.
#[actix_web::test]
#[serial]
async fn pipeline_runtime_config() {
    let config = TestClient::setup_and_full_cleanup().await;

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
                    cpu_cores_min: Some(5.0),
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
        let mut response = config.put_json("/v0/pipelines/test-1", &body).await;
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
        let mut response = config.put_json("/v0/pipelines/test-1", &body).await;
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
    let response = config.post_json("/v0/pipelines", &body).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Patching: apply patch which does not affect runtime_config
    let mut response = config.patch_json("/v0/pipelines/test-2", &json!({})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert_eq!(
        value["runtime_config"],
        serde_json::to_value(RuntimeConfig {
            workers: 100,
            storage: Some(StorageOptions::default()),
            resources: ResourceConfig {
                cpu_cores_min: Some(2.0),
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
    let mut response = config.patch_json("/v0/pipelines/test-2", &body).await;
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
    let config = TestClient::setup_and_full_cleanup().await;

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
                ..Default::default()
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
        let mut response = config.put_json("/v0/pipelines/test-1", &body).await;
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
        let mut response = config.put_json("/v0/pipelines/test-1", &body).await;
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
    let response = config.post_json("/v0/pipelines", &body).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Patching: apply patch which does not affect program_config
    let mut response = config.patch_json("/v0/pipelines/test-2", &json!({})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert_eq!(
        value["program_config"],
        serde_json::to_value(ProgramConfig {
            profile: Some(CompilationProfile::Unoptimized),
            cache: false,
            ..Default::default()
        })
        .unwrap()
    );

    // Patching: apply patch which affects program_config
    let body = json!({
        "program_config": {
            "cache": true
        }
    });
    let mut response = config.patch_json("/v0/pipelines/test-2", &body).await;
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
    let config = TestClient::setup_and_full_cleanup().await;
    let body = json!({
        "name":  "test",
        "description": "desc",
        "runtime_config": {},
        "program_code": "CREATE TABLE foo (bar INTEGER);",
        "program_config": {}
    });
    let response = config.post_json("/v0/pipelines", &body).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    // Try starting the program when it is in the CompilingRust state.
    // There is a possibility that rust compilation is so fast that we don't poll
    // at that moment but that's unlikely.
    let now = Instant::now();
    // println!("Waiting till program compilation state is in past the CompilingSql state...");
    loop {
        sleep(Duration::from_millis(50)).await;
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
    config.start_pipeline("test", true).await;
}

#[actix_web::test]
#[serial]
async fn json_ingress() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Basic test pipeline
    config.prepare_pipeline(
        "test",
        "create table t1(c1 integer, c2 bool, c3 varchar) with ('materialized' = 'true'); create materialized view v1 as select * from t1;",
    )
        .await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    // Push some data using default json config.
    let response = config
        .post(
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
        .post(
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
        .post(
            "/v0/pipelines/test/ingress/T1?format=json&update_format=insert_delete",
            r#"{"insert": [40, true, "buzz"]}"#.to_string(),
        )
        .await;
    assert!(response.status().is_success());

    // Use array of updates instead of newline-delimited JSON
    let response = config
        .post(
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
        .post(
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
        .post(
            "/v0/pipelines/test/ingress/t1?format=json&update_format=insert_delete",
            r#"{"insert": [25, true, ""]}{"delete": [40, "foo", "buzz"]}{"insert": [true, true, ""]}"#.to_string(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.body().await.unwrap();
    let error = std::str::from_utf8(&body).unwrap();
    assert!(error.starts_with(r#"{"message":"Errors parsing input data (2 errors):"#));
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
        .post(
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
        .post(
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
    assert!(error.starts_with(r#"{"message":"Errors parsing input data (1 errors):"#));

    assert_eq!(
        config
            .adhoc_query_json("test", "select * from t1 order by c1, c2, c3;")
            .await,
        json!([{"c1": 15, "c2": true, "c3": "foo"}, {"c1": 16, "c2": false, "c3": "unicode🚲"}, {"c1": 20, "c2": null, "c3": "foo"}, {"c1": 25, "c2": true, "c3": ""}, {"c1": 30, "c2": null, "c3": "bar"}, {"c1": 60, "c2": true, "c3": "hello"}])
    );

    // Stop force and clear the pipeline
    config.stop_force_and_clear_pipeline("test").await;
}

/// Test completion tokens with a pipeline that has no output connectors.
#[actix_web::test]
#[serial]
async fn completion_tokens() {
    let config = TestClient::setup_and_cleanup_pipeline("test").await;

    config.prepare_test_pipeline(
        "create table t1(c1 integer, c2 bool, c3 varchar) with ('materialized' = 'true'); create materialized view v1 as select * from t1;",
    )
        .await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    for i in 0..1000 {
        let token: CompletionTokenResponse = config
            .post(
                "/v0/pipelines/test/ingress/T1?format=json&update_format=raw",
                format!(r#"{{"c1": {i}, "c2": true}}"#),
            )
            .await
            .json()
            .await
            .unwrap();
        // println!("Iteration {i}, token: {}", token.token);
        loop {
            let mut response = config
                .get(&format!(
                    "/v0/pipelines/test/completion_status?token={}",
                    token.token
                ))
                .await;

            assert!(
                response.status().is_success(),
                "Unexpected response to /completion_status: {response:?}"
            );

            let status: CompletionStatusResponse = response.json().await.unwrap();
            if status.status == CompletionStatus::Complete {
                break;
            }
            // println!("status: {:?}", status.status);
            tokio::time::sleep(Duration::from_millis(10)).await
        }

        assert_eq!(
            config
                .adhoc_query_json("test", &format!("select count(*) from t1 where c1 = {i};"))
                .await,
            json!([{"count(*)": 1}])
        );
    }
}

/// Test completion tokens with a pipeline with output connectors.
#[actix_web::test]
#[serial]
async fn completion_tokens_with_outputs() {
    let config = TestClient::setup_and_cleanup_pipeline("test").await;

    let temp_output_path = NamedTempFile::new().unwrap().into_temp_path();
    let output_path1 = temp_output_path.to_str().unwrap().to_string();
    temp_output_path.close().unwrap();
    let temp_output_path = NamedTempFile::new().unwrap().into_temp_path();
    let output_path2 = temp_output_path.to_str().unwrap().to_string();
    temp_output_path.close().unwrap();
    let temp_output_path = NamedTempFile::new().unwrap().into_temp_path();
    let output_path3 = temp_output_path.to_str().unwrap().to_string();
    temp_output_path.close().unwrap();
    let temp_output_path = NamedTempFile::new().unwrap().into_temp_path();
    let output_path4 = temp_output_path.to_str().unwrap().to_string();
    temp_output_path.close().unwrap();

    config
        .prepare_test_pipeline(&format!(
            r#"create table t1(c1 integer, c2 bool, c3 varchar)
with (
    'materialized' = 'true',
    'connectors' = '[{{
        "name": "datagen_connector",
        "paused": true,
        "transport": {{
            "name": "datagen",
            "config": {{"plan": [{{ "limit": 1 }}]}}
        }}
    }}]'
);
create materialized view v1
with (
    'connectors' = '[{{
        "transport": {{
            "name": "file_output",
            "config": {{
                "path": {output_path1:?}
            }}
        }},
        "format": {{
            "name": "json"
        }}
    }},
    {{
        "transport": {{
            "name": "file_output",
            "config": {{
                "path": {output_path2:?}
            }}
        }},
        "format": {{
            "name": "json"
        }}
    }}]'
)
as select * from t1;
create materialized view v2
with (
    'connectors' = '[{{
        "transport": {{
            "name": "file_output",
            "config": {{
                "path": {output_path3:?}
            }}
        }},
        "format": {{
            "name": "json"
        }}
    }},
    {{
        "transport": {{
            "name": "file_output",
            "config": {{
                "path": {output_path4:?}
            }}
        }},
        "format": {{
            "name": "json"
        }}
    }}]'
)
as select * from t1;
"#
        ))
        .await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    let mut expected_output = String::new();

    for i in 0..100 {
        let token: CompletionTokenResponse = config
            .post(
                "/v0/pipelines/test/ingress/T1?format=json&update_format=raw",
                format!(r#"{{"c1": {i}, "c2": true}}"#),
            )
            .await
            .json()
            .await
            .unwrap();
        // println!("Iteration {i}, token: {}", token.token);
        loop {
            let mut response = config
                .get(&format!(
                    "/v0/pipelines/test/completion_status?token={}",
                    token.token
                ))
                .await;

            assert!(
                response.status().is_success(),
                "Unexpected response to /completion_status: {response:?}"
            );

            let status: CompletionStatusResponse = response.json().await.unwrap();
            if status.status == CompletionStatus::Complete {
                break;
            }
            // println!("status: {:?}", status.status);
            tokio::time::sleep(Duration::from_millis(10)).await
        }
        // println!(
        //     "{}: DONE",
        //     chrono::DateTime::<Utc>::from(SystemTime::now()).to_rfc3339()
        // );

        expected_output += &format!(
            r#"{{"insert":{{"c1":{i},"c2":true,"c3":null}}}}
"#
        );

        assert_eq!(
            config
                .adhoc_query_json("test", &format!("select count(*) from t1 where c1 = {i};"))
                .await,
            json!([{"count(*)": 1}])
        );

        // FIXME: This test can run against docker, in which case we cannot inspect the output files.
        // This is why the following lines are commented.

        // let output1 = fs::read_to_string(&output_path1).await.unwrap();
        // let output2 = fs::read_to_string(&output_path2).await.unwrap();
        // let output3 = fs::read_to_string(&output_path3).await.unwrap();
        // let output4 = fs::read_to_string(&output_path4).await.unwrap();

        // assert_eq!(&output1, &expected_output);
        // assert_eq!(&output2, &expected_output);
        // assert_eq!(&output3, &expected_output);
        // assert_eq!(&output4, &expected_output);
    }

    // Feed data from datagen; use the /completion_token endpoint to
    // generate the token.

    assert_eq!(
        config
            .post_no_body(
                "/v0/pipelines/test/tables/t1/connectors/datagen_connector/start".to_string()
            )
            .await
            .status(),
        StatusCode::OK
    );
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let mut response = config
        .get("/v0/pipelines/test/tables/t1/connectors/datagen_connector/completion_token")
        .await;

    assert!(
        response.status().is_success(),
        "Unexpected response to /completion_token: {:?}",
        response.body().await
    );

    let token: CompletionTokenResponse = response.json().await.unwrap();

    // println!("Datagen connector token: {}", token.token);
    loop {
        let mut response = config
            .get(&format!(
                "/v0/pipelines/test/completion_status?token={}",
                token.token
            ))
            .await;

        assert!(
            response.status().is_success(),
            "Unexpected response to /completion_status: {response:?}"
        );

        let status: CompletionStatusResponse = response.json().await.unwrap();
        if status.status == CompletionStatus::Complete {
            break;
        }
        // println!("status: {:?}", status.status);
        tokio::time::sleep(Duration::from_millis(10)).await
    }

    expected_output += r#"{"insert":{"c1":0,"c2":false,"c3":"0"}}
"#;

    assert_eq!(
        config
            .adhoc_query_json("test", "select count(*) from t1 where c1 = 0;")
            .await,
        json!([{"count(*)": 2}])
    );

    // let output1 = fs::read_to_string(&output_path1).await.unwrap();
    // let output2 = fs::read_to_string(&output_path2).await.unwrap();
    // let output3 = fs::read_to_string(&output_path3).await.unwrap();
    // let output4 = fs::read_to_string(&output_path4).await.unwrap();

    // assert_eq!(&output1, &expected_output);
    // assert_eq!(&output2, &expected_output);
    // assert_eq!(&output3, &expected_output);
    // assert_eq!(&output4, &expected_output);
}

/// Table with column of type MAP.
#[actix_web::test]
#[serial]
async fn map_column() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Basic test pipeline
    config.prepare_pipeline(
        "test",
        "create table t1(c1 integer, c2 bool, c3 MAP<varchar, varchar>) with ('materialized' = 'true'); create view v1 as select * from t1;",
    )
        .await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    // Push some data using default json config.
    let response = config
        .post(
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
}

#[actix_web::test]
#[serial]
async fn parse_datetime() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Basic test pipeline
    config
        .prepare_pipeline(
            "test",
            "create table t1(t TIME, ts TIMESTAMP, d DATE) with ('materialized' = 'true');",
        )
        .await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    // The parser should trim leading and trailing white space when parsing
    // dates/times.
    let response = config
        .post(
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
}

#[actix_web::test]
#[serial]
async fn quoted_columns() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Basic test pipeline
    config.prepare_pipeline(
        "test",
        r#"create table t1("c1" integer not null, "C2" bool not null, "😁❤" varchar not null, "αβγ" boolean not null, ΔΘ boolean not null) with ('materialized' = 'true')"#,
    )
        .await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    // Push some data using default json config.
    let response = config
        .post(
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
}

#[actix_web::test]
#[serial]
async fn primary_keys() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Basic test pipeline
    config.prepare_test_pipeline(
        r#"create table t1(id bigint not null, s varchar not null, primary key (id)) with ('materialized' = 'true')"#,
    )
        .await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    // Push some data using default json config.
    let response = config
        .post(
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
        .post(
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
        .post(
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
}

/// Test case-sensitive table ingress/egress behavior.
#[actix_web::test]
#[serial]
async fn case_sensitive_tables() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Table "TaBle1" and view "V1" are case-sensitive and can only be accessed
    // by quoting their name.
    // Table "v1" is also case-sensitive, but since its name is lowercase, it
    // can be accessed as both "v1" and "\"v1\""
    config
        .prepare_test_pipeline(
            r#"create table "TaBle1"(id bigint not null);
create table table1(id bigint);
create materialized view "V1" as select * from "TaBle1";
create materialized view "v1" as select * from table1;"#,
        )
        .await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    let mut response1 = config.delta_stream_request_json("test", "\"V1\"").await;
    let mut response2 = config.delta_stream_request_json("test", "\"v1\"").await;

    // Push some data using default json config.
    let response = config
        .post(
            "/v0/pipelines/test/ingress/\"TaBle1\"?format=json&update_format=insert_delete",
            r#"{"insert":{"id":1}}"#.to_string(),
        )
        .await;
    assert!(response.status().is_success());

    let response = config
        .post(
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
}

#[actix_web::test]
#[serial]
async fn duplicate_outputs() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Basic test pipeline
    config.prepare_test_pipeline(
        r#"create table t1(id bigint not null, s varchar not null); create view v1 as select s from t1;"#,
    )
        .await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    let mut response = config.delta_stream_request_json("test", "V1").await;

    // Push some data using default json config.
    let response2 = config
        .post(
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
        .post(
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
        .post(
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
}

#[actix_web::test]
#[serial]
async fn upsert() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Basic test pipeline
    config
        .prepare_test_pipeline(
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

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    let mut response = config.delta_stream_request_json("test", "T1").await;

    // Push some data.
    //
    // NOTE: we use `array=true` to push data in this test to make sure that all updates are
    // delivered atomically and all outputs are produced in a single chunk. It is still
    // theoretically possible that inputs are split across multiple `step`'s due to the
    // `ZSetHandle::append` method not being atomic.  This is highly improbable, but if it
    // happens, increasing buffering delay in DBSP should solve that.
    let response2 = config
        .post(
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
        .post(
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
        .post(
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
}

#[actix_web::test]
#[serial]
async fn pipeline_name_invalid() {
    let config = TestClient::setup_and_full_cleanup().await;
    // Empty
    let request_body = json!({
        "name": "",
        "description": "", "runtime_config": {}, "program_code": "", "program_config": {}
    });
    let response = config.post_json("/v0/pipelines", &request_body).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Too long
    let request_body = json!({
        "name": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "description": "", "runtime_config": {}, "program_code": "", "program_config": {}
    });
    let response = config.post_json("/v0/pipelines", &request_body).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Invalid characters
    let request_body = json!({
        "name": "%abc",
        "description": "", "runtime_config": {}, "program_code": "", "program_config": {}
    });
    let response = config.post_json("/v0/pipelines", &request_body).await;
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

    let config = TestClient::setup_and_full_cleanup().await;
    config.prepare_test_pipeline(PROGRAM).await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

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
    let config = TestClient::setup_and_full_cleanup().await;
    config.prepare_test_pipeline(PROGRAM).await;
    config.resume_pipeline("test", true).await;

    const ADHOC_SQL_EMPTY: &str = "SELECT COUNT(*) from \"TaBle1\"";
    let mut r = config.adhoc_query("test", ADHOC_SQL_EMPTY, "json").await;
    assert_eq!(r.status(), StatusCode::OK);
    let cnt_body = r.body().await.unwrap();
    let cnt_ret = std::str::from_utf8(cnt_body.as_ref()).unwrap();
    let cnt_ret_json = serde_json::from_str::<Value>(cnt_ret).unwrap();
    assert_eq!(cnt_ret_json, json!({"count(*)": 0}));
}

/// The pipeline should transition to Stopped status when being stopped after starting.
/// This test will take at least 20 seconds due to various waiting times after starting.
#[actix_web::test]
#[serial]
async fn pipeline_stop_force_after_start() {
    let config = TestClient::setup_and_full_cleanup().await;
    config
        .prepare_test_pipeline("CREATE TABLE t1(c1 INTEGER);")
        .await;
    config.stop_force_and_clear_pipeline("test").await;

    // Test a variety of waiting before stopping to capture the various states
    for duration_ms in [
        0, 50, 100, 250, 500, 750, 1000, 1250, 1500, 1750, 2000, 5000, 10000,
    ] {
        // Start pipeline in paused state
        let response = config
            .post_no_body("/v0/pipelines/test/start?initial=paused")
            .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // Shortly wait for the pipeline to transition to next state(s)
        sleep(Duration::from_millis(duration_ms)).await;

        // Stop force and clear the pipeline
        config.stop_force_and_clear_pipeline("test").await;
    }
}

#[actix_web::test]
#[serial]
async fn test_get_metrics() {
    let config = TestClient::setup_and_full_cleanup().await;
    // Basic test pipeline with a SQL program which is known to panic
    config.prepare_test_pipeline(
        "CREATE TABLE t1(c1 INTEGER) with ('materialized' = 'true'); CREATE VIEW v1 AS SELECT * FROM t1;",
    ).await;

    // Again pause the pipeline before it is started
    config.pause_pipeline("test", true).await;

    // Resume the pipeline
    config.resume_pipeline("test", true).await;

    // Push some data
    let response = config
        .post("/v0/pipelines/test/ingress/t1", "1\n2\n3\n".to_string())
        .await;
    assert!(response.status().is_success());

    let mut response = config.get("/v0/metrics").await;

    let resp = response.body().await.unwrap_or_default();
    let s = String::from_utf8(resp.to_vec()).unwrap_or_default();

    assert!(s.contains("# TYPE"));
}

/// Tests that logs can be retrieved from the pipeline in any status.
#[actix_web::test]
#[serial]
async fn pipeline_logs() {
    let config = TestClient::setup_and_full_cleanup().await;

    assert_eq!(
        config.get("/v0/pipelines/test/logs").await.status(),
        StatusCode::NOT_FOUND
    );
    config
        .post_pipeline(&json!({
            "name": "test",
            "program_code": "CREATE TABLE t1(c1 INTEGER) with ('materialized' = 'true');"
        }))
        .await;

    // Logs should eventually become available
    let mut i = 0;
    loop {
        let status = config.get("/v0/pipelines/test/logs").await.status();
        if status == StatusCode::OK {
            break;
        } else if status == StatusCode::NOT_FOUND {
            if i >= 50 {
                panic!("Took too long for the logs to become available (still {status})")
            }
        } else {
            panic!("Unexpected status (not OK or NOT_FOUND): {status}");
        }
        sleep(Duration::from_millis(100)).await;
        i += 1;
    }

    config.wait_for_compiled_program("test", 1).await;
    config.start_paused_pipeline("test", true).await;
    assert_eq!(
        config.get("/v0/pipelines/test/logs").await.status(),
        StatusCode::OK
    );
    config.resume_pipeline("test", true).await;
    assert_eq!(
        config.get("/v0/pipelines/test/logs").await.status(),
        StatusCode::OK
    );
    config.stop_force_pipeline("test", true).await;
    assert_eq!(
        config.get("/v0/pipelines/test/logs").await.status(),
        StatusCode::OK
    );
    config.clear_pipeline("test", true).await;
    assert_eq!(
        config.get("/v0/pipelines/test/logs").await.status(),
        StatusCode::OK
    );
    config.delete_pipeline("test").await;

    // Logs should eventually become unavailable
    let mut i = 0;
    loop {
        let status = config.get("/v0/pipelines/test/logs").await.status();
        if status == StatusCode::OK {
            if i >= 50 {
                panic!("Took too long for the logs to become unavailable (still {status})")
            }
        } else if status == StatusCode::NOT_FOUND {
            break;
        } else {
            panic!("Unexpected status (not OK or NOT_FOUND): {status}");
        }
        sleep(Duration::from_millis(100)).await;
        i += 1;
    }
}

/// The compiler needs to handle and continue to function when the pipeline is deleted
/// during program compilation.
#[actix_web::test]
#[serial]
async fn pipeline_deleted_during_program_compilation() {
    let config = TestClient::setup_and_full_cleanup().await;

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
        let response = config.post_json("/v0/pipelines", &request_body).await;
        assert_eq!(response.status(), StatusCode::CREATED);

        // Wait for compilation to progress
        sleep(Duration::from_millis(duration_ms)).await;

        // Delete the pipeline
        let response = config.delete("/v0/pipelines/test-pddpc").await;
        assert_eq!(StatusCode::OK, response.status());
    }

    // Validate the compiler still works correctly by fully compiling a program
    config.prepare_test_pipeline("").await;
}

/// Retrieves whether pipeline is paused, connector is paused, and number of processed
/// records, which is used by the basic orchestration test.
async fn basic_orchestration_info(
    config: &TestClient,
    pipeline_name: &str,
    table_name: &str,
    connector_name: &str,
) -> (bool, bool, u64) {
    let stats = config.stats_json(pipeline_name).await;
    let pipeline_paused = stats["global_metrics"]["state"].as_str() == Some("Paused");
    let num_processed = stats["global_metrics"]["total_processed_records"]
        .as_u64()
        .unwrap();

    let connector_paused =
        connector_paused(config, pipeline_name, table_name, connector_name).await;
    (pipeline_paused, connector_paused, num_processed)
}

/// Check if a connector is in the PAUSED state.
async fn connector_paused(
    config: &TestClient,
    pipeline_name: &str,
    table_name: &str,
    connector_name: &str,
) -> bool {
    config
        .input_connector_stats_json(pipeline_name, table_name, connector_name)
        .await["paused"]
        .as_bool()
        .unwrap()
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
        // One table with one connector
        let config = TestClient::setup_and_full_cleanup().await;
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
        config.prepare_test_pipeline(&sql).await;

        // Pipeline is paused, connector is running
        sleep(Duration::from_millis(500)).await;
        let (pipeline_paused, connector_paused, num_processed) =
            basic_orchestration_info(&config, "test", table_name, connector_name).await;
        assert!(pipeline_paused);
        assert!(!connector_paused);
        assert_eq!(num_processed, 0);

        // Pause the connector
        config
            .connector_action("test", table_name, connector_name, "pause")
            .await;

        // Pipeline is paused, connector is paused
        sleep(Duration::from_millis(500)).await;
        let (pipeline_paused, connector_paused, num_processed) =
            basic_orchestration_info(&config, "test", table_name, connector_name).await;
        assert!(pipeline_paused);
        assert!(connector_paused);
        assert_eq!(num_processed, 0);

        // Resume the pipeline
        let response = config.post_no_body("/v0/pipelines/test/resume").await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        config
            .wait_for_pipeline_status(
                "test",
                CombinedStatus::Running,
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
        config
            .connector_action("test", table_name, connector_name, "start")
            .await;

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
    let config = TestClient::setup_and_full_cleanup().await;
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
    config.prepare_test_pipeline(sql).await;

    // ACCEPTED
    for endpoint in ["/v0/pipelines/test/resume", "/v0/pipelines/test/pause"] {
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
        "/v0/pipelines/test/action2", // Invalid pipeline action
        "/v0/pipelines/test/Start",   // Invalid pipeline action (case-sensitive)
        "/v0/pipelines/test2/start",  // Pipeline not found
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
    StartPausedPipeline,
    PausePipeline,
    StartConnector(u32),
    PauseConnector(u32),
}

/// Tests for orchestration that the effects (i.e., pipeline and connector state) are
/// indeed as expected after each scenario consisting of various start and pause steps.
#[actix_web::test]
#[serial]
async fn pipeline_orchestration_scenarios() {
    let config = TestClient::setup_and_full_cleanup().await;

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
    config.prepare_test_pipeline(sql).await;

    // Stop for the first scenario
    config.stop_force_pipeline("test", true).await;

    // Various scenarios (steps) and the expected outcome (what is paused)
    for (steps, expected_pipeline_paused, expected_c1_paused, expected_c2_paused) in [
        // All four combination when the pipeline is paused
        (
            vec![OrchestrationTestStep::StartPausedPipeline],
            true,
            false,
            false,
        ),
        (
            vec![
                OrchestrationTestStep::StartPausedPipeline,
                OrchestrationTestStep::PauseConnector(1),
            ],
            true,
            true,
            false,
        ),
        (
            vec![
                OrchestrationTestStep::StartPausedPipeline,
                OrchestrationTestStep::PauseConnector(2),
            ],
            true,
            false,
            true,
        ),
        (
            vec![
                OrchestrationTestStep::StartPausedPipeline,
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
                    config.start_pipeline("test", true).await;
                }
                OrchestrationTestStep::StartPausedPipeline => {
                    config.start_paused_pipeline("test", true).await;
                }
                OrchestrationTestStep::PausePipeline => {
                    config.pause_pipeline("test", true).await;
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

        // Stop force and clear for the next scenario
        config.stop_force_and_clear_pipeline("test").await;
    }
}

/// Checkpoint should return NOT_IMPLEMENTED status if `feldera-enterprise` feature is not set.
#[actix_web::test]
#[serial]
async fn checkpoint() {
    let config = TestClient::setup_and_full_cleanup().await;
    config
        .prepare_test_pipeline("create table t(x int) with ('materialized' = 'true');")
        .await;

    #[cfg(not(feature = "feldera-enterprise"))]
    {
        let mut response = config.post_no_body("/v0/pipelines/test/checkpoint").await;
        let value: Value = response.json().await.unwrap();
        assert_ne!(value["error_code"], json!("InvalidPipelineAction"));

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(value["error_code"], json!("EnterpriseFeature"));
    }

    #[cfg(feature = "feldera-enterprise")]
    {
        for _ in 0..10 {
            let mut response = config.post_no_body("/v0/pipelines/test/checkpoint").await;
            assert_ne!(response.status(), StatusCode::NOT_IMPLEMENTED);

            assert!(response.status().is_success());

            let resp: CheckpointResponse = response.json().await.unwrap();
            let sequence_number = resp.checkpoint_sequence_number;

            let start = Instant::now();
            loop {
                let mut response = config.get("/v0/pipelines/test/checkpoint_status").await;
                assert!(response.status().is_success());
                let checkpoint_status = response.json::<CheckpointStatus>().await.unwrap();
                if checkpoint_status.success == Some(sequence_number) {
                    // println!("Checkpoint completed successfully.");
                    break;
                }

                if start.elapsed() > Duration::from_secs(10) {
                    panic!("Timeout waiting for checkpoint to complete. Current checkpoint status: {checkpoint_status:?}");
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
        config.stop_force_and_clear_pipeline("test").await;
    }
}

/// Incrementing of `refresh_version`.
#[actix_web::test]
#[serial]
async fn refresh_version() {
    let client = TestClient::setup_and_full_cleanup().await;

    // Initial refresh version should be 1
    let value = client
        .post_pipeline(&json!({
            "name": "test",
            "program_code": "",
        }))
        .await;
    assert_eq!(value["refresh_version"], json!(1));

    // After compilation, refresh version should be 3
    client.wait_for_compiled_program("test", 1).await;
    let value = client.get_pipeline("test").await;
    assert_eq!(value["refresh_version"], json!(3));

    // Starting and shutting down should have no effect on the refresh version
    client.start_pipeline("test", true).await;
    client.stop_force_and_clear_pipeline("test").await;
    let value = client.get_pipeline("test").await;
    assert_eq!(value["refresh_version"], json!(3));

    // Edits should have an impact, incrementing refresh version to 4
    let value = client
        .patch_pipeline(
            "test",
            &json!({ "program_code": "CREATE TABLE t1 ( v1 INT );" }),
        )
        .await;
    assert_eq!(value["refresh_version"], json!(4));

    // After compilation, refresh version should be 6
    client.wait_for_compiled_program("test", 2).await;
    let value = client.get_pipeline("test").await;
    assert_eq!(value["refresh_version"], json!(6));
}

/// Tests that circuit metrics can be retrieved from the pipeline.
#[actix_web::test]
#[serial]
async fn pipeline_metrics() {
    let client = TestClient::setup_and_full_cleanup().await;
    client.prepare_test_pipeline("").await;

    // Retrieve metrics in default format
    let mut response = client.get("/v0/pipelines/test/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.body().await.unwrap();
    let metrics_default = std::str::from_utf8(&body).unwrap();

    // Retrieve metrics in Prometheus format
    let mut response = client
        .get("/v0/pipelines/test/metrics?format=prometheus")
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.body().await.unwrap();
    let metrics_prometheus = std::str::from_utf8(&body).unwrap();

    // Retrieve metrics in JSON format
    let mut response = client.get("/v0/pipelines/test/metrics?format=json").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.body().await.unwrap();
    let metrics_json = std::str::from_utf8(&body).unwrap();
    let _metrics_json_value: Value = serde_json::from_str(metrics_json).unwrap();

    // Unknown format
    let response = client
        .get("/v0/pipelines/test/metrics?format=does-not-exist")
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Minimally check formats
    assert!(metrics_default.contains("# TYPE records_processed_total counter"));
    assert!(metrics_prometheus.contains("# TYPE records_processed_total counter"));
    assert!(metrics_json.contains("\"key\":\"records_processed_total\""));
}

/// Tests retrieving pipeline statistics via `/stats`.
#[actix_web::test]
#[serial]
async fn pipeline_stats() {
    let client = TestClient::setup_and_full_cleanup().await;

    // Basic test pipeline
    client
        .prepare_test_pipeline(
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

    // Resume the pipeline
    client.resume_pipeline("test", true).await;

    // Create output connector
    let response = client.post_no_body("/v0/pipelines/test/egress/v1").await;
    assert_eq!(response.status(), StatusCode::OK);

    // Give it some seconds to process
    sleep(Duration::from_secs(4)).await;

    // Check the output of `/stats`
    let mut response = client.get("/v0/pipelines/test/stats").await;
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
    assert_eq!(value["global_metrics"]["buffered_input_bytes"], json!(0));
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
            "buffered_bytes": 0,
            "buffered_records": 0,
            "end_of_input": true,
            "num_parse_errors": 0,
            "num_transport_errors": 0,
            "total_bytes": 40,
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

    // Check the output of `/time_series`
    let mut response = client.get("/v0/pipelines/test/time_series").await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: TimeSeries = response.json().await.unwrap();
    assert!(
        value.samples.len() > 1,
        "should have at least 2 samples (not {})",
        value.samples.len()
    );
    let last_sample = value.samples.last().unwrap();
    assert_eq!(last_sample.total_processed_records, 5);
}

/// Test suspend and resume functionality.
#[actix_web::test]
#[serial]
async fn suspend() {
    #[cfg(not(feature = "feldera-enterprise"))]
    {
        let config = TestClient::setup_and_full_cleanup().await;
        config
            .prepare_test_pipeline("create table t(x int) with ('materialized' = 'true');")
            .await;

        let mut response = config
            .post_no_body("/v0/pipelines/test/stop?force=false")
            .await;
        let value: Value = response.json().await.unwrap();

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(value["error_code"], json!("EnterpriseFeature"));
    }

    #[cfg(feature = "feldera-enterprise")]
    {
        enterprise_suspend().await;
    }
}

/// Test suspend and resume functionality.
///
/// * Start with three input connectors, all paused.
/// * Suspend and resume the pipeline; unpause connector1
/// * Suspend and resume the pipeline; unpause connector2, which automatically unpauses connector1.
///
/// Check for expected outputs after each step.
#[cfg(feature = "feldera-enterprise")]
async fn enterprise_suspend() {
    let config = TestClient::setup_and_full_cleanup().await;
    let pipeline_name = "test";

    config
        .prepare_test_pipeline(
            r#"create table t1 (
    x int
) with (
    'materialized' = 'true',
    'connectors' = '[{
        "name": "c1",
        "paused": true,
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "limit": 1,
                    "fields": {
                        "x": { "values": [1] }
                    }
                }]
            }
        }
    },
    {
        "name": "c2",
        "paused": true,
        "labels": ["label1"],
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "limit": 3,
                    "fields": {
                        "x": { "values": [2,3,4] }
                    }
                }]
            }
        }
    },
    {
        "name": "c3",
        "paused": true,
        "start_after": ["label1"],
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "limit": 5,
                    "fields": {
                        "x": { "values": [5,6,7,8,9] }
                    }
                }]
            }
        }
    }]'
);"#,
        )
        .await;

    // Suspend and resume:
    // Expected connector state: paused, paused, paused.
    config.stop_pipeline(pipeline_name, true).await;
    config.start_pipeline(pipeline_name, true).await;

    assert!(connector_paused(&config, pipeline_name, "t1", "c1").await);
    assert!(connector_paused(&config, pipeline_name, "t1", "c2").await);
    assert!(connector_paused(&config, pipeline_name, "t1", "c3").await);

    // Unpause connector1.
    config
        .connector_action(pipeline_name, "t1", "c1", "start")
        .await;

    // Receive data from connector1.
    wait_for_condition(
        "1 records from connector 1",
        || async {
            config
                .adhoc_query_json(pipeline_name, "select count(*) from t1")
                .await
                == json!([{"count(*)": 1}])
        },
        Duration::from_secs(10),
    )
    .await;

    // Suspend and resume:
    // Expected connector state: running (eoi), paused, paused.
    config.stop_pipeline(pipeline_name, true).await;
    config.start_pipeline(pipeline_name, true).await;

    assert!(!connector_paused(&config, pipeline_name, "t1", "c1").await);
    assert!(connector_paused(&config, pipeline_name, "t1", "c2").await);
    assert!(connector_paused(&config, pipeline_name, "t1", "c3").await);

    // Unpause connector2.
    config
        .connector_action(pipeline_name, "t1", "c2", "start")
        .await;

    // Receive data from connectors 2 and 3.
    // Receive data from connector1.
    wait_for_condition(
        "1 records from connector 1",
        || async {
            config
                .adhoc_query_json(pipeline_name, "select count(*) from t1")
                .await
                == json!([{"count(*)": 9}])
        },
        Duration::from_secs(10),
    )
    .await;

    // Suspend and resume:
    config.stop_pipeline(pipeline_name, true).await;
    config.start_pipeline(pipeline_name, true).await;

    // Expected connector state: running (eoi), running (eoi), running (eoi).
    assert!(!connector_paused(&config, pipeline_name, "t1", "c1").await);
    assert!(!connector_paused(&config, pipeline_name, "t1", "c2").await);
    assert!(!connector_paused(&config, pipeline_name, "t1", "c3").await);

    // Sleep for 5 seconds to allow data to be received;
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check that no more data was received.
    assert_eq!(
        config
            .adhoc_query_json(pipeline_name, "select count(*) from t1")
            .await,
        json!([{"count(*)": 9}])
    );
}

/// Tests that stopping pipelines with force.
#[actix_web::test]
#[serial]
async fn pipeline_stop_with_force() {
    let config = TestClient::setup_and_full_cleanup().await;

    // New pipeline
    config
        .post_pipeline(&json!({ "name": "test", "program_code": ""}))
        .await;

    // Already stopped
    config.stop_force_pipeline("test", true).await;

    // Start without waiting and immediately stop forcefully
    config.start_pipeline("test", false).await;
    config.stop_force_pipeline("test", true).await;

    // Start with waiting and stop forcefully once Running
    config.start_pipeline("test", true).await;
    config.stop_force_pipeline("test", true).await;

    // Start paused with waiting and stop forcefully once Paused
    config.start_paused_pipeline("test", true).await;
    config.stop_force_pipeline("test", true).await;

    // Start with waiting and stop forcefully twice in a row
    config.start_pipeline("test", true).await;
    config.stop_force_pipeline("test", false).await;
    config.stop_force_pipeline("test", true).await;
}

/// Tests stopping pipelines without force.
#[actix_web::test]
#[serial]
#[cfg(feature = "feldera-enterprise")]
async fn pipeline_stop_without_force() {
    let config = TestClient::setup_and_full_cleanup().await;

    // New pipeline
    config
        .post_pipeline(&json!({ "name": "test", "program_code": ""}))
        .await;

    // Already stopped
    config.stop_pipeline("test", true).await;

    // Start without waiting and immediately stop
    config.start_pipeline("test", false).await;
    config.stop_pipeline("test", true).await;

    // Start with waiting and stop once Running
    config.start_pipeline("test", true).await;
    config.stop_pipeline("test", true).await;

    // Start paused with waiting and stop once Paused
    config.start_paused_pipeline("test", true).await;
    config.stop_pipeline("test", true).await;

    // Start with waiting and stop twice in a row
    config.start_pipeline("test", true).await;
    config.stop_pipeline("test", false).await;
    // TODO: enable again once setting suspended is only setting desired status rather than
    //       synchronously performing the operation
    // config.stop_pipeline("test", true).await;
}

/// Tests clearing storage of pipelines.
#[actix_web::test]
#[serial]
async fn pipeline_clear() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Initially created: cleared
    config
        .post_pipeline(&json!({ "name": "test", "program_code": ""}))
        .await;
    assert_eq!(
        config.get_pipeline("test").await["storage_status"],
        "Cleared"
    );

    // Calling /clear does not have an effect
    config.clear_pipeline("test", true).await;

    // Start: now it is in use
    config.start_pipeline("test", true).await;
    assert_eq!(config.get_pipeline("test").await["storage_status"], "InUse");

    // While running, clear is not possible
    assert_eq!(
        config
            .post_no_body("/v0/pipelines/test/clear")
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );

    // After forceful stop, it should still be InUse
    config.stop_force_pipeline("test", true).await;
    assert_eq!(config.get_pipeline("test").await["storage_status"], "InUse");

    // Start again (now paused): it remains InUse
    config.start_paused_pipeline("test", true).await;
    assert_eq!(config.get_pipeline("test").await["storage_status"], "InUse");

    // While paused, clear is not possible
    assert_eq!(
        config
            .post_no_body("/v0/pipelines/test/clear")
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );

    // After again forceful stop, it should still be InUse
    config.stop_force_pipeline("test", true).await;
    assert_eq!(config.get_pipeline("test").await["storage_status"], "InUse");

    // Calling /clear will make it Cleared.
    // Perform it twice such that we might also test calling it during the Clearing state.
    config.clear_pipeline("test", false).await;
    config.clear_pipeline("test", true).await;
    assert_eq!(
        config.get_pipeline("test").await["storage_status"],
        "Cleared"
    );
}

/// Tests the connector endpoint naming is based on either the given or generated
/// `unnamed-{index}` connector name.
#[actix_web::test]
#[serial]
async fn pipeline_connector_endpoint_naming() {
    let config = TestClient::setup_and_full_cleanup().await;

    // Create pipeline
    let sql = r#"
        CREATE TABLE t1 (i1 BIGINT) WITH (
            'connectors' = '[
                { "name": "abc", "transport": { "name": "datagen", "config": {} } },
                { "transport": { "name": "datagen", "config": {} } },
                { "name": "def", "transport": { "name": "datagen", "config": {} } },
                { "transport": { "name": "datagen", "config": {} } }
            ]'
        );

        CREATE TABLE t2 (i1 BIGINT) WITH (
            'connectors' = '[
                { "name": "c1", "transport": { "name": "datagen", "config": {} } }
            ]'
        );

        CREATE TABLE t3 (i1 BIGINT) WITH (
            'connectors' = '[
                { "transport": { "name": "datagen", "config": {} } }
            ]'
        );

        CREATE MATERIALIZED VIEW v1 WITH (
            'connectors' = '[
                { "transport": { "name": "kafka_output", "config": { "topic": "p1" } } },
                { "name": "c1", "transport": { "name": "kafka_output", "config": { "topic": "p1" } } },
                { "transport": { "name": "kafka_output", "config": { "topic": "p1" } } },
                { "name": "c3", "transport": { "name": "kafka_output", "config": { "topic": "p1" } } }
            ]'
        ) AS ( SELECT  * FROM t1 );

        CREATE MATERIALIZED VIEW v2 WITH (
            'connectors' = '[
                { "name": "c1", "transport": { "name": "kafka_output", "config": { "topic": "p1" } } }
            ]'
        ) AS ( SELECT  * FROM t2 );

        CREATE MATERIALIZED VIEW v3 WITH (
            'connectors' = '[
                { "transport": { "name": "kafka_output", "config": { "topic": "p1" } } }
            ]'
        ) AS ( SELECT  * FROM t3 );
    "#;
    let (is_created, _) = config
        .put_pipeline(
            "test",
            &json!({
                "name": "test",
                "program_code": sql,
            }),
        )
        .await;
    assert!(is_created);
    config.wait_for_compiled_program("test", 1).await;

    // Check the input_connectors and output_connectors of `program_info`
    let value = config.get_pipeline("test").await;
    let mut input_names: Vec<String> = value["program_info"]["input_connectors"]
        .clone()
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    input_names.sort();
    assert_eq!(
        input_names,
        vec![
            "t1.abc",
            "t1.def",
            "t1.unnamed-1",
            "t1.unnamed-3",
            "t2.c1",
            "t3.unnamed-0",
        ]
    );
    let mut output_names: Vec<String> = value["program_info"]["output_connectors"]
        .clone()
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    output_names.sort();
    assert_eq!(
        output_names,
        vec![
            "v1.c1",
            "v1.c3",
            "v1.unnamed-0",
            "v1.unnamed-2",
            "v2.c1",
            "v3.unnamed-0",
        ]
    );
}

/// Tests the health check endpoint.
#[actix_web::test]
#[serial]
async fn health_check() {
    let config = TestClient::setup_and_full_cleanup().await;
    let mut attempt = 0;
    loop {
        let (status_code, body) =
            TestClient::parse_json_response(config.get("/healthz").await).await;
        if status_code == StatusCode::OK
            && body
                == json!({
                    "status": "healthy"
                })
        {
            // Healthy
            return;
        } else if status_code == StatusCode::INTERNAL_SERVER_ERROR
            && body
                == json!({
                    "status": "unhealthy: unable to reach database (see logs for further details)"
                })
        {
            // Unhealthy
            if attempt >= 30 {
                panic!("Took too long for health check to return healthy")
            }
        } else {
            // Unexpected response
            panic!("Unexpected health check status code ({status_code}) and/or body ({body})");
        }
        attempt += 1;
        sleep(Duration::from_secs(1)).await;
    }
}
