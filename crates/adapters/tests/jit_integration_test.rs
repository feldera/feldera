//! Integration tests for JIT-compiled circuits.
//!
//! Test cases in this module run the SQL compiler to generate program IR +
//! schema, start a pipeline server using this IR and feed some test data to it.

use reqwest::{blocking::Client, StatusCode};
use serde_json::json;
use serial_test::serial;
use std::{
    fs::{self},
    io::Error as IoError,
    path::Path,
    process::{Command, ExitStatus},
    thread::{self, sleep, JoinHandle},
    time::{Duration, Instant},
};
use test_bin::get_test_bin;

static PIPELINE_PORT: &str = "8088";
static TIMEOUT: Duration = Duration::from_millis(10_000);

fn endpoint(ep: &str) -> String {
    format!("http://localhost:{PIPELINE_PORT}/{ep}")
}

/// Start a pipeline server using `project.sql` and `config.yaml`
/// files in the `test_dir` directory.
fn start_pipeline(test_dir: &str) -> JoinHandle<Result<ExitStatus, IoError>> {
    println!("Running test: {:?}", test_dir);
    let ir_file = fs::File::create(Path::new(test_dir).join("ir.json")).unwrap();
    assert!(
        Command::new("../../../../../sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp")
            .current_dir(test_dir)
            .args([
                "-js",
                "schema.json",
                "-i",
                "-j",
                "--alltables",
                "project.sql",
            ])
            .stdout(ir_file)
            .output()
            .unwrap()
            .status
            .success()
    );

    let test_dir = test_dir.to_string();

    // Start the server it will run until we invoke the `/shutdown` endpoint.
    let server_thread = thread::spawn(move || {
        get_test_bin("pipeline")
            .current_dir(test_dir)
            .args([
                "--ir",
                "ir.json",
                "--schema",
                "schema.json",
                "--config-file",
                "config.yaml",
                "--default-port",
                PIPELINE_PORT,
            ])
            .spawn()
            .unwrap()
            .wait()
    });

    let client = Client::new();
    let start = Instant::now();
    loop {
        let response = client.get(endpoint("stats")).send();
        match response {
            Ok(response) => {
                if response.status().is_success() {
                    break;
                } else if response.status() == StatusCode::SERVICE_UNAVAILABLE {
                    println!(
                        "Waiting for the pipeline to initialize: {}",
                        response.status()
                    );
                } else {
                    panic!("Unexpected HTTP response: {response:?}");
                }
            }
            Err(e) => {
                println!("Waiting for the server: {e}");
            }
        }

        if start.elapsed() > TIMEOUT {
            panic!("Timeout waiting for the pipeline to initialize");
        }
        sleep(Duration::from_millis(100));
    }

    println!("Pipeline is running");
    assert!(client
        .get(endpoint("start"))
        .send()
        .unwrap()
        .status()
        .is_success());

    server_thread
}

// Server thread handle that shuts down the server on drop.
// Should make sure the server doesn't get stuck in memory
// on test failure.
struct ServerThread {
    handle: Option<JoinHandle<Result<ExitStatus, IoError>>>,
}

impl ServerThread {
    fn new(handle: JoinHandle<Result<ExitStatus, IoError>>) -> Self {
        Self {
            handle: Some(handle),
        }
    }
    fn shutdown(mut self) {
        let _ = Client::new().get(endpoint("shutdown")).send();
        self.handle.take().unwrap().join().unwrap().unwrap();
    }
}

impl Drop for ServerThread {
    fn drop(&mut self) {
        let _ = Client::new().get(endpoint("shutdown")).send();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[test]
#[serial]
fn supply_chain_test() {
    let server_thread = ServerThread::new(start_pipeline("tests/sql_tests/supply_chain"));

    let client = Client::new();
    assert!(client
        .post(endpoint("ingress/PART?format=json"))
        .body(
            r#"{"insert": {"ID": 1, "NAME": "Flux Capacitor"}}
{"insert": {"ID": 2, "NAME": "Warp Core"}}
{"insert": {"ID": 3, "NAME": "Kyber Crystal"}}"#
        )
        .send()
        .unwrap()
        .status()
        .is_success());

    assert!(client
        .post(endpoint("ingress/VENDOR?format=json"))
        .body(
            r#"{"insert": {"ID": 1, "NAME": "Gravitech Dynamics", "ADDRESS": "222 Graviton Lane"}}
{"insert": {"ID": 2, "NAME": "HyperDrive Innovations", "ADDRESS": "456 Warp Way"}}
{"insert": {"ID": 3, "NAME": "DarkMatter Devices", "ADDRESS": "333 Singularity Street"}}"#
        )
        .send()
        .unwrap()
        .status()
        .is_success());

    assert!(client
        .post(endpoint("ingress/PRICE?format=json"))
        .body(
            r#"{"insert": {"PART": 1, "VENDOR": 2, "PRICE": 10000}}
{"insert": {"PART": 2, "VENDOR": 1, "PRICE": 15000}}
{"insert": {"PART": 3, "VENDOR": 3, "PRICE": 9000}}"#
        )
        .send()
        .unwrap()
        .status()
        .is_success());

    sleep(Duration::from_millis(2_000));

    let expected_output = vec![
        r#"{"insert":{"PART_ID":1,"PART_NAME":"Flux Capacitor","VENDOR_ID":2,"VENDOR_NAME":"HyperDrive Innovations","PRICE":10000}}"#.to_string(),
        r#"{"insert":{"PART_ID":2,"PART_NAME":"Warp Core","VENDOR_ID":1,"VENDOR_NAME":"Gravitech Dynamics","PRICE":15000}}"#.to_string(),
        r#"{"insert":{"PART_ID":3,"PART_NAME":"Kyber Crystal","VENDOR_ID":3,"VENDOR_NAME":"DarkMatter Devices","PRICE":9000}}"#.to_string(),
    ];

    // The multithreaded circuit can produce outputs in non-deterministic order.
    let output = fs::read_to_string("tests/sql_tests/supply_chain/preferred_vendor.json").unwrap();
    let mut lines = output.lines().collect::<Vec<_>>();
    lines.sort();

    assert_eq!(expected_output, lines);

    // Test primary key behavior: delete
    assert!(client
        .post(endpoint("ingress/PART?format=json"))
        .body(
            r#"{"delete": {"ID": 1}}
{"insert": {"ID": 4, "NAME": "Part 4"}}"#
        )
        .send()
        .unwrap()
        .status()
        .is_success());

    // Test primary key behavior: upsert
    assert!(client
            .post(endpoint("ingress/VENDOR?format=json"))
            .body(
                r#"{"insert": {"ID": 1, "NAME": "Gravitech Dynamics-2", "ADDRESS": "222 Graviton Lane-2"}}"#
            )
            .send()
            .unwrap()
            .status()
            .is_success());

    sleep(Duration::from_millis(2_000));

    let expected_output: Vec<String> = vec![
        r#"{"delete":{"PART_ID":1,"PART_NAME":"Flux Capacitor","VENDOR_ID":2,"VENDOR_NAME":"HyperDrive Innovations","PRICE":10000}}"#.to_string(),
        r#"{"delete":{"PART_ID":2,"PART_NAME":"Warp Core","VENDOR_ID":1,"VENDOR_NAME":"Gravitech Dynamics","PRICE":15000}}"#.to_string(),
        r#"{"insert":{"PART_ID":2,"PART_NAME":"Warp Core","VENDOR_ID":1,"VENDOR_NAME":"Gravitech Dynamics-2","PRICE":15000}}"#.to_string(),
    ];

    // The multithreaded circuit can produce outputs in non-deterministic order.
    let output = fs::read_to_string("tests/sql_tests/supply_chain/preferred_vendor.json").unwrap();
    let mut lines = output.lines().skip(3).collect::<Vec<_>>();
    lines.sort();

    assert_eq!(expected_output, lines);

    server_thread.shutdown();
}

#[test]
#[serial]
fn secops_test() {
    let server_thread = ServerThread::new(start_pipeline("tests/sql_tests/secops"));

    // TODO: process some Kafka data. Requires CSV support.

    server_thread.shutdown();
}

#[test]
#[serial]
fn datetime_test() {
    let server_thread = ServerThread::new(start_pipeline("tests/sql_tests/datetime"));

    let client = Client::new();

    assert!(client
        .post(endpoint(
            "ingress/T1?format=json&json_flavor=debezium_mysql"
        ))
        .body(r#"{"insert": {"d": 16816, "ts": "2018-06-20T13:37:03Z"}}"#)
        .send()
        .unwrap()
        .status()
        .is_success());

    sleep(Duration::from_millis(2_000));

    server_thread.shutdown();
    assert_eq!(
        fs::read_to_string("tests/sql_tests/datetime/v1.json").unwrap(),
        r#"{"insert":{"D":"2016-01-16","TS":"2018-06-20 13:37:03"}}
"#
    );
    let mut serialized = serde_json::from_str::<serde_json::Value>(
        &fs::read_to_string("tests/sql_tests/datetime/v1-snowflake.json").unwrap(),
    )
    .unwrap();
    serialized.as_object_mut().unwrap().remove("__stream_id");

    assert_eq!(
        serialized,
        json!({
            "D":"2016-01-16",
            "TS":"2018-06-20T13:37:03+00:00",
            "__action":"insert",
            "__seq_number":0
        })
    );
}
