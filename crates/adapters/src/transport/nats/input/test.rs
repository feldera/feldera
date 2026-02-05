use crate::test::{
    DEFAULT_TIMEOUT_MS, TestStruct, init_test_logger, mock_input_pipeline, test_circuit, wait,
};
use crate::{Controller, PipelineConfig};
use anyhow::Result as AnyResult;
use async_nats::{self, jetstream};
use csv::ReaderBuilder as CsvReaderBuilder;
use feldera_types::deserialize_without_context;
use feldera_types::program_schema::Relation;
use serde::{Deserialize, Serialize};
use serde_json;
use std::{fs::create_dir, thread::sleep, time::Duration};
use tempfile::TempDir;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub struct NatsTestRecord {
    s: String,
    b: bool,
    i: i64,
}

impl NatsTestRecord {
    fn new(s: String, b: bool, i: i64) -> Self {
        Self { s, b, i }
    }
}

deserialize_without_context!(NatsTestRecord);

#[test]
fn test_nats_basic_input_consumption() -> AnyResult<()> {
    let stream_name = "str";
    let subject_name = "sub";

    let (_nats_process_guard, nats_url) = util::start_nats_and_get_address()?;

    let test_data = [
        NatsTestRecord::new("foo".to_string(), true, 10),
        NatsTestRecord::new("bar".to_string(), false, -10),
    ];

    // Create and populate NATS stream before initializing the input connector.
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5)).await?;
        let jetstream = jetstream::new(client);
        jetstream
            .create_stream(jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![subject_name.to_string()],
                storage: jetstream::stream::StorageType::Memory,
                ..Default::default()
            })
            .await?;

        for val in test_data.iter() {
            let ack = jetstream
                .publish(subject_name, serde_json::to_string(val)?.into())
                .await?;
            ack.await?;
        }

        Ok::<(), anyhow::Error>(())
    })?;

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
        stream_name: {stream_name}
        consumer_config:
            deliver_policy: All
            subjects: [{subject_name}]
format:
    name: json
    config:
        update_format: raw
"#
    );

    println!("Config:\n{}", config_str);

    let (endpoint, consumer, _parser, zset) =
        mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
        )
        .unwrap();

    sleep(Duration::from_millis(10));

    // No outputs should be produced at this point.
    assert!(!consumer.state().eoi);

    // Unpause the endpoint, wait for the data to appear at the output.
    endpoint.extend();
    wait(
        || {
            endpoint.queue(false);
            zset.state().flushed.len() == test_data.len()
        },
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();
    for (i, upd) in zset.state().flushed.iter().enumerate() {
        assert_eq!(upd.unwrap_insert(), &test_data[i]);
    }

    endpoint.disconnect();

    Ok(())
}

#[derive(Clone)]
struct NatsFtTestRound {
    n_records: usize,
    do_checkpoint: bool,
}

impl NatsFtTestRound {
    fn with_checkpoint(n_records: usize) -> Self {
        Self {
            n_records,
            do_checkpoint: true,
        }
    }

    fn without_checkpoint(n_records: usize) -> Self {
        Self {
            n_records,
            do_checkpoint: false,
        }
    }
}

struct NatsFtTestConfig<'a> {
    rounds: &'a [NatsFtTestRound],
    consumer_name: Option<&'a str>,
}

impl<'a> NatsFtTestConfig<'a> {
    fn new(rounds: &'a [NatsFtTestRound]) -> Self {
        Self {
            rounds,
            consumer_name: None,
        }
    }

    fn with_consumer_name(mut self, name: Option<&'a str>) -> Self {
        self.consumer_name = name;
        self
    }
}

fn test_nats_ft(rounds: &[NatsFtTestRound]) {
    test_nats_ft_with_config(NatsFtTestConfig::new(rounds));
}

fn test_nats_ft_with_config(test_config: NatsFtTestConfig<'_>) {
    init_test_logger();

    let (_nats_process_guard, nats_url) = util::start_nats_and_get_address().unwrap();

    let stream_name = "str";
    let subject_name = "sub";

    // Setup NATS stream
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = util::wait_for_nats_ready(&nats_url, Duration::from_secs(5))
            .await
            .unwrap();
        let jetstream = jetstream::new(client);
        jetstream
            .create_stream(jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![subject_name.to_string()],
                storage: jetstream::stream::StorageType::Memory,
                ..Default::default()
            })
            .await
            .unwrap();
    });

    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let output_path = tempdir_path.join("output.csv");

    let consumer_name_line = test_config
        .consumer_name
        .map(|n| format!("name: {n}"))
        .unwrap_or_default();

    let config_str = format!(
        r#"
name: test
workers: 4
storage_config:
    path: {storage_dir:?}
storage: true
fault_tolerance: {{}}
clock_resolution_usecs: null
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: nats_input
            config:
                connection_config:
                    server_url: {nats_url}
                stream_name: {stream_name}
                consumer_config:
                    {consumer_name_line}
                    deliver_policy: All
                    subjects: [{subject_name}]
        format:
            name: json
            config:
                update_format: raw
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: {output_path:?}
        format:
            name: csv
"#
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    let mut total_records = 0usize;
    let mut checkpointed_records = 0usize;

    for (
        round,
        NatsFtTestRound {
            n_records,
            do_checkpoint,
        },
    ) in test_config.rounds.iter().cloned().enumerate()
    {
        println!(
            "--- round {round}: add {n_records} records, {} ---",
            if do_checkpoint {
                "and checkpoint"
            } else {
                "no checkpoint"
            }
        );

        println!(
            "Writing records {total_records}..{}",
            total_records + n_records
        );
        if n_records > 0 {
            let nats_url = &nats_url;
            rt.block_on(async move {
                let client = util::wait_for_nats_ready(nats_url, Duration::from_secs(5))
                    .await
                    .unwrap();
                let jetstream = jetstream::new(client);

                for id in total_records..total_records + n_records {
                    let test_struct = TestStruct {
                        id: id as u32,
                        b: id % 2 == 0,
                        i: Some(id as i64),
                        s: format!("msg{}", id),
                    };
                    let json_data = serde_json::to_string(&test_struct).unwrap();
                    println!("Publishing: {}", json_data);
                    let ack = jetstream
                        .publish(subject_name, json_data.into())
                        .await
                        .unwrap();
                    let ack_result = ack.await.unwrap();
                    println!(
                        "Published message {} with sequence: {}",
                        id, ack_result.sequence
                    );
                }
                println!("Successfully published {} records to NATS", n_records);
            });
            total_records += n_records;
        }

        println!("start pipeline");
        let controller = Controller::with_test_config(
            |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &[],
                    &[Some("output")],
                ))
            },
            &config,
            Box::new(|e, _tag| {
                println!("Controller error: {e}");
                panic!("Controller error: {e}");
            }),
        )
        .unwrap();

        controller.start();

        // Wait for the records that are not in the checkpoint to be
        // processed or replayed.
        println!(
            "wait for {} records {checkpointed_records}..{total_records}",
            total_records - checkpointed_records
        );
        let mut last_n = 0;
        let result = wait(
            || {
                let n = controller
                    .status()
                    .output_status()
                    .get(&0)
                    .unwrap()
                    .transmitted_records() as usize;

                if n > last_n {
                    println!("received {n} records of {total_records}");
                    last_n = n;
                }
                n >= total_records
            },
            10_000,
        );

        if let Err(()) = result {
            println!(
                "Controller status:\n{}",
                serde_json::to_string_pretty(&controller.status().to_api_type(Ok(()), false))
                    .unwrap()
            );
            panic!("Failed to receive expected records within timeout");
        }

        // No more records should arrive, but give the controller some time
        // to send some more in case there's a bug.
        sleep(Duration::from_millis(100));

        // Then verify that the number is as expected.
        assert_eq!(
            controller
                .status()
                .output_status()
                .get(&0)
                .unwrap()
                .transmitted_records(),
            total_records as u64
        );

        if do_checkpoint {
            println!("checkpoint");
            controller.checkpoint().unwrap();
        }

        println!("stop controller");
        controller.stop().unwrap();

        let mut actual = CsvReaderBuilder::new()
            .has_headers(false)
            .from_path(&output_path)
            .unwrap()
            .deserialize::<(TestStruct, i32)>()
            .map(|res| {
                let (val, weight) = res.unwrap();
                assert_eq!(weight, 1);
                val
            })
            .collect::<Vec<_>>();
        actual.sort_by_key(|item| item.id);

        assert_eq!(actual.len(), total_records - checkpointed_records);
        for (record, expect_record) in
            actual
                .into_iter()
                .zip((checkpointed_records..).map(|id| TestStruct {
                    id: id as u32,
                    b: id % 2 == 0,
                    i: Some(id as i64),
                    s: format!("msg{}", id),
                }))
        {
            assert_eq!(record, expect_record);
        }

        if do_checkpoint {
            checkpointed_records = total_records;
        }
        println!();
    }
}

#[test]
fn test_nats_ft_simple() {
    test_nats_ft(&[NatsFtTestRound::with_checkpoint(5)]);
}

#[test]
fn test_nats_ft_with_checkpoints() {
    test_nats_ft(&[
        NatsFtTestRound::with_checkpoint(10),
        NatsFtTestRound::with_checkpoint(15),
        NatsFtTestRound::with_checkpoint(20),
    ]);
}

#[test]
fn test_nats_ft_without_checkpoints() {
    test_nats_ft(&[
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::without_checkpoint(15),
        NatsFtTestRound::without_checkpoint(20),
    ]);
}

#[test]
fn test_nats_ft_alternating() {
    test_nats_ft(&[
        NatsFtTestRound::with_checkpoint(10),
        NatsFtTestRound::without_checkpoint(15),
        NatsFtTestRound::with_checkpoint(20),
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::with_checkpoint(15),
    ]);
}

#[test]
fn test_nats_ft_initially_zero_without_checkpoint() {
    test_nats_ft(&[
        NatsFtTestRound::without_checkpoint(0),
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::without_checkpoint(0),
        NatsFtTestRound::with_checkpoint(15),
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::with_checkpoint(20),
    ]);
}

#[test]
fn test_nats_ft_initially_zero_with_checkpoint() {
    test_nats_ft(&[
        NatsFtTestRound::with_checkpoint(0),
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::without_checkpoint(0),
        NatsFtTestRound::with_checkpoint(15),
        NatsFtTestRound::without_checkpoint(10),
        NatsFtTestRound::with_checkpoint(20),
    ]);
}

#[test]
fn test_nats_ft_empty_step_checkpoint() {
    test_nats_ft(&[
        NatsFtTestRound::with_checkpoint(5),
        NatsFtTestRound::with_checkpoint(0),
        NatsFtTestRound::with_checkpoint(10),
        NatsFtTestRound::with_checkpoint(0),
        NatsFtTestRound::with_checkpoint(0),
        NatsFtTestRound::with_checkpoint(10),
    ]);
}

/// Tests rapid restart+replay with a named consumer.
///
/// This reproduces a bug where the previous ordered consumer hadn't expired yet,
/// causing "consumer already exists" errors. The fix generates unique consumer names
/// by appending a UUID suffix when a name is explicitly configured.
#[test]
fn test_nats_ft_with_named_consumer() {
    test_nats_ft_with_config(
        NatsFtTestConfig::new(&[
            NatsFtTestRound::with_checkpoint(5),
            NatsFtTestRound::with_checkpoint(5),
            NatsFtTestRound::with_checkpoint(0),
        ])
        .with_consumer_name(Some("my_named_consumer")),
    );
}

/// Helper to assert that a connection error contains expected context.
fn assert_nats_connect_error(
    result: AnyResult<(
        Box<dyn crate::InputReader>,
        crate::test::MockInputConsumer,
        crate::test::MockInputParser,
        crate::test::MockDeZSet<NatsTestRecord, NatsTestRecord>,
    )>,
    expected_url: &str,
    expected_cause: &str,
) {
    match result {
        Ok(_) => panic!("Expected connection to fail"),
        Err(err) => {
            let err_msg = format!("{err:#}"); // Full error chain
            assert!(
                err_msg.contains(expected_url),
                "Error message should contain server URL, got: {err_msg}"
            );
            assert!(
                err_msg.contains("Failed to connect"),
                "Error message should indicate connection failure, got: {err_msg}"
            );
            assert!(
                err_msg.contains(expected_cause),
                "Error message should contain cause '{expected_cause}', got: {err_msg}"
            );
        }
    }
}

/// Test that connecting to a non-existent server (connection refused) produces
/// a clear error message with the server URL included.
#[test]
fn test_nats_connection_refused_error() {
    let nonexistent_url = "nats://127.0.0.1:59999";

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nonexistent_url}
        stream_name: my_stream
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#
    );

    let result = mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
    );

    assert_nats_connect_error(result, nonexistent_url, "Connection refused");
}

/// Test that connecting to a valid server but requesting a non-existent stream
/// produces a clear error message with the stream name.
#[test]
fn test_nats_stream_not_found_error() {
    let (_nats_process_guard, nats_url) = util::start_nats_and_get_address().unwrap();

    // Wait for NATS to be ready
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        util::wait_for_nats_ready(&nats_url, Duration::from_secs(5))
            .await
            .unwrap();
    });

    let nonexistent_stream = "this_stream_does_not_exist";

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
        stream_name: {nonexistent_stream}
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#
    );

    let result = mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
    );

    match result {
        Ok(_) => panic!("Expected stream lookup to fail"),
        Err(err) => {
            let err_msg = format!("{err:#}"); // Full error chain
            // The error message should contain the stream name for easy debugging
            assert!(
                err_msg.contains(nonexistent_stream),
                "Error message should contain stream name, got: {err_msg}"
            );
            assert!(
                err_msg.contains("Failed to get stream"),
                "Error message should indicate stream lookup failure, got: {err_msg}"
            );
        }
    }
}

/// Test that connection timeout option is respected.
#[test]
fn test_nats_connection_timeout() {
    // Use a non-routable IP address that will cause a connection timeout
    // 10.255.255.1 is a reserved address that should not respond
    let non_routable_url = "nats://10.255.255.1:4222";
    let timeout_secs = 1;

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {non_routable_url}
            connection_timeout_secs: {timeout_secs}
        stream_name: some_stream
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#
    );

    let start = std::time::Instant::now();

    let result = mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
    );

    let elapsed = start.elapsed();

    // Should fail within a reasonable time relative to the timeout
    // Allow some slack for test execution overhead
    let max_expected = Duration::from_secs(timeout_secs + 3);
    assert!(
        elapsed < max_expected,
        "Connection should timeout within ~{timeout_secs}s, took {:?}",
        elapsed
    );

    assert_nats_connect_error(result, non_routable_url, "timed out");
}

mod util {
    use crate::test::wait;
    use anyhow::{Result as AnyResult, anyhow};
    use async_nats::Client;
    use serde::Deserialize;
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    pub struct ProcessKillGuard {
        process: Child,
    }

    impl ProcessKillGuard {
        fn new(process: Child) -> Self {
            Self { process }
        }
    }

    impl Drop for ProcessKillGuard {
        fn drop(&mut self) {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
    }

    pub async fn wait_for_nats_ready(addr: &str, timeout: Duration) -> anyhow::Result<Client> {
        let deadline = Instant::now() + timeout;
        loop {
            match async_nats::connect(addr).await {
                Ok(client) => return Ok(client),
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                Err(e) => return Err(anyhow::anyhow!("Timeout waiting for NATS: {e}")),
            }
        }
    }

    pub fn start_nats_and_get_address() -> AnyResult<(ProcessKillGuard, String)> {
        let nats_ip_addr = "127.0.0.1";
        const RANDOM_PORT: &str = "-1";

        let temp_dir = env::temp_dir();
        let port_file_dir = temp_dir.join("nats_ports");

        fs::create_dir_all(&port_file_dir)?;

        let child = Command::new("nats-server")
            .arg("-a")
            .arg(nats_ip_addr)
            .arg("-p")
            .arg(RANDOM_PORT)
            .arg("--ports_file_dir")
            .arg(port_file_dir.to_str().unwrap())
            .arg("--jetstream")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        let pid = child.id();
        let port_file_path = port_file_dir.join(format!("nats-server_{}.ports", pid));

        let child = ProcessKillGuard::new(child);

        if wait(|| port_file_path.exists(), 10_000).is_err() {
            return Err(anyhow!("Port file was not created within timeout period"));
        }

        fn get_address_from_ports_file(file_path: &Path) -> AnyResult<String> {
            #[derive(Deserialize)]
            struct PortsData {
                nats: Vec<String>,
            }

            let port_content = fs::read_to_string(file_path)?;
            let ports_data: PortsData = serde_json::from_str(&port_content)
                .map_err(|_| anyhow!("Could not parse ports file"))?;

            ports_data
                .nats
                .into_iter()
                .next()
                .ok_or(anyhow!("No NATS addresses found in port file"))
        }

        let nats_addr = get_address_from_ports_file(&port_file_path)?;

        Ok((child, nats_addr))
    }
}
