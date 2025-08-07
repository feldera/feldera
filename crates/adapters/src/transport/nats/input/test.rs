use crate::test::{mock_input_pipeline, wait, DEFAULT_TIMEOUT_MS};
use anyhow::Result as AnyResult;
use async_nats::{self, jetstream};
use feldera_types::deserialize_without_context;
use feldera_types::program_schema::Relation;
use serde::{Deserialize, Serialize};
use std::{thread::sleep, time::Duration};

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub struct TestStruct {
    s: String,
    b: bool,
    i: i64,
}

impl TestStruct {
    fn new(s: String, b: bool, i: i64) -> Self {
        Self { s, b, i }
    }
}

deserialize_without_context!(TestStruct);

#[test]
fn test_foo() -> AnyResult<()> {
    let stream_name = "str";
    let subject_name = "sub";

    let (_nats_process_guard, nats_url) = util::start_nats_server();

    let test_data = [
        TestStruct::new("foo".to_string(), true, 10),
        TestStruct::new("bar".to_string(), false, -10),
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

    let (endpoint, consumer, _parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
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

    Ok(())
}

mod util {
    use async_nats::Client;
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
            let _ = self.process.wait(); // Reap the process
        }
    }

    pub fn start_nats_server() -> (ProcessKillGuard, String) {
        let nats_addr = "127.0.0.1";
        let nats_port = 42220;
        let nats_url = format!("{}:{}", nats_addr, nats_port);

        let child = Command::new("nats-server")
            .arg("-a")
            .arg(nats_addr)
            .arg("-p")
            .arg(nats_port.to_string())
            .arg("--jetstream")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start nats-server");

        (ProcessKillGuard::new(child), nats_url)
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
}
