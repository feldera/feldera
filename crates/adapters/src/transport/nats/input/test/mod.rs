use feldera_types::deserialize_without_context;
use serde::{Deserialize, Serialize};

mod control_tests;
mod controller_framework;
mod custom_tests;
mod mock_framework;
mod mock_tests;

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

mod util {
    use crate::test::wait;
    use anyhow::{Result as AnyResult, anyhow};
    use async_nats::{self, Client, jetstream};
    use serde::{Deserialize, Serialize};
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
        const MAX_ATTEMPTS: usize = 2;
        let mut last_error: Option<anyhow::Error> = None;
        for attempt in 1..=MAX_ATTEMPTS {
            match start_nats_and_get_address_once() {
                Ok(result) => return Ok(result),
                Err(error) => {
                    last_error = Some(error);
                    if attempt < MAX_ATTEMPTS {
                        std::thread::sleep(Duration::from_millis(250));
                    }
                }
            }
        }

        Err(last_error.expect("at least one attempt should have failed"))
    }

    fn start_nats_and_get_address_once() -> AnyResult<(ProcessKillGuard, String)> {
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

    pub fn start_nats_on_port(port: u16) -> AnyResult<(ProcessKillGuard, String)> {
        let nats_ip_addr = "127.0.0.1";

        let child = Command::new("nats-server")
            .arg("-a")
            .arg(nats_ip_addr)
            .arg("-p")
            .arg(port.to_string())
            .arg("--jetstream")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        Ok((
            ProcessKillGuard::new(child),
            format!("nats://{nats_ip_addr}:{port}"),
        ))
    }

    pub async fn create_stream(nats_url: &str, stream: &str, subject: &str) -> AnyResult<()> {
        let client = wait_for_nats_ready(nats_url, Duration::from_secs(5)).await?;
        let js = jetstream::new(client);
        js.create_stream(jetstream::stream::Config {
            name: stream.to_string(),
            subjects: vec![subject.to_string()],
            storage: jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await?;
        Ok(())
    }

    pub async fn publish_json<T: Serialize>(
        nats_url: &str,
        subject: &str,
        values: &[T],
    ) -> AnyResult<()> {
        let client = wait_for_nats_ready(nats_url, Duration::from_secs(5)).await?;
        let js = jetstream::new(client);
        let subject = subject.to_string();
        for val in values {
            let ack = js
                .publish(subject.clone(), serde_json::to_string(val)?.into())
                .await?;
            ack.await?;
        }
        Ok(())
    }

    pub async fn purge_stream(nats_url: &str, stream: &str) -> AnyResult<()> {
        let client = wait_for_nats_ready(nats_url, Duration::from_secs(5)).await?;
        let js = jetstream::new(client);
        let stream = js.get_stream(stream).await?;
        stream.purge().await?;
        Ok(())
    }

    pub async fn delete_stream(nats_url: &str, stream: &str) -> AnyResult<()> {
        let client = wait_for_nats_ready(nats_url, Duration::from_secs(5)).await?;
        let js = jetstream::new(client);
        js.delete_stream(stream).await?;
        Ok(())
    }
}
