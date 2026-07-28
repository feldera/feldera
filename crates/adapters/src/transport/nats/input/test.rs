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
    use anyhow::{Result as AnyResult, anyhow};
    use async_nats::{self, Client, jetstream};
    use serde::{Deserialize, Serialize};
    use std::fs;
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    /// How long to wait for a spawned `nats-server` to bind its client port.
    const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

    /// A running `nats-server`, killed when the guard is dropped.
    ///
    /// Each server owns a private scratch directory for its JetStream store and
    /// its ports file. Sharing either across servers breaks tests that run
    /// concurrently:
    ///
    /// - Servers starting at the same moment into one JetStream store directory
    ///   race on `mkdir $G/streams`, and the loser dies with
    ///   `Can't start JetStream`. The window is open only while that directory
    ///   does not yet exist, so the race showed up on fresh CI containers and
    ///   almost never on a developer machine.
    /// - A ports file is named `nats-server_<pid>.ports`, and a server killed by
    ///   SIGKILL leaves its file behind. A later server assigned the same pid
    ///   then finds the stale file and reports the dead server's port.
    pub struct ProcessKillGuard {
        process: Child,
        /// Holds the JetStream store, the ports file, and the server log.
        /// Deleted after `process` has been killed and reaped.
        _scratch: TempDir,
        store_dir: PathBuf,
        ports_file: PathBuf,
        log_path: PathBuf,
    }

    impl ProcessKillGuard {
        /// Wait until the server reports the address it bound.
        ///
        /// `nats-server` writes its ports file only after binding the client
        /// port, so the file proves *this* server owns the port. A TCP probe
        /// would not: it also succeeds against an unrelated server that already
        /// held the port.
        fn wait_until_ready(&mut self) -> AnyResult<String> {
            let deadline = Instant::now() + STARTUP_TIMEOUT;
            loop {
                // Parse before checking liveness, so a server that became ready
                // and then died still counts as started. Parsing on every poll
                // also tolerates reading the file while it is still being
                // written: a partial read simply does not parse yet.
                if let Some(addr) = fs::read_to_string(&self.ports_file)
                    .ok()
                    .and_then(|content| parse_ports_file(&content))
                {
                    return Ok(addr);
                }
                if let Some(status) = self.process.try_wait()? {
                    return Err(anyhow!(
                        "nats-server exited with {status} before it became ready; log:\n{}",
                        self.log()
                    ));
                }
                if Instant::now() >= deadline {
                    return Err(anyhow!(
                        "nats-server did not report a bound port within {STARTUP_TIMEOUT:?}; log:\n{}",
                        self.log()
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        fn log(&self) -> String {
            fs::read_to_string(&self.log_path).unwrap_or_else(|e| format!("<unreadable: {e}>"))
        }
    }

    impl Drop for ProcessKillGuard {
        fn drop(&mut self) {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
    }

    /// Extract the first client address from a `nats-server` ports file.
    /// Returns `None` while the file is empty or incomplete.
    fn parse_ports_file(content: &str) -> Option<String> {
        #[derive(Deserialize)]
        struct PortsData {
            nats: Vec<String>,
        }

        serde_json::from_str::<PortsData>(content)
            .ok()?
            .nats
            .into_iter()
            .next()
    }

    /// Spawn a JetStream-enabled `nats-server` on `port` and wait until it binds.
    ///
    /// `port` is a `nats-server` port argument: a TCP port, or `-1` to let the
    /// operating system choose one. Returns the guard and the bound address.
    fn spawn_nats_server(port: &str) -> AnyResult<(ProcessKillGuard, String)> {
        let nats_ip_addr = "127.0.0.1";

        let scratch = TempDir::new()?;
        let store_dir = scratch.path().join("jetstream");
        let ports_file_dir = scratch.path().join("ports");
        let log_path = scratch.path().join("nats-server.log");
        fs::create_dir_all(&store_dir)?;
        fs::create_dir_all(&ports_file_dir)?;

        let log = fs::File::create(&log_path)?;
        let process = Command::new("nats-server")
            .arg("-a")
            .arg(nats_ip_addr)
            .arg("-p")
            .arg(port)
            .arg("--ports_file_dir")
            .arg(&ports_file_dir)
            .arg("--jetstream")
            .arg("--store_dir")
            .arg(&store_dir)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;

        let ports_file = ports_file_dir.join(format!("nats-server_{}.ports", process.id()));
        let mut guard = ProcessKillGuard {
            process,
            _scratch: scratch,
            store_dir,
            ports_file,
            log_path,
        };

        // On error the guard is dropped here, which kills the server.
        let addr = guard.wait_until_ready()?;
        Ok((guard, addr))
    }

    pub fn start_nats_and_get_address() -> AnyResult<(ProcessKillGuard, String)> {
        const MAX_ATTEMPTS: usize = 2;
        const RANDOM_PORT: &str = "-1";

        let mut last_error: Option<anyhow::Error> = None;
        for attempt in 1..=MAX_ATTEMPTS {
            match spawn_nats_server(RANDOM_PORT) {
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

    /// Start a server on a specific port, as tests do when restarting a killed
    /// server at the address its connector is configured with.
    ///
    /// Fails if the port is no longer available. Nothing reserves the port while
    /// the server is down, so an unrelated process may take it; failing here
    /// reports that directly instead of letting a later action time out against
    /// a foreign server.
    pub fn start_nats_on_port(port: u16) -> AnyResult<(ProcessKillGuard, String)> {
        let (guard, addr) = spawn_nats_server(&port.to_string())
            .map_err(|e| e.context(format!("failed to restart nats-server on port {port}")))?;

        if !addr.ends_with(&format!(":{port}")) {
            return Err(anyhow!(
                "nats-server was asked for port {port} but bound {addr}"
            ));
        }
        Ok((guard, addr))
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

    /// Tests for the server-spawning helpers themselves. Every NATS test depends
    /// on them, so a silent failure here surfaces as an unrelated timeout in
    /// whichever test happened to run.
    #[cfg(test)]
    mod tests {
        use super::*;

        /// A server that cannot bind its port must be reported as a failure.
        ///
        /// `Command::spawn` succeeds even when the server dies immediately after,
        /// so returning the guard unchecked left tests talking to whichever
        /// process actually held the port.
        #[test]
        fn start_nats_on_port_fails_when_port_is_taken() {
            let (_holder, addr) = start_nats_and_get_address().unwrap();
            let port: u16 = addr.rsplit_once(':').unwrap().1.parse().unwrap();

            let error = start_nats_on_port(port)
                .err()
                .expect("starting a second server on a taken port must fail");
            let text = format!("{error:#}");
            assert!(
                text.contains(&format!("port {port}")),
                "error should name the port, got: {text}"
            );
            assert!(
                text.contains("address already in use"),
                "error should include the server log explaining the failure, got: {text}"
            );
        }

        /// Servers must not share the directories they write into, for the
        /// reasons given on [`ProcessKillGuard`].
        ///
        /// Asserted structurally rather than by observing a race: both races are
        /// timing- and pid-dependent, so a test that waited to see one would pass
        /// while the defect was still present.
        #[test]
        fn servers_do_not_share_directories() {
            let (first, _) = start_nats_and_get_address().unwrap();
            let (second, _) = start_nats_and_get_address().unwrap();

            assert_ne!(
                first.store_dir, second.store_dir,
                "servers must not share a JetStream store directory"
            );
            assert_ne!(
                first.ports_file.parent(),
                second.ports_file.parent(),
                "servers must not share a ports file directory"
            );
        }

        /// Servers started at the same moment must all come up on distinct ports.
        #[test]
        fn concurrent_servers_start_independently() {
            const SERVERS: usize = 16;

            let barrier = std::sync::Barrier::new(SERVERS);
            let servers: Vec<_> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..SERVERS)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            start_nats_and_get_address()
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().expect("spawn thread panicked"))
                    .collect()
            });

            let mut addrs = Vec::new();
            for server in &servers {
                match server {
                    Ok((_guard, addr)) => addrs.push(addr.clone()),
                    Err(error) => panic!("concurrent server failed to start: {error:#}"),
                }
            }

            addrs.sort();
            addrs.dedup();
            assert_eq!(addrs.len(), SERVERS, "servers must bind distinct ports");
        }

        #[test]
        fn parse_ports_file_rejects_incomplete_content() {
            // A server writes its ports file after startup; polling can observe
            // it empty or half-written, which must read as "not ready yet"
            // rather than as an error or a bogus address.
            assert_eq!(parse_ports_file(""), None);
            assert_eq!(parse_ports_file("{\"nats\":[\"nats://127.0"), None);
            assert_eq!(parse_ports_file("{\"nats\":[]}"), None);
            assert_eq!(
                parse_ports_file("{\"nats\":[\"nats://127.0.0.1:4222\"]}"),
                Some("nats://127.0.0.1:4222".to_string())
            );
        }
    }
}
