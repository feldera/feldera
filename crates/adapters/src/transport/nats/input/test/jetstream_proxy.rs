//! A TCP proxy in front of a real `nats-server` that injects JetStream faults.
//!
//! The proxy understands just enough of the NATS client protocol to see each
//! JetStream API request and each reply. Everything it does not change goes
//! through unmodified, so the server still owns streams and consumers.
//!
//! Some faults are hard to get from a single real server on demand: a reader
//! that fails right after a successful consumer create, a create whose reply
//! is lost, or a create that the server applies after a later delete (as a
//! JetStream meta-leader change can do). The proxy makes them deterministic.

use anyhow::{Result as AnyResult, anyhow};
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

/// Faults the proxy injects. Counts are 1-based and global across all client
/// connections, because the connector opens a new connection on every retry.
#[derive(Clone, Debug, Default)]
pub(super) struct ProxyFaults {
    /// Answers every pull request with a `400 Bad Request` status, so every
    /// reader fails right after its consumer was created.
    pub fail_pulls: bool,
    /// Answers the first pull request with a `409 Consumer Deleted` status,
    /// so the client library recreates the consumer.
    pub consumer_deleted_on_first_pull: bool,
    /// Drops the server's reply to the nth consumer create. The server still
    /// creates the consumer.
    pub drop_create_reply: Option<usize>,
    /// Holds the nth consumer create until a delete of the same consumer has
    /// reached the server, then forwards the create.
    pub hold_create_until_delete: Option<usize>,
    /// Holds every consumer delete for this long before forwarding it.
    pub delay_deletes: Option<Duration>,
    /// Holds consumer deletes until the test calls `release_deletes()`.
    pub hold_deletes: bool,
}

/// State shared by all connections of one proxy.
#[derive(Default)]
struct ProxyState {
    faults: ProxyFaults,
    creates: usize,
    pulls: usize,
    deletes: usize,
    delete_gate: Option<Arc<tokio::sync::Semaphore>>,
    stream_info_gate: Option<Arc<tokio::sync::Semaphore>>,
    /// Reply subjects whose server message must not reach the client.
    dropped_replies: HashSet<String>,
    /// The held create: consumer name and raw frame.
    held_create: Option<(String, Vec<u8>)>,
    /// Number of stream info replies still to drop. Set at run time with
    /// [`JetStreamProxy::drop_stream_info_replies`].
    stream_info_replies_to_drop: usize,
}

pub(super) struct JetStreamProxy {
    url: String,
    state: Arc<Mutex<ProxyState>>,
    accept_task: JoinHandle<()>,
}

impl JetStreamProxy {
    /// Starts a proxy on a random local port that forwards to `upstream_url`.
    pub(super) fn start(rt: &Runtime, upstream_url: &str, faults: ProxyFaults) -> AnyResult<Self> {
        let upstream = upstream_url
            .strip_prefix("nats://")
            .unwrap_or(upstream_url)
            .to_string();
        let listener = rt.block_on(TcpListener::bind("127.0.0.1:0"))?;
        let url = format!("nats://{}", listener.local_addr()?);
        let delete_gate = faults
            .hold_deletes
            .then(|| Arc::new(tokio::sync::Semaphore::new(0)));
        let state = Arc::new(Mutex::new(ProxyState {
            faults,
            delete_gate,
            ..Default::default()
        }));
        let accept_state = state.clone();
        let accept_task = rt.spawn(async move {
            let state = accept_state;
            while let Ok((client, _)) = listener.accept().await {
                let upstream = upstream.clone();
                let state = state.clone();
                tokio::spawn(async move {
                    if let Ok(server) = TcpStream::connect(&upstream).await {
                        let _ = proxy_connection(client, server, state).await;
                    }
                });
            }
        });
        Ok(Self {
            url,
            state,
            accept_task,
        })
    }

    pub(super) fn url(&self) -> &str {
        &self.url
    }

    /// Drops the server's replies to the next `n` stream info requests, so
    /// those requests time out. The server still answers them.
    pub(super) fn drop_stream_info_replies(&self, n: usize) {
        self.state.lock().unwrap().stream_info_replies_to_drop = n;
    }

    /// Waits until cleanup has actually reached the proxy, not merely until
    /// the test sent a Pause or Disconnect command.
    pub(super) fn wait_for_delete_requests(&self, n: usize, timeout: Duration) -> AnyResult<()> {
        crate::test::wait(
            || self.state.lock().unwrap().deletes >= n,
            timeout.as_millis(),
        )
        .map(|_| ())
        .map_err(|()| anyhow!("proxy did not receive {n} consumer deletes within {timeout:?}"))
    }

    /// Keep replay validation from observing a partially repopulated stream.
    pub(super) fn hold_stream_info(&self) {
        self.state.lock().unwrap().stream_info_gate =
            Some(Arc::new(tokio::sync::Semaphore::new(0)));
    }

    pub(super) fn release_stream_info(&self) {
        if let Some(gate) = self.state.lock().unwrap().stream_info_gate.take() {
            gate.close();
        }
    }

    pub(super) fn release_deletes(&self) {
        if let Some(gate) = &self.state.lock().unwrap().delete_gate {
            gate.close();
        }
    }
}

impl Drop for JetStreamProxy {
    fn drop(&mut self) {
        self.release_deletes();
        self.release_stream_info();
        self.accept_task.abort();
    }
}

/// One protocol frame: the control line without its CRLF, plus the payload
/// with its trailing CRLF for operations that carry one.
struct Frame {
    line: String,
    payload: Vec<u8>,
}

impl Frame {
    fn args(&self) -> Vec<&str> {
        self.line.split_whitespace().collect()
    }

    fn op(&self) -> String {
        self.args()
            .first()
            .map(|op| op.to_ascii_uppercase())
            .unwrap_or_default()
    }

    fn bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.line.len() + 2 + self.payload.len());
        bytes.extend_from_slice(self.line.as_bytes());
        bytes.extend_from_slice(b"\r\n");
        bytes.extend_from_slice(&self.payload);
        bytes
    }
}

async fn read_frame<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<Option<Frame>> {
    let mut raw = Vec::new();
    if reader.read_until(b'\n', &mut raw).await? == 0 {
        return Ok(None);
    }
    let line = String::from_utf8_lossy(&raw)
        .trim_end_matches(['\r', '\n'])
        .to_string();
    let mut frame = Frame {
        line,
        payload: Vec::new(),
    };
    if matches!(frame.op().as_str(), "PUB" | "HPUB" | "MSG" | "HMSG") {
        // The last argument is the total payload size (headers included).
        let size: usize = frame
            .args()
            .last()
            .and_then(|size| size.parse().ok())
            .ok_or_else(|| io::Error::other(format!("bad frame: {}", frame.line)))?;
        frame.payload = vec![0; size + 2];
        reader.read_exact(&mut frame.payload).await?;
    }
    Ok(Some(frame))
}

/// Returns the consumer name of a `$JS.API.CONSUMER.<verb>.<stream>.<name>...`
/// request subject, when the subject is that verb.
fn consumer_name<'a>(subject: &'a str, verb: &str) -> Option<&'a str> {
    let tokens: Vec<&str> = subject.split('.').collect();
    let verb_tokens: Vec<&str> = verb.split('.').collect();
    let name_index = 3 + verb_tokens.len() + 1;
    (tokens.len() > name_index
        && tokens[..3] == ["$JS", "API", "CONSUMER"]
        && tokens[3..3 + verb_tokens.len()] == verb_tokens[..])
        .then(|| tokens[name_index])
}

/// A status-only message for subscription `sid` on `subject`.
fn status_message(subject: &str, sid: &str, status: &str) -> Vec<u8> {
    let headers = format!("NATS/1.0 {status}\r\n\r\n");
    format!(
        "HMSG {subject} {sid} {} {}\r\n{headers}\r\n",
        headers.len(),
        headers.len()
    )
    .into_bytes()
}

async fn proxy_connection(
    client: TcpStream,
    server: TcpStream,
    state: Arc<Mutex<ProxyState>>,
) -> AnyResult<()> {
    let (client_read, client_write) = client.into_split();
    let (server_read, mut server_write) = server.into_split();
    let client_write = Arc::new(tokio::sync::Mutex::new(client_write));

    // Server to client: forward everything except dropped replies.
    let to_client = {
        let state = state.clone();
        let client_write = client_write.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            while let Ok(Some(frame)) = read_frame(&mut reader).await {
                if matches!(frame.op().as_str(), "MSG" | "HMSG") {
                    let subject = frame.args()[1].to_string();
                    if state.lock().unwrap().dropped_replies.remove(&subject) {
                        continue;
                    }
                }
                if write_all(&client_write, &frame.bytes()).await.is_err() {
                    break;
                }
            }
        })
    };

    // Client to server: apply the faults to JetStream API requests.
    let mut reader = BufReader::new(client_read);
    let mut inbox_sids: HashMap<String, String> = HashMap::new();
    let result: AnyResult<()> = async {
        while let Some(frame) = read_frame(&mut reader).await? {
            let op = frame.op();
            let args = frame.args();
            if op == "SUB" {
                inbox_sids.insert(args[1].to_string(), args.last().unwrap().to_string());
            }
            if op != "PUB" && op != "HPUB" {
                server_write.write_all(&frame.bytes()).await?;
                continue;
            }

            let subject = args[1];
            let has_reply = (op == "PUB" && args.len() == 4) || (op == "HPUB" && args.len() == 5);
            let reply = has_reply.then(|| args[2]);
            let mut to_server = Vec::new();
            let mut to_client = Vec::new();
            let mut delay = None;
            let mut request_gate = None;
            {
                let mut state = state.lock().unwrap();
                if consumer_name(subject, "MSG.NEXT").is_some() {
                    state.pulls += 1;
                    let status = if state.faults.fail_pulls {
                        Some("400 Bad Request")
                    } else if state.faults.consumer_deleted_on_first_pull && state.pulls == 1 {
                        Some("409 Consumer Deleted")
                    } else {
                        None
                    };
                    match (status, reply.and_then(|reply| inbox_sids.get(reply))) {
                        (Some(status), Some(sid)) => {
                            to_client.push(status_message(reply.unwrap(), sid, status))
                        }
                        _ => to_server.push(frame.bytes()),
                    }
                } else if let Some(name) = consumer_name(subject, "CREATE") {
                    state.creates += 1;
                    let n = state.creates;
                    if state.faults.drop_create_reply == Some(n)
                        && let Some(reply) = reply
                    {
                        state.dropped_replies.insert(reply.to_string());
                    }
                    if state.faults.hold_create_until_delete == Some(n) {
                        state.held_create = Some((name.to_string(), frame.bytes()));
                    } else {
                        to_server.push(frame.bytes());
                    }
                } else if let Some(name) = consumer_name(subject, "DELETE") {
                    state.deletes += 1;
                    request_gate = state.delete_gate.clone();
                    delay = state.faults.delay_deletes;
                    to_server.push(frame.bytes());
                    if state
                        .held_create
                        .as_ref()
                        .is_some_and(|(held, _)| held == name)
                    {
                        let (_, create) = state.held_create.take().unwrap();
                        to_server.push(create);
                    }
                } else {
                    if subject.starts_with("$JS.API.STREAM.INFO.") {
                        request_gate = state.stream_info_gate.clone();
                    }
                    if subject.starts_with("$JS.API.STREAM.INFO.")
                        && state.stream_info_replies_to_drop > 0
                        && let Some(reply) = reply
                    {
                        state.stream_info_replies_to_drop -= 1;
                        state.dropped_replies.insert(reply.to_string());
                    }
                    to_server.push(frame.bytes());
                }
            }
            if let Some(gate) = request_gate {
                // Closing the semaphore releases all held deletes. Never
                // hold the synchronous state lock across this await.
                let _ = gate.acquire().await;
            }
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            for bytes in to_server {
                server_write.write_all(&bytes).await?;
                // Keep the delete and the released create in separate
                // requests on the server side.
                server_write.flush().await?;
            }
            for bytes in to_client {
                write_all(&client_write, &bytes)
                    .await
                    .map_err(|e| anyhow!("write to client: {e}"))?;
            }
        }
        Ok(())
    }
    .await;
    to_client.abort();
    result
}

async fn write_all(
    writer: &Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
    bytes: &[u8],
) -> io::Result<()> {
    writer.lock().await.write_all(bytes).await
}

#[cfg(test)]
mod tests {
    use super::consumer_name;

    #[test]
    fn consumer_name_parses_jetstream_api_subjects() {
        assert_eq!(
            consumer_name("$JS.API.CONSUMER.CREATE.str.c1.sub", "CREATE"),
            Some("c1")
        );
        assert_eq!(
            consumer_name("$JS.API.CONSUMER.CREATE.str.c1", "CREATE"),
            Some("c1")
        );
        assert_eq!(
            consumer_name("$JS.API.CONSUMER.DELETE.str.c1", "DELETE"),
            Some("c1")
        );
        assert_eq!(
            consumer_name("$JS.API.CONSUMER.MSG.NEXT.str.c1", "MSG.NEXT"),
            Some("c1")
        );
        assert_eq!(consumer_name("$JS.API.STREAM.INFO.str", "CREATE"), None);
        assert_eq!(
            consumer_name("$JS.API.CONSUMER.DELETE.str.c1", "CREATE"),
            None
        );
    }
}
