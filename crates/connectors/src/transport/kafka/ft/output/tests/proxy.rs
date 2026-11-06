//! Fault injection for the single-broker Kafka test fixture. Only clients
//! bootstrapped through this proxy are disconnected or have replies withheld.

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const FETCH: i16 = 1;
const METADATA: i16 = 3;
const FIND_COORDINATOR: i16 = 10;
const API_VERSIONS: i16 = 18;
const END_TXN: i16 = 26;

#[derive(Default)]
struct State {
    offline: AtomicBool,
    stopped: AtomicBool,
    hold_commit: AtomicBool,
    held_commits: AtomicUsize,
    fetches: AtomicUsize,
    passed_end_txn_errors: AtomicUsize,
    sockets: Mutex<Vec<TcpStream>>,
}

impl State {
    fn disconnect(&self) {
        for socket in self.sockets.lock().unwrap().drain(..) {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }
}

pub(super) struct BrokerProxy {
    pub address: String,
    state: Arc<State>,
    listener: Option<JoinHandle<()>>,
}

impl BrokerProxy {
    pub fn new(upstream: String) -> Self {
        let upstream = if upstream.contains(':') {
            upstream
        } else {
            format!("{upstream}:9092")
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(State::default());
        let serving = state.clone();
        let handle = thread::spawn(move || {
            let mut connections = Vec::new();
            while !serving.stopped.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((client, _)) => {
                        if serving.offline.load(Ordering::Acquire) {
                            continue;
                        }
                        let broker = TcpStream::connect(&upstream).unwrap();
                        // Accepted sockets inherit O_NONBLOCK on macOS.
                        client.set_nonblocking(false).unwrap();
                        client.set_nodelay(true).unwrap();
                        broker.set_nodelay(true).unwrap();
                        {
                            let mut sockets = serving.sockets.lock().unwrap();
                            if serving.offline.load(Ordering::Acquire)
                                || serving.stopped.load(Ordering::Acquire)
                            {
                                continue;
                            }
                            sockets.push(client.try_clone().unwrap());
                            sockets.push(broker.try_clone().unwrap());
                        }
                        let serving = serving.clone();
                        connections.push(thread::spawn(move || {
                            // Disconnects are intentional. Protocol parsing panics
                            // propagate through the joins at teardown.
                            let mut client = client;
                            let mut broker = broker;
                            let _ = forward(&mut client, &mut broker, address.port(), &serving);
                            let _ = client.shutdown(Shutdown::Both);
                            let _ = broker.shutdown(Shutdown::Both);
                        }));
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accepting a Kafka test connection: {error}"),
                }
            }
            serving.disconnect();
            for connection in connections {
                connection.join().unwrap();
            }
        });
        Self {
            address: address.to_string(),
            state,
            listener: Some(handle),
        }
    }

    pub fn disconnect(&self) {
        self.state.offline.store(true, Ordering::Release);
        self.state.disconnect();
    }

    pub fn reconnect(&self) {
        self.state.offline.store(false, Ordering::Release);
    }

    pub fn hold_commit(&self, hold: bool) {
        self.state.hold_commit.store(hold, Ordering::Release);
    }

    pub fn held_commits(&self) -> usize {
        self.state.held_commits.load(Ordering::Acquire)
    }

    /// Fetch requests forwarded so far, from consumers bootstrapped here.
    pub fn fetches(&self) -> usize {
        self.state.fetches.load(Ordering::Acquire)
    }

    /// Number of EndTxn error replies passed to the client while holding.
    pub fn passed_end_txn_errors(&self) -> usize {
        self.state.passed_end_txn_errors.load(Ordering::Acquire)
    }
}

impl Drop for BrokerProxy {
    fn drop(&mut self) {
        self.state.stopped.store(true, Ordering::Release);
        self.state.disconnect();
        let result = self.listener.take().unwrap().join();
        if !thread::panicking() {
            result.unwrap();
        }
    }
}

fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut size = [0; 4];
    stream.read_exact(&mut size)?;
    let size = i32::from_be_bytes(size);
    assert!(
        (0..=64 << 20).contains(&size),
        "invalid Kafka frame size {size}"
    );
    let mut frame = vec![0; size as usize];
    stream.read_exact(&mut frame)?;
    Ok(frame)
}

fn write_frame(stream: &mut TcpStream, frame: &[u8]) -> io::Result<()> {
    stream.write_all(&(frame.len() as i32).to_be_bytes())?;
    stream.write_all(frame)
}

fn forward(
    client: &mut TcpStream,
    broker: &mut TcpStream,
    port: u16,
    state: &State,
) -> io::Result<()> {
    loop {
        let request = read_frame(client)?;
        let api = i16::from_be_bytes(request[..2].try_into().unwrap());
        let version = i16::from_be_bytes(request[2..4].try_into().unwrap());
        write_frame(broker, &request)?;
        let mut response = read_frame(broker)?;
        assert_eq!(&response[..4], &request[4..8], "Kafka correlation id");
        match api {
            FETCH => {
                state.fetches.fetch_add(1, Ordering::Release);
            }
            API_VERSIONS => cap_address_api_versions(&mut response, version),
            METADATA => {
                assert!(version <= 8);
                let mut offset = 4 + if version >= 3 { 4 } else { 0 };
                let brokers = i32::from_be_bytes(response[offset..offset + 4].try_into().unwrap());
                assert_eq!(brokers, 1, "the Kafka test fixture must have one broker");
                offset += 8; // broker count and node id
                redirect(&mut response, offset, port);
            }
            FIND_COORDINATOR => {
                assert!(version <= 2);
                let mut offset = 4;
                if version >= 1 {
                    offset += 4; // throttle time
                }
                let error = i16::from_be_bytes(response[offset..offset + 2].try_into().unwrap());
                offset += 2;
                if version >= 1 {
                    skip_string(&response, &mut offset); // error message
                }
                if error == 0 {
                    redirect(&mut response, offset + 4, port); // node id
                }
            }
            END_TXN if state.hold_commit.load(Ordering::Acquire) => {
                let error = end_txn_error(&response, version);
                if error != 0 {
                    // The broker did not end the transaction. Let the client
                    // see the error and retry; hold the reply that succeeds.
                    eprintln!("proxy: passing EndTxn v{version} error {error} to the client");
                    state.passed_end_txn_errors.fetch_add(1, Ordering::Release);
                    write_frame(client, &response)?;
                    continue;
                }
                // The broker has answered, so the outcome is decided. It may
                // still be writing the commit markers to the data partitions.
                // Hold only the reply so the client cannot know the outcome.
                state.held_commits.fetch_add(1, Ordering::Release);
                while state.hold_commit.load(Ordering::Acquire)
                    && !state.offline.load(Ordering::Acquire)
                    && !state.stopped.load(Ordering::Acquire)
                {
                    thread::sleep(Duration::from_millis(10));
                }
            }
            _ => {}
        }
        write_frame(client, &response)?;
    }
}

/// Returns the error code of an EndTxn response. Version 3 and later use the
/// flexible response header, which has tagged fields after the correlation id.
fn end_txn_error(response: &[u8], version: i16) -> i16 {
    let mut offset = 4; // correlation id
    if version >= 3 {
        skip_tagged_fields(response, &mut offset);
    }
    offset += 4; // throttle time
    i16::from_be_bytes(response[offset..offset + 2].try_into().unwrap())
}

fn skip_tagged_fields(bytes: &[u8], offset: &mut usize) {
    let tags = unsigned_varint(bytes, offset);
    for _ in 0..tags {
        let _tag = unsigned_varint(bytes, offset);
        let size = unsigned_varint(bytes, offset);
        *offset += size;
    }
}

fn skip_string(bytes: &[u8], offset: &mut usize) {
    let size = i16::from_be_bytes(bytes[*offset..*offset + 2].try_into().unwrap());
    *offset += 2 + size.max(0) as usize;
}

fn redirect(response: &mut Vec<u8>, start: usize, port: u16) {
    let mut end = start;
    skip_string(response, &mut end);
    end += 4; // port
    let mut address = Vec::from(9i16.to_be_bytes());
    address.extend_from_slice(b"127.0.0.1");
    address.extend_from_slice(&(port as i32).to_be_bytes());
    response.splice(start..end, address);
}

fn unsigned_varint(bytes: &[u8], offset: &mut usize) -> usize {
    let mut result = 0;
    for shift in (0..35).step_by(7) {
        let byte = bytes[*offset];
        *offset += 1;
        result |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 {
            return result;
        }
    }
    panic!("invalid Kafka unsigned varint");
}

/// Negotiate ordinary strings for just the two responses whose addresses we
/// rewrite. Transaction and produce protocol versions remain unmodified.
fn cap_address_api_versions(response: &mut [u8], version: i16) {
    let mut offset = 6; // correlation id and error code; ApiVersions has no header tags
    let count = if version >= 3 {
        unsigned_varint(response, &mut offset) - 1
    } else {
        let count = i32::from_be_bytes(response[offset..offset + 4].try_into().unwrap());
        offset += 4;
        count as usize
    };
    for _ in 0..count {
        let api = i16::from_be_bytes(response[offset..offset + 2].try_into().unwrap());
        let max_version = &mut response[offset + 4..offset + 6];
        let maximum = i16::from_be_bytes(max_version.try_into().unwrap());
        let cap = match api {
            METADATA => 8,
            FIND_COORDINATOR => 2,
            _ => maximum,
        };
        max_version.copy_from_slice(&maximum.min(cap).to_be_bytes());
        offset += 6;
        if version >= 3 {
            skip_tagged_fields(response, &mut offset);
        }
    }
}

#[test]
fn end_txn_error_reads_every_header_version() {
    // Correlation id, throttle time, error code 51.
    let v0 = [0, 0, 0, 7, 0, 0, 0, 0, 0, 51];
    assert_eq!(end_txn_error(&v0, 0), 51);
    assert_eq!(end_txn_error(&v0, 2), 51);
    // Correlation id, no header tags, throttle time, error code 0, body tags.
    let v3 = [0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0];
    assert_eq!(end_txn_error(&v3, 3), 0);
    // Correlation id, one 2-byte header tag, throttle time, error code 47.
    let tagged = [0, 0, 0, 7, 1, 0, 2, 9, 9, 0, 0, 0, 0, 0, 47, 0];
    assert_eq!(end_txn_error(&tagged, 4), 47);
}
