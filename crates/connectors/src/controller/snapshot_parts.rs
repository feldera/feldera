//! Moving parts of a snapshot between the hosts of a multihost pipeline.
//!
//! A materialized view's integral is spread over the workers of all hosts.  To
//! send a snapshot of the view, the host with the connector fetches each other
//! worker's part through `/coordination/adhoc/scan` with
//! [AdHocScanFormat::Batches].  The reply is a sequence of chunks that
//! `SerBatchReader::serialize_chunks` wrote, each framed by [frame_chunk], which
//! [ChunkDecoder] splits apart again.
//!
//! [AdHocScanFormat::Batches]: feldera_types::coordination::AdHocScanFormat::Batches

use std::{
    collections::HashMap,
    ops::Range,
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::{Context, Result as AnyResult, anyhow, bail};
use bytes::{Buf, Bytes, BytesMut};
use feldera_mtls::PipelinePeerTls;
use feldera_types::{
    coordination::{AdHocScan, AdHocScanFormat, SnapshotLease, SnapshotLeaseId, Step},
    program_schema::SqlIdentifier,
};
use reqwest::{Client, StatusCode, Url};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::mpsc::Sender;
use tracing::{info, warn};
use uuid::Uuid;

use feldera_adapterlib::catalog::{SerBatchReader, SerBatchReaderHandle, SnapshotReaders};
use std::sync::Arc;

/// The parts of a snapshot, in the order to send them, as they arrive.  An
/// error ends the snapshot.  The sender closes the channel after the last part.
pub type SnapshotParts = tokio::sync::mpsc::Receiver<AnyResult<Arc<dyn SerBatchReader>>>;

/// About how many bytes of records each chunk holds.
pub const CHUNK_BYTES: usize = 1 << 20;

/// How many chunks can wait in each of the two queues between the fetch of another host's part
/// and the encoder: one queue holds the fetched bytes, and the other holds the deserialized
/// parts.  So a snapshot in progress holds about `2 * QUEUED_CHUNKS * CHUNK_BYTES` bytes of other
/// hosts' data.  A larger value lets the fetch and the encoder overlap more.  This host's own
/// parts also wait in the second queue, but they use no more memory, because the host already
/// holds them.
pub const QUEUED_CHUNKS: usize = 4;

/// The number of bytes of the length that precedes each chunk.
const LENGTH_BYTES: usize = size_of::<u64>();

/// Returns `chunk` preceded by its length, as an 8-byte little-endian integer.
pub fn frame_chunk(chunk: &[u8]) -> Bytes {
    let mut framed = BytesMut::with_capacity(LENGTH_BYTES + chunk.len());
    framed.extend_from_slice(&(chunk.len() as u64).to_le_bytes());
    framed.extend_from_slice(chunk);
    framed.freeze()
}

/// Splits a stream of chunks that [frame_chunk] framed, which arrives in
/// pieces of any size, back into chunks.
#[derive(Default)]
pub struct ChunkDecoder {
    buffer: BytesMut,
}

impl ChunkDecoder {
    /// Adds the next piece of the stream.
    pub fn push(&mut self, piece: &[u8]) {
        self.buffer.extend_from_slice(piece);
    }

    /// Returns the next whole chunk, or `None` if the stream so far ends
    /// partway through one.
    pub fn next_chunk(&mut self) -> Option<Bytes> {
        let length_bytes = self.buffer.get(..LENGTH_BYTES)?;
        let length = u64::from_le_bytes(length_bytes.try_into().unwrap()) as usize;
        if self.buffer.len() < LENGTH_BYTES + length {
            return None;
        }
        self.buffer.advance(LENGTH_BYTES);
        Some(self.buffer.split_to(length).freeze())
    }

    /// Checks that the stream ended between chunks, not partway through one.
    pub fn finish(&self) -> AnyResult<()> {
        if self.buffer.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(
                "the snapshot part ended with {} bytes of an incomplete chunk",
                self.buffer.len()
            ))
        }
    }
}

/// How long a snapshot lease may go without a scan before its host warns
/// about it, and how often the host repeats the warning.
pub const IDLE_WARNING: Duration = Duration::from_secs(5 * 60);

/// How long a snapshot lease may go without a scan before its host drops it.
pub const IDLE_EXPIRY: Duration = Duration::from_secs(60 * 60);

/// The snapshot leases that the other hosts of a multihost pipeline hold on
/// this host.
///
/// A lease keeps one step's snapshot until its holder releases it, not until
/// a connection drops, so that the holder can retry a scan that a network
/// error cut short.  A lease is in use while a scan runs with it.  A lease
/// that goes unused for [IDLE_WARNING] gets a warning in the log, again every
/// [IDLE_WARNING], and one that goes unused for [IDLE_EXPIRY] is dropped,
/// because a lease pins its snapshot's batches in memory and on storage.
pub struct SnapshotLeases {
    leases: Mutex<HashMap<Uuid, Lease>>,
    idle_warning: Duration,
    idle_expiry: Duration,

    /// For tests, refuses new leases, to act like a host that does not answer.
    #[cfg(any(test, feature = "testing"))]
    refuse_leases: std::sync::atomic::AtomicBool,

    /// For tests, refuses scans, to act like a host whose scans fail.
    #[cfg(any(test, feature = "testing"))]
    refuse_scans: std::sync::atomic::AtomicBool,
}

struct Lease {
    step: Step,

    /// The leased relation, and its readers at `step`.
    table: SqlIdentifier,
    readers: SnapshotReaders,

    /// The number of scans that use the lease now.
    scans: usize,

    /// When the last scan ended, or when the lease was taken.
    idle_since: Instant,

    /// When the last warning about this lease was logged.
    warned_at: Option<Instant>,
}

/// What [SnapshotLeases::check] did to a lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdleAction {
    /// Logged a warning.
    Warned,
    /// Dropped the lease.
    Expired,
}

impl Default for SnapshotLeases {
    fn default() -> Self {
        Self::with_limits(IDLE_WARNING, IDLE_EXPIRY)
    }
}

impl SnapshotLeases {
    /// Returns an empty set of leases that warns and expires after the given
    /// idle times.
    pub fn with_limits(idle_warning: Duration, idle_expiry: Duration) -> Self {
        Self {
            leases: Mutex::new(HashMap::new()),
            idle_warning,
            idle_expiry,
            #[cfg(any(test, feature = "testing"))]
            refuse_leases: Default::default(),
            #[cfg(any(test, feature = "testing"))]
            refuse_scans: Default::default(),
        }
    }

    /// For tests, makes [Self::take] refuse leases and [Self::start_scan]
    /// refuse scans, or stop refusing them.
    #[cfg(any(test, feature = "testing"))]
    pub fn set_refusing(&self, leases: bool, scans: bool) {
        use std::sync::atomic::Ordering::Relaxed;
        self.refuse_leases.store(leases, Relaxed);
        self.refuse_scans.store(scans, Relaxed);
    }

    /// Takes a lease on `readers`, which are relation `table`'s at step `step`,
    /// and returns its id, or `None` if a test made this host refuse leases.
    pub fn take(
        &self,
        step: Step,
        table: SqlIdentifier,
        readers: SnapshotReaders,
        now: Instant,
    ) -> Option<Uuid> {
        #[cfg(any(test, feature = "testing"))]
        if self
            .refuse_leases
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return None;
        }
        let id = Uuid::now_v7();
        self.leases.lock().unwrap().insert(
            id,
            Lease {
                step,
                table,
                readers,
                scans: 0,
                idle_since: now,
                warned_at: None,
            },
        );
        Some(id)
    }

    /// Releases lease `id`.  Returns false if there is no such lease, for
    /// example because it was released already, which is not an error.
    pub fn release(&self, id: Uuid) -> bool {
        self.leases.lock().unwrap().remove(&id).is_some()
    }

    /// Starts a scan with lease `id`, if it exists.  Returns the leased step,
    /// relation, and readers.  The lease is in use until `end_scan`.
    pub fn start_scan(&self, id: Uuid) -> Option<(Step, SqlIdentifier, SnapshotReaders)> {
        #[cfg(any(test, feature = "testing"))]
        if self.refuse_scans.load(std::sync::atomic::Ordering::Relaxed) {
            return None;
        }
        let mut leases = self.leases.lock().unwrap();
        let lease = leases.get_mut(&id)?;
        lease.scans += 1;
        Some((lease.step, lease.table.clone(), lease.readers.clone()))
    }

    /// Ends a scan that `start_scan` started with lease `id`.  The lease's
    /// idle time starts over at `now`.
    pub fn end_scan(&self, id: Uuid, now: Instant) {
        if let Some(lease) = self.leases.lock().unwrap().get_mut(&id) {
            lease.scans -= 1;
            lease.idle_since = now;
            lease.warned_at = None;
        }
    }

    /// Logs a warning about each lease that has gone unused for the warning
    /// time since it was last used or last warned about, and drops each one
    /// that has gone unused for the expiry time.  Returns what it did.
    pub fn check(&self, now: Instant) -> Vec<(Uuid, IdleAction)> {
        let mut actions = Vec::new();
        self.leases.lock().unwrap().retain(|id, lease| {
            if lease.scans > 0 {
                return true;
            }
            let idle = now.saturating_duration_since(lease.idle_since);
            if idle >= self.idle_expiry {
                info!(
                    "dropping snapshot lease {id} on step {}, which went unused for {} s",
                    lease.step,
                    idle.as_secs()
                );
                actions.push((*id, IdleAction::Expired));
                return false;
            }
            let last = lease.warned_at.unwrap_or(lease.idle_since);
            if now.saturating_duration_since(last) >= self.idle_warning {
                warn!(
                    "snapshot lease {id} on step {} has gone unused for {} s; it will be dropped after {} s",
                    lease.step,
                    idle.as_secs(),
                    self.idle_expiry.as_secs()
                );
                lease.warned_at = Some(now);
                actions.push((*id, IdleAction::Warned));
            }
            true
        });
        actions
    }
}

/// How many times [PeerClient::fetch_part] tries a scan before it gives up.
pub const SCAN_ATTEMPTS: u32 = 5;

/// How long [PeerClient::fetch_part] waits before its first retry.  Each later
/// retry waits twice as long as the one before.
const FIRST_RETRY_DELAY: Duration = Duration::from_millis(200);

/// A client for the snapshot endpoints of the other hosts of a multihost
/// pipeline.
#[derive(Clone)]
pub struct PeerClient {
    client: Client,

    /// The base URL of each host, in host order.
    host_urls: Vec<Url>,

    /// How long [Self::fetch_part] waits before its first retry.
    first_retry_delay: Duration,
}

impl PeerClient {
    /// Returns a client for the hosts at `host_urls`, in host order.  With
    /// `tls`, the client uses the pipeline's peer TLS configuration, which
    /// presents this host's pipeline certificate and admits only hosts of the
    /// same pipeline.  Otherwise, with `root_certificate`, the client trusts
    /// HTTPS certificates that chain to it, as the hosts' certificates do.
    pub fn new(
        host_urls: &[String],
        tls: Option<&PipelinePeerTls>,
        root_certificate: Option<reqwest::Certificate>,
    ) -> AnyResult<Self> {
        let host_urls = host_urls
            .iter()
            .map(|url| Url::parse(url).with_context(|| format!("invalid host URL {url:?}")))
            .collect::<AnyResult<Vec<_>>>()?;
        let mut builder = Client::builder();
        if let Some(tls) = tls {
            builder = builder.use_preconfigured_tls((*tls.client).clone());
        } else if let Some(root_certificate) = root_certificate {
            builder = builder.add_root_certificate(root_certificate);
        }
        Ok(Self {
            client: builder.build()?,
            host_urls,
            first_retry_delay: FIRST_RETRY_DELAY,
        })
    }

    /// Posts `request` as JSON to `path` on `host`.
    async fn post(
        &self,
        host: usize,
        path: &str,
        request: &impl Serialize,
    ) -> AnyResult<reqwest::Response> {
        let url = self.host_urls[host].join(path)?;
        let response = self
            .client
            .post(url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(request)?)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("POST {url} failed with {status}: {body}");
        }
        Ok(response)
    }

    /// Posts `request` to `path` on `host` and returns the reply, as JSON.
    async fn post_json<T: DeserializeOwned>(
        &self,
        host: usize,
        path: &str,
        request: &impl Serialize,
    ) -> AnyResult<T> {
        let body = self.post(host, path, request).await?.bytes().await?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// Takes a lease on step `step`'s snapshot of `table` on `host` and returns
    /// its id.
    pub async fn lease(&self, host: usize, step: Step, table: &SqlIdentifier) -> AnyResult<Uuid> {
        let request = SnapshotLease {
            step,
            table: table.clone(),
        };
        let reply: SnapshotLeaseId = self
            .post_json(host, "coordination/snapshot/lease", &request)
            .await?;
        Ok(reply.id)
    }

    /// Releases lease `id` on `host`.  A failure is only logged: the host
    /// drops a lease that goes unused for long enough.
    pub async fn release(&self, host: usize, id: Uuid) {
        if let Err(error) = self
            .post(
                host,
                "coordination/snapshot/release",
                &SnapshotLeaseId { id },
            )
            .await
        {
            warn!("releasing snapshot lease {id} on host {host}: {error:#}");
        }
    }

    /// Scans once, sending the chunks of `table` for `worker` in the step of
    /// lease `lease`, after the first `skip` of them, to `chunks`.  Returns the
    /// number of chunks sent, with any error that cut the scan short.
    async fn scan_once(
        &self,
        host: usize,
        scan: &AdHocScan,
        chunks: &Sender<Bytes>,
    ) -> (u64, AnyResult<()>) {
        let mut sent = 0;
        let result = async {
            let mut response = self.post(host, "coordination/adhoc/scan", scan).await?;
            let mut decoder = ChunkDecoder::default();
            while let Some(piece) = response.chunk().await? {
                decoder.push(&piece);
                while let Some(chunk) = decoder.next_chunk() {
                    chunks
                        .send(chunk)
                        .await
                        .map_err(|_| anyhow!("the snapshot's reader went away"))?;
                    sent += 1;
                }
            }
            decoder.finish()
        }
        .await;
        (sent, result)
    }

    /// Sends every chunk of `table` for `worker`, which is on `host`, in step
    /// `step` of lease `lease`, to `chunks`, in order and each exactly once.
    ///
    /// A scan that fails partway is tried again, up to [SCAN_ATTEMPTS] times
    /// in all, and the retry skips the chunks that were already sent.  A
    /// failure stops counting against the limit once a scan makes progress.
    pub async fn fetch_part(
        &self,
        host: usize,
        step: Step,
        lease: Uuid,
        table: &SqlIdentifier,
        worker: usize,
        chunks: &Sender<Bytes>,
    ) -> AnyResult<()> {
        let mut scan = AdHocScan {
            step,
            worker,
            table: table.clone(),
            projection: None,
            format: AdHocScanFormat::Batches,
            lease: Some(lease),
            skip_chunks: 0,
        };
        let mut failures = 0;
        let mut delay = self.first_retry_delay;
        loop {
            let (sent, result) = self.scan_once(host, &scan, chunks).await;
            scan.skip_chunks += sent;
            let Err(error) = result else {
                return Ok(());
            };
            if chunks.is_closed() {
                return Err(error);
            }
            if sent > 0 {
                failures = 0;
                delay = self.first_retry_delay;
            }
            failures += 1;
            if failures >= SCAN_ATTEMPTS {
                return Err(error.context(format!(
                    "scan of {table} on host {host}, worker {worker} failed {failures} times in a row"
                )));
            }
            warn!(
                "scan of {table} on host {host}, worker {worker} failed after {} chunks, retrying in {} ms: {error:#}",
                scan.skip_chunks,
                delay.as_millis()
            );
            tokio::time::sleep(delay).await;
            delay *= 2;
        }
    }
}

/// Sends the parts of `table`'s snapshot of step `step` to `parts`: first
/// `local`, this host's parts in worker order, then each other host's, which
/// `client` fetches under the lease in `leases` for that host, in host and
/// worker order.  `hosts` lists each host's workers, and `handle` is
/// `table`'s integral, which reads the fetched chunks.
#[allow(clippy::too_many_arguments)]
pub async fn send_snapshot_parts(
    client: &PeerClient,
    step: Step,
    table: &SqlIdentifier,
    handle: &dyn SerBatchReaderHandle,
    local: Vec<Arc<dyn SerBatchReader>>,
    hosts: &[Range<usize>],
    leases: &[(usize, Uuid)],
    parts: &Sender<AnyResult<Arc<dyn SerBatchReader>>>,
) -> AnyResult<()> {
    let closed = || anyhow!("the output connector stopped reading the snapshot");
    for part in local {
        parts.send(Ok(part)).await.map_err(|_| closed())?;
    }
    for &(host, lease) in leases {
        for worker in hosts[host].clone() {
            let (chunk_sender, mut chunks) = tokio::sync::mpsc::channel::<Bytes>(QUEUED_CHUNKS);
            let fetch = async move {
                let result = client
                    .fetch_part(host, step, lease, table, worker, &chunk_sender)
                    .await;
                drop(chunk_sender);
                result
            };
            let forward = async {
                while let Some(chunk) = chunks.recv().await {
                    // SAFETY: the chunk comes from a host of this pipeline,
                    // which runs the same program, and `serialize_chunks` for
                    // the same relation wrote it.  `deserialize_chunk` reads
                    // its rkyv archives without validating them, as dbsp's
                    // exchange does with other hosts' batches.
                    let part = unsafe { handle.deserialize_chunk(&chunk) };
                    parts.send(Ok(part)).await.map_err(|_| closed())?;
                }
                Ok::<_, anyhow::Error>(())
            };
            let (fetched, forwarded) = tokio::join!(fetch, forward);
            fetched?;
            forwarded?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use super::{ChunkDecoder, IdleAction, SnapshotLeases, frame_chunk};
    use feldera_adapterlib::catalog::SnapshotReaders;

    /// A relation with no records, for leases that no scan reads.
    struct EmptyReader;
    impl feldera_adapterlib::catalog::SerBatchReader for EmptyReader {
        fn key_count(&self) -> usize {
            0
        }
        fn len(&self) -> usize {
            0
        }
        fn cursor<'a>(
            &'a self,
            _record_format: feldera_adapterlib::catalog::RecordFormat,
        ) -> Result<
            Box<dyn feldera_adapterlib::catalog::SerCursor + Send + 'a>,
            feldera_adapterlib::errors::controller::ControllerError,
        > {
            unimplemented!()
        }
        fn batches(&self) -> Vec<Arc<dyn feldera_adapterlib::catalog::SerBatch>> {
            unimplemented!()
        }
        fn serialize_chunks(
            &self,
            _chunk_bytes: usize,
            _emit: &mut dyn FnMut(&[u8]) -> anyhow::Result<()>,
        ) -> anyhow::Result<()> {
            unimplemented!()
        }
        fn snapshot(&self) -> Arc<dyn feldera_adapterlib::catalog::SerBatchReader> {
            unimplemented!()
        }
        fn split(
            self: Arc<Self>,
            _num_ranges: usize,
            _record_format: feldera_adapterlib::catalog::RecordFormat,
        ) -> anyhow::Result<Vec<Box<dyn feldera_adapterlib::catalog::SerKeyRange>>> {
            unimplemented!()
        }
    }

    /// Takes a lease on step `step` of an empty relation `v`.
    fn take(leases: &SnapshotLeases, step: u64, now: Instant) -> Option<uuid::Uuid> {
        let readers = SnapshotReaders {
            whole: Arc::new(EmptyReader),
            partitions: Vec::new(),
        };
        leases.take(step, "v".into(), readers, now)
    }

    /// A lease lasts until it is released, a second release does nothing, and
    /// a scan cannot use a released lease.
    #[test]
    fn lease_lasts_until_released() {
        let leases = SnapshotLeases::default();
        let now = Instant::now();
        let id = take(&leases, 7, now).unwrap();
        assert_eq!(
            leases.start_scan(id).map(|(step, table, _)| (step, table)),
            Some((7, "v".into()))
        );
        leases.end_scan(id, now);
        assert!(leases.release(id));
        assert!(!leases.release(id));
        assert!(leases.start_scan(id).is_none());
    }

    /// An unused lease gets a warning after each warning time and is dropped
    /// after the expiry time, but a lease that a scan uses is left alone, and
    /// a scan starts its idle time over.
    #[test]
    fn idle_leases_warn_then_expire() {
        let minute = Duration::from_secs(60);
        let leases = SnapshotLeases::with_limits(5 * minute, 60 * minute);
        let start = Instant::now();
        let idle = take(&leases, 1, start).unwrap();
        let busy = take(&leases, 1, start).unwrap();
        leases.start_scan(busy).unwrap();

        assert_eq!(leases.check(start + 4 * minute), []);
        assert_eq!(
            leases.check(start + 5 * minute),
            [(idle, IdleAction::Warned)]
        );
        assert_eq!(leases.check(start + 9 * minute), []);
        assert_eq!(
            leases.check(start + 10 * minute),
            [(idle, IdleAction::Warned)]
        );

        // A scan starts the idle time over.
        leases.start_scan(idle).unwrap();
        leases.end_scan(idle, start + 30 * minute);
        assert_eq!(leases.check(start + 34 * minute), []);
        assert_eq!(
            leases.check(start + 35 * minute),
            [(idle, IdleAction::Warned)]
        );

        assert_eq!(
            leases.check(start + 90 * minute),
            [(idle, IdleAction::Expired)]
        );
        assert!(leases.start_scan(idle).is_none());

        // The busy lease was never warned about or dropped.
        assert!(leases.start_scan(busy).is_some());
    }

    /// Chunks come back whole and in order, however the stream is cut into
    /// pieces, and an empty chunk is still a chunk.
    #[test]
    fn chunks_survive_any_cut() {
        let chunks: Vec<Vec<u8>> =
            vec![b"first".to_vec(), Vec::new(), vec![7; 1000], b"x".to_vec()];
        let stream: Vec<u8> = chunks.iter().flat_map(|chunk| frame_chunk(chunk)).collect();

        for piece_size in [1, 3, 8, 9, 100, stream.len()] {
            let mut decoder = ChunkDecoder::default();
            let mut decoded = Vec::new();
            for piece in stream.chunks(piece_size) {
                decoder.push(piece);
                while let Some(chunk) = decoder.next_chunk() {
                    decoded.push(chunk.to_vec());
                }
            }
            decoder.finish().unwrap();
            assert_eq!(decoded, chunks, "pieces of {piece_size} bytes");
        }
    }

    /// A stream that stops partway through a chunk is an error, not a short
    /// snapshot.
    #[test]
    fn truncated_stream_is_an_error() {
        let stream = frame_chunk(b"0123456789");
        for end in 1..stream.len() {
            let mut decoder = ChunkDecoder::default();
            decoder.push(&stream[..end]);
            assert!(decoder.next_chunk().is_none());
            assert!(decoder.finish().is_err(), "cut at {end}");
        }
    }

    /// Serves `/coordination/adhoc/scan` from `chunks` on localhost, as a host
    /// does, honoring each request's `skip_chunks`.  Connection `i` sends only
    /// the first `cuts[i]` bytes of its reply body and then closes, or the whole
    /// body if `cuts` has no entry `i`.  Returns the URL and a list of each
    /// request's `skip_chunks`.
    async fn scan_server(
        chunks: Vec<Vec<u8>>,
        cuts: Vec<usize>,
    ) -> (String, Arc<std::sync::Mutex<Vec<u64>>>) {
        use feldera_types::coordination::AdHocScan;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let skips = Arc::new(std::sync::Mutex::new(Vec::new()));
        tokio::spawn({
            let skips = skips.clone();
            async move {
                for connection in 0..usize::MAX {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let (header_end, length) = loop {
                        let mut buffer = [0; 4096];
                        let n = stream.read(&mut buffer).await.unwrap();
                        request.extend_from_slice(&buffer[..n]);
                        let text = String::from_utf8_lossy(&request).to_ascii_lowercase();
                        if let Some(end) = text.find("\r\n\r\n") {
                            let length = text
                                .lines()
                                .find_map(|line| line.strip_prefix("content-length:"))
                                .unwrap()
                                .trim()
                                .parse::<usize>()
                                .unwrap();
                            break (end + 4, length);
                        }
                    };
                    while request.len() < header_end + length {
                        let mut buffer = [0; 4096];
                        let n = stream.read(&mut buffer).await.unwrap();
                        request.extend_from_slice(&buffer[..n]);
                    }
                    let scan: AdHocScan =
                        serde_json::from_slice(&request[header_end..header_end + length]).unwrap();
                    skips.lock().unwrap().push(scan.skip_chunks);

                    let body: Vec<u8> = chunks[scan.skip_chunks as usize..]
                        .iter()
                        .flat_map(|chunk| frame_chunk(chunk))
                        .collect();
                    let end = cuts
                        .get(connection)
                        .copied()
                        .unwrap_or(body.len())
                        .min(body.len());
                    let header = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(header.as_bytes()).await.unwrap();
                    stream.write_all(&body[..end]).await.unwrap();
                    stream.shutdown().await.unwrap();
                }
            }
        });
        (url, skips)
    }

    /// Without client certificates, a host reaches another host's HTTPS server
    /// by trusting the root CA of the pipeline certificate chain, which is not
    /// one of the public roots.
    #[actix_web::test]
    async fn https_lease_trusts_the_pipeline_root() {
        use actix_web::{App, HttpResponse, HttpServer, web};
        use feldera_types::coordination::SnapshotLeaseId;
        use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut ca_params = rcgen::CertificateParams::default();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_pem = ca_params.clone().self_signed(&ca_key).unwrap().pem();
        let mut leaf_params = rcgen::CertificateParams::default();
        leaf_params.subject_alt_names =
            vec![rcgen::SanType::DnsName("localhost".try_into().unwrap())];
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf = leaf_params
            .signed_by(&leaf_key, &rcgen::Issuer::new(ca_params, ca_key))
            .unwrap();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
            )
            .unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!(
            "https://localhost:{}/",
            listener.local_addr().unwrap().port()
        );
        let server = HttpServer::new(|| {
            App::new().route(
                "/coordination/snapshot/lease",
                web::post().to(|| async {
                    HttpResponse::Ok().json(SnapshotLeaseId {
                        id: uuid::Uuid::nil(),
                    })
                }),
            )
        })
        .workers(1)
        .listen_rustls_0_23(listener, server_config)
        .unwrap()
        .run();
        let handle = server.handle();
        actix_web::rt::spawn(server);

        let lease = |root: Option<reqwest::Certificate>| {
            let client = super::PeerClient::new(std::slice::from_ref(&url), None, root).unwrap();
            async move { client.lease(0, 4, &"v".into()).await }
        };

        // With the public roots alone, the host's certificate is refused.
        assert!(lease(None).await.is_err());

        let root = reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap();
        assert_eq!(lease(Some(root)).await.unwrap(), uuid::Uuid::nil());

        handle.stop(false).await;
    }
    /// Fetches worker 0's part from `url` as `client` does, and returns the
    /// chunks it got, or the error.
    async fn fetch(url: &str) -> (Vec<Vec<u8>>, anyhow::Result<()>) {
        let mut client = super::PeerClient::new(&[url.to_string()], None, None).unwrap();
        client.first_retry_delay = Duration::from_millis(1);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
        let result = client
            .fetch_part(0, 3, uuid::Uuid::nil(), &"v".into(), 0, &sender)
            .await;
        drop(sender);
        let mut received = Vec::new();
        while let Some(chunk) = receiver.recv().await {
            received.push(chunk.to_vec());
        }
        (received, result)
    }

    /// A scan whose connection drops partway is retried, and the retry skips
    /// the chunks that arrived, wherever the cut falls: inside a length,
    /// inside a chunk, or between chunks.  Every chunk arrives once, in order.
    #[tokio::test]
    async fn dropped_scan_resumes_where_it_stopped() {
        let chunks: Vec<Vec<u8>> = (0..6u8).map(|i| vec![i; 10 + i as usize]).collect();
        // Chunk 0 is framed as 18 bytes, chunk 1 as 19.
        let (url, skips) = scan_server(chunks.clone(), vec![3, 12, 18, 0, 19]).await;
        let (received, result) = fetch(&url).await;
        result.unwrap();
        assert_eq!(received, chunks);
        // No chunk, no chunk, chunk 0 (then a cut at a boundary), nothing, chunk 1.
        assert_eq!(*skips.lock().unwrap(), [0, 0, 0, 1, 1, 2]);
    }

    /// A scan that keeps failing without progress gives up after
    /// `SCAN_ATTEMPTS` tries.
    #[tokio::test]
    async fn scan_that_keeps_failing_gives_up() {
        let chunks = vec![vec![1; 10]];
        let (url, skips) = scan_server(chunks, vec![3; super::SCAN_ATTEMPTS as usize + 1]).await;
        let (received, result) = fetch(&url).await;
        assert!(received.is_empty());
        assert!(result.is_err());
        assert_eq!(skips.lock().unwrap().len(), super::SCAN_ATTEMPTS as usize);
    }
}
