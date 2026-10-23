use super::InputReaderCommand;
use crate::transport::InputEndpoint;
use crate::{
    InputBuffer, InputConsumer, InputReader, Parser, TransportInputEndpoint, format::StreamSplitter,
};
use anyhow::anyhow;
use anyhow::{Result as AnyResult, bail};
use async_channel::{Receiver, SendError, Sender, bounded, unbounded};
use aws_sdk_s3::operation::{get_object::GetObjectOutput, list_objects_v2::ListObjectsV2Error};
use chrono::{DateTime, Utc};
use feldera_adapterlib::{
    PipelineState,
    format::BufferSize,
    transport::{Resume, Watermark, parse_resume_info},
};
use feldera_storage::tokio::TOKIO;
use feldera_types::transport::s3::S3InputConfig;
use feldera_types::{config::FtModel, program_schema::Relation};
use futures::future::join_all;
#[cfg(any(test, feature = "testing"))]
use mockall::automock;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::{
    collections::{BTreeMap, VecDeque},
    hash::Hasher,
    sync::Arc,
    thread,
};
use tokio::sync::Mutex;
use tokio::sync::watch::{Receiver as WatchReceiver, channel as watch_channel, error::RecvError};
use tracing::{Instrument, error, info_span};

/// Number of object paths in the queue.
/// Must be small, since these paths are considered in-progress and
/// written as part of step metadata.
const OBJECT_QUEUE_LEN: usize = 8;

pub struct S3InputEndpoint {
    config: Arc<S3InputConfig>,
}

impl S3InputEndpoint {
    pub fn new(config: S3InputConfig) -> AnyResult<Self> {
        if !config.no_sign_request {
            match (&config.aws_access_key_id, &config.aws_secret_access_key) {
                (None, None) => {
                    tracing::info!(
                        "both 'aws_access_key_id' and 'aws_secret_access_key' are not set; reading credentials from the environment"
                    );
                }
                (Some(_), None) => bail!(
                    "'aws_access_key_id' set but 'aws_secret_access_key' not set; either set both or unset both to read from the environment"
                ),
                (None, Some(_)) => bail!(
                    "'aws_secret_access_key' set but 'aws_access_key_id' not set; either set both or unset both to read from the environment"
                ),
                _ => {}
            }
        }

        if config.key.is_none() && config.prefix.is_none() {
            bail!("either 'key' or 'prefix' property must be specified");
        }

        if config.key.is_some() && config.prefix.is_some() {
            bail!(
                "connector configuration specifies both 'key' and 'prefix' properties; please specify only one"
            );
        }

        if config.max_concurrent_fetches == 0 {
            bail!(
                "invalid 'max_concurrent_fetches' value: 'max_concurrent_fetches' must be greater than 0"
            );
        }

        Ok(Self {
            config: Arc::new(config),
        })
    }
}

/// 0-based index of the key in the list of keys retrieved from S3.
type KeyIndex = u64;

/// Describes a key that has been partially processed.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct PartiallyProcessedKey {
    /// Key
    key: String,

    /// All records before this offset have been ingested.
    /// None means tha the entire key was processed.
    completed_offset: Option<u64>,
}

struct PartiallyProcessedKeysInner {
    by_index: BTreeMap<KeyIndex, PartiallyProcessedKey>,
    by_key: BTreeMap<String, KeyIndex>,
}

impl PartiallyProcessedKeysInner {
    /// Remove key. Panics if the key is not present.
    fn remove_key(&mut self, key_index: KeyIndex) {
        let key = &self.by_index.get(&key_index).unwrap().key;

        self.by_key.remove(key);
        self.by_index.remove(&key_index);
    }

    /// Insert a new key. Assigns the next sequential index after the last index in the map to the new key.
    fn insert_key(&mut self, key: PartiallyProcessedKey) -> KeyIndex {
        let key_index = self
            .by_index
            .last_key_value()
            .map(|(k, _)| *k + 1)
            .unwrap_or(0);

        self.by_key.insert(key.key.clone(), key_index);
        self.by_index.insert(key_index, key);

        key_index
    }

    /// Remove keys from the start of the map such that all previous keys have been fully processed.
    /// Keeps at least one fully processed key, so we can use it as a starting point for listing
    /// objects after recovery.
    fn cleanup(&mut self) {
        while self.by_index.len() >= 2 {
            let mut iter = self.by_index.iter();
            if iter.next().unwrap().1.completed_offset.is_none()
                && iter.next().unwrap().1.completed_offset.is_none()
            {
                self.remove_key(*self.by_index.first_key_value().unwrap().0);
            } else {
                return;
            }
        }
    }
}

/// Information about partially processed keys.
///
/// Used to restore connector state from a checkpoint.
#[derive(Clone)]
struct PartiallyProcessedKeys {
    inner: Arc<Mutex<PartiallyProcessedKeysInner>>,
}

impl PartiallyProcessedKeys {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PartiallyProcessedKeysInner {
                by_index: BTreeMap::new(),
                by_key: BTreeMap::new(),
            })),
        }
    }

    async fn by_index(&self) -> BTreeMap<KeyIndex, PartiallyProcessedKey> {
        self.inner.lock().await.by_index.clone()
    }

    fn from_by_index(by_index: BTreeMap<KeyIndex, PartiallyProcessedKey>) -> Self {
        let by_key = by_index
            .iter()
            .map(|(key_index, key)| (key.key.clone(), *key_index))
            .collect();

        Self {
            inner: Arc::new(Mutex::new(PartiallyProcessedKeysInner { by_index, by_key })),
        }
    }

    /// Update completed_offset of an _existing_ key.
    /// Prune completed keys if possible.
    async fn update(&self, key_index: KeyIndex, completed_offset: Option<u64>) {
        let mut inner = self.inner.lock().await;

        inner.by_index.get_mut(&key_index).unwrap().completed_offset = completed_offset;
        inner.cleanup();
    }

    /// Return existing key index or create a new entry for the specified key.
    async fn get_or_add_key(&self, key: &str) -> KeyIndex {
        let mut inner = self.inner.lock().await;

        if let Some(key_index) = inner.by_key.get(key) {
            *key_index
        } else {
            inner.insert_key(PartiallyProcessedKey {
                key: key.to_string(),
                completed_offset: Some(0),
            })
        }
    }

    /// Lookup key by index.
    async fn get_key(&self, key_index: KeyIndex) -> Option<PartiallyProcessedKey> {
        self.inner.lock().await.by_index.get(&key_index).cloned()
    }

    /// Returns the key to start listing from or None to start from the beginning.
    /// Called when recovering connector state from a checkpoint.
    async fn start_after(&self) -> Option<String> {
        let inner = self.inner.lock().await;

        if let Some((_, key)) = inner.by_index.first_key_value() {
            if key.completed_offset.is_none() {
                Some(key.key.clone())
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Metadata {
    partially_processed_keys: BTreeMap<KeyIndex, PartiallyProcessedKey>,
}

impl InputEndpoint for S3InputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for S3InputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn crate::InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        seek: Option<serde_json::Value>,
    ) -> anyhow::Result<Box<dyn crate::InputReader>> {
        Ok(Box::new(S3InputReader::new(
            &self.config,
            consumer,
            parser,
            seek,
        )?))
    }
}

// This wrapper around the S3 SDK client allows us to
// mock it for tests (using the mockall crate).
//
// See: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/testing.html
//
// The automock macro generates a `MockS3Client` that we use during tests.
pub(crate) struct S3Client {
    inner: aws_sdk_s3::Client,
}

#[cfg_attr(any(test, feature = "testing"), automock)]
#[async_trait::async_trait]
impl S3Api for S3Client {
    async fn get_object_keys(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<String>,
        continuation_token: Option<String>,
    ) -> anyhow::Result<(Vec<String>, Option<String>)> {
        let res: (Vec<String>, Option<String>) = self
            .inner
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .set_start_after(start_after)
            .set_continuation_token(continuation_token)
            .send()
            .await
            .map(|output| {
                (
                    output
                        .contents()
                        .iter()
                        .map(|object| {
                            object
                                .key()
                                .expect("Objects should always have a key")
                                .to_string()
                        })
                        .collect(),
                    output.next_continuation_token,
                )
            })?;
        Ok(res)
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> anyhow::Result<GetObjectOutput> {
        Ok(self
            .inner
            .get_object()
            .bucket(bucket)
            .key(key)
            .set_range(match (start, end) {
                (0, None) => None,
                (0, Some(end)) => Some(format!("bytes=-{end}")),
                (start, Some(end)) => Some(format!("bytes={start}-{end}")),
                (start, None) => Some(format!("bytes={start}-")),
            })
            .send()
            .await?)
    }
}

#[async_trait::async_trait]
pub(crate) trait S3Api: Send + Sync {
    /// Get all object keys inside a bucket
    async fn get_object_keys(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<String>,
        continuation_token: Option<String>,
    ) -> anyhow::Result<(Vec<String>, Option<String>)>;

    /// Fetch an object by key within a bucket
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> anyhow::Result<GetObjectOutput>;
}

pub(crate) struct S3InputReader {
    sender: Sender<InputReaderCommand>,
}

impl InputReader for S3InputReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        let _ = self.sender.send_blocking(command);
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl Drop for S3InputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

struct QueuedBuffer {
    key_index: KeyIndex,
    key: String,
    start_offset: u64,
    end_offset: Option<u64>,
    buffer: Option<Box<dyn InputBuffer>>,
    timestamp: DateTime<Utc>,
}

impl S3InputReader {
    fn new(
        config: &Arc<S3InputConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        seek: Option<serde_json::Value>,
    ) -> AnyResult<S3InputReader> {
        let resume_info = if let Some(resume_info) = seek {
            Some(parse_resume_info::<Metadata>(&resume_info)?)
        } else {
            None
        };

        let s3_config = to_s3_config(config);
        let client = Arc::new(S3Client {
            inner: aws_sdk_s3::Client::from_conf(s3_config),
        }) as Arc<dyn S3Api>;
        Ok(Self::new_inner(
            config,
            consumer,
            parser,
            client,
            resume_info,
        ))
    }

    pub(crate) fn new_inner(
        config: &Arc<S3InputConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        s3_client: Arc<dyn S3Api>,
        resume_info: Option<Metadata>,
    ) -> S3InputReader {
        let (sender, receiver) = unbounded();
        thread::Builder::new()
            .name("s3-input-tokio-wrapper".to_string())
            .spawn({
                let config = config.clone();
                move || {
                    let span = info_span!("s3_input", bucket = config.bucket_name.clone());
                    TOKIO.block_on(async {
                        let _ = Self::worker_task(
                            s3_client,
                            config,
                            consumer,
                            parser,
                            receiver,
                            resume_info,
                        )
                        .instrument(span)
                        .await;
                    })
                }
            })
            .expect("failed to create S3 input connector tokio wrapper thread");
        S3InputReader { sender }
    }

    async fn worker_task(
        client: Arc<dyn S3Api>,
        config: Arc<S3InputConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        command_receiver: Receiver<InputReaderCommand>,
        resume_info: Option<Metadata>,
    ) -> anyhow::Result<()> {
        let mut command_receiver = BufferedReceiver::new(command_receiver);
        let mut partially_processed_keys = PartiallyProcessedKeys::new();

        if let Some(resume_info) = resume_info {
            partially_processed_keys =
                PartiallyProcessedKeys::from_by_index(resume_info.partially_processed_keys);
        }

        let (tx, rx) = bounded(OBJECT_QUEUE_LEN);
        let config_clone = config.clone();
        let client_clone = client.clone();
        let consumer_clone = consumer.clone();

        let partially_processed_keys_clone = partially_processed_keys.clone();

        // This task fetches object list and sends it to the channel created above.
        tokio::spawn(async move {
            let mut start_after = partially_processed_keys_clone.start_after().await;
            let mut continuation_token = None;
            loop {
                let (objects_to_fetch, next_token): (Vec<String>, Option<String>) =
                    if let Some(prefix) = &config_clone.prefix {
                        let result = client_clone
                            .get_object_keys(
                                &config_clone.bucket_name,
                                prefix,
                                start_after.take(),
                                continuation_token.take(),
                            )
                            .await;
                        match result {
                            Ok(ret) => ret,
                            Err(e) => {
                                error!("Could not fetch object keys (Error: {e:?}).");
                                match e.downcast_ref::<ListObjectsV2Error>() {
                                    // We consider a missing bucket a fatal error
                                    Some(ListObjectsV2Error::NoSuchBucket(_)) => {
                                        consumer_clone.error(true, e, None)
                                    }
                                    _ => consumer_clone.error(
                                        false,
                                        anyhow!("error retrieving object list: {e:?}"),
                                        None,
                                    ),
                                }
                                break;
                            }
                        }
                    } else {
                        // We checked that either key or prefix is set.
                        (vec![config_clone.key.as_ref().unwrap().clone()], None)
                    };
                continuation_token = next_token;
                for key in objects_to_fetch {
                    // Assign sequential index to the key. The key can already be in
                    // `partially_processed_keys_clone`. In this case, an existing index
                    // is returned.
                    let key_index = partially_processed_keys_clone.get_or_add_key(&key).await;
                    tx.send(key_index).await?;
                }
                if continuation_token.is_none() {
                    break;
                }
            }
            drop(tx); // We're done. Close the channel.
            Ok::<(), SendError<_>>(())
        });

        let (status_sender, status_receiver) = watch_channel(PipelineState::Paused);

        let queue = Arc::new(Mutex::new(VecDeque::<QueuedBuffer>::new()));

        let mut join_handles = Vec::new();

        let bucket_name = config.bucket_name.clone();
        let max_retries = config.max_retries;

        // Spawn reader tasks that dequeue object names from the channel, fetch and parse them.
        for _i in 0..config.max_concurrent_fetches {
            let rx = rx.clone();
            let client = client.clone();
            let bucket_name = bucket_name.clone();
            let mut parser = parser.fork();
            let consumer = consumer.clone();
            let queue = queue.clone();
            let mut status_receiver = status_receiver.clone();
            let partially_processed_keys = partially_processed_keys.clone();

            let handle = tokio::task::spawn(async move {
                let mut splitter: StreamSplitter = StreamSplitter::new(parser.splitter());

                'outer: loop {
                    let msg = { rx.recv().await };

                    match msg {
                        Ok(key_index) => {
                            if wait_running(&mut status_receiver).await.is_err() {
                                // channel closed
                                return;
                            }

                            let Some(partially_processed_key) =
                                partially_processed_keys.get_key(key_index).await
                            else {
                                consumer.error(
                                    true,
                                    anyhow!("internal error: invalid key index {key_index}"),
                                    Some("s3-key"),
                                );
                                continue;
                            };

                            let Some(mut start_offset) = partially_processed_key.completed_offset
                            else {
                                // completed_offset is None - key has been fully processed.
                                continue;
                            };

                            // Retry fetching the object for `max_retries`, with exponential
                            // backoff.
                            // If we still fail after that, return the error.
                            async fn retry_fetching_object(
                                client: Arc<dyn S3Api>,
                                bucket_name: &str,
                                key: &str,
                                start_offset: u64,
                                end: Option<u64>,
                                max_retries: u32,
                            ) -> Result<GetObjectOutput, anyhow::Error>
                            {
                                let mut retry_counter = 0;
                                // Max backoff duration is 20 seconds.
                                let backoff = |count: u32| {
                                    Duration::from_millis((100 * 2_u64.pow(count)).max(20000))
                                };

                                loop {
                                    match client
                                        .get_object(bucket_name, key, start_offset, end)
                                        .await
                                    {
                                        Ok(object) => return Ok(object),
                                        Err(e) => {
                                            if retry_counter < max_retries {
                                                let backoff_duration = backoff(retry_counter);
                                                tracing::warn!(
                                                    "could not fetch object '{key}': {e:?}; retrying ({retry_counter}/{max_retries}) in {backoff_duration:?}",
                                                );
                                                tokio::time::sleep(backoff_duration).await;
                                                retry_counter += 1;
                                                continue;
                                            }

                                            return Err(e);
                                        }
                                    }
                                }
                            }

                            let mut object = None;

                            // Initially, set the restarting offset, the offset used when
                            // re-fetching the object, to the start_offset.
                            // On subsequent retries, we might have already made progress, and
                            // shouldn't start from scratch.
                            let mut latest_start_offset = start_offset;
                            let mut latest_buf = Vec::new();
                            let mut reading_errors = 0;

                            splitter.seek(start_offset);
                            let mut eoi = false;
                            while !eoi {
                                if wait_running(&mut status_receiver).await.is_err() {
                                    // channel closed
                                    return;
                                }

                                // Use the time when we start reading the next chunk of the object as the timestamp
                                // for all buffers derived from this chunk.
                                let timestamp = Utc::now();

                                // Flag to indicate if we had a connection failure and reconnected when reading the object.
                                let mut reconnected = false;
                                let mut recovery_buffer = None;

                                'object_read: loop {
                                    if object.is_none() {
                                        match retry_fetching_object(
                                            client.clone(),
                                            &bucket_name,
                                            &partially_processed_key.key,
                                            latest_start_offset,
                                            None,
                                            max_retries,
                                        )
                                        .await
                                        {
                                            Ok(obj) => object = Some(obj),
                                            Err(e) => {
                                                consumer.error(
                                                    false,
                                                    anyhow!(
                                                        "could not fetch object '{}': {e:?}",
                                                        partially_processed_key.key
                                                    ),
                                                    Some("s3-obj-fetch"),
                                                );

                                                // Mark key as fully processed.
                                                partially_processed_keys
                                                    .update(key_index, None)
                                                    .await;

                                                continue 'outer;
                                            }
                                        };

                                        reconnected = true;
                                    }

                                    match object.as_mut().unwrap().body.next().await {
                                        Some(Err(e)) => {
                                            reading_errors += 1;

                                            let error_offset =
                                                latest_start_offset + latest_buf.len() as u64;
                                            tracing::warn!(
                                                "error reading object '{}' at offset '{error_offset}': {e:?}; retrying: ({reading_errors}/{max_retries})",
                                                &partially_processed_key.key
                                            );

                                            if reading_errors > max_retries {
                                                consumer.error(
                                                    false,
                                                    anyhow!(
                                                        "error reading object '{}' at offset '{error_offset}': {e:?}; max retries '{max_retries}' exceeded, this object is partially processed",
                                                        partially_processed_key.key,
                                                    ),
                                                    Some("s3-obj-read"),
                                                );

                                                // Mark key as fully processed.
                                                partially_processed_keys
                                                    .update(key_index, None)
                                                    .await;
                                                continue 'outer;
                                            }

                                            // This object has failed, we need to re-establish the
                                            // connection.
                                            object = None;

                                            continue 'object_read;
                                        }
                                        Some(Ok(bytes)) => {
                                            let latest_len = latest_buf.len();

                                            // This is a bit of a sanity check.
                                            // We want to ensure that the next chunk we read after reconnecting, is the
                                            // same as the previous "good" chunk before the error.
                                            //
                                            // WARN: This (`latest_buf`) increases the memory consumption as we will be holding this previous chunk in memory.
                                            // Typically, this seems to be around 8 KB.
                                            if reconnected {
                                                let min = latest_len.min(bytes.len());
                                                if latest_buf[..min] != bytes[..min] {
                                                    consumer.error(
                                                            false,
                                                            anyhow!(
                                                                "error when reading object after prior failure; object '{}': bytes at offset `{latest_start_offset}..{}` do not match after reconnection; did the object change?",
                                                                partially_processed_key.key,
                                                                latest_start_offset + min as u64
                                                            ),
                                                            Some("s3-obj-read"),
                                                        );

                                                    // As the object may have changed, mark it as fully processed and skip it.
                                                    partially_processed_keys
                                                        .update(key_index, None)
                                                        .await;
                                                    continue 'outer;
                                                }

                                                reconnected = false;
                                                recovery_buffer =
                                                    Some(Vec::with_capacity(2 * latest_len));
                                            }

                                            // Buffer bytes until we have new data.
                                            if let Some(mut buf) = recovery_buffer.take() {
                                                buf.extend_from_slice(&bytes);
                                                if buf.len() > latest_len {
                                                    splitter.append(&buf[latest_len..]);

                                                    latest_start_offset += latest_len as u64;
                                                    latest_buf = buf.split_off(latest_len);
                                                } else {
                                                    // Put the buffer back.
                                                    recovery_buffer = Some(buf);
                                                }

                                                continue 'object_read;
                                            }

                                            latest_start_offset += latest_len as u64;
                                            latest_buf = bytes.to_vec();

                                            splitter.append(&latest_buf);
                                        }
                                        None => eoi = true,
                                    };
                                    break 'object_read;
                                }

                                loop {
                                    start_offset = splitter.position();
                                    let Some(chunk) = splitter.next(eoi) else {
                                        break;
                                    };
                                    let (buffer, errors) = parser.parse(chunk, None);
                                    let errors = errors
                                        .into_iter()
                                        .map(|e| {
                                            e.map_description(|desc| {
                                                format!(
                                                    "error parsing object '{}': {desc}",
                                                    partially_processed_key.key
                                                )
                                            })
                                        })
                                        .collect::<Vec<_>>();
                                    consumer.buffered(buffer.len());
                                    let end_offset = splitter.position();

                                    consumer.parse_errors(errors);

                                    if !buffer.is_empty() {
                                        queue.lock().await.push_back(QueuedBuffer {
                                            key_index,
                                            key: partially_processed_key.key.clone(),
                                            start_offset,
                                            end_offset: Some(end_offset),
                                            buffer: Some(buffer.unwrap()),
                                            timestamp,
                                        });
                                    }
                                }
                            }
                            // Queue empty buffer with end_offset = None marking the key as fully processed.
                            queue.lock().await.push_back(QueuedBuffer {
                                key_index,
                                key: partially_processed_key.key.clone(),
                                start_offset: splitter.position(),
                                end_offset: None,
                                buffer: None,
                                timestamp: Utc::now(),
                            });
                        }
                        Err(_) => break, // Channel closed
                    }
                }
            });
            join_handles.push(handle);
        }

        // Wait for end of input.
        let consumer_clone = consumer.clone();
        tokio::task::spawn(async move {
            _ = join_all(join_handles).await;
            // All reader threads terminated means that we either reached the end of input or
            // the connector is terminating.
            if *status_receiver.borrow() != PipelineState::Terminated {
                consumer_clone.eoi();
            }
        });

        loop {
            match command_receiver.recv().await {
                Some(InputReaderCommand::Replay { .. }) => {
                    panic!(
                        "replay command is not supported by the S3 input connector; this is a bug, please report it to developers"
                    )
                }
                Some(InputReaderCommand::Extend) => {
                    let _ = status_sender.send_replace(PipelineState::Running);
                }
                Some(InputReaderCommand::Pause) => {
                    let _ = status_sender.send_replace(PipelineState::Paused);
                }
                Some(InputReaderCommand::Queue {
                    checkpoint_requested,
                }) => {
                    let mut total = BufferSize::empty();
                    let mut hasher = consumer.hasher();
                    let mut watermarks = Vec::new();

                    while total.records < consumer.max_batch_size() {
                        let Some(QueuedBuffer {
                            key_index,
                            key,
                            start_offset,
                            end_offset,
                            mut buffer,
                            timestamp,
                        }) = queue.lock().await.pop_front()
                        else {
                            break;
                        };

                        let Some(mut prefix) =
                            buffer.take_some(consumer.max_batch_size() - total.records)
                        else {
                            partially_processed_keys.update(key_index, end_offset).await;
                            continue;
                        };

                        total += prefix.len();
                        if let Some(hasher) = hasher.as_mut() {
                            prefix.hash(hasher);
                        }
                        consumer.check_flush(prefix.flush());
                        watermarks.push(Watermark::new(timestamp, None));

                        if buffer.is_empty() {
                            partially_processed_keys.update(key_index, end_offset).await;
                            if checkpoint_requested {
                                break;
                            }
                        } else {
                            queue.lock().await.push_front(QueuedBuffer {
                                key_index,
                                key,
                                start_offset,
                                end_offset,
                                buffer,
                                timestamp,
                            });
                        }
                    }

                    // We store the latest fully ingested offsets in `resume`. Partially
                    consumer.extended(
                        total,
                        Some(Resume::new_metadata_only(
                            serde_json::to_value(&Metadata {
                                partially_processed_keys: partially_processed_keys.by_index().await,
                            })
                            .unwrap(),
                            hasher.map(|h| h.finish()),
                        )),
                        watermarks,
                    );
                }
                Some(InputReaderCommand::Disconnect) | None => return Ok(()),
            };
        }
    }
}

/// Block until the state is `Running`.
async fn wait_running(receiver: &mut WatchReceiver<PipelineState>) -> Result<(), RecvError> {
    // An error indicates that the channel was closed.  It's ok to ignore
    // the error as this situation will be handled by the top-level select,
    // which will abort the worker thread.
    receiver
        .wait_for(|state| state == &PipelineState::Running)
        .await
        .map(|_| ())
}

/// Wraps [UnboundedReceiver] with a one-message buffer, to allow received messages to be
/// put back and received again later.
struct BufferedReceiver<T> {
    receiver: Receiver<T>,
    buffer: Option<T>,
}

impl<T> BufferedReceiver<T> {
    fn new(receiver: Receiver<T>) -> Self {
        Self {
            receiver,
            buffer: None,
        }
    }

    async fn recv(&mut self) -> Option<T> {
        match self.buffer.take() {
            Some(value) => Some(value),
            None => self.receiver.recv().await.ok(),
        }
    }
}

fn to_s3_config(config: &Arc<S3InputConfig>) -> aws_sdk_s3::Config {
    let mut config_builder =
        aws_sdk_s3::Config::builder().region(aws_types::region::Region::new(config.region.clone()));

    if let Some(endpoint) = &config.endpoint_url {
        config_builder = config_builder.endpoint_url(endpoint);
    }

    if config.no_sign_request {
        return config_builder.build();
    }

    if let (Some(access_key), Some(secret_key)) =
        (&config.aws_access_key_id, &config.aws_secret_access_key)
    {
        let credentials = aws_sdk_s3::config::Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "credential-provider",
        );

        config_builder.credentials_provider(credentials).build()
    } else {
        let provider = TOKIO.block_on(async {
            aws_config::default_provider::credentials::default_provider().await
        });

        config_builder.credentials_provider(provider).build()
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod test {
    use crate::{
        InputReader,
        preprocess::PassthroughPreprocessorFactory,
        test::{engine_test, parser, published_schema},
        transport::s3::{MockS3Client, S3InputReader},
    };
    use aws_sdk_s3::{
        operation::{
            get_object::{GetObjectError, GetObjectOutput},
            list_objects_v2::ListObjectsV2Error,
        },
        primitives::{ByteStream, SdkBody},
        types::error::builders::{NoSuchBucketBuilder, NoSuchKeyBuilder},
    };
    use feldera_adapterlib::{
        format::{MessageOrientedPreprocessedParser, Parser},
        preprocess::{PreprocessorFactory, PreprocessorWatchdog},
        test::{
            ConnectorTestEngine, ErrorCallback, InputOptions, MockInputConsumer, MockInputParser,
            TestCollection, Updates, wait_for_rows_within,
        },
    };
    use feldera_types::{
        config::{InputEndpointConfig, TransportConfig},
        preprocess::PreprocessorConfig,
        program_schema::{ColumnType, Field, Relation},
    };
    use mockall::predicate::eq;
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    /// How long a case waits for an error that the connector must report.
    const ERROR_TIMEOUT: Duration = Duration::from_secs(60);

    /// How long a case waits for the rows of the objects.
    const ROWS_TIMEOUT_MS: u64 = 10_000;

    /// The table that the cases read into: one BIGINT column, `i`.
    pub fn relation() -> Relation {
        Relation::new(
            "s3_input".into(),
            vec![Field::new("i".into(), ColumnType::bigint(false))],
            false,
            BTreeMap::new(),
        )
    }

    /// An endpoint that lists the objects of the bucket `test-bucket` with an empty prefix.
    const MULTI_KEY_CONFIG: &str = r#"
{
  "stream": "test_input",
  "transport": {
    "name": "s3_input",
    "config": {
      "aws_access_key_id": "FAKE_ACCESS_KEY",
      "aws_secret_access_key": "FAKE_SECRET",
      "bucket_name": "test-bucket",
      "region": "us-west-1",
      "prefix": ""
    }
  },
  "format": { "name": "csv" }
}"#;

    /// An endpoint that reads the object `obj1`.
    const SINGLE_KEY_CONFIG: &str = r#"
{
  "stream": "test_input",
  "transport": {
    "name": "s3_input",
    "config": {
      "aws_access_key_id": "FAKE_ACCESS_KEY",
      "aws_secret_access_key": "FAKE_SECRET",
      "bucket_name": "test-bucket",
      "region": "us-west-1",
      "key": "obj1"
    }
  },
  "format": { "name": "csv" }
}"#;

    /// An endpoint that reads newline-delimited JSON from the object `obj1`; `lines: single`
    /// selects the line splitter that takes as many records as it can.
    const NDJSON_SINGLE_KEY_CONFIG: &str = r#"
{
  "stream": "test_input",
  "transport": {
    "name": "s3_input",
    "config": {
      "aws_access_key_id": "FAKE_ACCESS_KEY",
      "aws_secret_access_key": "FAKE_SECRET",
      "bucket_name": "test-bucket",
      "region": "us-west-1",
      "key": "obj1"
    }
  },
  "format": { "name": "json", "config": { "update_format": "raw", "array": false, "lines": "single" } }
}"#;

    /// A reader over a mock S3 client, and the mocks that it feeds.
    struct Setup {
        reader: Box<dyn InputReader>,
        consumer: MockInputConsumer,
        parser: MockInputParser,
        collection: Box<dyn TestCollection>,
    }

    /// Builds a reader of `config` over `mock`, with `on_error` already installed, and with the
    /// parser behind a message-oriented preprocessor if `preprocess`.
    ///
    /// The reader lists objects as soon as it starts, so the error callback must be in place
    /// first, or the case can miss an error that the listing reports.
    fn setup(
        engine: &dyn ConnectorTestEngine,
        config: &str,
        mock: MockS3Client,
        on_error: ErrorCallback,
        preprocess: bool,
    ) -> Setup {
        let config: InputEndpointConfig = serde_json::from_str(config).unwrap();
        let TransportConfig::S3Input(transport) = config.connector_config.transport.clone() else {
            panic!("the configuration is not of an S3 input");
        };
        let collection = engine
            .input_collection(&relation(), &InputOptions::default())
            .unwrap();
        let parser = parser(
            collection.as_ref(),
            &config.connector_config.format.unwrap(),
        );
        let consumer = MockInputConsumer::new();
        consumer.on_error(Some(on_error));
        let endpoint_parser: Box<dyn Parser> = if preprocess {
            let preprocessor_config = PreprocessorConfig {
                name: "passthrough".to_string(),
                message_oriented: true,
                config: serde_json::Value::Null,
                max_allowed_call_duration: None,
            };
            let preprocessor = PassthroughPreprocessorFactory
                .create(&preprocessor_config)
                .unwrap();
            Box::new(MessageOrientedPreprocessedParser::new(
                preprocessor,
                Box::new(parser.clone()),
                PreprocessorWatchdog::new(&preprocessor_config),
            ))
        } else {
            Box::new(parser.clone())
        };
        let reader = Box::new(S3InputReader::new_inner(
            &Arc::new(transport),
            Box::new(consumer.clone()),
            endpoint_parser,
            Arc::new(mock),
            None,
        ));
        Setup {
            reader,
            consumer,
            parser,
            collection,
        }
    }

    /// Returns the rows of [`relation`] whose `i` is in `values`, each inserted once.
    fn rows(values: impl Iterator<Item = i64>) -> Updates {
        let rows: Vec<String> = values.map(|i| format!(r#"{{"i": {i}}}"#)).collect();
        let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, row.as_str())).collect();
        Updates::from_json(published_schema(&relation()), &rows).unwrap()
    }

    /// Returns an object whose body is `body`.
    fn object(body: impl Into<SdkBody>) -> GetObjectOutput {
        GetObjectOutput::builder()
            .body(ByteStream::from(body.into()))
            .build()
    }

    /// Checks that a reader of `config` over `mock` delivers nothing before it starts, and then
    /// inserts the rows of `values`, in any order.
    fn check(
        engine: &dyn ConnectorTestEngine,
        config: &str,
        mock: MockS3Client,
        values: impl Iterator<Item = i64>,
    ) {
        let expected = rows(values);
        let setup = setup(engine, config, mock, Box::new(|_, _| ()), false);
        assert!(setup.parser.state().data.is_empty());
        assert!(!setup.consumer.state().eoi);

        setup.reader.extend();
        let updates = wait_for_rows_within(
            setup.collection.as_ref(),
            expected.len(),
            ROWS_TIMEOUT_MS,
            || setup.reader.queue(false),
        );
        assert!(updates.weights().iter().all(|weight| *weight == 1));
        assert_eq!(updates.zset().unwrap(), expected.zset().unwrap());
    }

    /// An endpoint with a key reads that object.
    #[engine_test]
    pub fn single_key_read(engine: &dyn ConnectorTestEngine) {
        let mut mock = MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
            .return_once(|_, _, _, _| Ok((vec!["obj1".to_string()], None)));
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
            .return_once(|_, _, _, _| Ok(object("1\n2\n3\n")));
        check(engine, SINGLE_KEY_CONFIG, mock, 1..4);
    }

    /// A preprocessor makes `parse` return a nested buffer, and the endpoint hands the circuit at
    /// most `max_batch_size` records per step, so one parsed buffer must survive a split across
    /// steps; records lost at the split would keep the pipeline from completing.
    ///
    /// Newline-delimited JSON splits with a line splitter that takes as many records as it can,
    /// so one nested element holds many records and a step boundary falls inside it.
    #[engine_test]
    pub fn preprocessed_read_split_across_batches(engine: &dyn ConnectorTestEngine) {
        const RECORDS: i64 = 100;
        // Not a divisor of RECORDS, so the last step is short too.
        const MAX_BATCH_SIZE: usize = 7;
        let body: String = (1..=RECORDS).map(|i| format!("{{\"i\":{i}}}\n")).collect();
        let mut mock = MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
            .return_once(|_, _, _, _| Ok((vec!["obj1".to_string()], None)));
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
            .return_once(move |_, _, _, _| Ok(object(body)));

        let setup = setup(
            engine,
            NDJSON_SINGLE_KEY_CONFIG,
            mock,
            Box::new(|_, _| ()),
            true,
        );
        setup.consumer.set_max_batch_size(MAX_BATCH_SIZE);
        setup.reader.extend();
        let updates = wait_for_rows_within(
            setup.collection.as_ref(),
            RECORDS as usize,
            ROWS_TIMEOUT_MS,
            || setup.reader.queue(false),
        );
        assert!(updates.weights().iter().all(|weight| *weight == 1));
        assert_eq!(updates.zset().unwrap(), rows(1..=RECORDS).zset().unwrap());
    }

    /// An endpoint with a prefix reads the object it lists.
    #[engine_test]
    pub fn single_object_with_prefix_read(engine: &dyn ConnectorTestEngine) {
        let mut mock = MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
            .return_once(|_, _, _, _| Ok((vec!["obj1".to_string()], None)));
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
            .return_once(|_, _, _, _| Ok(object("1\n2\n3\n")));
        check(engine, MULTI_KEY_CONFIG, mock, 1..4);
    }

    /// An endpoint follows a listing's continuation token and reads the objects of both pages.
    #[engine_test]
    pub fn multi_object_read_with_continuation(engine: &dyn ConnectorTestEngine) {
        let mut mock = MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
            .return_once(|_, _, _, _| {
                Ok((vec!["obj1".to_string()], Some("next_token".to_string())))
            });
        mock.expect_get_object_keys()
            .with(
                eq("test-bucket"),
                eq(""),
                eq(&None),
                eq(Some("next_token".to_string())),
            )
            .return_once(|_, _, _, _| Ok((vec!["obj2".to_string()], None)));
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
            .return_once(|_, _, _, _| Ok(object("1\n2\n3\n")));
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj2"), eq(0), eq(&None))
            .return_once(|_, _, _, _| Ok(object("4\n5\n6\n")));
        check(engine, MULTI_KEY_CONFIG, mock, 1..7);
    }

    /// An endpoint reads a thousand objects of one record each.
    #[engine_test]
    pub fn multi_object_read(engine: &dyn ConnectorTestEngine) {
        let mut mock = MockS3Client::default();
        let objects: Vec<String> = (0..1000).map(|i| format!("obj{i}")).collect();
        for (i, key) in objects.iter().enumerate() {
            mock.expect_get_object()
                .with(eq("test-bucket"), eq(key.clone()), eq(0), eq(&None))
                .return_once(move |_, _, _, _| Ok(object(format!("{i}\n"))));
        }
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
            .return_once(|_, _, _, _| Ok((objects, None)));
        let setup = setup(engine, MULTI_KEY_CONFIG, mock, Box::new(|_, _| ()), false);
        setup.reader.extend();
        let updates =
            wait_for_rows_within(setup.collection.as_ref(), 1000, ROWS_TIMEOUT_MS, || {
                setup.reader.queue(false)
            });
        assert!(updates.weights().iter().all(|weight| *weight == 1));
        assert_eq!(updates.zset().unwrap(), rows(0..1000).zset().unwrap());
    }

    /// A listing that fails is a fatal error that names the S3 error.
    #[engine_test]
    pub fn list_object_read_error(engine: &dyn ConnectorTestEngine) {
        let mut mock = MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
            .return_once(|_, _, _, _| {
                Err(ListObjectsV2Error::NoSuchBucket(NoSuchBucketBuilder::default().build()).into())
            });
        let (sender, receiver) = std::sync::mpsc::channel();
        let setup = setup(
            engine,
            MULTI_KEY_CONFIG,
            mock,
            // The receiver may be gone when the reader reports during teardown.
            Box::new(move |fatal, error| {
                let _ = sender.send((fatal, format!("{error}")));
            }),
            false,
        );
        setup.reader.extend();
        assert_eq!(
            receiver
                .recv_timeout(ERROR_TIMEOUT)
                .expect("the connector reports no error for a missing bucket"),
            (true, "NoSuchBucket".to_string())
        );
    }

    /// An object that fails to read is skipped, and the next object still arrives.
    #[engine_test]
    pub fn get_object_read_error(engine: &dyn ConnectorTestEngine) {
        let mut mock = MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
            .return_once(|_, _, _, _| {
                Ok((vec!["obj1".to_string()], Some("next_token".to_string())))
            });
        mock.expect_get_object_keys()
            .with(
                eq("test-bucket"),
                eq(""),
                eq(&None),
                eq(Some("next_token".to_string())),
            )
            .return_once(|_, _, _, _| Ok((vec!["obj2".to_string()], None)));
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
            .return_once(|_, _, _, _| {
                Err(GetObjectError::NoSuchKey(NoSuchKeyBuilder::default().build()).into())
            });
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj2"), eq(0), eq(&None))
            .return_once(|_, _, _, _| Ok(object("4\n5\n6\n")));
        check(engine, MULTI_KEY_CONFIG, mock, 4..7);
    }
}
