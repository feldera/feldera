use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    thread,
};

use super::InputReaderCommand;
use crate::transport::InputEndpoint;
use crate::{
    format::StreamSplitter, InputBuffer, InputConsumer, InputReader, Parser, TransportInputEndpoint,
};
use anyhow::anyhow;
use anyhow::{bail, Result as AnyResult};
use async_channel::{bounded, unbounded, Receiver, SendError, Sender};
use aws_sdk_s3::operation::{get_object::GetObjectOutput, list_objects_v2::ListObjectsV2Error};
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::{
    format::BufferSize,
    transport::{parse_resume_info, Resume},
    PipelineState,
};
use feldera_types::transport::s3::S3InputConfig;
use feldera_types::{config::FtModel, program_schema::Relation};
use futures::future::join_all;
#[cfg(test)]
use mockall::automock;
use serde::{Deserialize, Serialize};
use tokio::sync::watch::{channel as watch_channel, error::RecvError, Receiver as WatchReceiver};
use tokio::sync::Mutex;
use tracing::{error, info_span, Instrument};

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
                    tracing::info!("both 'aws_access_key_id' and 'aws_secret_access_key' are not set; reading credentials from the environment");
                },
                (Some(_), None) => bail!("'aws_access_key_id' set but 'aws_secret_access_key' not set; either set both or unset both to read from the environment"),
                (None, Some(_)) => bail!("'aws_secret_access_key' set but 'aws_access_key_id' not set; either set both or unset both to read from the environment"),
                _ => {}
            }
        }

        if config.key.is_none() && config.prefix.is_none() {
            bail!("either 'key' or 'prefix' property must be specified");
        }

        if config.key.is_some() && config.prefix.is_some() {
            bail!("connector configuration specifies both 'key' and 'prefix' properties; please specify only one");
        }

        if config.max_concurrent_fetches == 0 {
            bail!("invalid 'max_concurrent_fetches' value: 'max_concurrent_fetches' must be greater than 0");
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
struct Metadata {
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
struct S3Client {
    inner: aws_sdk_s3::Client,
}

#[cfg_attr(test, automock)]
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
                (start, None) => Some(format!("bytes={start}")),
            })
            .send()
            .await?)
    }
}

#[async_trait::async_trait]
trait S3Api: Send + Sync {
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

struct S3InputReader {
    sender: Sender<InputReaderCommand>,
}

impl InputReader for S3InputReader {
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

    fn new_inner(
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
                let mut splitter = StreamSplitter::new(parser.splitter());

                loop {
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

                            let result = client
                                .get_object(
                                    &bucket_name,
                                    &partially_processed_key.key,
                                    start_offset,
                                    None,
                                )
                                .await;
                            let mut object = match result {
                                Ok(ret) => ret,
                                Err(e) => {
                                    consumer.error(
                                        false,
                                        anyhow!(
                                            "could not fetch object '{}': {e:?}",
                                            &partially_processed_key.key
                                        ),
                                        Some("s3-obj-fetch"),
                                    );
                                    // Mark key as fully processed.
                                    partially_processed_keys.update(key_index, None).await;
                                    continue;
                                }
                            };

                            splitter.seek(start_offset);
                            let mut eoi = false;
                            while !eoi {
                                if wait_running(&mut status_receiver).await.is_err() {
                                    // channel closed
                                    return;
                                }

                                match object.body.next().await {
                                    Some(Err(e)) => consumer.error(
                                        false,
                                        anyhow!(
                                            "error reading object '{}': {e:?}",
                                            &partially_processed_key.key
                                        ),
                                        Some("s3-obj-read"),
                                    ),
                                    Some(Ok(bytes)) => splitter.append(&bytes),
                                    None => eoi = true,
                                };
                                loop {
                                    start_offset = splitter.position();
                                    let Some(chunk) = splitter.next(eoi) else {
                                        break;
                                    };
                                    let (buffer, errors) = parser.parse(chunk);
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
                        "replay command is not supported by the S3 input connector; this is a bug, please report it to developers")
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
                    while total.records < consumer.max_batch_size() {
                        let Some(QueuedBuffer {
                            key_index,
                            key,
                            start_offset,
                            end_offset,
                            mut buffer,
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
                        prefix.flush();

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
                            hasher,
                        )),
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

#[cfg(test)]
mod test {
    use crate::{
        test::{mock_parser_pipeline, wait, MockDeZSet, MockInputConsumer, MockInputParser},
        transport::s3::{S3InputConfig, S3InputReader},
    };
    use aws_sdk_s3::{
        operation::{
            get_object::{GetObjectError, GetObjectOutput},
            list_objects_v2::ListObjectsV2Error,
        },
        primitives::{ByteStream, SdkBody},
        types::error::builders::{NoSuchBucketBuilder, NoSuchKeyBuilder},
    };
    use feldera_types::{
        config::{InputEndpointConfig, TransportConfig},
        deserialize_without_context,
        program_schema::Relation,
    };
    use mockall::predicate::eq;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, PartialOrd, Ord)]
    struct TestStruct {
        i: i64,
    }
    deserialize_without_context!(TestStruct);

    const MULTI_KEY_CONFIG_STR: &str = r#"
stream: test_input
transport:
    name: s3_input
    config:
        aws_access_key_id: FAKE_ACCESS_KEY
        aws_secret_access_key: FAKE_SECRET
        bucket_name: test-bucket
        region: us-west-1
        prefix: ''
format:
    name: csv
"#;

    const SINGLE_KEY_CONFIG_STR: &str = r#"
stream: test_input
transport:
    name: s3_input
    config:
        aws_access_key_id: FAKE_ACCESS_KEY
        aws_secret_access_key: FAKE_SECRET
        bucket_name: test-bucket
        region: us-west-1
        key: obj1
format:
    name: csv
"#;

    fn test_setup(
        config_str: &str,
        mock: super::MockS3Client,
    ) -> (
        Box<dyn crate::InputReader>,
        MockInputConsumer,
        MockInputParser,
        MockDeZSet<TestStruct, TestStruct>,
    ) {
        let config: InputEndpointConfig = serde_yaml::from_str(config_str).unwrap();
        let transport_config = config.connector_config.transport.clone();
        let transport_config: Arc<S3InputConfig> = match transport_config {
            TransportConfig::S3Input(config) => Arc::new(config),
            _ => {
                panic!("Expected S3Input transport configuration");
            }
        };
        let (consumer, parser, input_handle) = mock_parser_pipeline::<TestStruct, TestStruct>(
            &Relation::empty(),
            &config.connector_config.format.unwrap(),
        )
        .unwrap();
        consumer.on_error(Some(Box::new(|_, _| ())));
        let reader = Box::new(S3InputReader::new_inner(
            &transport_config,
            Box::new(consumer.clone()),
            Box::new(parser.clone()),
            Arc::new(mock),
            None,
        )) as Box<dyn crate::InputReader>;
        (reader, consumer, parser, input_handle)
    }

    fn run_test(config_str: &str, mock: super::MockS3Client, mut test_data: Vec<TestStruct>) {
        let (reader, consumer, parser, input_handle) = test_setup(config_str, mock);
        // No outputs should be produced at this point.
        assert!(parser.state().data.is_empty());
        assert!(!consumer.state().eoi);

        // Unpause the endpoint, wait for the data to appear at the output.
        reader.extend();
        wait(
            || {
                reader.queue(false);
                println!(
                    "actual: {}, expected: {}",
                    input_handle.state().flushed.len(),
                    test_data.len()
                );
                input_handle.state().flushed.len() == test_data.len()
            },
            10000,
        )
        .unwrap();

        assert_eq!(test_data.len(), input_handle.state().flushed.len());

        let mut outputs = input_handle
            .state()
            .flushed
            .iter()
            .map(|upd| upd.unwrap_insert())
            .cloned()
            .collect::<Vec<_>>();

        // Objects can be ingested in any order.
        test_data.sort();
        outputs.sort();

        assert_eq!(outputs, test_data);
    }

    #[test]
    fn single_key_read() {
        let mut mock = super::MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
            .return_once(|_, _, _, _| Ok((vec!["obj1".to_string()], None)));
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
            .return_once(|_, _, __, _| {
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(SdkBody::from("1\n2\n3\n")))
                    .build())
            });
        let test_data: Vec<TestStruct> = (1..4).map(|i| TestStruct { i }).collect();
        run_test(SINGLE_KEY_CONFIG_STR, mock, test_data);
    }

    #[test]
    fn single_object_with_prefix_read() {
        let mut mock = super::MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
            .return_once(|_, _, _, _| Ok((vec!["obj1".to_string()], None)));
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
            .return_once(|_, _, _, _| {
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(SdkBody::from("1\n2\n3\n")))
                    .build())
            });
        let test_data: Vec<TestStruct> = (1..4).map(|i| TestStruct { i }).collect();
        run_test(MULTI_KEY_CONFIG_STR, mock, test_data);
    }

    #[test]
    fn multi_object_read_with_continuation() {
        let mut mock = super::MockS3Client::default();
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
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(SdkBody::from("1\n2\n3\n")))
                    .build())
            });
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj2"), eq(0), eq(&None))
            .return_once(|_, _, _, _| {
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(SdkBody::from("4\n5\n6\n")))
                    .build())
            });
        let test_data: Vec<TestStruct> = (1..7).map(|i| TestStruct { i }).collect();
        run_test(MULTI_KEY_CONFIG_STR, mock, test_data);
    }

    #[test]
    fn multi_object_read_with_pause() {
        let mut mock = super::MockS3Client::default();
        // Read 1K objects by prefix, each with 1 row
        let objs: Vec<String> = (0..1000).map(|i| format!("obj{i}")).collect();
        for (i, key) in objs.iter().enumerate() {
            mock.expect_get_object()
                .with(eq("test-bucket"), eq(key.clone()), eq(0), eq(&None))
                .return_once(move |_, _, _, _| {
                    Ok(GetObjectOutput::builder()
                        .body(ByteStream::from(SdkBody::from(format!("{i}\n"))))
                        .build())
                });
        }
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
            .return_once(|_, _, _, _| Ok((objs, None)));
        let test_data: Vec<TestStruct> = (0..1000).map(|i| TestStruct { i }).collect();
        let (reader, _consumer, _parser, input_handle) = test_setup(MULTI_KEY_CONFIG_STR, mock);
        reader.extend();
        wait(
            || {
                reader.queue(false);
                input_handle.state().flushed.len() == test_data.len()
            },
            10000,
        )
        .unwrap();

        assert_eq!(test_data.len(), input_handle.state().flushed.len());

        let mut outputs = input_handle
            .state()
            .flushed
            .iter()
            .map(|upd| upd.unwrap_insert())
            .cloned()
            .collect::<Vec<_>>();

        // Objects can be ingested in any order.
        outputs.sort();

        assert_eq!(outputs, test_data);
    }

    #[test]
    fn list_object_read_error() {
        let mut mock = super::MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None), eq(&None))
            .return_once(|_, _, _, _| {
                Err(ListObjectsV2Error::NoSuchBucket(NoSuchBucketBuilder::default().build()).into())
            });
        let (reader, consumer, _parser, _) = test_setup(MULTI_KEY_CONFIG_STR, mock);
        let (tx, rx) = std::sync::mpsc::channel();
        consumer.on_error(Some(Box::new(move |fatal, err| {
            tx.send((fatal, format!("{err}"))).unwrap()
        })));
        reader.extend();
        assert_eq!((true, "NoSuchBucket".to_string()), rx.recv().unwrap());
    }

    #[test]
    fn get_object_read_error() {
        let mut mock = super::MockS3Client::default();
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
        // Reading obj1 fails but then obj2 succeeds. We should still see
        // the contents of objects 4, 5 and 6 in there.
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"), eq(0), eq(&None))
            .return_once(|_, _, _, _| {
                Err(GetObjectError::NoSuchKey(NoSuchKeyBuilder::default().build()).into())
            });
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj2"), eq(0), eq(&None))
            .return_once(|_, _, _, _| {
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(SdkBody::from("4\n5\n6\n")))
                    .build())
            });
        let test_data: Vec<TestStruct> = (4..7).map(|i| TestStruct { i }).collect();
        run_test(MULTI_KEY_CONFIG_STR, mock, test_data);
    }
}
