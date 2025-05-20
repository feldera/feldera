use std::{
    collections::{BTreeMap, VecDeque},
    hash::Hasher,
    sync::Arc,
};

use aws_sdk_s3::operation::{get_object::GetObjectOutput, list_objects_v2::ListObjectsV2Error};
use feldera_adapterlib::transport::Resume;
use tokio::sync::mpsc::{self, unbounded_channel, UnboundedReceiver, UnboundedSender};
use xxhash_rust::xxh3::Xxh3Default;

use crate::{
    format::StreamSplitter, InputBuffer, InputConsumer, InputReader, Parser, TransportInputEndpoint,
};
use anyhow::{bail, Result as AnyResult};
use dbsp::circuit::tokio::TOKIO;
use feldera_types::{config::FtModel, program_schema::Relation};
#[cfg(test)]
use mockall::automock;
use tracing::{error, info_span, Instrument};

use crate::transport::InputEndpoint;
use feldera_types::transport::s3::S3InputConfig;
use serde::{Deserialize, Serialize};

use super::InputReaderCommand;

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

        Ok(Self {
            config: Arc::new(config),
        })
    }
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
    ) -> anyhow::Result<Box<dyn crate::InputReader>> {
        Ok(Box::new(S3InputReader::new(&self.config, consumer, parser)))
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
trait S3Api: Send {
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
    sender: UnboundedSender<InputReaderCommand>,
}

impl InputReader for S3InputReader {
    fn request(&self, command: InputReaderCommand) {
        let _ = self.sender.send(command);
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
    key: String,
    start_offset: u64,
    end_offset: Option<u64>,
    buffer: Box<dyn InputBuffer>,
}

impl S3InputReader {
    fn new(
        config: &Arc<S3InputConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> S3InputReader {
        let s3_config = to_s3_config(config);
        let client = Box::new(S3Client {
            inner: aws_sdk_s3::Client::from_conf(s3_config),
        }) as Box<dyn S3Api>;
        Self::new_inner(config, consumer, parser, client)
    }

    fn new_inner(
        config: &Arc<S3InputConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        s3_client: Box<dyn S3Api>,
    ) -> S3InputReader {
        let (sender, receiver) = unbounded_channel();
        std::thread::spawn({
            let config = config.clone();
            move || {
                let span = info_span!("s3_input", bucket = config.bucket_name.clone());
                TOKIO.block_on(async {
                    let _ = Self::worker_task(s3_client, config, consumer, parser, receiver)
                        .instrument(span)
                        .await;
                })
            }
        });
        S3InputReader { sender }
    }

    async fn worker_task(
        client: Box<dyn S3Api>,
        config: Arc<S3InputConfig>,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
    ) -> anyhow::Result<()> {
        let mut splitter = StreamSplitter::new(parser.splitter());

        let mut command_receiver = BufferedReceiver::new(command_receiver);
        let mut start_position = None;
        match command_receiver.recv().await {
            Some(InputReaderCommand::Seek(metadata)) => {
                let metadata = serde_json::from_value::<Metadata>(metadata)?;
                if let Some((key, value)) = metadata.offsets.last_key_value() {
                    start_position = Some((key.clone(), value.1));
                }
            }
            None | Some(InputReaderCommand::Disconnect) => return Ok(()),
            Some(other) => {
                command_receiver.put_back(other);
            }
        };

        // Then replay as many steps as requested.
        loop {
            match command_receiver.recv().await {
                Some(InputReaderCommand::Seek(_)) => {
                    unreachable!("Seek must be the first input reader command")
                }
                None | Some(InputReaderCommand::Disconnect) => return Ok(()),
                Some(InputReaderCommand::Replay { metadata, .. }) => {
                    let metadata = serde_json::from_value::<Metadata>(metadata)?;
                    if let Some((key, value)) = metadata.offsets.last_key_value() {
                        start_position = Some((key.clone(), value.1));
                    }
                    let mut num_records = 0;
                    let mut hasher = Xxh3Default::new();
                    for (key, (start, end)) in metadata.offsets {
                        let mut object = client
                            .get_object(&config.bucket_name, &key, start, end)
                            .await?;
                        splitter.reset();
                        let mut eoi = false;
                        while !eoi {
                            match object.body.next().await.transpose()? {
                                Some(bytes) => splitter.append(&bytes),
                                None => eoi = true,
                            };
                            while let Some(chunk) = splitter.next(eoi) {
                                let (mut buffer, errors) = parser.parse(chunk);
                                consumer.parse_errors(errors);
                                consumer.buffered(buffer.len(), chunk.len());
                                num_records += buffer.len();
                                buffer.hash(&mut hasher);
                                buffer.flush();
                            }
                        }
                    }
                    consumer.replayed(num_records, hasher.finish());
                }
                Some(other) => {
                    command_receiver.put_back(other);
                    break;
                }
            }
        }

        // The worker thread fetches objects in the background while already retrieved
        // objects are being fed to the InputConsumer. We use a bounded channel
        // to make it so that no more than 8 objects are fetched while waiting
        // for object processing.
        let (tx, mut rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let mut start_after = match start_position {
                Some((key, Some(offset))) => {
                    let result = client
                        .get_object(&config.bucket_name, &key, offset, None)
                        .await
                        .map(|object| (key.clone(), object, offset));
                    tx.send(result).await?;
                    Some(key)
                }
                Some((key, None)) => Some(key),
                None => None,
            };
            let mut continuation_token = None;
            loop {
                let (objects_to_fetch, next_token): (Vec<String>, Option<String>) =
                    if let Some(prefix) = &config.prefix {
                        let result = client
                            .get_object_keys(
                                &config.bucket_name,
                                prefix,
                                start_after.take(),
                                continuation_token.take(),
                            )
                            .await;
                        match result {
                            Ok(ret) => ret,
                            Err(e) => {
                                error!("Could not fetch object keys (Error: {e:?}).");
                                tx.send(Err(e)).await?;
                                break;
                            }
                        }
                    } else {
                        // We checked that either key or prefix is set.
                        (vec![config.key.as_ref().unwrap().clone()], None)
                    };
                continuation_token = next_token;
                for key in &objects_to_fetch {
                    let result = client
                        .get_object(&config.bucket_name, key, 0, None)
                        .await
                        .map(|object| (key.clone(), object, 0));
                    tx.send(result).await?;
                }
                if continuation_token.is_none() {
                    break;
                }
            }
            drop(tx); // We're done. Close the channel.
            Ok::<(), mpsc::error::SendError<_>>(())
        });

        let mut running = false;
        let mut queue = VecDeque::<QueuedBuffer>::new();
        loop {
            tokio::select! {
                command = command_receiver.recv() => {
                    match command {
                        Some(command @ InputReaderCommand::Seek(_))
                            | Some(command @ InputReaderCommand::Replay { ..}) => {
                                unreachable!("{command:?} must be at the beginning of the command stream")
                            }
                        Some(InputReaderCommand::Extend) => running = true,
                        Some(InputReaderCommand::Pause) => running = false,
                        Some(InputReaderCommand::Queue{..}) => {
                            let mut total = 0;
                            let mut hasher = consumer.hasher();
                            let mut offsets = BTreeMap::<String, (u64, Option<u64>)>::new();
                            while total < consumer.max_batch_size() {
                                let Some(QueuedBuffer { key, start_offset, end_offset, mut buffer }) = queue.pop_front() else {
                                    break
                                };
                                total += buffer.len();
                                if let Some(hasher) = hasher.as_mut() {
                                    buffer.hash(hasher);
                                }
                                buffer.flush();
                                offsets.entry(key)
                                    .and_modify(|value| value.1 = end_offset)
                                    .or_insert((start_offset, end_offset));
                            }
                            consumer.extended(total, Some(Resume::new_metadata_only(serde_json::to_value(&Metadata { offsets }).unwrap(), hasher)));
                        }
                        Some(InputReaderCommand::Disconnect) | None => {
                            return Ok(())
                        }
                    }
                },
                // Poll the object stream.
                get_obj = rx.recv(), if running => {
                    match get_obj {
                        Some(Ok((key, mut object, start_offset))) => {
                            splitter.seek(start_offset);
                            let mut eoi = false;
                            let mut added_chunks = false;
                            while !eoi {
                                match object.body.next().await.transpose()? {
                                    Some(bytes) => splitter.append(&bytes),
                                    None => eoi = true,
                                };
                                loop {
                                    let start_offset = splitter.position();
                                    let Some(chunk) = splitter.next(eoi) else {
                                        break
                                    };
                                    let (buffer, errors) = parser.parse(chunk);
                                    consumer.buffered(buffer.len(), chunk.len());
                                    let end_offset = splitter.position();

                                    consumer.parse_errors(errors);
                                    if !buffer.is_empty() {
                                        queue.push_back(QueuedBuffer {
                                            key: key.clone(),
                                            start_offset,
                                            end_offset: Some(end_offset),
                                            buffer: buffer.unwrap(),
                                        });
                                        added_chunks = true;
                                    }
                                }
                            }
                            if added_chunks {
                                queue.back_mut().unwrap().end_offset = None;
                            }
                        }
                        Some(Err(e)) => {
                            match e.downcast_ref::<ListObjectsV2Error>() {
                                // We consider a missing bucket a fatal error
                                Some(ListObjectsV2Error::NoSuchBucket(_)) => {
                                    consumer.error(true, e)
                                },
                                _ => consumer.error(false, e)
                            }
                        }
                        None => {
                            // End of input.
                            consumer.eoi();
                            running = false;
                        }
                    }
                }
            };
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    offsets: BTreeMap<String, (u64, Option<u64>)>,
}

/// Wraps [UnboundedReceiver] with a one-message buffer, to allow received messages to be
/// put back and received again later.
struct BufferedReceiver<T> {
    receiver: UnboundedReceiver<T>,
    buffer: Option<T>,
}

impl<T> BufferedReceiver<T> {
    fn new(receiver: UnboundedReceiver<T>) -> Self {
        Self {
            receiver,
            buffer: None,
        }
    }

    async fn recv(&mut self) -> Option<T> {
        match self.buffer.take() {
            Some(value) => Some(value),
            None => self.receiver.recv().await,
        }
    }

    fn put_back(&mut self, value: T) {
        assert!(self.buffer.is_none());
        self.buffer = Some(value);
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

    #[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
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
        streaming: true
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
        streaming: false
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
            Box::new(mock),
        )) as Box<dyn crate::InputReader>;
        (reader, consumer, parser, input_handle)
    }

    fn run_test(config_str: &str, mock: super::MockS3Client, test_data: Vec<TestStruct>) {
        let (reader, consumer, parser, input_handle) = test_setup(config_str, mock);
        // No outputs should be produced at this point.
        assert!(parser.state().data.is_empty());
        assert!(!consumer.state().eoi);

        // Unpause the endpoint, wait for the data to appear at the output.
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
        for (i, upd) in input_handle.state().flushed.iter().enumerate() {
            assert_eq!(upd.unwrap_insert(), &test_data[i]);
        }
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
        for (i, upd) in input_handle.state().flushed.iter().enumerate() {
            assert_eq!(upd.unwrap_insert(), &test_data[i]);
        }
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
