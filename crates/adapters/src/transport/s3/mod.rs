use std::sync::Arc;

use aws_sdk_s3::operation::{get_object::GetObjectOutput, list_objects_v2::ListObjectsV2Error};
use log::error;
use tokio::sync::{
    mpsc,
    watch::{channel, Receiver, Sender},
};

use crate::{InputConsumer, InputReader, PipelineState, TransportInputEndpoint};
use dbsp::circuit::tokio::TOKIO;
#[cfg(test)]
use mockall::automock;
use pipeline_types::program_schema::Relation;

use crate::transport::InputEndpoint;
use pipeline_types::transport::s3::{AwsCredentials, ConsumeStrategy, ReadStrategy, S3InputConfig};

pub struct S3InputEndpoint {
    config: Arc<S3InputConfig>,
}

impl S3InputEndpoint {
    pub fn new(config: S3InputConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

impl InputEndpoint for S3InputEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

impl TransportInputEndpoint for S3InputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn crate::InputConsumer>,
        _start_step: super::Step,
        _schema: Relation,
    ) -> anyhow::Result<Box<dyn crate::InputReader>> {
        Ok(Box::new(S3InputReader::new(&self.config, consumer)))
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
        continuation_token: Option<String>,
    ) -> anyhow::Result<(Vec<String>, Option<String>)> {
        let res: (Vec<String>, Option<String>) = self
            .inner
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
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

    async fn get_object(&self, bucket: &str, key: &str) -> anyhow::Result<GetObjectOutput> {
        Ok(self
            .inner
            .get_object()
            .bucket(bucket)
            .key(key)
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
        continuation_token: Option<String>,
    ) -> anyhow::Result<(Vec<String>, Option<String>)>;

    /// Fetch an object by key within a bucket
    async fn get_object(&self, bucket: &str, key: &str) -> anyhow::Result<GetObjectOutput>;
}

struct S3InputReader {
    sender: Sender<PipelineState>,
}

impl InputReader for S3InputReader {
    fn start(&self, _step: super::Step) -> anyhow::Result<()> {
        self.sender.send_replace(PipelineState::Running);
        Ok(())
    }

    fn pause(&self) -> anyhow::Result<()> {
        self.sender.send_replace(PipelineState::Paused);
        Ok(())
    }

    fn disconnect(&self) {
        self.sender.send_replace(PipelineState::Terminated);
    }
}

impl Drop for S3InputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

impl S3InputReader {
    fn new(config: &Arc<S3InputConfig>, consumer: Box<dyn InputConsumer>) -> S3InputReader {
        let s3_config = to_s3_config(config);
        let client = Box::new(S3Client {
            inner: aws_sdk_s3::Client::from_conf(s3_config),
        }) as Box<dyn S3Api>;
        Self::new_inner(config, consumer, client)
    }

    fn new_inner(
        config: &Arc<S3InputConfig>,
        mut consumer: Box<dyn InputConsumer>,
        s3_client: Box<dyn S3Api>,
    ) -> S3InputReader {
        let (sender, receiver) = channel(PipelineState::Paused);
        let config_clone = config.clone();
        let receiver_clone = receiver.clone();
        std::thread::spawn(move || {
            TOKIO.block_on(async {
                let _ =
                    Self::worker_task(s3_client, config_clone, &mut consumer, receiver_clone).await;
            })
        });
        S3InputReader { sender }
    }

    async fn worker_task(
        client: Box<dyn S3Api>,
        config: Arc<S3InputConfig>,
        consumer: &mut Box<dyn InputConsumer>,
        mut receiver: Receiver<PipelineState>,
    ) -> anyhow::Result<()> {
        // The worker thread fetches objects in the background while already retrieved
        // objects are being fed to the InputConsumer. We use a bounded channel
        // to make it so that no more than 8 objects are fetched while waiting
        // for object processing.
        let (tx, mut rx) = mpsc::channel(8);
        let consume_strategy = config.consume_strategy.clone();
        tokio::spawn(async move {
            let mut continuation_token = None;
            loop {
                let (objects_to_fetch, next_token): (Vec<String>, Option<String>) = match &config
                    .read_strategy
                {
                    ReadStrategy::Prefix { prefix } => {
                        let result = client
                            .get_object_keys(&config.bucket_name, prefix, continuation_token.take())
                            .await;
                        match result {
                            Ok(ret) => ret,
                            Err(e) => {
                                error!("Could not fetch object keys (Error: {e:?}).");
                                tx.send(Err(e)).await.expect("Enqueue failed");
                                break;
                            }
                        }
                    }
                    ReadStrategy::SingleKey { key } => (vec![key.clone()], None),
                };
                continuation_token = next_token;
                for key in &objects_to_fetch {
                    let object = client.get_object(&config.bucket_name, key).await;
                    tx.send(object).await.expect("Enqueue failed");
                }
                if continuation_token.is_none() {
                    break;
                }
            }
            drop(tx); // We're done. Close the channel.
        });
        loop {
            let state = *receiver.borrow();
            match state {
                PipelineState::Paused => {
                    receiver.changed().await?;
                }
                PipelineState::Running => {
                    tokio::select! {
                        // If pipeline status has changed, exit the select
                        _ = receiver.changed() => (),
                        // Poll the object stream. If `None`, we have processed all objects.
                        get_obj = rx.recv() => {
                            match get_obj {
                                Some(Ok(mut object)) => {
                                    match consume_strategy {
                                        ConsumeStrategy::Fragment => match object.body.next().await {
                                            Some(Ok(bytes)) => {
                                                consumer.input_fragment(&bytes);
                                            }
                                            None => break,
                                            Some(Err(e)) => consumer.error(false, e.into())
                                        }
                                        ConsumeStrategy::Object =>
                                            match object.body.collect().await.map(|c| c.into_bytes()) {
                                                Ok(bytes) => {
                                                    consumer.input_chunk(&bytes);
                                                }
                                                Err(e) => consumer.error(false, e.into())
                                        }
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
                                None => break // Channel is closed, exit.
                            }
                        }
                    }
                }
                PipelineState::Terminated => break,
            };
        }
        Ok(())
    }
}

fn to_s3_config(config: &Arc<S3InputConfig>) -> aws_sdk_s3::Config {
    let config_builder =
        aws_sdk_s3::Config::builder().region(aws_types::region::Region::new(config.region.clone()));
    match &config.credentials {
        AwsCredentials::AccessKey {
            aws_access_key_id,
            aws_secret_access_key,
        } => {
            let credentials = aws_sdk_s3::config::Credentials::new(
                aws_access_key_id.clone(),
                aws_secret_access_key.clone(),
                None,
                None,
                "credential-provider",
            );
            config_builder.credentials_provider(credentials).build()
        }
        AwsCredentials::NoSignRequest => config_builder.build(),
    }
}

#[cfg(test)]
mod test {
    use crate::{
        test::{mock_parser_pipeline, wait, MockDeZSet, MockInputConsumer},
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
    use mockall::predicate::eq;
    use pipeline_types::{
        config::InputEndpointConfig, config::TransportConfig, deserialize_without_context,
    };
    use serde::{Deserialize, Serialize};
    use std::{sync::Arc, time::Duration};

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
    struct TestStruct {
        i: i64,
    }
    deserialize_without_context!(TestStruct);

    const MULTI_KEY_CONFIG_STR: &str = r#"
stream: test_input
transport:
    name: s3_input
    config:
        credentials:
            type: AccessKey
            aws_access_key_id: FAKE_ACCESS_KEY
            aws_secret_access_key: FAKE_SECRET
        bucket_name: test-bucket
        region: us-west-1
        read_strategy:
            type: Prefix
            prefix: ''
        consume_strategy:
            type: Fragment
format:
    name: csv
"#;

    const SINGLE_KEY_CONFIG_STR: &str = r#"
stream: test_input
transport:
    name: s3_input
    config:
        credentials:
            type: AccessKey
            aws_access_key_id: FAKE_ACCESS_KEY
            aws_secret_access_key: FAKE_SECRET
        bucket_name: test-bucket
        region: us-west-1
        read_strategy:
            type: SingleKey
            key: obj1
        consume_strategy:
            type: Object
format:
    name: csv
"#;

    fn test_setup(
        config_str: &str,
        mock: super::MockS3Client,
    ) -> (
        Box<dyn crate::InputReader>,
        MockInputConsumer,
        MockDeZSet<TestStruct, TestStruct>,
    ) {
        let config: InputEndpointConfig = serde_yaml::from_str(&config_str).unwrap();
        let transport_config = config.connector_config.transport.clone();
        let transport_config: Arc<S3InputConfig> = match transport_config {
            TransportConfig::S3Input(config) => Arc::new(config),
            _ => {
                panic!("Expected S3Input transport configuration");
            }
        };
        let (consumer, input_handle) = mock_parser_pipeline::<TestStruct, TestStruct>(
            &config.connector_config.format.unwrap(),
        )
        .unwrap();
        consumer.on_error(Some(Box::new(|_, _| ())));
        let reader = Box::new(S3InputReader::new_inner(
            &transport_config,
            Box::new(consumer.clone()),
            Box::new(mock),
        )) as Box<dyn crate::InputReader>;
        (reader, consumer, input_handle)
    }

    fn run_test(config_str: &str, mock: super::MockS3Client, test_data: Vec<TestStruct>) {
        let (reader, consumer, input_handle) = test_setup(config_str, mock);
        let _ = reader.pause();
        // No outputs should be produced at this point.
        assert!(consumer.state().data.is_empty());
        assert!(!consumer.state().eoi);

        // Unpause the endpoint, wait for the data to appear at the output.
        reader.start(0).unwrap();
        wait(
            || input_handle.state().flushed.len() == test_data.len(),
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
            .with(eq("test-bucket"), eq(""), eq(&None))
            .return_once(|_, _, _| Ok((vec!["obj1".to_string()], None)));
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"))
            .return_once(|_, _| {
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(SdkBody::from("1\n2\n3\n")))
                    .build())
            });
        let test_data: Vec<TestStruct> = (1..4).map(|i| TestStruct { i }).collect();
        run_test(&SINGLE_KEY_CONFIG_STR, mock, test_data);
    }

    #[test]
    fn single_object_with_prefix_read() {
        let mut mock = super::MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None))
            .return_once(|_, _, _| Ok((vec!["obj1".to_string()], None)));
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"))
            .return_once(|_, _| {
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(SdkBody::from("1\n2\n3\n")))
                    .build())
            });
        let test_data: Vec<TestStruct> = (1..4).map(|i| TestStruct { i }).collect();
        run_test(&MULTI_KEY_CONFIG_STR, mock, test_data);
    }

    #[test]
    fn multi_object_read_with_continuation() {
        let mut mock = super::MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None))
            .return_once(|_, _, _| Ok((vec!["obj1".to_string()], Some("next_token".to_string()))));
        mock.expect_get_object_keys()
            .with(
                eq("test-bucket"),
                eq(""),
                eq(Some("next_token".to_string())),
            )
            .return_once(|_, _, _| Ok((vec!["obj2".to_string()], None)));
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"))
            .return_once(|_, _| {
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(SdkBody::from("1\n2\n3\n")))
                    .build())
            });
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj2"))
            .return_once(|_, _| {
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(SdkBody::from("4\n5\n6\n")))
                    .build())
            });
        let test_data: Vec<TestStruct> = (1..7).map(|i| TestStruct { i }).collect();
        run_test(&MULTI_KEY_CONFIG_STR, mock, test_data);
    }

    #[test]
    fn multi_object_read_with_pause() {
        let mut mock = super::MockS3Client::default();
        // Read 1K objects by prefix, each with 1 row
        let objs: Vec<String> = (0..1000).map(|i| format!("obj{i}")).collect();
        for (i, key) in objs.iter().enumerate() {
            mock.expect_get_object()
                .with(eq("test-bucket"), eq(key.clone()))
                .return_once(move |_, _| {
                    Ok(GetObjectOutput::builder()
                        .body(ByteStream::from(SdkBody::from(format!("{i}\n"))))
                        .build())
                });
        }
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None))
            .return_once(|_, _, _| Ok((objs, None)));
        let test_data: Vec<TestStruct> = (0..1000).map(|i| TestStruct { i }).collect();
        let (reader, _, input_handle) = test_setup(MULTI_KEY_CONFIG_STR, mock);
        reader.start(0).unwrap();
        // Pause after 50 rows are recorded.
        wait(|| input_handle.state().flushed.len() > 50, 10000).unwrap();
        let _ = reader.pause();
        // Wait a few milliseconds for the worker to pause and write any WIP object
        std::thread::sleep(Duration::from_millis(10));
        let n = input_handle.state().flushed.len();
        // Wait a few more milliseconds and make sure no more entries were written
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(n, input_handle.state().flushed.len());
        assert_ne!(input_handle.state().flushed.len(), test_data.len());
        // Resume to completion
        reader.start(0).unwrap();
        wait(
            || input_handle.state().flushed.len() == test_data.len(),
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
            .with(eq("test-bucket"), eq(""), eq(&None))
            .return_once(|_, _, _| {
                Err(ListObjectsV2Error::NoSuchBucket(NoSuchBucketBuilder::default().build()).into())
            });
        let (reader, consumer, _) = test_setup(MULTI_KEY_CONFIG_STR, mock);
        let (tx, rx) = std::sync::mpsc::channel();
        consumer.on_error(Some(Box::new(move |fatal, err| {
            tx.send((fatal, format!("{err}"))).unwrap()
        })));
        reader.start(0).unwrap();
        assert_eq!((true, "NoSuchBucket".to_string()), rx.recv().unwrap());
    }

    #[test]
    fn get_object_read_error() {
        let mut mock = super::MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""), eq(&None))
            .return_once(|_, _, _| Ok((vec!["obj1".to_string()], Some("next_token".to_string()))));
        mock.expect_get_object_keys()
            .with(
                eq("test-bucket"),
                eq(""),
                eq(Some("next_token".to_string())),
            )
            .return_once(|_, _, _| Ok((vec!["obj2".to_string()], None)));
        // Reading obj1 fails but then obj2 succeeds. We should still see
        // the contents of objects 4, 5 and 6 in there.
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj1"))
            .return_once(|_, _| {
                Err(GetObjectError::NoSuchKey(NoSuchKeyBuilder::default().build()).into())
            });
        mock.expect_get_object()
            .with(eq("test-bucket"), eq("obj2"))
            .return_once(|_, _| {
                Ok(GetObjectOutput::builder()
                    .body(ByteStream::from(SdkBody::from("4\n5\n6\n")))
                    .build())
            });
        let test_data: Vec<TestStruct> = (4..7).map(|i| TestStruct { i }).collect();
        run_test(&MULTI_KEY_CONFIG_STR, mock, test_data);
    }
}
