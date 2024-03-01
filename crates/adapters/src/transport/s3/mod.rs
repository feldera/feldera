use std::{borrow::Cow, sync::Arc};

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use serde::Deserialize;
use tokio::sync::watch::{channel, Receiver, Sender};

use crate::{InputConsumer, InputEndpoint, InputReader, InputTransport, PipelineState};
#[cfg(test)]
use mockall::automock;

use pipeline_types::transport::s3::{AwsCredentials, ReadStrategy, S3InputConfig};

pub struct S3InputTransport;

impl InputTransport for S3InputTransport {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("s3")
    }

    fn new_endpoint(
        &self,
        config: &serde_yaml::Value,
    ) -> anyhow::Result<Box<dyn crate::InputEndpoint>> {
        let ep = S3InputEndpoint {
            config: Arc::new(S3InputConfig::deserialize(config)?),
        };
        Ok(Box::new(ep))
    }
}

struct S3InputEndpoint {
    config: Arc<S3InputConfig>,
}

impl InputEndpoint for S3InputEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        false
    }

    fn open(
        &self,
        consumer: Box<dyn crate::InputConsumer>,
        _start_step: super::Step,
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
    async fn get_object_keys(&self, bucket: &str, prefix: &str) -> anyhow::Result<Vec<String>> {
        let res: Vec<String> = self
            .inner
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .send()
            .await
            .map(|output| {
                output
                    .contents()
                    .iter()
                    .map(|object| {
                        object
                            .key()
                            .expect("Objects should always have a key")
                            .to_string()
                    })
                    .collect()
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
    async fn get_object_keys(&self, bucket: &str, prefix: &str) -> anyhow::Result<Vec<String>>;

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
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Could not create Tokio runtime");
            rt.block_on(async {
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
        let objects_to_fetch = match &config.read_strategy {
            ReadStrategy::Prefix { prefix } => {
                client.get_object_keys(&config.bucket_name, prefix).await?
            }
            ReadStrategy::SingleKey { key } => vec![key.clone()],
        };
        for key in &objects_to_fetch {
            let mut object = client.get_object(&config.bucket_name, key).await?;
            loop {
                let state = *receiver.borrow();
                match state {
                    PipelineState::Paused => {
                        receiver.changed().await?;
                    }
                    PipelineState::Running => {
                        tokio::select! {
                            _ = receiver.changed() => (),
                            result = object.body.next() => {
                                match result {
                                    Some(Ok(bytes)) => {
                                        consumer.input_fragment(&bytes);
                                    }
                                    None => break,
                                    Some(Err(e)) => Err(e)?
                                }
                            }
                        }
                    }
                    PipelineState::Terminated => break,
                };
            }
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
        deserialize_without_context,
        test::{mock_parser_pipeline, wait, MockDeZSet, MockInputConsumer},
        transport::s3::{S3InputConfig, S3InputReader},
    };
    use aws_sdk_s3::{
        operation::get_object::GetObjectOutput,
        primitives::{ByteStream, SdkBody},
    };
    use mockall::predicate::eq;
    use pipeline_types::config::InputEndpointConfig;
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
    name: s3
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
format:
    name: csv
"#;

    const SINGLE_KEY_CONFIG_STR: &str = r#"
stream: test_input
transport:
    name: s3
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
        let transport_config: Arc<S3InputConfig> = Arc::new(
            S3InputConfig::deserialize(config.connector_config.transport.config.clone()).unwrap(),
        );
        let (consumer, input_handle) =
            mock_parser_pipeline::<TestStruct, TestStruct>(&config.connector_config.format)
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
        wait(|| input_handle.state().flushed.len() == 3, 100);

        assert_eq!(test_data.len(), input_handle.state().flushed.len());
        for (i, upd) in input_handle.state().flushed.iter().enumerate() {
            assert_eq!(upd.unwrap_insert(), &test_data[i]);
        }
    }

    #[test]
    fn single_key_read() {
        let mut mock = super::MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""))
            .return_once(|_, _| Ok(vec!["obj1".to_string()]));
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
            .with(eq("test-bucket"), eq(""))
            .return_once(|_, _| Ok(vec!["obj1".to_string()]));
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
    fn multi_object_read() {
        let mut mock = super::MockS3Client::default();
        mock.expect_get_object_keys()
            .with(eq("test-bucket"), eq(""))
            .return_once(|_, _| Ok(vec!["obj1".to_string(), "obj2".to_string()]));
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
            .with(eq("test-bucket"), eq(""))
            .return_once(|_, _| Ok(objs));
        let test_data: Vec<TestStruct> = (0..1000).map(|i| TestStruct { i }).collect();
        let (reader, _, input_handle) = test_setup(MULTI_KEY_CONFIG_STR, mock);
        reader.start(0).unwrap();
        // Pause after 50 rows are recorded.
        wait(|| input_handle.state().flushed.len() > 50, 1000);
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
        );

        assert_eq!(test_data.len(), input_handle.state().flushed.len());
        for (i, upd) in input_handle.state().flushed.iter().enumerate() {
            assert_eq!(upd.unwrap_insert(), &test_data[i]);
        }
    }
}
