use std::{collections::HashMap, fmt::Debug, io, net::TcpStream, thread::sleep, time::Duration};

use crate::test::{
    init_test_logger, mock_input_pipeline, wait_for_output_ordered, wait_for_output_unordered,
    TestStruct,
};
use csv::WriterBuilder;
use dbsp::circuit::tokio::TOKIO;
use feldera_types::program_schema::Relation;
use google_cloud_gax::conn::Environment;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::{
    client::{Client, ClientConfig},
    publisher::Publisher,
    subscription::SubscriptionConfig,
    topic::Topic,
};
use proptest::prelude::*;
use serde::Serialize;
use tracing::info;

static EMULATOR_PROJECT_ID: &str = "feldera-test";

fn probe_port(address: &str) -> bool {
    let connection_result =
        TcpStream::connect_timeout(&address.parse().unwrap(), Duration::from_secs(1));

    match connection_result {
        Ok(_) => true, // Port is open
        Err(e) => {
            match e.kind() {
                io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut => false, // Port is closed or unreachable
                _ => {
                    panic!("Error probing port {}: {:?}", address, e);
                }
            }
        }
    }
}

#[cfg(feature = "pubsub-gcp-test")]
async fn credentials_json_from_file() -> Result<Vec<u8>, anyhow::Error> {
    use anyhow::bail;
    let path = match std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        Ok(s) => Ok::<_, anyhow::Error>(std::path::Path::new(s.as_str()).to_path_buf()),
        Err(_e) => {
            // get well known file name
            if cfg!(target_os = "windows") {
                let app_data = std::env::var("APPDATA")?;
                Ok(std::path::Path::new(app_data.as_str())
                    .join("gcloud")
                    .join("application_default_credentials.json"))
            } else {
                match home::home_dir() {
                    Some(s) => Ok(s
                        .join(".config")
                        .join("gcloud")
                        .join("application_default_credentials.json")),
                    None => bail!("no home directory found"),
                }
            }
        }
    }?;

    let credentials_json = tokio::fs::read(path).await?;

    Ok(credentials_json)
}

struct TestPublisher {
    client: Client,
    publisher: Publisher,
    topic: Topic,
}

impl TestPublisher {
    fn with_emulator(topic: &str, emulator: &str) -> Self {
        TOKIO.block_on(async {
            let mut config = ClientConfig::default();
            config.project_id = Some(EMULATOR_PROJECT_ID.to_string());
            config.environment = Environment::Emulator(emulator.to_string());
            let client = Client::new(config).await.unwrap();

            // Create topic.
            let topic = client.topic(topic);
            if topic.exists(None).await.unwrap() {
                topic.delete(None).await.unwrap();
            }

            topic.create(None, None).await.unwrap();

            Self {
                client,
                publisher: topic.new_publisher(None),
                topic,
            }
        })
    }

    fn with_auth(topic_id: &str) -> Self {
        TOKIO.block_on(async {
            info!("Creating test publisher using Application Default Credentials");

            let config = ClientConfig::default().with_auth().await.unwrap();
            let client = Client::new(config).await.unwrap();

            // Create topic.
            let topic = client.topic(topic_id);
            if topic.exists(None).await.unwrap() {
                info!("Deleting existing topic {topic_id}");

                topic.delete(None).await.unwrap();
            }

            info!("Creating topic {topic_id}");
            topic.create(None, None).await.unwrap();

            let publisher = topic.new_publisher(None);

            info!("Test publisher created");

            Self {
                client,
                publisher,
                topic,
            }
        })
    }

    fn create_subscription(&self, topic: &str, filter: &str) -> String {
        let subscription_id = format!("sub-{}", &uuid::Uuid::new_v4());
        TOKIO.block_on(async {
            self.client
                .create_subscription(
                    &subscription_id,
                    topic,
                    SubscriptionConfig {
                        enable_message_ordering: true,
                        filter: filter.to_string(),
                        retain_acked_messages: true,
                        ..Default::default()
                    },
                    None,
                )
                .await
                .unwrap();
        });

        subscription_id
    }

    fn send<T, LF>(&self, data: &[Vec<T>], ordered: bool, label_func: LF)
    where
        T: Clone + Serialize + Debug,
        LF: Fn(&T) -> HashMap<String, String>,
    {
        TOKIO.block_on(async {
            info!("sending");

            for batch in data {
                // Pub/Sub doesn't allow empty messages.
                if batch.is_empty() {
                    continue;
                }
                let mut writer = WriterBuilder::new()
                    .has_headers(false)
                    .from_writer(Vec::with_capacity(batch.len() * 32));

                for val in batch.iter().cloned() {
                    writer.serialize(val).unwrap();
                }
                writer.flush().unwrap();
                let bytes = writer.into_inner().unwrap();

                let message = PubsubMessage {
                    data: bytes,
                    ordering_key: if ordered {
                        "1".to_string()
                    } else {
                        String::new()
                    },
                    attributes: label_func(&batch[0]),
                    ..Default::default()
                };
                self.publisher.publish(message).await.get().await.unwrap();
            }
            info!("done sending");
        });
    }
}

impl Drop for TestPublisher {
    fn drop(&mut self) {
        TOKIO.block_on(async {
            info!("Deleting topic");
            self.topic.delete(None).await.unwrap()
        })
    }
}

fn label_func(x: &TestStruct) -> HashMap<String, String> {
    HashMap::from_iter([("flag".to_string(), x.b.to_string())])
}

fn test_pubsub_input(
    data: Vec<Vec<TestStruct>>,
    topic: &str,
    config: &[(&str, &str)],
    emulator: Option<&str>,
) {
    init_test_logger();

    let publisher = if let Some(emulator) = emulator {
        if !probe_port(&emulator) {
            panic!("Pub/Sub emulator not found on port '{emulator}', start the emulator before running the tests: 'gcloud beta emulators pubsub start --project=feldera-test --host-port={emulator}'")
        }

        TestPublisher::with_emulator(topic, &emulator)
    } else {
        TestPublisher::with_auth(topic)
    };

    let subscription = publisher.create_subscription(topic, "");

    let mut config_str = String::new();
    for (k, v) in config {
        config_str += &format!("            {k}: {v}\n");
    }

    let config_str = format!(
        r#"
    stream: test_input
    transport:
        name: pub_sub_input
        config:
            enable_message_ordering: true
            subscription: {subscription}
{config_str}
    format:
        name: csv
    "#
    );

    info!("test_pubsub_input: Building input pipeline");

    let (endpoint, _consumer, _parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
    )
    .unwrap();

    endpoint.extend();

    info!("test_pubsub_input: Test: Receive from a topic");

    // Send data to a topic with a single partition;
    publisher.send(&data, false, label_func);

    wait_for_output_unordered(&zset, &data, || endpoint.queue());
    zset.reset();

    info!("test_pubsub_input: Test: pause/resume");
    // //println!("records before pause: {}", zset.state().flushed.len());

    // Paused endpoint shouldn't receive any data.
    endpoint.pause();
    sleep(Duration::from_millis(1000));

    publisher.send(&data, true, label_func);

    sleep(Duration::from_millis(1000));
    assert_eq!(zset.state().flushed.len(), 0);

    // Receive everything after unpause.
    endpoint.extend();
    wait_for_output_ordered(&zset, &data, || endpoint.queue());
    zset.reset();

    info!("test_pubsub_input: Test: Disconnect");
    // Disconnected endpoint should not receive any data.
    endpoint.disconnect();
    sleep(Duration::from_millis(1000));

    publisher.send(&data, true, label_func);
    sleep(Duration::from_millis(1000));
    assert_eq!(zset.state().flushed.len(), 0);
}

// Create two subscribers, which should split the messages between them
// based on a filter.
#[cfg(feature = "pubsub-gcp-test")]
fn test_pubsub_multiple_subscribers(data: Vec<Vec<TestStruct>>, topic: &str) {
    init_test_logger();

    let publisher = TestPublisher::with_auth(topic);
    let subscription1 = publisher.create_subscription(topic, "attributes.flag = \"true\"");
    let subscription2 = publisher.create_subscription(topic, "attributes.flag = \"false\"");

    let config_str1 = format!(
        r#"
    stream: test_input
    transport:
        name: pub_sub_input
        config:
            subscription: {subscription1}
    format:
        name: csv
    "#
    );

    let config_str2 = format!(
        r#"
    stream: test_input
    transport:
        name: pub_sub_input
        config:
            subscription: {subscription2}
    format:
        name: csv
    "#
    );

    info!("test_pubsub_multiple_subscribers: Creating subscribers");

    let (endpoint1, _consumer, _parser, zset1) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(&config_str1).unwrap(),
        Relation::empty(),
    )
    .unwrap();

    endpoint1.extend();

    let (endpoint2, _consumer, _parser, zset2) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(&config_str2).unwrap(),
        Relation::empty(),
     )
    .unwrap();

    endpoint2.extend();

    info!("test_pubsub_multiple_subscribers: Sending data");

    // Send data to a topic with a single partition;
    publisher.send(&data, true, label_func);

    info!("test_pubsub_multiple_subscribers: Receiving");

    let data1 = data
        .iter()
        .cloned()
        .filter(|batch| !batch.is_empty() && batch[0].b)
        .collect::<Vec<_>>();

    let data2 = data
        .iter()
        .cloned()
        .filter(|batch| !batch.is_empty() && !batch[0].b)
        .collect::<Vec<_>>();

    wait_for_output_unordered(&zset1, &data1, || {
        endpoint1.queue();
        endpoint2.queue();
    });
    zset1.reset();

    wait_for_output_unordered(&zset2, &data2, || {
        endpoint1.queue();
        endpoint2.queue();
    });
    zset2.reset();

    info!("test_pubsub_multiple_subscribers: Test: Disconnect");
    endpoint1.disconnect();
    endpoint2.disconnect();
}

#[cfg(feature = "pubsub-emulator-test")]
#[test]
fn test_pubsub_input_errors() {
    let emulator =
        std::env::var("PUBSUB_EMULATOR_HOST").unwrap_or_else(|_| "127.0.0.1:8685".to_string());
    //let emulator = Emulator::new();

    if !probe_port(&emulator) {
        panic!("Pub/Sub emulator not found on port {emulator}', start the emulator before running the tests: 'gcloud beta emulators pubsub start --project=feldera-test --host-port={emulator}'")
    }

    info!("test_pubsub_input: Test: Specify invalid service address");

    let config_str = format!(
        r#"
    stream: test_input
    transport:
        name: pub_sub_input
        config:
            subscription: foo
            emulator: "not_an_emulator:5678"
    format:
        name: csv
    "#
    );

    match mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
    ) {
        Ok(_) => panic!("expected an error"),
        Err(e) => println!("test_pubsub_input: Error (expected): {e}"),
    };

    info!("test_pubsub_input: Test: Specify invalid subscription name");

    let config_str = format!(
        r#"
    stream: test_input
    transport:
        name: pub_sub_input
        config:
            project_id: {EMULATOR_PROJECT_ID}
            emulator: "{emulator}"
            subscription: this_subscription_does_not_exist
    format:
        name: csv
    "#
    );

    match mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
      ) {
        Ok(_) => panic!("expected an error"),
        Err(e) => println!("test_pubsub_input: Error (expected): {e}"),
    };
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2))]

    // Running this test requires starting a Pub/Sub emulator:
    // `gcloud beta emulators pubsub start --project=feldera-test --host-port=127.0.0.1:8685`.
    // If you use a different port number, set the PUBSUB_EMULATOR_HOST env variable to
    // point to that port.
    #[cfg(feature = "pubsub-emulator-test")]
    #[test]
    fn proptest_pubsub_input_with_emulator(data in crate::test::generate_test_batches(0, 10, 100)) {
        let emulator =
            std::env::var("PUBSUB_EMULATOR_HOST").unwrap_or_else(|_| "127.0.0.1:8685".to_string());

        let config  = [
            ("project_id", EMULATOR_PROJECT_ID),
            ("emulator", &emulator),
        ];

        test_pubsub_input(data, "test_pubsub_input", config.as_slice(), Some(&emulator));
    }
}

proptest! {
    // #![proptest_config(ProptestConfig::with_cases(2))]
    #![proptest_config(ProptestConfig {
        failure_persistence: None, // Disable persistence of failures
        max_shrink_iters: 0,       // Disable shrinking
        cases: 2,
        .. ProptestConfig::default()
    })]

    // To run these tests, you need a Google Cloud account containing a project named 'feldera-test'.
    // Authenticate to that account using 'gcloud' like this:
    // `./bin/gcloud auth application-default login``
    // Detailed instructions:
    // https://cloud.google.com/pubsub/docs/authentication#client-libs

    #[cfg(feature = "pubsub-gcp-test")]
    #[test]
    fn proptest_pubsub_input_with_gcp_auth(data in crate::test::generate_test_batches(0, 10, 100), topic_id in 0..1000000) {
        crate::ensure_default_crypto_provider();

        // Use randomized topic name; otherwise deleting and creating a topic is slow (~30s for me).
        let topic = format!("test_pubsub_input{topic_id}");

        let config  = [];

        test_pubsub_input(data, &topic, config.as_slice(), None);
    }

    #[cfg(feature = "pubsub-gcp-test")]
    #[test]
    fn proptest_pubsub_input_with_gcp_credentials(data in crate::test::generate_test_batches(0, 10, 100), topic_id in 0..1000000) {
        crate::ensure_default_crypto_provider();

        // Use randomized topic name; otherwise deleting and creating a topic is slow (~30s for me).
        let topic = format!("test_pubsub_input{topic_id}");

        let credentials = TOKIO.block_on(async {
            credentials_json_from_file()
                .await
                .map_err(|e| format!("Error finding GCP credentials file: {e}"))
                .unwrap()
        });
        let credentials_str = String::from_utf8(credentials).unwrap();
        // println!("Credentials: {credentials_str}");

        let credentials_str = serde_json::to_string(&credentials_str).unwrap();
        let config  = [
            ("credentials", credentials_str.as_str())
        ];

        test_pubsub_input(data, &topic, config.as_slice(), None);
    }

    #[cfg(feature = "pubsub-gcp-test")]
    #[test]
    fn proptest_pubsub_multiple_subscribers(data in crate::test::generate_test_batches(0, 10, 100), topic_id in 0..1000000) {
        crate::ensure_default_crypto_provider();

        // Use randomized topic name; otherwise deleting and creating a topic is slow (~30s for me).
        let topic = format!("test_pubsub_input-{topic_id}");

        test_pubsub_multiple_subscribers(data, &topic);
    }

}
