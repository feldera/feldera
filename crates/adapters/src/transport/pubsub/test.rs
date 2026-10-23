use crate::test::{
    engine_test, init_test_logger, input_pipeline, published_schema, relations, render, rows,
};
#[cfg(feature = "pubsub-gcp-test")]
use crate::test::{engine_test, init_test_logger, input_pipeline, published_schema, rows};
use arrow::array::AsArray;
use feldera_adapterlib::test::{
    ConnectorTestEngine, InputOptions, TestCollection, Updates, wait_for_rows,
};
#[cfg(feature = "pubsub-gcp-test")]
use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, Updates};
#[cfg(feature = "pubsub-gcp-test")]
use feldera_storage::tokio::TOKIO;
use feldera_storage::tokio::TOKIO;
use feldera_types::program_schema::Relation;
use google_cloud_gax::conn::Environment;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::{
    client::{Client, ClientConfig},
    publisher::Publisher,
    subscription::SubscriptionConfig,
    topic::Topic,
};
#[cfg(feature = "pubsub-gcp-test")]
use proptest::prelude::*;
use proptest::prelude::*;
#[cfg(feature = "pubsub-gcp-test")]
use serde_json::json;
use serde_json::{Map, json};
use std::{
    collections::HashMap,
    io,
    net::{TcpStream, ToSocketAddrs},
    thread::sleep,
    time::Duration,
};
#[cfg(feature = "pubsub-gcp-test")]
use tracing::info;
use tracing::info;

pub(super) static EMULATOR_PROJECT_ID: &str = "feldera-test";

/// The table that the cases read into, with the columns of [`relations::basic`].
pub fn relation() -> Relation {
    relations::basic("pubsub")
}

/// Returns the rows of `batches`, in order.
fn concat(batches: &[Updates]) -> Updates {
    Updates::concat(published_schema(&relation()), batches).unwrap()
}

/// Returns column `b` of the first row of `batch`, which [`TestPublisher::send`] writes into
/// the attribute `flag` of the message of the batch.
pub(super) fn flag(batch: &Updates) -> bool {
    batch
        .rows()
        .column_by_name("b")
        .unwrap()
        .as_boolean()
        .value(0)
}

/// Waits for the rows of `expected` to arrive in `collection`, in any order, calling `flush`.
pub(super) fn check_unordered(
    collection: &dyn TestCollection,
    expected: &Updates,
    flush: impl FnMut(),
) {
    let updates = wait_for_rows(collection, expected.len(), flush);
    assert!(updates.weights().iter().all(|weight| *weight == 1));
    assert_eq!(updates.zset().unwrap(), expected.zset().unwrap());
}

fn emulator() -> String {
    std::env::var("PUBSUB_EMULATOR_HOST").unwrap_or_else(|_| "127.0.0.1:8685".to_string())
}

fn probe_port(address: &str) -> bool {
    // The emulator address may be a hostname, e.g. a docker service name in
    // CI, so resolve rather than parse an IP literal.
    let resolved = address
        .to_socket_addrs()
        .unwrap_or_else(|e| panic!("cannot resolve emulator address {address}: {e}"))
        .next()
        .unwrap_or_else(|| panic!("emulator address {address} resolved to nothing"));
    let connection_result = TcpStream::connect_timeout(&resolved, Duration::from_secs(1));

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

pub(super) struct TestPublisher {
    client: Client,
    publisher: Publisher,
    topic: Topic,
}

impl TestPublisher {
    fn with_emulator(topic: &str, emulator: &str) -> Self {
        TOKIO.block_on(async {
            let config = ClientConfig {
                project_id: Some(EMULATOR_PROJECT_ID.to_string()),
                environment: Environment::Emulator(emulator.to_string()),
                ..Default::default()
            };
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

    pub(super) fn with_auth(topic_id: &str) -> Self {
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

    pub(super) fn create_subscription(&self, topic: &str, filter: &str) -> String {
        let subscription_id = format!("sub-{}", uuid::Uuid::new_v4());
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

    /// Publishes each batch of `data` as one message of CSV records, with the attribute `flag`
    /// of its first row's column `b`.
    pub(super) fn send(&self, data: &[Updates], ordered: bool) {
        TOKIO.block_on(async {
            info!("sending");

            for batch in data {
                // Pub/Sub doesn't allow empty messages.
                if batch.is_empty() {
                    continue;
                }
                let message = PubsubMessage {
                    data: render::csv(&relation(), batch.rows()),
                    ordering_key: if ordered {
                        "1".to_string()
                    } else {
                        String::new()
                    },
                    attributes: HashMap::from_iter([("flag".to_string(), flag(batch).to_string())]),
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

/// Publishes `data` to `topic` and reads it through a subscription, then checks that a paused
/// endpoint reads nothing until it resumes, and a disconnected one reads nothing.
pub(super) fn check_input(
    engine: &dyn ConnectorTestEngine,
    data: Vec<Updates>,
    topic: &str,
    config: &[(&str, &str)],
    emulator: Option<&str>,
) {
    init_test_logger();

    let publisher = if let Some(emulator) = emulator {
        if !probe_port(emulator) {
            panic!(
                "Pub/Sub emulator not found on port '{emulator}', start the emulator before running the tests: 'gcloud beta emulators pubsub start --project=feldera-test --host-port={emulator}'"
            )
        }

        TestPublisher::with_emulator(topic, emulator)
    } else {
        TestPublisher::with_auth(topic)
    };

    let subscription = publisher.create_subscription(topic, "");

    let mut transport_config = Map::new();
    transport_config.insert("enable_message_ordering".into(), true.into());
    transport_config.insert("subscription".into(), subscription.into());
    for (k, v) in config {
        transport_config.insert((*k).into(), (*v).into());
    }

    let config = serde_json::from_value(json!({
      "stream": "test_input",
      "transport": {
        "name": "pub_sub_input",
        "config": transport_config
      },
      "format": {
        "name": "csv"
      }
    }))
    .unwrap();

    info!("test_pubsub_input: Building input pipeline");

    let pipeline = input_pipeline(engine, &relation(), &InputOptions::default(), config).unwrap();
    let endpoint = &pipeline.reader;
    let collection = pipeline.collection.as_ref();

    endpoint.extend();

    info!("test_pubsub_input: Test: Receive from a topic");

    // Send data to a topic with a single partition;
    publisher.send(&data, false);

    let expected = concat(&data);
    check_unordered(collection, &expected, || endpoint.queue(false));
    collection.reset();

    info!("test_pubsub_input: Test: pause/resume");

    // Paused endpoint shouldn't receive any data.
    endpoint.pause();
    sleep(Duration::from_millis(1000));

    publisher.send(&data, true);

    sleep(Duration::from_millis(1000));
    // Only a flush moves what the endpoint read into the collection.
    endpoint.queue(false);
    sleep(Duration::from_millis(1000));
    assert_eq!(collection.len(), 0);

    // Receive everything after unpause.
    endpoint.extend();
    let updates = wait_for_rows(collection, expected.len(), || endpoint.queue(false));
    assert_eq!(updates, expected);
    collection.reset();

    info!("test_pubsub_input: Test: Disconnect");
    // Disconnected endpoint should not receive any data.
    endpoint.disconnect();
    sleep(Duration::from_millis(1000));

    publisher.send(&data, true);
    sleep(Duration::from_millis(1000));
    assert_eq!(collection.len(), 0);
}

/// Returns the rows of the batches of `data` whose attribute `flag` is `value`.
#[cfg(feature = "pubsub-gcp-test")]
fn flagged(data: &[Updates], value: bool) -> Updates {
    let batches: Vec<Updates> = data
        .iter()
        .filter(|batch| !batch.is_empty() && flag(batch) == value)
        .cloned()
        .collect();
    Updates::concat(published_schema(&relation()), &batches).unwrap()
}

// Create two subscribers, which should split the messages between them
// based on a filter.
#[cfg(feature = "pubsub-gcp-test")]
fn check_multiple_subscribers(engine: &dyn ConnectorTestEngine, data: Vec<Updates>, topic: &str) {
    init_test_logger();

    let publisher = TestPublisher::with_auth(topic);
    let subscription1 = publisher.create_subscription(topic, "attributes.flag = \"true\"");
    let subscription2 = publisher.create_subscription(topic, "attributes.flag = \"false\"");

    let config1 = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "pub_sub_input",
            "config": {
                "subscription": subscription1
            }
        },
        "format": {
            "name": "csv"
        }
    }))
    .unwrap();

    let config2 = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "pub_sub_input",
            "config": {
                "subscription": subscription2
            }
        },
        "format": {
            "name": "csv"
        }
    }))
    .unwrap();

    info!("test_pubsub_multiple_subscribers: Creating subscribers");

    let pipeline1 = input_pipeline(engine, &relation(), &InputOptions::default(), config1).unwrap();
    let endpoint1 = &pipeline1.reader;

    endpoint1.extend();

    let pipeline2 = input_pipeline(engine, &relation(), &InputOptions::default(), config2).unwrap();
    let endpoint2 = &pipeline2.reader;

    endpoint2.extend();

    info!("test_pubsub_multiple_subscribers: Sending data");

    // Send data to a topic with a single partition;
    publisher.send(&data, true);

    info!("test_pubsub_multiple_subscribers: Receiving");

    let data1 = flagged(&data, true);
    let data2 = flagged(&data, false);

    check_unordered(pipeline1.collection.as_ref(), &data1, || {
        endpoint1.queue(false);
        endpoint2.queue(false);
    });
    pipeline1.collection.reset();

    check_unordered(pipeline2.collection.as_ref(), &data2, || {
        endpoint1.queue(false);
        endpoint2.queue(false);
    });
    pipeline2.collection.reset();

    info!("test_pubsub_multiple_subscribers: Test: Disconnect");
    endpoint1.disconnect();
    endpoint2.disconnect();
}

/// An endpoint does not open with an emulator address that does not resolve, or with a
/// subscription that does not exist.
#[engine_test(cfg(feature = "pubsub-emulator-test"), services(PubSubEmulator))]
pub fn input_errors(engine: &dyn ConnectorTestEngine) {
    let emulator = emulator();

    if !probe_port(&emulator) {
        panic!(
            "Pub/Sub emulator not found on port {emulator}', start the emulator before running the tests: 'gcloud beta emulators pubsub start --project=feldera-test --host-port={emulator}'"
        )
    }

    info!("test_pubsub_input: Test: Specify invalid service address");

    let config = serde_json::from_value(json!({
      "stream": "test_input",
      "transport": {
        "name": "pub_sub_input",
        "config": {
          "subscription": "foo",
          "emulator": "not_an_emulator:5678"
        }
      },
      "format": {
        "name": "csv"
      }
    }))
    .unwrap();

    match input_pipeline(engine, &relation(), &InputOptions::default(), config) {
        Ok(_) => panic!("expected an error"),
        Err(error) => assert_eq!(
            error.to_string(),
            "error connecting to the Pub/Sub service: tonic error : transport error"
        ),
    };

    info!("test_pubsub_input: Test: Specify invalid subscription name");

    let config = serde_json::from_value(json!({
      "stream": "test_input",
      "transport": {
        "name": "pub_sub_input",
        "config": {
          "project_id": EMULATOR_PROJECT_ID,
          "emulator": emulator,
          "subscription": "this_subscription_does_not_exist"
        }
      },
      "format": {
        "name": "csv"
      }
    }))
    .unwrap();

    match input_pipeline(engine, &relation(), &InputOptions::default(), config) {
        Ok(_) => panic!("expected an error"),
        Err(error) => assert_eq!(
            error.to_string(),
            "subscription 'this_subscription_does_not_exist' does not exist"
        ),
    };
}

/// Random batches arrive through a subscription of the emulator.
#[engine_test(cfg(feature = "pubsub-emulator-test"), services(PubSubEmulator))]
pub fn random_input_with_emulator(engine: &dyn ConnectorTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=10, 100))| {
        let emulator = emulator();

        let config  = [
            ("project_id", EMULATOR_PROJECT_ID),
            ("emulator", &emulator),
        ];

        check_input(engine, data, "test_pubsub_input", config.as_slice(), Some(&emulator));
    });
}

/// The proptest configuration of the cases: two cases, without shrinking or persisted
/// failures.
#[cfg(feature = "pubsub-gcp-test")]
fn config() -> ProptestConfig {
    ProptestConfig {
        failure_persistence: None, // Disable persistence of failures
        max_shrink_iters: 0,       // Disable shrinking
        cases: 2,
        ..ProptestConfig::default()
    }
}

/// Random batches arrive through a subscription, with Application Default Credentials.
#[cfg(feature = "pubsub-gcp-test")]
#[engine_test]
pub fn random_input_with_auth(engine: &dyn ConnectorTestEngine) {
    proptest!(config(), |(data in rows::batches(&relation(), "id", 0..=10, 100), topic_id in 0..1000000)| {
        crate::ensure_default_crypto_provider();

        // Use randomized topic name; otherwise deleting and creating a topic is slow (~30s for me).
        let topic = format!("test_pubsub_input{topic_id}");

        let config  = [];

        check_input(engine, data, &topic, config.as_slice(), None);
    });
}

/// Random batches arrive through a subscription, with credentials in the connector's
/// configuration.
#[cfg(feature = "pubsub-gcp-test")]
#[engine_test]
pub fn random_input_with_credentials(engine: &dyn ConnectorTestEngine) {
    proptest!(config(), |(data in rows::batches(&relation(), "id", 0..=10, 100), topic_id in 0..1000000)| {
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

        let credentials_str = serde_json::to_string(&credentials_str).unwrap();
        let config  = [
            ("credentials", credentials_str.as_str())
        ];

        check_input(engine, data, &topic, config.as_slice(), None);
    });
}

/// Two subscribers with filters split random batches between them.
#[cfg(feature = "pubsub-gcp-test")]
#[engine_test]
pub fn random_multiple_subscribers(engine: &dyn ConnectorTestEngine) {
    proptest!(config(), |(data in rows::batches(&relation(), "id", 0..=10, 100), topic_id in 0..1000000)| {
        crate::ensure_default_crypto_provider();

        // Use randomized topic name; otherwise deleting and creating a topic is slow (~30s for me).
        let topic = format!("test_pubsub_input-{topic_id}");

        check_multiple_subscribers(engine, data, &topic);
    });
}
