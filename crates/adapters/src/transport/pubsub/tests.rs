//! The Pub/Sub input connector: CSV messages read from a subscription, in order when they carry
//! an ordering key, and nothing read while the endpoint is paused or after it disconnects.
//!
//! The cases need a Pub/Sub emulator at `PUBSUB_EMULATOR_HOST`, by default `127.0.0.1:8685`:
//! `gcloud beta emulators pubsub start --project=feldera-test --host-port=127.0.0.1:8685`.
//! `gcp_tests` runs the same checks against a Google Cloud project.

use std::collections::HashMap;
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::thread::sleep;
use std::time::Duration;

use arrow::array::AsArray;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::test::{
    ConnectorTestEngine, InputOptions, TestCollection, Updates, wait_for_rows,
};
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
use serde_json::{Map, json};
use tracing::info;

use crate::test::{init_test_logger, input_pipeline, published_schema, relations, render, rows};

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

/// An endpoint does not open with an emulator address that does not resolve, or with a
/// subscription that does not exist.
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
