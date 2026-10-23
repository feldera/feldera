//! The checks of `tests`, against a Google Cloud project, and two subscribers that
//! split the messages of a topic by a filter.
//!
//! To run these cases, you need a Google Cloud account containing a project named
//! 'feldera-test'. Authenticate to that account using 'gcloud' like this:
//! `./bin/gcloud auth application-default login`.
//! Detailed instructions: <https://cloud.google.com/pubsub/docs/authentication#client-libs>

use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, Updates};
use proptest::prelude::*;
use serde_json::json;
use tracing::info;

use super::tests::{TestPublisher, check_input, check_unordered, flag, relation};
use crate::test::{init_test_logger, input_pipeline, published_schema, rows};

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

/// Returns the rows of the batches of `data` whose attribute `flag` is `value`.
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

/// The proptest configuration of the cases: two cases, without shrinking or persisted
/// failures.
fn config() -> ProptestConfig {
    ProptestConfig {
        failure_persistence: None, // Disable persistence of failures
        max_shrink_iters: 0,       // Disable shrinking
        cases: 2,
        ..ProptestConfig::default()
    }
}

/// Random batches arrive through a subscription, with Application Default Credentials.
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
pub fn random_multiple_subscribers(engine: &dyn ConnectorTestEngine) {
    proptest!(config(), |(data in rows::batches(&relation(), "id", 0..=10, 100), topic_id in 0..1000000)| {
        crate::ensure_default_crypto_provider();

        // Use randomized topic name; otherwise deleting and creating a topic is slow (~30s for me).
        let topic = format!("test_pubsub_input-{topic_id}");

        check_multiple_subscribers(engine, data, &topic);
    });
}
