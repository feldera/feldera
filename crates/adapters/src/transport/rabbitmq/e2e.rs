//! Opt-in end-to-end tests against a live RabbitMQ 4.x broker over AMQP 1.0.
//!
//! Entities are declared via the HTTP management API (AMQP 1.0 cannot declare);
//! messages are published with `fe2o3-amqp`; ingest goes through the real input
//! endpoint, and the output endpoint publishes to a bound queue.
//!
//! Run with:
//! ```text
//! RABBITMQ_E2E=1 cargo test -p dbsp_adapters --features with-rabbitmq rabbitmq_e2e -- --nocapture
//! ```
//! Optional env: `RABBITMQ_HOST` (localhost), `RABBITMQ_AMQP_PORT` (5672),
//! `RABBITMQ_MANAGEMENT_URL` (http://localhost:15672).

use super::RabbitmqOutputEndpoint;
use crate::OutputEndpoint;
use crate::test::{DEFAULT_TIMEOUT_MS, mock_input_pipeline, wait};
use dbsp::circuit::tokio::TOKIO;
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::types::messaging::Message;
use fe2o3_amqp::types::primitives::Binary;
use fe2o3_amqp::{Connection, Sender, Session};
use feldera_types::deserialize_without_context;
use feldera_types::{
    config::InputEndpointConfig,
    program_schema::{ColumnType, Field, Relation, SqlIdentifier},
    transport::rabbitmq::{RabbitmqInputConfig, RabbitmqOffsetConfig, RabbitmqOutputConfig},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
struct E2eRow {
    n: i64,
}

deserialize_without_context!(E2eRow);

fn e2e_relation() -> Relation {
    Relation::new(
        SqlIdentifier::from("test_input"),
        vec![Field::new("n".into(), ColumnType::bigint(false))],
        true,
        BTreeMap::new(),
    )
}

fn host() -> String {
    std::env::var("RABBITMQ_HOST").unwrap_or_else(|_| "localhost".to_string())
}

fn amqp_port() -> u16 {
    std::env::var("RABBITMQ_AMQP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5672)
}

fn management_url() -> String {
    std::env::var("RABBITMQ_MANAGEMENT_URL")
        .unwrap_or_else(|_| "http://localhost:15672".to_string())
}

fn broker_reachable() -> bool {
    let Ok(addrs) = (host().as_str(), amqp_port()).to_socket_addrs() else {
        return false;
    };
    addrs
        .into_iter()
        .any(|addr| TcpStream::connect_timeout(&addr, Duration::from_secs(2)).is_ok())
}

/// Declare a queue/stream via the management HTTP API (idempotent).
async fn declare_queue(name: &str, queue_type: &str) -> anyhow::Result<()> {
    let body = serde_json::json!({
        "durable": true,
        "arguments": { "x-queue-type": queue_type },
    });
    let resp = reqwest::Client::new()
        .put(format!("{}/api/queues/%2F/{name}", management_url()))
        .basic_auth("guest", Some("guest"))
        .json(&body)
        .send()
        .await?;
    anyhow::ensure!(
        resp.status().is_success(),
        "declare queue {name}: {}",
        resp.status()
    );
    Ok(())
}

async fn declare_exchange(name: &str) -> anyhow::Result<()> {
    let body = serde_json::json!({ "type": "topic", "durable": true });
    let resp = reqwest::Client::new()
        .put(format!("{}/api/exchanges/%2F/{name}", management_url()))
        .basic_auth("guest", Some("guest"))
        .json(&body)
        .send()
        .await?;
    anyhow::ensure!(
        resp.status().is_success(),
        "declare exchange {name}: {}",
        resp.status()
    );
    Ok(())
}

async fn bind_queue(exchange: &str, queue: &str, routing_key: &str) -> anyhow::Result<()> {
    let body = serde_json::json!({ "routing_key": routing_key });
    let resp = reqwest::Client::new()
        .post(format!(
            "{}/api/bindings/%2F/e/{exchange}/q/{queue}",
            management_url()
        ))
        .basic_auth("guest", Some("guest"))
        .json(&body)
        .send()
        .await?;
    anyhow::ensure!(
        resp.status().is_success(),
        "bind {queue}->{exchange}: {}",
        resp.status()
    );
    Ok(())
}

async fn open_conn(
    container: &str,
) -> anyhow::Result<fe2o3_amqp::connection::ConnectionHandle<()>> {
    Ok(Connection::builder()
        .container_id(container)
        .hostname("vhost:/")
        .sasl_profile(SaslProfile::Plain {
            username: "guest".into(),
            password: "guest".into(),
        })
        .open(format!("amqp://{}:{}", host(), amqp_port()).as_str())
        .await?)
}

/// Publish `count` JSON rows `{"n":i}` to an AMQP 1.0 address.
async fn publish_rows(address: &str, count: usize) -> anyhow::Result<()> {
    let mut conn = open_conn("feldera-e2e-pub").await?;
    let mut session = Session::begin(&mut conn).await?;
    let mut sender = Sender::attach(&mut session, "feldera-e2e-pub", address).await?;
    for i in 0..count {
        let body = format!(r#"{{"n":{i}}}"#);
        sender
            .send(
                Message::builder()
                    .data(Binary::from(body.into_bytes()))
                    .build(),
            )
            .await?
            .accepted_or_else(|o| anyhow::anyhow!("publish not accepted: {o:?}"))?;
    }
    sender.close().await.ok();
    session.end().await.ok();
    conn.close().await.ok();
    Ok(())
}

fn input_config(transport: RabbitmqInputConfig) -> InputEndpointConfig {
    serde_json::from_value(serde_json::json!({
        "stream": "test_input",
        "transport": { "name": "rabbitmq_input", "config": transport },
        "format": { "name": "json", "config": { "update_format": "raw", "array": false } }
    }))
    .unwrap()
}

fn base_input(queue: &str) -> RabbitmqInputConfig {
    RabbitmqInputConfig {
        host: host(),
        port: amqp_port(),
        vhost: "/".to_string(),
        username: "guest".to_string(),
        password: "guest".to_string(),
        queue: queue.to_string(),
        offset: None,
        tls: false,
        tls_ca_pem: None,
        consumer_name: None,
    }
}

fn ingest_and_assert(config: InputEndpointConfig, expect: usize, ctx: &str) {
    let (reader, consumer, _parser, zset) =
        mock_input_pipeline::<E2eRow, E2eRow>(config, e2e_relation()).unwrap();
    reader.extend();
    let ok = wait(
        || {
            reader.queue(false);
            zset.state().flushed.len() >= expect
        },
        DEFAULT_TIMEOUT_MS,
    );
    assert!(
        ok.is_ok(),
        "{ctx}: expected {expect} rows, got {}",
        zset.state().flushed.len()
    );
    assert!(
        consumer.state().endpoint_error.is_none(),
        "{ctx}: connector error: {:?}",
        consumer.state().endpoint_error
    );
}

/// (a) Stream source consumed from the first offset.
#[test]
fn rabbitmq_e2e_stream_first() {
    if std::env::var("RABBITMQ_E2E").is_err() || !broker_reachable() {
        return;
    }
    let stream = format!("feldera_e2e_stream_{}", std::process::id());
    const N: usize = 5;
    TOKIO
        .block_on(async {
            declare_queue(&stream, "stream").await?;
            publish_rows(&format!("/queues/{stream}"), N).await
        })
        .expect("setup stream");

    let mut cfg = base_input(&stream);
    cfg.offset = Some(RabbitmqOffsetConfig::Policy {
        policy: "first".to_string(),
    });
    ingest_and_assert(input_config(cfg), N, "stream_first");
}

/// (a') Stream source with a numeric offset filter (skip the first two).
#[test]
fn rabbitmq_e2e_stream_offset_filter() {
    if std::env::var("RABBITMQ_E2E").is_err() || !broker_reachable() {
        return;
    }
    let stream = format!("feldera_e2e_offset_{}", std::process::id());
    const N: usize = 5;
    TOKIO
        .block_on(async {
            declare_queue(&stream, "stream").await?;
            publish_rows(&format!("/queues/{stream}"), N).await
        })
        .expect("setup stream");

    let mut cfg = base_input(&stream);
    // Start at offset 2 => expect N-2 rows.
    cfg.offset = Some(RabbitmqOffsetConfig::Offset { offset: 2 });
    ingest_and_assert(input_config(cfg), N - 2, "stream_offset_filter");
}

/// (b/c) Output endpoint publishes to an exchange with a routing key; a bound
/// queue receives it and the input endpoint consumes it back.
#[test]
fn rabbitmq_e2e_output_exchange_roundtrip() {
    if std::env::var("RABBITMQ_E2E").is_err() || !broker_reachable() {
        return;
    }
    let pid = std::process::id();
    let exchange = format!("feldera_e2e_ex_{pid}");
    let queue = format!("feldera_e2e_bound_{pid}");
    let routing_key = "results.v1";
    const N: usize = 4;

    TOKIO
        .block_on(async {
            declare_exchange(&exchange).await?;
            declare_queue(&queue, "classic").await?;
            bind_queue(&exchange, &queue, routing_key).await
        })
        .expect("setup exchange+queue+binding");

    // Publish via the output endpoint.
    let out_cfg = RabbitmqOutputConfig {
        host: host(),
        port: amqp_port(),
        vhost: "/".to_string(),
        username: "guest".to_string(),
        password: "guest".to_string(),
        exchange: exchange.clone(),
        routing_key: routing_key.to_string(),
        delivery_mode: Default::default(),
        tls: false,
        tls_ca_pem: None,
    };
    let mut endpoint = RabbitmqOutputEndpoint::new(out_cfg).unwrap();
    endpoint
        .connect(Box::new(|_, _, _| {}))
        .expect("output connect");
    for i in 0..N {
        endpoint
            .push_buffer(format!(r#"{{"n":{i}}}"#).as_bytes())
            .expect("push_buffer");
    }

    // Consume from the bound queue via the input endpoint.
    ingest_and_assert(
        input_config(base_input(&queue)),
        N,
        "output_exchange_roundtrip",
    );
}

fn tls_port() -> u16 {
    std::env::var("RABBITMQ_TLS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5671)
}

/// (d) TLS: output and input endpoints round-trip over `amqps`, trusting the
/// broker's private CA via `tls_ca_pem`. Requires a TLS-enabled broker and
/// `RABBITMQ_TLS_CA` pointing at the CA PEM; `RABBITMQ_MANAGEMENT_URL` must
/// point at that broker's management API.
#[test]
fn rabbitmq_e2e_tls_roundtrip() {
    if std::env::var("RABBITMQ_TLS_E2E").is_err() {
        return;
    }
    let Some(ca_pem) = std::env::var("RABBITMQ_TLS_CA")
        .ok()
        .and_then(|p| std::fs::read_to_string(p).ok())
    else {
        eprintln!("RABBITMQ_TLS_CA not set/readable, skipping TLS e2e");
        return;
    };
    let reachable = (host().as_str(), tls_port())
        .to_socket_addrs()
        .map(|addrs| {
            addrs
                .into_iter()
                .any(|a| TcpStream::connect_timeout(&a, Duration::from_secs(2)).is_ok())
        })
        .unwrap_or(false);
    if !reachable {
        eprintln!("TLS port {}:{} unreachable, skipping", host(), tls_port());
        return;
    }

    let pid = std::process::id();
    let exchange = format!("feldera_tls_ex_{pid}");
    let queue = format!("feldera_tls_q_{pid}");
    let routing_key = "tls.v1";
    const N: usize = 3;

    TOKIO
        .block_on(async {
            declare_exchange(&exchange).await?;
            declare_queue(&queue, "classic").await?;
            bind_queue(&exchange, &queue, routing_key).await
        })
        .expect("setup exchange+queue+binding");

    // Publish over amqps via the output endpoint.
    let out_cfg = RabbitmqOutputConfig {
        host: host(),
        port: tls_port(),
        vhost: "/".to_string(),
        username: "guest".to_string(),
        password: "guest".to_string(),
        exchange: exchange.clone(),
        routing_key: routing_key.to_string(),
        delivery_mode: Default::default(),
        tls: true,
        tls_ca_pem: Some(ca_pem.clone()),
    };
    let mut endpoint = RabbitmqOutputEndpoint::new(out_cfg).unwrap();
    endpoint
        .connect(Box::new(|_, _, _| {}))
        .expect("tls output connect");
    for i in 0..N {
        endpoint
            .push_buffer(format!(r#"{{"n":{i}}}"#).as_bytes())
            .expect("push_buffer");
    }

    // Consume over amqps via the input endpoint.
    let mut in_cfg = base_input(&queue);
    in_cfg.port = tls_port();
    in_cfg.tls = true;
    in_cfg.tls_ca_pem = Some(ca_pem);
    ingest_and_assert(input_config(in_cfg), N, "tls_roundtrip");
}
