use std::collections::HashMap;
use std::time::Duration;

use async_nats::HeaderMap;
use futures::StreamExt as _;
use serde_json::json;
use xxhash_rust::xxh3::xxh3_128;

use super::{MAX_MESSAGE_ID_LEN, MessageIdGenerator, header_block_bytes};
use crate::test::init_test_logger;
use crate::transport::nats::NatsOutputEndpoint;
use crate::transport::nats::input::test::util as input_util;
use feldera_adapterlib::transport::{OutputBatchType, OutputEndpoint, Step};
use feldera_types::transport::nats as cfg;
use feldera_types::transport::nats::NatsOutputConfig;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Configuration validation (no server required)
// ---------------------------------------------------------------------------

fn config_from_value(value: serde_json::Value) -> NatsOutputConfig {
    serde_json::from_value(value).unwrap()
}

#[test]
fn rejects_zero_publish_timeout() {
    let config = config_from_value(json!({
        "connection_config": {
            "server_url": "nats://localhost:4222",
        },
        "subject": "sub",
        "publish_timeout_secs": 0,
    }));
    let error = NatsOutputEndpoint::new(config, CancellationToken::new()).unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("publish_timeout_secs"),
        "error should name the invalid field, got: {text}"
    );
}

#[test]
fn rejects_empty_header_key() {
    let config = config_from_value(json!({
        "connection_config": {
            "server_url": "nats://localhost:4222",
        },
        "subject": "sub",
        "headers": [{"key": "", "value": "v"}],
    }));
    let error = NatsOutputEndpoint::new(config, CancellationToken::new()).unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("empty key"),
        "error should describe the invalid header, got: {text}"
    );
}

/// `push_buffer` before `connect` is a programming error, reported as such
/// rather than as a NATS failure.
#[test]
fn push_buffer_before_connect_fails() {
    let config = config_from_value(json!({
        "connection_config": {
            "server_url": "nats://localhost:4222",
        },
        "subject": "sub",
    }));
    let mut endpoint = NatsOutputEndpoint::new(config, CancellationToken::new()).unwrap();
    let error = endpoint.push_buffer(b"{}").unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("before the endpoint is connected"),
        "error should describe the lifecycle violation, got: {text}"
    );
}

/// `push_key` is not supported by the NATS connector: the NATS message model
/// has no key, so a key/value format must fail loudly instead of dropping
/// the key.
#[test]
fn push_key_is_rejected() {
    let config = config_from_value(json!({
        "connection_config": {
            "server_url": "nats://localhost:4222",
        },
        "subject": "sub",
    }));
    let mut endpoint = NatsOutputEndpoint::new(config, CancellationToken::new()).unwrap();
    let error = endpoint.push_key(Some(b"k"), Some(b"v"), &[]).unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("not supported"),
        "error should state the format is unsupported, got: {text}"
    );
}

// ---------------------------------------------------------------------------
// Message ids and header overhead (no server required)
// ---------------------------------------------------------------------------

/// Ids within one transaction: identical payloads get distinct ids (a record
/// with weight two must be stored twice), distinct payloads get distinct
/// ids, and the count of an identical payload does not depend on the order
/// in which the other payloads were published.
#[test]
fn message_ids_within_a_transaction() {
    let mut ids = MessageIdGenerator::default();
    ids.start_transaction(7);
    let a0 = ids.next_id(b"a").to_string();
    let b0 = ids.next_id(b"b").to_string();
    let a1 = ids.next_id(b"a").to_string();
    assert_ne!(a0, b0);
    assert_ne!(a0, a1);
    assert!(a0.starts_with("7-") && a0.ends_with("-0"), "{a0}");
    assert!(a1.ends_with("-1"), "{a1}");

    // Same multiset of payloads, different order: same set of ids.
    let mut reordered = MessageIdGenerator::default();
    reordered.start_transaction(7);
    let r_b0 = reordered.next_id(b"b").to_string();
    let r_a0 = reordered.next_id(b"a").to_string();
    let r_a1 = reordered.next_id(b"a").to_string();
    assert_eq!((r_a0, r_a1, r_b0), (a0, a1, b0));
}

/// Ids across transactions: the same payload in a later transaction is a
/// new message, and a replay of a transaction reproduces its ids exactly.
#[test]
fn message_ids_across_transactions() {
    let mut ids = MessageIdGenerator::default();
    ids.start_transaction(1);
    let first = ids.next_id(b"a").to_string();
    ids.start_transaction(2);
    let second = ids.next_id(b"a").to_string();
    assert_ne!(first, second);

    // Replay: the counts reset with the transaction number.
    let mut replay = MessageIdGenerator::default();
    replay.start_transaction(2);
    assert_eq!(replay.next_id(b"a"), second);

    // `start_transaction` with the same number keeps counting: a
    // transaction can span several batches.
    ids.start_transaction(2);
    assert!(ids.next_id(b"a").ends_with("-1"));
}

/// Every id fits the overhead bound reserved for it, including the longest
/// transaction number and count.
#[test]
fn message_id_fits_reserved_length() {
    let mut ids = MessageIdGenerator {
        transaction: u64::MAX,
        seen: HashMap::from([(xxh3_128(b"a"), u32::MAX)]),
        id: String::new(),
    };
    let id = ids.next_id(b"a");
    assert_eq!(id.len(), MAX_MESSAGE_ID_LEN, "{id}");
}

/// The header overhead is the exact wire encoding `async-nats` produces:
/// nothing without headers, else the version line, one line per header,
/// the longest `Nats-Msg-Id` line when ids are enabled, and the blank line
/// that ends the block.
#[test]
fn header_overhead_matches_wire_encoding() {
    let no_headers = HeaderMap::new();
    assert_eq!(header_block_bytes(&no_headers, false), 0);
    assert_eq!(
        header_block_bytes(&no_headers, true),
        "NATS/1.0\r\n".len() + "Nats-Msg-Id: ".len() + MAX_MESSAGE_ID_LEN + "\r\n\r\n".len()
    );

    let mut headers = HeaderMap::new();
    headers.insert("X-Source", "feldera");
    assert_eq!(
        header_block_bytes(&headers, false),
        "NATS/1.0\r\nX-Source: feldera\r\n\r\n".len()
    );
}

/// `message_id: none` publishes without an id and reserves no overhead for
/// one.
#[test]
fn message_id_none_disables_ids() {
    let config = config_from_value(json!({
        "connection_config": {
            "server_url": "nats://localhost:4222",
        },
        "subject": "sub",
        "message_id": "none",
    }));
    let endpoint = NatsOutputEndpoint::new(config, CancellationToken::new()).unwrap();
    assert!(endpoint.message_ids.is_none());
}

// ---------------------------------------------------------------------------
// End-to-end (requires nats-server; see crates/adapters/README.md)
// ---------------------------------------------------------------------------

/// Publishes each record in `records` through a `nats_output` endpoint built
/// from `config` against a private `nats-server` with a stream bound to the
/// configured subject, and returns the JetStream context for verification.
///
/// Each record is pushed with its own `push_buffer` call inside a batch for
/// the given step, as the `json` format's `insert_delete` update format
/// produces with `buffer_size_records: 1`. The server URL in `config` is
/// replaced with the private server's address.
fn publish_records(
    rt: &tokio::runtime::Runtime,
    config: &NatsOutputConfig,
    records: &[(Step, &str)],
) -> anyhow::Result<(input_util::ProcessKillGuard, async_nats::jetstream::Context)> {
    let (guard, addr) = input_util::start_nats_and_get_address()?;
    rt.block_on(input_util::create_stream(&addr, "str", &config.subject))?;

    let config = NatsOutputConfig {
        connection_config: cfg::ConnectOptions {
            server_url: addr.clone(),
            ..config.connection_config.clone()
        },
        ..config.clone()
    };

    let mut endpoint = NatsOutputEndpoint::new(config, CancellationToken::new())?;
    endpoint.connect(Box::new(|_fatal, _error, _tag| {}))?;

    for (step, record) in records {
        endpoint.batch_start(*step, OutputBatchType::Delta)?;
        endpoint.push_buffer(record.as_bytes())?;
        endpoint.batch_end()?;
    }

    let client = rt.block_on(async { async_nats::connect(&addr).await })?;
    let js = rt.block_on(async { async_nats::jetstream::new(client) });
    Ok((guard, js))
}

/// The full publish path: records pushed through the endpoint are stored by
/// the stream bound to the subject, in order.
#[test]
fn test_nats_output_publish() -> anyhow::Result<()> {
    init_test_logger();

    let rt = tokio::runtime::Runtime::new()?;

    let config = config_from_value(json!({
        "connection_config": {
            "server_url": "nats://replaced-by-the-test",
        },
        "subject": "sub",
    }));

    let records = [
        r#"{"insert":{"id":1,"b":true,"s":"first"}}"#,
        r#"{"insert":{"id":2,"b":false,"s":"second"}}"#,
    ];

    let (_server, js) = publish_records(&rt, &config, &[(0, records[0]), (1, records[1])])?;

    rt.block_on(async {
        let stream = js.get_stream("str").await?;
        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                filter_subject: "sub".into(),
                ..Default::default()
            })
            .await?;
        let mut messages = Vec::with_capacity(records.len());
        let mut stream_messages = consumer
            .batch()
            .max_messages(records.len())
            .messages()
            .await?;
        while messages.len() < records.len() {
            let message = tokio::time::timeout(Duration::from_secs(5), stream_messages.next())
                .await
                .map_err(|_| anyhow::anyhow!("timed out waiting for a published message"))?
                .ok_or_else(|| anyhow::anyhow!("stream ended before all messages arrived"))?
                .map_err(|e| anyhow::anyhow!("NATS receive failed: {e}"))?;
            messages.push(message.payload.to_vec());
        }
        assert_eq!(
            messages,
            records
                .iter()
                .map(|r| r.as_bytes().to_vec())
                .collect::<Vec<_>>()
        );
        Ok(())
    })
}

/// A subject bound to no stream is rejected at `connect`, not on the first
/// record: the publish would fail with "no responders" only after the
/// pipeline is already running.
#[test]
fn test_nats_output_unbound_subject_fails_at_connect() -> anyhow::Result<()> {
    init_test_logger();

    let _rt = tokio::runtime::Runtime::new()?;
    let (guard, addr) = input_util::start_nats_and_get_address()?;
    let _guard = guard;
    // No stream created: "sub" is bound to nothing.

    let config = config_from_value(json!({
        "connection_config": {
            "server_url": addr,
        },
        "subject": "sub",
    }));

    let mut endpoint = NatsOutputEndpoint::new(config, CancellationToken::new())?;
    let error = endpoint
        .connect(Box::new(|_fatal, _error, _tag| {}))
        .unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("no JetStream stream is bound to subject"),
        "error should name the cause, got: {text}"
    );
    Ok(())
}

/// `message_id: content_hash` against the server: a replay of a transaction
/// is dropped by the server within the duplicate window, while a record
/// published twice in one transaction (weight two) and the same record in
/// a later transaction are stored.
#[test]
fn test_nats_output_message_id_dedup() -> anyhow::Result<()> {
    init_test_logger();

    let rt = tokio::runtime::Runtime::new()?;

    let config = config_from_value(json!({
        "connection_config": {
            "server_url": "nats://replaced-by-the-test",
        },
        "subject": "sub",
    }));

    let record = r#"{"insert":{"id":1,"b":true,"s":"first"}}"#;

    // Transaction 1 publishes the record twice; a restart replays
    // transaction 1 (the endpoint is the same one, as the counts reset with
    // the transaction number); transaction 2 publishes the record again.
    // The helper's stream uses the server's default duplicate window
    // (2 minutes), which the immediate republish is well within.
    let (_server, js) = publish_records(
        &rt,
        &config,
        &[
            (1, record),
            (1, record),
            (2, record),
            (1, record),
            (1, record),
        ],
    )?;

    rt.block_on(async {
        let stream = js.get_stream("str").await?;
        let info = stream.cached_info();
        assert_eq!(
            info.state.messages, 3,
            "two copies in transaction 1 and one in transaction 2 are stored; the replay of transaction 1 is dropped"
        );
        Ok(())
    })
}

/// `max_message_size`: the connector discovers the bound stream's
/// `max_message_size` at connect time and rejects a payload over it with an
/// error naming the limit, instead of writing the message to the wire where
/// the server's refusal surfaces as an opaque ack failure the connector
/// would retry forever.
#[test]
fn test_nats_output_oversize_payload_rejected() -> anyhow::Result<()> {
    init_test_logger();

    let rt = tokio::runtime::Runtime::new()?;

    let config = config_from_value(json!({
        "connection_config": {
            "server_url": "nats://replaced-by-the-test",
        },
        "subject": "sub",
    }));

    let record = r#"{"insert":{"id":1,"b":true,"s":"first"}}"#;
    let small = record.as_bytes().to_vec();
    // Larger than the stream's max_message_size of 256 bytes below.
    let large = format!(
        r#"{{"insert":{{"id":1,"b":true,"s":"{}"}}}}"#,
        "x".repeat(300)
    );

    // A stream with a small max_message_size, below the large payload the
    // test pushes: the publish_records helper creates a stream without a
    // limit, so this test creates its own.
    let (guard, addr) = input_util::start_nats_and_get_address()?;
    let _guard = guard;
    rt.block_on(async {
        let client = async_nats::connect(&addr).await?;
        let js = async_nats::jetstream::new(client);
        js.create_stream(async_nats::jetstream::stream::Config {
            name: "str".to_string(),
            subjects: vec!["sub".to_string()],
            storage: async_nats::jetstream::stream::StorageType::Memory,
            max_message_size: 256,
            ..Default::default()
        })
        .await?;
        Ok::<_, anyhow::Error>(())
    })?;

    let config = NatsOutputConfig {
        connection_config: cfg::ConnectOptions {
            server_url: addr.clone(),
            ..config.connection_config.clone()
        },
        ..config.clone()
    };

    use feldera_adapterlib::transport::OutputEndpoint;
    let mut endpoint = NatsOutputEndpoint::new(config, CancellationToken::new())?;
    endpoint.connect(Box::new(|_fatal, _error, _tag| {}))?;

    // The stream's limit minus the `Nats-Msg-Id` header's overhead bounds
    // the encoder; a payload over it fails with the limit named.
    let max_buffer = endpoint.max_buffer_size_bytes();
    assert!(
        max_buffer < 256,
        "max_buffer_size_bytes should be below the stream's 256-byte limit, got {max_buffer}"
    );
    endpoint.batch_start(0, OutputBatchType::Delta)?;
    endpoint.push_buffer(&small)?;
    endpoint.batch_end()?;

    endpoint.batch_start(1, OutputBatchType::Delta)?;
    let error = endpoint.push_buffer(large.as_bytes()).unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("exceeds the message size limit"),
        "error should name the limit, got: {text}"
    );
    assert!(
        text.contains("max_message_size"),
        "error should suggest the max_message_size knobs, got: {text}"
    );
    Ok(())
}

/// The `max_message_size` config override beats the discovered stream limit,
/// for streams whose configured limit the server does not report usefully.
#[test]
fn test_nats_output_max_message_size_override() -> anyhow::Result<()> {
    init_test_logger();

    let rt = tokio::runtime::Runtime::new()?;
    let (guard, addr) = input_util::start_nats_and_get_address()?;
    let _guard = guard;
    rt.block_on(input_util::create_stream(&addr, "str", "sub"))?;

    // The helper's stream sets no limit of its own, so discovery resolves
    // the server's 1 MiB default; the override pins a smaller bound.
    let config = config_from_value(json!({
        "connection_config": {
            "server_url": addr,
        },
        "subject": "sub",
        "max_message_size": 256,
    }));

    use feldera_adapterlib::transport::OutputEndpoint;
    let mut endpoint = NatsOutputEndpoint::new(config, CancellationToken::new())?;
    endpoint.connect(Box::new(|_fatal, _error, _tag| {}))?;
    assert!(
        endpoint.max_buffer_size_bytes() <= 256,
        "the override should bound the encoder, got {}",
        endpoint.max_buffer_size_bytes()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Controller-level (requires nats-server; see crates/adapters/README.md)
//
// Exercises the full production path the endpoint tests bypass: input
// connector -> circuit -> json encoder -> OutputProbe -> nats_output.
// ---------------------------------------------------------------------------

/// The full pipeline with the default json output format and message ids:
/// records flow through the encoder and the endpoint to the stream bound to
/// the subject, and every message carries a `Nats-Msg-Id` header the
/// connector derived from the payload without knowing its format.
#[test]
fn test_nats_output_pipeline_end_to_end() -> anyhow::Result<()> {
    init_test_logger();

    use crate::{
        Controller,
        test::{TestStruct, test_circuit, wait},
    };
    use std::io::Write;
    use tempfile::NamedTempFile;

    let rt = tokio::runtime::Runtime::new()?;
    let (guard, addr) = input_util::start_nats_and_get_address()?;
    let _guard = guard;
    rt.block_on(input_util::create_stream(&addr, "str", "sub"))?;

    // Two inserts followed by a delete of the first record.
    let input_file = NamedTempFile::new()?;
    input_file.as_file().write_all(
        br#"{"insert":{"id":1,"b":true,"s":"first"}}
{"insert":{"id":2,"b":false,"s":"second"}}
{"delete":{"id":1,"b":true,"s":"first"}}
"#,
    )?;

    let config = serde_json::from_value(json!({
      "name": "test",
      "workers": 4,
      "inputs": {
        "file1": {
          "paused": true,
          "stream": "test_input1",
          "transport": {
            "name": "file_input",
            "config": {
              "path": input_file.path(),
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "insert_delete"
            }
          }
        }
      },
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": {
            "name": "nats_output",
            "config": {
              "connection_config": {
                "server_url": addr,
              },
              "subject": "sub"
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "insert_delete"
            }
          }
        }
      }
    }))?;

    let schema = TestStruct::schema().to_vec();

    let (err_sender, err_receiver) = crossbeam::channel::unbounded();
    let controller = Controller::with_test_config(
        move |workers| Ok(test_circuit::<TestStruct>(workers, &schema, &[None])),
        &config,
        Box::new(move |e, _| {
            err_sender
                .send(format!("nats_output_test: error: {e}"))
                .unwrap()
        }),
    )?;

    controller.start();
    controller.start_input_endpoint("file1")?;

    // Three records in, three records out.
    wait(
        || controller.status().num_total_processed_records() == 3 || !err_receiver.is_empty(),
        10_000,
    )
    .expect("timeout");

    assert!(
        err_receiver.is_empty(),
        "pipeline reported errors: {:?}",
        err_receiver.try_iter().collect::<Vec<_>>()
    );

    let messages = rt.block_on(async {
        let client = async_nats::connect(&addr).await?;
        let js = async_nats::jetstream::new(client);
        let stream = js.get_stream("str").await?;
        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                filter_subject: "sub".into(),
                ..Default::default()
            })
            .await?;
        let mut messages = Vec::new();
        let mut stream_messages = consumer.batch().max_messages(10).messages().await?;
        loop {
            // Read until a 5-second idle gap: the number of messages the
            // connector published is what this test asserts on, so the
            // reader cannot assume it up front. The gap is generous because
            // output delivery runs on the endpoint thread, trailing the
            // circuit's processed-records count the test waits on.
            match tokio::time::timeout(Duration::from_secs(5), stream_messages.next()).await {
                Ok(Some(Ok(message))) => messages.push(message),
                Ok(Some(Err(e))) => return Err(anyhow::anyhow!("NATS receive failed: {e}")),
                // Batch ended: the server had no more messages to send.
                Ok(None) => break,
                // No message for 1s: the connector has published everything
                // it is going to; the pipeline is idle.
                Err(_) => break,
            }
        }
        Ok::<_, anyhow::Error>(messages)
    })?;

    // The test circuit materializes the input zset: the batch the encoder
    // sees is the net change of the view, so `+1, +2, -1` collapses to the
    // single record `+2`, which the encoder emits in one buffer.
    assert_eq!(
        messages.len(),
        1,
        "the net change of one record must arrive as one message, got: {:?}",
        messages
            .iter()
            .map(|m| String::from_utf8_lossy(&m.payload).to_string())
            .collect::<Vec<_>>()
    );
    let record: serde_json::Value = serde_json::from_slice(&messages[0].payload)?;
    assert_eq!(record["insert"]["s"], "second");
    let message_id = messages[0]
        .headers
        .as_ref()
        .and_then(|headers| headers.get(async_nats::header::NATS_MESSAGE_ID))
        .expect("message must carry a Nats-Msg-Id header")
        .as_str();
    assert!(
        message_id.len() <= MAX_MESSAGE_ID_LEN && message_id.matches('-').count() == 2,
        "unexpected message id shape: {message_id}"
    );

    controller.stop()?;
    Ok(())
}
