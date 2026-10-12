//! NATS JetStream output adapter.
//!
//! This adapter publishes each buffer the encoder hands it as a message to a
//! NATS subject bound to a JetStream stream. Each publish requests an
//! acknowledgment and is retried until the server confirms it stored the
//! message, so a momentarily unavailable server stalls the pipeline instead
//! of silently dropping output. A publish the server rejects outright (no
//! stream bound to the subject, a violated stream constraint) fails the
//! buffer immediately, and the controller records the error and drops it.
//!
//! # Idempotence
//!
//! With the default `message_id: content_hash`, every message carries a
//! `Nats-Msg-Id` header derived from the message itself, see
//! [`MessageIdGenerator`]. Within the stream's duplicate window the server
//! discards a publish whose id it has already stored, so output the pipeline
//! replays after a restart is not duplicated downstream. The id is computed
//! from the encoded bytes, so the adapter needs no knowledge of the data
//! format.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Error as AnyError, Result as AnyResult, anyhow, bail};
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::{self, message::PublishMessage};
use async_nats::{HeaderMap, HeaderName, HeaderValue};
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::transport::{OutputBatchType, Step};
use feldera_types::transport::nats as cfg;
use tokio_util::sync::CancellationToken;
use tracing::span::EnteredSpan;
use tracing::{debug, info_span, warn};
use xxhash_rust::xxh3::xxh3_128;

use crate::transport::nats::input::config_utils::translate_connect_options;
use crate::{AsyncErrorCallback, OutputEndpoint};

#[cfg(test)]
mod test;

/// Longest `Nats-Msg-Id` value [`MessageIdGenerator`] produces:
/// `<u64 transaction>-<128-bit hash as 32 hex digits>-<u32 count>`.
const MAX_MESSAGE_ID_LEN: usize = 20 + 1 + 32 + 1 + 10;

/// Produces the `Nats-Msg-Id` of each message under `message_id:
/// content_hash`.
///
/// The id is `<transaction>-<hash>-<n>`:
///
/// * `transaction` is the number the controller passes to `batch_start`. The
///   controller reproduces it when it replays output after a restart (see
///   `BatchQueueEntry::transaction`), and it distinguishes an insert of a
///   record in one transaction from an identical insert in a later one, which
///   a bare payload hash would merge.
///
/// * `hash` is the 128-bit xxh3 of the payload bytes.
///
/// * `n` is how many earlier messages in the same transaction had the same
///   hash. A record with weight two is published twice with identical bytes;
///   without `n` the server would drop the second copy. The count depends
///   only on the multiset of payloads in the transaction, not on their order,
///   so a replay that splits the transaction's output across batches
///   differently still produces the same ids.
///
/// The per-transaction counts are dropped when the transaction number
/// changes, so memory is bounded by the number of distinct payloads in one
/// transaction.
#[derive(Debug, Default)]
struct MessageIdGenerator {
    transaction: Step,
    /// Count of messages published in `transaction`, by payload hash.
    seen: HashMap<u128, u32>,
    /// Reused buffer for the formatted id.
    id: String,
}

impl MessageIdGenerator {
    fn start_transaction(&mut self, transaction: Step) {
        if transaction != self.transaction {
            self.transaction = transaction;
            self.seen.clear();
        }
    }

    /// Returns the id for the next message with `payload`.
    fn next_id(&mut self, payload: &[u8]) -> &str {
        let hash = xxh3_128(payload);
        let n = self.seen.entry(hash).or_insert(0);
        self.id.clear();
        write!(self.id, "{}-{hash:032x}-{n}", self.transaction).unwrap();
        *n = n.wrapping_add(1);
        &self.id
    }
}

/// A connected JetStream context plus the static per-message configuration.
#[derive(Debug)]
struct NatsOutputConnection {
    jetstream: jetstream::Context,
    headers: HeaderMap,
    /// Exact upper bound on the bytes of wire overhead per message: the
    /// header block's framing, the static headers, and the longest
    /// `Nats-Msg-Id` line. Subtracting it from the message size limit yields
    /// the payload bound offered to the encoder.
    overhead_bytes: usize,
    /// Message size limit in bytes, headers and payload combined, from the
    /// config override or discovered from the stream/server.
    max_message_size: usize,
}

/// Whether a failed publish attempt is worth repeating.
enum PublishFailure {
    /// The server did not answer in time, or the answer was lost: the message
    /// may or may not be stored. Retrying is safe because the message id, if
    /// any, makes the server drop a copy it already has.
    Transient(AnyError),
    /// The server rejected the publish and will reject it again.
    Permanent(AnyError),
}

/// Classifies an ack error from `async-nats`.
///
/// `Other` carries server errors the client does not map to a kind (such as
/// an exceeded message size, which the pre-publish check keeps off this
/// path): retried, because a transient server-side condition also lands
/// here, and the retry loop logs the error text so a permanent one is
/// visible in the logs.
fn classify(error: jetstream::context::PublishError) -> PublishFailure {
    match error.kind() {
        PublishErrorKind::StreamNotFound
        | PublishErrorKind::WrongLastMessageId
        | PublishErrorKind::WrongLastSequence => PublishFailure::Permanent(error.into()),
        PublishErrorKind::TimedOut
        | PublishErrorKind::BrokenPipe
        | PublishErrorKind::MaxAckPending
        | PublishErrorKind::Other => PublishFailure::Transient(error.into()),
    }
}

impl NatsOutputConnection {
    /// Largest payload the encoder may place in one message.
    fn max_payload_bytes(&self) -> usize {
        self.max_message_size
            .saturating_sub(self.overhead_bytes)
            .max(1)
    }

    /// Publishes `payload` and waits for the server's acknowledgment,
    /// retrying transient failures until acknowledged or `shutdown` fires.
    fn publish(
        &self,
        subject: &str,
        payload: &[u8],
        message_id: Option<&str>,
        publish_timeout: Duration,
        shutdown: &CancellationToken,
    ) -> AnyResult<()> {
        // A payload over the limit is not rejected before it reaches the wire
        // on the JetStream publish path: the server's refusal comes back as an
        // ack failure of kind `Other`. Check up front and fail with the limit
        // named instead.
        if payload.len() > self.max_payload_bytes() {
            bail!(
                "NATS output: payload of {} bytes exceeds the message size limit of {} bytes (subject '{subject}'). Raise the stream's max_message_size, configure the connector's max_message_size, or split the record.",
                payload.len(),
                self.max_message_size
            );
        }
        let subject = subject.to_string();
        let payload: Vec<u8> = payload.to_vec();
        TOKIO.block_on(async move {
            let mut attempt = 0u64;
            let mut delay = Duration::from_millis(10);
            loop {
                if shutdown.is_cancelled() {
                    bail!("gave up publishing to NATS subject '{subject}' because the pipeline is shutting down");
                }
                // Rebuilt per attempt: a failed publish consumes the builder.
                let mut message = PublishMessage::build().payload(payload.clone().into());
                if !self.headers.is_empty() {
                    message = message.headers(self.headers.clone());
                }
                if let Some(message_id) = message_id {
                    message = message.message_id(message_id);
                }
                let outcome = async {
                    let ack = self
                        .jetstream
                        .send_publish(subject.clone(), message)
                        .await
                        .map_err(classify)?;
                    ack.await.map_err(classify)
                };
                let failure = match tokio::time::timeout(publish_timeout, outcome).await {
                    Ok(Ok(ack)) => {
                        if ack.duplicate {
                            debug!("NATS publish to '{subject}' was deduplicated by the server");
                        } else {
                            debug!("NATS publish to '{subject}' acknowledged");
                        }
                        return Ok(());
                    }
                    Ok(Err(PublishFailure::Permanent(error))) => {
                        return Err(error.context(format!(
                            "NATS server rejected the publish to subject '{subject}'"
                        )));
                    }
                    Ok(Err(PublishFailure::Transient(error))) => error,
                    Err(_) => anyhow!("no acknowledgment within {publish_timeout:?}"),
                };
                attempt += 1;
                if attempt % 100 == 1 {
                    warn!(
                        "Attempts to publish to NATS subject '{subject}' are failing (attempt {attempt}: {failure:#}); will keep retrying"
                    );
                }
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, Duration::from_secs(1));
            }
        })
    }
}

/// Handles output to a NATS subject.
#[derive(Debug)]
pub struct NatsOutputEndpoint {
    config: cfg::NatsOutputConfig,
    connection: Option<NatsOutputConnection>,
    /// `Some` under `message_id: content_hash`.
    message_ids: Option<MessageIdGenerator>,
    /// `ControllerInner::shutdown_token`, which ends a wait on an
    /// unacknowledged publish.
    shutdown: CancellationToken,
}

impl NatsOutputEndpoint {
    pub fn new(config: cfg::NatsOutputConfig, shutdown: CancellationToken) -> AnyResult<Self> {
        if config.publish_timeout_secs == 0 {
            bail!(
                "Invalid NATS output configuration: publish_timeout_secs must be at least 1 second"
            );
        }
        for header in &config.headers {
            if header.key.is_empty() {
                bail!("Invalid NATS output configuration: a header has an empty key");
            }
        }
        let message_ids = match config.message_id {
            cfg::NatsMessageId::ContentHash => Some(MessageIdGenerator::default()),
            cfg::NatsMessageId::None => None,
        };
        Ok(Self {
            config,
            connection: None,
            message_ids,
            shutdown,
        })
    }

    fn span(&self) -> EnteredSpan {
        info_span!(
            "nats_output",
            ft = false,
            subject = %self.config.subject,
            server_url = %self.config.connection_config.server_url,
        )
        .entered()
    }
}

/// Bytes `async-nats` writes for the header block of a message carrying
/// `headers` and, when `with_message_id`, the longest possible `Nats-Msg-Id`
/// line. Zero when the message carries no headers at all, because the block
/// is then omitted.
///
/// The encoding is `NATS/1.0\r\n`, then `key: value\r\n` per header, then a
/// blank `\r\n` line.
fn header_block_bytes(headers: &HeaderMap, with_message_id: bool) -> usize {
    if headers.is_empty() && !with_message_id {
        return 0;
    }
    let static_headers: usize = headers
        .iter()
        .map(|(key, values)| {
            values
                .iter()
                .map(|value| key.to_string().len() + b": ".len() + value.as_str().len() + 2)
                .sum::<usize>()
        })
        .sum();
    let message_id_line = if with_message_id {
        b"Nats-Msg-Id: ".len() + MAX_MESSAGE_ID_LEN + 2
    } else {
        0
    };
    b"NATS/1.0\r\n".len() + static_headers + message_id_line + b"\r\n".len()
}

impl OutputEndpoint for NatsOutputEndpoint {
    fn connect(&mut self, _async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        let _guard = self.span();
        let connection_config = self.config.connection_config.clone();
        let subject = self.config.subject.clone();
        let connect_timeout = Duration::from_secs(
            self.config.connection_config.connection_timeout_secs
                + self.config.connection_config.request_timeout_secs,
        );
        let max_message_size_override = self.config.max_message_size;
        let with_message_id = self.message_ids.is_some();

        // Fail fast on an unusable configuration: fetching the stream the
        // subject is bound to bounds the time initialization can take, rejects
        // a subject with no stream at startup instead of failing with
        // "no responders" on the first record, and returns the stream's
        // `max_message_size`, which sizes the messages the encoder builds.
        let connection = TOKIO.block_on(async {
            let connect_options = translate_connect_options(&connection_config).await?;
            let client = connect_options
                .connect(&connection_config.server_url)
                .await
                .map_err(|e| {
                    anyhow!(
                        "error connecting to the NATS server at {}: {e}",
                        connection_config.server_url
                    )
                })?;

            let mut headers = HeaderMap::new();
            for header in &self.config.headers {
                headers.insert(
                    HeaderName::from_str(&header.key)?,
                    HeaderValue::from_str(&header.value)?,
                );
            }
            let overhead_bytes = header_block_bytes(&headers, with_message_id);

            // Captured before the client is moved into the JetStream
            // context: the server's max payload is the fallback limit when
            // the stream does not set its own.
            let server_max_payload = client.server_info().max_payload;
            let jetstream = jetstream::new(client);
            let verify = async {
                // Resolve the stream the subject is bound to (an error when
                // no stream covers it) and read its message size limit.
                let stream_name = jetstream
                    .stream_by_subject(subject.clone())
                    .await
                    .map_err(|e| {
                        anyhow!("no JetStream stream is bound to subject '{subject}': {e}")
                    })?;
                let stream = jetstream.get_stream(stream_name).await?;
                Ok::<i32, AnyError>(stream.cached_info().config.max_message_size)
            };
            let stream_max_message_size = tokio::time::timeout(connect_timeout, verify)
                .await
                .map_err(|_| {
                    anyhow!("NATS output initialization timed out after {connect_timeout:?}")
                })??;

            // The config override wins; else the stream's setting when the
            // stream has one; else the server's max payload.
            let max_message_size = match max_message_size_override {
                Some(max) => max,
                None if stream_max_message_size > 0 => stream_max_message_size as usize,
                None if server_max_payload > 0 => server_max_payload,
                None => cfg::DEFAULT_MAX_MESSAGE_SIZE_BYTES,
            };
            if overhead_bytes >= max_message_size {
                bail!(
                    "NATS output: the message headers consume {overhead_bytes} bytes, leaving no room for payload in the message size limit of {max_message_size} bytes"
                );
            }

            Ok::<_, AnyError>(NatsOutputConnection {
                jetstream,
                headers,
                overhead_bytes,
                max_message_size,
            })
        })?;

        debug!(
            "Connected NATS output endpoint to subject '{subject}' (max message size: {} bytes, overhead: {} bytes)",
            connection.max_message_size, connection.overhead_bytes
        );

        self.connection = Some(connection);
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        match self.connection.as_ref() {
            Some(connection) => connection.max_payload_bytes(),
            // The controller connects the endpoint before building the
            // encoder, so this is only reached by a misuse of the trait.
            // Conservative pre-connect bound: the nats-server default.
            None => cfg::DEFAULT_MAX_MESSAGE_SIZE_BYTES,
        }
    }

    fn batch_start(&mut self, step: Step, _batch_type: OutputBatchType) -> AnyResult<()> {
        if let Some(message_ids) = self.message_ids.as_mut() {
            message_ids.start_transaction(step);
        }
        Ok(())
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        let _guard = self.span();
        let Some(connection) = self.connection.as_ref() else {
            bail!("NATS output: pushing data before the endpoint is connected: unreachable");
        };
        let message_id = self
            .message_ids
            .as_mut()
            .map(|message_ids| message_ids.next_id(buffer));
        connection.publish(
            &self.config.subject,
            buffer,
            message_id,
            Duration::from_secs(self.config.publish_timeout_secs),
            &self.shutdown,
        )
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        bail!("NATS output: key/value formats are not supported by the NATS connector")
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}
