use crate::{AsyncErrorCallback, OutputEndpoint, TransportConfig};
use actix_web::{http::header::ContentType, web::Bytes, HttpResponse};
use anyhow::{anyhow, bail, Result as AnyResult};
use async_stream::stream;
use crossbeam::sync::ShardedLock;
use serde::{ser::SerializeStruct, Serializer};
use serde_json::value::RawValue;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::{debug, error, info_span};

// TODO: make this configurable via endpoint config.
const MAX_BUFFERS: usize = 100;

enum Format {
    Binary,
    Text,
    #[allow(dead_code)]
    Json,
}

/// HTTP output transport.
///
/// HTTP endpoints are instantiated via the REST API, so this type doesn't
/// implement `trait OutputTransport`.  It is only used to
/// collect static functions related to HTTP.
pub(crate) struct HttpOutputTransport;

impl HttpOutputTransport {
    pub(crate) fn config() -> TransportConfig {
        TransportConfig::HttpOutput
    }

    pub(crate) fn default_format() -> String {
        String::from("csv")
    }

    pub(crate) fn default_max_buffered_records() -> u64 {
        100_000
    }
}

#[derive(Clone)]
struct Buffer {
    pub sequence_number: u64,
    pub data: Bytes,
}

impl Buffer {
    fn new(sequence_number: u64, data: Bytes) -> Self {
        Self {
            sequence_number,
            data,
        }
    }
}

type SendRequest = (Buffer, Option<oneshot::Sender<()>>);

struct HttpOutputEndpointInner {
    name: String,
    format: Format,

    // Apply backpressure on the pipeline when the client or the network
    // are not keeping up.
    //
    // When `false`, the connector buffers up to `MAX_BUFFERS` chunks and
    // drops chunks on the floor when the buffer is full.  When `true`, the
    // connector sends one chunk at a time, eventually forcing the pipeline
    // to wait for the slow client to receive the data.
    backpressure: bool,
    total_buffers: AtomicU64,
    sender: ShardedLock<Option<mpsc::Sender<SendRequest>>>,
    // async_error_callback: RwLock<Option<AsyncErrorCallback>>,
}

impl HttpOutputEndpointInner {
    pub(crate) fn new(name: &str, format: Format, backpressure: bool) -> Self {
        Self {
            name: name.to_string(),
            format,
            backpressure,
            total_buffers: AtomicU64::new(0),
            sender: ShardedLock::new(None),
            // async_error_callback: RwLock::new(None),
        }
    }

    fn push_buffer(&self, buffer: Option<&[u8]>, blocking: bool) -> AnyResult<()> {
        let seq_number = self.total_buffers.fetch_add(1, Ordering::AcqRel);

        let json_buf = Vec::with_capacity(buffer.map(|b| b.len()).unwrap_or(0) + 1024);
        let mut serializer = serde_json::Serializer::new(json_buf);
        let mut struct_serializer = serializer
            .serialize_struct("Chunk", if buffer.is_some() { 2 } else { 1 })
            .map_err(|e| anyhow!("error serializing 'Chunk' struct: '{e}'"))?;
        struct_serializer
            .serialize_field("sequence_number", &seq_number)
            .map_err(|e| anyhow!("error serializing 'sequence_number' field: '{e}'"))?;

        if let Some(buffer) = buffer {
            match self.format {
                Format::Binary => unimplemented!(),
                Format::Text => {
                    let data_str = std::str::from_utf8(buffer).map_err(|e| {
                        anyhow!("received an invalid UTF8 string from encoder: {e}")
                    })?;
                    struct_serializer
                        .serialize_field("text_data", data_str)
                        .map_err(|e| anyhow!("error serializing 'text_data' field: {e}"))?;
                }
                Format::Json => {
                    let data_str = std::str::from_utf8(buffer).map_err(|e| {
                        anyhow!("received an invalid UTF8 string from encoder: {e}")
                    })?;
                    let json_value =
                        serde_json::from_str::<Box<RawValue>>(data_str).map_err(|e| {
                            error!("Invalid JSON: {data_str}");
                            anyhow!("received an invalid JSON string from encoder: {e}")
                        })?;
                    struct_serializer
                        .serialize_field("json_data", &json_value)
                        .map_err(|e| anyhow!("error serializing 'json_data' field: {e}"))?;
                }
            }
        }
        struct_serializer
            .end()
            .map_err(|e| anyhow!("error serializing chunk: '{e}'"))?;

        let mut json_buf = serializer.into_inner();
        json_buf.push(b'\r');
        json_buf.push(b'\n');

        // In blocking mode, create a one-shot acknowledgement channel for the sender thread
        // to notify us when it's done sending the chunk.
        let (ack_sender, ack_receiver) = if blocking {
            let (sender, receiver) = oneshot::channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        // A failure simply means that there are no receivers.
        if let Some(Ok(_)) = self.sender.read().unwrap().as_ref().map(|sender| {
            sender.try_send((Buffer::new(seq_number, Bytes::from(json_buf)), ack_sender))
        }) {
            if let Some(ack_receiver) = ack_receiver {
                let _ = ack_receiver.blocking_recv();
            }
        }

        Ok(())
    }
}

struct RequestGuard {
    finalizer: Box<dyn FnMut()>,
}

impl RequestGuard {
    fn new(finalizer: Box<dyn FnMut()>) -> Self {
        Self { finalizer }
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        (self.finalizer)();
    }
}

/// Output endpoint that sends a data stream to the client as a response to an
/// HTTP request.
///
/// This implementation provides no support for reliable delivery
/// and is mostly intended for browser-based testing.
#[derive(Clone)]
pub(crate) struct HttpOutputEndpoint {
    inner: Arc<HttpOutputEndpointInner>,
}

impl HttpOutputEndpoint {
    pub(crate) fn new(name: &str, format: &str, backpressure: bool) -> Self {
        let format = match format {
            "csv" => Format::Text,
            "json" => Format::Json,
            _ => Format::Binary,
        };
        Self {
            inner: Arc::new(HttpOutputEndpointInner::new(name, format, backpressure)),
        }
    }

    fn name(&self) -> &str {
        self.inner.name.as_str()
    }

    fn connect(&self) -> mpsc::Receiver<SendRequest> {
        let (sender, receiver) = mpsc::channel(MAX_BUFFERS);
        *self.inner.sender.write().unwrap() = Some(sender);

        receiver
    }

    /// Create an HTTP response object with a streaming body that
    /// will continue sending output updates until the circuit
    /// terminates or the client disconnects.
    ///
    /// This method returns instantly.  The resulting `HttpResponse`
    /// object can be returned to the actix framework, which will
    /// run its streaming body and invoke `finalizer` upon completion.
    pub(crate) fn request(&self, finalizer: Box<dyn FnMut()>) -> HttpResponse {
        let mut receiver = self.connect();
        let name = self.name().to_string();
        let guard = RequestGuard::new(finalizer);

        let inner = self.inner.clone();

        HttpResponse::Ok()
            .insert_header(ContentType::json())
            .streaming(stream! {
                let _guard = guard;
                loop {
                    // There is a bug in actix (https://github.com/actix/actix-web/issues/1313)
                    // that prevents it from dropping HTTP connections on client disconnect
                    // unless the endpoint periodically sends some data.  As a workaround,
                    // if there is no real payload to send for more than 3 seconds, we will
                    // generate an empty chunk.  Note that it takes 6s, i.e., 2x the timeout
                    // period for actix to actually drop the connection.
                    match timeout(Duration::from_millis(3_000), receiver.recv()).await {
                        Err(_) => {
                            // Send the empty chunk via the `push_buffer` method to
                            // make sure it gets assigned correct sequence number.
                            let _ = inner.push_buffer(None, false);
                        }
                        Ok(None) => break,
                        Ok(Some((buffer, ack_sender))) => {
                            debug!(
                                "HTTP output endpoint '{}': sending chunk #{} ({} bytes)",
                                name,
                                buffer.sequence_number,
                                buffer.data.len(),
                            );
                            yield <AnyResult<_>>::Ok(buffer.data);
                            if let Some(ack_sender) = ack_sender {
                                let _ = ack_sender.send(());
                            }
                        },
                    }
                }
            })
    }
}

impl OutputEndpoint for HttpOutputEndpoint {
    fn connect(&mut self, _async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        // *self.inner.async_error_callback.write().unwrap() =
        // Some(async_error_callback);
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        let _guard = info_span!("http_output").entered();
        self.inner
            .push_buffer(Some(buffer), self.inner.backpressure)
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        bail!(
            "HTTP output transport does not support key-value pairs. \
This output endpoint was configured with a data format that produces outputs as key-value pairs; \
however the HTTP transport does not support this representation."
        );
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}
