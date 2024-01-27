use crate::{AsyncErrorCallback, OutputEndpoint, TransportConfig};
use actix_web::{http::header::ContentType, web::Bytes, HttpResponse};
use anyhow::{anyhow, bail, Result as AnyResult};
use async_stream::stream;
use crossbeam::sync::ShardedLock;
use log::debug;
use log::error;
use serde::{ser::SerializeStruct, Serializer};
use serde_json::value::RawValue;
use serde_yaml::Value as YamlValue;
use std::{
    borrow::Cow,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::broadcast::{self, error::RecvError},
    time::timeout,
};

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
        TransportConfig {
            name: Cow::from("api"),
            config: YamlValue::Null,
        }
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

struct HttpOutputEndpointInner {
    name: String,
    format: Format,

    total_buffers: AtomicU64,
    sender: ShardedLock<Option<broadcast::Sender<Buffer>>>,
    // This endpoint starts with sending a snapshot of a relation.
    snapshot: bool,
    stream: bool,
    // async_error_callback: RwLock<Option<AsyncErrorCallback>>,
}

impl HttpOutputEndpointInner {
    pub(crate) fn new(name: &str, format: Format, snapshot: bool, stream: bool) -> Self {
        Self {
            name: name.to_string(),
            format,
            total_buffers: AtomicU64::new(0),
            sender: ShardedLock::new(Some(broadcast::channel(MAX_BUFFERS).0)),
            snapshot,
            stream,
            // async_error_callback: RwLock::new(None),
        }
    }

    fn push_buffer(&self, buffer: Option<&[u8]>) -> AnyResult<()> {
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

        // A failure simply means that there are no receivers.
        let _ = self
            .sender
            .read()
            .unwrap()
            .as_ref()
            .map(|sender| sender.send(Buffer::new(seq_number, Bytes::from(json_buf))));
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
    pub(crate) fn new(name: &str, format: &str, snapshot: bool, stream: bool) -> Self {
        let format = match format {
            "csv" => Format::Text,
            "json" => Format::Json,
            _ => Format::Binary,
        };
        Self {
            inner: Arc::new(HttpOutputEndpointInner::new(name, format, snapshot, stream)),
        }
    }

    fn name(&self) -> &str {
        self.inner.name.as_str()
    }

    fn connect(&self) -> broadcast::Receiver<Buffer> {
        self.inner
            .sender
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .subscribe()
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
                    // if there is not real payload to send for more than 3 seconds, we will
                    // generate an empty chunk.  Note that it takes 6s, i.e., 2x the timeout
                    // period for actix to actually drop the connection.
                    match timeout(Duration::from_millis(3_000), receiver.recv()).await {
                        Err(_) => {
                            // Send the empty chunk via the `push_buffer` method to
                            // make sure it gets assigned correct sequence number.
                            let _ = inner.push_buffer(None);
                        }
                        Ok(Err(RecvError::Closed)) => break,
                        Ok(Err(RecvError::Lagged(_))) => (),
                        Ok(Ok(buffer)) => {
                            debug!(
                                "HTTP output endpoint '{}': sending chunk #{} ({} bytes)",
                                name,
                                buffer.sequence_number,
                                buffer.data.len(),
                            );
                            yield <AnyResult<_>>::Ok(buffer.data);
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
        self.inner.push_buffer(Some(buffer))
    }

    fn push_key(&mut self, _key: &[u8], _val: &[u8]) -> AnyResult<()> {
        bail!(
            "HTTP output transport does not support key-value pairs. \
This output endpoint was configured with a data format that produces outputs as key-value pairs; \
however the HTTP transport does not support this representation."
        );
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        // If we're sending an empty snapshot, output an explicit empty
        // batch to give the client a hint that the snapshot is empty
        // (but any correct client must handle any number of batches in
        // a snapshot, including 0, 1, and more).
        // Drop the sender after receiving the first batch of updates in
        // the snapshot mode.  The receiver will receive all buffered
        // messages followed by a `RecvError::Closed` notification.
        if self.inner.snapshot && self.inner.total_buffers.load(Ordering::Acquire) == 0 {
            let _ = self.inner.push_buffer(Some(&[]));
        }

        if !self.inner.stream {
            *self.inner.sender.write().unwrap() = None;
        }
        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}
