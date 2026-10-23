use crate::controller::ApiConnectionGuard;
use crate::format::StreamSplitter;
use crate::transport::{InputEndpoint, InputQueue, InputReaderCommand};
use crate::{
    ControllerError, InputConsumer, PipelineState, TransportInputEndpoint,
    server::{MAX_REPORTED_PARSE_ERRORS, PipelineError},
    transport::InputReader,
};
use crate::{InputBuffer, ParseError, Parser};
use actix_web::web::Payload;
use anyhow::{Error as AnyError, Result as AnyResult, anyhow};
use atomic::Atomic;
use chrono::{DateTime, Utc};
use circular_queue::CircularQueue;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::ConnectorMetadata;
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::transport::{Resume, Watermark};
use feldera_sqllib::FlatVariant;
use feldera_types::config::FtModel;
use feldera_types::program_schema::Relation;
use feldera_types::transport::http::HttpInputConfig;
use feldera_types::variant::VariantValue;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::{
    hash::Hasher,
    iter::repeat,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::{sync::watch, time::timeout};
use tracing::{debug, info_span};
use xxhash_rust::xxh3::Xxh3Default;

/// HTTP input transport.
///
/// HTTP endpoints are instantiated via the REST API, so this type doesn't
/// implement `trait InputTransport`.  It is only used to
/// collect static functions related to HTTP.
pub(crate) struct HttpInputTransport;

impl HttpInputTransport {
    /// Default data format assumed by API endpoints when not explicit
    /// "?format=" argument provided.
    // TODO: json is a better default, once we support it.
    pub(crate) fn default_format() -> String {
        String::from("csv")
    }

    pub(crate) fn default_max_buffered_records() -> u64 {
        100_000
    }

    // pub(crate) fn default_mode() -> HttpIngressMode {
    //    HttpIngressMode::Stream
    // }
}

/// Connector metadata that a client attaches to an `/ingress` request.
///
/// The connector passes the metadata to the parser with every chunk of the
/// request, so `CONNECTOR_METADATA()` returns it for every record of the
/// request.
#[derive(Clone, Debug)]
pub(crate) struct RequestMetadata {
    /// The JSON object as the client sent it.  The exactly-once journal
    /// stores it with each chunk of the request, and a replay decodes it
    /// again.
    json: Arc<str>,
    metadata: ConnectorMetadata,
}

impl RequestMetadata {
    /// Decodes the `connector_metadata` query parameter.
    pub(crate) fn parse(json: &str) -> Result<Self, String> {
        let value: FlatVariant = serde_json::from_str(json)
            .map_err(|e| format!("'connector_metadata' is not valid JSON: {e}"))?;
        let value = VariantValue::try_from(&value).map_err(|e| {
            format!(
                "'connector_metadata' holds a value that connector metadata cannot represent: {e}"
            )
        })?;
        let VariantValue::Object(attributes) = value else {
            return Err("'connector_metadata' must be a JSON object".to_string());
        };
        Ok(Self {
            json: Arc::from(json),
            metadata: ConnectorMetadata::from(attributes),
        })
    }
}

/// What the exactly-once journal keeps about one chunk of a request.
#[derive(Default)]
struct JournaledChunk {
    data: Vec<u8>,
    /// The `connector_metadata` of the chunk's request, as the client sent it.
    connector_metadata: Option<Arc<str>>,
}

struct HttpInputEndpointDetails {
    consumer: Box<dyn InputConsumer>,
    parser: Box<dyn Parser>,
    queue: InputQueue<JournaledChunk>,
}

/// Test hooks that hold the replay of the HTTP input endpoint with a given
/// name. The endpoint's background task calls the hook before it parses and
/// flushes the replayed input.
#[cfg(test)]
pub(crate) static REPLAY_GATES: std::sync::LazyLock<
    Mutex<std::collections::HashMap<String, ReplayGate>>,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
pub(crate) type ReplayGate = Box<dyn FnOnce() + Send>;

struct HttpInputEndpointInner {
    name: String,
    state: Atomic<PipelineState>,
    status_notifier: watch::Sender<()>,
    details: Mutex<Option<HttpInputEndpointDetails>>,
}

impl HttpInputEndpointInner {
    fn new(config: HttpInputConfig, receiver: UnboundedReceiver<InputReaderCommand>) -> Arc<Self> {
        let inner = Arc::new(Self {
            name: config.name,
            state: Atomic::new(PipelineState::Paused),
            status_notifier: watch::channel(()).0,
            details: Mutex::new(None),
        });

        TOKIO.spawn(HttpInputEndpointInner::background_task(
            inner.clone(),
            receiver,
        ));

        inner
    }

    async fn background_task(self: Arc<Self>, mut receiver: UnboundedReceiver<InputReaderCommand>) {
        let input_span = info_span!("http_input");
        while let Some(message) = receiver.recv().await {
            input_span.in_scope(|| match message {
                InputReaderCommand::Replay { data, .. } => {
                    #[cfg(test)]
                    if let Some(gate) = REPLAY_GATES.lock().unwrap().remove(&self.name) {
                        gate();
                    }
                    let Data {
                        chunks,
                        connector_metadata,
                    } = rmpv::ext::from_value(data).unwrap();
                    let mut guard = self.details.lock().unwrap();
                    let details = guard.as_mut().unwrap();
                    let mut total = BufferSize::empty();
                    let mut hasher = Xxh3Default::new();
                    // A journal written before `connector_metadata` existed holds no
                    // entries for its chunks.
                    let mut connector_metadata = connector_metadata.into_iter().chain(repeat(None));
                    for chunk in chunks {
                        // The replay parses
                        // each chunk with the metadata of its request to reproduce the
                        // records and their hash.
                        let metadata = match connector_metadata
                            .next()
                            .flatten()
                            .map(|json| RequestMetadata::parse(&json))
                            .transpose()
                        {
                            Ok(request) => request.map(|request| request.metadata),
                            // A journal that an older version wrote can hold metadata that
                            // this version refuses, such as a number with more digits than a
                            // VARIANT decimal holds.
                            Err(error) => {
                                details.consumer.error(
                                    true,
                                    anyhow!("Cannot replay a journaled request: {error}"),
                                    Some("http-input"),
                                );
                                return;
                            }
                        };
                        let (mut buffer, errors) = details.parser.parse(&chunk, metadata);
                        let len = buffer.len();
                        details.consumer.buffered(len);
                        details.consumer.parse_errors(errors);
                        total += len;
                        buffer.hash(&mut hasher);
                        buffer.flush();
                    }
                    details.consumer.replayed(total, hasher.finish());
                }
                InputReaderCommand::Extend => self.set_state(PipelineState::Running),
                InputReaderCommand::Pause => self.set_state(PipelineState::Paused),
                InputReaderCommand::Queue { .. } => {
                    let mut guard = self.details.lock().unwrap();
                    let details = guard.as_mut().unwrap();
                    let (num_records, hasher, chunks) = details.queue.flush_with_aux();
                    let (timestamps, chunks) = chunks.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();
                    let resume = Resume::new_data_only(
                        || rmpv::ext::to_value(Data::from(chunks)).unwrap(),
                        hasher.map(|h| h.finish()),
                    );
                    details.consumer.extended(
                        num_records,
                        Some(resume),
                        timestamps
                            .into_iter()
                            .map(|t| Watermark::new(t, None))
                            .collect(),
                    );
                }
                InputReaderCommand::Disconnect => self.set_state(PipelineState::Terminated),
            });
        }
    }

    fn notify(&self) {
        self.status_notifier.send_replace(());
    }

    fn set_state(&self, state: PipelineState) {
        self.state.store(state, Ordering::Release);
        self.notify();
    }
}

/// Input endpoint that streams input data via HTTP.
#[derive(Clone)]
pub(crate) struct HttpInputEndpoint {
    inner: Arc<HttpInputEndpointInner>,
    sender: UnboundedSender<InputReaderCommand>,
    _guard: Option<Arc<ApiConnectionGuard>>,
}

impl HttpInputEndpoint {
    pub(crate) fn new(config: HttpInputConfig) -> Self {
        let (sender, receiver) = unbounded_channel();
        Self {
            inner: HttpInputEndpointInner::new(config, receiver),
            sender,
            _guard: None,
        }
    }

    pub(crate) fn with_api_connection_guard(self, guard: ApiConnectionGuard) -> Self {
        Self {
            _guard: Some(Arc::new(guard)),
            ..self
        }
    }

    fn state(&self) -> PipelineState {
        self.inner.state.load(Ordering::Acquire)
    }

    pub(crate) fn name(&self) -> &str {
        &self.inner.name
    }

    fn push(
        &self,
        chunk: &[u8],
        connector_metadata: Option<&RequestMetadata>,
        errors: &mut CircularQueue<ParseError>,
        timestamp: DateTime<Utc>,
    ) -> usize {
        let mut guard = self.inner.details.lock().unwrap();
        let details = guard.as_mut().unwrap();
        let mut total_errors = 0;
        let (buffer, new_errors) = details.parser.parse(
            chunk,
            connector_metadata.map(|metadata| metadata.metadata.clone()),
        );
        let aux = if details.consumer.pipeline_fault_tolerance() == Some(FtModel::ExactlyOnce) {
            JournaledChunk {
                data: Vec::from(chunk),
                connector_metadata: connector_metadata.map(|metadata| metadata.json.clone()),
            }
        } else {
            JournaledChunk::default()
        };
        details
            .queue
            .push_with_aux((buffer, new_errors.clone()), timestamp, aux);
        total_errors += new_errors.len();
        for error in new_errors {
            errors.push(error);
        }
        total_errors
    }

    fn error(&self, fatal: bool, error: AnyError, tag: Option<&'static str>) {
        self.inner
            .details
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .consumer
            .error(fatal, error, tag);
    }

    fn _queue_len(&self) -> usize {
        self.inner
            .details
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .queue
            .len()
    }

    /// Read the `payload` stream and push it to the pipeline.
    ///
    /// Returns on reaching the end of the `payload` stream
    /// (if any) or when the pipeline terminates.
    pub(crate) async fn complete_request(
        &self,
        mut payload: Payload,
        force: bool,
        connector_metadata: Option<RequestMetadata>,
    ) -> Result<(), PipelineError> {
        debug!("HTTP input endpoint '{}': start of request", self.name());

        let mut num_bytes = 0;
        let mut errors = CircularQueue::with_capacity(MAX_REPORTED_PARSE_ERRORS);
        let mut num_errors = 0;
        let mut status_watch = self.inner.status_notifier.subscribe();

        let mut splitter = StreamSplitter::new(
            self.inner
                .details
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .parser
                .splitter(),
        );
        loop {
            let pipeline_state = self.state();

            let forced_state = if force && pipeline_state == PipelineState::Paused {
                PipelineState::Running
            } else {
                pipeline_state
            };

            match forced_state {
                PipelineState::Paused => {
                    let _ = status_watch.changed().await;
                }
                PipelineState::Terminated => {
                    return Err(PipelineError::Terminating);
                }
                PipelineState::Running => {
                    // Use the time when we started reading the next chunk of the payload as the
                    // ingestion timestamp.
                    let timestamp = Utc::now();

                    // Check pipeline status at least every second.
                    let eoi = match timeout(Duration::from_millis(1_000), payload.next()).await {
                        Err(_elapsed) => continue,
                        Ok(Some(Err(e))) => {
                            // A broken request body, for example a client that
                            // hangs up, fails this request only. The endpoint is
                            // shared by later requests to the same table, so a
                            // fatal error here would stay in `/stats` while the
                            // endpoint keeps accepting data.
                            self.error(false, anyhow!(e.to_string()), None);
                            Err(ControllerError::input_transport_error(
                                self.name(),
                                false,
                                anyhow!(e),
                            ))?
                        }
                        Ok(Some(Ok(bytes))) => {
                            num_bytes += bytes.len();
                            splitter.append(&bytes);
                            false
                        }
                        Ok(None) => true,
                    };
                    while let Some(chunk) = splitter.next(eoi) {
                        num_errors +=
                            self.push(chunk, connector_metadata.as_ref(), &mut errors, timestamp);
                    }
                    if eoi {
                        break;
                    }
                }
            }
        }

        debug!(
            "HTTP input endpoint '{}': end of request, {num_bytes} received",
            self.name()
        );
        if errors.is_empty() {
            Ok(())
        } else {
            Err(PipelineError::parse_errors(num_errors, errors.asc_iter()))
        }
    }
}

impl InputEndpoint for HttpInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for HttpInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        _resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        let queue = InputQueue::new(consumer.clone());
        *self.inner.details.lock().unwrap() = Some(HttpInputEndpointDetails {
            consumer,
            parser,
            queue,
        });
        Ok(Box::new(self.clone()))
    }
}

impl InputReader for HttpInputEndpoint {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        let _ = self.sender.send(command);
    }

    fn is_closed(&self) -> bool {
        false
    }
}

#[derive(Serialize, Deserialize)]
struct Data {
    chunks: Vec<ByteBuf>,
    /// The `connector_metadata` of each chunk's request.
    /// Empty in a journal written before the parameter existed.
    #[serde(default)]
    connector_metadata: Vec<Option<String>>,
}

impl From<Vec<JournaledChunk>> for Data {
    fn from(chunks: Vec<JournaledChunk>) -> Self {
        Self {
            connector_metadata: chunks
                .iter()
                .map(|chunk| chunk.connector_metadata.as_deref().map(str::to_string))
                .collect(),
            chunks: chunks
                .into_iter()
                .map(|chunk| ByteBuf::from(chunk.data))
                .collect(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use dbsp::operator::input::StagedBuffers;
    use feldera_adapterlib::ConnectorMetadata;
    use feldera_adapterlib::format::{InputBuffer, ParseError, Parser, Splitter};
    use feldera_types::program_schema::Relation;
    use feldera_types::transport::http::HttpInputConfig;
    use feldera_types::variant::VariantValue;
    use serde_bytes::ByteBuf;

    use super::{Data, HttpInputEndpoint, RequestMetadata};
    use crate::TransportInputEndpoint;
    use crate::test::{MockInputConsumer, wait};
    use crate::transport::InputReaderCommand;

    /// Each field of the `connector_metadata` object becomes one attribute,
    /// converted the way any JSON value converts to a VARIANT.
    #[test]
    fn request_metadata_holds_one_attribute_per_field() {
        let metadata = RequestMetadata::parse(
            r#"{"kafka_topic": "events", "kafka_offset": 42, "kafka_headers": {"h": null}}"#,
        )
        .unwrap()
        .metadata;
        assert_eq!(
            metadata.get_by_name("kafka_topic"),
            Some(&VariantValue::String("events".into()))
        );
        // JSON reads a non-negative integer as unsigned, which widens to a BIGINT.
        assert_eq!(
            metadata.get_by_name("kafka_offset"),
            Some(&VariantValue::Int64(42))
        );
        assert_eq!(
            metadata.get_by_name("kafka_headers"),
            Some(&VariantValue::Object(BTreeMap::from([(
                "h".to_string(),
                VariantValue::Null
            )])))
        );
    }

    /// Of two fields with one name, the last one counts, as in a JSON object.
    #[test]
    fn request_metadata_keeps_the_last_of_two_fields() {
        let metadata = RequestMetadata::parse(r#"{"a": 1, "a": "two"}"#)
            .unwrap()
            .metadata;
        assert_eq!(
            metadata.get_by_name("a"),
            Some(&VariantValue::String("two".into()))
        );
    }

    /// A number with more digits than a VARIANT decimal holds is refused.
    #[test]
    fn request_metadata_refuses_a_value_it_cannot_represent() {
        let error = RequestMetadata::parse(r#"{"x": 123456789012345678901234567890123456789}"#)
            .unwrap_err();
        assert!(
            error.contains("'connector_metadata' holds a value"),
            "{error}"
        );
    }

    /// A parameter that is not a JSON object is refused, and the error names
    /// the parameter.
    #[test]
    fn request_metadata_must_be_a_json_object() {
        for invalid in ["not json", "[1, 2]", "\"text\"", "7", "null"] {
            let error = RequestMetadata::parse(invalid).unwrap_err();
            assert!(error.contains("'connector_metadata'"), "{invalid}: {error}");
        }
    }

    /// The parser of a replay that fails before it parses a chunk.
    struct UnusedParser;

    impl Parser for UnusedParser {
        fn parse(
            &mut self,
            _data: &[u8],
            _metadata: Option<ConnectorMetadata>,
        ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
            unreachable!("the replay fails before it parses a chunk")
        }

        fn stage(&self, _buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
            unreachable!("the replay fails before it stages a chunk")
        }

        fn splitter(&self) -> Box<dyn Splitter> {
            unreachable!("a replay needs no splitter")
        }

        fn fork(&self) -> Box<dyn Parser> {
            unreachable!("the endpoint parses on one thread")
        }
    }

    /// A replay of a journal that holds connector metadata this version refuses, as a journal
    /// of an older version can, fails the endpoint instead of panicking.
    #[test]
    fn replay_of_refused_metadata_fails_the_endpoint() {
        let consumer = MockInputConsumer::new();
        let errors = Arc::new(Mutex::new(Vec::new()));
        consumer.on_error(Some(Box::new({
            let errors = errors.clone();
            move |fatal, error| errors.lock().unwrap().push((fatal, error.to_string()))
        })));
        let endpoint = HttpInputEndpoint::new(HttpInputConfig {
            name: "replay".to_string(),
        });
        let reader = endpoint
            .open(
                Box::new(consumer),
                Box::new(UnusedParser),
                Relation::empty(),
                None,
            )
            .unwrap();
        let data = Data {
            chunks: vec![ByteBuf::from(b"{}".to_vec())],
            connector_metadata: vec![Some(
                r#"{"x": 123456789012345678901234567890123456789}"#.to_string(),
            )],
        };
        reader.request(InputReaderCommand::Replay {
            metadata: serde_json::Value::Null,
            data: rmpv::ext::to_value(data).unwrap(),
        });

        wait(|| !errors.lock().unwrap().is_empty(), 10_000).unwrap();
        let errors = errors.lock().unwrap();
        let (fatal, error) = &errors[0];
        assert!(*fatal, "{error}");
        assert!(
            error.starts_with(
                "Cannot replay a journaled request: 'connector_metadata' holds a value"
            ),
            "{error}"
        );
    }
}
