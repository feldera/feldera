use crate::format::StreamSplitter;
use crate::transport::{InputEndpoint, InputQueue, InputReaderCommand};
use crate::{
    server::{PipelineError, MAX_REPORTED_PARSE_ERRORS},
    transport::InputReader,
    ControllerError, InputConsumer, PipelineState, TransportInputEndpoint,
};
use crate::{InputBuffer, ParseError, Parser};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use atomic::Atomic;
use chrono::{DateTime, Utc};
use circular_queue::CircularQueue;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::transport::{Resume, Watermark};
use feldera_types::config::FtModel;
use feldera_types::program_schema::Relation;
use feldera_types::transport::http::HttpInputConfig;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::{
    hash::Hasher,
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
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

struct HttpInputEndpointDetails {
    consumer: Box<dyn InputConsumer>,
    parser: Box<dyn Parser>,
    splitter: StreamSplitter,
    queue: InputQueue<Vec<u8>>,
}

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
                    let Data { chunks } = rmpv::ext::from_value(data).unwrap();
                    let mut guard = self.details.lock().unwrap();
                    let details = guard.as_mut().unwrap();
                    let mut total = BufferSize::empty();
                    let mut hasher = Xxh3Default::new();
                    for chunk in chunks {
                        let (mut buffer, errors) = details.parser.parse(&chunk);
                        details.consumer.buffered(buffer.len());
                        details.consumer.parse_errors(errors);
                        total += buffer.len();
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
                        || {
                            rmpv::ext::to_value(Data {
                                chunks: chunks.into_iter().map(ByteBuf::from).collect(),
                            })
                            .unwrap()
                        },
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
}

impl HttpInputEndpoint {
    pub(crate) fn new(config: HttpInputConfig) -> Self {
        let (sender, receiver) = unbounded_channel();
        Self {
            inner: HttpInputEndpointInner::new(config, receiver),
            sender,
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
        bytes: Option<&[u8]>,
        errors: &mut CircularQueue<ParseError>,
        timestamp: DateTime<Utc>,
    ) -> usize {
        let mut guard = self.inner.details.lock().unwrap();
        let details = guard.as_mut().unwrap();
        if let Some(bytes) = bytes {
            details.splitter.append(bytes);
        }
        let mut total_errors = 0;
        while let Some(chunk) = details.splitter.next(bytes.is_none()) {
            let (buffer, new_errors) = details.parser.parse(chunk);
            let aux = if details.consumer.pipeline_fault_tolerance() == Some(FtModel::ExactlyOnce) {
                Vec::from(chunk)
            } else {
                Vec::new()
            };
            details
                .queue
                .push_with_aux((buffer, new_errors.clone()), timestamp, aux);
            total_errors += new_errors.len();
            for error in new_errors {
                errors.push(error);
            }
        }
        drop(guard);

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
        mut payload: axum::body::Body,
        force: bool,
    ) -> Result<(), PipelineError> {
        debug!("HTTP input endpoint '{}': start of request", self.name());

        let mut num_bytes = 0;
        let mut errors = CircularQueue::with_capacity(MAX_REPORTED_PARSE_ERRORS);
        let mut num_errors = 0;
        let mut status_watch = self.inner.status_notifier.subscribe();

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

                    // Read the entire body at once
                    let bytes = axum::body::to_bytes(payload, usize::MAX).await
                        .map_err(|e| ControllerError::io_error("failed to read request body".to_string(), std::io::Error::new(std::io::ErrorKind::Other, e)))?;

                    num_bytes += bytes.len();
                    num_errors += self.push(Some(&bytes), &mut errors, timestamp);
                    break; // We've read the entire body
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
        let splitter = StreamSplitter::new(parser.splitter());
        let queue = InputQueue::new(consumer.clone());
        *self.inner.details.lock().unwrap() = Some(HttpInputEndpointDetails {
            consumer,
            parser,
            queue,
            splitter,
        });
        Ok(Box::new(self.clone()))
    }
}

impl InputReader for HttpInputEndpoint {
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
}
