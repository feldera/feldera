use crate::format::StreamSplitter;
use crate::transport::{InputEndpoint, InputQueue, InputReaderCommand};
use crate::{
    server::{PipelineError, MAX_REPORTED_PARSE_ERRORS},
    transport::InputReader,
    ControllerError, InputConsumer, PipelineState, TransportInputEndpoint,
};
use crate::{InputBuffer, ParseError, Parser};
use actix_web::{web::Payload, HttpResponse};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use atomic::Atomic;
use circular_queue::CircularQueue;
use dbsp::circuit::tokio::TOKIO;
use feldera_types::program_schema::Relation;
use feldera_types::transport::http::HttpInputConfig;
use futures_util::StreamExt;
use rmpv::Value as RmpValue;
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

#[derive(Clone, Debug, Deserialize)]
pub(crate) enum HttpIngressMode {
    Batch,
    Stream,
    Chunks,
}

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
                InputReaderCommand::Seek(_) => (),
                InputReaderCommand::Replay { data, .. } => {
                    let Data { chunks } = rmpv::ext::from_value(data).unwrap();
                    let mut guard = self.details.lock().unwrap();
                    let details = guard.as_mut().unwrap();
                    let mut num_records = 0;
                    let mut hasher = Xxh3Default::new();
                    for chunk in chunks {
                        let (mut buffer, errors) = details.parser.parse(&chunk);
                        details.consumer.buffered(buffer.len(), chunk.len());
                        details.consumer.parse_errors(errors);
                        num_records += buffer.len();
                        buffer.hash(&mut hasher);
                        buffer.flush();
                    }
                    details.consumer.replayed(num_records, hasher.finish());
                }
                InputReaderCommand::Extend => self.set_state(PipelineState::Running),
                InputReaderCommand::Pause => self.set_state(PipelineState::Paused),
                InputReaderCommand::Queue => {
                    let mut guard = self.details.lock().unwrap();
                    let details = guard.as_mut().unwrap();
                    let (num_records, hash, chunks) = details.queue.flush_with_aux();
                    let data = if details.consumer.is_pipeline_fault_tolerant() {
                        rmpv::ext::to_value(Data {
                            chunks: chunks.into_iter().map(ByteBuf::from).collect(),
                        })
                        .unwrap()
                    } else {
                        RmpValue::Nil
                    };
                    details
                        .consumer
                        .extended(num_records, hash, serde_json::Value::Null, data);
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

    fn name(&self) -> &str {
        &self.inner.name
    }

    fn push(&self, bytes: Option<&[u8]>, errors: &mut CircularQueue<ParseError>) -> usize {
        let mut guard = self.inner.details.lock().unwrap();
        let details = guard.as_mut().unwrap();
        if let Some(bytes) = bytes {
            details.splitter.append(bytes);
        }
        let mut total_errors = 0;
        while let Some(chunk) = details.splitter.next(bytes.is_none()) {
            let (buffer, new_errors) = details.parser.parse(chunk);
            let aux = if details.consumer.is_pipeline_fault_tolerant() {
                Vec::from(chunk)
            } else {
                Vec::new()
            };
            details
                .queue
                .push_with_aux((buffer, new_errors.clone()), chunk.len(), aux);
            total_errors += new_errors.len();
            for error in new_errors {
                errors.push(error);
            }
        }
        drop(guard);

        total_errors
    }

    fn error(&self, fatal: bool, error: AnyError) {
        self.inner
            .details
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .consumer
            .error(fatal, error);
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
    ) -> Result<HttpResponse, PipelineError> {
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
                    // Check pipeline status at least every second.
                    match timeout(Duration::from_millis(1_000), payload.next()).await {
                        Err(_elapsed) => (),
                        Ok(Some(Ok(bytes))) => {
                            num_bytes += bytes.len();
                            num_errors += self.push(Some(&bytes), &mut errors);
                        }
                        Ok(Some(Err(e))) => {
                            self.error(true, anyhow!(e.to_string()));
                            Err(ControllerError::input_transport_error(
                                self.name(),
                                true,
                                anyhow!(e),
                            ))?
                        }
                        Ok(None) => {
                            num_errors += self.push(None, &mut errors);
                            break;
                        }
                    }
                }
            }
        }

        debug!(
            "HTTP input endpoint '{}': end of request, {num_bytes} received",
            self.name()
        );
        if errors.is_empty() {
            Ok(HttpResponse::Ok().finish())
        } else {
            Err(PipelineError::parse_errors(num_errors, errors.asc_iter()))
        }
    }
}

impl InputEndpoint for HttpInputEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        true
    }
}

impl TransportInputEndpoint for HttpInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
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
