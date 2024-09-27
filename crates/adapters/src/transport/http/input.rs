use crate::format::AppendSplitter;
use crate::transport::{InputEndpoint, InputQueue};
use crate::{
    server::{PipelineError, MAX_REPORTED_PARSE_ERRORS},
    transport::{InputReader, Step},
    ControllerError, InputConsumer, PipelineState, TransportConfig, TransportInputEndpoint,
};
use crate::{ParseError, Parser};
use actix_web::{web::Payload, HttpResponse};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use atomic::Atomic;
use circular_queue::CircularQueue;
use feldera_types::program_schema::Relation;
use futures_util::StreamExt;
use log::debug;
use serde::Deserialize;
use std::{
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};
use tokio::{sync::watch, time::timeout};

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

    pub(crate) fn config() -> TransportConfig {
        TransportConfig::HttpInput
    }
}

struct HttpInputEndpointDetails {
    consumer: Box<dyn InputConsumer>,
    parser: Box<dyn Parser>,
    splitter: AppendSplitter,
    queue: InputQueue,
}

struct HttpInputEndpointInner {
    name: String,
    state: Atomic<PipelineState>,
    status_notifier: watch::Sender<()>,
    #[allow(clippy::type_complexity)]
    details: Mutex<Option<HttpInputEndpointDetails>>,
    /// Ingest data even if the pipeline is paused.
    force: bool,
}

impl HttpInputEndpointInner {
    fn new(name: &str, force: bool) -> Self {
        Self {
            name: name.to_string(),
            state: Atomic::new(if force {
                PipelineState::Running
            } else {
                PipelineState::Paused
            }),
            status_notifier: watch::channel(()).0,
            details: Mutex::new(None),
            force,
        }
    }
}

/// Input endpoint that streams input data via HTTP.
#[derive(Clone)]
pub(crate) struct HttpInputEndpoint {
    inner: Arc<HttpInputEndpointInner>,
}

impl HttpInputEndpoint {
    pub(crate) fn new(name: &str, force: bool) -> Self {
        Self {
            inner: Arc::new(HttpInputEndpointInner::new(name, force)),
        }
    }

    fn state(&self) -> PipelineState {
        self.inner.state.load(Ordering::Acquire)
    }

    fn name(&self) -> &str {
        &self.inner.name
    }

    fn notify(&self) {
        self.inner.status_notifier.send_replace(());
    }

    fn push(&self, bytes: Option<&[u8]>, errors: &mut CircularQueue<ParseError>) -> usize {
        let mut guard = self.inner.details.lock().unwrap();
        let details = guard.as_mut().unwrap();
        if let Some(bytes) = bytes {
            details.splitter.append(bytes);
        }
        let mut total_errors = 0;
        while let Some(chunk) = details.splitter.next(bytes.is_none()) {
            let (buffer, mut new_errors) = details.parser.parse(chunk);
            total_errors += new_errors.len();
            details
                .queue
                .push(chunk.len(), (buffer, new_errors.clone()));
            for error in new_errors.drain(..) {
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

    /// Read the `payload` stream and push it to the pipeline.
    ///
    /// Returns on reaching the end of the `payload` stream
    /// (if any) or when the pipeline terminates.
    pub(crate) async fn complete_request(
        &self,
        mut payload: Payload,
    ) -> Result<HttpResponse, PipelineError> {
        debug!("HTTP input endpoint '{}': start of request", self.name());

        let mut num_bytes = 0;
        let mut errors = CircularQueue::with_capacity(MAX_REPORTED_PARSE_ERRORS);
        let mut num_errors = 0;
        let mut status_watch = self.inner.status_notifier.subscribe();

        loop {
            match self.state() {
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

        // Wait for the controller to process all of our records. Otherwise, the
        // queue would get destroyed when the caller drops us, which could lead
        // to some of our records never getting processed.
        while !self
            .inner
            .details
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .queue
            .is_empty()
        {
            status_watch.changed().await.unwrap();
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
        false
    }
}

impl TransportInputEndpoint for HttpInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _start_step: Step,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        let splitter = AppendSplitter::new(parser.splitter());
        *self.inner.details.lock().unwrap() = Some(HttpInputEndpointDetails {
            queue: InputQueue::new(consumer.clone()),
            consumer,
            parser,
            splitter,
        });
        Ok(Box::new(self.clone()))
    }
}

impl InputReader for HttpInputEndpoint {
    fn pause(&self) -> AnyResult<()> {
        if !self.inner.force {
            self.inner
                .state
                .store(PipelineState::Paused, Ordering::Release);
            self.notify();
        }

        Ok(())
    }

    fn start(&self, _step: Step) -> AnyResult<()> {
        self.inner
            .state
            .store(PipelineState::Running, Ordering::Release);
        self.notify();

        Ok(())
    }

    fn disconnect(&self) {
        self.inner
            .state
            .store(PipelineState::Terminated, Ordering::Release);
        self.notify();
    }

    fn flush(&self, n: usize) -> usize {
        let mut guard = self.inner.details.lock().unwrap();
        let details = guard.as_mut().unwrap();
        let total = details.queue.flush(n);
        if details.queue.is_empty() {
            self.notify();
        }
        total
    }
}
