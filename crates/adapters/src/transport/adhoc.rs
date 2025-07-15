use crate::catalog::ArrowStream;
use crate::transport::{InputEndpoint, InputQueue, InputReaderCommand};
use crate::{
    server::PipelineError, transport::InputReader, ControllerError, InputConsumer, PipelineState,
    TransportInputEndpoint,
};
use crate::{InputBuffer, Parser};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use atomic::Atomic;
use bytes::Bytes;
use datafusion::execution::SendableRecordBatchStream;
use feldera_adapterlib::transport::Resume;
use feldera_types::config::FtModel;
use feldera_types::program_schema::Relation;
use feldera_types::transport::adhoc::AdHocInputConfig;
use futures::future::{BoxFuture, FutureExt};
use futures_util::StreamExt;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::{
    hash::Hasher,
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::watch,
    time::{sleep, timeout},
};
use xxhash_rust::xxh3::Xxh3Default;

/// An [AsyncFileWriter] that appends to a byte vector.
struct BufferWriter<'a> {
    buffer: &'a mut Vec<u8>,
}

impl AsyncFileWriter for BufferWriter<'_> {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        self.buffer.extend(bs);
        async move { Ok(()) }.boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async move { Ok(()) }.boxed()
    }
}

struct AdHocInputEndpointDetails {
    consumer: Box<dyn InputConsumer>,
    parser: Box<dyn Parser>,
    queue: InputQueue<Vec<u8>>,
}

struct AdHocInputEndpointInner {
    name: String,
    state: Atomic<PipelineState>,
    status_notifier: watch::Sender<()>,
    details: Mutex<Option<AdHocInputEndpointDetails>>,
}

impl AdHocInputEndpointInner {
    fn new(config: AdHocInputConfig) -> Self {
        Self {
            name: config.name,
            state: Atomic::new(PipelineState::Paused),
            status_notifier: watch::channel(()).0,
            details: Mutex::new(None),
        }
    }
}

/// Input endpoint for updates from ad-hoc queries.
#[derive(Clone)]
pub(crate) struct AdHocInputEndpoint {
    inner: Arc<AdHocInputEndpointInner>,
}

/// Ad-hoc input endpoint.
///
/// We can create an ad-hoc input endpoint in two ways:
///
/// - Directly from an ad-hoc query. In that case, the query passes the batch to
///   insert to [AdHocInputEndpoint::complete_request] as a [RecordBatch]. If
///   fault tolerance is enabled, we also serialize the batch as Parquet format
///   to write to the log.
///
/// - From a fault tolerance log replay. In that case, we parse the batch in
///   Parquet format and apply it. (There is nothing in the replay path that
///   guarantees the batch is in Parquet format, but the ad-hoc query code
///   configures the input adapter to use Parquet format, which propagates to
///   the log record that creates the `AdHocInputEndpoint`.)
impl AdHocInputEndpoint {
    pub(crate) fn new(config: AdHocInputConfig) -> Self {
        Self {
            inner: Arc::new(AdHocInputEndpointInner::new(config)),
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

    fn fault_tolerance(&self) -> Option<FtModel> {
        let mut guard = self.inner.details.lock().unwrap();
        let details = guard.as_mut().unwrap();
        details.consumer.pipeline_fault_tolerance()
    }

    async fn push(
        &self,
        batch: RecordBatch,
        schema: &Arc<Schema>,
        arrow_inserter: &mut Box<dyn ArrowStream>,
    ) -> AnyResult<u64> {
        arrow_inserter.insert(&batch)?;
        let buffer = arrow_inserter.take_all();
        let num_records = buffer.len();
        if !buffer.is_empty() {
            let mut aux = Vec::new();
            if self.fault_tolerance() == Some(FtModel::ExactlyOnce) {
                let mut writer = AsyncArrowWriter::try_new(
                    BufferWriter { buffer: &mut aux },
                    schema.clone(),
                    Some(
                        WriterProperties::builder()
                            .set_compression(Compression::SNAPPY)
                            .build(),
                    ),
                )?;
                writer.write(&batch).await?;
                writer.flush().await?;
                writer.close().await?;
            };

            let mut guard = self.inner.details.lock().unwrap();
            let details = guard.as_mut().unwrap();
            details.queue.push_with_aux((buffer, Vec::new()), 0, aux);
        }

        Ok(num_records as u64)
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

    fn queue_len(&self) -> usize {
        self.inner
            .details
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .queue
            .len()
    }

    pub(crate) async fn complete_request(
        &self,
        mut data: SendableRecordBatchStream,
        mut arrow_inserter: Box<dyn ArrowStream>,
        schema: &Arc<Schema>,
    ) -> Result<u64, PipelineError> {
        let mut num_records = 0;
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
                    match timeout(Duration::from_millis(1_000), data.next()).await {
                        Err(_elapsed) => (),
                        Ok(Some(Ok(batch))) => {
                            num_records += self
                                .push(batch, schema, &mut arrow_inserter)
                                .await
                                .map_err(|e| PipelineError::AdHocQueryError {
                                    error: e.to_string(),
                                    df: None,
                                })?;
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
                            break;
                        }
                    }
                }
            }
        }

        // Wait for the controller to process all of our records. Otherwise, the
        // queue would get destroyed when the caller drops us, which could lead
        // to some of our records never getting processed.
        while self.queue_len() > 0 {
            sleep(Duration::from_millis(100)).await;
        }

        Ok(num_records)
    }

    fn set_state(&self, state: PipelineState) {
        self.inner.state.store(state, Ordering::Release);
        self.notify();
    }
}

impl InputEndpoint for AdHocInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for AdHocInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        _resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        let queue = InputQueue::new(consumer.clone());
        *self.inner.details.lock().unwrap() = Some(AdHocInputEndpointDetails {
            consumer,
            parser,
            queue,
        });
        Ok(Box::new(self.clone()))
    }
}

impl InputReader for AdHocInputEndpoint {
    fn request(&self, command: InputReaderCommand) {
        match command {
            InputReaderCommand::Replay { data, .. } => {
                let Metadata { batches: chunks } = rmpv::ext::from_value(data).unwrap();
                let mut guard = self.inner.details.lock().unwrap();
                let details = guard.as_mut().unwrap();
                let mut num_records = 0;
                let mut hasher = Xxh3Default::new();
                for chunk in chunks {
                    let (mut buffer, errors) = details.parser.parse(&chunk);
                    details.consumer.buffered(buffer.len(), !chunk.len());
                    details.consumer.parse_errors(errors);
                    num_records += buffer.len();
                    buffer.hash(&mut hasher);
                    buffer.flush();
                }
                details.consumer.replayed(num_records, hasher.finish());
            }
            InputReaderCommand::Extend => self.set_state(PipelineState::Running),
            InputReaderCommand::Pause => self.set_state(PipelineState::Paused),
            InputReaderCommand::Queue { .. } => {
                let mut guard = self.inner.details.lock().unwrap();
                let details = guard.as_mut().unwrap();
                let (num_records, hasher, batches) = details.queue.flush_with_aux();
                let resume = Resume::new_data_only(
                    || {
                        rmpv::ext::to_value(Metadata {
                            batches: batches.into_iter().map(ByteBuf::from).collect(),
                        })
                        .unwrap()
                    },
                    hasher,
                );
                details.consumer.extended(num_records, Some(resume));
            }
            InputReaderCommand::Disconnect => self.set_state(PipelineState::Terminated),
        }
    }

    fn is_closed(&self) -> bool {
        false
    }
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    /// Serialized batches in Parquet format.
    batches: Vec<ByteBuf>,
}
