use crate::{client::SnowflakeClient, query::build_snapshot_query, snowflake_input_serde_config};
use anyhow::{Context, Result as AnyResult, anyhow};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::{
    PipelineState,
    catalog::{ArrowStream, InputCollectionHandle},
    errors::journal::ControllerError,
    format::{InputBuffer, ParseError},
    transport::{
        InputConsumer, InputEndpoint, InputQueue, InputQueueEntry, InputReader, InputReaderCommand,
        IntegratedInputEndpoint, Resume, Watermark, parse_resume_info,
    },
};
use feldera_types::{
    adapter_stats::ConnectorHealth,
    config::FtModel,
    program_schema::Relation,
    transport::snowflake::{SnowflakeIngestMode, SnowflakeReaderConfig, SnowflakeTransactionMode},
};
use futures_util::StreamExt;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex},
    thread,
};
use tokio::{
    select,
    sync::{
        mpsc,
        watch::{Receiver, Sender, channel},
    },
};

/// Integrated input connector that reads from a Snowflake table.
pub struct SnowflakeInputEndpoint {
    inner: Arc<SnowflakeInputEndpointInner>,
}

impl SnowflakeInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &SnowflakeReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        Self {
            inner: Arc::new(SnowflakeInputEndpointInner::new(
                endpoint_name,
                config.clone(),
                consumer,
            )),
        }
    }
}

impl InputEndpoint for SnowflakeInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        // This test-only fault tolerance supports full-refresh checkpoints, not recovery
        // within a snapshot: before EOI, recovery reruns the entire snapshot.
        Some(FtModel::AtLeastOnce)
    }
}

impl IntegratedInputEndpoint for SnowflakeInputEndpoint {
    fn open(
        self: Box<Self>,
        input_handle: &InputCollectionHandle,
        seek: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(SnowflakeInputReader::new(
            &self.inner,
            input_handle,
            seek,
        )?))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SnowflakeResumeInfo {
    eoi: bool,
}

impl SnowflakeResumeInfo {
    fn initial() -> Self {
        Self { eoi: false }
    }

    fn eoi() -> Self {
        Self { eoi: true }
    }

    fn to_resume(&self) -> Resume {
        Resume::Seek {
            seek: serde_json::to_value(self).unwrap(),
        }
    }
}

struct SnowflakeInputReader {
    sender: Sender<PipelineState>,
    inner: Arc<SnowflakeInputEndpointInner>,
}

impl SnowflakeInputReader {
    fn new(
        endpoint: &Arc<SnowflakeInputEndpointInner>,
        input_handle: &InputCollectionHandle,
        resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Self> {
        endpoint.config.validate().map_err(|e| {
            ControllerError::invalid_transport_configuration(&endpoint.endpoint_name, &e)
        })?;

        let resume_info = resume_info
            .as_ref()
            .map(parse_resume_info::<SnowflakeResumeInfo>)
            .transpose()?;
        let eoi = resume_info
            .as_ref()
            .is_some_and(|resume_info| resume_info.eoi);

        if let Some(resume_info) = &resume_info {
            *endpoint.last_resume_status.lock().unwrap() = Some(resume_info.clone());
            endpoint
                .queue
                .push_with_aux((None, Vec::new()), Utc::now(), Some(resume_info.clone()));
        }

        let (sender, receiver) = channel(PipelineState::Paused);

        let input_stream = input_handle
            .handle
            .configure_arrow_deserializer(snowflake_input_serde_config())?;
        let schema = input_handle.schema.clone();
        let endpoint_name = endpoint.endpoint_name.clone();

        if eoi {
            info!(
                "snowflake {endpoint_name}: skipping connector initialization because the snapshot is already complete"
            );
            endpoint.consumer.eoi();
        } else {
            let endpoint_clone = endpoint.clone();
            let receiver_clone = receiver.clone();
            let (init_status_sender, mut init_status_receiver) =
                mpsc::channel::<Result<(), ControllerError>>(1);

            thread::Builder::new()
                .name(format!("{endpoint_name}-snowflake-input-tokio-wrapper"))
                .spawn(move || {
                    TOKIO.block_on(async {
                        endpoint_clone
                            .worker_task(input_stream, schema, receiver_clone, init_status_sender)
                            .await;
                    })
                })
                .expect("failed to spawn snowflake-input tokio wrapper thread");

            init_status_receiver.blocking_recv().ok_or_else(|| {
                anyhow!("worker thread terminated unexpectedly during initialization")
            })??;
        }

        Ok(Self {
            sender,
            inner: endpoint.clone(),
        })
    }
}

impl InputReader for SnowflakeInputReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        match command {
            InputReaderCommand::Replay { .. } => panic!(
                "replay command is not supported by SnowflakeInputReader; this is a bug, please report it to developers"
            ),
            InputReaderCommand::Extend => {
                let _ = self.sender.send_replace(PipelineState::Running);
            }
            InputReaderCommand::Pause => {
                let _ = self.sender.send_replace(PipelineState::Paused);
            }
            InputReaderCommand::Queue {
                checkpoint_requested,
            } => {
                let stop_at: &dyn Fn(&Option<SnowflakeResumeInfo>) -> bool = if checkpoint_requested
                {
                    &Option::is_some
                } else {
                    &|_| false
                };
                let (total, _, resume_info) = self.inner.queue.flush_with_aux_until(stop_at);
                let resume_status = match resume_info.last() {
                    None => self.inner.last_resume_status.lock().unwrap().clone(),
                    Some((_, resume_info)) => resume_info.clone(),
                };
                *self.inner.last_resume_status.lock().unwrap() = resume_status.clone();

                let resume = match resume_status {
                    Some(resume_info) => resume_info.to_resume(),
                    None => Resume::Barrier,
                };

                self.inner.consumer.extended(
                    total,
                    Some(resume),
                    resume_info
                        .into_iter()
                        .map(|(timestamp, resume_info)| {
                            Watermark::new(
                                timestamp,
                                resume_info
                                    .map(|resume_info| serde_json::to_value(resume_info).unwrap()),
                            )
                        })
                        .collect(),
                );
            }
            InputReaderCommand::Disconnect => {
                let _ = self.sender.send_replace(PipelineState::Terminated);
            }
        }
    }

    fn is_closed(&self) -> bool {
        self.inner.queue.is_empty() && self.sender.is_closed()
    }
}

impl Drop for SnowflakeInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

struct SnowflakeInputEndpointInner {
    endpoint_name: String,
    config: SnowflakeReaderConfig,
    consumer: Box<dyn InputConsumer>,
    queue: InputQueue<Option<SnowflakeResumeInfo>>,
    last_resume_status: Mutex<Option<SnowflakeResumeInfo>>,
}

impl SnowflakeInputEndpointInner {
    fn new(
        endpoint_name: &str,
        config: SnowflakeReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        let queue = InputQueue::new(consumer.clone());
        Self {
            endpoint_name: endpoint_name.to_string(),
            config,
            consumer,
            queue,
            last_resume_status: Mutex::new(Some(SnowflakeResumeInfo::initial())),
        }
    }

    async fn worker_task(
        self: Arc<Self>,
        input_stream: Box<dyn ArrowStream>,
        schema: Relation,
        receiver: Receiver<PipelineState>,
        init_status_sender: mpsc::Sender<Result<(), ControllerError>>,
    ) {
        let mut receiver_clone = receiver.clone();
        select! {
            _ = Self::worker_task_inner(self.clone(), input_stream, schema, receiver, init_status_sender) => {
                debug!("snowflake {}: worker task terminated", &self.endpoint_name);
            }
            _ = receiver_clone.wait_for(|state| state == &PipelineState::Terminated) => {
                debug!("snowflake {}: received termination command; worker task canceled", &self.endpoint_name);
            }
        }
    }

    async fn worker_task_inner(
        self: Arc<Self>,
        input_stream: Box<dyn ArrowStream>,
        schema: Relation,
        mut receiver: Receiver<PipelineState>,
        init_status_sender: mpsc::Sender<Result<(), ControllerError>>,
    ) {
        let query = match self.config.mode {
            SnowflakeIngestMode::Snapshot => build_snapshot_query(
                &self.config.table,
                self.config.snapshot_filter.as_deref(),
                &self.config.column_mapping,
                &schema,
            ),
        };
        let query = match query {
            Ok(query) => query,
            Err(e) => {
                let _ = init_status_sender
                    .send(Err(ControllerError::invalid_transport_configuration(
                        &self.endpoint_name,
                        &e.to_string(),
                    )))
                    .await;
                return;
            }
        };

        let _ = init_status_sender.send(Ok(())).await;
        wait_running(&mut receiver).await;

        debug!(
            "snowflake {}: prepared snapshot query for table '{}'",
            &self.endpoint_name, &self.config.table
        );

        let client = match SnowflakeClient::from_reader_config(&self.config).await {
            Ok(client) => client,
            Err(e) => {
                let message = format!("error connecting to Snowflake for snapshot input: {e:#}");
                self.consumer
                    .update_connector_health(ConnectorHealth::unhealthy(&message));
                self.consumer.error(true, anyhow!(message), None);
                return;
            }
        };

        let result_stream = match client
            .query_arrow_batch_stream(
                &query,
                self.config.max_concurrent_readers(),
                self.config.number_mode,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                let message = format!("error executing Snowflake snapshot query: {e:#}");
                self.consumer
                    .update_connector_health(ConnectorHealth::unhealthy(&message));
                self.consumer.error(true, anyhow!(message), None);
                return;
            }
        };
        let query_id = result_stream
            .metadata
            .query_id
            .clone()
            .unwrap_or_else(|| "<unknown>".to_string());
        let total_rows = result_stream.metadata.total_rows;
        self.consumer
            .update_connector_health(ConnectorHealth::healthy());

        info!(
            "snowflake {}: reading snapshot with Snowflake query '{}' (rows: {})",
            &self.endpoint_name,
            query_id,
            total_rows
                .map(|rows| rows.to_string())
                .unwrap_or_else(|| "<unknown>".to_string())
        );

        let mut timestamp = Utc::now();
        let transaction_label = match self.config.transaction_mode {
            SnowflakeTransactionMode::None => None,
            SnowflakeTransactionMode::Snapshot => {
                Some(Some(format!("snowflake-snapshot:{query_id}")))
            }
        };
        let batches = result_stream.batches;
        let parser_receiver = receiver.clone();
        let parser_query_id = query_id.clone();
        let mut parsed_batches = batches
            .map(move |batch_result| {
                let batch_stream = input_stream.fork();
                let batch_query_id = parser_query_id.clone();
                let mut receiver = parser_receiver.clone();
                async move {
                    let batch = batch_result?;
                    wait_running(&mut receiver).await;
                    tokio::task::spawn_blocking(move || {
                        parse_arrow_batch(batch_stream, batch, &batch_query_id)
                    })
                    .await
                    .context("Snowflake Arrow batch parser task failed")
                }
            })
            .buffer_unordered(self.config.num_parsers as usize);

        while let Some(parsed_batch) = parsed_batches.next().await {
            wait_running(&mut receiver).await;
            let (buffer, errors) = match parsed_batch {
                Ok(parsed) => parsed,
                Err(error) => {
                    self.commit_transaction_on_error(transaction_label.is_some());
                    let message = format!("error processing Snowflake snapshot query: {error:#}");
                    self.consumer
                        .update_connector_health(ConnectorHealth::unhealthy(&message));
                    self.consumer.error(true, anyhow!(message), None);
                    return;
                }
            };

            let entry = InputQueueEntry::new_with_aux(timestamp, None)
                .with_buffer(buffer)
                .with_start_transaction(transaction_label.clone());

            self.queue.push_entry(entry, errors);
            timestamp = Utc::now();
        }

        let mut completion_entry =
            InputQueueEntry::new_with_aux(timestamp, Some(SnowflakeResumeInfo::eoi()));
        if transaction_label.is_some() {
            completion_entry = completion_entry
                .with_start_transaction(transaction_label)
                .with_commit_transaction(true);
        }
        self.queue.push_entry(completion_entry, Vec::new());

        info!(
            "snowflake {}: snapshot load completed (query: '{}')",
            &self.endpoint_name, query_id
        );

        self.consumer.eoi();
    }

    fn commit_transaction_on_error(&self, transaction_enabled: bool) {
        if transaction_enabled {
            // Feldera does not currently expose transaction rollback to connectors. Commit any
            // already queued records so a failed connector cannot leave the pipeline blocked.
            self.queue.push_entry(
                InputQueueEntry::new_with_aux(Utc::now(), None).with_commit_transaction(true),
                Vec::new(),
            );
        }
    }
}

fn parse_arrow_batch(
    mut input_stream: Box<dyn ArrowStream>,
    batch: RecordBatch,
    query_id: &str,
) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
    let errors = input_stream.insert(&batch, &None).map_or_else(
        |error| {
            vec![ParseError::bin_envelope_error(
                format!(
                    "error deserializing Snowflake Arrow batch from query '{query_id}': {error}"
                ),
                &[],
                None,
            )]
        },
        |()| Vec::new(),
    );
    (input_stream.take_all(), errors)
}

async fn wait_running(receiver: &mut Receiver<PipelineState>) {
    let _ = receiver
        .wait_for(|state| state == &PipelineState::Running)
        .await;
}
