use crate::db::types::common::Version;
use crate::db::types::pipeline::{ExtendedPipelineDescr, PipelineId};
use crate::db::types::program::generate_pipeline_config;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use crate::runner::logs_buffer::LogsBuffer;
use async_trait::async_trait;
use feldera_types::config::PipelineConfig;
use log::{debug, error};
use std::time::Duration;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::{select, spawn};

/// A log follow message, which either is a log line or a notification
/// that the log stream has ended due to a particular cause.
#[derive(Clone)]
pub enum LogMessage {
    Line(String),
    End(String),
}

/// A description of a pipeline to execute.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct PipelineExecutionDesc {
    pub pipeline_id: PipelineId,
    pub pipeline_name: String,
    pub program_version: Version,
    pub program_binary_url: String,
    pub deployment_config: PipelineConfig,
}

/// Trait to be implemented by any pipeline runner.
/// The `PipelineAutomaton` invokes these methods per pipeline.
#[async_trait]
pub trait PipelineExecutor: Sync + Send {
    /// Configuration unique to the runner type.
    type Config: Clone;

    // Timing constants, which should be set based on the runner type.
    const PROVISIONING_TIMEOUT: Duration;
    const PROVISIONING_POLL_PERIOD: Duration;
    const SHUTDOWN_TIMEOUT: Duration;
    const SHUTDOWN_POLL_PERIOD: Duration;

    // Logs buffer size limit constants.
    const LOGS_BUFFER_LIMIT_BYTE: usize = 1_000_000; // 1 MB
    const LOGS_BUFFER_LIMIT_NUM_LINES: usize = 50_000; // 50K lines

    /// Constructs a new runner for the provided pipeline and starts rejection logging.
    fn new(
        pipeline_id: PipelineId,
        config: Self::Config,
        follow_request_receiver: mpsc::Receiver<mpsc::Sender<LogMessage>>,
    ) -> Self;

    /// Converts an extended pipeline descriptor retrieved from the database
    /// into an execution descriptor which has no optional fields.
    async fn to_execution_desc(
        &self,
        pipeline: &ExtendedPipelineDescr,
    ) -> Result<PipelineExecutionDesc, ManagerError> {
        // Handle optional fields
        let (inputs, outputs) = match pipeline.program_info.clone() {
            None => {
                return Err(ManagerError::from(
                    RunnerError::PipelineMissingProgramInfo {
                        pipeline_name: pipeline.name.clone(),
                        pipeline_id: pipeline.id,
                    },
                ))
            }
            Some(program_info) => (
                program_info.input_connectors,
                program_info.output_connectors,
            ),
        };
        let program_binary_url = match pipeline.program_binary_url.clone() {
            None => {
                return Err(ManagerError::from(
                    RunnerError::PipelineMissingProgramBinaryUrl {
                        pipeline_name: pipeline.name.clone(),
                        pipeline_id: pipeline.id,
                    },
                ))
            }
            Some(program_binary_url) => program_binary_url,
        };

        let deployment_config =
            generate_pipeline_config(pipeline.id, &pipeline.runtime_config, &inputs, &outputs);
        Ok(PipelineExecutionDesc {
            pipeline_id: pipeline.id,
            pipeline_name: pipeline.name.clone(),
            program_version: pipeline.program_version,
            program_binary_url,
            deployment_config,
        })
    }

    /// Initializes internal state by reconnecting to a pipeline instance that is already started
    /// by a prior runner and is still potentially running. In particular, it should switch to
    /// operational logging if this is supported by the instance type.
    async fn init(&mut self, was_started: bool);

    /// Brings up some instance (e.g., process) that runs the pipeline binary and switches to operational logging.
    async fn start(&mut self, ped: PipelineExecutionDesc) -> Result<(), ManagerError>;

    /// Attempts to retrieve the hostname:port over which the pipeline's HTTP server should be
    /// reachable. `Ok(None)` indicates that the pipeline is still initializing.
    async fn get_location(&mut self) -> Result<Option<String>, ManagerError>;

    /// Shuts down the instance (e.g., send SIGTERM to the process) that is running the pipeline
    /// binary and switches to rejection logging.
    async fn shutdown(&mut self) -> Result<(), ManagerError>;

    /// Returns whether the instance (e.g., process) running the pipeline binary has been shutdown.
    async fn check_if_shutdown(&mut self) -> bool;

    /// Sets up a thread which replies to any log follow request with a rejection that the pipeline
    /// has not yet started.
    /// Returns a sender to invoke termination of the thread and the corresponding join handle.
    fn setup_log_rejection(
        pipeline_id: PipelineId,
        mut log_follow_request_receiver: mpsc::Receiver<mpsc::Sender<LogMessage>>,
    ) -> (
        oneshot::Sender<()>,
        JoinHandle<mpsc::Receiver<mpsc::Sender<LogMessage>>>,
    ) {
        let (terminate_sender, mut terminate_receiver) = oneshot::channel::<()>();
        let join_handle = spawn(async move {
            loop {
                select! {
                    // Terminate
                    _ = &mut terminate_receiver => {
                        debug!("Terminating rejection logging by request");
                        break;
                    }
                    // New followers immediately receive a rejection
                    follower = log_follow_request_receiver.recv() => {
                        if let Some(follower) = follower {
                            let _ = follower.send(LogMessage::End("LOG STREAM UNAVAILABLE: the pipeline has likely not yet started".to_string())).await;
                        } else {
                            // Log follow request sender itself closed, which happens when the pipeline is deleted.
                            // A pipeline should only be deleted when shutdown, and during shutdown the rejection thread is running.
                            // As such, this case is not erroneous.
                            debug!("Log follow request sender itself closed, which happens when the pipeline is deleted. Log rejection thread is ended for this reason.");
                            break;
                        }
                    }
                }
            }
            log_follow_request_receiver
        });
        debug!(
            "Logging request are being rejected for pipeline {}",
            pipeline_id
        );
        (terminate_sender, join_handle)
    }

    /// Terminates the log thread (either rejection of operational) by sending the termination
    /// message and joining the thread.
    /// Returns the log follow request receiver.
    async fn terminate_log_thread(
        terminate_sender: oneshot::Sender<()>,
        join_handle: JoinHandle<mpsc::Receiver<mpsc::Sender<LogMessage>>>,
    ) -> mpsc::Receiver<mpsc::Sender<LogMessage>> {
        // The terminate receiver might have been dropped already if there was an unrecoverable
        // error. If this occurred, the thread will have exited by itself. As such, it is not
        // needed to check whether send was a success or not.
        let _ = terminate_sender.send(());
        join_handle
            .await
            .expect("Unable to receive joined log thread result")
    }

    /// Catches up the follower by sending all the buffered logs to it.
    /// Afterward, adds it to the list of known followers if there was
    /// no error during sending the catch-up.
    async fn catch_up_and_add_follower(
        logs: &mut LogsBuffer,
        log_followers: &mut Vec<mpsc::Sender<LogMessage>>,
        follower: mpsc::Sender<LogMessage>,
    ) {
        // Catch up the new follower if there are any lines to catch up on
        let mut failed = false;

        // First line mentions the number of discarded lines due to the circular buffer
        if logs.num_discarded_lines() > 0 {
            let first_line = format!("... {} prior log lines were discarded due to buffer constraints and are thus not shown.", logs.num_discarded_lines());
            if let Err(e) = follower.try_send(LogMessage::Line(first_line)) {
                match e {
                    TrySendError::Full(_) => {
                        error!("Unable to catch up new follower because buffer is full, the follower will be dropped")
                    }
                    TrySendError::Closed(_) => {
                        debug!("Unable to catch up new follower because the receiver was already dropped")
                    }
                }
                failed = true;
            }
        }

        // Feed all the lines stored in the circular buffer
        if !failed {
            for line in logs.lines() {
                if let Err(e) = follower.try_send(LogMessage::Line(line.clone())) {
                    match e {
                        TrySendError::Full(_) => {
                            error!("Unable to catch up new follower because buffer is full, the follower will be dropped")
                        }
                        TrySendError::Closed(_) => {
                            debug!("Unable to catch up new follower because the receiver was already dropped")
                        }
                    }
                    failed = true;
                    break;
                }
            }
        }

        // Any failure in sending results in the follower Sender not being added
        // to the list, and thus going out of scope and being dropped.
        // The Receiver in that case will be notified no Sender exists anymore.
        if !failed {
            log_followers.push(follower);
        }
    }

    /// Process a new log line by adding it to the lines buffer and
    /// sending it out to all followers. Any followers that exhibit
    /// a send error are removed.
    async fn process_log_line_with_followers(
        logs: &mut LogsBuffer,
        log_followers: &mut Vec<mpsc::Sender<LogMessage>>,
        line: String,
    ) {
        // Add copy of line to buffer
        logs.append(line.clone());

        // Send to all followers the new line
        let mut retain_indexes = vec![];
        for (idx, follower) in log_followers.iter().enumerate() {
            match follower.try_send(LogMessage::Line(line.clone())) {
                Ok(()) => {
                    retain_indexes.push(idx);
                }
                Err(e) => match e {
                    TrySendError::Full(_) => {
                        // If the follower is unable to keep up, it will be removed.
                        // There exists a buffer to give a follower the chance to catch up.
                        // However, if the limit of the buffer is reached and thus unable to send new,
                        // the log follower will be removed to prevent it from slowing down the rest.
                        error!("Unable to send log line to follower because buffer is full: the follower will be removed")
                    }
                    TrySendError::Closed(_) => {
                        debug!("Unable to send log line to follower because the receiver was already dropped: the follower will be removed")
                    }
                },
            }
        }

        // Only keep the followers to who we were able to send the new line
        let mut idx: usize = 0;
        log_followers.retain(|_follower| {
            let keep = retain_indexes.contains(&idx);
            idx += 1;
            keep
        });

        // Any Senders that were not retained will go out of scope, which
        // results in them being dropped and the Receiver being notified
        // no Sender exists anymore.
    }

    /// End the log of all followers with a message explaining the reason.
    async fn end_log_of_followers(
        log_followers: &mut Vec<mpsc::Sender<LogMessage>>,
        end_message: LogMessage,
    ) {
        for follower in log_followers.iter() {
            if let Err(e) = follower.try_send(end_message.clone()) {
                match e {
                    TrySendError::Full(_) => {
                        error!("Unable to send ending log line to follower because buffer is full")
                    }
                    TrySendError::Closed(_) => {
                        debug!("Unable to send ending log line to follower because the receiver was already dropped")
                    }
                }
            }
        }
        log_followers.clear();

        // All Senders were cleared and will go out of scope, which
        // results in them being dropped and the Receivers being notified
        // no Senders exists anymore.
    }
}
