use crate::db::types::pipeline::PipelineId;
use crate::db::types::version::Version;
use crate::error::ManagerError;
use crate::runner::logs_buffer::LogsBuffer;
use async_trait::async_trait;
use chrono::SecondsFormat;
use feldera_types::config::{PipelineConfig, StorageConfig};
use log::{debug, error, Level};
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

/// Text shown at the end of logs upon shutdown.
pub const LOGS_END_MESSAGE: &str = "End of logs: pipeline is being shutdown";

/// Trait to be implemented by any pipeline runner.
/// The `PipelineAutomaton` invokes these methods per pipeline.
#[async_trait]
pub trait PipelineExecutor: Sync + Send {
    /// Configuration unique to the runner type.
    type Config: Clone;

    /// Timeout for the `Provisioning` stage in which resources are provisioned.
    const DEFAULT_PROVISIONING_TIMEOUT: Duration;

    // Logs buffer size limit constants.
    const LOGS_BUFFER_LIMIT_BYTE: usize = 1_000_000; // 1 MB
    const LOGS_BUFFER_LIMIT_NUM_LINES: usize = 50_000; // 50K lines

    /// Constructs a new runner for the provided pipeline and starts rejection logging.
    fn new(
        pipeline_id: PipelineId,
        config: Self::Config,
        client: reqwest::Client,
        follow_request_receiver: mpsc::Receiver<mpsc::Sender<LogMessage>>,
    ) -> Self;

    /// Generates the storage configuration of the pipeline if storage is enabled.
    /// The storage configuration is part of the pipeline deployment configuration.
    async fn generate_storage_config(&self) -> StorageConfig;

    /// Initializes any runner internal state. In particular, reconnects with
    /// pipeline resources provisioned by a prior runner, including switching
    /// to operational logging.
    async fn init(&mut self, was_provisioned: bool);

    /// Provisions resources required for the pipeline to run.
    /// The provisioned resources must be uniquely identifiable/addressable through the
    /// pipeline identifier, such that `shutdown()` without any other state is able to
    /// delete them. The backing storage must be mounted at the storage directory
    /// specified earlier by `generate_storage_config()` and be empty. Calls to
    /// `provision()` must be idempotent as it can be called again if the runner is
    /// unexpectedly restarted during provisioning.
    ///
    /// The implementation should be as non-blocking as possible -- resources which might take
    /// a long time should have their provisioning initiated, and completion validation done
    /// within `is_provisioned()`. This enables a user to swiftly shut down a provisioning pipeline.
    async fn provision(
        &mut self,
        deployment_config: &PipelineConfig,
        program_binary_url: &str,
        program_version: Version,
    ) -> Result<(), ManagerError>;

    /// Validates whether the provisioning initiated by `provision()` is completed.
    /// Returns the following:
    /// - `Ok(Some(deployment_location))` if provisioning completed successfully
    /// - `Ok(None)` if provisioning is still ongoing
    /// - `Err(...)` if provisioning failed
    async fn is_provisioned(&mut self) -> Result<Option<String>, ManagerError>;

    /// Checks the pipeline.
    /// Returns an error if the provisioned resources encountered a fatal error.
    async fn check(&mut self) -> Result<(), ManagerError>;

    /// Terminates and deletes provisioned resources (including storage),
    /// and switches to rejection logging.
    async fn shutdown(&mut self) -> Result<(), ManagerError>;

    /// Sets up a thread which replies to any log follow request with a rejection that the pipeline
    /// has not yet started.
    /// Returns a sender to invoke termination of the thread and the corresponding join handle.
    #[allow(clippy::type_complexity)]
    fn setup_log_rejection(
        pipeline_id: PipelineId,
        mut log_follow_request_receiver: mpsc::Receiver<mpsc::Sender<LogMessage>>,
        mut log_deployment_check_receiver: mpsc::Receiver<String>,
    ) -> (
        oneshot::Sender<()>,
        JoinHandle<(
            mpsc::Receiver<mpsc::Sender<LogMessage>>,
            mpsc::Receiver<String>,
        )>,
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
                    // New deployment check lines are ignored
                    line = log_deployment_check_receiver.recv() => {
                        if line.is_none() {
                            debug!("Log deployment check sender itself terminated. Log rejection thread is ended for this reason.");
                            break;
                        }
                    }
                }
            }
            (log_follow_request_receiver, log_deployment_check_receiver)
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
        join_handle: JoinHandle<(
            mpsc::Receiver<mpsc::Sender<LogMessage>>,
            mpsc::Receiver<String>,
        )>,
    ) -> (
        mpsc::Receiver<mpsc::Sender<LogMessage>>,
        mpsc::Receiver<String>,
    ) {
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

    /// Formats a control plane log line including the tag, timestamp and level.
    fn format_control_plane_log_line(level: Level, message: &str) -> String {
        let timestamp = chrono::Utc::now();
        format!(
            // This is in line with the tracing format printed by the pipeline itself.
            "[control-plane] {}  {level} {message}",
            timestamp.to_rfc3339_opts(SecondsFormat::Micros, true)
        )
    }
}
