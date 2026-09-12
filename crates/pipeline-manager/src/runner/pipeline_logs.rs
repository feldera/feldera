use chrono::{SecondsFormat, Utc};
use feldera_observability::json_logging::use_json_log_format;
use serde_json::json;
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio::{select, spawn};
use tracing::{Level, debug, error, warn};
use uuid::Uuid;

// Logs buffer size limit constants.
const LOGS_BUFFER_LIMIT_BYTE: usize = 1_000_000; // 1 MB
const LOGS_BUFFER_LIMIT_NUM_LINES: usize = 50_000; // 50K lines

// Timeout to send to the pipeline logs.
const SEND_LOG_MESSAGE_TIMEOUT: Duration = Duration::from_millis(100);

// Number of time to try sending the logs message before giving up.
const SEND_LOG_MESSAGE_TRIES: u64 = 100;

/// Response headers naming a resuming follower's position in the log stream.
///
/// The position travels beside the body rather than inside it, so a cursor-aware
/// follower's body holds log lines and nothing else. Sharing the body with control
/// information would leave every reader guessing which is which, and a log line is free to
/// look like anything, including whatever shape the control information takes.
pub const LOGS_EPOCH_HEADER: &str = "feldera-logs-epoch";
pub const LOGS_SEQ_HEADER: &str = "feldera-logs-seq";
pub const LOGS_GAP_HEADER: &str = "feldera-logs-gap";

/// Position in a pipeline's log stream, presented by a follower that wants to resume
/// where a previous connection left off.
///
/// Rendered as `<epoch>:<sequence>`. The epoch identifies one lifetime of a
/// [`LogsBuffer`], which is what gives the sequence number meaning: the buffer is held
/// only in memory, so a runner restart resets numbering to zero while followers still
/// hold cursors issued by the previous instance. Comparing identity rather than counter
/// values is what stops such a cursor from being accepted against unrelated lines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogCursor {
    /// Identifies the buffer instance that issued the sequence number.
    pub epoch: Uuid,
    /// Sequence number of the last line the follower received. Zero means none.
    pub seq: u64,
}

impl Display for LogCursor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.epoch, self.seq)
    }
}

impl FromStr for LogCursor {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (epoch, seq) = s
            .split_once(':')
            .ok_or_else(|| format!("expected the form <epoch>:<sequence>, got '{s}'"))?;
        Ok(LogCursor {
            epoch: Uuid::parse_str(epoch)
                .map_err(|e| format!("epoch '{epoch}' is not a valid UUID: {e}"))?,
            seq: seq
                .parse()
                .map_err(|e| format!("sequence '{seq}' is not a valid number: {e}"))?,
        })
    }
}

/// What a follower asks to receive when it connects.
pub enum FollowMode {
    /// The entire retained buffer, preceded in-band by a notice naming how many lines
    /// were discarded. Selected by followers that do not speak the cursor protocol.
    Full,
    /// Only the lines the follower is missing, preceded by a [`FollowerMessage::Resume`]
    /// naming the epoch, the position the catch-up starts at, and any lines lost along the
    /// way. `None` is a first connection, which has no position yet.
    Resume(Option<LogCursor>),
}

/// A message delivered to a single follower.
pub enum FollowerMessage {
    /// Where the follower's catch-up starts. Sent once, ahead of every line, and only to
    /// a follower that asked to resume. The HTTP handler consumes it and answers with the
    /// [`LOGS_EPOCH_HEADER`], [`LOGS_SEQ_HEADER`] and [`LOGS_GAP_HEADER`] response headers.
    Resume { epoch: Uuid, seq: u64, gap: u64 },
    /// One log line.
    Line(String),
}

/// Request to follow a pipeline's logs.
pub struct FollowRequest {
    /// Channel over which the follower's messages are delivered.
    pub sender: mpsc::Sender<FollowerMessage>,
    /// What the follower asks to receive.
    pub mode: FollowMode,
}

/// A follower that has completed catch-up, and can therefore only receive log lines.
///
/// Registering a follower converts its sender into one of these, which is what makes
/// sending a [`FollowerMessage::Resume`] to a registered follower unrepresentable rather
/// than merely incorrect: the position describes the lines that follow it, so one arriving
/// mid-stream would describe nothing.
pub struct LineSender(mpsc::Sender<FollowerMessage>);

impl LineSender {
    fn try_send_line(&self, line: String) -> Result<(), TrySendError<FollowerMessage>> {
        self.0.try_send(FollowerMessage::Line(line))
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

/// Where a resuming follower's catch-up starts, as resolved by
/// [`LogsBuffer::resume_from`].
#[derive(Debug, PartialEq, Eq)]
pub struct Resume {
    /// Sequence number of the line preceding the first line the follower receives.
    pub seq: u64,
    /// Lines that were evicted between the requested cursor and `seq`, and which the
    /// follower will therefore never see. Zero means the resume is exact.
    pub gap: u64,
    /// Lines to skip from the front of the buffer to reach the first line to send.
    pub skip: usize,
}

#[derive(Clone)]
pub enum LogMessage {
    Pipeline {
        line: String,
    },
    ControlPlane {
        target: &'static str,
        service: &'static str,
        pipeline_name: String,
        pipeline_id: String,
        level: Level,
        line: String,
        timestamp: String,
    },
}

impl LogMessage {
    /// Constructs a log message for a line generated by the pipeline itself.
    /// The pipeline log line already contains a timestamp and level.
    pub fn new_from_pipeline(line: &str) -> LogMessage {
        LogMessage::Pipeline {
            line: line.to_string(),
        }
    }

    /// Constructs a log message for a line generated by the runner (automaton or executor).
    pub fn new_from_control_plane(
        target: &'static str,
        service: &'static str,
        pipeline_name: impl Into<String>,
        pipeline_id: impl Into<String>,
        level: Level,
        line: &str,
    ) -> LogMessage {
        LogMessage::ControlPlane {
            target,
            service,
            pipeline_name: pipeline_name.into(),
            pipeline_id: pipeline_id.into(),
            level,
            line: line.to_string(),
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true),
        }
    }
}

/// Sets up a thread which listens to follow requests and new incoming log lines.
/// New followers are caught up and existing followers receive new lines as they come in.
/// Returns a termination sender and the corresponding join handle.
#[allow(clippy::type_complexity)]
pub fn start_thread_pipeline_logs(
    pipeline_id: impl Into<String>,
    pipeline_name: impl Into<String>,
    mut follow_request_receiver: mpsc::Receiver<FollowRequest>,
    mut logs_receiver: mpsc::Receiver<LogMessage>,
) -> (oneshot::Sender<()>, JoinHandle<()>) {
    let pipeline_id = pipeline_id.into();
    let pipeline_name = pipeline_name.into();
    let (terminate_sender, mut terminate_receiver) = oneshot::channel::<()>();
    let join_handle = spawn(async move {
        // Identifies this instance of the buffer. The buffer is held only in memory, so a
        // runner restart resets line numbering to zero while followers still hold cursors
        // from the previous instance. Comparing identity rather than counter values is what
        // stops such a cursor from being accepted against unrelated lines.
        let epoch = Uuid::now_v7();

        // Buffer with the latest lines
        let mut logs = LogsBuffer::new(LOGS_BUFFER_LIMIT_BYTE, LOGS_BUFFER_LIMIT_NUM_LINES);

        // First line
        logs.append(format_log_line(&LogMessage::new_from_control_plane(
            module_path!(),
            "runner",
            pipeline_name.clone(),
            pipeline_id.clone(),
            Level::INFO,
            "Fresh start of pipeline logs",
        )));

        // Followers interested in receiving the logs
        let mut log_followers: Vec<LineSender> = Vec::new();

        // Normally dead followers are dropped when we receive new message.
        // This just helps in cleanup without the need for new message.
        // Hence, we can just do it if idle for 5 mins, as in ideal case,
        // there would be new messages before it.
        let mut idle_interval_for_cleanup = interval(Duration::from_secs(300));

        loop {
            select! {
                // Termination request
                _ = &mut terminate_receiver => {
                    break;
                }

                // Follow request
                follower = follow_request_receiver.recv() => {
                    if let Some(follower) = follower {
                        catch_up_and_add_follower(
                            &pipeline_id,
                            &pipeline_name,
                            epoch,
                            &logs,
                            &mut log_followers,
                            follower,
                        )
                        .await;
                    } else {
                        // The follow request sender has been dropped, which occurs when the pipeline is deleted.
                        // In this case, the logs thread is also terminated.
                        break;
                    }
                }

                // Log message
                message = logs_receiver.recv() => {
                    match message {
                        Some(message) => {
                            let line = format_log_line(&message);
                            process_log_line_with_followers(
                                &mut logs,
                                &mut log_followers,
                                line
                            ).await;
                        },
                        None => {
                            // All logs senders have been dropped, which can occur when the pipeline is deleted.
                            // In this case, the logs thread is also terminated.
                            break;
                        }
                    }
                }

                _ = idle_interval_for_cleanup.tick() => {
                    // drop the dead followers
                    log_followers.retain(|follower| !follower.is_closed());
                }
            }
        }
    });
    (terminate_sender, join_handle)
}

/// Catches up the follower by sending it the buffered logs it is missing.
/// Afterward, adds it to the list of known followers if there was
/// no error during sending the catch-up.
async fn catch_up_and_add_follower(
    pipeline_id: &str,
    pipeline_name: &str,
    epoch: Uuid,
    logs: &LogsBuffer,
    log_followers: &mut Vec<LineSender>,
    request: FollowRequest,
) {
    let FollowRequest {
        sender: new_follower,
        mode,
    } = request;

    // Catch up the new follower if there are any lines to catch up on
    let mut failed = false;

    // How much of the buffer the follower still needs, and what to tell it first.
    let (skip, opening_message) = match mode {
        FollowMode::Full => {
            // The notice names the number of lines discarded due to the circular buffer.
            // Tag as control-plane metadata so it is formatted (text or JSON) consistently
            // with other runner messages.
            let notice = (logs.num_discarded_lines() > 0).then(|| {
                FollowerMessage::Line(format_log_line(&LogMessage::new_from_control_plane(
                    module_path!(),
                    "runner",
                    pipeline_name.to_string(),
                    pipeline_id.to_string(),
                    Level::WARN,
                    &format!(
                        "... {} prior log lines were discarded due to buffer constraints and are thus not shown.",
                        logs.num_discarded_lines()
                    ),
                )))
            });
            (0, notice)
        }
        FollowMode::Resume(cursor) => {
            // Sent ahead of the lines it describes, on the same channel, so the follower's
            // position cannot arrive after the lines it is meant to number. Every message
            // that follows is a log line, which is what lets the follower derive its next
            // cursor by counting rather than by inspecting what it receives.
            let resume = logs.resume_from(epoch, cursor);
            (
                resume.skip,
                Some(FollowerMessage::Resume {
                    epoch,
                    seq: resume.seq,
                    gap: resume.gap,
                }),
            )
        }
    };

    if let Some(message) = opening_message
        && let Err(e) = new_follower.try_send(message)
    {
        match e {
            TrySendError::Full(_) => {
                error!(
                    "Unable to catch up new follower because buffer is full, the follower will be dropped"
                );
            }
            TrySendError::Closed(_) => {}
        }
        failed = true;
    }

    // Feed the lines the follower is missing from the circular buffer
    if !failed {
        for line in logs.lines().iter().skip(skip) {
            if let Err(e) = new_follower.try_send(FollowerMessage::Line(line.clone())) {
                match e {
                    TrySendError::Full(_) => {
                        error!(
                            "Unable to catch up new follower because buffer is full, the follower will be dropped"
                        )
                    }
                    TrySendError::Closed(_) => {}
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
        log_followers.push(LineSender(new_follower));
    }
}

/// Process a new log line by adding it to the lines buffer and
/// sending it out to all followers. Any followers that exhibit
/// a send error are removed.
async fn process_log_line_with_followers(
    logs: &mut LogsBuffer,
    log_followers: &mut Vec<LineSender>,
    line: String,
) {
    // Add copy of line to buffer
    logs.append(line.clone());

    // Send to all followers the new line
    let mut retain_indexes = vec![];
    for (idx, follower) in log_followers.iter().enumerate() {
        match follower.try_send_line(line.clone()) {
            Ok(()) => {
                retain_indexes.push(idx);
            }
            Err(e) => match e {
                TrySendError::Full(_) => {
                    // If the follower is unable to keep up, it will be removed.
                    // There exists a buffer to give a follower the chance to catch up.
                    // However, if the limit of the buffer is reached and thus unable to send new,
                    // the log follower will be removed to prevent it from slowing down the rest.
                    error!(
                        "Unable to send log line to follower because buffer is full: the follower will be removed"
                    )
                }
                TrySendError::Closed(_) => {}
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

fn format_log_line(message: &LogMessage) -> String {
    if !use_json_log_format() {
        return match message {
            LogMessage::Pipeline { line } => line.clone(),
            LogMessage::ControlPlane {
                target,
                service: _,
                pipeline_name,
                pipeline_id,
                level,
                line,
                timestamp,
            } => {
                let display_name = if pipeline_name.is_empty() {
                    "N/A"
                } else {
                    pipeline_name.as_str()
                };
                let mut output = format!("[manager] {timestamp} {level:>5} {target}: {line}");
                if !pipeline_id.is_empty() {
                    output.push_str(&format!(" pipeline-id={pipeline_id:?}"));
                }
                if !display_name.is_empty() {
                    output.push_str(&format!(" pipeline-name={display_name:?}"));
                }
                output
            }
        };
    }

    match message {
        // Pipeline lines are already formatted, forward as-is
        LogMessage::Pipeline { line } => line.clone(),
        LogMessage::ControlPlane {
            target,
            service,
            pipeline_name,
            pipeline_id,
            level,
            line,
            timestamp,
        } => json!({
            "timestamp": timestamp,
            "level": level.as_str(),
            "target": target,
            "feldera-service": service,
            "pipeline-name": pipeline_name,
            "pipeline-id": pipeline_id,
            "fields": { "line": line },
        })
        .to_string(),
    }
}

/// Wrapper around the logs sender channel, which gracefully handles inability to send a message.
#[derive(Clone)]
pub struct LogsSender {
    sender: mpsc::Sender<LogMessage>,
}

impl LogsSender {
    pub fn new(sender: mpsc::Sender<LogMessage>) -> Self {
        Self { sender }
    }

    pub async fn send(&mut self, mut message: LogMessage) {
        // This will momentarily block when the receiver buffer is full. This should generally not
        // happen as the receiving thread continuously listens for new log messages and puts them
        // into a circular buffer. It retries a set amount of times with a timeout inbetween before
        // giving up.
        for i in 1..=SEND_LOG_MESSAGE_TRIES {
            message = match self
                .sender
                .send_timeout(message, SEND_LOG_MESSAGE_TIMEOUT)
                .await
            {
                Ok(()) => {
                    // Successfully sent
                    return;
                }
                Err(e) => match e {
                    SendTimeoutError::Timeout(unsent_message) => {
                        warn!(
                            "Unable to send logs message because receiver buffer is full -- trying again in {}ms (attempt {} / {})",
                            SEND_LOG_MESSAGE_TIMEOUT.as_millis(),
                            i,
                            SEND_LOG_MESSAGE_TRIES
                        );
                        unsent_message
                    }
                    SendTimeoutError::Closed(_) => {
                        debug!(
                            "Unable to send logs message because receiver is closed -- this can happen when the pipeline is deleted"
                        );
                        return;
                    }
                },
            }
        }
        error!(
            "Unable to send logs message after attempting {SEND_LOG_MESSAGE_TRIES} times -- receiver buffer is consistently full: message (byte length: {}) is dropped",
            format_log_line(&message).len()
        )
    }
}

/// The LogsBuffer maintains internally a circular buffer of Strings whose
/// size in byte and number of elements does not exceed the limits.
/// When appending new log lines (Strings) to the buffer, the limits are
/// enforced by discarding existing lines if the limits would be exceeded.
pub struct LogsBuffer {
    /// Buffer size limit in byte.
    size_limit_byte: usize,
    /// Buffer size limit in number of lines.
    size_limit_num_lines: usize,
    /// The lines buffer.
    buffer: VecDeque<String>,
    /// Current lines buffer size.
    size_byte: usize,
    /// Number of lines that have been discarded to enforce size limit.
    num_discarded_lines: usize,
    /// Number of lines appended over the lifetime of this buffer, which is also the
    /// sequence number of the most recently appended line. Lines are numbered from one.
    num_appended_lines: u64,
}

impl LogsBuffer {
    /// Construct a new logs buffer.
    pub fn new(size_limit_byte: usize, size_limit_num_lines: usize) -> Self {
        Self {
            size_limit_byte,
            size_limit_num_lines,
            buffer: VecDeque::new(),
            size_byte: 0,
            num_discarded_lines: 0,
            num_appended_lines: 0,
        }
    }

    /// Append a new line to the buffer.
    ///
    /// A message spanning several lines is stored as one entry per line. Followers receive
    /// the buffer as newline-terminated entries, so a retained embedded newline would put
    /// two lines on the wire under one sequence number and leave every cursor derived from
    /// that count pointing one line short of where the follower believes it is. Splitting
    /// gives each line its own number and sends the same bytes as before.
    ///
    /// Control-plane messages interpolate arbitrary `Display` values, notably error chains,
    /// which is where multi-line messages come from.
    pub fn append(&mut self, line: String) {
        if line.contains('\n') {
            for single_line in line.split('\n') {
                self.append_single_line(single_line.to_string());
            }
        } else {
            self.append_single_line(line);
        }
    }

    /// Appends one line, which must not contain a newline.
    /// - If the new line exceeds the buffer size limit by itself, all the lines in the buffer and
    ///   the new line are discarded, leaving an empty buffer.
    /// - Otherwise, lines are removed from the buffer until the new line will fit. Once there
    ///   is sufficient space, the line is added to the buffer.
    fn append_single_line(&mut self, line: String) {
        debug_assert!(!line.contains('\n'));
        // Counted on every path, including the ones that drop the line, so that a line's
        // sequence number is fixed the moment it arrives and stays valid after eviction.
        self.num_appended_lines += 1;

        if line.len() > self.size_limit_byte {
            self.num_discarded_lines += self.buffer.len() + 1;
            self.buffer.clear();
            self.size_byte = 0;
        } else {
            // Ensure size in byte is not exceeded
            while self.size_byte + line.len() > self.size_limit_byte {
                let popped_line = self
                    .buffer
                    .pop_front()
                    .expect("Cannot remove log line even though size is non-zero");
                self.size_byte -= popped_line.len();
                self.num_discarded_lines += 1;
            }

            // Ensure size in number of lines is not exceeded
            if self.size_limit_num_lines > 0 {
                while self.buffer.len() + 1 > self.size_limit_num_lines {
                    let popped_line = self
                        .buffer
                        .pop_front()
                        .expect("Cannot remove log line even though length is non-zero");
                    self.size_byte -= popped_line.len();
                    self.num_discarded_lines += 1;
                }
                self.size_byte += line.len();
                self.buffer.push_back(line);
            }
        }
    }

    /// Retrieves the lines in the buffer.
    pub fn lines(&self) -> &VecDeque<String> {
        &self.buffer
    }

    /// Retrieves the number of lines in the buffer.
    pub fn num_lines(&self) -> usize {
        self.buffer.len()
    }

    /// Retrieves the number of lines discarded due to buffer limit enforcement.
    pub fn num_discarded_lines(&self) -> usize {
        self.num_discarded_lines
    }

    /// Retrieves the number of lines appended over the lifetime of the buffer, which is
    /// the sequence number of the most recently appended line.
    pub fn num_appended_lines(&self) -> u64 {
        self.num_appended_lines
    }

    /// Retrieves the sequence number of the oldest retained line. An empty buffer answers
    /// with the number the next appended line will receive.
    ///
    /// Derived from the append counter rather than from `num_discarded_lines`, which does
    /// not count a line dropped because the buffer permits no lines at all.
    pub fn first_seq(&self) -> u64 {
        self.num_appended_lines - self.buffer.len() as u64 + 1
    }

    /// Resolves where a follower's catch-up starts.
    ///
    /// A cursor is honored only when it belongs to this instance of the buffer, and only
    /// as far as the end of the stream. One carrying any other epoch is treated as no
    /// cursor at all, which yields a full catch-up with the lines discarded so far
    /// reported as the gap. Every way a cursor can be unusable therefore degrades to
    /// sending more than asked for, never to silently sending less.
    pub fn resume_from(&self, epoch: Uuid, cursor: Option<LogCursor>) -> Resume {
        let requested = match cursor {
            Some(cursor) if cursor.epoch == epoch => cursor.seq,
            _ => 0,
        }
        // A position past the end of the stream cannot be honored: echoing it back would
        // answer every reconnect with nothing at all and no gap to explain it, since the
        // follower would keep presenting the same unreachable position. Read such a cursor
        // as a request for the end of the stream, which the next line then follows.
        .min(self.num_appended_lines);
        // Sequence number of the line preceding the oldest retained one.
        let before_first = self.first_seq() - 1;
        let seq = requested.max(before_first);
        Resume {
            seq,
            gap: seq - requested,
            // Equals the buffer length for a follower that is already caught up, which
            // sends nothing rather than replaying lines the follower claims to hold.
            skip: (seq - before_first) as usize,
        }
    }

    /// Retrieves the total buffer size.
    pub fn size_byte(&self) -> usize {
        self.size_byte
    }

    /// Retrieves the buffer size limit in byte.
    pub fn size_limit_byte(&self) -> usize {
        self.size_limit_byte
    }

    /// Retrieves the buffer size limit in number of lines.
    pub fn size_limit_num_lines(&self) -> usize {
        self.size_limit_num_lines
    }
}

#[cfg(test)]
mod test {
    use super::{
        FollowMode, FollowRequest, FollowerMessage, LOGS_BUFFER_LIMIT_BYTE, LogCursor, LogMessage,
        LogsBuffer, Resume, start_thread_pipeline_logs,
    };
    use std::collections::VecDeque;
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use uuid::Uuid;

    #[test]
    fn log_cursor_round_trip() {
        let cursor = LogCursor {
            epoch: Uuid::from_u128(0x0199c3f12d0a7e84b7116f2c9a1d4e08),
            seq: 41272,
        };
        assert_eq!(
            cursor.to_string(),
            "0199c3f1-2d0a-7e84-b711-6f2c9a1d4e08:41272"
        );
        assert_eq!(LogCursor::from_str(&cursor.to_string()), Ok(cursor));

        // Anything a client could not have received from a position header is rejected outright,
        // rather than being silently reinterpreted as a different position.
        for malformed in [
            "",
            "41272",
            "0199c3f1-2d0a-7e84-b711-6f2c9a1d4e08",
            "not-a-uuid:1",
            "0199c3f1-2d0a-7e84-b711-6f2c9a1d4e08:",
            "0199c3f1-2d0a-7e84-b711-6f2c9a1d4e08:-1",
            "0199c3f1-2d0a-7e84-b711-6f2c9a1d4e08:1.5",
        ] {
            assert!(
                LogCursor::from_str(malformed).is_err(),
                "cursor '{malformed}' should not parse"
            );
        }
    }

    #[test]
    fn logs_buffer_sequence_numbers() {
        // Buffer with 20 byte and 5 lines limit
        let mut buffer = LogsBuffer::new(20, 5);
        assert_eq!(buffer.num_appended_lines(), 0);
        assert_eq!(buffer.first_seq(), 1);

        for line in ["abcde", "fghij", "klmno", "pqrst"] {
            buffer.append(line.to_string());
        }
        assert_eq!(buffer.num_appended_lines(), 4);
        assert_eq!(buffer.first_seq(), 1);

        // Exceeding the byte limit evicts the two oldest lines
        buffer.append("uvwxyz1".to_string());
        assert_eq!(buffer.num_appended_lines(), 5);
        assert_eq!(buffer.num_lines(), 3);
        assert_eq!(buffer.first_seq(), 3);

        // Exceeding the line limit evicts one more
        buffer.append("2".to_string());
        buffer.append("3".to_string());
        buffer.append("4".to_string());
        assert_eq!(buffer.num_appended_lines(), 8);
        assert_eq!(buffer.num_lines(), 5);
        assert_eq!(buffer.first_seq(), 4);

        // A line too large for the buffer discards everything, itself included
        buffer.append("aaaaabbbbbcccccddddde".to_string());
        assert_eq!(buffer.num_appended_lines(), 9);
        assert_eq!(buffer.num_lines(), 0);
        assert_eq!(buffer.first_seq(), 10);
    }

    /// A buffer that retains nothing still numbers what passes through it. The append
    /// counter is what makes this hold: `num_discarded_lines` does not count these lines.
    #[test]
    fn logs_buffer_sequence_numbers_without_retention() {
        let mut buffer = LogsBuffer::new(1000, 0);
        for i in 1..=4 {
            buffer.append(format!("line {i}"));
        }
        assert_eq!(buffer.num_lines(), 0);
        assert_eq!(buffer.num_appended_lines(), 4);
        assert_eq!(buffer.first_seq(), 5);

        let epoch = Uuid::now_v7();
        assert_eq!(
            buffer.resume_from(epoch, Some(LogCursor { epoch, seq: 1 })),
            Resume {
                seq: 4,
                gap: 3,
                skip: 0
            }
        );
    }

    #[test]
    fn logs_buffer_resume_from() {
        let epoch = Uuid::now_v7();
        let other_epoch = Uuid::now_v7();
        let mut buffer = LogsBuffer::new(1000, 5);
        for i in 1..=8 {
            buffer.append(format!("line {i}"));
        }
        // Lines 4 through 8 are retained; 1 through 3 were evicted.
        assert_eq!(buffer.num_lines(), 5);
        assert_eq!(buffer.first_seq(), 4);

        let at = |seq| buffer.resume_from(epoch, Some(LogCursor { epoch, seq }));

        // Resuming at the oldest retained boundary sends the whole buffer
        assert_eq!(
            at(3),
            Resume {
                seq: 3,
                gap: 0,
                skip: 0
            }
        );
        // Resuming from inside the buffer sends only what follows the cursor
        assert_eq!(
            at(6),
            Resume {
                seq: 6,
                gap: 0,
                skip: 3
            }
        );
        // A follower that is already caught up receives nothing
        assert_eq!(
            at(8),
            Resume {
                seq: 8,
                gap: 0,
                skip: 5
            }
        );
        // A cursor whose lines were evicted gets the whole buffer and a gap naming the loss
        assert_eq!(
            at(1),
            Resume {
                seq: 3,
                gap: 2,
                skip: 0
            }
        );
        // A cursor past the end of the stream is answered as if it named the end: the
        // follower receives nothing now and the lines that follow on its next connection,
        // instead of being held at a position the buffer can never reach.
        for beyond_end in [9, 99, u64::MAX] {
            assert_eq!(
                at(beyond_end),
                Resume {
                    seq: 8,
                    gap: 0,
                    skip: 5
                },
                "cursor at {beyond_end} should resolve to the end of the stream"
            );
        }
        // A first connection has no position, so the gap names every line discarded so far
        assert_eq!(
            buffer.resume_from(epoch, None),
            Resume {
                seq: 3,
                gap: 3,
                skip: 0
            }
        );
        // A cursor issued by another instance of the buffer is discarded rather than
        // trusted: its sequence numbers refer to lines this buffer never held.
        assert_eq!(
            buffer.resume_from(
                epoch,
                Some(LogCursor {
                    epoch: other_epoch,
                    seq: 6
                })
            ),
            Resume {
                seq: 3,
                gap: 3,
                skip: 0
            }
        );
    }

    /// A multi-line message occupies as many sequence numbers as it occupies lines on the
    /// wire, so a follower counting received lines stays aligned with the buffer.
    #[test]
    fn logs_buffer_splits_multi_line_messages() {
        let epoch = Uuid::now_v7();
        let mut buffer = LogsBuffer::new(1000, 10);
        buffer.append("first".to_string());
        buffer.append("panicked at src/lib.rs:1:\nstack backtrace:\n   0: main".to_string());
        buffer.append("last".to_string());

        assert_eq!(
            buffer.lines(),
            &VecDeque::from([
                "first".to_string(),
                "panicked at src/lib.rs:1:".to_string(),
                "stack backtrace:".to_string(),
                "   0: main".to_string(),
                "last".to_string(),
            ])
        );
        assert_eq!(buffer.num_appended_lines(), 5);

        // A follower that received the first four lines resumes exactly at the fifth,
        // which is what its own count of received lines names.
        assert_eq!(
            buffer.resume_from(epoch, Some(LogCursor { epoch, seq: 4 })),
            Resume {
                seq: 4,
                gap: 0,
                skip: 4
            }
        );

        // A trailing newline is a line of its own, matching the empty line it puts on the
        // wire once every entry is newline-terminated.
        buffer.append("trailing\n".to_string());
        assert_eq!(buffer.num_appended_lines(), 7);
        assert_eq!(
            buffer.lines().iter().rev().take(2).collect::<Vec<_>>(),
            vec!["", "trailing"]
        );
    }

    /// Reads one message from a follower, failing rather than hanging when none arrives.
    async fn recv_message(receiver: &mut mpsc::Receiver<FollowerMessage>) -> FollowerMessage {
        tokio::time::timeout(Duration::from_secs(10), receiver.recv())
            .await
            .expect("timed out waiting for a follower message")
            .expect("follower channel was closed")
    }

    /// Reads one log line, failing if the follower delivered anything else.
    async fn recv_line(receiver: &mut mpsc::Receiver<FollowerMessage>) -> String {
        match recv_message(receiver).await {
            FollowerMessage::Line(line) => line,
            FollowerMessage::Resume { seq, gap, .. } => {
                panic!("expected a log line, received a position (seq {seq}, gap {gap})")
            }
        }
    }

    /// Asserts a follower has nothing further to deliver.
    async fn assert_no_more_lines(receiver: &mut mpsc::Receiver<FollowerMessage>) {
        assert!(
            tokio::time::timeout(Duration::from_millis(200), receiver.recv())
                .await
                .is_err(),
            "follower received more lines than expected"
        );
    }

    fn follow(
        follow_request_sender: &mpsc::Sender<FollowRequest>,
        mode: FollowMode,
    ) -> mpsc::Receiver<FollowerMessage> {
        let (sender, receiver) = mpsc::channel(100_000);
        follow_request_sender
            .try_send(FollowRequest { sender, mode })
            .unwrap_or_else(|_| panic!("unable to submit follow request"));
        receiver
    }

    /// Reads a resuming follower's position, failing if a log line arrives first.
    async fn recv_resume(receiver: &mut mpsc::Receiver<FollowerMessage>) -> (Uuid, u64, u64) {
        match recv_message(receiver).await {
            FollowerMessage::Resume { epoch, seq, gap } => (epoch, seq, gap),
            FollowerMessage::Line(line) => {
                panic!("expected a position, received a log line: {line}")
            }
        }
    }

    /// Starts a logs thread and feeds it `num_lines` lines, returning the follow-request
    /// sender, the log sender, and a follower that has already consumed everything.
    ///
    /// The follower is attached before the lines are sent, because draining it is what
    /// proves the thread has appended them. The thread selects between the follow-request
    /// and log-message channels, so send order alone would establish nothing.
    #[allow(clippy::type_complexity)]
    async fn logs_thread_with_lines(
        num_lines: usize,
    ) -> (
        oneshot::Sender<()>,
        mpsc::Sender<FollowRequest>,
        mpsc::Sender<LogMessage>,
        Uuid,
        mpsc::Receiver<FollowerMessage>,
    ) {
        let (follow_sender, follow_receiver) = mpsc::channel(10);
        let (logs_sender, logs_receiver) = mpsc::channel(10_000);
        let (terminate, _join_handle) = start_thread_pipeline_logs(
            "00000000-0000-0000-0000-000000000000",
            "test-pipeline",
            follow_receiver,
            logs_receiver,
        );

        let mut live = follow(&follow_sender, FollowMode::Resume(None));
        let (epoch, seq, gap) = recv_resume(&mut live).await;
        assert_eq!((seq, gap), (0, 0));
        // Sequence 1 is the thread's own opening line.
        assert!(
            recv_line(&mut live)
                .await
                .contains("Fresh start of pipeline logs")
        );

        for i in 1..=num_lines {
            logs_sender
                .send(LogMessage::new_from_pipeline(&format!("line {i}")))
                .await
                .unwrap_or_else(|_| panic!("unable to send log line"));
        }
        for i in 1..=num_lines {
            assert_eq!(recv_line(&mut live).await, format!("line {i}"));
        }

        (terminate, follow_sender, logs_sender, epoch, live)
    }

    /// A cursor issued by this buffer replays only what the follower is missing.
    #[tokio::test]
    async fn follower_resumes_from_a_cursor() {
        let (_terminate, follow_sender, _logs_sender, epoch, _live) =
            logs_thread_with_lines(100).await;

        // The opening line holds sequence 1, so "line i" holds sequence i + 1. A cursor at
        // 61 has therefore seen through "line 60" and is owed "line 61" onwards.
        let mut resumed = follow(
            &follow_sender,
            FollowMode::Resume(Some(LogCursor { epoch, seq: 61 })),
        );
        assert_eq!(recv_resume(&mut resumed).await, (epoch, 61, 0));
        for i in 61..=100 {
            assert_eq!(recv_line(&mut resumed).await, format!("line {i}"));
        }
        assert_no_more_lines(&mut resumed).await;
    }

    /// A follower that is already caught up receives its position and then waits, rather
    /// than being replayed anything it already holds.
    #[tokio::test]
    async fn caught_up_follower_receives_nothing_to_replay() {
        let (_terminate, follow_sender, logs_sender, epoch, _live) =
            logs_thread_with_lines(100).await;

        let mut resumed = follow(
            &follow_sender,
            FollowMode::Resume(Some(LogCursor { epoch, seq: 101 })),
        );
        assert_eq!(recv_resume(&mut resumed).await, (epoch, 101, 0));
        assert_no_more_lines(&mut resumed).await;

        // It still receives lines that arrive after it attached.
        logs_sender
            .send(LogMessage::new_from_pipeline("line 101"))
            .await
            .unwrap_or_else(|_| panic!("unable to send log line"));
        assert_eq!(recv_line(&mut resumed).await, "line 101");
    }

    /// A cursor from a previous instance of the buffer refers to lines this instance never
    /// held. Accepting it would silently skip history, so it degrades to a full catch-up.
    #[tokio::test]
    async fn cursor_from_another_epoch_replays_everything() {
        let (_terminate, follow_sender, _logs_sender, epoch, _live) =
            logs_thread_with_lines(100).await;

        let mut resumed = follow(
            &follow_sender,
            FollowMode::Resume(Some(LogCursor {
                epoch: Uuid::now_v7(),
                seq: 61,
            })),
        );
        assert_eq!(recv_resume(&mut resumed).await, (epoch, 0, 0));
        assert!(
            recv_line(&mut resumed)
                .await
                .contains("Fresh start of pipeline logs")
        );
        for i in 1..=100 {
            assert_eq!(recv_line(&mut resumed).await, format!("line {i}"));
        }
        assert_no_more_lines(&mut resumed).await;
    }

    /// When the lines a cursor points at have been evicted, the follower is told how many
    /// it lost instead of being left to believe the stream is contiguous.
    #[tokio::test]
    async fn evicted_cursor_reports_the_gap() {
        let (_terminate, follow_sender, logs_sender, epoch, mut live) =
            logs_thread_with_lines(100).await;

        // A single line larger than the whole buffer discards everything, itself included,
        // which is the cheapest way to force eviction at the production limits.
        let oversized = "x".repeat(LOGS_BUFFER_LIMIT_BYTE + 1);
        logs_sender
            .send(LogMessage::new_from_pipeline(&oversized))
            .await
            .unwrap_or_else(|_| panic!("unable to send log line"));
        assert_eq!(recv_line(&mut live).await, oversized);

        // 101 lines were appended before the oversized one, which is sequence 102. A cursor
        // at 61 is owed sequences 62 through 102, all of which are gone.
        let mut resumed = follow(
            &follow_sender,
            FollowMode::Resume(Some(LogCursor { epoch, seq: 61 })),
        );
        assert_eq!(recv_resume(&mut resumed).await, (epoch, 102, 41));
        assert_no_more_lines(&mut resumed).await;
    }

    /// Followers that do not ask to resume must see exactly what they saw before the
    /// cursor protocol existed: no position message, the whole buffer, and the discard
    /// notice in-band.
    #[tokio::test]
    async fn full_mode_is_unchanged() {
        let (_terminate, follow_sender, logs_sender, _epoch, mut live) =
            logs_thread_with_lines(3).await;

        let mut full = follow(&follow_sender, FollowMode::Full);
        assert!(
            recv_line(&mut full)
                .await
                .contains("Fresh start of pipeline logs")
        );
        for i in 1..=3 {
            assert_eq!(recv_line(&mut full).await, format!("line {i}"));
        }
        assert_no_more_lines(&mut full).await;

        // Once lines have been discarded, the notice leads the catch-up.
        let oversized = "x".repeat(LOGS_BUFFER_LIMIT_BYTE + 1);
        logs_sender
            .send(LogMessage::new_from_pipeline(&oversized))
            .await
            .unwrap_or_else(|_| panic!("unable to send log line"));
        assert_eq!(recv_line(&mut live).await, oversized);

        let mut full = follow(&follow_sender, FollowMode::Full);
        let notice = recv_line(&mut full).await;
        assert!(
            notice.contains("5 prior log lines were discarded"),
            "unexpected notice: {notice}"
        );
        assert_no_more_lines(&mut full).await;
    }

    #[test]
    #[rustfmt::skip] // Skip formatting to keep the test cases readable
    fn logs_buffer_variety() {
        let test_cases: Vec<(usize, usize, Vec<&str>, Vec<&str>)> = vec![
            // Potentially exceed number of byte
            (0, 1000, vec!["a"], vec![]),
            (0, 1000, vec!["a"], vec![]),
            (0, 1000, vec!["a", "b"], vec![]),
            (1, 1000, vec!["a"], vec!["a"]),
            (1, 1000, vec!["a", "b"], vec!["b"]),
            (1, 1000, vec!["a", "b", "c"], vec!["c"]),
            (2, 1000, vec!["a", "b", "c"], vec!["b", "c"]),
            (2, 1000, vec!["a", "b", "c", "d"], vec!["c", "d"]),
            // Potentially exceed number of lines
            (1000, 0, vec![], vec![]),
            (1000, 0, vec!["a"], vec![]),
            (1000, 0, vec!["a", "b"], vec![]),
            (1000, 1, vec!["a"], vec!["a"]),
            (1000, 1, vec!["a", "b"], vec!["b"]),
            (1000, 1, vec!["a", "b", "c"], vec!["c"]),
            (1000, 2, vec!["a", "b", "c"], vec!["b", "c"]),
            (1000, 2, vec!["a", "b", "c", "d"], vec!["c", "d"]),
            // Empty lines exceed number of lines
            (1000, 0, vec![""], vec![]),
            (1000, 1, vec![""], vec![""]),
            (1000, 1, vec!["", ""], vec![""]),
            (1000, 5, vec!["", "", "", "", "", ""], vec!["", "", "", "", ""]),
            // Exceed both potentially
            (0, 0, vec!["a"], vec![]),
            (0, 0, vec!["a", "b"], vec![]),
            (1, 1, vec!["a"], vec!["a"]),
            (1, 1, vec!["a", "b"], vec!["b"]),
            (1, 1, vec!["a", "b", "c"], vec!["c"]),
            (2, 2, vec!["a", "b", "c"], vec!["b", "c"]),
            (2, 2, vec!["a", "b", "c", "d"], vec!["c", "d"]),
            // Others
            (5, 10, vec!["abc", "def"], vec!["def"]),
            (5, 2, vec!["abc", "def"], vec!["def"]),
            (6, 2, vec!["abc", "def"], vec!["abc", "def"]),
        ];

        // Run the test cases
        for (limit_byte, limit_num_lines, input, output) in test_cases {
            let mut buffer = LogsBuffer::new(limit_byte, limit_num_lines);
            for s in &input {
                buffer.append(s.to_string().clone());
            }
            let mut expected = VecDeque::new();
            for s in &output {
                expected.push_back(s.to_string());
            }
            assert_eq!(buffer.lines(), &expected, "Failed for test case (lb={}, ln={}, i={:?}) -> o={:?}", limit_byte, limit_num_lines, input, output);
        }
    }

    #[test]
    fn logs_buffer_normal() {
        // Buffer with 20 byte and 5 lines limit
        let mut buffer = LogsBuffer::new(20, 5);
        assert_eq!(buffer.lines(), &VecDeque::from([]));
        assert_eq!(buffer.num_lines(), 0);
        assert_eq!(buffer.num_discarded_lines(), 0);
        assert_eq!(buffer.size_byte(), 0);
        assert_eq!(buffer.size_limit_byte(), 20);
        assert_eq!(buffer.size_limit_num_lines(), 5);

        // Exceed the byte limit
        buffer.append("abcde".to_string());
        buffer.append("fghij".to_string());
        buffer.append("klmno".to_string());
        buffer.append("pqrst".to_string());
        assert_eq!(buffer.num_discarded_lines(), 0);
        buffer.append("uvwxyz1".to_string());
        assert_eq!(
            buffer.lines(),
            &VecDeque::from([
                "klmno".to_string(),
                "pqrst".to_string(),
                "uvwxyz1".to_string()
            ])
        );
        assert_eq!(buffer.num_lines(), 3);
        assert_eq!(buffer.num_discarded_lines(), 2);
        assert_eq!(buffer.size_byte(), 17);
        assert_eq!(buffer.size_limit_byte(), 20);
        assert_eq!(buffer.size_limit_num_lines(), 5);

        // Exceed the number of lines limit
        buffer.append("2".to_string());
        buffer.append("3".to_string());
        assert_eq!(buffer.num_discarded_lines(), 2);
        buffer.append("4".to_string());
        assert_eq!(
            buffer.lines(),
            &VecDeque::from([
                "pqrst".to_string(),
                "uvwxyz1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "4".to_string()
            ])
        );
        assert_eq!(buffer.num_lines(), 5);
        assert_eq!(buffer.num_discarded_lines(), 3);
        assert_eq!(buffer.size_byte(), 15);
        assert_eq!(buffer.size_limit_byte(), 20);
        assert_eq!(buffer.size_limit_num_lines(), 5);

        // Exceed the number of bytes with a string larger than can fit in buffer
        buffer.append("aaaaabbbbbcccccddddde".to_string());
        assert_eq!(buffer.num_lines(), 0);
        assert_eq!(buffer.num_discarded_lines(), 9);
        assert_eq!(buffer.size_byte(), 0);
        assert_eq!(buffer.size_limit_byte(), 20);
        assert_eq!(buffer.size_limit_num_lines(), 5);
    }
}
