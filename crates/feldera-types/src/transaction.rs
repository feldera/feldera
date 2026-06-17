use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub type TransactionId = i64;

/// Response to a `/start_transaction` request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StartTransactionResponse {
    pub transaction_id: i64,
}

impl StartTransactionResponse {
    pub fn new(transaction_id: TransactionId) -> Self {
        Self { transaction_id }
    }
}

/// Summary of transaction commit progress.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct CommitProgressSummary {
    /// Number of operators that have been fully flushed.
    pub completed: u64,

    /// Number of operators that are currently being flushed.
    pub in_progress: u64,

    /// Number of operators that haven't started flushing.
    pub remaining: u64,

    /// Number of records processed by operators that are currently being flushed.
    pub in_progress_processed_records: u64,

    /// Total number of records that operators that are currently being flushed need to process.
    pub in_progress_total_records: u64,
}

impl CommitProgressSummary {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn merge(&mut self, other: &CommitProgressSummary) {
        self.completed += other.completed;
        self.in_progress += other.in_progress;
        self.remaining += other.remaining;
        self.in_progress_processed_records += other.in_progress_processed_records;
        self.in_progress_total_records += other.in_progress_total_records;
    }
}

impl Display for CommitProgressSummary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "completed: {} operators, evaluating: {} operators [{}/{} changes processed], remaining: {} operators",
            self.completed,
            self.in_progress,
            self.in_progress_processed_records,
            self.in_progress_total_records,
            self.remaining
        )
    }
}

/// Phase of an in-progress concurrent bootstrap.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum ConcurrentBootstrapPhase {
    /// New and modified views are backfilling in the background while the
    /// pre-existing views stay live and serving.
    ConcurrentBootstrapping,

    /// The cutover window: inputs are paused while recorded changes are
    /// synchronized into the new views.
    Synchronizing,

    /// The new views have been cut over; a final transaction is initializing
    /// their snapshots before the pipeline is reported as running.
    Finalizing,
}

impl Display for ConcurrentBootstrapPhase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConcurrentBootstrapPhase::ConcurrentBootstrapping => {
                write!(f, "concurrent bootstrapping")
            }
            ConcurrentBootstrapPhase::Synchronizing => write!(f, "synchronizing"),
            ConcurrentBootstrapPhase::Finalizing => write!(f, "finalizing"),
        }
    }
}

/// Progress of an in-progress concurrent bootstrap.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ConcurrentBootstrapProgress {
    /// The current phase of the concurrent bootstrap.
    pub phase: ConcurrentBootstrapPhase,

    /// Progress of the bootstrap circuit's transaction commit, when a commit is
    /// in progress: the backfill transaction during `ConcurrentBootstrapping`
    /// and the synchronization transaction during `Synchronizing`. `None` while
    /// no commit is in progress (e.g. inputs are still being replayed, or
    /// during `Finalizing`, when the bootstrap circuit no longer exists).
    pub commit_progress: Option<CommitProgressSummary>,
}

impl Display for ConcurrentBootstrapProgress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.commit_progress {
            Some(progress) => {
                write!(
                    f,
                    "{}: committing bootstrap transaction ({progress})",
                    self.phase
                )
            }
            None => write!(f, "{}", self.phase),
        }
    }
}
