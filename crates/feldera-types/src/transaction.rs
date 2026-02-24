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
