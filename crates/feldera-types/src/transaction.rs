use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub type TransactionId = u64;

/// Response to a `/start_transaction` request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StartTransactionResponse {
    transaction_id: TransactionId,
}

impl StartTransactionResponse {
    pub fn new(transaction_id: TransactionId) -> Self {
        Self { transaction_id }
    }
}
