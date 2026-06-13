use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

fn default_threads() -> usize {
    1
}

fn default_max_retries() -> Option<u8> {
    Some(10)
}

fn default_max_buffer_size_bytes() -> usize {
    1024 * 1024
}

fn default_max_concurrent_requests() -> usize {
    16
}

fn default_write_mode() -> DynamoDBWriteMode {
    DynamoDBWriteMode::Batch
}

/// DynamoDB write API used by the output connector.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DynamoDBWriteMode {
    /// Use DynamoDB `BatchWriteItem`.
    ///
    /// This is the high-throughput path. Writes are flushed at batch end, but
    /// DynamoDB does not make the whole batch atomic.
    Batch,

    /// Use DynamoDB `TransactWriteItems`.
    ///
    /// This provides atomicity for each transaction chunk, but is substantially
    /// slower and is limited to 100 writes per request.
    Transactional,
}

/// DynamoDB output connector configuration.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct DynamoDBWriterConfig {
    /// Name of the DynamoDB table to write to.
    pub table: String,

    /// AWS region.
    pub region: String,

    /// Optional endpoint URL, for example when using a local
    /// DynamoDB-compatible service.
    pub endpoint_url: Option<String>,

    /// AWS access key ID.
    ///
    /// If both `aws_access_key_id` and `aws_secret_access_key` are specified,
    /// the connector uses these static credentials. Otherwise it uses the
    /// default AWS credential provider chain, including IAM Roles for Service
    /// Accounts (IRSA) in EKS.
    pub aws_access_key_id: Option<String>,

    /// AWS secret access key.
    pub aws_secret_access_key: Option<String>,

    /// Maximum number of write requests in one DynamoDB write call.
    ///
    /// DynamoDB supports at most 100 for `TransactWriteItems` and at most 25
    /// for `BatchWriteItem`. If omitted, the connector uses the maximum for
    /// the selected `write_mode`.
    #[serde(default)]
    pub batch_size: Option<usize>,

    /// Write API to use when flushing records at batch end.
    ///
    /// The default is `batch`, where each worker bulk-writes its own split
    /// using DynamoDB `BatchWriteItem`.
    #[serde(default = "default_write_mode")]
    #[schema(default = default_write_mode)]
    pub write_mode: DynamoDBWriteMode,

    /// Maximum number of bytes buffered by each worker before flushing writes.
    ///
    /// This is an approximate size based on encoded DynamoDB attributes.
    #[serde(default = "default_max_buffer_size_bytes")]
    #[schema(default = default_max_buffer_size_bytes)]
    pub max_buffer_size_bytes: usize,

    /// Maximum number of DynamoDB write requests in flight per worker thread.
    ///
    /// The total in-flight request count across the connector is
    /// `threads × max_concurrent_requests`. Size this accordingly when
    /// tuning against a provisioned-throughput table to avoid excessive
    /// throttling.
    #[serde(default = "default_max_concurrent_requests")]
    #[schema(default = default_max_concurrent_requests)]
    pub max_concurrent_requests: usize,

    /// Number of worker threads used to encode and write disjoint key ranges.
    #[serde(default = "default_threads")]
    #[schema(default = default_threads)]
    pub threads: usize,

    /// Maximum number of retries for a failed or partially-applied DynamoDB write chunk.
    ///
    /// For `batch` writes, `BatchWriteItem` may return some items as "unprocessed" in a
    /// successful 200 response; those are re-submitted and counted as retries. For
    /// `transactional` writes, a failed `TransactWriteItems` call is retried in full.
    ///
    /// Transient errors (throttling, network failures) are first handled transparently by the
    /// AWS SDK. Only attempts that reach this connector's retry loop count against this limit.
    /// Each retry waits longer than the previous one (exponential backoff), up to a ceiling
    /// of roughly 13 seconds.
    ///
    /// Set to `null` to retry indefinitely. After the backoff ceiling is reached the connector
    /// keeps retrying at that interval, providing backpressure until DynamoDB recovers.
    /// Defaults to `10`.
    #[serde(default = "default_max_retries")]
    #[schema(default = default_max_retries)]
    pub max_retries: Option<u8>,
}

impl DynamoDBWriterConfig {
    pub fn effective_batch_size(&self) -> usize {
        self.batch_size
            .unwrap_or_else(|| self.write_mode.max_batch_size())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.table.trim().is_empty() {
            return Err("table cannot be empty".to_string());
        }
        if self.table.len() < 3 || self.table.len() > 255 {
            return Err(format!(
                "table name '{}' is {} character(s); DynamoDB requires 3–255 characters",
                self.table,
                self.table.len()
            ));
        }
        if !self
            .table
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
        {
            return Err(format!(
                "table name '{}' contains invalid characters; DynamoDB requires [a-zA-Z0-9_.-]",
                self.table
            ));
        }
        if self.region.trim().is_empty() {
            return Err("region cannot be empty".to_string());
        }
        if let Some(batch_size) = self.batch_size {
            let max_batch_size = self.write_mode.max_batch_size();
            if batch_size == 0 || batch_size > max_batch_size {
                return Err(format!(
                    "batch_size must be between 1 and {max_batch_size} for {:?} write mode",
                    self.write_mode
                ));
            }
        }
        if self.threads == 0 {
            return Err("threads must be at least 1".to_string());
        }
        if self.max_buffer_size_bytes == 0 {
            return Err("max_buffer_size_bytes must be greater than 0".to_string());
        }
        if self.max_concurrent_requests == 0 {
            return Err("max_concurrent_requests must be greater than 0".to_string());
        }
        match (&self.aws_access_key_id, &self.aws_secret_access_key) {
            (Some(_), Some(_)) | (None, None) => {}
            _ => {
                return Err(
                    "aws_access_key_id and aws_secret_access_key must be specified together"
                        .to_string(),
                );
            }
        }
        Ok(())
    }
}

impl DynamoDBWriteMode {
    pub fn max_batch_size(self) -> usize {
        match self {
            Self::Batch => 25,
            Self::Transactional => 100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(write_mode: DynamoDBWriteMode, batch_size: Option<usize>) -> DynamoDBWriterConfig {
        DynamoDBWriterConfig {
            table: "test_table".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            batch_size,
            write_mode,
            max_buffer_size_bytes: 1024 * 1024,
            max_concurrent_requests: 16,
            threads: 1,
            max_retries: Some(10u8),
        }
    }

    #[test]
    fn default_batch_size_is_mode_specific() {
        assert_eq!(
            config(DynamoDBWriteMode::Batch, None).effective_batch_size(),
            25
        );
        assert_eq!(
            config(DynamoDBWriteMode::Transactional, None).effective_batch_size(),
            100
        );
    }

    #[test]
    fn explicit_batch_size_is_validated_against_mode_limit() {
        assert!(
            config(DynamoDBWriteMode::Batch, Some(25))
                .validate()
                .is_ok()
        );
        assert!(
            config(DynamoDBWriteMode::Batch, Some(26))
                .validate()
                .is_err()
        );
        assert!(
            config(DynamoDBWriteMode::Transactional, Some(100))
                .validate()
                .is_ok()
        );
        assert!(
            config(DynamoDBWriteMode::Transactional, Some(101))
                .validate()
                .is_err()
        );
    }

    #[test]
    fn table_name_validation() {
        let base = config(DynamoDBWriteMode::Batch, None);

        // too short
        assert!(
            DynamoDBWriterConfig {
                table: "ab".to_string(),
                ..base.clone()
            }
            .validate()
            .is_err()
        );
        // minimum length
        assert!(
            DynamoDBWriterConfig {
                table: "abc".to_string(),
                ..base.clone()
            }
            .validate()
            .is_ok()
        );
        // too long
        assert!(
            DynamoDBWriterConfig {
                table: "a".repeat(256),
                ..base.clone()
            }
            .validate()
            .is_err()
        );
        // maximum length
        assert!(
            DynamoDBWriterConfig {
                table: "a".repeat(255),
                ..base.clone()
            }
            .validate()
            .is_ok()
        );
        // invalid characters
        assert!(
            DynamoDBWriterConfig {
                table: "my/table".to_string(),
                ..base.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            DynamoDBWriterConfig {
                table: "my table".to_string(),
                ..base.clone()
            }
            .validate()
            .is_err()
        );
        // valid characters
        assert!(
            DynamoDBWriterConfig {
                table: "my.table-name_1".to_string(),
                ..base.clone()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn null_max_retries_deserializes_as_unbounded() {
        let json = r#"{"table":"t","region":"us-east-1","max_retries":null}"#;
        let cfg: DynamoDBWriterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_retries, None);
    }

    #[test]
    fn omitted_max_retries_deserializes_as_default() {
        let json = r#"{"table":"t","region":"us-east-1"}"#;
        let cfg: DynamoDBWriterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_retries, Some(10u8));
    }
}
