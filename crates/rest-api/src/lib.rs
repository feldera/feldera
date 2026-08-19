mod generated {
    #![allow(clippy::all, unused)]
    include!(concat!(env!("OUT_DIR"), "/codegen.rs"));
}
pub use generated::*;

mod retry;
pub use retry::RetryPolicy;

// ClientInfo is already in scope through the generated code's public
// re-export; importing it again here would shadow that re-export.
use progenitor_client::{ClientHooks, OperationInfo};

/// Route every request through the retry layer. Overrides the no-op default
/// on `&Client` via auto-ref specialization (see `progenitor_client::ClientHooks`).
impl ClientHooks<RetryPolicy> for Client {
    async fn exec(
        &self,
        request: reqwest::Request,
        info: &OperationInfo,
    ) -> reqwest::Result<reqwest::Response> {
        retry::execute_with_retry(self.client(), self.inner(), request, info.operation_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::types::CheckpointMetadata;

    /// Reproduces issue #6841: `fda pipelines`/`fda status` failed to parse
    /// any pipeline's checkpoint list once one checkpoint's fingerprint had
    /// the high bit set, because the generated `fingerprint` field was `i64`.
    /// `format: uint64` on that field (checkpoint.rs) makes progenitor/typify
    /// generate `u64` instead, so this must deserialize without error.
    #[test]
    fn checkpoint_metadata_accepts_fingerprint_above_i64_max() {
        let raw = r#"{
            "uuid": "00000000-0000-0000-0000-000000000000",
            "fingerprint": 14128757731148314856
        }"#;
        let metadata: CheckpointMetadata = serde_json::from_str(raw).unwrap();
        assert_eq!(metadata.fingerprint, 14128757731148314856);
    }
}
