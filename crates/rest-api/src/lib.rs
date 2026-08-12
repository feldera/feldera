#![allow(clippy::all, unused)]

use feldera_observability::ReqwestTracingExt;

include!(concat!(env!("OUT_DIR"), "/codegen.rs"));

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
