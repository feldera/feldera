//! Read a completed IR-only hybrid shadow attempt.
use crate::compiler::sql_compiler::SqlCompilationError;
use crate::compiler::util::{checksum_file, read_file_content};
use crate::db::types::program::{RuntimeSelector, SqlCompilerMessage};
use serde::Deserialize;
use std::{collections::BTreeMap, path::Path};

#[derive(Deserialize)]
struct Attempt {
    schema_version: u32,
    compiler: String,
    #[serde(default)]
    rust_optimizer_passes: bool,
    producer_commit: String,
    package_sha256: Option<String>,
    runtime_abi: String,
    engine: String,
    status: String,
    reason: Option<String>,
    messages: Vec<SqlCompilerMessage>,
    artifacts: BTreeMap<String, String>,
}

pub(in crate::compiler) async fn messages(
    directory: &Path,
    runtime: &RuntimeSelector,
    rust_optimizer: bool,
    success: bool,
    package: Option<&mut String>,
) -> Result<Vec<SqlCompilerMessage>, SqlCompilationError> {
    let raw = read_file_content(&directory.join("result.json")).await?;
    let attempt: Attempt = serde_json::from_str(&raw).map_err(|error| {
        SqlCompilationError::SystemError(format!("Invalid hybrid result: {error}"))
    })?;
    let engine = if runtime.is_gen2() { "gen2" } else { "classic" };
    if attempt.schema_version != 1
        || attempt.compiler != "hybrid"
        || attempt.rust_optimizer_passes != rust_optimizer
        || attempt.producer_commit != env!("VERGEN_GIT_SHA")
        || attempt.runtime_abi.as_bytes() != runtime.as_bytes()
        || attempt.engine != engine
    {
        return Err(SqlCompilationError::SystemError(
            "Hybrid result identity mismatch".into(),
        ));
    }
    match (attempt.status.as_str(), success) {
        ("ok", true) => verify_artifacts(directory, runtime.is_gen2(), &attempt.artifacts).await?,
        ("sql_error", false) if attempt.artifacts.is_empty() => (),
        _ => {
            return Err(SqlCompilationError::SystemError(format!(
                "Hybrid compilation {}: {}",
                attempt.status,
                attempt.reason.as_deref().unwrap_or("no reason supplied")
            )));
        }
    }
    if let (Some(output), Some(digest)) = (package, attempt.package_sha256)
        && digest.len() == 64
        && digest
            .bytes()
            .all(|c| c.is_ascii_digit() || matches!(c, b'a'..=b'f'))
    {
        *output = digest;
    }
    Ok(attempt.messages)
}

async fn verify_artifacts(
    directory: &Path,
    gen2: bool,
    digests: &BTreeMap<String, String>,
) -> Result<(), SqlCompilationError> {
    let required: &[&str] = if gen2 {
        &["schema.json", "dataflow.json", "circuit.json"]
    } else {
        &["schema.json", "dataflow.json"]
    };
    if digests.len() != required.len() || required.iter().any(|name| !digests.contains_key(*name)) {
        return Err(SqlCompilationError::SystemError(
            "Hybrid artifact manifest differs from the requested IR-only output".into(),
        ));
    }
    for &name in required {
        let (_, actual) = checksum_file(&directory.join(name)).await?;
        if actual != digests[name] {
            return Err(SqlCompilationError::SystemError(format!(
                "Hybrid artifact checksum mismatch for {name}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn artifacts_must_match_the_engine_and_published_contents() {
        let directory = tempfile::tempdir().unwrap();
        let directory = directory.path();
        let mut digests = BTreeMap::new();
        for name in ["schema.json", "dataflow.json", "circuit.json"] {
            let path = directory.join(name);
            tokio::fs::write(&path, name).await.unwrap();
            digests.insert(name.to_owned(), checksum_file(&path).await.unwrap().1);
        }
        verify_artifacts(directory, true, &digests).await.unwrap();
        assert!(verify_artifacts(directory, false, &digests).await.is_err());
        digests.remove("circuit.json");
        assert!(verify_artifacts(directory, true, &digests).await.is_err());
        verify_artifacts(directory, false, &digests).await.unwrap();
        tokio::fs::write(directory.join("schema.json"), "changed")
            .await
            .unwrap();
        assert!(verify_artifacts(directory, false, &digests).await.is_err());
    }
}
