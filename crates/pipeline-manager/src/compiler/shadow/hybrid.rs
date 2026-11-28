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
    shadow: &mut super::Attempt,
) -> Result<Vec<SqlCompilerMessage>, SqlCompilationError> {
    let raw = read_file_content(&directory.join("result.json"))
        .await
        .inspect_err(|e| shadow.cause.file_error("read hybrid result", e))?;
    shadow.cause.operation = "invalid hybrid result";
    let attempt: Attempt = serde_json::from_str(&raw).map_err(|error| {
        SqlCompilationError::SystemError(format!("Invalid hybrid result: {error}"))
    })?;
    let engine = if runtime.is_gen2() { "gen2" } else { "classic" };
    let identity = [
        ("schema_version", attempt.schema_version == 1),
        ("compiler", attempt.compiler == "hybrid"),
        (
            "rust_optimizer_passes",
            attempt.rust_optimizer_passes == rust_optimizer,
        ),
        (
            "producer_commit",
            attempt.producer_commit == env!("VERGEN_GIT_SHA"),
        ),
        (
            "runtime_abi",
            attempt.runtime_abi.as_bytes() == runtime.as_bytes(),
        ),
        ("engine", attempt.engine == engine),
    ];
    if let Some((field, _)) = identity.iter().find(|(_, is_expected)| !is_expected) {
        shadow.cause.operation = "hybrid result identity mismatch";
        return Err(SqlCompilationError::SystemError(format!(
            "Hybrid result identity mismatch in {field}. Install the sql2dbsp of this manager release."
        )));
    }
    match (attempt.status.as_str(), success) {
        ("ok", true) => {
            shadow.cause.operation = "verify hybrid artifacts";
            verify_artifacts(directory, runtime.is_gen2(), &attempt.artifacts).await?;
        }
        ("sql_error", false) if attempt.artifacts.is_empty() => (),
        _ => {
            shadow.timed_out = attempt.status == "resource_limit"
                && attempt.reason.as_deref() == Some("compilation deadline expired");
            shadow.cause.operation = match attempt.status.as_str() {
                "resource_limit" => "hybrid resource limit",
                "cancelled" => "hybrid compilation cancelled",
                "internal_error" => "hybrid internal error",
                _ => "unexpected hybrid result status",
            };
            return Err(SqlCompilationError::SystemError(format!(
                "Hybrid compilation {}: {}",
                attempt.status,
                attempt.reason.as_deref().unwrap_or("no reason supplied")
            )));
        }
    }
    if let Some(digest) = attempt.package_sha256
        && digest.len() == 64
        && digest
            .bytes()
            .all(|c| c.is_ascii_digit() || matches!(c, b'a'..=b'f'))
    {
        shadow.package = digest;
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
