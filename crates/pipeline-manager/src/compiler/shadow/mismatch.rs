//! Local review bundles for Rust optimizer differences. Only the anonymizer's
//! successful output can supply SQL. Compiler logs and artifacts stay in the attempt.
use super::{Difference, MAX_OBSERVATION_BYTES, limit_jvm, limit_output_files, try_wait};
use crate::compiler::util::ProcessGroupTerminator;
use crate::config::CompilerConfig;
use crate::db::types::program::ProgramConfig;
use serde::Deserialize;
use serde_json::{Value, json};
use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;
use tokio::time::{Duration, Instant, sleep, timeout_at};
use uuid::Uuid;

const RETAINED_BUNDLES: usize = 32;

async fn anonymize(
    jar: &Path,
    attempt: &Path,
    process_group: &mut Option<u32>,
) -> io::Result<Option<String>> {
    let output = attempt.join("anonymized.sql");
    let mut command = Command::new("java");
    limit_jvm(&mut command);
    limit_output_files(&mut command);
    let mut child = command
        .arg("-jar")
        .arg(std::path::absolute(jar)?)
        .arg("--anonymize")
        .arg("-o")
        .arg("anonymized.sql")
        .arg("program.sql")
        .current_dir(attempt)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| io::Error::other("missing anonymizer process ID"))?;
    *process_group = Some(pid);
    let mut group = ProcessGroupTerminator::new("SQL shadow anonymizer", pid);
    loop {
        if let Some(status) = try_wait(&mut child, &mut group)? {
            if !status.success() {
                return Ok(None);
            }
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    let size = tokio::fs::metadata(&output).await?.len();
    if size == 0 || size > MAX_OBSERVATION_BYTES as u64 {
        return Ok(None);
    }
    Ok(Some(tokio::fs::read_to_string(output).await?))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn retain(
    config: &CompilerConfig,
    program: &Value,
    attempt: &Path,
    id: Uuid,
    difference: Difference,
    package: Option<&str>,
    expires: Instant,
    process_group: &mut Option<u32>,
) -> io::Result<()> {
    let program = ProgramConfig::deserialize(program).map_err(io::Error::other)?;
    let runtime = program.runtime_version();
    let mut bundle = json!({
        "version": 1,
        "compiler_build": env!("VERGEN_GIT_SHA"),
        "package_sha256": package,
        "runtime": runtime.to_string(),
        "engine": if runtime.is_gen2() { "gen2" } else { "classic" },
        "use_platform_compiler": program.use_platform_compiler,
        "first_difference": difference,
    });
    let status = match timeout_at(
        expires,
        anonymize(Path::new(&config.sql_compiler_path), attempt, process_group),
    )
    .await
    {
        Ok(Ok(Some(sql))) => {
            bundle["anonymized_sql"] = sql.into();
            "available"
        }
        Ok(Ok(None)) => "unavailable",
        Ok(Err(_)) => "io_error",
        Err(_) => "timed_out",
    };
    bundle["anonymized_sql_status"] = status.into();
    let directory = config.working_dir().join("sql-shadow-mismatches");
    let attempt = attempt.to_owned();
    tokio::task::spawn_blocking(move || publish(&directory, &attempt, id, &bundle))
        .await
        .map_err(io::Error::other)?
}

fn publish(directory: &Path, attempt: &Path, id: Uuid, bundle: &Value) -> io::Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(directory)?;
    // Both directories belong to the same compiler working directory. Staging
    // under the attempt lets startup cleanup reclaim interrupted writes.
    let mut file = tempfile::NamedTempFile::new_in(attempt)?;
    serde_json::to_writer(&mut file, bundle)?;
    // Serialize publishers sharing a working directory. Prune before publishing:
    // a permission or filesystem failure must not grow the archive unboundedly.
    let lock = std::fs::File::open(directory)?;
    lock.lock()?;
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json")
            && let Some(id) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| Uuid::parse_str(s).ok())
            && id.get_version_num() == 7
            && entry.file_type()?.is_file()
        {
            entries.push((id, path));
        }
    }
    let destination = directory.join(format!("{id}.json"));
    entries.push((id, destination.clone()));
    entries.sort_unstable();
    let obsolete = entries.len().saturating_sub(RETAINED_BUNDLES);
    for (_, path) in entries.into_iter().take(obsolete) {
        if path == destination {
            return Ok(());
        }
        if let Err(e) = std::fs::remove_file(path)
            && e.kind() != io::ErrorKind::NotFound
        {
            return Err(e);
        }
    }
    file.persist_noclobber(destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn failed_anonymizer_keeps_only_review_metadata() {
        let root = tempfile::tempdir().unwrap();
        let mut config = CompilerConfig::parse_from(["test"]);
        config.compiler_working_directory = root.path().to_str().unwrap().to_owned();
        config.sql_compiler_path = root.path().join("absent.jar").display().to_string();
        let attempt = tempfile::tempdir_in(root.path()).unwrap();
        std::fs::write(
            attempt.path().join("program.sql"),
            "SELECT 'private literal'",
        )
        .unwrap();
        let id = Uuid::now_v7();
        retain(
            &config,
            &json!({}),
            attempt.path(),
            id,
            Difference::Schema,
            None,
            Instant::now() + Duration::from_secs(30),
            &mut None,
        )
        .await
        .unwrap();
        let bytes =
            std::fs::read(root.path().join(format!("sql-shadow-mismatches/{id}.json"))).unwrap();
        let bundle: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(bundle["first_difference"], "schema");
        assert_ne!(bundle["anonymized_sql_status"], "available");
        assert!(!bundle.as_object().unwrap().contains_key("anonymized_sql"));
        assert!(
            !String::from_utf8(bytes)
                .unwrap()
                .contains("private literal")
        );
    }

    #[test]
    fn publishing_keeps_recent_private_bundles_across_restarts() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("bundles");
        let mut ids = Vec::new();
        for _ in 0..RETAINED_BUNDLES + 2 {
            let id = Uuid::now_v7();
            publish(&directory, root.path(), id, &json!({"version":1})).unwrap();
            ids.push(id);
        }
        ids.sort_unstable();
        let path = |id| directory.join(format!("{id}.json"));
        assert!(!path(ids[1]).exists());
        assert_eq!(
            std::fs::read_dir(&directory).unwrap().count(),
            RETAINED_BUNDLES
        );
        for &id in &ids[2..] {
            let file = path(id);
            assert_eq!(
                std::fs::metadata(&file).unwrap().permissions().mode() & 0o077,
                0
            );
            assert_eq!(std::fs::read_to_string(file).unwrap(), r#"{"version":1}"#);
        }
    }
}
