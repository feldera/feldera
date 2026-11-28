//! Run the Java front end, the isolated Rust S3 worker and the classic Java
//! printer as one compilation. Only a completed attempt becomes visible.
mod package;
mod supervise;

use self::supervise::Supervisor;
use crate::worker::Failure;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    env,
    ffi::OsString,
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
    process::{Command, ExitCode, ExitStatus},
    time::Duration,
};

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum Engine {
    Classic,
    Gen2,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Workspace {
    crate_name: String,
    runtime_sources: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    java_classpath: String,
    producer_commit: String,
    runtime_abi: String,
    engine: Engine,
    sql: PathBuf,
    slots: PathBuf,
    deadline_ms: u64,
    workspace: Option<Workspace>,
    #[serde(default)]
    rust_optimizer_passes: bool,
    #[serde(default)]
    ir_only: bool,
    #[serde(default)]
    enterprise: bool,
    /// The caller has established that this revision has no retained state.
    #[serde(default)]
    allow_stateless_java_fallback: bool,
}

/// An error with no path of its own, such as a pipe or signal failure.
impl From<io::Error> for Failure {
    fn from(value: io::Error) -> Self {
        Self::Internal(value.to_string())
    }
}

/// Name the operation and the path of a failed file access.
trait AtPath<T> {
    fn at(self, operation: &str, path: &Path) -> Result<T, Failure>;
}

impl<T, E: std::fmt::Display> AtPath<T> for Result<T, E> {
    fn at(self, operation: &str, path: &Path) -> Result<T, Failure> {
        self.map_err(|error| Failure::at_path(operation, path, error))
    }
}

fn current_executable() -> Result<PathBuf, Failure> {
    env::current_exe().map_err(|error| {
        Failure::Internal(format!("Cannot locate the sql2dbsp executable: {error}"))
    })
}

/// Java emits a JSON diagnostic array on stderr under -je. A JVM startup line
/// may precede it. Accept only a complete array of structured messages.
fn messages(file: &Path) -> Result<Value, Failure> {
    let bytes = fs::read(file).at("read the compiler diagnostics", file)?;
    let mut input = bytes.trim_ascii_start();
    while input.starts_with(b"WARNING:") || input.starts_with(b"Picked up JAVA_TOOL_OPTIONS:") {
        input = input
            .splitn(2, |&b| b == b'\n')
            .nth(1)
            .unwrap_or_default()
            .trim_ascii_start();
    }
    if input.is_empty() {
        return Ok(json!([]));
    }
    if let Ok(Value::Array(items)) = serde_json::from_slice(input) {
        if items
            .iter()
            .all(|v| v["warning"].is_boolean() && v["message"].is_string())
        {
            return Ok(items.into());
        }
    }
    Err(Failure::Internal(format!(
        "compiler emitted malformed diagnostics in {}. Check the Java classpath and compiler version",
        file.display()
    )))
}

impl Request {
    fn jvm(&self, rust_passes: bool) -> Result<Command, Failure> {
        let mut command = Command::new("java");
        if rust_passes {
            let binary = current_executable()?;
            let directory = binary
                .parent()
                .ok_or_else(|| Failure::Internal("compiler has no parent directory".into()))?;
            let mut library_path = OsString::from("-Djava.library.path=");
            library_path.push(directory);
            // Each optimizer JVM serves one request. Limit JIT compilation to C1
            // for both the classic printer session and the Gen-2 front end.
            command
                .arg("--enable-native-access=ALL-UNNAMED")
                .arg(library_path)
                .arg("-Xss8m")
                .arg("-XX:TieredStopAtLevel=1");
        }
        command.args([
            "-Xmx512m",
            "-XX:MaxMetaspaceSize=128m",
            "-XX:MaxDirectMemorySize=64m",
            "-XX:ActiveProcessorCount=1",
            "-XX:-HeapDumpOnOutOfMemoryError",
            "-XX:ErrorFile=/dev/null",
            "-cp",
            &self.java_classpath,
        ]);
        Ok(command)
    }

    fn compiler_args(&self, rust_passes: bool) -> Vec<OsString> {
        let mut args = Vec::from(
            [
                "-i",
                "-je",
                "--alltables",
                "--ignoreOrder",
                "--dump-producer-commit",
                &self.producer_commit,
                "--dump-runtime-abi",
                &self.runtime_abi,
            ]
            .map(OsString::from),
        );
        if rust_passes {
            args.push("--rust-optimizer-passes".into());
        }
        if self.enterprise {
            args.push("--enterprise".into());
        }
        if matches!(self.engine, Engine::Gen2) {
            args.push("--gen2".into());
        }
        if let Some(ws) = &self.workspace {
            args.extend([
                "--crates".into(),
                ws.crate_name.clone().into(),
                "--runtime".into(),
                ws.runtime_sources.as_os_str().to_owned(),
            ]);
        }
        args
    }

    fn java(&self, rust_passes: bool) -> Result<Command, Failure> {
        let mut command = self.jvm(rust_passes)?;
        command
            .arg("org.dbsp.sqlCompiler.CompilerMain")
            .args(self.compiler_args(rust_passes));
        Ok(command)
    }

    fn run_java(
        &self,
        supervisor: &Supervisor,
        command: &mut Command,
        log: &Path,
    ) -> Result<Value, Failure> {
        let exit = supervisor.run(command, log, false)?;
        Self::java_result(exit, log)
    }

    fn java_result(exit: ExitStatus, log: &Path) -> Result<Value, Failure> {
        let diagnostics = messages(log)?;
        Failure::check_messages(&diagnostics)?;
        if !exit.success() {
            return Err(Failure::Internal(format!(
                "Java compiler exited with {exit}"
            )));
        }
        Ok(diagnostics)
    }

    fn output_args(&self, out: &Path) -> Result<Vec<OsString>, Failure> {
        if self.ir_only && matches!(self.engine, Engine::Classic) {
            let mut args = vec!["--noRust".into()];
            if self.workspace.is_some() {
                args.extend(["-o".into(), out.into()]);
            }
            return Ok(args);
        }
        let target = match (&self.engine, &self.workspace) {
            (Engine::Gen2, _) => out.join("circuit.json"),
            (Engine::Classic, Some(_)) => {
                let path = out.join("rust");
                fs::create_dir_all(path.join("crates")).at("create the workspace", &path)?;
                path
            }
            (Engine::Classic, None) => out.join("circuit.rs"),
        };
        Ok(vec!["-o".into(), target.into()])
    }

    fn front_args(&self, scratch: &Path) -> Vec<OsString> {
        let mut args = vec![
            self.sql.clone().into(),
            "--write-s3".into(),
            scratch.join("front.s3.json").into(),
            "--noRust".into(),
        ];
        if self.workspace.is_some() {
            args.extend(["-o".into(), scratch.into()]);
        }
        args
    }

    fn printer_args(&self, scratch: &Path, out: &Path) -> Result<Vec<OsString>, Failure> {
        let mut args = vec![
            "--read-s3".into(),
            scratch.join("worker/result/s3.json").into(),
        ];
        args.extend(self.output_args(out)?);
        Ok(args)
    }

    fn print_in_front_jvm(
        &self,
        supervisor: &Supervisor,
        scratch: &Path,
        out: &Path,
    ) -> Result<Value, Failure> {
        let ready = scratch.join("front.ready");
        let front_log = scratch.join("front.log");
        let tail_log = scratch.join("tail.log");
        let mut front = self.compiler_args(true);
        front.extend(self.front_args(scratch));
        let mut session = self.jvm(true)?;
        session
            .arg("org.dbsp.sqlCompiler.compiler.backend.S3PrinterSession")
            .arg(&ready)
            .arg(front.len().to_string())
            .args(front)
            .args(self.compiler_args(false))
            .args(self.printer_args(scratch, out)?)
            .arg("--errors")
            .arg(&tail_log);
        let exit = supervisor.run_with_checkpoint(&mut session, &front_log, &ready, || {
            self.run_worker(supervisor, scratch, out).map(|_| ())
        })?;
        Self::java_result(
            exit,
            if ready.try_exists()? {
                &tail_log
            } else {
                &front_log
            },
        )
    }

    fn attempt(
        &self,
        supervisor: &Supervisor,
        scratch: &Path,
        out: &Path,
        java_only: bool,
    ) -> Result<Value, Failure> {
        if java_only {
            let mut command = self.java(false)?;
            command
                .arg(&self.sql)
                .arg("-js")
                .arg(out.join("schema.json"))
                .arg("--dataflow")
                .arg(out.join("dataflow.json"))
                .args(self.output_args(out)?);
            return self.run_java(supervisor, &mut command, &scratch.join("java.log"));
        }
        let needs_printer = matches!(self.engine, Engine::Classic) && !self.ir_only;
        if needs_printer && self.rust_optimizer_passes {
            return self.print_in_front_jvm(supervisor, scratch, out);
        }
        let mut command = self.java(self.rust_optimizer_passes)?;
        command.args(self.front_args(scratch));
        self.run_java(supervisor, &mut command, &scratch.join("front.log"))?;
        let diagnostics = self.run_worker(supervisor, scratch, out)?;
        if needs_printer {
            let mut tail = self.java(false)?;
            tail.args(self.printer_args(scratch, out)?);
            return self.run_java(supervisor, &mut tail, &scratch.join("tail.log"));
        }
        Ok(diagnostics)
    }

    fn run_worker(
        &self,
        supervisor: &Supervisor,
        scratch: &Path,
        out: &Path,
    ) -> Result<Value, Failure> {
        let attempt = scratch.join("worker");
        let mut worker = Command::new(current_executable()?);
        worker
            .arg("--s3-worker")
            .arg(&self.producer_commit)
            .arg(&self.runtime_abi)
            .arg(scratch.join("front.s3.json"))
            .arg(&attempt);
        let status = supervisor.run(&mut worker, &scratch.join("worker.log"), true)?;
        let files = attempt.join("result");
        let result = files.join("result.json");
        let bytes = fs::read(&result).at("read the S3 worker result", &result)?;
        let record: Value =
            serde_json::from_slice(&bytes).at("parse the S3 worker result", &result)?;
        for (field, expected) in [
            ("producer_commit", &self.producer_commit),
            ("runtime_abi", &self.runtime_abi),
        ] {
            if record[field] != **expected {
                return Err(Failure::Internal(format!(
                    "The S3 worker result has {field} {}, not {expected}. \
                     Run the worker of this sql2dbsp build",
                    record[field]
                )));
            }
        }
        match record["status"].as_str() {
            Some("ok") if status.success() => (),
            Some("resource_limit") => {
                let reason = record["reason"].as_str().unwrap_or("Check the worker log");
                return Err(Failure::Limit(format!("S3 worker: {reason}")));
            }
            Some("sql_error") => {
                Failure::check_messages(&record["messages"])?;
                return Err(Failure::Internal(
                    "S3 worker reported an SQL error without a diagnostic. Check the worker log"
                        .into(),
                ));
            }
            _ => {
                return Err(Failure::Internal(format!(
                    "S3 worker failed with {status}: {}",
                    record["reason"]
                )))
            }
        }
        let mut artifacts = vec!["schema.json", "dataflow.json"];
        if matches!(self.engine, Engine::Gen2) {
            artifacts.push("circuit.json");
        }
        for name in artifacts {
            let artifact = files.join(name);
            fs::rename(&artifact, out.join(name))
                .at("publish the S3 worker artifact", &artifact)?;
        }
        Ok(record["messages"].clone())
    }
}

fn digests(
    supervisor: &Supervisor,
    root: &Path,
    dir: &Path,
    hashes: &mut serde_json::Map<String, Value>,
) -> Result<(), Failure> {
    for item in fs::read_dir(dir).at("list", dir)? {
        let path = item.at("list", dir)?.path();
        if path.is_dir() {
            digests(supervisor, root, &path, hashes)?;
        } else {
            hashes.insert(
                path.strip_prefix(root)
                    .expect("artifact inside staging")
                    .to_string_lossy()
                    .into_owned(),
                hex::encode(file_digest(supervisor, &path)?).into(),
            );
        }
    }
    Ok(())
}

fn file_digest(supervisor: &Supervisor, path: &Path) -> Result<[u8; 32], Failure> {
    let mut digest = Sha256::new();
    let mut file = File::open(path).at("open", path)?;
    let mut buffer = [0; 65536];
    loop {
        supervisor.remaining()?;
        let count = file.read(&mut buffer).at("read", path)?;
        if count == 0 {
            return Ok(digest.finalize().into());
        }
        digest.update(&buffer[..count]);
    }
}

pub(super) fn run(manifest: &Path, attempt: &Path) -> io::Result<ExitCode> {
    let invalid_request = |reason: &dyn std::fmt::Display| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid hybrid request {}: {reason}", manifest.display()),
        )
    };
    let file = File::open(manifest).map_err(|e| invalid_request(&e))?;
    if file.metadata()?.len() > 1024 * 1024 {
        return Err(invalid_request(&"the file exceeds one MiB"));
    }
    let request: Request = serde_json::from_reader(file).map_err(|e| invalid_request(&e))?;
    let rejected_field = [
        (
            "producer_commit is empty",
            request.producer_commit.is_empty(),
        ),
        ("runtime_abi is empty", request.runtime_abi.is_empty()),
        ("deadline_ms is zero", request.deadline_ms == 0),
        (
            "the gen2 engine does not accept a workspace",
            matches!(request.engine, Engine::Gen2) && request.workspace.is_some(),
        ),
    ]
    .into_iter()
    .find_map(|(reason, is_rejected)| is_rejected.then_some(reason));
    if let Some(reason) = rejected_field {
        return Err(invalid_request(&reason));
    }
    fs::create_dir(attempt)?;
    let mut ready = tempfile::tempdir_in(attempt)?;
    let mut family = "hybrid";
    let mut attempts = 0;
    let mut hashes = serde_json::Map::new();
    let mut package_sha256 = None;
    let mut fallback_reason = None;
    let result = (|| -> Result<Value, Failure> {
        let supervisor =
            Supervisor::new(&request.slots, Duration::from_millis(request.deadline_ms))?;
        let scratch = tempfile::tempdir_in(attempt)?;
        attempts += 1;
        let package = if request.rust_optimizer_passes
            && request.ir_only
            && env::var_os("FELDERA_SHADOW_IDENTIFY_PACKAGE").is_some()
        {
            package::Package::identify(&supervisor, Path::new(&request.java_classpath)).ok()
        } else {
            None
        };
        let first = request.attempt(&supervisor, scratch.path(), ready.path(), false);
        package_sha256 = package.and_then(package::Package::verified_digest);
        let messages = match first {
            Err(failure @ (Failure::Internal(_) | Failure::Limit(_)))
                if request.allow_stateless_java_fallback =>
            {
                supervisor.remaining()?;
                ready = tempfile::tempdir_in(attempt)?;
                let scratch = tempfile::tempdir_in(attempt)?;
                let (status, reason, _) = failure.fields();
                fallback_reason = Some(json!({"status": status, "reason": reason}));
                family = "java";
                attempts += 1;
                request.attempt(&supervisor, scratch.path(), ready.path(), true)
            }
            result => result,
        }?;
        digests(&supervisor, ready.path(), ready.path(), &mut hashes)?;
        supervisor.remaining()?;
        Ok(messages)
    })();
    let (status, reason, messages) = match result {
        Ok(messages) => ("ok", Value::Null, messages),
        Err(e) => {
            ready = tempfile::tempdir_in(attempt)?;
            hashes.clear();
            e.fields()
        }
    };
    let mut record = json!({"schema_version":1,"compiler":family,"worker_version":env!("CARGO_PKG_VERSION"),
        "producer_commit":request.producer_commit,"runtime_abi":request.runtime_abi,
        "rust_optimizer_passes":family == "hybrid" && request.rust_optimizer_passes,
        "engine":request.engine,"status":status,"reason":reason,"messages":messages,"attempts":attempts,"artifacts":hashes});
    if let Some(reason) = fallback_reason {
        record["fallback_reason"] = reason;
    }
    if let Some(digest) = package_sha256.filter(|_| family == "hybrid") {
        record["package_sha256"] = digest.into();
    }
    super::write_json(File::create_new(ready.path().join("result.json"))?, &record)?;
    fs::rename(ready.path(), attempt.join("result"))?;
    Ok(if status == "ok" {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn jvm_startup_lines_can_precede_empty_or_structured_diagnostics() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("front.log");
        let startup =
            "Picked up JAVA_TOOL_OPTIONS: -Xmx512m\nWARNING: A restricted method was called\n";
        let sql = json!([{"warning": false, "message": "Invalid SQL"}]);
        fs::write(&log, format!("{startup}{sql}")).unwrap();
        assert_eq!(messages(&log).unwrap(), sql);
        fs::write(&log, startup).unwrap();
        assert_eq!(messages(&log).unwrap(), json!([]));
    }

    #[test]
    fn malformed_diagnostics_are_internal_errors() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("front.log");
        for stderr in [
            "unexpected output\n[]",
            "WARNING: startup\n[broken\n[]",
            "WARNING: startup\n[{}]",
            "[]\nWARNING: trailing output",
        ] {
            fs::write(&log, stderr).unwrap();
            assert!(
                matches!(messages(&log), Err(Failure::Internal(_))),
                "{stderr}"
            );
        }
    }

    #[test]
    fn java_failure_kind_controls_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("front.log");
        for (kind, expected) in [
            ("Optimizer resource limit", "resource_limit"),
            ("S3 export failed", "resource_limit"),
            ("S3 replay failed", "resource_limit"),
            ("Error reading file", "internal_error"),
            ("Compiler error", "internal_error"),
            ("Error parsing SQL", "sql_error"),
        ] {
            let diagnostic = json!([{"warning":false,"error_type":kind,"message":"test failure"}]);
            fs::write(&log, diagnostic.to_string()).unwrap();
            let (status, reason, messages) = Request::java_result(ExitStatus::from_raw(256), &log)
                .unwrap_err()
                .fields();
            assert_eq!(status, expected, "{kind}");
            if status == "sql_error" {
                assert_eq!(messages, diagnostic);
            } else {
                assert!(
                    reason.as_str().unwrap().contains("test failure"),
                    "{reason}"
                );
            }
        }
    }

    #[test]
    fn optimizer_jvm_enables_native_access() {
        let request: Request = serde_json::from_value(json!({
            "java_classpath":"compiler.jar","producer_commit":"test","runtime_abi":"test",
            "engine":"classic","sql":"test.sql","slots":"slots","deadline_ms":60000
        }))
        .unwrap();
        for rust in [false, true] {
            let command = request.jvm(rust).unwrap();
            let native = command
                .get_args()
                .any(|arg| arg == "--enable-native-access=ALL-UNNAMED");
            assert_eq!(native, rust);
        }
    }
}
