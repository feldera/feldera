//! Run the Java front end, the isolated Rust S3 worker and the classic Java
//! printer as one compilation. Only a completed attempt becomes visible.
mod package;
mod supervise;

use self::supervise::Supervisor;
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

#[derive(Debug)]
enum Failure {
    Internal(String),
    Limit(String),
    Cancelled,
    Sql(Value),
}

impl From<io::Error> for Failure {
    fn from(value: io::Error) -> Self {
        Self::Internal(value.to_string())
    }
}
impl From<serde_json::Error> for Failure {
    fn from(value: serde_json::Error) -> Self {
        Self::Internal(value.to_string())
    }
}

impl Failure {
    fn fields(self) -> (&'static str, Value, Value) {
        match self {
            Self::Internal(reason) => ("internal_error", reason.into(), json!([])),
            Self::Limit(reason) => ("resource_limit", reason.into(), json!([])),
            Self::Cancelled => ("cancelled", "compilation cancelled".into(), json!([])),
            Self::Sql(messages) => ("sql_error", Value::Null, messages),
        }
    }
}

/// Java emits a JSON diagnostic array on stderr under -je. A JVM startup line
/// may precede it. Accept only a complete array of structured messages.
fn messages(file: &Path) -> Result<Value, Failure> {
    let bytes = fs::read(file)?;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(json!([]));
    }
    for (i, ch) in bytes.iter().enumerate() {
        if *ch != b'[' {
            continue;
        }
        if let Ok(Value::Array(items)) = serde_json::from_slice::<Value>(&bytes[i..]) {
            if items
                .iter()
                .all(|v| v["warning"].is_boolean() && v["message"].is_string())
            {
                return Ok(items.into());
            }
        }
    }
    Err(Failure::Internal(format!(
        "compiler emitted malformed diagnostics in {}",
        file.display()
    )))
}

impl Request {
    fn jvm(&self, rust_passes: bool) -> Result<Command, Failure> {
        let mut command = Command::new("java");
        if rust_passes {
            let binary = env::current_exe()?;
            let directory = binary
                .parent()
                .ok_or_else(|| Failure::Internal("compiler has no parent directory".into()))?;
            let mut library_path = OsString::from("-Djava.library.path=");
            library_path.push(directory);
            command.arg(library_path).arg("-Xss8m");
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
        let entries = diagnostics
            .as_array()
            .expect("messages validates the diagnostic array");
        if let Some(bug) = entries.iter().find(|m| {
            m["warning"] == false
                && matches!(
                    m["error_type"].as_str(),
                    Some(
                        "Compiler error"
                            | "This is a bug in the compiler (please report it to the developers)"
                    )
                )
        }) {
            return Err(Failure::Internal(format!(
                "Java compiler failure: {}",
                bug["message"]
            )));
        }
        if entries.iter().any(|m| m["warning"] == false) {
            return Err(Failure::Sql(diagnostics));
        }
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
                fs::create_dir_all(path.join("crates"))?;
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
        // This JVM serves one SQL request and exits after printing its workspace.
        // Limit JIT compilation to C1 to reduce startup work on the cold path.
        session
            .arg("-XX:TieredStopAtLevel=1")
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
        let mut worker = Command::new(env::current_exe()?);
        worker
            .arg("--s3-worker")
            .arg(&self.producer_commit)
            .arg(&self.runtime_abi)
            .arg(scratch.join("front.s3.json"))
            .arg(&attempt);
        let status = supervisor.run(&mut worker, &scratch.join("worker.log"), true)?;
        let files = attempt.join("result");
        let record: Value = serde_json::from_slice(&fs::read(files.join("result.json"))?)?;
        if record["producer_commit"] != self.producer_commit
            || record["runtime_abi"] != self.runtime_abi
        {
            return Err(Failure::Internal("worker result identity mismatch".into()));
        }
        match record["status"].as_str() {
            Some("ok") if status.success() => (),
            Some("sql_error") => return Err(Failure::Sql(record["messages"].clone())),
            _ => {
                return Err(Failure::Internal(format!(
                    "S3 worker failed with {status}: {}",
                    record["reason"]
                )))
            }
        }
        for name in ["schema.json", "dataflow.json"] {
            fs::rename(files.join(name), out.join(name))?;
        }
        if matches!(self.engine, Engine::Gen2) {
            fs::rename(files.join("circuit.json"), out.join("circuit.json"))?;
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
    for item in fs::read_dir(dir)? {
        let path = item?.path();
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
    let mut file = File::open(path)?;
    let mut buffer = [0; 65536];
    loop {
        supervisor.remaining()?;
        let count = file.read(&mut buffer)?;
        if count == 0 {
            return Ok(digest.finalize().into());
        }
        digest.update(&buffer[..count]);
    }
}

pub(super) fn run(manifest: &Path, attempt: &Path) -> io::Result<ExitCode> {
    let file = File::open(manifest)?;
    if file.metadata()?.len() > 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hybrid request exceeds one MiB",
        ));
    }
    let request: Request = serde_json::from_reader(file)?;
    if request.producer_commit.is_empty()
        || request.runtime_abi.is_empty()
        || request.deadline_ms == 0
        || matches!(request.engine, Engine::Gen2) && request.workspace.is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid compiler identity, deadline, or workspace mode",
        ));
    }
    fs::create_dir(attempt)?;
    let mut ready = tempfile::tempdir_in(attempt)?;
    let mut family = "hybrid";
    let mut attempts = 0;
    let mut hashes = serde_json::Map::new();
    let mut package_sha256 = None;
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
            Err(Failure::Internal(_) | Failure::Limit(_))
                if request.allow_stateless_java_fallback =>
            {
                supervisor.remaining()?;
                ready = tempfile::tempdir_in(attempt)?;
                let scratch = tempfile::tempdir_in(attempt)?;
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
