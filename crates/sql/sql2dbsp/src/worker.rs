//! Emit the SQL compiler tail of a completed compiler attempt. The supervisor reads
//! only the published result directory. A killed worker leaves no ready result.
use crate::{checkpoint::Output, write_json};
use serde_json::{json, Value};
use std::{
    error::Error,
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::Path,
    process::ExitCode,
};

#[derive(Debug)]
pub(super) enum Failure {
    Internal(String),
    Limit(String),
    Cancelled,
    Sql(Value),
}

impl Failure {
    pub(super) fn check_messages(messages: &Value) -> Result<(), Self> {
        let entries = messages.as_array().ok_or_else(|| {
            Self::Internal(
                "Expected an array of compiler diagnostics. Check the compiler log".into(),
            )
        })?;
        let mut sql_error = false;
        for diagnostic in entries.iter().filter(|m| m["warning"] == false) {
            let kind = diagnostic["error_type"].as_str().unwrap_or_default();
            let message = diagnostic["message"].as_str().unwrap_or_default();
            match kind {
                "Optimizer resource limit" | "S3 export failed" | "S3 replay failed" => {
                    return Err(Self::Limit(format!("{kind}: {message}")));
                }
                "Error reading file"
                | "Compiler error"
                | "This is a bug in the compiler (please report it to the developers)" => {
                    return Err(Self::Internal(format!("{kind}: {message}")));
                }
                _ => sql_error = true,
            }
        }
        if sql_error {
            Err(Self::Sql(messages.clone()))
        } else {
            Ok(())
        }
    }

    pub(super) fn fields(self) -> (&'static str, Value, Value) {
        match self {
            Self::Internal(reason) => ("internal_error", reason.into(), json!([])),
            Self::Limit(reason) => ("resource_limit", reason.into(), json!([])),
            Self::Cancelled => ("cancelled", "compilation cancelled".into(), json!([])),
            Self::Sql(messages) => ("sql_error", Value::Null, messages),
        }
    }
}

fn write_file(dir: &Path, name: &str, document: &Value) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create_new(dir.join(name))?);
    write_json(&mut writer, document)?;
    writer.flush()
}

fn emit(input: &Path, producer: &str, runtime: &str) -> Result<Value, Box<dyn Error>> {
    let input = fs::read(input)?;
    let checkpoint = feldera_sql_ir::parse_circuit_envelope(&input)?;
    if producer.is_empty()
        || runtime.is_empty()
        || checkpoint["producer_commit"] != producer
        || checkpoint["runtime_abi"] != runtime
    {
        return Err("S3 producer or runtime ABI differs from the requested identity".into());
    }
    Output::Artifacts.emit_checkpoint(checkpoint)
}

/// The caller assigns a fresh attempt directory to each invocation. Owning it
/// exclusively prevents concurrent retries from mixing files. A single rename
/// publishes the status, messages, compiler identity and successful artifacts.
pub(super) fn run(
    input: &Path,
    attempt: &Path,
    producer: &str,
    runtime: &str,
) -> io::Result<ExitCode> {
    fs::create_dir(attempt)?;
    let staging = tempfile::tempdir_in(attempt)?;
    let mut result = json!({"schema_version":1,"compiler":"sql2dbsp","worker_version":env!("CARGO_PKG_VERSION"),
        "producer_commit":producer,"runtime_abi":runtime,"messages":[]});
    let status = match emit(input, producer, runtime) {
        Ok(mut artifacts) => {
            result["messages"] = artifacts["diagnostics"].take();
            if let Err(failure) = Failure::check_messages(&result["messages"]) {
                let (status, reason, messages) = failure.fields();
                result["status"] = status.into();
                result["messages"] = messages;
                if !reason.is_null() {
                    result["reason"] = reason;
                }
                ExitCode::FAILURE
            } else {
                let executable_input = if artifacts.get("checkpoint").is_some() {
                    ("checkpoint", "s3.json")
                } else {
                    ("circuit", "circuit.json")
                };
                for (artifact, filename) in [
                    executable_input,
                    ("schema", "schema.json"),
                    ("dataflow", "dataflow.json"),
                ] {
                    write_file(staging.path(), filename, &artifacts[artifact])?;
                }
                write_file(staging.path(), "diagnostics.json", &result["messages"])?;
                result["status"] = "ok".into();
                ExitCode::SUCCESS
            }
        }
        Err(error) => {
            result["status"] = "internal_error".into();
            result["reason"] = error.to_string().into();
            ExitCode::FAILURE
        }
    };
    write_file(staging.path(), "result.json", &result)?;
    fs::rename(staging.path(), attempt.join("result"))?;
    Ok(status)
}
