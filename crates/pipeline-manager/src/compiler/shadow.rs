//! Bounded, observational SQL compilation. This module has no database or
//! artifact-delivery handle. Differences can produce a private local review bundle.

pub(super) mod hybrid;
mod mismatch;
mod sample;

use super::sql_compiler::{
    SqlCompilationError, SqlCompilationOutput, SqlCompilationRequest, SqlCompilationResult,
    ephemeral_compilation_dir, perform_sql_compilation_inner,
};
use super::util::ProcessGroupTerminator;
use crate::config::{CommonConfig, CompilerConfig};
use crate::db::types::{
    pipeline::PipelineId, program::SqlCompilationInfo, tenant::TenantId, version::Version,
};
use nix::libc;
use serde::Deserialize;
use serde_json::Value;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::{
    process::{Child, Command},
    sync::Semaphore,
    task::JoinHandle,
    time::{Duration, Instant, timeout_at},
};
use tracing::{Dispatch, info, instrument::WithSubscriber, warn};
use uuid::Uuid;

const MAX_SQL_BYTES: usize = 1024 * 1024;
const MAX_OBSERVATION_BYTES: usize = 4 * 1024 * 1024;
const MAX_OUTPUT_FILE_BYTES: libc::rlim_t = 16 * 1024 * 1024;
static SLOT: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(1)));
static SKIPPED: AtomicU64 = AtomicU64::new(0);
static COMPLETED: AtomicU64 = AtomicU64::new(0);
// Stop admission after cleanup failure so orphaned outputs cannot accumulate.
static CLEANUP_HEALTHY: AtomicBool = AtomicBool::new(true);

#[derive(Debug, PartialEq, Eq)]
enum Observation {
    Success(serde_json::Map<String, Value>, SqlCompilationInfo),
    SqlError(SqlCompilationInfo),
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Difference {
    Verdict,
    Diagnostics,
    Schema,
    Dataflow,
    CircuitIr,
    ProgramInfo,
}

/// Count serialized bytes without buffering or copying a large primary result.
struct ByteBudget(usize);

impl Write for ByteBudget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_sub(bytes.len())
            .ok_or_else(|| io::Error::other("shadow observation exceeds 4 MiB"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Observation {
    fn compare(self, reference: Self) -> Result<(), Difference> {
        match (self, reference) {
            (Self::Success(mut actual, diagnostics), Self::Success(mut expected, other)) => {
                if diagnostics != other {
                    return Err(Difference::Diagnostics);
                }
                // Gen-2 node numbers are local to each compiler. Compare graph edges,
                // sharing and persistent IDs using the same contract as corpus replay.
                let left = actual.remove("circuit_ir");
                let right = expected.remove("circuit_ir");
                if actual != expected {
                    return Err(if actual.get("schema") != expected.get("schema") {
                        Difference::Schema
                    } else if actual.get("dataflow") != expected.get("dataflow") {
                        Difference::Dataflow
                    } else {
                        Difference::ProgramInfo
                    });
                }
                match (&left, &right) {
                    (Some(a), Some(b)) if a.is_object() && b.is_object() => {
                        feldera_sql_difftest::compare_circuits(b, a)
                            .map_err(|_| Difference::CircuitIr)
                    }
                    _ if left == right => Ok(()),
                    _ => Err(Difference::CircuitIr),
                }
            }
            (Self::SqlError(actual), Self::SqlError(expected)) if actual == expected => Ok(()),
            (Self::SqlError(_), Self::SqlError(_)) => Err(Difference::Diagnostics),
            _ => Err(Difference::Verdict),
        }
    }

    fn from_result(result: &SqlCompilationResult) -> Result<Self, Failure> {
        let mut budget = ByteBudget(MAX_OBSERVATION_BYTES);
        match result {
            Ok((program, _, diagnostics)) => {
                // Generated Rust is a potentially large archive, and the shadow
                // never generates or publishes it. Retain every semantic field.
                let fields = program
                    .as_object()
                    .ok_or(Failure::InvalidObservation)?
                    .iter()
                    .filter(|(key, _)| !matches!(key.as_str(), "main_rust" | "udf_stubs"));
                serde_json::to_writer(&mut budget, diagnostics)
                    .map_err(|_| Failure::ObservationTooLarge)?;
                for entry in fields.clone() {
                    serde_json::to_writer(&mut budget, &entry)
                        .map_err(|_| Failure::ObservationTooLarge)?;
                }
                let fields = fields
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                Ok(Self::Success(fields, diagnostics.clone()))
            }
            Err(SqlCompilationError::SqlError(info)) => {
                serde_json::to_writer(&mut budget, info)
                    .map_err(|_| Failure::ObservationTooLarge)?;
                Ok(Self::SqlError(info.clone()))
            }
            Err(SqlCompilationError::TerminatedBySignal) => Err(Failure::TerminatedBySignal),
            Err(SqlCompilationError::SystemError(_)) => Err(Failure::SystemError),
            Err(SqlCompilationError::NoLongerExists | SqlCompilationError::Outdated) => {
                Err(Failure::Obsolete)
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Failure {
    InvalidObservation,
    ObservationTooLarge,
    TerminatedBySignal,
    SystemError,
    Obsolete,
    Cleanup,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Outcome {
    Equal,
    Different,
    Failed(Failure),
    TimedOut,
}

/// Does not queue work: a busy shadow slot causes this sample to be skipped.
/// Dropping the returned handle detaches the bounded task, as the primary caller
/// does. The task owns no reference to a mutable primary result.
pub(super) fn launch(
    common: &CommonConfig,
    config: &CompilerConfig,
    program_config: &Value,
    sql: &str,
    primary: &SqlCompilationResult,
) -> Option<JoinHandle<Outcome>> {
    let compiler = config.shadow_sql_compiler?;
    let skipped = |reason: &str| {
        let skipped = SKIPPED.fetch_add(1, Ordering::Relaxed) + 1;
        info!(reason, skipped, "SQL shadow skipped");
        None
    };
    if sql.len() > MAX_SQL_BYTES {
        return skipped("SQL exceeds 1 MiB");
    }
    let Ok(permit) = SLOT.clone().try_acquire_owned() else {
        return skipped("worker is busy");
    };
    if !CLEANUP_HEALTHY.load(Ordering::Relaxed) {
        return skipped("cleanup failed");
    }
    let reference = match Observation::from_result(primary) {
        Ok(reference) => reference,
        Err(Failure::ObservationTooLarge) => return skipped("primary observation exceeds 4 MiB"),
        Err(_) => return skipped("primary result is unavailable"),
    };
    let Ok(mut selected) = crate::db::types::program::ProgramConfig::deserialize(program_config)
    else {
        return skipped("invalid program configuration");
    };
    selected.compiler = Some(crate::db::types::program::CompilerFamily::Java);
    let Ok(program_config) = serde_json::to_value(selected) else {
        return skipped("cannot serialize program configuration");
    };
    let accepted = matches!(reference, Observation::Success(..));
    let deadline = Duration::from_secs(config.shadow_sql_compiler_timeout_secs);
    let common = common.clone();
    let config = config.clone();
    let sql = sql.to_owned();
    Some(tokio::spawn(async move {
        let outcome = run(&common, &config, &program_config, &sql, reference, deadline).await;
        if compiler == crate::config::ShadowCompiler::RustOptimizer {
            sample::record(&sql, accepted, &outcome);
        }
        drop(permit);
        let completed = COMPLETED.fetch_add(1, Ordering::Release) + 1;
        match &outcome {
            Outcome::Failed(class) => {
                info!(outcome = "Failed", failure_class = ?class, completed, "SQL shadow completed")
            }
            _ => info!(?outcome, completed, "SQL shadow completed"),
        }
        outcome
    }))
}

async fn run(
    common: &CommonConfig,
    config: &CompilerConfig,
    program_config: &Value,
    sql: &str,
    reference: Observation,
    deadline: Duration,
) -> Outcome {
    let id = PipelineId(Uuid::now_v7());
    let directory = ephemeral_compilation_dir(config).join(id.to_string());
    let expires = Instant::now() + deadline;
    // Primary compiler logs can contain SQL and connector settings.
    let result = async {
        timeout_at(
            expires,
            perform_sql_compilation_inner(SqlCompilationRequest {
                common_config: common,
                config,
                db: None,
                tenant_id: TenantId(Uuid::nil()),
                pipeline_id: id,
                pipeline_name: None,
                platform_version: &common.platform_version,
                program_version: Version(1),
                program_config,
                program_code: sql,
                output: SqlCompilationOutput::Shadow,
            }),
        )
        .await
    }
    .with_subscriber(Dispatch::none())
    .await;
    let outcome = match result {
        Err(_) => Outcome::TimedOut,
        Ok(result) => match Observation::from_result(&result) {
            Ok(actual) => match actual.compare(reference) {
                Ok(()) => Outcome::Equal,
                Err(difference) => {
                    if config.shadow_sql_compiler
                        == Some(crate::config::ShadowCompiler::RustOptimizer)
                        && let Err(error) = mismatch::retain(
                            config,
                            program_config,
                            &directory,
                            id.0,
                            difference,
                            expires,
                        )
                        .await
                    {
                        warn!(kind = ?error.kind(), "Could not retain the local SQL optimizer mismatch bundle");
                    }
                    Outcome::Different
                }
            },
            Err(class) => Outcome::Failed(class),
        },
    };
    // Timeout drops the attempt's ProcessGroupTerminator before cleanup. The
    // semaphore remains held until cleanup finishes, including after a failure.
    finish(
        outcome,
        tokio::fs::remove_dir_all(&directory).await,
        &CLEANUP_HEALTHY,
    )
}

fn finish(outcome: Outcome, cleanup: io::Result<()>, healthy: &AtomicBool) -> Outcome {
    if let Err(error) = cleanup
        && error.kind() != io::ErrorKind::NotFound
    {
        healthy.store(false, Ordering::Relaxed);
        warn!(
            "SQL shadow cleanup failed. Check the compiler working directory and restart the compiler server to enable shadows."
        );
        // A known difference must still reset the field window.
        if outcome != Outcome::Different {
            return Outcome::Failed(Failure::Cleanup);
        }
    }
    outcome
}

/// Limit each output file in the child. Shadow classic compilation uses
/// --noRust, so only schema/dataflow/diagnostic files are written; Gen-2 also
/// writes its circuit IR. JVM limits bound heap, metaspace, and direct buffers,
/// not total RSS; a hard RSS quota belongs to the deployment's cgroup budget.
pub(super) fn limit_output_files(command: &mut Command) {
    command.kill_on_drop(true);
    // SAFETY: the post-fork closure calls only the async-signal-safe setrlimit
    // syscall and constructs an OS error without allocation, locks, or logging.
    unsafe {
        command.pre_exec(|| {
            let no_core = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if libc::setrlimit(libc::RLIMIT_CORE, &no_core) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let limit = libc::rlimit {
                rlim_cur: MAX_OUTPUT_FILE_BYTES,
                rlim_max: MAX_OUTPUT_FILE_BYTES,
            };
            if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
}

/// JVM options must precede the jar and its application arguments.
pub(super) fn limit_jvm(command: &mut Command) {
    command.args([
        "-Xmx512m",
        "-XX:MaxMetaspaceSize=128m",
        "-XX:MaxDirectMemorySize=64m",
        "-XX:ActiveProcessorCount=1",
        "-XX:-HeapDumpOnOutOfMemoryError",
        "-XX:ErrorFile=/dev/null",
    ]);
}

/// A shadow owns its entire process group, including helpers whose leader exits
/// first. Observe the leader without reaping it, kill remaining group members,
/// and only then reap. This ordering prevents reuse of the leader's process id
/// between observing its exit and signalling its group.
pub(super) fn try_wait(
    process: &mut Child,
    terminator: &mut ProcessGroupTerminator,
) -> io::Result<Option<std::process::ExitStatus>> {
    let Some(id) = process.id() else {
        terminator.cancel();
        return process.try_wait();
    };
    // SAFETY: siginfo_t accepts zero initialization, and waitid writes only to
    // this live, aligned local object. P_PID names our owned child. WNOWAIT keeps
    // it waitable; WNOHANG leaves si_pid zero when there is no exit to report.
    // libc's accessor reads the SIGCHLD member populated by waitid. Using libc
    // here also supports macOS, where nix 0.29 does not expose waitid.
    let (result, exited_pid) = unsafe {
        let mut status: libc::siginfo_t = std::mem::zeroed();
        let result = libc::waitid(
            libc::P_PID,
            id as libc::id_t,
            &mut status,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        );
        (result, status.si_pid())
    };
    if result != 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINTR) {
            return Ok(None);
        }
        if error.raw_os_error() == Some(libc::ECHILD) {
            // Our polling loop never polls Child::wait, and Tokio's signal/pidfd
            // drivers only wake waiters; they do not reap an owned, unpolled
            // child. If another caller violated that ownership, its old id is
            // no longer safe to signal. Fail the observation instead.
            terminator.cancel();
        }
        return Err(error);
    }
    if exited_pid == 0 {
        return Ok(None);
    }
    match terminator.terminate() {
        Ok(()) => {}
        #[cfg(target_os = "macos")]
        Err(nix::errno::Errno::EPERM) if group_contains_only_leader(exited_pid) => {
            // macOS returns EPERM when the only member is the exited leader.
            terminator.cancel();
        }
        Err(error) => return Err(error.into()),
    }
    process.try_wait()
}

#[cfg(target_os = "macos")]
fn group_contains_only_leader(leader: libc::pid_t) -> bool {
    let mut members: [libc::pid_t; 2] = [0; 2];
    // SAFETY: the buffer holds two aligned process IDs and the size is in bytes.
    let count = unsafe {
        libc::proc_listpgrppids(
            leader,
            members.as_mut_ptr().cast(),
            size_of_val(&members) as libc::c_int,
        )
    };
    count == 1 && members[0] == leader
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use serde_json::json;
    use std::process::Stdio;
    use tokio::time::timeout;

    #[test]
    fn cleanup_failure_preserves_a_known_difference_and_stops_admission() {
        let healthy = AtomicBool::new(true);
        let failure = || Err(io::ErrorKind::PermissionDenied.into());
        assert_eq!(
            finish(Outcome::Different, failure(), &healthy),
            Outcome::Different
        );
        assert!(!healthy.load(Ordering::Relaxed));
        assert_eq!(
            finish(Outcome::Equal, failure(), &healthy),
            Outcome::Failed(Failure::Cleanup)
        );
    }

    #[tokio::test]
    async fn exited_shadow_leader_does_not_leave_group_members_running() {
        // Keep the leader alive until the second process joins its group. Both
        // children belong to this test so it can reap them, even when PID 1 does
        // not reap orphans (as in some devcontainers).
        let mut leader = Command::new("sh")
            .args(["-c", "cat >/dev/null"])
            .stdin(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let group = leader.id().unwrap();
        let mut terminator = ProcessGroupTerminator::new("shadow regression", group);
        let mut member = Command::new("sleep")
            .arg("60")
            .process_group(i32::try_from(group).unwrap())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        drop(leader.stdin.take());
        let status = timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = try_wait(&mut leader, &mut terminator).unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("leader must exit after its input closes");
        assert!(status.success());
        let completion = timeout(Duration::from_secs(5), member.wait()).await;
        if completion.is_err() {
            // Keep the failing regression from leaking a process or a zombie.
            member.kill().await.unwrap();
        }
        let status = completion
            .expect("shadow group member must be killed after leader exit")
            .unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGKILL));
    }

    #[tokio::test]
    async fn externally_reaped_leader_is_an_error_and_its_old_id_is_not_signalled() {
        let mut leader = Command::new("sh")
            .args(["-c", "exit 0"])
            .process_group(0)
            .spawn()
            .unwrap();
        let group = leader.id().unwrap();
        let mut terminator = ProcessGroupTerminator::new("reaped shadow regression", group);
        // Simulate a caller violating Child ownership. The supervisor must not
        // signal this numerical id once waitid can no longer reserve it.
        nix::sys::wait::waitpid(nix::unistd::Pid::from_raw(group as i32), None).unwrap();
        let error = try_wait(&mut leader, &mut terminator).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ECHILD));
        // A cancelled guard is inert even if its former group id is reused.
        terminator.terminate().unwrap();
    }

    #[tokio::test]
    async fn output_file_limit_is_enforced_by_the_child() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("output");
        let mut command = Command::new("sh");
        command
            .args(["-c", "exec head -c 33554432 /dev/zero"])
            .stdout(Stdio::from(std::fs::File::create(&path).unwrap()))
            .stderr(Stdio::null());
        limit_output_files(&mut command);
        assert!(!command.status().await.unwrap().success());
        assert!(path.metadata().unwrap().len() <= MAX_OUTPUT_FILE_BYTES);
    }

    #[test]
    fn observation_keeps_schema_dataflow_and_diagnostics() {
        let result = |info| Ok((info, Duration::ZERO, SqlCompilationInfo::success()));
        let primary = result(
            json!({"schema": {}, "dataflow": {"id": "same"}, "main_rust": "archive", "udf_stubs": "stubs"}),
        );
        let shadow = result(
            json!({"schema": {}, "dataflow": {"id": "same"}, "main_rust": "", "udf_stubs": ""}),
        );
        assert_eq!(
            Observation::from_result(&primary),
            Observation::from_result(&shadow)
        );
        let changed = result(json!({"schema": {}, "dataflow": {"id": "changed"}}));
        assert_ne!(
            Observation::from_result(&primary),
            Observation::from_result(&changed)
        );
        assert_ne!(
            Observation::from_result(&primary),
            Observation::from_result(&Err(SqlCompilationError::SqlError(
                SqlCompilationInfo::success()
            )))
        );
    }

    #[test]
    fn observation_budget_excludes_generated_artifacts() {
        let large = "x".repeat(MAX_OBSERVATION_BYTES + 1);
        let mut program = json!({"schema": {}, "main_rust": large});
        let result = |program| Ok((program, Duration::ZERO, SqlCompilationInfo::success()));
        assert!(Observation::from_result(&result(program.clone())).is_ok());
        program["schema"] = program["main_rust"].take();
        assert_eq!(
            Observation::from_result(&result(program)),
            Err(Failure::ObservationTooLarge)
        );
    }

    fn real_compiler_config(directory: &std::path::Path) -> CompilerConfig {
        let mut config = CompilerConfig::parse_from([
            "test",
            "--shadow-sql-compiler",
            "java",
            "--shadow-sql-compiler-timeout-secs",
            "120",
        ]);
        config.sql_compiler_path = std::env::var("FELDERA_SHADOW_TEST_JAR").unwrap();
        config.compiler_working_directory = directory.to_string_lossy().into_owned();
        config
    }

    #[tokio::test]
    #[ignore = "set FELDERA_SHADOW_TEST_JAR to the SQL compiler jar"]
    async fn full_java_primary_matches_no_rust_shadow() {
        check_full_primary_against_shadow(crate::config::ShadowCompiler::Java, &json!({})).await;
    }

    #[tokio::test]
    #[ignore = "set FELDERA_SHADOW_TEST_JAR and put sql2dbsp on PATH"]
    async fn full_java_primary_matches_hybrid_shadow() {
        check_full_primary_against_shadow(crate::config::ShadowCompiler::Hybrid, &json!({})).await;
    }

    #[tokio::test]
    #[ignore = "set FELDERA_SHADOW_TEST_JAR and put sql2dbsp on PATH"]
    async fn full_gen2_primary_matches_hybrid_shadow() {
        crate::enable_test_unstable_features();
        check_full_primary_against_shadow(
            crate::config::ShadowCompiler::Hybrid,
            &json!({"runtime_version":"gen2"}),
        )
        .await;
    }

    #[tokio::test]
    #[ignore = "requires a matching SQL compiler jar, sql2dbsp and the native optimizer library"]
    async fn full_primary_matches_rust_optimizer_shadow_in_both_engines() {
        crate::enable_test_unstable_features();
        for program in [json!({}), json!({"runtime_version":"gen2"})] {
            check_full_primary_against_shadow(
                crate::config::ShadowCompiler::RustOptimizer,
                &program,
            )
            .await;
        }
    }

    #[tokio::test]
    #[ignore = "requires a matching SQL compiler jar, sql2dbsp and the native optimizer library"]
    async fn optimizer_difference_retains_only_the_anonymized_review_bundle() {
        let root = tempfile::tempdir().unwrap();
        let mut config = real_compiler_config(root.path());
        config.shadow_sql_compiler = Some(crate::config::ShadowCompiler::RustOptimizer);
        let sql = "CREATE TABLE private_shadow_input(i INT) WITH ('connectors' = '[]'); \
                   CREATE VIEW private_shadow_view AS SELECT i FROM private_shadow_input;";
        let outcome = run(
            &CommonConfig::test_config(),
            &config,
            &json!({}),
            sql,
            Observation::SqlError(SqlCompilationInfo::success()),
            Duration::from_secs(120),
        )
        .await;
        assert_eq!(outcome, Outcome::Different);
        let files: Vec<_> = std::fs::read_dir(root.path().join("sql-shadow-mismatches"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(files.len(), 1);
        let bundle: Value = serde_json::from_slice(&std::fs::read(&files[0]).unwrap()).unwrap();
        assert_eq!(bundle["first_difference"], "verdict");
        assert_eq!(bundle["anonymized_sql_status"], "available");
        let anonymized = bundle["anonymized_sql"].as_str().unwrap().to_lowercase();
        assert!(anonymized.contains("create table id_"));
        assert!(!anonymized.contains("private_shadow") && !anonymized.contains("connectors"));
        assert_eq!(
            std::fs::read_dir(ephemeral_compilation_dir(&config))
                .unwrap()
                .count(),
            0
        );
    }

    async fn check_full_primary_against_shadow(
        family: crate::config::ShadowCompiler,
        program_config: &Value,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let mut config = real_compiler_config(directory.path());
        config.shadow_sql_compiler = Some(family);
        let common = CommonConfig::test_config();
        let id = PipelineId(Uuid::now_v7());
        let sql = "CREATE TABLE t(i INT); CREATE VIEW v AS SELECT i + 1 AS j FROM t;";
        let full = perform_sql_compilation_inner(SqlCompilationRequest {
            common_config: &common,
            config: &config,
            db: None,
            tenant_id: TenantId(Uuid::nil()),
            pipeline_id: id,
            pipeline_name: None,
            platform_version: &common.platform_version,
            program_version: Version(1),
            program_config,
            program_code: sql,
            output: SqlCompilationOutput::Full,
        })
        .await;
        let program = &full.as_ref().unwrap().0;
        let runtime = crate::db::types::program::ProgramConfig::deserialize(program_config)
            .unwrap()
            .runtime_version();
        if runtime.is_gen2() {
            assert!(program["circuit_ir"].is_object());
        } else {
            assert!(!program["main_rust"].as_str().unwrap().is_empty());
        }
        let log_path = directory.path().join("shadow.log");
        let subscriber = tracing_subscriber::fmt()
            .with_writer(std::fs::File::create(&log_path).unwrap())
            .finish();
        let outcome = run(
            &common,
            &config,
            program_config,
            sql,
            Observation::from_result(&full).unwrap(),
            Duration::from_secs(config.shadow_sql_compiler_timeout_secs),
        )
        .with_subscriber(subscriber)
        .await;
        assert_eq!(outcome, Outcome::Equal, "{family:?} shadow for {runtime:?}");
        assert!(std::fs::read(log_path).unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore = "set FELDERA_SHADOW_TEST_JAR to the SQL compiler jar"]
    async fn real_java_shadow_is_bounded_and_does_not_publish() {
        let directory = tempfile::tempdir().unwrap();
        let config = real_compiler_config(directory.path());
        let common = CommonConfig::test_config();
        let sql = "CREATE TABLE t(i INT); CREATE VIEW v AS SELECT i + 1 AS j FROM t;";
        let id = PipelineId(Uuid::now_v7());
        let completed = COMPLETED.load(Ordering::Acquire);
        let primary = super::super::sql_compiler::perform_sql_compilation(
            &common,
            &config,
            None,
            TenantId(Uuid::nil()),
            id,
            None,
            &common.platform_version,
            Version(1),
            &json!({}),
            sql,
            SqlCompilationOutput::IrOnly,
        )
        .await;
        assert!(primary.is_ok(), "{primary:?}");
        timeout(
            Duration::from_secs(config.shadow_sql_compiler_timeout_secs + 5),
            async {
                while COMPLETED.load(Ordering::Acquire) == completed {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
        )
        .await
        .expect("the primary entry point must launch and finish a shadow");
        let primary_schema = ephemeral_compilation_dir(&config)
            .join(id.to_string())
            .join("schema.json");
        let before = tokio::fs::read(&primary_schema).await.unwrap();
        let handle = launch(&common, &config, &json!({}), sql, &primary).unwrap();
        assert!(launch(&common, &config, &json!({}), sql, &primary).is_none());
        assert_eq!(handle.await.unwrap(), Outcome::Equal);
        assert_eq!(tokio::fs::read(&primary_schema).await.unwrap(), before);
        let (program, elapsed, diagnostics) = primary.as_ref().unwrap();
        let mut changed = program.clone();
        changed["extra_semantic_field"] = json!(true);
        let changed_reference = Ok((changed, *elapsed, diagnostics.clone()));
        assert_eq!(
            launch(&common, &config, &json!({}), sql, &changed_reference)
                .unwrap()
                .await
                .unwrap(),
            Outcome::Different,
        );
        let mut broken = config.clone();
        broken.sql_compiler_path = directory
            .path()
            .join("missing.jar")
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            launch(&common, &broken, &json!({}), sql, &primary)
                .unwrap()
                .await
                .unwrap(),
            Outcome::Failed(Failure::SystemError),
        );
        assert!(
            launch(
                &common,
                &config,
                &json!({}),
                &"x".repeat(MAX_SQL_BYTES + 1),
                &primary
            )
            .is_none()
        );
        let mut entries = tokio::fs::read_dir(ephemeral_compilation_dir(&config))
            .await
            .unwrap();
        assert_eq!(
            entries.next_entry().await.unwrap().unwrap().file_name(),
            id.to_string().as_str()
        );
        assert!(entries.next_entry().await.unwrap().is_none());
        let reference = Observation::from_result(&primary).unwrap();
        assert_eq!(
            run(&common, &config, &json!({}), sql, reference, Duration::ZERO).await,
            Outcome::TimedOut
        );
        assert_eq!(tokio::fs::read(&primary_schema).await.unwrap(), before);

        // A zero-duration timeout can expire before spawn. Also prove that a
        // timeout kills a child that has started and entered a blocking wait.
        let source = directory.path().join("ShadowSleeper.java");
        std::fs::write(
            &source,
            r#"import java.nio.file.*;
public class ShadowSleeper {
    public static void main(String[] args) throws Exception {
        Path marker = Path.of(Files.readString(Path.of(args[0])));
        Files.writeString(marker, Long.toString(ProcessHandle.current().pid()));
        Thread.sleep(300000);
    }
}
"#,
        )
        .unwrap();
        assert!(
            Command::new("javac")
                .arg(&source)
                .status()
                .await
                .unwrap()
                .success()
        );
        let jar = directory.path().join("sleeper.jar");
        assert!(
            Command::new("jar")
                .args(["--create", "--file"])
                .arg(&jar)
                .args(["--main-class", "ShadowSleeper", "-C"])
                .arg(directory.path())
                .arg("ShadowSleeper.class")
                .status()
                .await
                .unwrap()
                .success()
        );
        let marker = directory.path().join("started.pid");
        let mut sleeping = config.clone();
        sleeping.sql_compiler_path = jar.to_string_lossy().into_owned();
        let deadline = Duration::from_secs(config.shadow_sql_compiler_timeout_secs);
        let program_config = json!({});
        let (outcome, pid) = tokio::join!(
            run(
                &common,
                &sleeping,
                &program_config,
                marker.to_str().unwrap(),
                Observation::from_result(&primary).unwrap(),
                deadline
            ),
            async {
                let pid = timeout(deadline, async {
                    loop {
                        if let Ok(text) = tokio::fs::read_to_string(&marker).await
                            && let Ok(pid) = text.parse::<i32>()
                        {
                            break pid;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .expect("the child must write its PID before the deadline");
                // Advance the deadline after the JVM has started.
                tokio::time::pause();
                tokio::time::advance(deadline).await;
                tokio::time::resume();
                pid
            }
        );
        assert_eq!(outcome, Outcome::TimedOut);
        timeout(Duration::from_secs(5), async {
            while nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None)
                != Err(nix::errno::Errno::ESRCH)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed-out shadow child must be killed and reaped");
        assert_eq!(tokio::fs::read(&primary_schema).await.unwrap(), before);
    }
}
