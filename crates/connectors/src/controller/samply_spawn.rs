//! macOS-specific samply process spawning.
//!
//! On macOS, samply must not be a direct child of the process it profiles
//! (the process deadlocks when trying to run samply on itself).
//! Spawning through a subshell that exits immediately reparents samply to
//! launchd, which allows `task_for_pid` attach to succeed.

use anyhow::{Context, Error as AnyError, bail};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use std::time::Duration;
use tokio::process::Command;
use tracing::info;

/// Quotes `s` for safe embedding in a `/bin/sh -c` script.
fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Returns `true` if a process with `pid` is still running.
async fn process_exists(pid: u32) -> bool {
    tokio::task::spawn_blocking(move || kill(Pid::from_raw(pid as i32), None).is_ok())
        .await
        .unwrap_or(false)
}

/// Blocks until the process `pid` exits.
async fn wait_for_process_exit_unbounded(pid: u32) {
    while process_exists(pid).await {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Blocks until the process `pid` exits or `timeout` elapses.
async fn wait_for_process_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if !process_exists(pid).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Spawns samply in a detached subshell to profile `target_pid`, records for
/// up to `duration` seconds, then returns.
///
/// Samply stdout/stderr are written to a temporary log file. On failure the log
/// contents are included in the error.
pub async fn run_detached_samply_record(
    target_pid: u32,
    profile_file: &str,
    duration: u64,
) -> Result<(), AnyError> {
    let log_file = tempfile::Builder::new()
        .prefix("samply_log_")
        .suffix(".log")
        .rand_bytes(10)
        .tempfile()
        .context("failed to create tempfile for samply log")?;
    let log_path = log_file
        .path()
        .to_str()
        .context("failed to convert samply log path to str")?;

    // The subshell (`( )`) is used to ensure that the samply process is detached from the current process.
    let sh_cmd = format!(
        "( samply record -p {target_pid} -o {} --save-only --presymbolicate > {} 2>&1 & echo $! )",
        sh_quote(profile_file),
        sh_quote(log_path),
    );

    let launcher = Command::new("sh")
        .arg("-c")
        .arg(&sh_cmd)
        .output()
        .await
        .context("failed to spawn detached samply process")?;

    let samply_pid = parse_samply_pid(&launcher.stdout).with_context(|| {
        format!(
            "failed to parse samply pid from launcher output `{}`; launcher status: {}, \
             launcher stderr: `{}`",
            String::from_utf8_lossy(&launcher.stdout).trim(),
            launcher.status,
            String::from_utf8_lossy(&launcher.stderr).trim(),
        )
    })?;

    info!(samply_pid, target_pid, "started detached samply profiler");

    tokio::select! {
        _ = wait_for_process_exit_unbounded(samply_pid) => {}
        _ = tokio::time::sleep(Duration::from_secs(duration)) => {
            kill(Pid::from_raw(samply_pid as i32), Signal::SIGINT)
                .context("failed to send SIGINT to samply process")?;
        }
    }

    if !wait_for_process_exit(samply_pid, Duration::from_secs(30)).await {
        let _ = kill(Pid::from_raw(samply_pid as i32), Signal::SIGKILL);
        wait_for_process_exit(samply_pid, Duration::from_secs(5)).await;
    }

    let log = tokio::fs::read_to_string(log_path)
        .await
        .unwrap_or_default();

    let profile = tokio::fs::read(profile_file).await.unwrap_or_default();
    if profile.is_empty() {
        bail!("samply profile is empty; samply log: `{}`", log.trim());
    }

    if log.contains("Error:") {
        bail!("samply process failed; samply log: `{}`", log.trim());
    }

    Ok(())
}

fn parse_samply_pid(stdout: &[u8]) -> Result<u32, AnyError> {
    let stdout = String::from_utf8_lossy(stdout);
    let pid_str = stdout.trim();
    if pid_str.is_empty() {
        bail!("launcher returned empty pid");
    }
    pid_str
        .parse::<u32>()
        .with_context(|| format!("invalid pid `{pid_str}`"))
}

#[cfg(test)]
mod tests {
    use super::sh_quote;

    #[test]
    fn sh_quote_escapes_single_quotes() {
        assert_eq!(sh_quote("/tmp/foo"), "'/tmp/foo'");
        assert_eq!(sh_quote("/tmp/a'b"), "'/tmp/a'\\''b'");
    }
}
