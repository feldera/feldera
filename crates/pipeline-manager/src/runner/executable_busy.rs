//! Retry policy for local pipeline spawn failures caused by ETXTBSY.
//!
//! Under concurrent local tests the runner can retrieve a pipeline binary and
//! then immediately `exec` it while another task still has the file open for
//! write. The kernel reports that as ETXTBSY ("Text file busy"). The condition
//! is transient: once the writer is gone, the next spawn succeeds.
//!
//! This module classifies the error, bounds the retry budget so the sleeps
//! stay inside the default 20s provisioning timeout (#4952), and retries
//! through an injectable sleeper so tests do not wait for real backoff.

use crate::runner::error::RunnerError;
use std::future::Future;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tracing::warn;

/// Number of times to retry after the first ETXTBSY failure.
pub(crate) const MAX_EXECUTABLE_BUSY_RETRIES: u64 = 10;

/// Default runner provisioning timeout. The sum of backoffs across
/// [`MAX_EXECUTABLE_BUSY_RETRIES`] must stay below this, or a persistent
/// ETXTBSY would surface as a provisioning timeout (#4952) instead of the
/// original spawn error.
#[cfg(test)]
const DEFAULT_PROVISIONING_TIMEOUT: Duration = Duration::from_secs(20);

/// First backoff. A just-written file is usually free in milliseconds.
const EXECUTABLE_BUSY_BACKOFF_INITIAL_MS: u64 = 50;

/// Shift cap so backoff grows 50, 100, 200, 400, then stays at 800ms.
const EXECUTABLE_BUSY_BACKOFF_SHIFT_CAP: u32 = 4;

/// Whether `error` is the "text file busy" spawn failure.
///
/// Match both `ErrorKind::ExecutableFileBusy` and the raw ETXTBSY errno.
/// Current Rust maps the errno to that kind on Unix, but concurrent runs
/// have also surfaced the failure as `os error 26` with a less specific
/// kind, and the kind-only constructor has no raw OS code at all.
pub(crate) fn is_executable_file_busy(error: &Error) -> bool {
    if error.kind() == ErrorKind::ExecutableFileBusy {
        return true;
    }
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(nix::libc::ETXTBSY)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// What to do with a failed `Command::spawn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpawnRetryDecision {
    /// Sleep this long and spawn again.
    RetryAfter(Duration),
    /// The error is ETXTBSY but the retry budget is spent.
    Exhausted,
    /// Not a transient busy error; fail immediately.
    Fatal,
}

/// Backoff for retry `attempt` (0-based), doubling from 50ms and capped at 800ms.
pub(crate) fn executable_busy_backoff(attempt: u64) -> Duration {
    let shift = u32::try_from(attempt)
        .unwrap_or(u32::MAX)
        .min(EXECUTABLE_BUSY_BACKOFF_SHIFT_CAP);

    Duration::from_millis(EXECUTABLE_BUSY_BACKOFF_INITIAL_MS << shift)
}

/// Classify a spawn failure at the given 0-based retry attempt.
pub(crate) fn classify_executable_busy_spawn_error(
    error: &Error,
    attempt: u64,
) -> SpawnRetryDecision {
    if !is_executable_file_busy(error) {
        return SpawnRetryDecision::Fatal;
    }
    if attempt < MAX_EXECUTABLE_BUSY_RETRIES {
        SpawnRetryDecision::RetryAfter(executable_busy_backoff(attempt))
    } else {
        SpawnRetryDecision::Exhausted
    }
}

/// Outcome of [`retry_on_executable_busy`] after the spawn loop stops.
#[derive(Debug)]
pub(crate) enum SpawnError {
    BusyRetriesExhausted { source: Error },
    Other { source: Error },
}

impl SpawnError {
    pub(crate) fn into_provision_error(self) -> RunnerError {
        match self {
            Self::BusyRetriesExhausted { source } => RunnerError::RunnerProvisionError {
                error: format!(
                    "unable to spawn process due to: {source} \
                     (gave up after {MAX_EXECUTABLE_BUSY_RETRIES} ETXTBSY retries)"
                ),
            },
            Self::Other { source } => RunnerError::RunnerProvisionError {
                error: format!("unable to spawn process due to: {source}"),
            },
        }
    }
}

/// Call `spawn` until it succeeds, a non-busy error occurs, or the ETXTBSY
/// budget is exhausted. Uses the production tokio sleeper.
pub(crate) async fn retry_on_executable_busy<T>(
    spawn: impl FnMut() -> Result<T, Error>,
) -> Result<T, SpawnError> {
    retry_on_executable_busy_with_sleeper(spawn, tokio::time::sleep).await
}

/// Same as [`retry_on_executable_busy`], with a sleeper the tests can no-op.
pub(crate) async fn retry_on_executable_busy_with_sleeper<T, S, Fut>(
    mut spawn: impl FnMut() -> Result<T, Error>,
    mut sleeper: S,
) -> Result<T, SpawnError>
where
    S: FnMut(Duration) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut attempt = 0;
    loop {
        match spawn() {
            Ok(value) => return Ok(value),
            Err(error) => match classify_executable_busy_spawn_error(&error, attempt) {
                SpawnRetryDecision::RetryAfter(backoff) => {
                    warn!(
                        attempt = attempt + 1,
                        max_attempts = MAX_EXECUTABLE_BUSY_RETRIES,
                        os_error = ?error.raw_os_error(),
                        backoff_ms = backoff.as_millis() as u64,
                        "pipeline executable is busy (ETXTBSY), retrying..."
                    );
                    sleeper(backoff).await;
                    attempt += 1;
                }
                SpawnRetryDecision::Exhausted => {
                    return Err(SpawnError::BusyRetriesExhausted { source: error });
                }
                SpawnRetryDecision::Fatal => {
                    return Err(SpawnError::Other { source: error });
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_PROVISIONING_TIMEOUT, MAX_EXECUTABLE_BUSY_RETRIES, SpawnError, SpawnRetryDecision,
        classify_executable_busy_spawn_error, executable_busy_backoff, is_executable_file_busy,
        retry_on_executable_busy_with_sleeper,
    };
    use proptest::prelude::*;
    use std::io::{Error, ErrorKind};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    fn busy_kind() -> Error {
        Error::from(ErrorKind::ExecutableFileBusy)
    }

    fn etxtbsy_os_error() -> Error {
        #[cfg(unix)]
        {
            Error::from_raw_os_error(nix::libc::ETXTBSY)
        }
        #[cfg(not(unix))]
        {
            busy_kind()
        }
    }

    /// `ErrorKind::ExecutableFileBusy` with no raw OS code must still retry.
    #[test]
    fn detects_executable_file_busy_kind() {
        let error = busy_kind();
        assert!(error.raw_os_error().is_none());
        assert!(is_executable_file_busy(&error));
        assert!(matches!(
            classify_executable_busy_spawn_error(&error, 0),
            SpawnRetryDecision::RetryAfter(_)
        ));
    }

    /// The errno reported in #4672 ("os error 26") must retry even if we
    /// only looked at `raw_os_error`.
    #[cfg(unix)]
    #[test]
    fn detects_etxtbsy_via_raw_os_error() {
        let error = etxtbsy_os_error();
        assert_eq!(error.raw_os_error(), Some(nix::libc::ETXTBSY));
        assert!(is_executable_file_busy(&error));
    }

    /// Pin the std mapping the detector depends on, the same way compiler
    /// tests pin ENOSPC / EROFS to their `ErrorKind`s.
    #[cfg(unix)]
    #[test]
    fn etxtbsy_errno_maps_to_executable_file_busy_kind() {
        let error = Error::from_raw_os_error(nix::libc::ETXTBSY);
        assert_eq!(error.kind(), ErrorKind::ExecutableFileBusy);
        assert_eq!(error.raw_os_error(), Some(nix::libc::ETXTBSY));
    }

    /// Unrelated spawn failures must not enter the ETXTBSY retry path.
    #[test]
    fn other_spawn_errors_are_fatal() {
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::PermissionDenied,
            ErrorKind::WouldBlock,
            ErrorKind::InvalidInput,
            ErrorKind::UnexpectedEof,
            ErrorKind::Other,
        ] {
            let error = Error::from(kind);
            assert!(!is_executable_file_busy(&error), "{kind:?}");
            assert_eq!(
                classify_executable_busy_spawn_error(&error, 0),
                SpawnRetryDecision::Fatal,
                "{kind:?}"
            );
        }
    }

    /// A typical "binary not there yet" errno must not look like ETXTBSY.
    #[cfg(unix)]
    #[test]
    fn enoent_is_not_executable_busy() {
        let error = Error::from_raw_os_error(nix::libc::ENOENT);
        assert!(!is_executable_file_busy(&error));
        assert_eq!(
            classify_executable_busy_spawn_error(&error, 0),
            SpawnRetryDecision::Fatal
        );
    }

    #[test]
    fn backoff_doubles_then_caps() {
        assert_eq!(executable_busy_backoff(0), Duration::from_millis(50));
        assert_eq!(executable_busy_backoff(1), Duration::from_millis(100));
        assert_eq!(executable_busy_backoff(2), Duration::from_millis(200));
        assert_eq!(executable_busy_backoff(3), Duration::from_millis(400));
        assert_eq!(executable_busy_backoff(4), Duration::from_millis(800));
        assert_eq!(executable_busy_backoff(5), Duration::from_millis(800));
        assert_eq!(
            executable_busy_backoff(u64::MAX),
            Duration::from_millis(800)
        );
    }

    /// Ten retries of the production backoff must not consume the default
    /// 20s provisioning window.
    #[test]
    fn total_retry_budget_fits_default_provision_timeout() {
        let total = (0..MAX_EXECUTABLE_BUSY_RETRIES)
            .map(executable_busy_backoff)
            .fold(Duration::ZERO, |acc, step| acc + step);

        assert!(
            total < DEFAULT_PROVISIONING_TIMEOUT,
            "retry sleeps {total:?} would exceed the default {:?} provisioning timeout",
            DEFAULT_PROVISIONING_TIMEOUT
        );
    }

    #[test]
    fn last_retry_is_allowed_then_exhausted() {
        let error = busy_kind();
        assert!(matches!(
            classify_executable_busy_spawn_error(&error, MAX_EXECUTABLE_BUSY_RETRIES - 1),
            SpawnRetryDecision::RetryAfter(_)
        ));
        assert_eq!(
            classify_executable_busy_spawn_error(&error, MAX_EXECUTABLE_BUSY_RETRIES),
            SpawnRetryDecision::Exhausted
        );
    }

    proptest! {
        /// Any attempt inside the budget retries; any attempt at or past it
        /// is exhausted. Fatal errors are covered by the table test above.
        #[test]
        fn busy_error_retries_exactly_while_budget_remains(attempt in 0u64..128) {
            let error = busy_kind();
            match classify_executable_busy_spawn_error(&error, attempt) {
                SpawnRetryDecision::RetryAfter(backoff)
                    if attempt < MAX_EXECUTABLE_BUSY_RETRIES =>
                {
                    prop_assert_eq!(backoff, executable_busy_backoff(attempt));
                }
                SpawnRetryDecision::Exhausted if attempt >= MAX_EXECUTABLE_BUSY_RETRIES => {}
                other => {
                    return Err(TestCaseError::fail(format!(
                        "unexpected {other:?} for attempt {attempt}"
                    )));
                }
            }
        }

        #[test]
        fn backoff_never_exceeds_cap(attempt in 0u64..10_000) {
            prop_assert!(executable_busy_backoff(attempt) <= Duration::from_millis(800));
            prop_assert!(executable_busy_backoff(attempt) >= Duration::from_millis(50));
        }
    }

    #[tokio::test]
    async fn succeeds_after_transient_etxtbsy() {
        let attempts = AtomicU64::new(0);
        let sleeps = Mutex::new(Vec::new());
        let result = retry_on_executable_busy_with_sleeper(
            || {
                let n = attempts.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(etxtbsy_os_error())
                } else {
                    Ok(n)
                }
            },
            |backoff| {
                sleeps.lock().unwrap().push(backoff);

                std::future::ready(())
            },
        )
        .await;

        assert_eq!(result.unwrap(), 2);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(
            *sleeps.lock().unwrap(),
            vec![executable_busy_backoff(0), executable_busy_backoff(1)]
        );
    }

    #[tokio::test]
    async fn gives_up_after_max_retries() {
        let attempts = AtomicU64::new(0);
        let sleeps = Mutex::new(Vec::new());
        let result: Result<(), _> = retry_on_executable_busy_with_sleeper(
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(busy_kind())
            },
            |backoff| {
                sleeps.lock().unwrap().push(backoff);

                std::future::ready(())
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(SpawnError::BusyRetriesExhausted { .. })
        ));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            MAX_EXECUTABLE_BUSY_RETRIES + 1
        );
        let sleeps = sleeps.lock().unwrap().clone();
        assert_eq!(sleeps.len() as u64, MAX_EXECUTABLE_BUSY_RETRIES);
        assert_eq!(
            sleeps,
            (0..MAX_EXECUTABLE_BUSY_RETRIES)
                .map(executable_busy_backoff)
                .collect::<Vec<_>>()
        );
        let message = result.unwrap_err().into_provision_error().to_string();
        assert!(
            message.contains("gave up after 10 ETXTBSY retries"),
            "unexpected provision error: {message}"
        );
    }

    #[tokio::test]
    async fn does_not_retry_not_found() {
        let attempts = AtomicU64::new(0);
        let result: Result<(), _> = retry_on_executable_busy_with_sleeper(
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(Error::from(ErrorKind::NotFound))
            },
            |_| async { panic!("must not sleep for a fatal spawn error") },
        )
        .await;

        assert!(matches!(result, Err(SpawnError::Other { .. })));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let message = match result {
            Err(error) => error.into_provision_error().to_string(),
            Ok(()) => panic!("expected a spawn error"),
        };
        assert!(
            message.contains("unable to spawn process due to:"),
            "unexpected provision error: {message}"
        );
        assert!(
            !message.contains("ETXTBSY"),
            "fatal errors must not mention the ETXTBSY budget: {message}"
        );
    }
}

#[cfg(all(test, target_os = "linux"))]
mod linux_etxtbsy_tests {
    use super::{is_executable_file_busy, retry_on_executable_busy_with_sleeper};
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Copy `/bin/true` (an ELF, not a shebang script) so `execve` of that
    /// path is what the kernel checks for ETXTBSY.
    fn copied_true() -> Option<(tempfile::TempDir, PathBuf)> {
        let src = ["/usr/bin/true", "/bin/true"]
            .iter()
            .map(Path::new)
            .find(|path| path.is_file())?;
        let dir = tempfile::tempdir().ok()?;
        let dest = dir.path().join("feldera_pipeline_bin");

        std::fs::copy(src, &dest).ok()?;

        let mut perms = std::fs::metadata(&dest).ok()?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&dest, perms).ok()?;

        Some((dir, dest))
    }

    fn spawn_copy(path: &Path) -> Result<tokio::process::Child, std::io::Error> {
        tokio::process::Command::new(path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
    }

    /// The kernel condition the local runner is retrying: a writable open
    /// of the executable makes `execve` return ETXTBSY.
    #[tokio::test]
    async fn spawn_of_file_open_for_write_is_etxtbsy() {
        let Some((_dir, dest)) = copied_true() else {
            panic!("expected /bin/true or /usr/bin/true on Linux CI");
        };
        let mut probe = spawn_copy(&dest).expect("copied true must be executable");
        let status = probe.wait().await.expect("wait on probe");
        assert!(
            status.success(),
            "copied true must run before we pin it busy"
        );

        let _writer = std::fs::OpenOptions::new()
            .write(true)
            .open(&dest)
            .expect("open copied true for write");
        let error = spawn_copy(&dest).expect_err("exec of a writable-open ELF is ETXTBSY");
        assert!(
            is_executable_file_busy(&error),
            "expected ETXTBSY from tokio::process::Command, got {error:?} kind={:?}",
            error.kind()
        );
    }

    /// The production retry loop: fail while a writer is held, succeed after
    /// it is dropped. Sleeps are no-ops so the test stays fast.
    #[tokio::test]
    async fn retries_until_the_write_handle_is_released() {
        let Some((_dir, dest)) = copied_true() else {
            panic!("expected /bin/true or /usr/bin/true on Linux CI");
        };
        let writer = Mutex::new(Some(
            std::fs::OpenOptions::new()
                .write(true)
                .open(&dest)
                .expect("open copied true for write"),
        ));
        let attempts = AtomicU32::new(0);
        let result = retry_on_executable_busy_with_sleeper(
            || {
                let n = attempts.fetch_add(1, Ordering::SeqCst);
                if n == 2 {
                    drop(writer.lock().unwrap().take());
                }

                spawn_copy(&dest)
            },
            |_| std::future::ready(()),
        )
        .await;

        match result {
            Ok(mut child) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
            }
            Err(error) => panic!("expected spawn to succeed after releasing the writer: {error:?}"),
        }
        assert!(
            attempts.load(Ordering::SeqCst) >= 3,
            "writer should force at least two ETXTBSY failures before success"
        );
    }
}
