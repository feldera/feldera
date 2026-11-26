//! The parent owns cancellation and the deadline across every stage and retry.
use super::Failure;
use signal_hook::{low_level, SigId};
use std::{
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
    os::fd::AsRawFd,
    os::unix::{
        net::UnixStream,
        process::{CommandExt, ExitStatusExt},
    },
    path::Path,
    process::{Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

const POLL: Duration = Duration::from_millis(5);
const FILE_LIMIT: libc::rlim_t = 128 * 1024 * 1024;
#[cfg(not(target_os = "macos"))]
const WORKER_MEMORY: libc::rlim_t = 1024 * 1024 * 1024;

pub(super) struct Supervisor {
    deadline: Instant,
    cancellation: Cancellation,
    _slot: File,
}

/// Signals wake the supervisor while it is waiting for a slot, checkpoint
/// or child process. The read end is private to this compilation attempt.
struct Cancellation {
    reader: UnixStream,
    registrations: Vec<SigId>,
}

impl Cancellation {
    fn new() -> io::Result<Self> {
        let (reader, writer) = UnixStream::pair()?;
        let mut cancellation = Self {
            reader,
            registrations: Vec::new(),
        };
        for signal in [libc::SIGINT, libc::SIGTERM] {
            cancellation
                .registrations
                .push(low_level::pipe::register(signal, writer.try_clone()?)?);
        }
        Ok(cancellation)
    }

    fn check(&self, deadline: Instant, wait: Duration) -> Result<(), Failure> {
        let mut event = libc::pollfd {
            fd: self.reader.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            let timeout = deadline.saturating_duration_since(Instant::now()).min(wait);
            // POLL is five milliseconds, and poll takes integer milliseconds.
            let millis = timeout.as_nanos().div_ceil(1_000_000) as libc::c_int;
            // SAFETY: event contains a live descriptor and space for one result.
            let count = unsafe { libc::poll(&mut event, 1, millis) };
            if count > 0 {
                // Leave the byte unread. Outer stages must also see a signal
                // when an inner checkpoint worker has already observed it.
                return Err(Failure::Cancelled);
            }
            if count == 0 {
                break;
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error.into());
            }
        }
        if Instant::now() >= deadline {
            return Err(Failure::Limit("compilation deadline expired".into()));
        }
        Ok(())
    }
}

impl Drop for Cancellation {
    fn drop(&mut self) {
        for id in self.registrations.drain(..) {
            low_level::unregister(id);
        }
    }
}

impl Supervisor {
    pub(super) fn new(directory: &Path, budget: Duration) -> Result<Self, Failure> {
        let deadline = Instant::now()
            .checked_add(budget)
            .ok_or_else(|| Failure::Internal("invalid deadline".into()))?;
        let cancellation = Cancellation::new()?;
        fs::create_dir_all(directory)?;
        loop {
            cancellation.check(deadline, Duration::ZERO)?;
            for slot in 0..4 {
                let file = File::options()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(directory.join(format!("worker-{slot}.lock")))?;
                match file.try_lock() {
                    Ok(()) => {
                        return Ok(Self {
                            deadline,
                            cancellation,
                            _slot: file,
                        })
                    }
                    Err(std::fs::TryLockError::WouldBlock) => {}
                    Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
                }
            }
            cancellation.check(deadline, POLL)?;
        }
    }

    pub(super) fn remaining(&self) -> Result<(), Failure> {
        self.cancellation.check(self.deadline, Duration::ZERO)
    }

    pub(super) fn run(
        &self,
        command: &mut Command,
        stderr: &Path,
        rust_worker: bool,
    ) -> Result<ExitStatus, Failure> {
        self.run_child(command, stderr, rust_worker, Stdio::null(), |_| Ok(()))
    }

    /// Run the isolated S3 worker once the front end has closed its checkpoint.
    /// The same JVM waits on stdin until the worker publishes the printer's input.
    pub(super) fn run_with_checkpoint(
        &self,
        command: &mut Command,
        stderr: &Path,
        ready: &Path,
        worker: impl FnOnce() -> Result<(), Failure>,
    ) -> Result<ExitStatus, Failure> {
        let mut pending = Some(worker);
        self.run_child(command, stderr, false, Stdio::piped(), |child| {
            if pending.is_some() && ready.try_exists()? {
                pending.take().expect("pending checkpoint worker")()?;
                child
                    .stdin
                    .take()
                    .ok_or_else(|| io::Error::other("printer input pipe missing"))?
                    .write_all(&[1])?;
            }
            Ok(())
        })
    }

    fn run_child(
        &self,
        command: &mut Command,
        stderr: &Path,
        rust_worker: bool,
        stdin: Stdio,
        mut poll: impl FnMut(&mut std::process::Child) -> Result<(), Failure>,
    ) -> Result<ExitStatus, Failure> {
        self.remaining()?;
        command
            .stdin(stdin)
            .stdout(Stdio::null())
            .stderr(File::create_new(stderr)?);
        // Children inherit the service's process group so cancellation of a program
        // revision also reaches the JVM and the worker while the supervisor is running.
        // SAFETY: the post-fork hook only reads errno and calls getrlimit, setrlimit, and write.
        unsafe {
            command.pre_exec(move || {
                let limit = |resource, value| {
                    let mut cap = libc::rlimit {
                        rlim_cur: 0,
                        rlim_max: 0,
                    };
                    if libc::getrlimit(resource, &mut cap) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                    cap.rlim_max = cap.rlim_cur.min(cap.rlim_max).min(value);
                    cap.rlim_cur = cap.rlim_max;
                    if libc::setrlimit(resource, &cap) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                };
                // spawn returns only errno. Send the limit name through the stage log.
                let set_limit = |resource, value, name: &[u8]| {
                    limit(resource, value).inspect_err(|_| {
                        libc::write(libc::STDERR_FILENO, name.as_ptr().cast(), name.len());
                    })
                };
                set_limit(libc::RLIMIT_CORE, 0, b"RLIMIT_CORE")?;
                set_limit(libc::RLIMIT_FSIZE, FILE_LIMIT, b"RLIMIT_FSIZE")?;
                // Darwin aliases RLIMIT_AS to RLIMIT_RSS and rejects this limit.
                #[cfg(not(target_os = "macos"))]
                if rust_worker {
                    set_limit(libc::RLIMIT_AS, WORKER_MEMORY, b"RLIMIT_AS")?;
                }
                Ok(())
            });
        }
        let mut child = command.spawn().map_err(|error| {
            let stage = stderr.file_stem().unwrap_or_default().to_string_lossy();
            let limit = fs::read_to_string(stderr).unwrap_or_default();
            if limit.is_empty() {
                Failure::Internal(format!("Could not start the {stage} stage: {error}. Check the compiler executable"))
            } else {
                Failure::Internal(format!("Could not set {limit} for the {stage} stage: {error}. Check the process limits"))
            }
        })?;
        let mut interval = Duration::ZERO;
        loop {
            if let Err(stop) = self
                .cancellation
                .check(self.deadline, interval)
                .and_then(|()| poll(&mut child))
            {
                let _ = child.kill();
                child.wait()?;
                return Err(stop);
            }
            if let Some(status) = child.try_wait()? {
                return match status.signal() {
                    Some(libc::SIGXFSZ | libc::SIGKILL) => {
                        Err(Failure::Limit(format!("compiler terminated by {status}")))
                    }
                    Some(libc::SIGABRT) if rust_worker && allocation_failed(stderr)? => {
                        Err(Failure::Limit(
                            "S3 worker memory allocation failed. Compile with the Java optimizer"
                                .into(),
                        ))
                    }
                    Some(_) => Err(Failure::Internal(format!(
                        "compiler terminated by {status}"
                    ))),
                    None => Ok(status),
                };
            }
            interval = POLL;
        }
    }
}

// The Rust allocator prints the failed allocation size before it aborts.
fn allocation_failed(stderr: &Path) -> io::Result<bool> {
    let mut log = File::open(stderr)?;
    let tail = log.metadata()?.len().min(1024) as i64;
    log.seek(SeekFrom::End(-tail))?;
    let mut bytes = Vec::new();
    log.read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).lines().any(|line| {
        line.strip_prefix("memory allocation of ")
            .and_then(|size| size.strip_suffix(" bytes failed"))
            .is_some_and(|size| size.parse::<usize>().is_ok())
    }))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn four_slots_cover_the_whole_attempt_and_release_on_drop() {
        let directory = tempfile::tempdir().unwrap();
        let acquire = || Supervisor::new(directory.path(), Duration::from_secs(2));
        let mut supervisors: Vec<_> = (0..4).map(|_| acquire().unwrap()).collect();
        assert!(matches!(
            Supervisor::new(directory.path(), Duration::from_millis(30)),
            Err(Failure::Limit(_))
        ));
        supervisors.pop();
        assert!(acquire().is_ok());
    }

    #[test]
    fn worker_observes_memory_output_and_core_limits() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = Supervisor::new(directory.path(), Duration::from_secs(10)).unwrap();
        let log = directory.path().join("limits");
        // Capture the process's own limits on stderr, the same descriptor used for worker logs.
        let mut command = Command::new("sh");
        command.args(["-c", "exec cat /proc/self/limits >&2"]);
        let status = supervisor.run(&mut command, &log, true).unwrap();
        assert!(status.success());
        let report = fs::read_to_string(log).unwrap();
        for (name, ceiling) in [
            ("Max address space", WORKER_MEMORY),
            ("Max file size", FILE_LIMIT),
            ("Max core file size", 0),
        ] {
            let line = report
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .unwrap();
            for field in line.split_whitespace().take(2) {
                assert!(field.parse::<u64>().unwrap() <= ceiling, "{name}: {field}");
            }
        }
    }
}

#[cfg(test)]
mod child_tests {
    use super::*;

    #[test]
    fn supervised_worker_starts_with_platform_limits() {
        let dir = tempfile::tempdir().unwrap();
        let supervisor = Supervisor::new(dir.path(), Duration::from_secs(60)).unwrap();
        let mut child = Command::new("sh");
        child.args(["-c", "ulimit -c >&2; ulimit -f >&2"]);
        let log = dir.path().join("worker.log");
        assert!(supervisor.run(&mut child, &log, true).unwrap().success());
        let output = fs::read_to_string(log).unwrap();
        let mut limits = output.lines();
        assert_eq!(limits.next(), Some("0"));
        assert!(limits.next().unwrap().parse::<u64>().unwrap() <= FILE_LIMIT / 512);
    }

    #[test]
    fn only_worker_allocation_aborts_are_resource_limits() {
        let dir = tempfile::tempdir().unwrap();
        let supervisor = Supervisor::new(dir.path(), Duration::from_secs(60)).unwrap();
        for (name, worker, message, kind) in [
            (
                "allocation",
                true,
                "memory allocation of 123 bytes failed",
                "resource_limit",
            ),
            ("abort", true, "assertion failed", "internal_error"),
            (
                "jvm",
                false,
                "memory allocation of 123 bytes failed",
                "internal_error",
            ),
        ] {
            let mut child = Command::new("sh");
            child.args(["-c", "echo \"$1\" >&2; kill -ABRT $$", "sh", message]);
            let failure = supervisor
                .run(&mut child, &dir.path().join(name), worker)
                .unwrap_err();
            assert_eq!(failure.fields().0, kind, "{name}");
        }
    }

    #[test]
    fn worker_allocation_failure_is_a_resource_limit() {
        const CHILD: &str = "SQL2DBSP_TEST_WORKER_ALLOCATION";
        if std::env::var_os(CHILD).is_some() {
            #[cfg(not(target_os = "macos"))]
            let size = WORKER_MEMORY as usize + 1;
            #[cfg(target_os = "macos")]
            let size = isize::MAX as usize;
            std::hint::black_box(Vec::<u8>::with_capacity(size));
            panic!("Worker allocation exceeded the address-space limit");
        }
        let dir = tempfile::tempdir().unwrap();
        let supervisor = Supervisor::new(dir.path(), Duration::from_secs(60)).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .args([
                "--exact",
                "hybrid::supervise::child_tests::worker_allocation_failure_is_a_resource_limit",
                "--nocapture",
            ])
            .env(CHILD, "1");
        let result = supervisor.run(&mut child, &dir.path().join("worker.log"), true);
        assert!(matches!(result, Err(Failure::Limit(_))), "{result:?}");
    }
}
