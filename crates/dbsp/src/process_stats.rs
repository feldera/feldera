//! Resource usage of the current process, for benchmark reports.

use serde::Serialize;
use std::{io::Error as IoError, mem::MaybeUninit};

/// CPU time, peak memory, and page faults for the current process.
///
/// All values are totals since the process started. To get the usage of one
/// part of a program, take a sample before and after it and subtract.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ProcessStats {
    /// CPU time in user mode, in milliseconds.
    pub user_ms: u64,
    /// CPU time in kernel mode, in milliseconds.
    pub system_ms: u64,
    /// Peak resident set size, in bytes.
    pub peak_rss: u64,
    /// Minor and major page faults.
    pub page_faults: u64,
}

impl ProcessStats {
    /// Samples the resource usage of the current process.
    ///
    /// # Panics
    ///
    /// Panics if `getrusage` fails. POSIX lets it fail only for a bad
    /// argument, so this does not occur in practice.
    pub fn current() -> Self {
        let mut usage = MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: `usage` is valid for writes of one `rusage`.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            panic!("getrusage failed: {}", IoError::last_os_error());
        }
        // SAFETY: `getrusage` succeeded, so it initialized `usage`.
        let usage = unsafe { usage.assume_init() };

        Self {
            user_ms: timeval_to_ms(usage.ru_utime),
            system_ms: timeval_to_ms(usage.ru_stime),
            peak_rss: maxrss_to_bytes(usage.ru_maxrss),
            page_faults: (usage.ru_minflt as u64) + (usage.ru_majflt as u64),
        }
    }
}

fn timeval_to_ms(tv: libc::timeval) -> u64 {
    (tv.tv_sec as u64) * 1000 + (tv.tv_usec as u64) / 1000
}

/// Converts `ru_maxrss` to bytes. macOS reports bytes; other systems report
/// kilobytes.
fn maxrss_to_bytes(maxrss: libc::c_long) -> u64 {
    let maxrss = maxrss as u64;
    if cfg!(target_os = "macos") {
        maxrss
    } else {
        maxrss * 1024
    }
}

#[cfg(test)]
mod tests {
    use super::{ProcessStats, timeval_to_ms};
    use std::hint::black_box;

    #[test]
    fn timeval_conversion() {
        let tv = libc::timeval {
            tv_sec: 3,
            tv_usec: 456_789,
        };
        assert_eq!(timeval_to_ms(tv), 3456);
    }

    #[test]
    fn current_is_monotonic() {
        let before = ProcessStats::current();
        assert!(before.peak_rss > 0);

        // Touch 64 MiB so that peak RSS and page faults do not go down.
        let buffer = black_box(vec![1u8; 64 << 20]);
        let after = ProcessStats::current();
        drop(buffer);

        assert!(after.user_ms >= before.user_ms);
        assert!(after.system_ms >= before.system_ms);
        assert!(after.page_faults >= before.page_faults);
        assert!(after.peak_rss >= 64 << 20);
    }
}
