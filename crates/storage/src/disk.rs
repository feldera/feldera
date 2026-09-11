//! Usage of the disk backing a storage path.

use std::path::Path;

#[cfg(unix)]
use tracing::error;

/// Disk usage, as `statvfs` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskUsage {
    /// Capacity of the disk, in bytes.
    pub total_bytes: u64,

    /// Bytes this process can still write.
    pub available_bytes: u64,
}

impl DiskUsage {
    /// Returns the usage of the disk that holds `path`, or `None` when it
    /// cannot be read.
    // `fsblkcnt_t` is 32 bits on macOS, 64 on Linux, so the widening is only
    // redundant on some targets.
    #[allow(clippy::useless_conversion)]
    #[cfg(unix)]
    pub fn from_path(path: &Path) -> Option<Self> {
        match nix::sys::statvfs::statvfs(path) {
            Ok(stat) => {
                // Counts are in units of `f_frsize`; `f_bsize` is only an I/O hint.
                // statvfs(3): https://man7.org/linux/man-pages/man3/statvfs.3.html
                let block_bytes = u64::from(stat.fragment_size());
                let total_bytes = block_bytes.checked_mul(u64::from(stat.blocks()))?;
                let available_bytes =
                    block_bytes.checked_mul(u64::from(stat.blocks_available()))?;
                // No capacity means the disk cannot report, not that it is full.
                (total_bytes > 0).then_some(Self {
                    total_bytes,
                    available_bytes,
                })
            }
            Err(errno) => {
                error!("Failed to read disk usage for {}: {errno}", path.display());
                None
            }
        }
    }

    /// Returns the usage of the disk that holds `path`, or `None` when it
    /// cannot be read.
    #[cfg(not(unix))]
    pub fn from_path(_path: &Path) -> Option<Self> {
        None
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use super::DiskUsage;

    #[test]
    fn reports_disk_usage_for_an_existing_directory() {
        let temp_dir = std::env::temp_dir();
        let usage = DiskUsage::from_path(&temp_dir)
            .unwrap_or_else(|| panic!("a disk must hold {}", temp_dir.display()));
        assert!(usage.total_bytes > 0, "{usage:?}");
        assert!(usage.available_bytes <= usage.total_bytes, "{usage:?}");
    }

    #[test]
    fn reports_no_disk_usage_for_a_missing_path() {
        // Unknown, not an empty or full disk.
        assert_eq!(
            DiskUsage::from_path(Path::new("/nonexistent-feldera-storage-path")),
            None
        );
    }
}
