pub mod backend;
pub mod buffer_cache;
pub mod file;

mod init {
    use ctor::ctor;
    use fdlimit::raise_fd_limit;
    use fdlimit::Outcome::LimitRaised;
    use log::warn;

    /// Raise the fd limit to run the storage library to avoid surprises.
    ///
    /// Note that this is a no-op on Windows platforms, hence it's not behind an
    /// architectural cfg.
    #[ctor]
    fn init() {
        match raise_fd_limit() {
            Ok(LimitRaised { from, to }) => {
                const WARN_THRESHOLD: u64 = 1 << 19;
                if to < WARN_THRESHOLD {
                    warn!("Raised fd limit from {} to {}. It's still very low -- try increasing the fd hard-limit (in your limits.conf).", from, to);
                }
            }
            Ok(_) => { /* not on unix */ }
            Err(e) => {
                warn!("Failed to raise fd limit: {}", e);
            }
        }
    }
}
