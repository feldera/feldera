/// The storage backend implementations.
pub mod backend;
/// This module contains a buffer cache that can be used to cache reads.
pub mod buffer_cache;
/// File based data format for feldera.
pub mod file;

mod init {
    use ctor::ctor;
    use fdlimit::raise_fd_limit;

    /// Raise the fd limit to run the storage library to avoid surprises.
    ///
    /// Note that this is a no-op on Windows platforms, hence it's not behind an
    /// architecture cfg.
    ///
    /// TODO: We should raise a warning/abort if the fd limit is (still) too
    /// e.g., due to hard-limit being set too low and display a message on
    /// how to fix it.
    #[ctor]
    fn init() {
        let _r = raise_fd_limit();
    }
}
