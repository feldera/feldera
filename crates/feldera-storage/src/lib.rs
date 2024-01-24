pub mod backend;
pub mod buffer_cache;
pub mod file;

mod init {
    use ctor::ctor;
    use fdlimit::raise_fd_limit;

    /// Raise the fd limit to run the storage library to avoid surprises.
    ///
    /// Note that this is a no-op on Windows platforms, hence it's not behind an
    /// architecture cfg.
    ///
    /// TODO: We should raise a warning/abort if the fd limit is (still) too low
    /// e.g., due to hard-limit being set too low and display a message on
    /// how to fix it.
    #[ctor]
    fn init() {
        let _r = raise_fd_limit();
    }
}
