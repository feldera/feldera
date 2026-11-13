//! A pool of OS threads for CPU-heavy work, such as parsing, that must not
//! block a Tokio runtime.
//!
//! A Tokio worker thread must not run for long without an `.await`: the other
//! tasks on that thread (connector I/O, storage I/O, the exchange between
//! hosts) wait until it does. Parsing one record batch can take hundreds of
//! milliseconds, so it runs here, and the Tokio task only waits for its result.

use std::{
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::LazyLock,
    thread,
};

use futures::channel::oneshot;

type Job = Box<dyn FnOnce() + Send>;

/// The pool that [`JobQueue`](super::job_queue::JobQueue) runs its jobs on,
/// with one thread per core.
pub static PARSER_POOL: LazyLock<CpuPool> = LazyLock::new(|| {
    CpuPool::new(
        "feldera-parser",
        thread::available_parallelism().map_or(1, |n| n.get()),
    )
});

/// A fixed set of threads that run jobs in the order they arrive.
///
/// The threads stop when the pool is dropped.
pub struct CpuPool {
    sender: async_channel::Sender<Job>,
}

impl CpuPool {
    /// Start `threads` threads named `{name}-{index}`.
    pub fn new(name: &str, threads: usize) -> Self {
        assert_ne!(threads, 0);
        let (sender, receiver) = async_channel::unbounded::<Job>();
        for index in 0..threads {
            let receiver = receiver.clone();
            thread::Builder::new()
                .name(format!("{name}-{index}"))
                // The same as the Tokio runtime: deeply nested records recurse deeply.
                .stack_size(6 * 1024 * 1024)
                .spawn(move || {
                    while let Ok(job) = receiver.recv_blocking() {
                        job();
                    }
                })
                .expect("failed to start a CPU pool thread");
        }
        Self { sender }
    }

    /// Run `f` on the pool. The caller waits without blocking its thread.
    ///
    /// A panic in `f` does not stop the pool thread: it continues in the caller.
    pub async fn run<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (result_sender, result_receiver) = oneshot::channel();
        let job: Job = Box::new(move || {
            let _ = result_sender.send(catch_unwind(AssertUnwindSafe(f)));
        });
        // The channel is unbounded, and the threads hold the receivers until
        // the pool is dropped, so the send cannot fail.
        self.sender.try_send(job).expect("CPU pool threads stopped");
        match result_receiver
            .await
            .expect("CPU pool thread dropped a job")
        {
            Ok(result) => result,
            Err(panic) => resume_unwind(panic),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CpuPool;
    use std::time::Duration;

    #[tokio::test]
    async fn panic_reaches_caller_and_thread_survives() {
        let pool = std::sync::Arc::new(CpuPool::new("test-pool", 1));

        let panicking = pool.clone();
        let result =
            tokio::spawn(async move { panicking.run(|| -> u32 { panic!("job failed") }).await })
                .await;
        assert!(result.unwrap_err().is_panic());

        // The only thread of the pool still runs jobs.
        let result = tokio::time::timeout(Duration::from_secs(10), pool.run(|| 42)).await;
        assert_eq!(result, Ok(42));
    }
}
