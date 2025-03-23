use std::borrow::Cow;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{
    fs::File,
    io::{Error as IoError, Write},
};

#[cfg(feature = "with-deltalake")]
use std::{error::Error, future::Future, pin::Pin};

#[cfg(feature = "with-deltalake")]
use futures::channel::oneshot;
use tempfile::NamedTempFile;
#[cfg(feature = "with-deltalake")]
use tokio::{spawn, task::JoinHandle};

#[cfg(feature = "with-deltalake")]
pub(crate) fn root_cause(mut err: &dyn Error) -> &dyn Error {
    while let Some(source) = err.source() {
        err = source;
    }
    err
}

pub(crate) fn truncate_ellipse<'a>(s: &'a str, len: usize, ellipse: &str) -> Cow<'a, str> {
    if s.len() <= len {
        return Cow::Borrowed(s);
    } else if len == 0 {
        return Cow::Borrowed("");
    }

    let result = s.chars().take(len).chain(ellipse.chars()).collect();
    Cow::Owned(result)
}

pub(crate) fn write_file_atomically<P>(path: P, data: &[u8]) -> Result<(), IoError>
where
    P: AsRef<Path>,
{
    // Find the file's directory and open it.
    let dir = path.as_ref().parent().unwrap_or(Path::new("."));
    let dir_handle = File::open(dir)?;

    // Write and rename and sync the file.
    let mut temp = NamedTempFile::new_in(dir)?;
    temp.write_all(data)?;
    temp.as_file().sync_data()?;
    temp.persist(path)?;

    // Then sync the directory to make sure that the rename is committed.
    dir_handle.sync_data()?;

    Ok(())
}

/// For logging with a non-constant level.  From
/// <https://github.com/tokio-rs/tracing/issues/2730>
#[macro_export]
macro_rules! dyn_event {
    ($lvl:expr, $($arg:tt)+) => {
        match $lvl {
            ::tracing::Level::TRACE => ::tracing::trace!($($arg)+),
            ::tracing::Level::DEBUG => ::tracing::debug!($($arg)+),
            ::tracing::Level::INFO => ::tracing::info!($($arg)+),
            ::tracing::Level::WARN => ::tracing::warn!($($arg)+),
            ::tracing::Level::ERROR => ::tracing::error!($($arg)+),
        }
    };
}

/// A job queue that dispatches work to a pool of tokio tasks.
///
/// While the jobs can complete out-of-order, their outputs are consumed in the same order
/// they were enqueued. This is useful for implementing parallel parsing, where parsed
/// records must be fed into the circuit in the order they were received.
/*
                          sync
   ┌─────────────────────────────────────────────────────────┐
   │                           ┌───────┐                     │
   │                        ┌─►│worker1├────────┐            │
   │              jobs      │  └───────┘        │            │
   ▼          ┌─┬─┬─┬─┬─┬─┐ │  ┌───────┐        │       ┌────┴───┐
producer  ─┬─►│ │ │ │ │ │ ├─┼─►│worker2├────┐   │       │consumer│
           │  └─┴─┴─┴─┴─┴─┘ │  └───────┘    │   │       └────────┘
           │                │  ┌───────┐    │   │            ▲
           │                └─►│worker3├──┐ │   │            │
           │                   └───────┘  ▼ ▼   ▼            │
           │                             ┌─┬─┬─┬─┬─┬─┐       │
           └────────────────────────────►│ │ │ │ │ │ ├───────┘
                                         └─┴─┴─┴─┴─┴─┘
                                          completions

* Producer adds jobs to the job queue. A job consists of an input value and
  a completion one-shot channel where the worker will send the output of the job.

* The receiving side of the channel is pushed to the completions queue.

* Worker tasks dequeue jobs from the jobs queue and send the result to the
  one-shot channel associated with each job. The consumer receives the next
  item from the completion queue and waits for the output of the job.

* When the producer needs to wait for all jobs in the queue to complete, it
  sends a special Sync message to the completions queue. Upon receiving
  this message, the consumer sends an acknowledgement to the sync channel.
*/
#[cfg(feature = "with-deltalake")]
pub(crate) struct JobQueue<I, O> {
    /// The producer side of the job queue.
    job_sender: async_channel::Sender<(I, oneshot::Sender<O>)>,

    /// The producer side of the completions queue.
    completion_sender: async_channel::Sender<Completion<O>>,

    /// The receiving side of the sync channel.
    sync_receiver: async_channel::Receiver<()>,

    /// Worker tasks.
    workers: Vec<JoinHandle<()>>,

    /// Consumer task.
    consumer: JoinHandle<()>,
}

// Every message in the completions channel contains the receiving side of the
// oneshot channel that will contain the result of the completed job, or a special
// Sync message.
#[cfg(feature = "with-deltalake")]
enum Completion<O> {
    Completion(oneshot::Receiver<O>),
    Sync,
}

#[cfg(feature = "with-deltalake")]
impl<I, O> JobQueue<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    /// Create a job queue.
    ///
    /// # Arguments
    ///
    /// * `num_workers` - the number of threads in the worker pool. Must be >0.
    /// * `worker_builder` - a closure that returns a closure that each worker will execute for each job.
    ///    The outer closure is needed to allocate any resources needed by the worker.
    /// * `consumer_func` - closure that the consumer will execute for each completed job.
    pub(crate) fn new(
        num_workers: usize,
        worker_builder: impl Fn() -> Box<dyn FnMut(I) -> Pin<Box<dyn Future<Output = O> + Send>> + Send>,
        mut consumer_func: impl FnMut(O) + Send + 'static,
    ) -> Self {
        assert_ne!(num_workers, 0);

        // The jobs queue length is equal to the number of workers. This way, workers don't get
        // starved, but we also don't queue more data than necessary to keep the workers busy.
        // TODO: does it make sense to make queue length separately configurable?
        let (job_sender, job_receiver) =
            async_channel::bounded::<(I, oneshot::Sender<O>)>(num_workers);

        // The completion queue can contain at most one message per worker + one message per
        // job in the jobs queue plus the Sync message.
        let (completion_sender, completion_receiver) = async_channel::bounded(2 * num_workers + 1);
        let (sync_sender, sync_receiver) = async_channel::bounded(1);

        let workers = (0..num_workers)
            .map(move |_| {
                let mut worker_fn = worker_builder();

                let job_receiver = job_receiver.clone();
                spawn(async move {
                    loop {
                        let Ok((input, completion_sender)) = job_receiver.recv().await else {
                            return;
                        };
                        let result = worker_fn(input).await;
                        if completion_sender.send(result).is_err() {
                            return;
                        };
                    }
                })
            })
            .collect();

        let consumer = spawn(async move {
            loop {
                match completion_receiver.recv().await {
                    Err(_) => {
                        return;
                    }
                    Ok(Completion::Completion(receiver)) => {
                        let Ok(v) = receiver.await else {
                            continue;
                        };
                        consumer_func(v);
                    }
                    Ok(Completion::Sync) => {
                        if sync_sender.send(()).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        Self {
            job_sender,
            completion_sender,
            sync_receiver,
            workers,
            consumer,
        }
    }

    /// Push a new job to the queue. Blocks until there is space in the queue.
    pub(crate) async fn push_job(&self, job: I) {
        let (completion_sender, completion_receiver) = oneshot::channel();
        let _ = self.job_sender.send((job, completion_sender)).await;
        let _ = self
            .completion_sender
            .send(Completion::Completion(completion_receiver))
            .await;
    }

    /// Wait for all previously queued jobs to complete.
    pub(crate) async fn flush(&self) {
        let _ = self.completion_sender.send(Completion::Sync).await;
        let _ = self.sync_receiver.recv().await;
    }
}

#[cfg(feature = "with-deltalake")]
impl<I, O> Drop for JobQueue<I, O> {
    fn drop(&mut self) {
        self.consumer.abort();
        for worker in self.workers.drain(..) {
            worker.abort();
        }
    }
}

/// Execute a set of tasks on a thread pool with `num_threads`.
///
/// Will execute up to `num_threads` tasks in parallel.
///
/// # Error handling
///
/// If one of the tasks returns an error, this error is returned to the caller.
/// At this point any tasks that are not yet running will be canceled; however
/// tasks that are a already running will be allowed to finish; however tasks that
/// are already running will continue running, since we don't have a way to cancel them.
/// The function doesn't wait for these tasks to finish; thery will continue running
/// in the background after the function returns.
///
// TODO:
// * add a flag to wait for all tasks to finish on error.
// * if we go async, it may be possible to cancel tasks.
pub(crate) fn run_on_thread_pool<I, T, E>(name: &str, num_threads: usize, tasks: I) -> Result<(), E>
where
    I: IntoIterator<Item = Box<dyn FnOnce() -> Result<T, E> + Send>>,
    T: Send + 'static,
    E: Send + 'static,
{
    let thread_pool = threadpool::Builder::new()
        .num_threads(num_threads)
        .thread_name(name.to_string())
        .build();
    let (tx, rx) = std::sync::mpsc::channel();
    let cancel = Arc::new(AtomicBool::new(false));
    let mut num_tasks = 0;

    for task in tasks {
        num_tasks += 1;
        let cancel = cancel.clone();
        let tx = tx.clone();
        thread_pool.execute(move || {
            if cancel.load(Ordering::Acquire) {
                return;
            }

            let result = task();
            if result.is_err() {
                cancel.store(true, Ordering::Release);
            }
            let _ = tx.send(result);
        })
    }

    for _ in 0..num_tasks {
        rx.recv().unwrap()?;
    }

    thread_pool.join();

    Ok(())
}

#[cfg(test)]
#[cfg(feature = "with-deltalake")]
mod test {
    use std::sync::{Arc, Mutex};

    use crate::util::JobQueue;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_job_queue() {
        let result = Arc::new(Mutex::new(Vec::new()));
        let result_clone = result.clone();

        let job_queue = JobQueue::new(
            6,
            || Box::new(|i: u32| Box::pin(async move { i })),
            move |i| result_clone.lock().unwrap().push(i),
        );

        for i in 0..1000000 {
            job_queue.push_job(i).await;
        }

        job_queue.flush().await;

        let expected: Vec<u32> = (0..1000000).collect();

        assert_eq!(&*result.lock().unwrap(), &*expected);
    }
}
