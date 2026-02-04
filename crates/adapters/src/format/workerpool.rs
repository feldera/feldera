//! Workerpool for parallel encoders.

use feldera_adapterlib::{
    catalog::{BatchSplitter, RecordFormat, SerBatchReader, SplitCursorBuilder},
    transport::Step,
};
use std::sync::{Arc, Mutex, atomic::AtomicBool};

use crossbeam::channel::{Receiver, Sender};

type Job = Box<dyn WorkerFn>;

pub(crate) struct Workerpool {
    sender: Sender<(Job, SplitCursorBuilder)>,
    threads: Vec<std::thread::JoinHandle<()>>,
    errors: Vec<anyhow::Error>,
}

/// The trait each worker job must implement.
pub(crate) trait WorkerFn: Send + Sync + 'static {
    /// Process a partition of the batch.
    fn process(&mut self, cursor: SplitCursorBuilder) -> anyhow::Result<()>;
}

#[allow(unused)]
pub enum ConsumerMessage {
    BatchStart {
        step: Step,
    },
    PushBuffer {
        buffer: Vec<u8>,
        num_records: usize,
    },
    PushKey {
        key: Option<Vec<u8>>,
        val: Option<Vec<u8>>,
        headers: Vec<(String, Option<Vec<u8>>)>,
        num_records: usize,
    },
    BatchEnd,
}

struct Worker {
    id: usize,
    busy: AtomicBool,
    error: Arc<Mutex<Option<anyhow::Error>>>,
    rx: Receiver<(Job, SplitCursorBuilder)>,
}

impl Worker {
    fn new(id: usize, rx: Receiver<(Job, SplitCursorBuilder)>) -> Self {
        Worker {
            id,
            busy: AtomicBool::new(false),
            error: Arc::new(Mutex::new(None)),
            rx,
        }
    }

    fn run(&self) {
        while let Ok((mut job, cursor)) = self.rx.recv() {
            self.busy.store(true, std::sync::atomic::Ordering::SeqCst);
            if let Err(e) = job.process(cursor) {
                tracing::error!("encoder worker thread exited with: {e:?}");
                *self.error.lock().unwrap() = Some(e);
            }
            // Explicitly drop the job as jobs are supposed to be one-time use.
            std::mem::drop(job);
            self.busy.store(false, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl Workerpool {
    /// Create a new workerpool.
    pub fn new(workers: usize) -> anyhow::Result<Self> {
        if workers == 0 {
            anyhow::bail!("number of workers must be greater than zero");
        }

        let (tx, rx) = crossbeam::channel::bounded(workers);
        let mut threads = Vec::with_capacity(workers);

        for _ in 0..workers {
            let rx: Receiver<(Job, SplitCursorBuilder)> = rx.clone();
            // let worker = Worker::new(0, rx.clone());

            let thread = std::thread::spawn(move || {
                while let Ok((mut job, cursor)) = rx.recv() {
                    if let Err(e) = job.process(cursor) {
                        println!("encoder worker thread exited with error: {e:?}");
                        tracing::error!("encoder worker thread exited with: {e:?}");
                    }

                    // Explicitly drop the job as jobs are supposed to be one-time use.
                    std::mem::drop(job);
                }
            });
            threads.push(thread);
        }

        Ok(Workerpool {
            sender: tx,
            threads,
            errors: Vec::new(),
        })
    }

    /// Give the workers this batch to process.
    pub fn process_batch(
        &self,
        batch: Arc<dyn SerBatchReader>,
        format: RecordFormat,
        job_factory: Arc<impl Fn() -> Job>,
    ) -> anyhow::Result<()> {
        let splitter = BatchSplitter::new(batch, self.threads.len(), format);

        while let Some(cursor) = splitter.next_split() {
            let job = (job_factory)();
            self.sender.send((job, cursor))?;
        }

        Ok(())
    }
}
