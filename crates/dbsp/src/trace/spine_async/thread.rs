//! A compactor thread that merges the batches for the spine-fueled trace.

use crate::Runtime;
use std::cell::RefCell;
use std::mem::replace;
use std::sync::{Arc, Mutex, Weak};
use std::thread::{Builder, Thread};
use std::thread_local;

/// Return value for a worker function.
pub enum WorkerStatus {
    /// The worker has more work to do (it only returned to allow other workers
    /// to run).
    Busy,

    /// The worker has no more work to do now, but it might have more later.
    Idle,

    /// The worker has exited.
    Done,
}

struct Inner {
    new_workers: Vec<WorkerConstructorFn>,
    exiting: bool,
    thread: Option<Thread>,
}
pub struct BackgroundThread(Mutex<Inner>);

// There are three cases:
//
// 1. We are a worker thread inside a `Runtime`. The background thread
//    should correspond to the worker thread. We accomplish this.
//
// 2. We are part of a circuit that is not part of a `Runtime`. We would
//    prefer to have a background thread for the circuit. We do not
//    accomplish this; instead, we have a background thread for the
//    thread in which the circuit exists (one could have more than one
//    circuit per thread). However, since a `ChildCircuit` is not
//    `Send`, at least we will have only one background thread per
//    circuit.
//
// 3. We are not part of a circuit at all; that is, something
//    instantiated a `Spine` outside a circuit (probably in a unit
//    test). We create a background thread for the thread that initially
//    owned the `Spine`.  (`Spine` is not `Send` either.)
//
// It doesn't make sense to tie the background thread to the `Runtime`
// or the `Circuit` because of cases 2 and 3 (that is, sometimes neither
// one exists), so instead we tie it to the current thread.
thread_local! {
    static THREAD: RefCell<Weak<BackgroundThread>> = const { RefCell::new(Weak::new()) };
}

/// A function that returns a [Worker].
///
/// This exists because the [Worker] that we use constructs a merger, which are
/// not required to be `Send` and in practice are not (because our storage
/// implementations are thread-specific).  This means that the caller of
/// [BackgroundThread::add_worker] can't construct a merger for the worker,
/// because it would then be moved from the caller's thread to the background
/// thread. Thus, instead, the `WorkerConstructorFn` is called once in the
/// background thread to do the construction.
type WorkerConstructorFn = Box<dyn FnOnce() -> Box<dyn Worker> + Send>;

pub trait Worker {
    /// Does some work. Called repeatedly until it reports that it is done.
    fn run(&mut self) -> WorkerStatus;

    /// Returns a priority for running the worker. Higher numbers are higher
    /// priorities.
    fn priority(&self) -> usize;
}

impl BackgroundThread {
    pub fn add_worker(worker: WorkerConstructorFn) {
        THREAD.with_borrow_mut(|thread| {
            if let Some(thread) = thread.upgrade() {
                let mut inner = thread.0.lock().unwrap();
                if !inner.exiting {
                    inner.new_workers.push(worker);
                    return;
                }
            }
            let _ = replace(thread, Self::new(worker));
        });
    }

    fn new(worker: WorkerConstructorFn) -> Weak<Self> {
        let bg = Arc::new(Self(Mutex::new(Inner {
            new_workers: vec![worker],
            exiting: false,
            thread: None,
        })));
        let name = if let Some(name) = std::thread::current().name() {
            format!("{name}-bg")
        } else {
            String::from("dbsp-bg")
        };
        let thread = Runtime::spawn_background_thread(Builder::new().name(name), {
            let bg = bg.clone();
            move || bg.run()
        });
        bg.0.lock().unwrap().thread = Some(thread);
        Arc::downgrade(&bg)
    }

    pub fn wake() {
        THREAD.with_borrow(|thread| {
            if let Some(thread) = thread.upgrade() {
                let inner = thread.0.lock().unwrap();
                if let Some(thread) = inner.thread.as_ref() {
                    thread.unpark();
                }
            }
        });
    }

    fn run(self: Arc<Self>) {
        let mut workers = Vec::new();
        let mut priorities = Vec::new();
        loop {
            // Gather newly submitted workers.
            let mut inner = self.0.lock().unwrap();
            for new_worker in inner.new_workers.drain(..) {
                workers.push(new_worker());
            }
            if workers.is_empty() {
                inner.exiting = true;
                return;
            }
            drop(inner);

            // Collect the priority of every worker.
            priorities.clear();
            for worker in workers.iter() {
                priorities.push(worker.priority());
            }
            let max = priorities.iter().copied().max().unwrap();

            // Run workers.
            //
            // We run only the worker(s) with the highest priority. If all of
            // them are idle (which is unlikely since they have the highest
            // priority) then we give all the workers a chance to run.
            let mut idle = true;
            let mut priority = priorities.iter().copied();
            workers.retain_mut(|worker| {
                priority.next().unwrap() != max || run_worker(worker, &mut idle)
            });
            if idle {
                workers.retain_mut(|worker| run_worker(worker, &mut idle));
            }

            // If there's at least one worker and all of them are idle, wait for
            // something to change.
            //
            // If there are no workers, go around again to get a new worker or
            // exit (if we just exit immediately then that could drop a new
            // worker).
            if idle && !workers.is_empty() {
                std::thread::park();
            }
        }
    }
}

fn run_worker(worker: &mut Box<dyn Worker>, idle: &mut bool) -> bool {
    match worker.run() {
        WorkerStatus::Busy => {
            *idle = false;
            true
        }
        WorkerStatus::Idle => true,
        WorkerStatus::Done => false,
    }
}
