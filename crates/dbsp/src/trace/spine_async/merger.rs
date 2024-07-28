//! A compactor thread that merges the batches for the spine-fueled trace.

use crate::storage::backend::StorageError as Error;
use crate::trace::spine_async::{BatchIdent, MAX_LEVELS};
use crate::trace::Batch;
use crate::Runtime;
use std::cell::RefCell;
use std::mem::replace;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Arc, Weak};
use std::thread::Builder;
use std::{
    sync::mpsc::{Receiver, RecvError},
    thread_local,
};

pub enum BackgroundOperation {
    /// This is a closure that will be called by the compactor thread which ultimately
    /// merges batches inside of it.
    ///
    /// The `isize` argument is the fuel that the closure can use to perform its work.
    /// If the fuel is exhausted (e.g., 0 after the call it means the merge was not
    /// completed and the closure should be called again.
    /// If the fuel is non-zero, it means the merge has completed and the closure
    /// should not be called again/can be disposed.
    Merge(Box<dyn FnMut(&mut isize) + Send>),
}

pub enum MergeResult<B>
where
    B: Batch,
{
    MergeCompleted(Result<(BatchIdent, B), Error>),
}

pub struct BatchMerger {
    /// A handle to receive merge operations on the merger thread.
    receiver: Receiver<BackgroundOperation>,
}

impl BatchMerger {
    /// How many concurrent merges we allow.
    const CONCURRENT_MERGES: usize = MAX_LEVELS;

    /// Returns a merger for this thread.
    pub fn get() -> Arc<SyncSender<BackgroundOperation>> {
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
        // one exists), so instead we tie it to the current thread. To ensure
        // that it exits when it's no longer needed, we use a thread-local
        // `Weak` that we upgrade to an `Arc` to pass back to the `Spine`. Thus,
        // when all the `Spine`s get dropped and drop their strong reference,
        // the `Weak` will cause the background thread to exit.
        thread_local! {
            static MERGER_THREAD: RefCell<Weak<SyncSender<BackgroundOperation>>> = const { RefCell::new(Weak::new()) };
        }

        MERGER_THREAD.with_borrow_mut(|merger| {
            merger.upgrade().unwrap_or_else(|| {
                let m = Arc::new(Self::new());
                let _ = replace(merger, Arc::downgrade(&m));
                m
            })
        })
    }

    #[allow(clippy::new_ret_no_self)]
    fn new() -> SyncSender<BackgroundOperation> {
        let (sender, receiver) = sync_channel(128);
        let name = if let Some(name) = std::thread::current().name() {
            format!("{name}-bg")
        } else {
            String::from("dbsp-bg")
        };
        let _ = Runtime::spawn_background_thread(Builder::new().name(name), || {
            BatchMerger { receiver }.run()
        });
        sender
    }

    fn run(&mut self) {
        let mut in_progress = Vec::with_capacity(Self::CONCURRENT_MERGES);
        loop {
            let op: Result<BackgroundOperation, RecvError> = self.receiver.recv();
            match op {
                Ok(BackgroundOperation::Merge(merge_fun)) => {
                    in_progress.push(merge_fun);
                }
                Err(e) => {
                    // We dropped all references to the recv channel, this means
                    // the circuit was destroyed, we can exit the compactor thread.
                    log::trace!(
                        "exiting compactor thread due to rx error on channel: {:?}",
                        e
                    );
                    return;
                }
            }

            while !in_progress.is_empty() {
                in_progress.retain_mut(|f| {
                    let mut fuel = 100_000isize; // Chosen arbitrarily. Might need some tuning.
                    f(&mut fuel);
                    fuel <= 0
                });

                if in_progress.len() < Self::CONCURRENT_MERGES {
                    in_progress.extend(
                        self.receiver
                            .try_iter()
                            .take(Self::CONCURRENT_MERGES - in_progress.len())
                            .map(|BackgroundOperation::Merge(merge_fun)| merge_fun),
                    );
                }
            }
        }
    }
}
