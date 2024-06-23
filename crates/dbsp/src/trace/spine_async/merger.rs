//! A compactor thread that merges the batches for the spine-fueled trace.

use crate::storage::backend::metrics::COMPACTION_QUEUE_LENGTH;
use crate::storage::backend::StorageError as Error;
use crate::trace::spine_async::{BatchIdent, MAX_LEVELS};
use crate::trace::Batch;
use crate::Runtime;
use crossbeam::channel::internal::SelectHandle;
use crossbeam::channel::{Receiver, RecvError};
use metrics::gauge;

pub(crate) enum BackgroundOperation {
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

pub(super) enum MergeResult<B>
where
    B: Batch,
{
    MergeCompleted(Result<(BatchIdent, B), Error>),
}

pub(crate) struct BatchMerger {
    /// A handle to receive merge operations on the merger thread.
    receiver: Receiver<BackgroundOperation>,
    /// In progress merges.
    in_progress: Vec<Box<dyn FnMut(&mut isize) + Send>>,
}

impl BatchMerger {
    /// Size of the incoming merge queue.
    pub(crate) const RX_QUEUE_SIZE: usize = 128;
    /// How many concurrent merges we allow.
    const CONCURRENT_MERGES: usize = MAX_LEVELS;

    pub(crate) fn new(receiver: Receiver<BackgroundOperation>) -> Self {
        Self {
            receiver,
            in_progress: Vec::with_capacity(Self::CONCURRENT_MERGES),
        }
    }

    pub(crate) fn run(&mut self) {
        let label = vec![("compactor", Runtime::background_index().to_string())];
        while !Runtime::kill_in_progress() {
            gauge!(COMPACTION_QUEUE_LENGTH, &label).set(self.receiver.len() as f64);
            let op: Result<BackgroundOperation, RecvError> = self.receiver.recv();
            match op {
                Ok(BackgroundOperation::Merge(merge_fun)) => {
                    self.in_progress.push(merge_fun);
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

            while !self.in_progress.is_empty() {
                self.in_progress.retain_mut(|f| {
                    let mut fuel = 100_000isize; // Chosen arbitrarily. Might need some tuning.
                    f(&mut fuel);
                    fuel <= 0
                });

                if self.in_progress.len() < Self::CONCURRENT_MERGES && self.receiver.is_ready() {
                    break;
                }
            }
        }
    }
}
