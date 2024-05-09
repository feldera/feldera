//! A compactor thread that merges the batches for the spine-fueled trace.

use crate::storage::backend::metrics::COMPACTION_QUEUE_LENGTH;
use crate::storage::backend::StorageError as Error;
use crate::trace::spine_async::BatchIdent;
use crate::trace::Batch;
use crate::Runtime;
use crossbeam::channel::{Receiver, RecvError};
use metrics::gauge;

pub(crate) enum BackgroundOperation {
    Merge(Box<dyn FnMut(isize) -> bool + Send>),
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
    _in_progress: Vec<Box<dyn FnMut(isize) -> bool + Send>>,
}

impl BatchMerger {
    /// Size of the incoming merge queue.
    pub(crate) const RX_QUEUE_SIZE: usize = 128;
    /// How many concurrent merges we allow.
    const CONCURRENT_MERGES: usize = 1;

    pub(crate) fn new(receiver: Receiver<BackgroundOperation>) -> Self {
        Self {
            receiver,
            _in_progress: Vec::with_capacity(Self::CONCURRENT_MERGES),
        }
    }

    pub(crate) fn run(&mut self) {
        let label = vec![("compactor", Runtime::background_index().to_string())];
        while !Runtime::kill_in_progress() {
            let op: Result<BackgroundOperation, RecvError> = self.receiver.recv();
            gauge!(COMPACTION_QUEUE_LENGTH, &label).set(self.receiver.len() as f64);

            match op {
                Ok(BackgroundOperation::Merge(mut merge_fun)) => {
                    let fuel = isize::MAX;
                    let r = merge_fun(fuel);
                    assert!(r); // for now, we expect the merge_fun to complete the merge in one invocation
                }
                Err(e) => {
                    // We dropped all references to the recv channel, this means
                    // the circuit was destroyed, we can exit the compactor thread.
                    log::trace!(
                        "exiting compactor thread due to rx error on channel: {:?}",
                        e
                    );
                    break;
                }
            }
        }
    }
}
