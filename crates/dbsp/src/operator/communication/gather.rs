use crate::{trace::BatchReaderFactories, typed_batch::Batch, Circuit, Stream};

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch<Time = ()>,
    B::InnerBatch: Send,
{
    /// Collect all shards of a stream at the same worker.
    ///
    /// The output stream in `receiver_worker` will contain a union of all
    /// input batches across all workers. The output streams in all other
    /// workers will contain empty batches.
    #[track_caller]
    pub fn gather(&self, receiver_worker: usize) -> Stream<C, B> {
        let factories = BatchReaderFactories::new::<B::Key, B::Val, B::R>();

        self.inner().dyn_gather(&factories, receiver_worker).typed()
    }
}
