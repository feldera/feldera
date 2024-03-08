use crate::{trace::BatchReaderFactories, typed_batch::Batch, Circuit, Stream};

impl<C, IB> Stream<C, IB>
where
    C: Circuit,
    IB: Batch<Time = ()>,
    IB::InnerBatch: Send,
{
    /// Shard batches across multiple worker threads based on keys.
    ///
    /// # Theory
    ///
    /// We parallelize processing across `N` worker threads by creating a
    /// replica of the same circuit per thread and sharding data across
    /// replicas.  To ensure correctness (i.e., that the sum of outputs
    /// produced by individual workers is equal to the output produced
    /// by processing the entire dataset by one worker), sharding must satisfy
    /// certain requirements determined by each operator.  In particular,
    /// for `distinct`, and `aggregate` all tuples that share the same key
    /// must be processed by the same worker.  For `join`, tuples from both
    /// input streams with the same key must be processed by the same worker.
    ///
    /// Other operators, e.g., `filter` and `flat_map`, impose no restrictions
    /// on the sharding scheme: as long as each tuple in a batch is
    /// processed by some worker, the correct result will be produced.  This
    /// is true for all linear operators.
    ///
    /// The `shard` operator shards input batches based on the hash of the key,
    /// making sure that tuples with the same key always end up at the same
    /// worker.  More precisely, the operator **re-shards** its input by
    /// partitioning batches in the input stream of each worker based on the
    /// hash of the key, distributing resulting fragments among peers
    /// and re-assembling fragments at each peer:
    ///
    /// ```text
    ///         ┌──────────────────┐
    /// worker1 │                  │
    /// ───────►├─────┬───────────►├──────►
    ///         │     │            │
    /// ───────►├─────┴───────────►├──────►
    /// worker2 │                  │
    ///         └──────────────────┘
    /// ```
    ///
    /// # Usage
    ///
    /// Most users do not need to invoke `shard` directly (and doing so is
    /// likely to lead to incorrect results unless you know exactly what you
    /// are doing).  Instead, each operator re-shards its inputs as
    /// necessary, e.g., `join` applies `shard` to both of its
    /// input streams, while `filter` consumes its input directly without
    /// re-sharding.
    ///
    /// # Performance considerations
    ///
    /// In the current implementation, the `shard` operator introduces a
    /// synchronization barrier across all workers: its output at any worker
    /// is only produced once input batches have been collected from all
    /// workers.  This limits the scalability since a slow worker (e.g., running
    /// on a busy CPU core or sharing the core with other workers) or uneven
    /// sharding can slow down the whole system and reduce gains from
    /// parallelization.
    pub fn shard(&self) -> Stream<C, IB> {
        let factories = BatchReaderFactories::new::<IB::Key, IB::Val, IB::R>();
        self.inner().dyn_shard(&factories).typed()
    }
}
