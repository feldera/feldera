use crate::{
    operator::dynamic::distinct::DistinctFactories, trace::BatchReaderFactories,
    typed_batch::IndexedZSet, Circuit, Stream, ZWeight,
};

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
    Z: IndexedZSet,
    Z::InnerBatch: Send,
{
    /// Reduces input batches to one occurrence of each element.
    ///
    /// For each input batch `B`, the operator produces an output batch
    /// that contains at most one occurrence of each tuple in `B`.
    /// Specifically, for each input tuple `(key, value, weight)` with
    /// `weight > 0` the operator produces an output tuple `(key, value, 1)`.
    /// Tuples with `weight <= 0` are dropped.
    ///
    /// Intuitively, the operator converts the input multiset into a set
    /// by eliminating duplicates.
    pub fn stream_distinct(&self) -> Stream<C, Z> {
        let factories = BatchReaderFactories::new::<Z::Key, Z::Val, ZWeight>();

        self.inner().dyn_stream_distinct(&factories).typed()
    }
}

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
    Z: IndexedZSet,
    Z::InnerBatch: Send,
{
    /// Incrementally deduplicate input stream.
    ///
    /// This is an incremental version of the
    /// [`stream_distinct`](`Self::stream_distinct`) operator.
    /// Given a stream of changes to relation `A`, it computes a stream of
    /// changes to relation `A'`, that for each `(key, value, weight)` tuple
    /// in `A` with `weight > 0`, contains a tuple `(key, value, 1)`.
    ///
    /// Intuitively, the operator converts the input multiset into a set
    /// by eliminating duplicates.
    pub fn distinct(&self) -> Stream<C, Z> {
        let factories = DistinctFactories::new::<Z::Key, Z::Val>();

        self.inner().dyn_distinct(&factories).typed()
    }
}
