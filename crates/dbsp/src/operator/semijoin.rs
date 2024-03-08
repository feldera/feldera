use crate::{
    dynamic::DynPair,
    operator::dynamic::semijoin::SemijoinStreamFactories,
    typed_batch::{IndexedZSet, ZSet},
    Circuit, Stream,
};

impl<C, Pairs> Stream<C, Pairs>
where
    C: Circuit,
    Pairs: IndexedZSet,
    Pairs::InnerBatch: Send,
{
    /// Semijoin two streams of batches.
    ///
    /// The operator takes two streams of batches indexed with the same key type
    /// (`Pairs::Key = Keys::Key`) and outputs a stream obtained by joining each
    /// pair of inputs.
    ///
    /// Input streams will typically be produced by [`Stream::map_index()`].
    ///
    /// #### Type arguments
    ///
    /// * `Pairs` - batch type in the first input stream.
    /// * `Keys` - batch type in the second input stream.
    /// * `Out` - output Z-set type.
    pub fn semijoin_stream<Keys, Out>(&self, keys: &Stream<C, Keys>) -> Stream<C, Out>
    where
        Keys: ZSet<Key = Pairs::Key, DynK = Pairs::DynK>,
        Keys::InnerBatch: Send,
        Out: ZSet<Key = (Pairs::Key, Pairs::Val), DynK = DynPair<Pairs::DynK, Pairs::DynV>>,
    {
        let factories = SemijoinStreamFactories::<Pairs::Inner, Keys::Inner, Out::Inner>::new::<
            Pairs::Key,
            Pairs::Val,
        >();

        self.inner()
            .dyn_semijoin_stream(&factories, &keys.inner())
            .typed()
    }
}
