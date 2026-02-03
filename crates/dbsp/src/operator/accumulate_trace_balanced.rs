use super::dynamic::trace::TimedSpine;
use crate::{
    DBData, RootCircuit, Stream, ZWeight,
    algebra::IndexedZSet as DynIndexedZSet,
    dynamic::Erase,
    trace::BatchReaderFactories,
    typed_batch::{Spine, TypedBatch},
};

impl<K, V, B> Stream<RootCircuit, TypedBatch<K, V, ZWeight, B>>
where
    B: DynIndexedZSet,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
{
    // This isn't useful as a standalone operator, it's only used for testing `dyn_accumulate_trace_balanced`.
    #[track_caller]
    pub fn accumulate_trace_balanced(
        &self,
    ) -> (
        Stream<RootCircuit, Option<Spine<TypedBatch<K, V, ZWeight, B>>>>,
        Stream<RootCircuit, TypedBatch<K, V, ZWeight, TimedSpine<B, RootCircuit>>>,
    ) {
        let trace_factories = BatchReaderFactories::new::<K, V, ZWeight>();
        let batch_factories = BatchReaderFactories::new::<K, V, ZWeight>();
        let (accumulator_stream, trace) = self
            .inner()
            .dyn_accumulate_trace_balanced(&trace_factories, &batch_factories);

        (
            unsafe { accumulator_stream.transmute_payload() },
            trace.typed(),
        )
    }
}
