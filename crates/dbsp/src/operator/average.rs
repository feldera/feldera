use crate::{
    algebra::{GroupValue, MulByRef},
    dynamic::{ClonableTrait, DowncastTrait, DynData, DynWeight},
    operator::dynamic::aggregate::AvgFactories,
    storage::file::Deserializable,
    typed_batch::{IndexedZSet, OrdIndexedZSet},
    Circuit, DBData, Stream, ZWeight,
};
use std::ops::Div;

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
    Z: IndexedZSet<DynK = DynData>,
{
    /// Incremental average aggregate.
    ///
    /// This operator is a specialization of [`Stream::aggregate`] that for
    /// each key `k` in the input indexed Z-set computes the average value as:
    ///
    ///
    /// ```text
    ///    __                __
    ///    ╲                 ╲
    ///    ╱ v * w     /     ╱  w
    ///    ‾‾                ‾‾
    ///   (v,w) ∈ Z[k]      (v,w) ∈ Z[k]
    /// ```
    ///
    /// # Design
    ///
    /// Average is a quasi-linear aggregate, meaning that it can be efficiently
    /// computed as a composition of two linear aggregates: sum and count.
    /// The `(sum, count)` pair with pair-wise operations is also a linear
    /// aggregate and can be computed with a single
    /// [`Stream::aggregate_linear`] operator. The actual average is
    /// computed by applying the `(sum, count) -> sum / count`
    /// transformation to its output.
    #[track_caller]
    pub fn average<A, F>(&self, f: F) -> Stream<C, OrdIndexedZSet<Z::Key, A>>
    where
        A: DBData + From<ZWeight> + MulByRef<ZWeight, Output = A> + Div<Output = A> + GroupValue,
        F: Fn(&Z::Val) -> A + Clone + 'static,
        <Z::Key as Deserializable>::ArchivedDeser: Ord,
    {
        let factories: AvgFactories<Z::Inner, DynData, DynWeight, C::Time> =
            AvgFactories::new::<Z::Key, A, ZWeight>();

        self.inner()
            .dyn_average::<DynData, DynWeight>(
                &factories,
                Box::new(move |_k, v, w, sum| unsafe {
                    *sum.downcast_mut() = f(v.downcast()).mul_by_ref(w.downcast());
                }),
                Box::new(|w, a| w.as_data_mut().move_to(a)),
            )
            .typed()
        //Box<dyn Fn(&Z::Key, &Z::Val, &DynZWeight, &mut W)>
    }
}
