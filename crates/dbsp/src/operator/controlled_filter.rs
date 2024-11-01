use crate::{
    dynamic::{DowncastTrait, DynData},
    operator::dynamic::controlled_filter::ControlledFilterFactories,
    Circuit, DBData, IndexedZSet, OrdZSet, Stream, TypedBox, ZWeight,
};

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
    Z: IndexedZSet,
{
    pub fn controlled_key_filter<T, E, F, RF>(
        &self,
        threshold: &Stream<C, TypedBox<T, DynData>>,
        filter_func: F,
        report_func: RF,
    ) -> (Stream<C, Z>, Stream<C, OrdZSet<E>>)
    where
        E: DBData,
        T: DBData,
        F: Fn(&T, &Z::Key) -> bool + 'static,
        RF: Fn(&T, &Z::Key, &Z::Val, ZWeight) -> E + 'static,
    {
        let factories = ControlledFilterFactories::new::<Z::Key, Z::Val, E>();
        let (output, error_stream) = self.inner().dyn_controlled_key_filter(
            factories,
            &threshold.inner_data(),
            Box::new(move |t: &DynData, k: &Z::DynK| unsafe {
                filter_func(t.downcast(), k.downcast())
            }),
            Box::new(
                move |t: &DynData, k: &Z::DynK, v: &Z::DynV, w: ZWeight, e: &mut DynData| unsafe {
                    *e.downcast_mut() = report_func(t.downcast(), k.downcast(), v.downcast(), w)
                },
            ),
        );

        (output.typed(), error_stream.typed())
    }

    pub fn controlled_value_filter<T, E, F, RF>(
        &self,
        threshold: &Stream<C, TypedBox<T, DynData>>,
        filter_func: F,
        report_func: RF,
    ) -> (Stream<C, Z>, Stream<C, OrdZSet<E>>)
    where
        E: DBData,
        T: DBData,
        F: Fn(&T, &Z::Key, &Z::Val) -> bool + 'static,
        RF: Fn(&T, &Z::Key, &Z::Val, ZWeight) -> E + 'static,
    {
        let factories = ControlledFilterFactories::new::<Z::Key, Z::Val, E>();
        let (output, error_stream) = self.inner().dyn_controlled_value_filter(
            factories,
            &threshold.inner_data(),
            Box::new(move |t: &DynData, k: &Z::DynK, v: &Z::DynV| unsafe {
                filter_func(t.downcast(), k.downcast(), v.downcast())
            }),
            Box::new(
                move |t: &DynData, k: &Z::DynK, v: &Z::DynV, w: ZWeight, e: &mut DynData| unsafe {
                    *e.downcast_mut() = report_func(t.downcast(), k.downcast(), v.downcast(), w)
                },
            ),
        );

        (output.typed(), error_stream.typed())
    }
}
