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
    /// A controlled filter operator that discards input keys by comparing them to a scalar
    /// value in the `threshold` stream.
    ///
    /// This operator compares each key in the input indexed Z-set against the current value in
    /// the `threshold` stream using `filter_func` and discards keys that don't pass the filter.
    ///
    /// Returns a pair of output streams.
    ///
    /// **Filtered output stream**: (key, value, weight) tuples whose key passes the filter
    /// are sent to the first output stream unmodified.
    ///
    /// **Error stream**: Keys that don't pass the check are transformed by `report_func` and sent
    /// to the second output stream. More precisely `report_func` is invoked for each (key, value, weight)
    /// tuple whose key does not pass the filter and returns a value of type `E`, which is sent
    /// with weight 1 to the error stream.
    #[track_caller]
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

    /// Like [`Self::controlled_key_filter`], but values in the `threshold` stream are strongly typed
    /// instead of having type `TypedBox`.
    #[cfg(not(feature = "backend-mode"))]
    #[track_caller]
    pub fn controlled_key_filter_typed<T, E, F, RF>(
        &self,
        threshold: &Stream<C, T>,
        filter_func: F,
        report_func: RF,
    ) -> (Stream<C, Z>, Stream<C, OrdZSet<E>>)
    where
        E: DBData,
        T: DBData,
        F: Fn(&T, &Z::Key) -> bool + 'static,
        RF: Fn(&T, &Z::Key, &Z::Val, ZWeight) -> E + 'static,
    {
        self.controlled_key_filter(&threshold.typed_box::<DynData>(), filter_func, report_func)
    }

    /// A controlled filter operator that discards input key/values pairs by comparing them to a scalar
    /// value in the `threshold` stream.
    ///
    /// This operator compares each key/value pair in the input indexed Z-set against the current value in
    /// the `threshold` stream using `filter_func` and discards tuples that don't pass the filter.
    ///
    /// Returns a pair of output streams.
    ///
    /// **Filtered output stream**: (key, value, weight) tuples that pass the check are sent to the
    /// first output stream unmodified.
    ///
    /// **Error stream**: Tuples that don't pass the check are transformed by `report_func` and sent
    /// to the second output stream. More precisely `report_func` is invoked for each (key, value, weight)
    /// tuple whose key and value do not pass the filter and returns a value of type `E`, which is sent
    /// with weight 1 to the error stream.
    #[track_caller]
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

    /// Like [`Self::controlled_value_filter`], but values in the `threshold` stream are strongly typed
    /// instead of having type `TypedBox`.
    #[track_caller]
    pub fn controlled_value_filter_typed<T, E, F, RF>(
        &self,
        threshold: &Stream<C, T>,
        filter_func: F,
        report_func: RF,
    ) -> (Stream<C, Z>, Stream<C, OrdZSet<E>>)
    where
        E: DBData,
        T: DBData,
        F: Fn(&T, &Z::Key, &Z::Val) -> bool + 'static,
        RF: Fn(&T, &Z::Key, &Z::Val, ZWeight) -> E + 'static,
    {
        self.controlled_value_filter(&threshold.typed_box::<DynData>(), filter_func, report_func)
    }
}
