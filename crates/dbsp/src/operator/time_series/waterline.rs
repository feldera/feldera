use crate::{
    dynamic::{DataTrait, DowncastTrait, Erase},
    trace::Rkyv,
    typed_batch::{BatchReader, TypedBox},
    DBData, NumEntries, RootCircuit, Stream,
};
use size_of::SizeOf;

impl<B> Stream<RootCircuit, B>
where
    B: BatchReader + 'static,
    B::Inner: Clone,
{
    /// Compute the waterline of a time series, where the waterline function is
    /// monotonic in event time.  The notion of time here is distinct from the
    /// DBSP logical time and can be modeled using any type that implements
    /// `Ord`.
    ///
    /// We use the term "waterline" instead of the more conventional
    /// "watermark", to avoid confusion with watermarks in systems like
    /// Flink.
    ///
    /// Waterline is an attribute of a time series that indicates the latest
    /// timestamp such that no data points with timestamps older than the
    /// waterline should appear in the stream. Every record in the time
    /// series carries waterline information that can be extracted by
    /// applying a user-provided function to it.  The waterline of the time
    /// series is the maximum of waterlines of all its data points.
    ///
    /// This method computes the waterline of a time series assuming that the
    /// waterline function is monotonic in the event time, e.g., `waterline
    /// = event_time - 5s`.  Such waterlines are the most common in practice
    /// and can be computed efficiently by only considering the latest
    /// timestamp in each input batch.   The method takes a stream of batches
    /// indexed by timestamp and outputs a stream of waterlines (scalar
    /// values).  Its output at each timestamp is a scalar (not a Z-set),
    /// computed as the maximum of the previous waterline and the largest
    /// waterline in the new input batch.
    #[track_caller]
    pub fn waterline_monotonic<TS, DynTS, IF, WF>(
        &self,
        init: IF,
        waterline_func: WF,
    ) -> Stream<RootCircuit, TypedBox<TS, DynTS>>
    where
        DynTS: DataTrait + ?Sized,
        Box<DynTS>: Clone + SizeOf + NumEntries + Rkyv,
        TS: DBData + Erase<DynTS>,
        IF: Fn() -> TS + 'static,
        WF: Fn(&B::Key) -> TS + 'static,
    {
        let result = self.inner().dyn_waterline_monotonic(
            Box::new(move || Box::new(init()).erase_box()),
            Box::new(move |key: &B::DynK, ts: &mut DynTS| unsafe {
                *ts.downcast_mut() = waterline_func(key.downcast())
            }),
        );

        unsafe { result.typed_data() }
    }

    /// Compute the least upper bound over all records that occurred in the
    /// stream with respect to some user-defined lattice.
    ///
    /// We use the term "waterline" instead of the more conventional
    /// "watermark", to avoid confusion with watermarks in systems like
    /// Flink.
    ///
    /// The primary use of this function is in time series analytics in
    /// computing the largest timestamp observed in the stream, which can in
    /// turn be used in computing retainment policies for data in this
    /// stream and streams derived from it (see
    /// [`Stream::integrate_trace_retain_keys`] and
    /// [`Stream::integrate_trace_retain_values`]).
    ///
    /// Note: the notion of time here is distinct from the DBSP logical time and
    /// represents one or several physical timestamps embedded in the input
    /// data.
    ///
    /// In the special case where timestamps form a total order and the input
    /// stream is indexed by time, the
    /// [`waterline_monotonic`](`Stream::waterline_monotonic`) function can
    /// be used instead of this method to compute the bound more
    /// efficiently.
    ///
    /// # Arguments
    ///
    /// * `init` - initial value of the bound, usually the bottom element of the
    ///   lattice.
    /// * `extract_ts` - extracts a timestamp from a key-value pair.
    /// * `least_upper_bound` - computes the least upper bound of two
    ///   timestamps.
    #[track_caller]
    pub fn waterline<TS, DynTS, WF, IF, LB>(
        &self,
        init: IF,
        extract_ts: WF,
        least_upper_bound: LB,
    ) -> Stream<RootCircuit, TypedBox<TS, DynTS>>
    where
        DynTS: DataTrait + ?Sized,
        Box<DynTS>: Clone + SizeOf + NumEntries + Rkyv,
        TS: DBData + Erase<DynTS>,
        IF: Fn() -> TS + 'static,
        WF: Fn(&B::Key, &B::Val) -> TS + 'static,
        LB: Fn(&TS, &TS) -> TS + Clone + 'static,
    {
        let result = self.inner().dyn_waterline(
            Box::new(move || Box::new(init()).erase_box()),
            Box::new(move |k, v, ts: &mut DynTS| unsafe {
                *ts.downcast_mut::<TS>() = extract_ts(k.downcast(), v.downcast())
            }),
            Box::new(move |l, r, ts| unsafe {
                *ts.downcast_mut() = least_upper_bound(l.downcast(), r.downcast())
            }),
        );

        unsafe { result.typed_data() }
    }
}
