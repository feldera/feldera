use crate::{
    dynamic::{DowncastTrait, DynData, Erase},
    operator::{dynamic::trace::ValSpine, TraceBound},
    trace::BatchReaderFactories,
    typed_batch::{Batch, DynBatch, DynBatchReader, Spine, TypedBatch, TypedBox},
    Circuit, DBData, DBWeight, Stream,
};
use dyn_clone::clone_box;
use size_of::SizeOf;

impl<C, K, V, R, B> Stream<C, TypedBatch<K, V, R, B>>
where
    C: Circuit,
    B: DynBatch<Time = ()>,
    K: DBData + Erase<B::Key>,
    V: DBData + Erase<B::Val>,
    R: DBWeight + Erase<B::R>,
{
    // TODO: derive timestamp type from the parent circuit.

    /// Record batches in `self` in a trace.
    ///
    /// This operator labels each untimed batch in the stream with the current
    /// timestamp and adds it to a trace.
    pub fn trace(&self) -> Stream<C, TypedBatch<K, V, R, ValSpine<B, C>>> {
        let factories = BatchReaderFactories::new::<K, V, R>();
        self.inner().dyn_trace(&factories).typed()
    }

    /// Record batches in `self` in a trace with bounds `lower_key_bound` and
    /// `lower_val_bound`.
    ///
    /// ```text
    ///          ┌─────────────┐ trace
    /// self ───►│ TraceAppend ├─────────┐───► output
    ///          └─────────────┘         │
    ///            ▲                     │
    ///            │                     │
    ///            │ local   ┌───────┐   │z1feedback
    ///            └─────────┤Z1Trace│◄──┘
    ///                      └───────┘
    /// ```
    pub fn trace_with_bound<T>(
        &self,
        lower_key_bound: TraceBound<B::Key>,
        lower_val_bound: TraceBound<B::Val>,
    ) -> Stream<C, TypedBatch<K, V, R, ValSpine<B, C>>> {
        let factories = BatchReaderFactories::new::<K, V, R>();
        self.inner()
            .dyn_trace_with_bound(&factories, lower_key_bound, lower_val_bound)
            .typed()
    }
}

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch<Time = ()>,
{
    /// Like `integrate_trace`, but additionally applies a retainment policy to
    /// keys in the trace.
    ///
    /// ## Background
    ///
    /// Relations that store time series data typically have the property that
    /// any new updates can only affect records with recent timestamps.
    /// Depending on how the relation is used in queries this might mean
    /// that, while records with older timestamps still exist in the
    /// relation, they cannot affect any future incremental computation and
    /// therefore don't need to be stored.
    ///
    /// ## Design
    ///
    /// We support two mechanism to specify and eventually discard such unused
    /// records.
    ///
    /// The first mechanism, exposed via the
    /// [`integrate_trace_with_bound`](`Self::integrate_trace_with_bound`)
    /// method, is only applicable when keys and/or values in the collection
    /// are ordered by time.  It allows _each_ consumer of the trace to specify
    /// a lower bound on the keys and values it is interested in.  The
    /// effective bound is the minimum of all bounds specified by individual
    /// consumers.
    ///
    /// The second mechanism, implemented by this method and the
    /// [`integrate_trace_retain_values`](`Self::integrate_trace_retain_values`)
    /// method, is more general and allows the caller to specify an
    /// arbitrary condition on keys and values in the trace respectively.
    /// Keys or values that don't satisfy the condition are eventually
    /// reclaimed by the trace.  This mechanism is applicable to collections
    /// that are not ordered by time.  Hence it doesn't require rearranging
    /// the data in time order.  Furthermore, it is applicable to collections
    /// that contain multiple timestamp column.  Such multidimensional
    /// timestamps only form a partial order.
    ///
    /// Unlike the first mechanism, this mechanism only allows one global
    /// condition to be applied to the stream.  This bound affects _all_
    /// operators that use the trace of the stream, i.e., call
    /// `integrate_trace` (or `trace` in the root scope) on it. This includes
    /// for instance `join`, `aggregate`, and `distinct`.  All such operators
    /// will reference the same instance of a trace.  Therefore bounds
    /// specified by this API must be based on a global analysis of the
    /// entire program.
    ///
    /// The two mechanisms described above interact in different ways for keys
    /// and values. For keys, the lower bound and the retainment condition
    /// are independent and can be active at the same time.  Internally,
    /// they are enforced using different techniques. Lower bounds are
    /// enforced at essentially zero cost.  The retention condition is more
    /// expensive, but more general.
    ///
    /// For values, only one of the two mechanisms can be enabled for any given
    /// stream.  Whenever a retainment condition is specified it supersedes
    /// any lower bounds constraints.
    ///
    /// ## Arguments
    ///
    /// * `bounds_stream` - This stream carries scalar values (i.e., single
    ///   records, not Z-sets).  The key retainment condition is defined
    ///   relative to the last value received from this stream. Typically, this
    ///   value represents the lowest upper bound of all partially ordered
    ///   timestamps in `self` or some other stream, computed with the help of
    ///   the [`waterline`](`Stream::waterline`) operator and adjusted by some
    ///   contstant offsets, dictated, e.g., by window sizes used in the queries
    ///   and the maximal out-of-ordedness of data in the input streams.
    ///
    /// * `retain_key_func` - given the value received from the `bounds_stream`
    ///   at the last clock cycle and a key, returns `true` if the key should be
    ///   retained in the trace and `false` if it should be discarded.
    ///
    /// ## Correctness
    ///
    /// * As discussed above, the retainment policy set using this method
    ///   applies to all consumers of the trace.  An incorrect policy may
    ///   reclaim keys that are still needed by some of the operators, leading
    ///   to incorrect results.  Computing a correct retainment policy can be a
    ///   subtle and error prone task, which is probably best left to automatic
    ///   tools like compilers.
    ///
    /// * The retainment policy set using this method only applies to `self`,
    ///   but not any stream derived from it.  In particular, if `self` is
    ///   re-sharded using the `shard` operator, then it may be necessary to
    ///   call `integrate_trace_retain_keys` on the resulting stream. In
    ///   general, computing a correct retainment policy requires keep track of
    ///   * Streams that are sharded by construction and hence the `shard`
    ///     operator is a no-op for such streams.  For instance, the
    ///     `add_input_set` and `aggregate` operators produce sharded streams.
    ///   * Operators that `shard` their input streams, e.g., `join`.
    ///
    /// * This method should be invoked at most once for a stream.
    ///
    /// * `retain_key_func` must be monotone in its first argument: for any
    ///   timestamp `ts1` and key `k` such that `retain_key_func(ts1, k) =
    ///   false`, and for any `ts2 >= ts1` it must hold that
    ///   `retain_key_func(ts2, k) = false`, i.e., once a key is rejected, it
    ///   will remain rejected as the bound increases.
    #[track_caller]
    pub fn integrate_trace_retain_keys<TS, RK>(
        &self,
        bounds_stream: &Stream<C, TypedBox<TS, DynData>>,
        retain_key_func: RK,
    ) -> Stream<C, Spine<B>>
    where
        TS: DBData + Erase<DynData>,
        RK: Fn(&B::Key, &TS) -> bool + Clone + 'static,
        B::InnerBatch: Send,
    {
        let factories = BatchReaderFactories::new::<B::Key, B::Val, B::R>();

        self.inner()
            .dyn_integrate_trace_retain_keys(
                &factories,
                &bounds_stream.inner_data(),
                Box::new(move |ts| {
                    let ts = clone_box(ts);
                    let retain_key_func = retain_key_func.clone();
                    Box::new(move |k| {
                        retain_key_func(unsafe { k.downcast::<B::Key>() }, unsafe {
                            ts.as_ref().downcast::<TS>()
                        })
                    })
                }),
            )
            .typed()
    }

    /// Similar to
    /// [`integrate_trace_retain_keys`](`Self::integrate_trace_retain_keys`),
    /// but applies a retainment policy to values in the trace.
    #[track_caller]
    pub fn integrate_trace_retain_values<TS, RV>(
        &self,
        bounds_stream: &Stream<C, TypedBox<TS, DynData>>,
        retain_value_func: RV,
    ) -> Stream<C, Spine<B>>
    where
        TS: DBData + Erase<DynData>,
        RV: Fn(&B::Val, &TS) -> bool + Clone + 'static,
        B::InnerBatch: Send,
    {
        let factories = BatchReaderFactories::new::<B::Key, B::Val, B::R>();

        self.inner()
            .dyn_integrate_trace_retain_values(
                &factories,
                &bounds_stream.inner_data(),
                Box::new(move |ts: &DynData| {
                    let ts = clone_box(ts);
                    let retain_val_func = retain_value_func.clone();
                    Box::new(move |v| {
                        retain_val_func(unsafe { v.downcast::<B::Val>() }, unsafe {
                            ts.as_ref().downcast::<TS>()
                        })
                    })
                }),
            )
            .typed()
    }

    #[track_caller]
    pub fn integrate_trace(&self) -> Stream<C, Spine<B>>
    where
        Spine<B>: SizeOf,
    {
        let factories = BatchReaderFactories::new::<B::Key, B::Val, B::R>();

        self.inner().dyn_integrate_trace(&factories).typed()
    }

    pub fn integrate_trace_with_bound(
        &self,
        lower_key_bound: TraceBound<<B::Inner as DynBatchReader>::Key>,
        lower_val_bound: TraceBound<<B::Inner as DynBatchReader>::Val>,
    ) -> Stream<C, Spine<B>>
    where
        Spine<B>: SizeOf,
    {
        let factories = BatchReaderFactories::new::<B::Key, B::Val, B::R>();

        self.inner()
            .dyn_integrate_trace_with_bound(&factories, lower_key_bound, lower_val_bound)
            .typed()
    }
}
