use crate::{
    trace::BatchReaderFactories,
    typed_batch::{IndexedZSet, TypedBox},
    Circuit, Stream, ZWeight,
};

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: IndexedZSet,
    Box<B::DynK>: Clone,
{
    /// Extract a subset of values that fall within a moving window from a
    /// stream of time-indexed values.
    ///
    /// This is a general form of the windowing operator that supports tumbling,
    /// rolling windows, watermarks, etc., by relying on a user-supplied
    /// function to compute window bounds at each clock cycle.
    ///
    /// This operator maintains the window **incrementally**, i.e., it outputs
    /// changes to the contents of the window at each clock cycle.  The
    /// complete contents of the window can be computed by integrating the
    /// output stream.
    ///
    /// # Arguments
    ///
    /// * `self` - stream of indexed Z-sets (indexed by time).  The notion of
    ///   time here is distinct from the DBSP logical time and can be modeled
    ///   using any type that implements `Ord`.
    ///
    /// * `bounds` - stream that contains window bounds to use at each clock
    ///   cycle.  At each clock cycle, it contains a `(start_time, end_time)`
    ///   that describes a right-open time range `[start_time..end_time)`, where
    ///   `end_time >= start_time`.  `start_time` must grow monotonically, i.e.,
    ///   `start_time1` and `start_time2` read from the stream at two successive
    ///   clock cycles must satisfy `start_time2 >= start_time1`.
    ///
    /// # Output
    ///
    /// The output stream contains **changes** to the contents of the window: at
    /// every clock cycle it retracts values that belonged to the window at
    /// the previous cycle, but no longer do, and inserts new values added
    /// to the window.  The latter include new values in the input stream
    /// that belong to the `[start_time..end_time)` range and values from
    /// earlier inputs that fall within the new range, but not the previous
    /// range.
    ///
    /// # Circuit
    ///
    /// ```text
    ///                       bounds
    ///
    /// ───────────────────────────────────────────────────┐
    ///                                                    │
    ///        ┌────────────────────────────────────────┐  │
    ///        │                                        │  │
    ///        │                                        ▼  ▼
    /// self   │     ┌────────────────┐             ┌───────────┐
    /// ───────┴────►│ TraceAppend    ├──┐          │ Window    ├─────►
    ///              └────────────────┘  │          └───────────┘
    ///                ▲                 │                 ▲
    ///                │    ┌──┐         │                 │
    ///                └────┤z1│◄────────┘                 │
    ///                     └┬─┘                           │
    ///                      │            trace            │
    ///                      └─────────────────────────────┘
    /// ```
    pub fn window(
        &self,
        bounds: &Stream<C, (TypedBox<B::Key, B::DynK>, TypedBox<B::Key, B::DynK>)>,
    ) -> Stream<C, B> {
        let factories = BatchReaderFactories::new::<B::Key, B::Val, ZWeight>();

        let bounds = unsafe { bounds.transmute_payload::<(Box<B::DynK>, Box<B::DynK>)>() };
        self.inner().dyn_window(&factories, &bounds).typed()
    }
}
