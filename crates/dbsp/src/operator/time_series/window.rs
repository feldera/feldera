//! Operators to organize time series data into windows.

use crate::{
    algebra::{IndexedZSet, NegByRef},
    circuit::{
        operator_traits::{Operator, TernaryOperator},
        Circuit, OwnershipPreference, Scope, Stream,
    },
    operator::trace::TraceBound,
    trace::{cursor::Cursor, BatchReader, Spine},
};
use std::{borrow::Cow, cmp::max, marker::PhantomData};

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: IndexedZSet,
    B::R: NegByRef,
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
    pub fn window(&self, bounds: &Stream<C, (B::Key, B::Key)>) -> Stream<C, B> {
        let bound = TraceBound::new();
        let bound_clone = bound.clone();
        bounds.apply(move |(lower, _upper)| {
            bound_clone.set(lower.clone());
            ()
        });
        let trace = self.integrate_trace_with_bound(Some(bound)).delay_trace();
        self.circuit()
            .add_ternary_operator(<Window<B>>::new(), &trace, self, bounds)
    }
}

struct Window<B>
where
    B: IndexedZSet,
{
    // `None` means we're at the start of a clock epoch, no inputs
    // have been received yet, and window boundaries haven't been set.
    window: Option<(B::Key, B::Key)>,
    _phantom: PhantomData<B>,
}

impl<B> Window<B>
where
    B: IndexedZSet,
{
    pub fn new() -> Self {
        Self {
            window: None,
            _phantom: PhantomData,
        }
    }
}

impl<B> Operator for Window<B>
where
    B: IndexedZSet,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Window")
    }

    fn clock_start(&mut self, _scope: Scope) {
        self.window = None;
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // Windows can currently only be used in top-level circuits.
        // Do we have meaningful examples of using windows inside nested scopes?
        panic!("'Window' operator used in fixedpoint iteration")
    }
}

impl<B> TernaryOperator<Spine<B>, B, (B::Key, B::Key), B> for Window<B>
where
    B: IndexedZSet,
    B::R: NegByRef,
{
    /// * `batch` - input stream containing new time series data points indexed
    ///   by time.
    /// * `trace` - trace of the input stream up to, but not including current
    ///   clock cycle.
    /// * `bounds` - window bounds.  The lower bound must grow monotonically.
    // TODO: This can be optimized to add tuples in order, so we can use the
    // builder API to construct the output batch.  This requires processing
    // regions in order + extra cate to iterate over `batch` and `trace` jointly
    // in region3.
    fn eval(
        &mut self,
        trace: Cow<'_, Spine<B>>,
        batch: Cow<'_, B>,
        bounds: Cow<'_, (B::Key, B::Key)>,
    ) -> B {
        //           ┌────────────────────────────────────────┐
        //           │       previous window                  │
        //           │                                        │             e1
        // ──────────┴────────────┬───────────────────────────┴──────────────┬─────►
        //          s0          s1│                          e0              │
        //                        │         new window                       │
        //                        └──────────────────────────────────────────┘
        //  region 1: [s0 .. s1)
        //  region 2: [s1 .. e0)
        //  region 3: [e0 .. e1)

        let (start1, end1) = bounds.into_owned();
        let trace = trace.as_ref();
        let batch = batch.as_ref();

        // TODO: In order to preallocate the buffer, we need to estimate the number of
        // keys in each component below.  For this, we need to extend
        // `Cursor::seek` to return the number of keys skipped over by the search.
        let mut tuples = Vec::new();
        let mut trace_cursor = trace.cursor();
        let mut batch_cursor = batch.cursor();

        if let Some((start0, end0)) = &self.window {
            // Retract tuples in `trace` that slid out of the window (region 1).
            trace_cursor.seek_key(start0);
            while trace_cursor.key_valid()
                && trace_cursor.key() < &start1
                && trace_cursor.key() < end0
            {
                let key = trace_cursor.key().clone();
                trace_cursor.map_values(|val, weight| {
                    tuples.push((B::item_from(key.clone(), val.clone()), weight.neg_by_ref()))
                });
                trace_cursor.step_key();
            }

            // If the window shrunk, retract values that dropped off the right end of the
            // window.
            if &end1 < end0 {
                trace_cursor.seek_key(&end1);
                while trace_cursor.key_valid() && trace_cursor.key() < end0 {
                    let key = trace_cursor.key().clone();
                    trace_cursor.map_values(|val, weight| {
                        tuples.push((B::item_from(key.clone(), val.clone()), weight.neg_by_ref()))
                    });
                    trace_cursor.step_key();
                }
            }

            // Add tuples in `trace` that slid into the window (region 3).
            trace_cursor.seek_key(max(end0, &start1));
            while trace_cursor.key_valid() && trace_cursor.key() < &end1 {
                let key = trace_cursor.key().clone();
                trace_cursor.map_values(|val, weight| {
                    tuples.push((B::item_from(key.clone(), val.clone()), weight.clone()))
                });
                trace_cursor.step_key();
            }
        };

        // Insert tuples in `batch` that fall within the new window.
        batch_cursor.seek_key(&start1);
        while batch_cursor.key_valid() && batch_cursor.key() < &end1 {
            let key = batch_cursor.key().clone();
            batch_cursor.map_values(|val, weight| {
                tuples.push((B::item_from(key.clone(), val.clone()), weight.clone()))
            });
            batch_cursor.step_key();
        }

        self.window = Some((start1, end1));
        B::from_tuples((), tuples)
    }

    fn input_preference(
        &self,
    ) -> (
        OwnershipPreference,
        OwnershipPreference,
        OwnershipPreference,
    ) {
        (
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{
        indexed_zset, operator::{Generator, trace::TraceBound}, zset, Circuit, OrdIndexedZSet, RootCircuit, Runtime, Stream,
    };
    use std::vec;
    use size_of::SizeOf;

    #[test]
    fn sliding() {
        let circuit = RootCircuit::build(move |circuit| {
            type Time = usize;

            let mut input = vec![
                zset! {
                    // old value before the first window, should never appear in the output.
                    (800, "800".to_string()) => 1, (900, "900".to_string()) => 1, (950, "950".to_string()) => 1, (999, "999".to_string()) => 1,
                    // will appear in the next window
                    (1000, "1000".to_string()) => 1
                },
                zset! {
                    // old value before the first window
                    (700, "700".to_string()) => 1,
                    // too late, the window already moved forward
                    (900, "900".to_string()) => 1,
                    (901, "901".to_string()) => 1,
                    (999, "999".to_string()) => 1,
                    (1000, "1000".to_string()) => 1,
                    (1001, "1001".to_string()) => 1, // will appear in the next window
                    (1002, "1002".to_string()) => 1, // will appear two windows later
                    (1003, "1003".to_string()) => 1, // will appear three windows later
                },
                zset! { (1004, "1004".to_string()) => 1 }, // no new values in this window
                zset! {},
                zset! {},
                zset! {},
            ]
            .into_iter();

            let mut output = vec![
                indexed_zset! { 900 => {"900".to_string() => 1} , 950 => {"950".to_string() => 1} , 999 => {"999".to_string() => 1} },
                indexed_zset! { 900 => {"900".to_string() => -1} , 901 => {"901".to_string() => 1} , 999 => {"999".to_string() => 1} , 1000 => {"1000".to_string() => 2} },
                indexed_zset! { 901 => {"901".to_string() => -1} , 1001 => {"1001".to_string() => 1} },
                indexed_zset! { 1002 => {"1002".to_string() => 1} },
                indexed_zset! { 1003 => {"1003".to_string() => 1} },
                indexed_zset! { 1004 => {"1004".to_string() => 1} },
            ]
            .into_iter();

            let mut clock: Time = 1000;
            let bounds: Stream<_, (Time, Time)> = circuit.add_source(Generator::new(move || {
                let res = (clock - 100, clock);
                clock += 1;
                res
            }));

            let index1: Stream<_, OrdIndexedZSet<Time, String, isize>> = circuit
                .add_source(Generator::new(move || input.next().unwrap()))
                .index();
            index1
                .window(&bounds)
                .inspect(move |batch| assert_eq!(batch, &output.next().unwrap()));
        })
        .unwrap()
        .0;

        for _ in 0..6 {
            circuit.step().unwrap();
        }
    }

    #[test]
    fn tumbling() {
        let circuit = RootCircuit::build(move |circuit| {
            type Time = usize;

            let mut input = vec![
                // window: 995..1000
                zset! { (700, "700".to_string()) => 1 , (995, "995".to_string()) => 1 , (996, "996".to_string()) => 1 , (999, "999".to_string()) => 1 , (1000, "1000".to_string()) => 1 },
                zset! { (995, "995".to_string()) =>  1 , (1000, "1000".to_string()) => 1 , (1001, "1001".to_string()) => 1 },
                zset! { (999, "999".to_string()) => 1 },
                zset! { (1002, "1002".to_string()) => 1 },
                zset! { (1003, "1003".to_string()) => 1 },
                // window: 1000..1005
                zset! { (996, "996".to_string()) => 1 }, // no longer within window
                zset! { (999, "999".to_string()) => 1 },
                zset! { (1004, "1004".to_string()) => 1 },
                zset! { (1005, "1005".to_string()) => 1 }, // next window
                zset! { (1010, "1010".to_string()) => 1 },
                // window: 1005..1010
                zset! { (1005, "1005".to_string()) => 1  },
            ]
            .into_iter();

            let mut output = vec![
                indexed_zset! { 995 => {"995".to_string() => 1} , 996 => {"996".to_string() => 1} , 999 => {"999".to_string() => 1} },
                indexed_zset! { 995 => {"995".to_string() => 1} },
                indexed_zset! { 999 => {"999".to_string() => 1} },
                indexed_zset! {},
                indexed_zset! {},
                indexed_zset! { 1000 => {"1000".to_string() => 2} , 1001 => {"1001".to_string() => 1} , 1002 => {"1002".to_string() => 1} , 1003 => {"1003".to_string() => 1} , 995 => {"995".to_string() => -2} , 996 => {"996".to_string() => -1} , 999 => {"999".to_string() => -2} },
                indexed_zset! {},
                indexed_zset! { 1004 => {"1004".to_string() => 1} },
                indexed_zset! {},
                indexed_zset! {},
                indexed_zset! { 1000 => {"1000".to_string() => -2} , 1001 => {"1001".to_string() => -1} , 1002 => {"1002".to_string() => -1} , 1003 => {"1003".to_string() => -1} , 1004 => {"1004".to_string() => -1} , 1005 => {"1005".to_string() => 2} },
            ]
            .into_iter();

            const WINDOW_SIZE: Time = 5;
            let mut clock: Time = 1000;
            let bounds: Stream<_, (Time, Time)> = circuit
                .add_source(Generator::new(move || {
                    let start = (clock / WINDOW_SIZE) * WINDOW_SIZE - WINDOW_SIZE;
                    let res = (start, start + WINDOW_SIZE);
                    clock += 1;
                    res
                }));

            let index1: Stream<_, OrdIndexedZSet<Time, String, isize>> = circuit
                .add_source(Generator::new(move || input.next().unwrap()))
                .index();
            index1
                .window(&bounds)
                .inspect(move |batch| assert_eq!(batch, &output.next().unwrap()));
        })
        .unwrap().0;

        for _ in 0..11 {
            circuit.step().unwrap();
        }
    }

    #[test]
    fn shrinking() {
        let circuit = RootCircuit::build(move |circuit| {
            type Time = usize;

            let mut input = vec![
                zset! {
                    (800, "800".to_string()) => 1,
                    (900, "900".to_string()) => 1,
                    (950, "950".to_string()) => 1,
                    (990, "990".to_string()) => 1,
                    (999, "999".to_string()) => 1,
                    (1000, "1000".to_string()) => 1
                },
                zset! {
                    (700, "700".to_string()) => 1,
                    (900, "900".to_string()) => 1,
                    (901, "901".to_string()) => 1,
                    (915, "915".to_string()) => 1,
                    (940, "940".to_string()) => 1,
                    (985, "985".to_string()) => 1,
                    (999, "999".to_string()) => 1,
                    (1000, "1000".to_string()) => 1,
                    (1001, "1001".to_string()) => 1,
                    (1002, "1002".to_string()) => 1,
                    (1003, "1003".to_string()) => 1,
                },
                zset! { (1004, "1004".to_string()) => 1,
                        (1010, "1010".to_string()) => 1,
                        (1020, "1020".to_string()) => 1,
                        (1039, "1039".to_string()) => 1 },
                zset! {},
                zset! {},
                zset! {},
            ]
            .into_iter();

            let mut output = vec![
                indexed_zset! { 900 => {"900".to_string() => 1} , 950 => {"950".to_string() => 1} , 990 => {"990".to_string() => 1}, 999 => {"999".to_string() => 1} },
                indexed_zset! { 900 => {"900".to_string() => -1} , 915 => {"915".to_string() => 1} , 940 => {"940".to_string() => 1} , 985 => {"985".to_string() => 1}, 990 => {"990".to_string() => -1}, 999 => {"999".to_string() => -1} },
                indexed_zset! { 915 => {"915".to_string() => -1} , 985 => {"985".to_string() => -1} },
                indexed_zset! { 1000 => {"1000".to_string() => 2}, 1001 => {"1001".to_string() => 1}, 1002 => {"1002".to_string() => 1}, 1003 => {"1003".to_string() => 1}, 1004 => {"1004".to_string() => 1}, 1010 => {"1010".to_string() => 1}, 1020 => {"1020".to_string() => 1}, 1039 => {"1039".to_string() => 1}, 985 => {"985".to_string() => 1}, 990 => {"990".to_string() => 1}, 999 => {"999".to_string() => 2} },
                indexed_zset! { 1039 => {"1039".to_string() => -1}, 940 => {"940".to_string() => -1} },
                indexed_zset! { 1020 => {"1020".to_string() => -1}, 950 => {"950".to_string() => -1} },
            ]
            .into_iter();

            let mut windows = vec![
                // Shrink window from both ends.
                (900, 1000),
                (910, 990),
                (920, 980),
                // Jump forward and start shrinking again.
                (940, 1040),
                (950, 1030),
                (960, 1020),
            ]
            .into_iter();

            let bounds: Stream<_, (Time, Time)> = circuit
                .add_source(Generator::new(move || {
                    windows.next().unwrap()
                }));


            let index1: Stream<_, OrdIndexedZSet<Time, String, isize>> = circuit
                .add_source(Generator::new(move || input.next().unwrap()))
                .index();
            index1
                .window(&bounds)
                .inspect(move |batch| assert_eq!(batch, &output.next().unwrap()));
        })
        .unwrap().0;

        for _ in 0..6 {
            circuit.step().unwrap();
        }
    }

    #[test]
    fn bounded_memory() {
        let (mut dbsp, input_handle) = Runtime::init_circuit(8, |circuit| {
            let (input, input_handle) = circuit.add_input_zset::<isize, isize>();
            let bounds = input.watermark_monotonic(|ts| *ts).apply(|ts| (*ts - 1000, *ts));

            let bound = TraceBound::new();
            bound.set(isize::max_value());

            input.window(&bounds);

            input.integrate_trace_with_bound(Some(bound))
                .apply(|trace| {
                    assert!(trace.size_of().total_bytes() < 20000);
                    ()
                });

            input_handle
        }).unwrap();

        for i in 0 .. 10000 {
            for j in i * 100 .. (i + 1) * 100 {
                input_handle.push(j, 1);
            }
            dbsp.step().unwrap();
        }
    }
}
