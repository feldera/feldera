//! Operators to organize time series data into windows.

use crate::{
    algebra::{IndexedZSet, NegByRef},
    circuit::{
        operator_traits::{Operator, TernaryOperator},
        Circuit, OwnershipPreference, Scope, Stream,
    },
    dynamic::ClonableTrait,
    operator::dynamic::trace::TraceBound,
    trace::{Batch, BatchFactories, BatchReader, BatchReaderFactories, Cursor, Spillable, Spine},
};
use std::{borrow::Cow, cmp::max, marker::PhantomData};

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: IndexedZSet + Spillable,
    Box<B::Key>: Clone,
{
    /// See [`Stream::window`].
    pub fn dyn_window(
        &self,
        input_factories: &B::Factories,
        stored_factories: &<B::Spilled as BatchReader>::Factories,
        bounds: &Stream<C, (Box<B::Key>, Box<B::Key>)>,
    ) -> Stream<C, B::Spilled> {
        let bound = TraceBound::new();
        let bound_clone = bound.clone();
        bounds.apply(move |(lower, _upper)| {
            bound_clone.set(lower.clone());
        });
        let trace = self
            .dyn_spill(stored_factories)
            .dyn_integrate_trace_with_bound(stored_factories, bound, TraceBound::new())
            .delay_trace();
        self.circuit().add_ternary_operator(
            <Window<B>>::new(input_factories, stored_factories),
            &trace,
            self,
            bounds,
        )
    }
}

struct Window<B>
where
    B: IndexedZSet + Spillable,
{
    factories: B::Factories,
    output_factories: <B::Spilled as BatchReader>::Factories,
    // `None` means we're at the start of a clock epoch, no inputs
    // have been received yet, and window boundaries haven't been set.
    window: Option<(Box<B::Key>, Box<B::Key>)>,
    _phantom: PhantomData<B>,
}

impl<B> Window<B>
where
    B: IndexedZSet + Spillable,
{
    pub fn new(
        factories: &B::Factories,
        output_factories: &<B::Spilled as BatchReader>::Factories,
    ) -> Self {
        Self {
            factories: factories.clone(),
            output_factories: output_factories.clone(),
            window: None,
            _phantom: PhantomData,
        }
    }
}

impl<B> Operator for Window<B>
where
    B: IndexedZSet + Spillable,
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

impl<B> TernaryOperator<Spine<B::Spilled>, B, (Box<B::Key>, Box<B::Key>), B::Spilled> for Window<B>
where
    B: IndexedZSet + Spillable,
    Box<B::Key>: Clone,
{
    /// * `batch` - input stream containing new time series data points indexed
    ///   by time.
    /// * `trace` - trace of the input stream up to, but not including current
    ///   clock cycle.
    /// * `bounds` - window bounds.  The lower bound must grow monotonically.
    // TODO: This can be optimized to add tuples in order, so we can use the
    // builder API to construct the output batch.  This requires processing
    // regions in order + extra care to iterate over `batch` and `trace` jointly
    // in region3.
    fn eval(
        &mut self,
        trace: Cow<'_, Spine<B::Spilled>>,
        batch: Cow<'_, B>,
        bounds: Cow<'_, (Box<B::Key>, Box<B::Key>)>,
    ) -> B::Spilled {
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
        let mut tuples = self.factories.weighted_items_factory().default_box();
        let mut tuple = self.factories.weighted_item_factory().default_box();
        let mut key = self.factories.key_factory().default_box();

        let mut trace_cursor = trace.cursor();
        let mut batch_cursor = batch.cursor();

        if let Some((start0, end0)) = &self.window {
            // Retract tuples in `trace` that slid out of the window (region 1).
            trace_cursor.seek_key(start0);
            while trace_cursor.key_valid()
                && trace_cursor.key() < &start1
                && trace_cursor.key() < end0
            {
                trace_cursor.key().clone_to(&mut *key);
                trace_cursor.map_values(&mut |val, weight| {
                    let (kv, w) = tuple.split_mut();
                    let (k, v) = kv.split_mut();
                    key.clone_to(k);
                    val.clone_to(v);
                    **w = weight.neg_by_ref();
                    tuples.push_val(&mut *tuple);
                });
                trace_cursor.step_key();
            }

            // If the window shrunk, retract values that dropped off the right end of the
            // window.
            if &end1 < end0 {
                trace_cursor.seek_key(&end1);
                while trace_cursor.key_valid() && trace_cursor.key() < end0 {
                    trace_cursor.key().clone_to(&mut key);
                    trace_cursor.map_values(&mut |val, weight| {
                        let (kv, w) = tuple.split_mut();
                        let (k, v) = kv.split_mut();
                        key.clone_to(k);
                        val.clone_to(v);
                        **w = weight.neg_by_ref();

                        tuples.push_val(&mut *tuple)
                    });
                    trace_cursor.step_key();
                }
            }

            // Add tuples in `trace` that slid into the window (region 3).
            trace_cursor.seek_key(max(end0, &start1));
            while trace_cursor.key_valid() && trace_cursor.key() < &end1 {
                trace_cursor.key().clone_to(&mut key);
                trace_cursor.map_values(&mut |val, weight| {
                    let (kv, w) = tuple.split_mut();
                    let (k, v) = kv.split_mut();
                    key.clone_to(k);
                    val.clone_to(v);
                    weight.clone_to(w);

                    tuples.push_val(&mut *tuple)
                });
                trace_cursor.step_key();
            }
        };

        // Insert tuples in `batch` that fall within the new window.
        batch_cursor.seek_key(&start1);
        while batch_cursor.key_valid() && batch_cursor.key() < &end1 {
            batch_cursor.key().clone_to(&mut key);
            batch_cursor.map_values(&mut |val, weight| {
                let (kv, w) = tuple.split_mut();
                let (k, v) = kv.split_mut();
                key.clone_to(k);
                val.clone_to(v);
                weight.clone_to(w);

                tuples.push_val(&mut *tuple)
            });
            batch_cursor.step_key();
        }

        self.window = Some((start1, end1));
        B::Spilled::dyn_from_tuples(&self.output_factories, (), &mut tuples)
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
        dynamic::{DowncastTrait, DynData, Erase},
        indexed_zset,
        operator::{dynamic::trace::TraceBound, Generator},
        typed_batch::{OrdIndexedZSet, TypedBox},
        utils::Tup2,
        zset, Circuit, RootCircuit, Runtime, Stream,
    };
    use size_of::SizeOf;
    use std::vec;

    #[test]
    fn sliding() {
        let circuit = RootCircuit::build(move |circuit| {
            type Time = u64;

            let mut input = vec![
                zset! {
                    // old value before the first window, should never appear in the output.
                    Tup2(800, "800".to_string()) => 1i64, Tup2(900, "900".to_string()) => 1, Tup2(950, "950".to_string()) => 1, Tup2(999, "999".to_string()) => 1,
                    // will appear in the next window
                    Tup2(1000, "1000".to_string()) => 1
                },
                zset! {
                    // old value before the first window
                    Tup2(700, "700".to_string()) => 1,
                    // too late, the window already moved forward
                    Tup2(900, "900".to_string()) => 1,
                    Tup2(901, "901".to_string()) => 1,
                    Tup2(999, "999".to_string()) => 1,
                    Tup2(1000, "1000".to_string()) => 1,
                    Tup2(1001, "1001".to_string()) => 1, // will appear in the next window
                    Tup2(1002, "1002".to_string()) => 1, // will appear two windows later
                    Tup2(1003, "1003".to_string()) => 1, // will appear three windows later
                },
                zset! { Tup2(1004, "1004".to_string()) => 1 }, // no new values in this window
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
            let bounds: Stream<_, (TypedBox<Time, DynData>, TypedBox<Time, DynData>)> = circuit.add_source(Generator::new(move || {
                let res = (TypedBox::new(clock - 100), TypedBox::new(clock));
                clock += 1;
                res
            }));

            let index1: Stream<_, OrdIndexedZSet<Time, String>> = circuit
                .add_source(Generator::new(move || input.next().unwrap()))
                .map_index(|Tup2(k, v)| (*k, v.clone()));
            index1
                .window(&bounds)
                .inspect(move |batch| assert_eq!(batch, &output.next().unwrap()));
            Ok(())
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
            type Time = u64;

            let mut input = vec![
                // window: 995..1000
                zset! { Tup2(700, "700".to_string()) => 1 , Tup2(995, "995".to_string()) => 1 , Tup2(996, "996".to_string()) => 1 , Tup2(999, "999".to_string()) => 1 , Tup2(1000, "1000".to_string()) => 1 },
                zset! { Tup2(995, "995".to_string()) =>  1 , Tup2(1000, "1000".to_string()) => 1 , Tup2(1001, "1001".to_string()) => 1 },
                zset! { Tup2(999, "999".to_string()) => 1 },
                zset! { Tup2(1002, "1002".to_string()) => 1 },
                zset! { Tup2(1003, "1003".to_string()) => 1 },
                // window: 1000..1005
                zset! { Tup2(996, "996".to_string()) => 1 }, // no longer within window
                zset! { Tup2(999, "999".to_string()) => 1 },
                zset! { Tup2(1004, "1004".to_string()) => 1 },
                zset! { Tup2(1005, "1005".to_string()) => 1 }, // next window
                zset! { Tup2(1010, "1010".to_string()) => 1 },
                // window: 1005..1010
                zset! { Tup2(1005, "1005".to_string()) => 1  },
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
            let bounds: Stream<_, (TypedBox<Time, DynData>, TypedBox<Time, DynData>)> = circuit
                .add_source(Generator::new(move || {
                    let start = (clock / WINDOW_SIZE) * WINDOW_SIZE - WINDOW_SIZE;
                    let res = (TypedBox::new(start), TypedBox::new(start + WINDOW_SIZE));
                    clock += 1;
                    res
                }));

            let index1: Stream<_, OrdIndexedZSet<Time, String>> = circuit
                .add_source(Generator::new(move || input.next().unwrap()))
                .map_index(|Tup2(k, v)| (*k, v.clone()));
            index1
                .window(&bounds)
                .inspect(move |batch| assert_eq!(batch, &output.next().unwrap()));
            Ok(())
        })
        .unwrap().0;

        for _ in 0..11 {
            circuit.step().unwrap();
        }
    }

    #[test]
    fn shrinking() {
        let circuit = RootCircuit::build(move |circuit| {
            type Time = u64;

            let mut input= vec![
                zset! {
                    Tup2(800u64, "800".to_string()) => 1,
                    Tup2(900, "900".to_string()) => 1,
                    Tup2(950, "950".to_string()) => 1,
                    Tup2(990, "990".to_string()) => 1,
                    Tup2(999, "999".to_string()) => 1,
                    Tup2(1000, "1000".to_string()) => 1
                },
                zset! {
                    Tup2(700, "700".to_string()) => 1,
                    Tup2(900, "900".to_string()) => 1,
                    Tup2(901, "901".to_string()) => 1,
                    Tup2(915, "915".to_string()) => 1,
                    Tup2(940, "940".to_string()) => 1,
                    Tup2(985, "985".to_string()) => 1,
                    Tup2(999, "999".to_string()) => 1,
                    Tup2(1000, "1000".to_string()) => 1,
                    Tup2(1001, "1001".to_string()) => 1,
                    Tup2(1002, "1002".to_string()) => 1,
                    Tup2(1003, "1003".to_string()) => 1,
                },
                zset! { Tup2(1004, "1004".to_string()) => 1,
                        Tup2(1010, "1010".to_string()) => 1,
                        Tup2(1020, "1020".to_string()) => 1,
                        Tup2(1039, "1039".to_string()) => 1 },
                zset! {},
                zset! {},
                zset! {},
            ]
            .into_iter();

            let mut output = vec![
                indexed_zset! { Time => String: 900 => {"900".to_string() => 1} , 950 => {"950".to_string() => 1} , 990 => {"990".to_string() => 1}, 999 => {"999".to_string() => 1} },
                indexed_zset! { 900 => {"900".to_string() => -1} , 915 => {"915".to_string() => 1} , 940 => {"940".to_string() => 1} , 985 => {"985".to_string() => 1}, 990 => {"990".to_string() => -1}, 999 => {"999".to_string() => -1} },
                indexed_zset! { 915 => {"915".to_string() => -1} , 985 => {"985".to_string() => -1} },
                indexed_zset! { 1000 => {"1000".to_string() => 2}, 1001 => {"1001".to_string() => 1}, 1002 => {"1002".to_string() => 1}, 1003 => {"1003".to_string() => 1}, 1004 => {"1004".to_string() => 1}, 1010 => {"1010".to_string() => 1}, 1020 => {"1020".to_string() => 1}, 1039 => {"1039".to_string() => 1}, 985 => {"985".to_string() => 1}, 990 => {"990".to_string() => 1}, 999 => {"999".to_string() => 2} },
                indexed_zset! { 1039 => {"1039".to_string() => -1}, 940 => {"940".to_string() => -1} },
                indexed_zset! { 1020 => {"1020".to_string() => -1}, 950 => {"950".to_string() => -1} },
            ]
            .into_iter();

            let mut windows = vec![
                // Shrink window from both ends.
                (900u64, 1000u64),
                (910, 990),
                (920, 980),
                // Jump forward and start shrinking again.
                (940, 1040),
                (950, 1030),
                (960, 1020),
            ]
            .into_iter();

            let bounds: Stream<_, (TypedBox<Time, DynData>, TypedBox<Time, DynData>)> = circuit
                .add_source(Generator::new(move || {
                    let (t1, t2) = windows.next().unwrap();
                    (TypedBox::new(t1), TypedBox::new(t2))
                }));

            let index1: Stream<_, OrdIndexedZSet<Time, String>> = circuit
                .add_source(Generator::new(move || input.next().unwrap()))
                .map_index(|Tup2(k, v)| (*k, v.clone()));
            index1
                .window(&bounds)
                .inspect(move |batch| assert_eq!(batch, &output.next().unwrap()));
            Ok(())
        })
        .unwrap().0;

        for _ in 0..6 {
            circuit.step().unwrap();
        }
    }

    #[test]
    fn bounded_memory() {
        let (mut dbsp, input_handle) = Runtime::init_circuit(8, |circuit| {
            let (input, input_handle) = circuit.add_input_zset::<i64>();
            let bounds =
                input
                    .waterline_monotonic(|| 0, |ts| *ts)
                    .apply(|ts: &TypedBox<i64, DynData>| {
                        (
                            TypedBox::new(*ts.inner().downcast_checked::<i64>() - 1000),
                            TypedBox::new(*ts.inner().downcast_checked::<i64>()),
                        )
                    });

            let bound = TraceBound::new();
            bound.set(Box::new(i64::max_value()).erase_box());

            input.window(&bounds);

            input
                .spill()
                .integrate_trace_with_bound(bound, TraceBound::new())
                .apply(|trace| {
                    assert!(trace.size_of().total_bytes() < 20000);
                });

            Ok(input_handle)
        })
        .unwrap();

        for i in 0..10000 {
            for j in i * 100..(i + 1) * 100 {
                input_handle.push(j, 1);
            }
            dbsp.step().unwrap();
        }
    }
}
