use crate::{
    Circuit, DBData, NestedCircuit, RootCircuit, Stream, Timestamp, ZWeight,
    algebra::{IndexedZSet, Lattice, OrdIndexedZSet},
    circuit::{WithClock, circuit_builder::CircuitBase},
    dynamic::{
        DataTrait, DynData, DynDataTyped, DynPair, DynPairs, DynUnit, Erase, Factory, LeanVec,
        WithFactory,
    },
    operator::{
        apply_n,
        dynamic::{
            MonoIndexedZSet, MonoZSet,
            multijoin::match_keys::{
                MatchBuilder, MatchFactories, MatchFunc, MatchGenerator, MatchKeyGenerator,
            },
        },
    },
    trace::{
        BatchReader, BatchReaderFactories, Cursor, SpineSnapshot, WeightedItem,
        cursor::SaturatingCursor, merge_batches_by_reference,
    },
    utils::Tup2,
};
use dyn_clone::DynClone;
use std::marker::PhantomData;

pub struct StarJoinFactories<I, O, T>
where
    I: IndexedZSet,
    O: IndexedZSet,
    T: Timestamp,
{
    pub input_factories: Vec<(I::Factories, <T::TimedBatch<I> as BatchReader>::Factories)>,
    pub output_factories: O::Factories,
    pub timed_item_factory:
        &'static dyn Factory<DynPair<DynDataTyped<T>, WeightedItem<O::Key, O::Val, O::R>>>,
    pub timed_items_factory:
        &'static dyn Factory<DynPairs<DynDataTyped<T>, WeightedItem<O::Key, O::Val, O::R>>>,
}

impl<I, O, T> Clone for StarJoinFactories<I, O, T>
where
    I: IndexedZSet,
    O: IndexedZSet,
    T: Timestamp,
{
    fn clone(&self) -> Self {
        Self {
            input_factories: self.input_factories.clone(),
            output_factories: self.output_factories.clone(),
            timed_item_factory: self.timed_item_factory,
            timed_items_factory: self.timed_items_factory,
        }
    }
}

impl<I, O, T> StarJoinFactories<I, O, T>
where
    I: IndexedZSet,
    O: IndexedZSet,
    T: Timestamp,
{
    pub fn new<OKType, OVType>() -> Self
    where
        OKType: DBData + Erase<O::Key>,
        OVType: DBData + Erase<O::Val>,
    {
        Self {
            input_factories: vec![],
            output_factories: BatchReaderFactories::new::<OKType, OVType, ZWeight>(),
            timed_item_factory:
                WithFactory::<Tup2<T, Tup2<Tup2<OKType, OVType>, ZWeight>>>::FACTORY,
            timed_items_factory:
                WithFactory::<LeanVec<Tup2<T, Tup2<Tup2<OKType, OVType>, ZWeight>>>>::FACTORY,
        }
    }

    pub fn add_input_factories(
        &mut self,
        batch_factories: I::Factories,
        trace_factories: <T::TimedBatch<I> as BatchReader>::Factories,
    ) {
        self.input_factories
            .push((batch_factories, trace_factories));
    }
}

/// A star join function combines values from multiple cursors with a common key.
///
/// The first argument is the untimed prefix cursor, the second argument is a list
/// of timed trace cursors.
///
/// The cursors are immutable, i.e., the join function can inspect current keys and
/// values, but it cannot move the cursors, nor access weights or times.
///
/// The callback is invoked 0 or more times, once for each output tuple.
pub trait StarJoinFuncTrait<C: WithClock, I: IndexedZSet, OK: ?Sized, OV: ?Sized>:
    FnMut(
        &<SpineSnapshot<I> as BatchReader>::Cursor<'_>,
        &[SaturatingCursor<'_, I::Key, I::Val, C::Time>],
        &mut dyn FnMut(&mut OK, &mut OV),
    ) + DynClone
{
}

impl<C, I, OK, OV, F> StarJoinFuncTrait<C, I, OK, OV> for F
where
    C: WithClock,
    I: IndexedZSet,
    OK: ?Sized,
    OV: ?Sized,
    F: FnMut(
            &<SpineSnapshot<I> as BatchReader>::Cursor<'_>,
            &[SaturatingCursor<'_, I::Key, I::Val, C::Time>],
            &mut dyn FnMut(&mut OK, &mut OV),
        ) + Clone
        + 'static,
{
}

dyn_clone::clone_trait_object! {<C: WithClock, I: IndexedZSet, OK: ?Sized, OV: ?Sized> StarJoinFuncTrait<C, I, OK, OV>}

pub type StarJoinFunc<C, I, OK, OV> = Box<dyn StarJoinFuncTrait<C, I, OK, OV>>;

impl Stream<RootCircuit, MonoIndexedZSet> {
    /// See `define_star_join_index!`.
    #[track_caller]
    pub fn dyn_star_join_index_mono(
        &self,
        factories: &StarJoinFactories<MonoIndexedZSet, MonoIndexedZSet, ()>,
        others: &[(Stream<RootCircuit, MonoIndexedZSet>, bool)],
        join_funcs: &[StarJoinFunc<RootCircuit, MonoIndexedZSet, DynData, DynData>],
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_star_join(factories, others, join_funcs)
    }

    /// See `define_star_join!`.
    #[track_caller]
    pub fn dyn_star_join_mono(
        &self,
        factories: &StarJoinFactories<MonoIndexedZSet, MonoZSet, ()>,
        others: &[(Stream<RootCircuit, MonoIndexedZSet>, bool)],
        join_funcs: &[StarJoinFunc<RootCircuit, MonoIndexedZSet, DynData, DynUnit>],
    ) -> Stream<RootCircuit, MonoZSet> {
        self.dyn_star_join(factories, others, join_funcs)
    }

    /// See `define_inner_star_join_index!`.
    #[track_caller]
    pub fn dyn_inner_star_join_index_mono(
        &self,
        factories: &StarJoinFactories<MonoIndexedZSet, MonoIndexedZSet, ()>,
        others: &[Stream<RootCircuit, MonoIndexedZSet>],
        join_funcs: &[StarJoinFunc<RootCircuit, MonoIndexedZSet, DynData, DynData>],
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_inner_star_join(factories, others, join_funcs)
    }

    /// See `define_inner_star_join!`.
    #[track_caller]
    pub fn dyn_inner_star_join_mono(
        &self,
        factories: &StarJoinFactories<MonoIndexedZSet, MonoZSet, ()>,
        others: &[Stream<RootCircuit, MonoIndexedZSet>],
        join_funcs: &[StarJoinFunc<RootCircuit, MonoIndexedZSet, DynData, DynUnit>],
    ) -> Stream<RootCircuit, MonoZSet> {
        self.dyn_inner_star_join(factories, others, join_funcs)
    }
}

impl Stream<NestedCircuit, MonoIndexedZSet> {
    /// See `define_inner_star_join_index!`.
    #[track_caller]
    pub fn dyn_inner_star_join_index_mono(
        &self,
        factories: &StarJoinFactories<
            MonoIndexedZSet,
            MonoIndexedZSet,
            <NestedCircuit as WithClock>::Time,
        >,
        others: &[Stream<NestedCircuit, MonoIndexedZSet>],
        join_funcs: &[StarJoinFunc<NestedCircuit, MonoIndexedZSet, DynData, DynData>],
    ) -> Stream<NestedCircuit, MonoIndexedZSet> {
        self.dyn_inner_star_join(factories, others, join_funcs)
    }

    /// See `define_inner_star_join!`.
    #[track_caller]
    pub fn dyn_inner_star_join_mono(
        &self,
        factories: &StarJoinFactories<
            MonoIndexedZSet,
            MonoZSet,
            <NestedCircuit as WithClock>::Time,
        >,
        others: &[Stream<NestedCircuit, MonoIndexedZSet>],
        join_funcs: &[StarJoinFunc<NestedCircuit, MonoIndexedZSet, DynData, DynUnit>],
    ) -> Stream<NestedCircuit, MonoZSet> {
        self.dyn_inner_star_join(factories, others, join_funcs)
    }
}

/// An implementation of the `MatchFunc` trait that wraps a `StarJoinFunc`.
///
/// Invokes the inner join_func for each combination of (value, time, weight) tuples
/// across all cursors for the current key.
///
/// Returns the output of the join_func with weight equal to the product of the weights
/// and time equal to the join of the times of the cursors.
struct StarJoinMatchFunc<C, I, OK, OV>
where
    C: Circuit,
    I: IndexedZSet,
    OK: ?Sized,
    OV: ?Sized,
{
    join_func: StarJoinFunc<C, I, OK, OV>,
    num_traces: usize,
}

impl<C, I, OK, OV> StarJoinMatchFunc<C, I, OK, OV>
where
    C: Circuit,
    I: IndexedZSet,
    OK: ?Sized,
    OV: ?Sized,
{
    fn new(join_func: StarJoinFunc<C, I, OK, OV>, num_traces: usize) -> Self {
        Self {
            join_func,
            num_traces,
        }
    }
}

impl<C, I, OK, OV> MatchFunc<C, I, OK, OV> for StarJoinMatchFunc<C, I, OK, OV>
where
    C: Circuit,
    I: IndexedZSet,
    OK: ?Sized + 'static,
    OV: ?Sized + 'static,
{
    type Generator = StarJoinMatchGenerator<C, I, OK, OV>;

    fn new_generator(&self, current_time: C::Time) -> Self::Generator {
        StarJoinMatchGenerator::new(self.join_func.clone(), current_time, self.num_traces)
    }
}

struct StarJoinMatchGenerator<C, I, OK, OV>
where
    C: WithClock,
    I: IndexedZSet,
    OK: ?Sized,
    OV: ?Sized,
{
    join_func: StarJoinFunc<C, I, OK, OV>,

    /// Current circuit time.
    current_time: C::Time,

    /// For each trace cursor, a vector of (weight, time) tuples for the current value
    /// under the cursor.
    weight_times: Vec<(Vec<(ZWeight, C::Time)>, usize)>,
}

impl<C, I, OK, OV> StarJoinMatchGenerator<C, I, OK, OV>
where
    C: WithClock,
    I: IndexedZSet,
    OK: ?Sized,
    OV: ?Sized,
{
    fn new(
        join_func: StarJoinFunc<C, I, OK, OV>,
        current_time: C::Time,
        num_traces: usize,
    ) -> Self {
        Self {
            join_func,
            current_time,
            weight_times: vec![(Vec::new(), 0); num_traces],
        }
    }
}

impl<C, I, OK, OV> MatchGenerator<C, I, OK, OV> for StarJoinMatchGenerator<C, I, OK, OV>
where
    C: WithClock,
    I: IndexedZSet,
    OK: ?Sized + 'static,
    OV: ?Sized + 'static,
{
    type Generator<'a, 'b>
        = StarJoinMatchIter<'a, 'b, C, I, OK, OV>
    where
        Self: 'a,
        'b: 'a;

    fn new_generator_for_key<'a, 'b>(
        &'a mut self,
        prefix_cursor: &'a mut <SpineSnapshot<I> as BatchReader>::Cursor<'b>,
        trace_cursors: &'a mut [SaturatingCursor<'b, I::Key, I::Val, C::Time>],
    ) -> Self::Generator<'a, 'b>
    where
        'b: 'a,
    {
        debug_assert_eq!(trace_cursors.len(), self.weight_times.len());
        debug_assert!(
            trace_cursors
                .iter()
                .all(|cursor| cursor.key_valid() && cursor.val_valid())
        );
        debug_assert!(
            trace_cursors
                .iter()
                .all(|cursor| cursor.key() == prefix_cursor.key())
        );

        StarJoinMatchIter::new(
            &mut self.join_func,
            self.current_time.clone(),
            prefix_cursor,
            trace_cursors,
            &mut self.weight_times,
        )
    }
}

struct StarJoinMatchIter<'a, 'b, C, I, OK, OV>
where
    C: WithClock,
    I: IndexedZSet,
    OK: ?Sized + 'static,
    OV: ?Sized + 'static,
{
    join_func: &'a mut StarJoinFunc<C, I, OK, OV>,

    /// Current circuit time.
    current_time: C::Time,

    prefix_cursor: &'a mut <SpineSnapshot<I> as BatchReader>::Cursor<'b>,
    trace_cursors: &'a mut [SaturatingCursor<'b, I::Key, I::Val, C::Time>],

    weight_times: &'a mut [(Vec<(ZWeight, C::Time)>, usize)],

    /// Index of the current trace cursor.
    current_index: usize,

    phantom: PhantomData<fn(&OK, &OV)>,
}

impl<'a, 'b, C, I, OK, OV> StarJoinMatchIter<'a, 'b, C, I, OK, OV>
where
    C: WithClock,
    I: IndexedZSet,
    OK: ?Sized + 'static,
    OV: ?Sized + 'static,
{
    fn new(
        join_func: &'a mut StarJoinFunc<C, I, OK, OV>,
        current_time: C::Time,
        prefix_cursor: &'a mut <SpineSnapshot<I> as BatchReader>::Cursor<'b>,
        trace_cursors: &'a mut [SaturatingCursor<'b, I::Key, I::Val, C::Time>],
        weight_times: &'a mut [(Vec<(ZWeight, C::Time)>, usize)],
    ) -> Self {
        let mut result = Self {
            join_func,
            current_time,
            prefix_cursor,
            trace_cursors,
            weight_times,
            current_index: 0,
            phantom: PhantomData,
        };
        result.init_weight_times();
        result
    }

    /// The current (weight, time) tuple for trace_cursor[current_index - 1] (or prefix cursor if current_index == 0).
    fn previous_weight_time(&mut self) -> (ZWeight, C::Time) {
        if self.current_index == 0 {
            (**self.prefix_cursor.weight(), self.current_time.clone())
        } else {
            self.weight_times[self.current_index - 1].0[self.weight_times[self.current_index - 1].1]
                .clone()
        }
    }

    /// Initialize weight_times[current_index]
    fn init_weight_times(&mut self) {
        self.weight_times[self.current_index].0.clear();
        let (previous_weight, previous_time) = self.previous_weight_time();

        self.trace_cursors[self.current_index].map_times(&mut |time, weight| {
            let weight = **weight * previous_weight;
            let time = time.join(&previous_time);

            self.weight_times[self.current_index]
                .0
                .push((weight, time.clone()));
        });
        self.weight_times[self.current_index].1 = 0;
    }

    /// Increment weight_times[current_index].1 by 1. If it reaches the end of the vector,
    /// advance trace_cursors[current_index] to the next value.
    fn advance_weight_times(&mut self) {
        if self.weight_times[self.current_index].1
            == self.weight_times[self.current_index].0.len() - 1
        {
            self.trace_cursors[self.current_index].step_val();
            // println!(
            //     "{} advance_weight_times: moving cursor {} to value: {:?}",
            //     Runtime::worker_index(),
            //     self.current_index,
            //     self.trace_cursors[self.current_index].get_val()
            // );
            self.init_weight_times();
        } else {
            self.weight_times[self.current_index].1 += 1;
        }
    }
}

impl<'a, 'b, C, I, OK, OV> MatchKeyGenerator<C, I, OK, OV>
    for StarJoinMatchIter<'a, 'b, C, I, OK, OV>
where
    C: WithClock,
    I: IndexedZSet,
    OK: ?Sized + 'static,
    OV: ?Sized + 'static,
{
    fn next(&mut self, mut cb: impl FnMut(&mut OK, &mut OV, C::Time, ZWeight)) -> bool {
        // Iterate over all (v, t, w) combinations in a depth-first order.
        loop {
            if self.trace_cursors[self.current_index].val_valid() {
                if self.current_index == self.trace_cursors.len() - 1 {
                    // current_index points to the last trace cursor: invoke join_func and yield.

                    let (w, t) = self.weight_times[self.current_index].0
                        [self.weight_times[self.current_index].1]
                        .clone();
                    (self.join_func)(self.prefix_cursor, self.trace_cursors, &mut |k, v| {
                        cb(k, v, t.clone(), w)
                    });
                    self.advance_weight_times();
                    return true;
                } else {
                    // we're not yet at the last trace cursor: advance to the next one.
                    self.current_index += 1;
                    self.init_weight_times();
                }
            } else if self.current_index == 0 {
                // Advance the prefix cursor.
                self.prefix_cursor.step_val();
                if !self.prefix_cursor.val_valid() {
                    // We're done.
                    return false;
                }

                // Rewind cursor in preparation for the next iteration.
                self.trace_cursors[self.current_index].rewind_vals();
                self.init_weight_times();
            } else {
                // Rewind cursor in preparation for the next iteration.
                self.trace_cursors[self.current_index].rewind_vals();

                // Pop the stack; advance the previous cursor.
                self.current_index -= 1;
                self.advance_weight_times();
            }
        }
    }
}

impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    #[track_caller]
    pub fn dyn_star_join<O>(
        &self,
        factories: &StarJoinFactories<OrdIndexedZSet<K, V>, O, ()>,
        others: &[(Self, bool)],
        join_funcs: &[StarJoinFunc<RootCircuit, OrdIndexedZSet<K, V>, O::Key, O::Val>],
    ) -> Stream<RootCircuit, O>
    where
        O: IndexedZSet,
    {
        // println!("dyn_star_join: {} others", others.len());
        assert!(!others.is_empty());
        assert_eq!(factories.input_factories.len(), others.len() + 1);
        assert_eq!(join_funcs.len(), others.len() + 1);

        // Shard and saturate the input streams.
        let streams = std::iter::once((self.clone(), false))
            .chain(others.iter().cloned())
            .zip(factories.input_factories.iter())
            .map(|((stream, saturate), (batch_factories, trace_factories))| {
                let stream = stream.dyn_shard(batch_factories);
                let saturated = if saturate {
                    Some(stream.dyn_saturate(batch_factories))
                } else {
                    None
                };
                (stream, saturated, (batch_factories, trace_factories))
            })
            .collect::<Vec<_>>();

        // Traces
        let traces = streams
            .iter()
            .map(|(stream, saturated, (batch_factories, trace_factories))| {
                (
                    stream.dyn_accumulate_trace(trace_factories, batch_factories),
                    saturated.is_some(),
                    batch_factories,
                )
            })
            .collect::<Vec<_>>();

        // Delayed traces
        let delayed_traces = traces
            .iter()
            .map(|(trace, saturated, batch_factories)| {
                (trace.accumulate_delay_trace(), *saturated, batch_factories)
            })
            .collect::<Vec<_>>();

        let mut output_streams = Vec::new();

        for (i, (stream, saturated, (batch_factories, _trace_factories))) in
            streams.iter().enumerate()
        {
            // Compute the i'th components of the join.
            // delta_i x delayed_traces[0..i] x traces[i..]

            let match_factories = MatchFactories {
                prefix_factories: factories.input_factories[i].0.clone(),
                output_factories: factories.output_factories.clone(),
                timed_item_factory: factories.timed_item_factory,
                timed_items_factory: factories.timed_items_factory,
            };

            let delayed = &delayed_traces[..i];
            let current = &traces[i + 1..];
            let join_func = join_funcs[i].clone();

            let output_stream: Stream<RootCircuit, O> =
                self.circuit()
                    .add_custom_node(format!("StarJoin_{i}").into(), move |node_id| {
                        let mut builder = if let Some(saturated) = saturated {
                            MatchBuilder::new(
                                &match_factories,
                                self.circuit().global_node_id().child(node_id),
                                self.circuit().clone(),
                                saturated.clone(),
                                StarJoinMatchFunc::new(join_func, others.len()),
                            )
                        } else {
                            MatchBuilder::new(
                                &match_factories,
                                self.circuit().global_node_id().child(node_id),
                                self.circuit().clone(),
                                stream.dyn_accumulate(batch_factories),
                                StarJoinMatchFunc::new(join_func, others.len()),
                            )
                        };
                        delayed
                            .iter()
                            .for_each(|(trace, saturate, batch_factories)| {
                                builder.add_input(
                                    trace.clone(),
                                    batch_factories.val_factory(),
                                    *saturate,
                                );
                            });
                        current
                            .iter()
                            .for_each(|(trace, saturate, batch_factories)| {
                                builder.add_input(
                                    trace.clone(),
                                    batch_factories.val_factory(),
                                    *saturate,
                                );
                            });
                        let join = builder.build();
                        let output_stream = join.output_stream();

                        (join, output_stream)
                    });
            output_streams.push(output_stream);
        }

        // Merge outputs.
        let output_factories = factories.output_factories.clone();

        apply_n(self.circuit(), output_streams.iter(), move |batches| {
            merge_batches_by_reference(
                &output_factories,
                batches.collect::<Vec<_>>().iter().map(|b| b.as_ref()),
                &None,
                &None,
            )
        })
    }
}

impl<C, K, V> Stream<C, OrdIndexedZSet<K, V>>
where
    C: Circuit,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    #[track_caller]
    pub fn dyn_inner_star_join<O>(
        &self,
        factories: &StarJoinFactories<OrdIndexedZSet<K, V>, O, C::Time>,
        others: &[Self],
        join_funcs: &[StarJoinFunc<C, OrdIndexedZSet<K, V>, O::Key, O::Val>],
    ) -> Stream<C, O>
    where
        O: IndexedZSet,
    {
        assert!(!others.is_empty());
        assert_eq!(factories.input_factories.len(), others.len() + 1);
        assert_eq!(join_funcs.len(), others.len() + 1);

        // Shard the input streams.
        let streams = std::iter::once(self)
            .chain(others.iter())
            .zip(factories.input_factories.iter())
            .map(|(stream, (batch_factories, trace_factories))| {
                (
                    stream.dyn_shard(batch_factories),
                    (batch_factories, trace_factories),
                )
            })
            .collect::<Vec<_>>();

        // Traces
        let traces = streams
            .iter()
            .map(|(stream, (batch_factories, trace_factories))| {
                (
                    stream.dyn_accumulate_trace(trace_factories, batch_factories),
                    batch_factories,
                )
            })
            .collect::<Vec<_>>();

        // Delayed traces
        let delayed_traces = traces
            .iter()
            .map(|(trace, batch_factories)| (trace.accumulate_delay_trace(), batch_factories))
            .collect::<Vec<_>>();

        let mut output_streams = Vec::new();

        for (i, (stream, (batch_factories, _trace_factories))) in streams.iter().enumerate() {
            let delayed = &delayed_traces[..i];
            let current = &traces[i + 1..];
            let join_func = join_funcs[i].clone();

            let match_factories = MatchFactories {
                prefix_factories: factories.input_factories[i].0.clone(),
                output_factories: factories.output_factories.clone(),
                timed_item_factory: factories.timed_item_factory,
                timed_items_factory: factories.timed_items_factory,
            };

            let output: Stream<C, O> = self.circuit().add_custom_node(
                format!("InnerStarJoin_{i}").into(),
                move |node_id| {
                    let mut builder = MatchBuilder::new(
                        &match_factories,
                        self.circuit().global_node_id().child(node_id),
                        self.circuit().clone(),
                        stream.dyn_accumulate(batch_factories),
                        StarJoinMatchFunc::new(join_func, others.len()),
                    );
                    delayed.iter().for_each(|(trace, batch_factories)| {
                        builder.add_input(trace.clone(), batch_factories.val_factory(), false);
                    });
                    current.iter().for_each(|(trace, batch_factories)| {
                        builder.add_input(trace.clone(), batch_factories.val_factory(), false);
                    });
                    let join = builder.build();
                    let output_stream = join.output_stream();

                    (join, output_stream)
                },
            );
            output_streams.push(output);
        }

        // Merge outputs.
        let output_factories = factories.output_factories.clone();

        apply_n(self.circuit(), output_streams.iter(), move |batches| {
            merge_batches_by_reference(
                &output_factories,
                batches.collect::<Vec<_>>().iter().map(|b| b.as_ref()),
                &None,
                &None,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        OrdIndexedZSet, OrdZSet, Runtime, Stream, ZWeight,
        algebra::F64,
        circuit::CircuitConfig,
        define_inner_star_join, define_inner_star_join_flatmap, define_inner_star_join_index,
        define_star_join, define_star_join_flatmap, define_star_join_index,
        utils::{Tup2, Tup3, Tup4},
    };
    use proptest::prelude::*;

    define_star_join_index!(4);
    define_star_join!(4);
    define_star_join_flatmap!(4);
    define_inner_star_join!(3);
    define_inner_star_join!(4);
    define_inner_star_join_index!(4);
    define_inner_star_join_flatmap!(4);

    fn star_join_test(
        input_data: Vec<(
            Vec<Tup2<i32, Tup2<u32, ZWeight>>>,
            Vec<Tup2<i32, Tup2<u64, ZWeight>>>,
            Vec<Tup2<i32, Tup2<String, ZWeight>>>,
            Vec<Tup2<i32, Tup2<F64, ZWeight>>>,
        )>,
        transaction: bool,
    ) {
        let (
            mut dbsp,
            (
                stream1,
                stream2,
                stream3,
                stream4,
                inner_join_index_output,
                inner_join_index_output2,
                inner_join_index_expected_output,
                inner_join_output,
                inner_join_output2,
                inner_join_expected_output,
                inner_join_flatmap_output,
                inner_join_flatmap_output2,
                inner_join_flatmap_expected_output,
                left_join_index1_output,
                left_join_index1_expected_output,
                left_join_index2_output,
                left_join_index2_expected_output,
            ),
        ) = Runtime::init_circuit(
            CircuitConfig::from(4).with_splitter_chunk_size_records(2),
            |circuit| {
                let (stream1, stream1_handle) = circuit.add_input_indexed_zset::<i32, u32>();
                let (stream2, stream2_handle) = circuit.add_input_indexed_zset::<i32, u64>();
                let (stream3, stream3_handle) = circuit.add_input_indexed_zset::<i32, String>();
                let (stream4, stream4_handle) = circuit.add_input_indexed_zset::<i32, F64>();

                let stream1 = stream1.map_index(|(k, v)| (*k, Some(*v)));
                let stream2 = stream2.map_index(|(k, v)| (*k, Some(*v)));
                let stream3 = stream3.map_index(|(k, v)| (*k, Some(v.clone())));
                let stream4 = stream4.map_index(|(k, v)| (*k, Some(*v)));

                // Inner join
                let inner_join_index_output = star_join_index4(
                    &stream1,
                    (&stream2, false),
                    (&stream3, false),
                    (&stream4, false),
                    |k, v1, v2, v3, v4| Some((*k, Tup4(*v1, *v2, v3.clone(), *v4))),
                )
                .accumulate_output();

                let inner_join_index_output2 = inner_star_join_index4(
                    &stream1,
                    &stream2,
                    &stream3,
                    &stream4,
                    |k, v1, v2, v3, v4| Some((*k, Tup4(*v1, *v2, v3.clone(), *v4))),
                )
                .accumulate_output();

                let inner_join_index_expected_output = stream1
                    .join_index(&stream2, |k, v1, v2| Some((*k, Tup2(*v1, *v2))))
                    .join_index(&stream3, |k, Tup2(v1, v2), v3| {
                        Some((*k, Tup3(*v1, *v2, v3.clone())))
                    })
                    .join_index(&stream4, |k, Tup3(v1, v2, v3), v4| {
                        Some((*k, Tup4(*v1, *v2, v3.clone(), *v4)))
                    })
                    .accumulate_output();

                let inner_join_output = star_join4(
                    &stream1,
                    (&stream2, false),
                    (&stream3, false),
                    (&stream4, false),
                    |_k, v1, v2, v3, v4| Some(Tup4(*v1, *v2, v3.clone(), *v4)),
                )
                .accumulate_output();

                let inner_join_output2 = inner_star_join4(
                    &stream1,
                    &stream2,
                    &stream3,
                    &stream4,
                    |_k, v1, v2, v3, v4| Some(Tup4(*v1, *v2, v3.clone(), *v4)),
                )
                .accumulate_output();

                let inner_join_expected_output = stream1
                    .join(&stream2, |k, v1, v2| Tup2(*k, Tup2(*v1, *v2)))
                    .map_index(|Tup2(k, v)| (*k, v.clone()))
                    .join(&stream3, |k, Tup2(v1, v2), v3| {
                        Tup2(*k, Tup3(*v1, *v2, v3.clone()))
                    })
                    .map_index(|Tup2(k, v)| (*k, v.clone()))
                    .join(&stream4, |_k, Tup3(v1, v2, v3), v4| {
                        Some(Tup4(*v1, *v2, v3.clone(), *v4))
                    })
                    .accumulate_output();

                let inner_join_flatmap_output = star_join_flatmap4(
                    &stream1,
                    (&stream2, false),
                    (&stream3, false),
                    (&stream4, false),
                    |_k, v1, v2, v3, v4| Some(Tup4(*v1, *v2, v3.clone(), *v4)),
                )
                .accumulate_output();

                let inner_join_flatmap_output2 = inner_star_join_flatmap4(
                    &stream1,
                    &stream2,
                    &stream3,
                    &stream4,
                    |_k, v1, v2, v3, v4| Some(Tup4(*v1, *v2, v3.clone(), *v4)),
                )
                .accumulate_output();

                let inner_join_flatmap_expected_output = stream1
                    .join(&stream2, |k, v1, v2| Tup2(*k, Tup2(*v1, *v2)))
                    .map_index(|Tup2(k, v)| (*k, v.clone()))
                    .join(&stream3, |k, Tup2(v1, v2), v3| {
                        Tup2(*k, Tup3(*v1, *v2, v3.clone()))
                    })
                    .map_index(|Tup2(k, v)| (*k, v.clone()))
                    .join_flatmap(&stream4, |_k, Tup3(v1, v2, v3), v4| {
                        Some(Tup4(*v1, *v2, v3.clone(), *v4))
                    })
                    .accumulate_output();

                // Outer join 1
                let left_join_index1_output = star_join_index4(
                    &stream1,
                    (&stream2, true),
                    (&stream3, true),
                    (&stream4, true),
                    |k, v1, v2, v3, v4| Some((*k, Tup4(*v1, *v2, v3.clone(), *v4))),
                )
                .accumulate_output();

                let left_join_index1_expected_output = stream1
                    .left_join_index(&stream2, |k, v1, v2| Some((*k, Tup2(*v1, *v2))))
                    .left_join_index(&stream3, |k, Tup2(v1, v2), v3| {
                        Some((*k, Tup3(*v1, *v2, v3.clone())))
                    })
                    .left_join_index(&stream4, |k, Tup3(v1, v2, v3), v4| {
                        Some((*k, Tup4(*v1, *v2, v3.clone(), *v4)))
                    })
                    .accumulate_output();

                // Outer join 2
                let left_join_index2_output = star_join_index4(
                    &stream1,
                    (&stream2, true),
                    (&stream3, false),
                    (&stream4, true),
                    |k, v1, v2, v3, v4| Some((*k, Tup4(*v1, *v2, v3.clone(), *v4))),
                )
                .accumulate_output();

                let left_join_index2_expected_output = stream1
                    .left_join_index(&stream2, |k, v1, v2| Some((*k, Tup2(*v1, *v2))))
                    .join_index(&stream3, |k, Tup2(v1, v2), v3| {
                        Some((*k, Tup3(*v1, *v2, v3.clone())))
                    })
                    .left_join_index(&stream4, |k, Tup3(v1, v2, v3), v4| {
                        Some((*k, Tup4(*v1, *v2, v3.clone(), *v4)))
                    })
                    .accumulate_output();

                Ok((
                    stream1_handle,
                    stream2_handle,
                    stream3_handle,
                    stream4_handle,
                    inner_join_index_output,
                    inner_join_index_output2,
                    inner_join_index_expected_output,
                    inner_join_output,
                    inner_join_output2,
                    inner_join_expected_output,
                    inner_join_flatmap_output,
                    inner_join_flatmap_output2,
                    inner_join_flatmap_expected_output,
                    left_join_index1_output,
                    left_join_index1_expected_output,
                    left_join_index2_output,
                    left_join_index2_expected_output,
                ))
            },
        )
        .unwrap();

        if transaction {
            dbsp.start_transaction().unwrap();
        }

        for step in 0..input_data.len() {
            // println!("step: {}", step);
            stream1.append(&mut input_data[step].0.clone());
            stream2.append(&mut input_data[step].1.clone());
            stream3.append(&mut input_data[step].2.clone());
            stream4.append(&mut input_data[step].3.clone());

            if !transaction {
                dbsp.transaction().unwrap();
                let inner_join_index_output = inner_join_index_output.concat().consolidate();
                let inner_join_index_output2 = inner_join_index_output2.concat().consolidate();
                let inner_join_index_expected_output =
                    inner_join_index_expected_output.concat().consolidate();
                let inner_join_output = inner_join_output.concat().consolidate();
                let inner_join_output2 = inner_join_output2.concat().consolidate();
                let inner_join_expected_output = inner_join_expected_output.concat().consolidate();
                let inner_join_flatmap_output = inner_join_flatmap_output.concat().consolidate();
                let inner_join_flatmap_output2 = inner_join_flatmap_output2.concat().consolidate();
                let inner_join_flatmap_expected_output =
                    inner_join_flatmap_expected_output.concat().consolidate();
                let left_join_index1_output = left_join_index1_output.concat().consolidate();
                let left_join_index1_expected_output =
                    left_join_index1_expected_output.concat().consolidate();
                let left_join_index2_output = left_join_index2_output.concat().consolidate();
                let left_join_index2_expected_output =
                    left_join_index2_expected_output.concat().consolidate();
                assert_eq!(inner_join_index_output, inner_join_index_expected_output);
                assert_eq!(inner_join_index_output2, inner_join_index_expected_output);
                assert_eq!(inner_join_output, inner_join_expected_output);
                assert_eq!(inner_join_output2, inner_join_expected_output);
                assert_eq!(left_join_index1_output, left_join_index1_expected_output);
                assert_eq!(left_join_index2_output, left_join_index2_expected_output);
                assert_eq!(
                    inner_join_flatmap_output,
                    inner_join_flatmap_expected_output
                );
                assert_eq!(
                    inner_join_flatmap_output2,
                    inner_join_flatmap_expected_output
                );
            }
        }

        if transaction {
            dbsp.commit_transaction().unwrap();
            let inner_join_index_output = inner_join_index_output.concat().consolidate();
            let inner_join_index_output2 = inner_join_index_output2.concat().consolidate();
            let inner_join_index_expected_output =
                inner_join_index_expected_output.concat().consolidate();
            let inner_join_output = inner_join_output.concat().consolidate();
            let inner_join_output2 = inner_join_output2.concat().consolidate();
            let inner_join_expected_output = inner_join_expected_output.concat().consolidate();
            let inner_join_flatmap_output = inner_join_flatmap_output.concat().consolidate();
            let inner_join_flatmap_output2 = inner_join_flatmap_output2.concat().consolidate();
            let inner_join_flatmap_expected_output =
                inner_join_flatmap_expected_output.concat().consolidate();
            let left_join_index1_output = left_join_index1_output.concat().consolidate();
            let left_join_index1_expected_output =
                left_join_index1_expected_output.concat().consolidate();
            let left_join_index2_output = left_join_index2_output.concat().consolidate();
            let left_join_index2_expected_output =
                left_join_index2_expected_output.concat().consolidate();
            assert_eq!(inner_join_index_output, inner_join_index_expected_output);
            assert_eq!(inner_join_index_output2, inner_join_index_expected_output);
            assert_eq!(inner_join_output, inner_join_expected_output);
            assert_eq!(inner_join_output2, inner_join_expected_output);
            assert_eq!(left_join_index1_output, left_join_index1_expected_output);
            assert_eq!(left_join_index2_output, left_join_index2_expected_output);
            assert_eq!(
                inner_join_flatmap_output,
                inner_join_flatmap_expected_output
            );
            assert_eq!(
                inner_join_flatmap_output2,
                inner_join_flatmap_expected_output
            );
        }
    }

    /// Compute paths in a directed graph such that all inner points of a path belong to the set of key points.
    /// (This key points constraint  justifies the use of the multiway join operator).
    ///
    /// `input_data` includes edges and key points to be added/removed at each step.
    fn recursive_star_join_test(
        input_data: Vec<(Vec<Tup2<Tup2<u32, u32>, ZWeight>>, Vec<Tup2<u32, ZWeight>>)>,
        transaction: bool,
    ) {
        let (mut dbsp, (edges_handle, key_points_handle, paths, expected_paths)) =
            Runtime::init_circuit(
                CircuitConfig::from(4).with_splitter_chunk_size_records(2),
                move |circuit| {
                    let (edges, edges_handle) = circuit.add_input_zset::<Tup2<u32, u32>>();
                    let (key_points, key_points_handle) = circuit.add_input_zset::<u32>();

                    let edges = edges.distinct();
                    let key_points = key_points.distinct();

                    let (paths, expected_paths) = circuit
                        .recursive(
                            |child,
                             (paths_delayed, expected_paths_delayed): (
                                Stream<_, OrdZSet<Tup2<u32, u32>>>,
                                Stream<_, OrdZSet<Tup2<u32, u32>>>,
                            )| {
                                let edges = edges.delta0(child);
                                let key_points = key_points.delta0(child).map_index(|x| (*x, ()));

                                let paths_inverted_indexed: Stream<_, OrdIndexedZSet<u32, u32>> =
                                    paths_delayed.map_index(|&Tup2(x, y)| (y, x));

                                let paths_inverted_indexed_expected: Stream<
                                    _,
                                    OrdIndexedZSet<u32, u32>,
                                > = expected_paths_delayed.map_index(|&Tup2(x, y)| (y, x));

                                let edges_indexed = edges.map_index(|Tup2(k, v)| (*k, *v));

                                let paths = edges.plus(&inner_star_join3_nested(
                                    &paths_inverted_indexed,
                                    &edges_indexed,
                                    &key_points,
                                    |_via, from, to, &()| Tup2(*from, *to),
                                ));

                                let expected_paths = edges.plus(
                                    &paths_inverted_indexed_expected
                                        .join_index(&edges_indexed, |via, from, to| {
                                            Some((*via, Tup2(*from, *to)))
                                        })
                                        .join(&key_points, |_via, &Tup2(from, to), &()| {
                                            Tup2(from, to)
                                        }),
                                );

                                Ok((paths, expected_paths))
                            },
                        )
                        .unwrap();

                    let paths_output = paths.accumulate_output();
                    let expected_paths_output = expected_paths.accumulate_output();

                    Ok((
                        edges_handle,
                        key_points_handle,
                        paths_output,
                        expected_paths_output,
                    ))
                },
            )
            .unwrap();

        if transaction {
            dbsp.start_transaction().unwrap();
        }

        for step in 0..input_data.len() {
            edges_handle.append(&mut input_data[step].0.clone());
            key_points_handle.append(&mut input_data[step].1.clone());
            if !transaction {
                dbsp.transaction().unwrap();
                let paths_output = paths.concat().consolidate();
                let expected_paths_output = expected_paths.concat().consolidate();
                assert_eq!(paths_output, expected_paths_output);
            }
        }

        if transaction {
            dbsp.commit_transaction().unwrap();
            let paths_output = paths.concat().consolidate();
            let expected_paths_output = expected_paths.concat().consolidate();
            assert_eq!(paths_output, expected_paths_output);
        }
    }

    #[test]
    fn test_recursive_star_join() {
        let input_data = vec![
            // Step 0: add edges 1->2, 2->3; mark 2 as key point.
            (
                vec![Tup2(Tup2(1, 2), 1), Tup2(Tup2(2, 3), 1)],
                vec![Tup2(2, 1)],
            ),
            // Step 1: extend chain with 3->4; mark 3 as key point.
            (vec![Tup2(Tup2(3, 4), 1)], vec![Tup2(3, 1)]),
            // Step 2: extend chain with 4->5; mark 4 as key point.
            (vec![Tup2(Tup2(4, 5), 1)], vec![Tup2(4, 1)]),
            // Step 3: remove key point 3 (paths via 3 should disappear).
            (vec![], vec![Tup2(3, -1)]),
            // Step 4: remove edge 2->3 (breaks the original chain).
            (vec![Tup2(Tup2(2, 3), -1)], vec![]),
            // Step 5: add shortcut edge 2->5 (bypasses missing 3->4).
            (vec![Tup2(Tup2(2, 5), 1)], vec![]),
            // Step 6: add 5->6 and mark 5,6 as key points.
            (vec![Tup2(Tup2(5, 6), 1)], vec![Tup2(5, 1), Tup2(6, 1)]),
            // Step 7: add edge 6->2 (introduce a cycle).
            (vec![Tup2(Tup2(6, 2), 1)], vec![]),
            // Step 8: remove key point 2 (cycle still exists but via non-key).
            (vec![], vec![Tup2(2, -1)]),
            // Step 9: remove edge 6->2 and key point 5 (break cycle, shrink keys).
            (vec![Tup2(Tup2(6, 2), -1)], vec![Tup2(5, -1)]),
            // Step 10: re-add key point 2 and edge 2->3 (restore part of chain).
            (vec![Tup2(Tup2(2, 3), 1)], vec![Tup2(2, 1)]),
            // Step 11: re-add key point 3 and edge 3->4 (chain continues again).
            (vec![Tup2(Tup2(3, 4), 1)], vec![Tup2(3, 1)]),
            // Step 12: add new branch 3->7 and mark 7 as key point.
            (vec![Tup2(Tup2(3, 7), 1)], vec![Tup2(7, 1)]),
            // Step 13: add edge 7->5 (reconnect branch to existing node).
            (vec![Tup2(Tup2(7, 5), 1)], vec![]),
            // Step 14: add edge 5->8 and mark 8 as key point.
            (vec![Tup2(Tup2(5, 8), 1)], vec![Tup2(8, 1)]),
            // Step 15: remove edge 2->5 (drop the shortcut).
            (vec![Tup2(Tup2(2, 5), -1)], vec![]),
            // Step 16: remove key point 4 (paths via 4 should drop).
            (vec![], vec![Tup2(4, -1)]),
            // Step 17: add edge 8->2 (new cycle through 8).
            (vec![Tup2(Tup2(8, 2), 1)], vec![]),
            // Step 18: remove edge 7->5 (break branch reconnection).
            (vec![Tup2(Tup2(7, 5), -1)], vec![]),
            // Step 19: remove key point 7 and edge 8->2 (collapse new cycle).
            (vec![Tup2(Tup2(8, 2), -1)], vec![Tup2(7, -1)]),
        ];

        recursive_star_join_test(input_data, false);
    }

    fn weighted_vec_u32(
        max_keys: i32,
        max_values: u32,
        max_weight: ZWeight,
        max_vec_len: usize,
    ) -> impl Strategy<Value = Vec<Tup2<i32, Tup2<u32, ZWeight>>>> {
        prop::collection::vec(
            (0..=max_keys, 0u32..=max_values, -max_weight..=max_weight)
                .prop_map(|(k, v, w)| Tup2(k, Tup2(v, w))),
            0..max_vec_len,
        )
    }

    fn weighted_vec_u64(
        max_keys: i32,
        max_values: u64,
        max_weight: ZWeight,
        max_vec_len: usize,
    ) -> impl Strategy<Value = Vec<Tup2<i32, Tup2<u64, ZWeight>>>> {
        prop::collection::vec(
            (0..=max_keys, 0u64..=max_values, -max_weight..=max_weight)
                .prop_map(|(k, v, w)| Tup2(k, Tup2(v, w))),
            0..max_vec_len,
        )
    }

    fn weighted_vec_string(
        max_keys: i32,
        max_values: u64,
        max_weight: ZWeight,
        max_vec_len: usize,
    ) -> impl Strategy<Value = Vec<Tup2<i32, Tup2<String, ZWeight>>>> {
        prop::collection::vec(
            (
                -max_keys..=max_keys,
                0..=max_values,
                -max_weight..=max_weight,
            )
                .prop_map(|(k, v, w)| Tup2(k, Tup2(v.to_string(), w))),
            0..max_vec_len,
        )
    }

    fn weighted_vec_f64(
        max_keys: i32,
        max_values: f64,
        max_weight: ZWeight,
        max_vec_len: usize,
    ) -> impl Strategy<Value = Vec<Tup2<i32, Tup2<F64, ZWeight>>>> {
        prop::collection::vec(
            (
                -max_keys..=max_keys,
                0.0f64..=max_values,
                -max_weight..=max_weight,
            )
                .prop_map(|(k, v, w)| Tup2(k, Tup2(F64::from(v), w))),
            0..max_vec_len,
        )
    }

    fn input_step_strategy(
        max_keys: i32,
        max_values: u32,
        max_weight: ZWeight,
        max_vec_len: usize,
    ) -> impl Strategy<
        Value = (
            Vec<Tup2<i32, Tup2<u32, ZWeight>>>,
            Vec<Tup2<i32, Tup2<u64, ZWeight>>>,
            Vec<Tup2<i32, Tup2<String, ZWeight>>>,
            Vec<Tup2<i32, Tup2<F64, ZWeight>>>,
        ),
    > {
        (
            weighted_vec_u32(max_keys, max_values, max_weight, max_vec_len),
            weighted_vec_u64(max_keys, max_values as u64, max_weight, max_vec_len),
            weighted_vec_string(max_keys, max_values as u64, max_weight, max_vec_len),
            weighted_vec_f64(max_keys, max_values as f64, max_weight, max_vec_len),
        )
    }

    fn recursive_input_step_strategy(
        max_node: u32,
        max_weight: ZWeight,
        max_edges: usize,
        max_key_points: usize,
    ) -> impl Strategy<Value = (Vec<Tup2<Tup2<u32, u32>, ZWeight>>, Vec<Tup2<u32, ZWeight>>)> {
        let edges = prop::collection::vec(
            (0..=max_node, 0..=max_node, -max_weight..=max_weight)
                .prop_map(|(from, to, w)| Tup2(Tup2(from, to), w)),
            0..max_edges,
        );
        let key_points = prop::collection::vec(
            (0..=max_node, -max_weight..=max_weight).prop_map(|(node, w)| Tup2(node, w)),
            0..max_key_points,
        );
        (edges, key_points)
    }

    proptest! {
        #[test]
        fn proptest_star_join_index4_small_steps(
            input_data in prop::collection::vec(input_step_strategy(5, 10, 2, 5), 0..15),
        ) {
            star_join_test(input_data, false);
        }

        #[test]
        fn proptest_star_join_index4_big_steps(
            input_data in prop::collection::vec(input_step_strategy(5, 10, 2, 5), 0..15),
        ) {
            star_join_test(input_data, true);
        }

        #[test]
        fn proptest_star_join_index4_sparse_small_steps(
            input_data in prop::collection::vec(input_step_strategy(100, 10, 2, 5), 0..15),
        ) {
            star_join_test(input_data, false);
        }

        #[test]
        fn proptest_star_join_index4_sparse_big_steps(
            input_data in prop::collection::vec(input_step_strategy(100, 10, 2, 5), 0..15),
        ) {
            star_join_test(input_data, true);
        }

        #[test]
        fn proptest_recursive_star_join_small_steps(
            input_data in prop::collection::vec(recursive_input_step_strategy(10, 2, 6, 4), 0..20),
        ) {
            recursive_star_join_test(input_data, false);
        }

        #[test]
        fn proptest_recursive_star_join_big_steps(
            input_data in prop::collection::vec(recursive_input_step_strategy(10, 2, 6, 4), 0..15),
        ) {
            recursive_star_join_test(input_data, true);
        }
    }
}
