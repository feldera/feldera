use super::{radix_tree_update, Prefix, TreeNode};
use crate::{
    algebra::{HasOne, IndexedZSet, ZRingValue},
    circuit::{
        operator_traits::{Operator, TernaryOperator},
        GlobalNodeId, OwnershipPreference, Scope,
    },
    circuit_cache_key,
    operator::{
        trace::{DelayedTraceId, IntegrateTraceId, UntimedTraceAppend, Z1Trace},
        Aggregator,
    },
    trace::{Batch, BatchReader, Builder, Spine},
    Circuit, NumEntries, OrdIndexedZSet, Stream,
};
use num::PrimInt;
use size_of::SizeOf;
use std::{borrow::Cow, cmp::Ordering, marker::PhantomData, ops::Neg};

circuit_cache_key!(TreeAggregateId<C, D, Agg>(GlobalNodeId => Stream<C, D>));

/// A batch that contains updates to a radix tree.
pub trait RadixTreeBatch<TS, A>: Batch<Key = Prefix<TS>, Val = TreeNode<TS, A>, Time = ()> {}

impl<TS, A, B> RadixTreeBatch<TS, A> for B where
    B: Batch<Key = Prefix<TS>, Val = TreeNode<TS, A>, Time = ()>
{
}

pub trait RadixTreeReader<TS, A>:
    BatchReader<Key = Prefix<TS>, Val = TreeNode<TS, A>, Time = ()>
{
}

impl<TS, A, B> RadixTreeReader<TS, A> for B where
    B: BatchReader<Key = Prefix<TS>, Val = TreeNode<TS, A>, Time = ()>
{
}

pub type OrdRadixTree<TS, A, R> = OrdIndexedZSet<Prefix<TS>, TreeNode<TS, A>, R>;

impl<P, Z> Stream<Circuit<P>, Z>
where
    P: Clone + 'static,
    Z: Clone + 'static,
{
    /// Given a batch of updates to a time series stream, computes a stream of
    /// updates to its radix tree.
    ///
    /// # Limitations
    ///
    /// Unlike `Stream::partitioned_tree_aggregate()`, this operator is
    /// currently not parallelized, performing all work in a single worker
    /// thread.
    pub fn tree_aggregate<Agg>(
        &self,
        aggregator: Agg,
    ) -> Stream<Circuit<P>, OrdRadixTree<Z::Key, Agg::Accumulator, isize>>
    where
        Z: IndexedZSet + SizeOf + NumEntries + Send,
        Z::Key: PrimInt,
        Agg: Aggregator<Z::Val, (), Z::R>,
        Agg::Accumulator: Default,
    {
        self.tree_aggregate_generic::<Agg, OrdRadixTree<Z::Key, Agg::Accumulator, isize>>(
            aggregator,
        )
    }

    /// Like [`Self::tree_aggregate`], but can return any batch type.
    pub fn tree_aggregate_generic<Agg, O>(&self, aggregator: Agg) -> Stream<Circuit<P>, O>
    where
        Z: IndexedZSet + SizeOf + NumEntries + Send,
        Z::Key: PrimInt,
        Agg: Aggregator<Z::Val, (), Z::R>,
        Agg::Accumulator: Default,
        O: RadixTreeBatch<Z::Key, Agg::Accumulator>,
        O::R: ZRingValue,
    {
        self.circuit()
            .cache_get_or_insert_with(
                <TreeAggregateId<_, _, Agg>>::new(self.origin_node_id().clone()),
                move || {
                    let aggregator = aggregator.clone();
                    self.circuit().region("tree_aggregate", move || {
                        let circuit = self.circuit();
                        let stream = self.gather(0);

                        // We construct the following circuit.  See `RadixTreeAggregate`
                        // documentation for details.
                        //
                        // ```
                        //          ┌─────────────────────────────────────────┐
                        //          │                                         │                           output
                        //          │                                         │                 ┌─────────────────────────────────►
                        //          │                                         ▼                 │
                        //    stream│     ┌───────────────┐         ┌──────────────────────┐    │      ┌──────────────────┐
                        // ─────────┴─────┤integrate_trace├───────► │  RadixTreeAggregate  ├────┴─────►│UntimedTraceAppend├──┐
                        //                └───────────────┘         └──────────────────────┘           └──────────────────┘  │
                        //                                                    ▲                               ▲              │output_trace
                        //                                                    │                               │              │
                        //                                                    │                           ┌───┴───┐          │
                        //                                                    └───────────────────────────┤Z1Trace│◄─────────┘
                        //                                                     output_trace_delayed       └───────┘
                        // ```
                        let (output_trace_delayed, z1feedback) =
                            circuit.add_feedback(<Z1Trace<Spine<O>>>::new(
                                false,
                                self.circuit().root_scope(),
                            ));

                        let output = circuit.add_ternary_operator(
                            RadixTreeAggregate::new(aggregator),
                            &stream,
                            &stream.integrate_trace(),
                            &output_trace_delayed,
                        );

                        let output_trace = circuit.add_binary_operator_with_preference(
                            <UntimedTraceAppend<Spine<O>>>::new(),
                            (
                                &output_trace_delayed,
                                OwnershipPreference::STRONGLY_PREFER_OWNED,
                            ),
                            (&output, OwnershipPreference::PREFER_OWNED),
                        );
                        z1feedback.connect_with_preference(
                            &output_trace,
                            OwnershipPreference::STRONGLY_PREFER_OWNED,
                        );

                        circuit.cache_insert(
                            DelayedTraceId::new(output_trace.origin_node_id().clone()),
                            output_trace_delayed,
                        );
                        circuit.cache_insert(
                            IntegrateTraceId::new(output.origin_node_id().clone()),
                            output_trace,
                        );

                        output
                    })
                },
            )
            .clone()
    }
}

/// Ternary operator that implements the internals of `tree_aggregate`.
///
/// * Input stream 1: updates to the time series.  Only used to identify
///   affected times.
/// * Input stream 2: trace containing the accumulated time series data.
/// * Input stream 3: trace containing the current contents of the radix tree.
struct RadixTreeAggregate<Z, IT, OT, Agg, O> {
    aggregator: Agg,
    phantom: PhantomData<(Z, IT, OT, O)>,
}

impl<Z, IT, OT, Agg, O> RadixTreeAggregate<Z, IT, OT, Agg, O> {
    pub fn new(aggregator: Agg) -> Self {
        Self {
            aggregator,
            phantom: PhantomData,
        }
    }
}

impl<Z, IT, OT, Agg, O> Operator for RadixTreeAggregate<Z, IT, OT, Agg, O>
where
    Z: 'static,
    IT: 'static,
    OT: 'static,
    Agg: 'static,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("RadixTreeAggregate")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z, IT, OT, Agg, O> TernaryOperator<Z, IT, OT, O> for RadixTreeAggregate<Z, IT, OT, Agg, O>
where
    Z: IndexedZSet,
    Z::Key: PrimInt,
    IT: BatchReader<Key = Z::Key, Val = Z::Val, Time = (), R = Z::R> + Clone,
    OT: RadixTreeReader<Z::Key, Agg::Accumulator, R = O::R> + Clone,
    Agg: Aggregator<Z::Val, (), Z::R>,
    Agg::Accumulator: Default,
    O: RadixTreeBatch<Z::Key, Agg::Accumulator>,
    O::R: ZRingValue,
{
    fn eval<'a>(
        &mut self,
        delta: Cow<'a, Z>,
        input_trace: Cow<'a, IT>,
        output_trace: Cow<'a, OT>,
    ) -> O {
        let mut updates = Vec::with_capacity(delta.key_count());

        radix_tree_update::<Z::Key, Z::Val, Z::R, Agg, _, _, _, _>(
            delta.cursor(),
            input_trace.cursor(),
            output_trace.cursor(),
            &self.aggregator,
            &mut updates,
        );

        let mut builder = O::Builder::with_capacity((), updates.len() * 2);

        // `updates` are already ordered by prefix.  All that remains is to order
        // insertion and deletion within each update.
        for update in updates.into_iter() {
            match update.new.cmp(&update.old) {
                Ordering::Equal => {}
                Ordering::Less => {
                    if let Some(new) = update.new {
                        builder.push((O::item_from(update.prefix.clone(), new), O::R::one()));
                    };
                    if let Some(old) = update.old {
                        builder.push((O::item_from(update.prefix, old), O::R::one().neg()));
                    };
                }
                Ordering::Greater => {
                    if let Some(old) = update.old {
                        builder.push((O::item_from(update.prefix.clone(), old), O::R::one().neg()));
                    };
                    if let Some(new) = update.new {
                        builder.push((O::item_from(update.prefix, new), O::R::one()));
                    };
                }
            }
        }

        builder.done()
    }
}

#[cfg(test)]
mod test {
    use super::super::{test::test_aggregate_range, RadixTreeCursor};
    use crate::{
        algebra::DefaultSemigroup, operator::Fold, trace::BatchReader, Circuit, CollectionHandle,
    };
    use std::{
        collections::{btree_map::Entry, BTreeMap},
        sync::{Arc, Mutex},
    };

    fn update_key(
        input: &CollectionHandle<u64, (u64, isize)>,
        contents: &mut BTreeMap<u64, u64>,
        key: u64,
        upd: (u64, isize),
    ) {
        input.push(key, upd);
        match contents.entry(key) {
            Entry::Vacant(ve) => {
                assert_eq!(upd.1, 1);
                ve.insert(upd.0);
            }
            Entry::Occupied(mut oe) => {
                assert!(upd.1 == 1 || upd.1 == -1);
                if upd.1 == 1 {
                    *oe.get_mut() += upd.0;
                } else {
                    *oe.get_mut() -= upd.0;
                }
                if *oe.get() == 0 {
                    oe.remove();
                }
            }
        }
    }

    #[test]
    fn test_tree_aggregate() {
        let contents = Arc::new(Mutex::new(BTreeMap::new()));
        let contents_clone = contents.clone();

        let (circuit, input) = Circuit::build(move |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<u64, u64, isize>();

            let aggregator = <Fold<_, DefaultSemigroup<_>, _, _>>::new(
                0u64,
                |agg: &mut u64, val: &u64, _w: isize| *agg += val,
            );

            input
                .tree_aggregate(aggregator)
                .integrate_trace()
                .apply(move |tree_trace| {
                    println!("Radix tree:");
                    let mut treestr = String::new();
                    tree_trace.cursor().format_tree(&mut treestr).unwrap();
                    println!("{treestr}");
                    tree_trace
                        .cursor()
                        .validate::<DefaultSemigroup<_>>(&contents_clone.lock().unwrap());
                    test_aggregate_range::<_, _, _, _, DefaultSemigroup<_>>(
                        &mut tree_trace.cursor(),
                        &contents_clone.lock().unwrap(),
                    );
                });

            input_handle
        })
        .unwrap();

        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0x1000_0000_0000_0001,
            (1, 1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0x1000_0000_0000_0002,
            (2, 1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0x1000_1000_0000_0000,
            (3, 1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0x1000_0000_0000_0002,
            (2, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf100_0000_0000_0001,
            (4, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf200_0000_0000_0001,
            (5, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_0000_0000_0001,
            (6, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_0000_0001,
            (7, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_0001,
            (8, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_1001,
            (9, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1100_1001,
            (10, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1100_1001,
            (10, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf400_1000_1100_1001,
            (11, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_0000_0001,
            (7, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0x1000_0000_0000_0001,
            (1, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0x1000_1000_0000_0000,
            (3, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf100_0000_0000_0001,
            (4, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf200_0000_0000_0001,
            (5, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_0000_0000_0001,
            (6, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_0001,
            (8, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_1001,
            (9, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf400_1000_1100_1001,
            (11, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf100_0000_0000_0001,
            (4, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf200_0000_0000_0001,
            (5, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_0000_0000_0001,
            (6, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_0000_0001,
            (7, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_0001,
            (8, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_0001,
            (11, 1),
        );
        circuit.step().unwrap();
    }
}
