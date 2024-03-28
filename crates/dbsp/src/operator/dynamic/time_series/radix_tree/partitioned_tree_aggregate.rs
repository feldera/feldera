use super::{
    radix_tree_update, DynPrefix, DynTreeNode, Prefix, RadixTreeCursor, RadixTreeFactories,
    TreeNode,
};
use crate::{
    algebra::{HasOne, OrdIndexedZSet},
    circuit::{
        operator_traits::{Operator, TernaryOperator},
        Scope,
    },
    dynamic::{ClonableTrait, DataTrait, DynDataTyped, DynPair, Erase},
    operator::dynamic::{
        aggregate::DynAggregator,
        time_series::{
            PartitionCursor, PartitionedBatch, PartitionedBatchReader, PartitionedIndexedZSet,
        },
        trace::{TraceBounds, TraceFeedback},
    },
    trace::{cursor::CursorEmpty, BatchReader, BatchReaderFactories, Builder, Cursor, Spine},
    utils::Tup2,
    Circuit, DBData, DynZWeight, RootCircuit, Stream, ZWeight,
};
use dyn_clone::clone_box;
use num::PrimInt;
use size_of::SizeOf;
use std::{
    borrow::Cow, cmp::Ordering, collections::BTreeMap, fmt, fmt::Write, marker::PhantomData,
    ops::Neg,
};

/// Partitioned radix tree batch.
///
/// Partitioned batch where each partition contains a radix tree.
pub trait PartitionedRadixTreeBatch<TS: DBData + PrimInt, A: DataTrait + ?Sized>:
    PartitionedBatch<DynPrefix<TS>, DynTreeNode<TS, A>, R = DynZWeight>
{
}

impl<TS: DBData + PrimInt, A: DataTrait + ?Sized, B> PartitionedRadixTreeBatch<TS, A> for B where
    B: PartitionedBatch<DynPrefix<TS>, DynTreeNode<TS, A>, R = DynZWeight>
{
}

pub trait PartitionedRadixTreeReader<TS: DBData + PrimInt, A: DataTrait + ?Sized>:
    PartitionedBatchReader<DynPrefix<TS>, DynTreeNode<TS, A>, R = DynZWeight>
{
}

impl<TS: DBData + PrimInt, A: DataTrait + ?Sized, B> PartitionedRadixTreeReader<TS, A> for B where
    B: PartitionedBatchReader<DynPrefix<TS>, DynTreeNode<TS, A>, R = DynZWeight>
{
}

pub type OrdPartitionedRadixTree<PK, TS, A> =
    OrdIndexedZSet<PK, DynPair<DynPrefix<TS>, DynTreeNode<TS, A>>>;

pub type OrdPartitionedRadixTreeFactories<PK, TS, A> =
    <OrdPartitionedRadixTree<PK, TS, A> as BatchReader>::Factories;

pub type OrdPartitionedRadixTreeStream<PK, TS, A> =
    Stream<RootCircuit, OrdPartitionedRadixTree<PK, TS, A>>;

pub struct PartitionedTreeAggregateFactories<TS, V, Z, O, Acc>
where
    Z: PartitionedIndexedZSet<DynDataTyped<TS>, V>,
    O: PartitionedRadixTreeBatch<TS, Acc, Key = Z::Key>,
    Acc: DataTrait + ?Sized,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
{
    input_factories: Z::Factories,
    output_factories: O::Factories,
    radix_tree_factories: RadixTreeFactories<TS, Acc>,
    phantom: PhantomData<fn(&TS, &V)>,
}

impl<TS, V, Z, O, Acc> PartitionedTreeAggregateFactories<TS, V, Z, O, Acc>
where
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
    Z: PartitionedIndexedZSet<DynDataTyped<TS>, V>,
    O: PartitionedRadixTreeBatch<TS, Acc, Key = Z::Key>,
    Acc: DataTrait + ?Sized,
{
    pub fn new<KType, VType, AType>() -> Self
    where
        KType: DBData + Erase<Z::Key>,
        VType: DBData + Erase<V>,
        AType: DBData + Erase<Acc>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, Tup2<TS, VType>, ZWeight>(),
            output_factories: BatchReaderFactories::new::<
                KType,
                Tup2<Prefix<TS>, TreeNode<TS, AType>>,
                ZWeight,
            >(),
            radix_tree_factories: RadixTreeFactories::new::<AType>(),
            phantom: PhantomData,
        }
    }
}

impl<TS, V, Z, O, Acc> Clone for PartitionedTreeAggregateFactories<TS, V, Z, O, Acc>
where
    Z: PartitionedIndexedZSet<DynDataTyped<TS>, V>,
    O: PartitionedRadixTreeBatch<TS, Acc, Key = Z::Key>,
    Acc: DataTrait + ?Sized,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            input_factories: self.input_factories.clone(),
            output_factories: self.output_factories.clone(),
            radix_tree_factories: self.radix_tree_factories.clone(),
            phantom: PhantomData,
        }
    }
}

pub type OrdPartitionedTreeAggregateFactories<TS, V, Z, Acc> = PartitionedTreeAggregateFactories<
    TS,
    V,
    Z,
    OrdPartitionedRadixTree<<Z as BatchReader>::Key, TS, Acc>,
    Acc,
>;

/// Cursor over partitioned radix tree.
pub trait PartitionedRadixTreeCursor<PK, TS, A>:
    Cursor<PK, DynPair<DynPrefix<TS>, DynTreeNode<TS, A>>, (), DynZWeight> + Sized
where
    PK: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    TS: DBData + PrimInt,
{
    /// Produce a semi-human-readable representation of the partitioned tree
    /// for debugging purposes.
    fn format_tree<W>(&mut self, writer: &mut W) -> Result<(), fmt::Error>
    where
        W: Write,
    {
        while self.key_valid() {
            writeln!(writer, "Partition: {:?}", self.key())?;

            let mut partition_cursor = PartitionCursor::new(self);
            partition_cursor.format_tree(writer)?;
            self.step_key();
        }
        Ok(())
    }

    /// Self-diagnostics: validate that `self` points to a well-formed
    /// partitioned radix tree whose contents is equivalent to `contents`.
    fn validate(
        &mut self,
        contents: &BTreeMap<Box<PK>, BTreeMap<TS, Box<A>>>,
        combine: &dyn Fn(&mut A, &A),
    ) {
        let empty = BTreeMap::new();

        while self.key_valid() {
            let partition_contents = contents.get(self.key()).unwrap_or(&empty);

            let mut partition_cursor = PartitionCursor::new(self);
            partition_cursor.validate(partition_contents, combine);

            self.step_key();
        }
    }
}

impl<PK, TS, A, C> PartitionedRadixTreeCursor<PK, TS, A> for C
where
    A: DataTrait + ?Sized,
    PK: DataTrait + ?Sized,
    TS: DBData + PrimInt,
    C: Cursor<PK, DynPair<DynPrefix<TS>, DynTreeNode<TS, A>>, (), DynZWeight>,
{
}

impl<Z> Stream<RootCircuit, Z>
where
    Z: Clone + 'static,
{
    /// Given a batch of updates to a partitioned time series stream, computes a
    /// stream of updates to its partitioned radix tree.
    ///
    /// This is a building block for higher-level operators such as
    /// [`Stream::partitioned_rolling_aggregate`].
    pub fn partitioned_tree_aggregate<TS, V, Acc, Out>(
        &self,
        factories: &OrdPartitionedTreeAggregateFactories<TS, V, Z, Acc>,
        aggregator: &dyn DynAggregator<V, (), DynZWeight, Accumulator = Acc, Output = Out>,
    ) -> OrdPartitionedRadixTreeStream<Z::Key, TS, Acc>
    where
        Z: PartitionedIndexedZSet<DynDataTyped<TS>, V> + SizeOf + Send,
        TS: DBData + PrimInt,
        V: DataTrait + ?Sized,
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
    {
        self.partitioned_tree_aggregate_generic::<TS, V, Acc, Out, OrdPartitionedRadixTree<Z::Key, TS, Acc>>(
            factories,
            aggregator,
        )
    }

    /// Like [`Self::partitioned_tree_aggregate`], but can return any
    /// partitioned batch type.
    ///
    /// This is a building block for higher-level operators such as
    /// [`Stream::partitioned_rolling_aggregate`].
    pub fn partitioned_tree_aggregate_generic<TS, V, Acc, Out, O>(
        &self,
        factories: &PartitionedTreeAggregateFactories<TS, V, Z, O, Acc>,
        aggregator: &dyn DynAggregator<V, (), DynZWeight, Accumulator = Acc, Output = Out>,
    ) -> Stream<RootCircuit, O>
    where
        Z: PartitionedIndexedZSet<DynDataTyped<TS>, V> + SizeOf + Send,
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
        TS: DBData + PrimInt,
        V: DataTrait + ?Sized,
        O: PartitionedRadixTreeBatch<TS, Acc, Key = Z::Key>,
    {
        let stream = self.dyn_shard(&factories.input_factories);

        self.circuit()
            .region("partitioned_tree_aggregate", move || {
                let circuit = self.circuit();

                // We construct the following circuit.  See `RadixTreeAggregate`
                // documentation for details.
                //
                // ```
                //          ┌─────────────────────────────────────────┐
                //          │                                         │                                output
                //          │                                         │                        ┌─────────────────────────────────►
                //          │                                         ▼                        │
                //    stream│     ┌───────────────┐         ┌─────────────────────────────┐    │      ┌──────────────────┐
                // ─────────┴─────┤integrate_trace├───────► │PartitionedRadixTreeAggregate├────┴─────►│UntimedTraceAppend├──┐
                //                └───────────────┘         └─────────────────────────────┘           └──────────────────┘  │
                //                                                    ▲                                    ▲                │output_trace
                //                                                    │                                    │                │
                //                                                    │                                ┌───┴───┐            │
                //                                                    └────────────────────────────────┤Z1Trace│◄───────────┘
                //                                                          delayed_trace              └───────┘
                // ```

                // Note: In most use cases `partitioned_tree_aggregate` is applied to
                // the output of the `window` operator, in which case its input and
                // output traces are naturally bounded as we are maintaining the tree
                // over a bounded range of keys.
                let bounds = <TraceBounds<O::Key, O::Val>>::unbounded();

                let feedback = circuit
                    .add_integrate_trace_feedback::<Spine<O>>(&factories.output_factories, bounds);

                let output = circuit
                    .add_ternary_operator(
                        PartitionedRadixTreeAggregate::new(factories, aggregator),
                        &stream,
                        &stream.dyn_integrate_trace(&factories.input_factories),
                        &feedback.delayed_trace,
                    )
                    .mark_sharded();

                feedback.connect(&output);

                output
            })
    }
}

/// Ternary operator that implements the internals of
/// `partitioned_tree_aggregate`.
///
/// * Input stream 1: updates to the time series.  Only used to identify
///   affected times in each partition.
/// * Input stream 2: trace containing the accumulated partitioned time series
///   data.
/// * Input stream 3: trace containing the current contents of the partitioned
///   radix tree.
struct PartitionedRadixTreeAggregate<TS, V, Z, IT, OT, Acc, Out, O>
where
    Z: PartitionedIndexedZSet<DynDataTyped<TS>, V>,
    O: PartitionedRadixTreeBatch<TS, Acc, Key = Z::Key>,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
{
    aggregator: Box<dyn DynAggregator<V, (), DynZWeight, Accumulator = Acc, Output = Out>>,
    factories: PartitionedTreeAggregateFactories<TS, V, Z, O, Acc>,
    phantom: PhantomData<fn(&IT, &OT)>,
}

impl<TS, V, Z, IT, OT, Acc, Out, O> PartitionedRadixTreeAggregate<TS, V, Z, IT, OT, Acc, Out, O>
where
    Z: PartitionedIndexedZSet<DynDataTyped<TS>, V>,
    O: PartitionedRadixTreeBatch<TS, Acc, Key = Z::Key>,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
{
    pub fn new(
        factories: &PartitionedTreeAggregateFactories<TS, V, Z, O, Acc>,
        aggregator: &dyn DynAggregator<V, (), DynZWeight, Accumulator = Acc, Output = Out>,
    ) -> Self {
        Self {
            aggregator: clone_box(aggregator),
            factories: factories.clone(),
            phantom: PhantomData,
        }
    }
}

impl<TS, V, Z, IT, OT, Acc, Out, O> Operator
    for PartitionedRadixTreeAggregate<TS, V, Z, IT, OT, Acc, Out, O>
where
    Z: PartitionedIndexedZSet<DynDataTyped<TS>, V>,
    O: PartitionedRadixTreeBatch<TS, Acc, Key = Z::Key>,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
    IT: 'static,
    OT: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("PartitionedRadixTreeAggregate")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<TS, V, Z, IT, OT, Acc, Out, O> TernaryOperator<Z, IT, OT, O>
    for PartitionedRadixTreeAggregate<TS, V, Z, IT, OT, Acc, Out, O>
where
    Z: PartitionedIndexedZSet<DynDataTyped<TS>, V>,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    TS: DBData + PrimInt,
    V: DataTrait + ?Sized,
    IT: PartitionedBatchReader<DynDataTyped<TS>, V, Key = Z::Key, R = Z::R> + Clone,
    OT: PartitionedRadixTreeReader<TS, Acc, Key = Z::Key, R = O::R> + Clone,
    O: PartitionedRadixTreeBatch<TS, Acc, Key = Z::Key>,
{
    fn eval<'a>(
        &mut self,
        delta: Cow<'a, Z>,
        input_trace: Cow<'a, IT>,
        output_trace: Cow<'a, OT>,
    ) -> O {
        let mut builder =
            O::Builder::with_capacity(&self.factories.output_factories, (), delta.len() * 2);

        let mut updates = self
            .factories
            .radix_tree_factories
            .node_updates_factory
            .default_box();

        let mut delta_cursor = delta.cursor();
        let mut input_cursor = input_trace.cursor();
        let mut output_cursor = output_trace.cursor();

        let mut pair = self.factories.output_factories.val_factory().default_box();
        let mut key = self.factories.input_factories.key_factory().default_box();
        let mut key_clone = self.factories.input_factories.key_factory().default_box();

        while delta_cursor.key_valid() {
            // println!("partition: {:?}", delta_cursor.key());

            delta_cursor.key().clone_to(&mut *key);

            let delta_partition_cursor = PartitionCursor::new(&mut delta_cursor);

            input_cursor.seek_key(&key);
            output_cursor.seek_key(&key);

            updates.clear();

            if input_cursor.key_valid() && input_cursor.key() == &*key {
                // println!("input partition exists");
                /*while input_cursor.val_valid() {
                    // println!("input val: {:x?}", input_cursor.val());
                    input_cursor.step_val();
                }*/
                //input_cursor.rewind_vals();

                if output_cursor.key_valid() && output_cursor.key() == &*key {
                    // println!("tree partition exists");

                    radix_tree_update::<TS, V, Acc, _, _, _, _>(
                        &self.factories.radix_tree_factories,
                        delta_partition_cursor,
                        PartitionCursor::new(&mut input_cursor),
                        PartitionCursor::new(&mut output_cursor),
                        self.aggregator.as_ref(),
                        &mut *updates,
                    );
                } else {
                    radix_tree_update::<TS, V, Acc, _, _, _, _>(
                        &self.factories.radix_tree_factories,
                        delta_partition_cursor,
                        PartitionCursor::new(&mut input_cursor),
                        <CursorEmpty<_, _, _, O::R>>::new(
                            self.factories.output_factories.weight_factory(),
                        ),
                        self.aggregator.as_ref(),
                        &mut *updates,
                    );
                }
            } else if output_cursor.key_valid() && output_cursor.key() == &*key {
                radix_tree_update::<TS, V, Acc, _, _, _, _>(
                    &self.factories.radix_tree_factories,
                    delta_partition_cursor,
                    CursorEmpty::new(self.factories.input_factories.weight_factory()),
                    PartitionCursor::new(&mut output_cursor),
                    self.aggregator.as_ref(),
                    &mut *updates,
                );
            } else {
                radix_tree_update::<TS, V, Acc, _, _, _, _>(
                    &self.factories.radix_tree_factories,
                    delta_partition_cursor,
                    CursorEmpty::new(self.factories.input_factories.weight_factory()),
                    <CursorEmpty<_, _, _, O::R>>::new(
                        self.factories.output_factories.weight_factory(),
                    ),
                    self.aggregator.as_ref(),
                    &mut *updates,
                );
            }

            // `updates` are already ordered by prefix.  All that remains is to order
            // insertion and deletion within each update.
            for update in updates.dyn_iter_mut() {
                match update.new().cmp(update.old()) {
                    Ordering::Equal => {}
                    Ordering::Less => {
                        let mut prefix = update.prefix();

                        if let Some(new) = update.new_mut().get_mut() {
                            pair.from_vals(prefix.clone().erase_mut(), new);
                            key.clone_to(&mut key_clone);
                            builder.push_vals(
                                &mut key_clone,
                                &mut *pair,
                                ZWeight::one().erase_mut(),
                            );
                        };
                        if let Some(old) = update.old_mut().get_mut() {
                            pair.from_vals(prefix.erase_mut(), old);
                            key.clone_to(&mut key_clone);

                            builder.push_vals(
                                &mut key_clone,
                                &mut *pair,
                                ZWeight::one().neg().erase_mut(),
                            );
                        };
                    }
                    Ordering::Greater => {
                        let mut prefix = update.prefix();

                        if let Some(old) = update.old_mut().get_mut() {
                            pair.from_vals(prefix.clone().erase_mut(), old);
                            key.clone_to(&mut key_clone);

                            builder.push_vals(
                                &mut key_clone,
                                &mut *pair,
                                ZWeight::one().neg().erase_mut(),
                            );
                        };
                        if let Some(new) = update.new_mut().get_mut() {
                            pair.from_vals(prefix.erase_mut(), new);
                            key.clone_to(&mut key_clone);

                            builder.push_vals(
                                &mut key_clone,
                                &mut *pair,
                                ZWeight::one().erase_mut(),
                            );
                        };
                    }
                }
            }

            delta_cursor.step_key();
        }

        builder.done()
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{test::test_aggregate_range, OrdPartitionedTreeAggregateFactories, Prefix},
        OrdPartitionedRadixTree, PartitionCursor, PartitionedRadixTreeCursor,
    };
    use crate::{
        algebra::{DefaultSemigroup, Semigroup},
        dynamic::{DowncastTrait, DynData, DynDataTyped, DynPair, Erase},
        operator::{
            dynamic::{
                aggregate::DynAggregatorImpl,
                input::{AddInputIndexedZSetFactories, CollectionHandle},
                time_series::TreeNode,
            },
            Fold,
        },
        trace::{BatchReader, BatchReaderFactories},
        utils::Tup2,
        DBData, DynZWeight, RootCircuit, Stream, ZWeight,
    };
    use num::PrimInt;
    use std::{
        collections::{btree_map::Entry, BTreeMap},
        ops::AddAssign,
        sync::{Arc, Mutex},
    };

    // Checks that `aggregate_range` correctly computes aggregates for all
    // possible ranges in all partitions.
    fn test_partitioned_aggregate_range<PK, TS, A, C, S>(
        cursor: &mut C,
        contents: &BTreeMap<Box<DynData /* <PK> */>, BTreeMap<TS, Box<DynData /* <A> */>>>,
    ) where
        C: PartitionedRadixTreeCursor<DynData /* <PK> */, TS, DynData /* <A> */>,
        PK: DBData,
        TS: DBData + PrimInt,
        A: DBData,
        S: Semigroup<A>,
    {
        let empty = BTreeMap::new();

        while cursor.key_valid() {
            let partition_contents = contents.get(cursor.key()).unwrap_or(&empty);

            let mut partition_cursor = PartitionCursor::new(cursor);
            test_aggregate_range::<_, _, _, S>(&mut partition_cursor, partition_contents);

            cursor.step_key();
        }
    }

    fn update_key(
        input: &CollectionHandle<
            DynData, /* <u64> */
            DynPair<DynPair<DynDataTyped<u64>, DynData /* <u64> */>, DynZWeight>,
        >,
        contents: &mut BTreeMap<Box<DynData /* <u64> */>, BTreeMap<u64, Box<DynData /* <u64> */>>>,
        partition: u64,
        key: u64,
        upd: (u64, ZWeight),
    ) {
        input.dyn_push(
            partition.clone().erase_mut(),
            Tup2(Tup2(key, upd.0), upd.1).erase_mut(),
        );

        match contents
            .entry(Box::new(partition).erase_box())
            .or_default()
            .entry(key)
        {
            Entry::Vacant(ve) => {
                assert_eq!(upd.1, 1);
                ve.insert(Box::new(upd.0).erase_box());
            }
            Entry::Occupied(mut oe) => {
                assert!(upd.1 == 1 || upd.1 == -1);
                if upd.1 == 1 {
                    *oe.get_mut().downcast_mut_checked::<u64>() += upd.0;
                } else {
                    *oe.get_mut().downcast_mut_checked::<u64>() -= upd.0;
                }
                if *oe.get().downcast_checked::<u64>() == 0 {
                    oe.remove();
                }
            }
        }
    }

    #[test]
    fn test_partitioned_tree_aggregate() {
        let contents = Arc::new(Mutex::new(BTreeMap::new()));
        let contents_clone = contents.clone();

        let (circuit, input) = RootCircuit::build(move |circuit| {
            let (input, input_handle) = circuit
                .dyn_add_input_indexed_zset::<DynData/*<u64>*/, DynPair<DynDataTyped<u64>, DynData/*<u64>*/>>(
                    &AddInputIndexedZSetFactories::new::<u64, Tup2<u64, u64>>(),
                );

            let aggregator = <Fold<_, _, DefaultSemigroup<_>, _, _>>::new(
                0u64,
                |agg: &mut u64, val: &u64, _w: ZWeight| *agg += val,
            );

            let aggregate: Stream<_, OrdPartitionedRadixTree<DynData/*<u64>*/, u64, DynData/*<u64>*/>> =
                input.partitioned_tree_aggregate::<u64, DynData/*<u64>*/, DynData/*<u64>*/, DynData/*<u64>*/>(
                    &OrdPartitionedTreeAggregateFactories::new::<u64, u64, u64>(),
                    &DynAggregatorImpl::new(aggregator),
                );

            aggregate
                .dyn_integrate_trace(&BatchReaderFactories::new::<u64, Tup2<Prefix<u64>, TreeNode<u64, u64>>, ZWeight>())
                .apply(move |tree_trace| {
                    println!("Radix trees:");
                    let mut treestr = String::new();
                    tree_trace.cursor().format_tree(&mut treestr).unwrap();
                    println!("{treestr}");
                    tree_trace
                        .cursor()
                        .validate(&contents_clone.lock().unwrap(), &|acc, val| {
                            acc.downcast_mut_checked::<u64>().add_assign(val.downcast_checked::<u64>())
                        });
                    test_partitioned_aggregate_range::<u64, u64, u64, _, DefaultSemigroup<_>>(
                        &mut tree_trace.cursor(),
                        &contents_clone.lock().unwrap(),
                    );
                });

            Ok(input_handle)
        })
        .unwrap();

        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0x1000_0000_0000_0001,
            (1, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            1,
            0x1000_0000_0000_0001,
            (1, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            1,
            0x0000_0000_0000_0001,
            (2, 1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0x1000_0000_0000_0002,
            (2, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            1,
            0x0000_f000_0000_0002,
            (3, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            1,
            0x0000_0000_0000_0001,
            (2, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0x1000_1000_0000_0000,
            (3, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            2,
            0x0000_0000_f000_0000,
            (1, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            2,
            0x0000_0000_f100_0000,
            (2, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            2,
            0x0000_0000_f200_0000,
            (3, 1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0x1000_0000_0000_0002,
            (2, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf100_0000_0000_0001,
            (4, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf200_0000_0000_0001,
            (5, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_0000_0000_0001,
            (6, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_1000_0000_0001,
            (7, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_1000_1000_0001,
            (8, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_1000_1000_1001,
            (9, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_1000_1100_1001,
            (10, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_1000_1100_1001,
            (10, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            2,
            0x0000_0000_f200_0000,
            (4, 1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf400_1000_1100_1001,
            (11, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_1000_0000_0001,
            (7, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0x1000_0000_0000_0001,
            (1, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0x1000_1000_0000_0000,
            (3, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf100_0000_0000_0001,
            (4, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf200_0000_0000_0001,
            (5, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            2,
            0x0000_0000_f200_0000,
            (4, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            2,
            0x0000_0000_f0f0_0000,
            (5, 1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_0000_0000_0001,
            (6, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_1000_1000_0001,
            (8, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_1000_1000_1001,
            (9, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf400_1000_1100_1001,
            (11, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf100_0000_0000_0001,
            (4, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf200_0000_0000_0001,
            (5, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_0000_0000_0001,
            (6, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_1000_0000_0001,
            (7, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_1000_1000_0001,
            (8, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0,
            0xf300_1000_1000_0001,
            (11, 1),
        );
        circuit.step().unwrap();
    }
}
