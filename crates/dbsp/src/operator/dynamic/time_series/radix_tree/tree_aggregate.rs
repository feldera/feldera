use super::{radix_tree_update, DynTreeNode, Prefix, RadixTreeFactories};
use crate::{
    algebra::{HasOne, IndexedZSet, IndexedZSetReader, OrdIndexedZSet},
    circuit::{
        operator_traits::{Operator, TernaryOperator},
        Scope,
    },
    dynamic::{DataTrait, DynDataTyped, Erase},
    operator::dynamic::{
        aggregate::DynAggregator,
        time_series::radix_tree::treenode::TreeNode,
        trace::{TraceBounds, TraceFeedback},
    },
    trace::{Batch, BatchReader, BatchReaderFactories, Builder, Spine},
    Circuit, DBData, DynZWeight, Stream, ZWeight,
};
use dyn_clone::clone_box;
use num::PrimInt;
use size_of::SizeOf;
use std::{borrow::Cow, cmp::Ordering, marker::PhantomData, ops::Neg};

/// A batch that contains updates to a radix tree.
pub trait RadixTreeBatch<TS, A>:
    IndexedZSet<Key = DynDataTyped<Prefix<TS>>, Val = DynTreeNode<TS, A>>
where
    TS: DBData + PrimInt,
    A: DataTrait + ?Sized,
{
}

impl<TS, A, B> RadixTreeBatch<TS, A> for B
where
    TS: DBData + PrimInt,
    A: DataTrait + ?Sized,
    B: IndexedZSet<Key = DynDataTyped<Prefix<TS>>, Val = DynTreeNode<TS, A>>,
{
}

pub trait RadixTreeReader<TS, A>:
    IndexedZSetReader<Key = DynDataTyped<Prefix<TS>>, Val = DynTreeNode<TS, A>>
where
    TS: DBData + PrimInt,
    A: DataTrait + ?Sized,
{
}

impl<TS, A, B> RadixTreeReader<TS, A> for B
where
    B: IndexedZSetReader<Key = DynDataTyped<Prefix<TS>>, Val = DynTreeNode<TS, A>>,
    TS: DBData + PrimInt,
    A: DataTrait + ?Sized,
{
}

pub type OrdRadixTree<TS, A> = OrdIndexedZSet<DynDataTyped<Prefix<TS>>, DynTreeNode<TS, A>>;

pub struct TreeAggregateFactories<
    TS: DBData + PrimInt,
    Z: IndexedZSet<Key = DynDataTyped<TS>>,
    O: RadixTreeBatch<TS, Acc>,
    Acc: DataTrait + ?Sized,
> {
    input_factories: Z::Factories,
    output_factories: O::Factories,
    radix_tree_factories: RadixTreeFactories<TS, Acc>,
}

impl<TS, Z, O, Acc> TreeAggregateFactories<TS, Z, O, Acc>
where
    TS: DBData + PrimInt,
    Z: IndexedZSet<Key = DynDataTyped<TS>>,
    O: RadixTreeBatch<TS, Acc>,
    Acc: DataTrait + ?Sized,
{
    pub fn new<VType, AType>() -> Self
    where
        VType: DBData + Erase<Z::Val>,
        AType: DBData + Erase<Acc>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<TS, VType, ZWeight>(),
            output_factories: BatchReaderFactories::new::<Prefix<TS>, TreeNode<TS, AType>, ZWeight>(
            ),
            radix_tree_factories: RadixTreeFactories::new::<AType>(),
        }
    }
}

impl<C, Z, TS> Stream<C, Z>
where
    C: Circuit,
    Z: IndexedZSet<Key = DynDataTyped<TS>> + SizeOf + Send,
    TS: DBData + PrimInt,
{
    /// Given a batch of updates to a time series stream, computes a stream of
    /// updates to its radix tree.
    ///
    /// This is intended as a building block for higher-level operators.
    ///
    /// # Limitations
    ///
    /// Unlike `Stream::partitioned_tree_aggregate()`, this operator is
    /// currently not parallelized, performing all work in a single worker
    /// thread.
    pub fn tree_aggregate<Acc, Out>(
        &self,
        factories: &TreeAggregateFactories<TS, Z, OrdRadixTree<TS, Acc>, Acc>,
        aggregator: &dyn DynAggregator<Z::Val, (), Z::R, Accumulator = Acc, Output = Out>,
    ) -> Stream<C, OrdRadixTree<TS, Acc>>
    where
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
    {
        self.tree_aggregate_generic::<Acc, Out, OrdRadixTree<TS, Acc>>(factories, aggregator)
    }

    /// Like [`Self::tree_aggregate`], but can return any batch type.
    pub fn tree_aggregate_generic<Acc, Out, O>(
        &self,
        factories: &TreeAggregateFactories<TS, Z, O, Acc>,
        aggregator: &dyn DynAggregator<Z::Val, (), DynZWeight, Accumulator = Acc, Output = Out>,
    ) -> Stream<C, O>
    where
        Acc: DataTrait + ?Sized,
        Out: DataTrait + ?Sized,
        O: RadixTreeBatch<TS, Acc>,
    {
        self.circuit().region("tree_aggregate", move || {
            let circuit = self.circuit();
            let stream = self.dyn_gather(&factories.input_factories, 0);

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
            //                                                            delayed_trace       └───────┘
            // ```

            let feedback = circuit.add_integrate_trace_feedback::<Spine<O>>(
                &factories.output_factories,
                <TraceBounds<O::Key, O::Val>>::unbounded(),
            );

            let output = circuit.add_ternary_operator(
                RadixTreeAggregate::new(
                    &factories.radix_tree_factories,
                    &factories.output_factories,
                    aggregator,
                ),
                &stream,
                &stream.dyn_integrate_trace(&factories.input_factories),
                &feedback.delayed_trace,
            );

            feedback.connect(&output);

            output
        })
    }
}

/// Ternary operator that implements the internals of `tree_aggregate`.
///
/// * Input stream 1: updates to the time series.  Only used to identify
///   affected times.
/// * Input stream 2: trace containing the accumulated time series data.
/// * Input stream 3: trace containing the current contents of the radix tree.
struct RadixTreeAggregate<Z, TS, IT, OT, Acc, Out, O>
where
    Z: BatchReader<Key = DynDataTyped<TS>>,
    TS: DBData + PrimInt,
    O: Batch,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
{
    aggregator: Box<dyn DynAggregator<Z::Val, (), Z::R, Accumulator = Acc, Output = Out>>,
    radix_tree_factories: RadixTreeFactories<TS, Acc>,
    output_factories: O::Factories,
    phantom: PhantomData<(Z, IT, OT, O)>,
}

impl<Z, TS, IT, OT, Acc, Out, O> RadixTreeAggregate<Z, TS, IT, OT, Acc, Out, O>
where
    Z: BatchReader<Key = DynDataTyped<TS>>,
    TS: DBData + PrimInt,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    O: Batch,
{
    pub fn new(
        radix_tree_factories: &RadixTreeFactories<TS, Acc>,
        output_factories: &O::Factories,
        aggregator: &dyn DynAggregator<Z::Val, (), Z::R, Accumulator = Acc, Output = Out>,
    ) -> Self {
        Self {
            radix_tree_factories: radix_tree_factories.clone(),
            output_factories: output_factories.clone(),
            aggregator: clone_box(aggregator),
            phantom: PhantomData,
        }
    }
}

impl<Z, TS, IT, OT, Acc, Out, O> Operator for RadixTreeAggregate<Z, TS, IT, OT, Acc, Out, O>
where
    Z: BatchReader<Key = DynDataTyped<TS>>,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    TS: DBData + PrimInt,
    IT: 'static,
    OT: 'static,
    O: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("RadixTreeAggregate")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z, TS, IT, OT, Acc, Out, O> TernaryOperator<Z, IT, OT, O>
    for RadixTreeAggregate<Z, TS, IT, OT, Acc, Out, O>
where
    Z: IndexedZSet<Key = DynDataTyped<TS>>,
    TS: DBData + PrimInt,
    Acc: DataTrait + ?Sized,
    Out: DataTrait + ?Sized,
    O: RadixTreeBatch<TS, Acc>,
    IT: IndexedZSetReader<Key = Z::Key, Val = Z::Val> + Clone,
    OT: RadixTreeReader<TS, Acc> + Clone,
{
    fn eval<'a>(
        &mut self,
        delta: Cow<'a, Z>,
        input_trace: Cow<'a, IT>,
        output_trace: Cow<'a, OT>,
    ) -> O {
        let mut updates = self.radix_tree_factories.node_updates_factory.default_box();
        updates.reserve(delta.key_count());

        radix_tree_update::<TS, Z::Val, Acc, Out, _, _, _>(
            &self.radix_tree_factories,
            delta.cursor(),
            input_trace.cursor(),
            output_trace.cursor(),
            self.aggregator.as_ref(),
            &mut *updates,
        );

        let mut builder = O::Builder::with_capacity(&self.output_factories, (), updates.len() * 2);

        // `updates` are already ordered by prefix.  All that remains is to order
        // insertion and deletion within each update.
        for update in updates.dyn_iter_mut() {
            match update.new().cmp(update.old()) {
                Ordering::Equal => {}
                Ordering::Less => {
                    let mut prefix = update.prefix();
                    if let Some(new) = update.new_mut().get_mut() {
                        builder.push_vals(
                            prefix.clone().erase_mut(),
                            new,
                            ZWeight::one().erase_mut(),
                        );
                    };
                    if let Some(old) = update.old_mut().get_mut() {
                        builder.push_vals(
                            prefix.erase_mut(),
                            old,
                            ZWeight::one().neg().erase_mut(),
                        );
                    };
                }
                Ordering::Greater => {
                    let mut prefix = update.prefix();

                    if let Some(old) = update.old_mut().get_mut() {
                        builder.push_vals(
                            prefix.clone().erase_mut(),
                            old,
                            ZWeight::one().neg().erase_mut(),
                        );
                    };
                    if let Some(new) = update.new_mut().get_mut() {
                        builder.push_vals(prefix.erase_mut(), new, ZWeight::one().erase_mut());
                    };
                }
            }
        }

        builder.done()
    }
}

#[cfg(test)]
mod test {
    use super::super::RadixTreeCursor;
    use crate::{
        algebra::DefaultSemigroup,
        dynamic::{DowncastTrait, DynData, DynDataTyped, DynPair, Erase},
        operator::{
            dynamic::{
                aggregate::DynAggregatorImpl,
                input::{AddInputIndexedZSetFactories, CollectionHandle},
                time_series::{
                    radix_tree::{
                        test::test_aggregate_range,
                        tree_aggregate::{OrdRadixTree, TreeAggregateFactories},
                        Prefix,
                    },
                    TreeNode,
                },
            },
            Fold,
        },
        trace::{BatchReader, BatchReaderFactories},
        utils::Tup2,
        DynZWeight, RootCircuit, Stream, ZWeight,
    };
    use std::{
        collections::{btree_map::Entry, BTreeMap},
        ops::AddAssign,
        sync::{Arc, Mutex},
    };

    fn update_key(
        input: &CollectionHandle<DynDataTyped<u64>, DynPair<DynData, DynZWeight>>,
        contents: &mut BTreeMap<u64, Box<DynData /* <u64> */>>,
        key: u64,
        upd: Tup2<u64, ZWeight>,
    ) {
        input.dyn_push(key.clone().erase_mut(), upd.clone().erase_mut());
        match contents.entry(key) {
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
    fn test_tree_aggregate() {
        let contents = Arc::new(Mutex::new(BTreeMap::new()));
        let contents_clone = contents.clone();

        let (circuit, input) = RootCircuit::build(move |circuit| {
            let (input, input_handle) =
                circuit.dyn_add_input_indexed_zset::<DynDataTyped<u64>, DynData/*u64*/>(&AddInputIndexedZSetFactories::new::<u64, u64>());

            let aggregator = <Fold<u64, _, DefaultSemigroup<_>, _, _>>::new(
                0u64,
                |agg: &mut u64, val: &u64, _w: ZWeight| *agg += val,
            );

            let aggregate: Stream<_, OrdRadixTree<u64, DynData /* <u64> */>> = input
                .tree_aggregate::<DynData/*<u64>*/, DynData/*<u64>*/>(
                    &TreeAggregateFactories::new::<u64, u64>(),
                    &DynAggregatorImpl::new(aggregator),
                );
            aggregate
                .dyn_integrate_trace(&BatchReaderFactories::new::<Prefix<u64>, TreeNode<u64, u64>, ZWeight>())
                .apply(move |tree_trace| {
                    println!("Radix tree:");
                    let mut treestr = String::new();
                    tree_trace.cursor().format_tree(&mut treestr).unwrap();
                    println!("{treestr}");
                    tree_trace
                        .cursor()
                        .validate(&contents_clone.lock().unwrap(), &|acc, val| {
                            acc.downcast_mut_checked::<u64>().add_assign(val.downcast_checked::<u64>())
                        });
                    test_aggregate_range::<u64, u64, _, DefaultSemigroup<_>>(
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
            0x1000_0000_0000_0001,
            Tup2(1, 1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0x1000_0000_0000_0002,
            Tup2(2, 1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0x1000_1000_0000_0000,
            Tup2(3, 1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0x1000_0000_0000_0002,
            Tup2(2, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf100_0000_0000_0001,
            Tup2(4, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf200_0000_0000_0001,
            Tup2(5, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_0000_0000_0001,
            Tup2(6, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_0000_0001,
            Tup2(7, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_0001,
            Tup2(8, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_1001,
            Tup2(9, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1100_1001,
            Tup2(10, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1100_1001,
            Tup2(10, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf400_1000_1100_1001,
            Tup2(11, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_0000_0001,
            Tup2(7, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0x1000_0000_0000_0001,
            Tup2(1, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0x1000_1000_0000_0000,
            Tup2(3, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf100_0000_0000_0001,
            Tup2(4, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf200_0000_0000_0001,
            Tup2(5, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_0000_0000_0001,
            Tup2(6, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_0001,
            Tup2(8, -1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_1001,
            Tup2(9, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf400_1000_1100_1001,
            Tup2(11, -1),
        );
        circuit.step().unwrap();

        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf100_0000_0000_0001,
            Tup2(4, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf200_0000_0000_0001,
            Tup2(5, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_0000_0000_0001,
            Tup2(6, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_0000_0001,
            Tup2(7, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_0001,
            Tup2(8, 1),
        );
        update_key(
            &input,
            &mut contents.lock().unwrap(),
            0xf300_1000_1000_0001,
            Tup2(11, 1),
        );
        circuit.step().unwrap();
    }
}
