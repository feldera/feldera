use crate::{
    algebra::{IndexedZSet, IndexedZSetReader, OrdIndexedZSet, OrdIndexedZSetFactories, OrdZSet},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Scope,
    },
    declare_trait_object,
    dynamic::{Data, DataTrait, DynDataTyped, DynPair, Erase},
    trace::{Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, Spine},
    utils::Tup2,
    Circuit, DBData, NumEntries, RootCircuit, Stream, ZWeight,
};
use rkyv::Archive;
use serde::{Deserialize, Serialize};
use size_of::SizeOf;
use std::{borrow::Cow, marker::PhantomData};

/// A contiguous range of rows preceding and following a fixed "anchor"
/// point.
///
/// A neighborhood is a contiguous range of rows preceding and following
/// a fixed "anchor" point.  The anchor may represent the first row in
/// the on-screen grid.  Rows are numbered, with number 0 representing
/// the anchor, negative numbers representing rows preceding the anchor,
/// and positive numbers rows following the anchor.
///
/// Each row contains a key/value pair.  Rows are ordered by keys and,
/// for each key, by values.
///
/// Similar to other DBSP collections, the [`DynNeighborhood`] type can represent
/// a complete neighborhood or a delta that must be applied to the previous
/// state of the neighborhood to obtain a new complete snapshot.  A complete
/// neighborhood always contains a contiguous set of indexes, as in the
/// following example:
///
/// ```text
///             │  #  │ key │ val │ w
///             ├─────┼─────┼─────┼───
///             │ -3  │ "a" │ 100 │ 1
///             │ -2  │ "a" │ 101 │ 1
///             │ -1  │ "b" │   5 │ 5
/// anchor ───► │  0  │ "b" │   7 │ 1
///             │  1  │ "b" │  10 │ 1
///             │  2  │ "c" │ 324 │ 1
///             │  3  │ "d" │   0 │ 1
///             │  4  │ "e" │  11 │ 2
///             │  5  │ "e" │  12 │ 1
/// ```
///
/// A delta on the other hand only contains affected indexes.  The following
/// delta, for instance, inserts the key/value pair `("a", 102)` in position
/// `-2` in the neighborhood, shifting `("a", 101)` to position `-3`
/// and pushing `("a", 100)` out of the neighborhood:
///
/// ```text
///             │  #  │ key │ val │ w
///             ├─────┼─────┼─────┼───
///             │ -3  │ "a" │ 100 │ -1 // ("a", 100) pushed out of the neighborhood
///             │ -3  │ "a" │ 101 │ +1 // ("a", 101) pushed from position -2 to -3
///             │ -2  │ "a" │ 101 │ -1
///             │ -2  │ "a" │ 102 │ +1 // ("a", 102) in inserted in positon -2.
/// ```
pub type DynNeighborhood<K, V> = OrdZSet<DynPair<DynDataTyped<i64>, DynPair<K, V>>>;

/// Neighborhood descriptor represents a request from the user to
/// output a specific neighborhood.
///
/// The neighborhood is defined in terms of its central point
/// (`anchor`) and the number of rows preceding and following the
/// anchor to output.
///
/// The `anchor` value of `None` is equivalent to specifying the
/// smallest value of type `K`.
#[derive(
    Clone,
    Default,
    Debug,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Hash,
    SizeOf,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(
    bound(
        archive = "<K as Archive>::Archived: Eq + Ord, <Option<K> as Archive>::Archived: Eq + Ord"
    ),
    compare(PartialEq, PartialOrd)
)]
pub struct NeighborhoodDescr<K: DBData, V: DBData> {
    pub anchor: Option<K>,
    #[serde(default)]
    pub anchor_val: V,
    pub before: u64,
    pub after: u64,
}

impl<K: DBData, V: DBData> NeighborhoodDescr<K, V> {
    pub fn new(anchor: Option<K>, anchor_val: V, before: u64, after: u64) -> Self {
        Self {
            anchor,
            anchor_val,
            before,
            after,
        }
    }
}

impl<K: DBData, V: DBData> NumEntries for NeighborhoodDescr<K, V> {
    const CONST_NUM_ENTRIES: Option<usize> = Some(1);

    fn num_entries_shallow(&self) -> usize {
        1
    }

    fn num_entries_deep(&self) -> usize {
        1
    }
}

pub trait NeighborhoodDescrTrait<K: DataTrait + ?Sized, V: DataTrait + ?Sized>: Data {
    fn anchor(&self) -> Option<&K>;
    fn anchor_val(&self) -> &V;
    fn before(&self) -> u64;
    fn after(&self) -> u64;

    #[allow(clippy::wrong_self_convention)]
    fn from_refs(&mut self, anchor: Option<&K>, anchor_val: &V, before: u64, after: u64);
}

impl<K, V, KType, VType> NeighborhoodDescrTrait<K, V> for NeighborhoodDescr<KType, VType>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    KType: DBData + Erase<K>,
    VType: DBData + Erase<V>,
{
    fn anchor(&self) -> Option<&K> {
        self.anchor.as_ref().map(Erase::erase)
    }

    fn anchor_val(&self) -> &V {
        self.anchor_val.erase()
    }

    fn before(&self) -> u64 {
        self.before
    }

    fn after(&self) -> u64 {
        self.after
    }

    fn from_refs(&mut self, anchor: Option<&K>, anchor_val: &V, before: u64, after: u64) {
        self.anchor = anchor.map(|a| unsafe { a.downcast::<KType>().clone() });
        self.anchor_val = unsafe { anchor_val.downcast::<VType>().clone() };
        self.before = before;
        self.after = after;
    }
}

declare_trait_object!(DynNeighborhoodDescr<KTrait, VTrait> = dyn NeighborhoodDescrTrait<KTrait, VTrait>
where
    KTrait: DataTrait + ?Sized,
    VTrait: DataTrait + ?Sized,
);

/// Stream of neighborhoods output by the [`Stream::neighborhood`] operator.
pub type NeighborhoodStream<K, V> = Stream<RootCircuit, DynNeighborhood<K, V>>;

/// Stream of neighborhood descriptors supplied as input to the
/// [`Stream::neighborhood`] operator.
pub type NeighborhoodDescrStream<K, V> =
    Stream<RootCircuit, Option<Box<DynNeighborhoodDescr<K, V>>>>;

pub struct NeighborhoodFactories<B: IndexedZSetReader> {
    input_factories: B::Factories,
    local_factories: OrdIndexedZSetFactories<B::Key, B::Val>,
    output_factories: <DynNeighborhood<B::Key, B::Val> as BatchReader>::Factories,
}

impl<B> NeighborhoodFactories<B>
where
    B: IndexedZSetReader,
{
    pub fn new<KType, VType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<B::Val>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            local_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            output_factories: BatchReaderFactories::new::<Tup2<i64, Tup2<KType, VType>>, (), ZWeight>(
            ),
        }
    }
}

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet,
{
    /// Returns a small contiguous range of rows ([`DynNeighborhood`]) of the input
    /// table.
    ///
    /// This operator helps to visualize the contents of the input table in a
    /// UI.  The UI client may not have enough throughput/memory to store the
    /// entire table, and will instead limit its state to a small range of
    /// rows that fit on the screen.  We specify such a range, or
    /// _neighborhood_, in terms of its center (or "anchor"), and the number
    /// of rows preceding and following the anchor (see
    /// [`NeighborhoodDescr`]).  The user may be interested in a static
    /// snapshot of the neighborhood or in a changing view.  Both modes are
    /// supported by this operator (see the `reset` argument).  The output of
    /// the operator is a stream of [`DynNeighborhood`]s.
    ///
    /// NOTE: This operator assumes that the integral of the input stream does
    /// not contain negative weights (which should normally be the case) and
    /// may produce incorrect outputs otherwise.
    ///
    /// # Arguments
    ///
    /// * `self` - a stream of changes to an indexed Z-set.
    ///
    /// * `neighborhood_descr` - contains the neighborhood descriptor to
    ///   evaluate at every clock tick.  Set to `None` to disable the operator
    ///   (it will output an empty neighborhood).
    ///
    /// # Output
    ///
    /// Outputs a stream of changes to the neighborhood.
    ///
    /// The output neighborhood will contain rows with indexes between
    /// `-descr.before` and `descr.after - 1`.  Row 0 is the anchor row, i.e.,
    /// is the first row in the input stream greater than or equal to
    /// `descr.anchor`.  If there is no such row (i.e., all rows in the input
    /// stream are smaller than the anchor), then the neighborhood will only
    /// contain negative indexes.
    ///
    /// The first index in the neighborhood may be greater
    /// than `-descr.before` if the input stream doesn't contain enough rows
    /// preceding the specified anchor.  The last index may be smaller than
    /// `descr.after - 1` if the input stream doesn't contain `descr.after`
    /// rows following the anchor point.
    pub fn dyn_neighborhood(
        &self,
        factories: &NeighborhoodFactories<B>,
        neighborhood_descr: &NeighborhoodDescrStream<B::Key, B::Val>,
    ) -> NeighborhoodStream<B::Key, B::Val> {
        self.circuit().region("neighborhood", || {
            // Compute local neighborhood in each worker.  We don't shard
            // the input stream, which means that multiple workers can
            // contain the same key in their neighborhood.  This shouldn't
            // affect correctness assuming that the integral of the input
            // stream does not contain negative weights (which should normally
            // be the case) and so identical key/value pairs in different
            // workers won't cancel out.
            let stream = self.try_sharded_version();
            let local_output = self
                .circuit()
                .add_binary_operator(
                    NeighborhoodLocal::new(&factories.local_factories),
                    &stream.dyn_integrate_trace(&factories.input_factories),
                    neighborhood_descr,
                )
                .differentiate_with_zero(Batch::dyn_empty(&factories.local_factories, ()));

            // Gather all results in worker 0.  Worker 0 then computes
            // the final neighborhood.
            // TODO: use different workers for different collections.
            let output = self.circuit().add_binary_operator(
                NeighborhoodNumbered::<Spine<OrdIndexedZSet<B::Key, B::Val>>>::new(
                    &factories.output_factories,
                ),
                &local_output
                    .dyn_gather(&factories.local_factories, 0)
                    .dyn_integrate_trace(&factories.local_factories),
                neighborhood_descr,
            );

            output.differentiate_with_zero(DynNeighborhood::dyn_empty(
                &factories.output_factories,
                (),
            ))
        })
    }
}

/// Computes the neighborhood without row numbers.
///
/// Used for per-worker neighborhood computation in each worker, where
/// row numbers are not needed (since row numbers are assigned by
/// worker 0).
///
/// The operator takes a trace of the input table and produces
/// a complete neighborhood as its output.  The internal implementation
/// is non-incremental and will do the work proportional to the size
/// of the neighborhood even if the neighborhood has not
/// changed since the last clock cycle.  This is ok assuming small
/// neighborhoods.  We may want to switch to an incremental
/// implementation in the future.
struct NeighborhoodLocal<T>
where
    T: IndexedZSetReader,
{
    output_factories: OrdIndexedZSetFactories<T::Key, T::Val>,
    _phantom: PhantomData<T>,
}

impl<T> NeighborhoodLocal<T>
where
    T: IndexedZSetReader,
{
    fn new(output_factories: &OrdIndexedZSetFactories<T::Key, T::Val>) -> Self {
        Self {
            output_factories: output_factories.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> Operator for NeighborhoodLocal<T>
where
    T: IndexedZSetReader + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("NeighborhoodLocal")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T>
    BinaryOperator<
        T,
        Option<Box<DynNeighborhoodDescr<T::Key, T::Val>>>,
        OrdIndexedZSet<T::Key, T::Val>,
    > for NeighborhoodLocal<T>
where
    T: IndexedZSetReader,
{
    fn eval<'a>(
        &mut self,
        input_trace: &T,
        descr: &Option<Box<DynNeighborhoodDescr<T::Key, T::Val>>>,
    ) -> OrdIndexedZSet<T::Key, T::Val> {
        let mut cursor = input_trace.cursor();

        let mut item = self.output_factories.weighted_item_factory().default_box();

        if let Some(descr) = descr {
            let anchor_key = descr.anchor();
            let anchor_val = descr.anchor_val();

            // Forward pass: locate the anchor and `decr.after`
            // following rows.
            let mut after = self.output_factories.weighted_items_factory().default_box();
            after.reserve((descr.after() + 1) as usize);

            let mut offset = 0;

            if let Some(anchor_key) = anchor_key {
                cursor.seek_keyval(anchor_key, anchor_val);
            };
            while cursor.keyval_valid() && offset <= descr.after() {
                let w = **cursor.weight();

                if !cursor.weight().is_zero() {
                    let (kv, weight) = item.split_mut();
                    kv.from_refs(cursor.key(), cursor.val());
                    **weight = w;

                    after.push_val(item.as_mut());
                    offset += 1;
                }
                cursor.step_keyval();
            }

            // Reverse pass: find `descr.before` rows preceding the anchor.
            cursor.fast_forward_keys();
            cursor.fast_forward_vals();

            let mut before = self.output_factories.weighted_items_factory().default_box();
            before.reserve(descr.before() as usize);

            offset = 1;

            if let Some(anchor_key) = anchor_key {
                cursor.seek_keyval_reverse(anchor_key, anchor_val);
                if cursor.keyval_valid() && cursor.keyval() == (anchor_key, anchor_val) {
                    cursor.step_keyval_reverse();
                }

                while cursor.keyval_valid() && offset <= descr.before() {
                    let w = **cursor.weight();

                    if !cursor.weight().is_zero() {
                        let (kv, weight) = item.split_mut();
                        kv.from_refs(cursor.key(), cursor.val());
                        **weight = w;

                        before.push_val(item.as_mut());
                        offset += 1;
                    }
                    cursor.step_keyval_reverse();
                }
            }

            // Assemble final result.
            let mut builder = <<OrdIndexedZSet<_, _> as Batch>::Builder>::with_capacity(
                &self.output_factories,
                (),
                before.len() + after.len(),
            );
            for update in before.dyn_iter_mut().rev() {
                builder.push(update);
            }
            for update in after.dyn_iter_mut() {
                builder.push(update);
            }

            builder.done()
        } else {
            Batch::dyn_empty(&self.output_factories, ())
        }
    }
}

/// Computes the neighborhood including row numbers.
///
/// Used to compute the final output of the [`Stream::neighborhood`]
/// operator in worker-0.
struct NeighborhoodNumbered<T>
where
    T: IndexedZSetReader,
{
    output_factories: <DynNeighborhood<T::Key, T::Val> as BatchReader>::Factories,
    _phantom: PhantomData<T>,
}

impl<T> NeighborhoodNumbered<T>
where
    T: IndexedZSetReader,
{
    fn new(output_factories: &<DynNeighborhood<T::Key, T::Val> as BatchReader>::Factories) -> Self {
        Self {
            output_factories: output_factories.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> Operator for NeighborhoodNumbered<T>
where
    T: IndexedZSetReader + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("NeighborhoodNumbered")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T>
    BinaryOperator<
        T,
        Option<Box<DynNeighborhoodDescr<T::Key, T::Val>>>,
        DynNeighborhood<T::Key, T::Val>,
    > for NeighborhoodNumbered<T>
where
    T: IndexedZSetReader + Clone,
{
    fn eval<'a>(
        &mut self,
        input_trace: &T,
        descr: &Option<Box<DynNeighborhoodDescr<T::Key, T::Val>>>,
    ) -> DynNeighborhood<T::Key, T::Val> {
        let mut cursor = input_trace.cursor();

        let mut item = self.output_factories.weighted_item_factory().default_box();

        if let Some(descr) = &descr {
            let anchor_key = descr.anchor();
            let anchor_val = descr.anchor_val();

            let mut after = self.output_factories.weighted_items_factory().default_box();
            after.reserve((descr.after() + 1) as usize);

            let mut offset = 0;

            if let Some(anchor_key) = anchor_key {
                cursor.seek_keyval(anchor_key, anchor_val);
            }
            while cursor.keyval_valid() && offset <= descr.after() {
                let w = **cursor.weight();

                if !cursor.weight().is_zero() {
                    let (kv, weight) = item.split_mut();
                    let (k, _unit) = kv.split_mut();
                    let (idx, vals) = k.split_mut();

                    **idx = offset as i64;
                    vals.from_refs(cursor.key(), cursor.val());
                    **weight = w;

                    after.push_val(item.as_mut());
                    offset += 1;
                }
                cursor.step_keyval();
            }

            cursor.fast_forward_keys();
            cursor.fast_forward_vals();

            let mut before = self.output_factories.weighted_items_factory().default_box();
            before.reserve(descr.before() as usize);

            offset = 1;

            if let Some(anchor_key) = anchor_key {
                cursor.seek_keyval_reverse(anchor_key, anchor_val);
                if cursor.keyval_valid() && cursor.keyval() == (anchor_key, anchor_val) {
                    cursor.step_keyval_reverse();
                }

                while cursor.keyval_valid() && offset <= descr.before() {
                    let w = **cursor.weight();

                    if !cursor.weight().is_zero() {
                        let (kv, weight) = item.split_mut();
                        let (k, _unit) = kv.split_mut();
                        let (idx, vals) = k.split_mut();

                        **idx = -(offset as i64);
                        vals.from_refs(cursor.key(), cursor.val());
                        **weight = w;

                        before.push_val(item.as_mut());
                        offset += 1;
                    }
                    cursor.step_keyval_reverse();
                }
            }

            let mut builder = <<DynNeighborhood<T::Key, T::Val> as Batch>::Builder>::with_capacity(
                &self.output_factories,
                (),
                before.len() + after.len(),
            );
            for update in before.dyn_iter_mut().rev() {
                builder.push(update);
            }
            for update in after.dyn_iter_mut() {
                builder.push(update);
            }

            builder.done()
        } else {
            Batch::dyn_empty(&self.output_factories, ())
        }
    }
}

#[cfg(test)]
#[allow(clippy::type_complexity)]
mod test {
    use crate::{
        dynamic::{DowncastTrait, DynData, Erase},
        operator::{
            IndexedZSetHandle, InputHandle, NeighborhoodDescr, NeighborhoodDescrBox, OutputHandle,
        },
        trace::{
            test::test_batch::{
                assert_batch_eq, batch_to_tuples, typed_batch_to_tuples, TestBatch,
                TestBatchFactories,
            },
            BatchReaderFactories, Trace,
        },
        typed_batch::{BatchReader, DynOrdIndexedZSet, OrdIndexedZSet, TypedBox},
        utils::Tup2,
        DBData, DynZWeight, RootCircuit, Runtime, Stream, ZWeight,
    };
    use anyhow::Result as AnyResult;
    use proptest::{collection::vec, prelude::*};
    use std::cmp::{max, min};

    impl TestBatch<DynData, DynData, (), DynZWeight> {
        fn neighborhood<K, V>(
            &self,
            descr: &Option<NeighborhoodDescr<K, V>>,
        ) -> TestBatch<DynData /* <i64> */, DynData /* <(K, V)> */, (), DynZWeight>
        where
            K: DBData,
            V: DBData,
        {
            if let Some(descr) = &descr {
                let descr = descr;
                let anchor_k = &descr.anchor;
                let anchor_v = &descr.anchor_val;

                let tuples = batch_to_tuples(self);
                let start = if let Some(anchor_k) = anchor_k {
                    tuples
                        .iter()
                        .position(|((k, v, ()), _w)| {
                            (k.downcast_checked(), v.downcast_checked()) >= (anchor_k, anchor_v)
                        })
                        .unwrap_or(tuples.len()) as i64
                } else {
                    0
                };

                let mut from = start - descr.before as i64;
                let mut to = start + descr.after as i64 + 1;

                from = max(from, 0);
                to = min(to, tuples.len() as i64);

                let output = tuples[from as usize..to as usize]
                    .iter()
                    .enumerate()
                    .map(|(i, ((k, v, ()), w))| {
                        (
                            (
                                Box::new(i as i64 - (start - from)).erase_box(),
                                Box::new(Tup2(
                                    k.downcast_checked::<K>().clone(),
                                    v.downcast_checked::<V>().clone(),
                                ))
                                .erase_box(),
                                (),
                            ),
                            w.clone(),
                        )
                    })
                    .collect::<Vec<_>>();

                TestBatch::from_data(output.as_slice())
            } else {
                TestBatch::new(&TestBatchFactories::new(), "")
            }
        }
    }

    fn test_circuit(
        circuit: &mut RootCircuit,
    ) -> AnyResult<(
        InputHandle<Option<NeighborhoodDescrBox<i32, i32>>>,
        IndexedZSetHandle<i32, i32>,
        OutputHandle<OrdIndexedZSet<i64, Tup2<i32, i32>>>,
    )> {
        let (descr_stream, descr_handle) =
            circuit.add_input_stream::<Option<NeighborhoodDescrBox<i32, i32>>>();
        let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32>();

        let range: Stream<_, DynOrdIndexedZSet<DynData, DynData>> = input_stream
            .neighborhood(&descr_stream)
            .integrate()
            .inner()
            .dyn_map_index(
                &BatchReaderFactories::new::<i64, Tup2<i32, i32>, ZWeight>(),
                Box::new(|kv, out| out.from_refs(kv.fst().as_data(), kv.snd().as_data())),
            );

        let range_handle = range.typed().output();

        Ok((descr_handle, input_handle, range_handle))
    }

    #[test]
    fn neighborhood_test() {
        let (mut dbsp, (descr_handle, input_handle, output_handle)) =
            Runtime::init_circuit(4, test_circuit).unwrap();

        // Empty collection.
        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(
            Some(10),
            10,
            3,
            5,
        ))));

        dbsp.step().unwrap();

        assert!(typed_batch_to_tuples(&output_handle.consolidate()).is_empty());

        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(None, 10, 3, 5))));

        dbsp.step().unwrap();

        assert!(typed_batch_to_tuples(&output_handle.consolidate()).is_empty());

        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(
            Some(10),
            10,
            3,
            5,
        ))));
        input_handle.push(9, (0, 1));

        dbsp.step().unwrap();

        assert_eq!(
            &typed_batch_to_tuples(&output_handle.consolidate()),
            &[((-1, Tup2(9, 0), ()), 1)]
        );

        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(None, 10, 3, 5))));
        dbsp.step().unwrap();
        assert_eq!(
            &typed_batch_to_tuples(&output_handle.consolidate()),
            &[((0, Tup2(9, 0), ()), 1)]
        );

        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(
            Some(10),
            10,
            3,
            5,
        ))));
        input_handle.push(9, (1, 1));

        dbsp.step().unwrap();

        assert_eq!(
            &typed_batch_to_tuples(&output_handle.consolidate()),
            &[((-2, Tup2(9, 0), ()), 1), ((-1, Tup2(9, 1), ()), 1)]
        );

        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(
            Some(10),
            10,
            3,
            5,
        ))));
        input_handle.push(8, (1, 1));

        dbsp.step().unwrap();

        assert_eq!(
            &typed_batch_to_tuples(&output_handle.consolidate()),
            &[
                ((-3, Tup2(8, 1), ()), 1),
                ((-2, Tup2(9, 0), ()), 1),
                ((-1, Tup2(9, 1), ()), 1)
            ]
        );

        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(
            Some(10),
            10,
            3,
            5,
        ))));
        input_handle.push(7, (1, 1));

        dbsp.step().unwrap();

        assert_eq!(
            &typed_batch_to_tuples(&output_handle.consolidate()),
            &[
                ((-3, Tup2(8, 1), ()), 1),
                ((-2, Tup2(9, 0), ()), 1),
                ((-1, Tup2(9, 1), ()), 1)
            ]
        );

        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(
            Some(10),
            10,
            3,
            5,
        ))));
        input_handle.push(10, (10, 1));

        dbsp.step().unwrap();

        assert_eq!(
            &typed_batch_to_tuples(&output_handle.consolidate()),
            &[
                ((-3, Tup2(8, 1), ()), 1),
                ((-2, Tup2(9, 0), ()), 1),
                ((-1, Tup2(9, 1), ()), 1),
                ((0, Tup2(10, 10), ()), 1)
            ]
        );

        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(
            Some(10),
            10,
            3,
            5,
        ))));
        input_handle.push(10, (11, 1));
        input_handle.push(12, (0, 1));

        dbsp.step().unwrap();

        assert_eq!(
            &typed_batch_to_tuples(&output_handle.consolidate()),
            &[
                ((-3, Tup2(8, 1), ()), 1),
                ((-2, Tup2(9, 0), ()), 1),
                ((-1, Tup2(9, 1), ()), 1),
                ((0, Tup2(10, 10), ()), 1),
                ((1, Tup2(10, 11), ()), 1),
                ((2, Tup2(12, 0), ()), 1)
            ]
        );

        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(
            Some(10),
            10,
            3,
            5,
        ))));
        input_handle.push(10, (10, -1));

        dbsp.step().unwrap();

        assert_eq!(
            &typed_batch_to_tuples(&output_handle.consolidate()),
            &[
                ((-3, Tup2(8, 1), ()), 1),
                ((-2, Tup2(9, 0), ()), 1),
                ((-1, Tup2(9, 1), ()), 1),
                ((0, Tup2(10, 11), ()), 1),
                ((1, Tup2(12, 0), ()), 1)
            ]
        );

        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(
            Some(10),
            10,
            3,
            5,
        ))));
        input_handle.push(13, (0, 1));
        input_handle.push(14, (0, 1));
        input_handle.push(14, (1, 1));
        input_handle.push(14, (2, 1));
        input_handle.push(14, (3, 1));

        dbsp.step().unwrap();

        assert_eq!(
            &typed_batch_to_tuples(&output_handle.consolidate()),
            &[
                ((-3, Tup2(8, 1), ()), 1),
                ((-2, Tup2(9, 0), ()), 1),
                ((-1, Tup2(9, 1), ()), 1),
                ((0, Tup2(10, 11), ()), 1),
                ((1, Tup2(12, 0), ()), 1),
                ((2, Tup2(13, 0), ()), 1),
                ((3, Tup2(14, 0), ()), 1),
                ((4, Tup2(14, 1), ()), 1),
                ((5, Tup2(14, 2), ()), 1)
            ]
        );

        descr_handle.set_for_all(Some(TypedBox::new(NeighborhoodDescr::new(None, 10, 3, 5))));

        dbsp.step().unwrap();

        assert_eq!(
            &typed_batch_to_tuples(&output_handle.consolidate()),
            &[
                ((0, Tup2(7, 1), ()), 1),
                ((1, Tup2(8, 1), ()), 1),
                ((2, Tup2(9, 0), ()), 1),
                ((3, Tup2(9, 1), ()), 1),
                ((4, Tup2(10, 11), ()), 1),
                ((5, Tup2(12, 0), ()), 1),
            ]
        );
    }

    fn input_trace(
        max_key: i32,
        max_val: i32,
        max_batch_size: usize,
        max_batches: usize,
    ) -> impl Strategy<Value = Vec<(Vec<(i32, i32, ZWeight)>, (i32, i32), u64, u64)>> {
        vec(
            (
                vec((0..max_key, 0..max_val, 1..2i64), 0..max_batch_size),
                (0..max_key, 0..max_val),
                (0..(max_key * max_val) as u64),
                (0..(max_key * max_val) as u64),
            ),
            0..max_batches,
        )
    }

    proptest! {
        #[test]
        fn neighborhood_proptest(trace in input_trace(100, 5, 200, 20)) {

            let (mut dbsp, (descr_handle, input_handle, output_handle)) =
                Runtime::init_circuit(4, test_circuit).unwrap();

            let mut ref_trace = TestBatch::new(&TestBatchFactories::new(), "");

            for (batch, (start_key, start_val), before, after) in trace.into_iter() {

                let records = batch.iter().map(|(k, v, r)| ((*k, *v, ()), *r)).collect::<Vec<_>>();

                let ref_batch = TestBatch::from_typed_data(&records);
                ref_trace.insert(ref_batch);

                for (k, v, r) in batch.into_iter() {
                    input_handle.push(k, (v, r));
                }
                let descr = NeighborhoodDescr::new(Some(start_key), start_val, before, after);
                descr_handle.set_for_all(Some(TypedBox::new(descr.clone())));

                dbsp.step().unwrap();

                let output = output_handle.consolidate();
                let ref_output = ref_trace.neighborhood(&Some(descr));

                assert_batch_eq(output.inner(), &ref_output);

                let descr = NeighborhoodDescr::new(None, start_val, before, after);
                descr_handle.set_for_all(Some(TypedBox::new(descr.clone())));

                dbsp.step().unwrap();

                let output = output_handle.consolidate();
                let ref_output = ref_trace.neighborhood(&Some(descr));

                assert_batch_eq(output.inner(), &ref_output);
            }
        }
    }
}
