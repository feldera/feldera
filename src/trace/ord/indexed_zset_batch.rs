use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero, MonoidValue, NegByRef},
    lattice::Lattice,
    trace::{
        layers::{
            column_leaf::{OrderedColumnLeaf, OrderedColumnLeafBuilder},
            ordered::{OrdOffset, OrderedBuilder, OrderedCursor, OrderedLayer},
            ordered_leaf::OrderedLeaf,
            Builder as TrieBuilder, Cursor as TrieCursor, MergeBuilder, Trie, TupleBuilder,
        },
        ord::merge_batcher::MergeBatcher,
        Batch, BatchReader, Builder, Cursor, Merger,
    },
    NumEntries,
};
use deepsize::DeepSizeOf;
use std::{
    cmp::max,
    convert::{TryFrom, TryInto},
    fmt::{self, Debug, Display},
    ops::{Add, AddAssign, Neg},
    rc::Rc,
};
use timely::progress::Antichain;

type Layers<K, V, R, O> = OrderedLayer<K, OrderedColumnLeaf<V, R>, O>;

/// An immutable collection of update tuples.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OrdIndexedZSet<K, V, R, O = usize>
where
    K: Ord,
    V: Ord,
    R: Clone,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    /// Where all the data is.
    pub layer: Layers<K, V, R, O>,
    pub lower: Antichain<()>,
    pub upper: Antichain<()>,
}

impl<K, V, R, O> Display for OrdIndexedZSet<K, V, R, O>
where
    K: Ord + Clone + Display,
    V: Ord + Clone + Display + 'static,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone + Display + 'static,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
    OrderedLayer<K, OrderedLeaf<V, R>, O>: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "lower: {:?}, upper: {:?}\nlayer:\n{}",
            self.lower,
            self.upper,
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

impl<K, V, R, O> Default for OrdIndexedZSet<K, V, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    #[inline]
    fn default() -> Self {
        Self::empty(())
    }
}

impl<K, V, R, O> From<Layers<K, V, R, O>> for OrdIndexedZSet<K, V, R, O>
where
    K: Ord,
    V: Ord,
    R: Clone,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    #[inline]
    fn from(layer: Layers<K, V, R, O>) -> Self {
        Self {
            layer,
            lower: Antichain::from_elem(()),
            upper: Antichain::new(),
        }
    }
}

impl<K, V, R, O> From<Layers<K, V, R, O>> for Rc<OrdIndexedZSet<K, V, R, O>>
where
    K: Ord,
    V: Ord,
    R: Clone,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    #[inline]
    fn from(layer: Layers<K, V, R, O>) -> Self {
        Rc::new(From::from(layer))
    }
}

impl<K, V, R, O> DeepSizeOf for OrdIndexedZSet<K, V, R, O>
where
    K: DeepSizeOf + Ord,
    V: DeepSizeOf + Ord,
    R: DeepSizeOf + Clone,
    O: DeepSizeOf + OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    #[inline]
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.layer.deep_size_of_children(context)
    }
}

impl<K, V, R, O> NumEntries for OrdIndexedZSet<K, V, R, O>
where
    K: Clone + Ord,
    V: Clone + Ord,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    const CONST_NUM_ENTRIES: Option<usize> =
        <OrderedLayer<K, OrderedLeaf<V, R>, O>>::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K, V, R, O> NegByRef for OrdIndexedZSet<K, V, R, O>
where
    K: Ord + Clone,
    V: Ord + Clone,
    R: MonoidValue + NegByRef,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    #[inline]
    fn neg_by_ref(&self) -> Self {
        Self {
            layer: self.layer.neg_by_ref(),
            lower: self.lower.clone(),
            upper: self.upper.clone(),
        }
    }
}

impl<K, V, R, O> Neg for OrdIndexedZSet<K, V, R, O>
where
    K: Ord + Clone,
    V: Ord + Clone,
    R: MonoidValue + Neg<Output = R>,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        Self {
            layer: self.layer.neg(),
            lower: self.lower,
            upper: self.upper,
        }
    }
}

// TODO: by-value merge
impl<K, V, R, O> Add<Self> for OrdIndexedZSet<K, V, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Output = Self;
    #[inline]

    fn add(self, rhs: Self) -> Self::Output {
        let lower = self.lower().meet(rhs.lower());
        let upper = self.upper().join(rhs.upper());

        Self {
            layer: self.layer.add(rhs.layer),
            lower,
            upper,
        }
    }
}

impl<K, V, R, O> AddAssign<Self> for OrdIndexedZSet<K, V, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.lower = self.lower().meet(rhs.lower());
        self.upper = self.upper().join(rhs.upper());
        self.layer.add_assign(rhs.layer);
    }
}

impl<K, V, R, O> AddAssignByRef for OrdIndexedZSet<K, V, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    #[inline]
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.layer.add_assign_by_ref(&rhs.layer);
        self.lower = self.lower().meet(rhs.lower());
        self.upper = self.upper().join(rhs.upper());
    }
}

impl<K, V, R, O> AddByRef for OrdIndexedZSet<K, V, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    #[inline]
    fn add_by_ref(&self, rhs: &Self) -> Self {
        Self {
            layer: self.layer.add_by_ref(&rhs.layer),
            lower: self.lower().meet(rhs.lower()),
            upper: self.upper().join(rhs.upper()),
        }
    }
}

impl<K, V, R, O> BatchReader for OrdIndexedZSet<K, V, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Key = K;
    type Val = V;
    type Time = ();
    type R = R;
    type Cursor<'s> = OrdIndexedZSetCursor<'s, K, V, R, O> where V: 's, O: 's;

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        OrdIndexedZSetCursor {
            cursor: self.layer.cursor(),
        }
    }

    #[inline]
    fn key_count(&self) -> usize {
        self.layer.keys()
    }

    #[inline]
    fn len(&self) -> usize {
        self.layer.tuples()
    }

    #[inline]
    fn lower(&self) -> &Antichain<()> {
        &self.lower
    }

    #[inline]
    fn upper(&self) -> &Antichain<()> {
        &self.upper
    }
}

impl<K, V, R, O> Batch for OrdIndexedZSet<K, V, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Item = (K, V);
    type Batcher = MergeBatcher<(K, V), (), R, Self>;
    type Builder = OrdIndexedZSetBuilder<K, V, R, O>;
    type Merger = OrdIndexedZSetMerger<K, V, R, O>;

    fn item_from(key: K, val: V) -> Self::Item {
        (key, val)
    }

    fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self
    where
        Self::Val: From<()>,
    {
        Self::from_tuples(
            time,
            keys.into_iter()
                .map(|(k, w)| ((k, From::from(())), w))
                .collect(),
        )
    }

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        OrdIndexedZSetMerger::new(self, other)
    }

    fn recede_to(&mut self, _frontier: &()) {}
}

/// State for an in-progress merge.
pub struct OrdIndexedZSetMerger<K, V, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    // result that we are currently assembling.
    result: <Layers<K, V, R, O> as Trie>::MergeBuilder,
}

impl<K, V, R, O> Merger<K, V, (), R, OrdIndexedZSet<K, V, R, O>>
    for OrdIndexedZSetMerger<K, V, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    #[inline]
    fn new(batch1: &OrdIndexedZSet<K, V, R, O>, batch2: &OrdIndexedZSet<K, V, R, O>) -> Self {
        Self {
            result: <<Layers<K, V, R, O> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(
                &batch1.layer,
                &batch2.layer,
            ),
        }
    }

    #[inline]
    fn done(self) -> OrdIndexedZSet<K, V, R, O> {
        OrdIndexedZSet {
            layer: self.result.done(),
            lower: Antichain::from_elem(()),
            upper: Antichain::new(),
        }
    }

    #[inline]
    fn work(
        &mut self,
        source1: &OrdIndexedZSet<K, V, R, O>,
        source2: &OrdIndexedZSet<K, V, R, O>,
        fuel: &mut isize,
    ) {
        *fuel -= self
            .result
            .push_merge(source1.layer.cursor(), source2.layer.cursor()) as isize;
        *fuel = max(*fuel, 1);
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdIndexedZSetCursor<'s, K, V, R, O>
where
    K: Ord + Clone,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset + PartialEq,
    <O as TryInto<usize>>::Error: Debug,
    <O as TryFrom<usize>>::Error: Debug,
{
    cursor: OrderedCursor<'s, K, O, OrderedColumnLeaf<V, R>>,
}

impl<'s, K, V, R, O> Cursor<'s, K, V, (), R> for OrdIndexedZSetCursor<'s, K, V, R, O>
where
    K: Ord + Clone,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    #[inline]
    fn key(&self) -> &K {
        self.cursor.key()
    }

    #[inline]
    fn val(&self) -> &V {
        self.cursor.child.current_key()
    }

    #[inline]
    fn map_times<L: FnMut(&(), &R)>(&mut self, mut logic: L) {
        if self.cursor.child.valid() {
            logic(&(), self.cursor.child.current_diff());
        }
    }

    #[inline]
    fn map_times_through<L: FnMut(&(), &R)>(&mut self, logic: L, _upper: &()) {
        self.map_times(logic)
    }

    #[inline]
    fn weight(&mut self) -> R {
        debug_assert!(self.cursor.child.valid());
        self.cursor.child.current_diff().clone()
    }

    #[inline]
    fn key_valid(&self) -> bool {
        self.cursor.valid()
    }

    #[inline]
    fn val_valid(&self) -> bool {
        self.cursor.child.valid()
    }

    #[inline]
    fn step_key(&mut self) {
        self.cursor.step();
    }

    #[inline]
    fn seek_key(&mut self, key: &K) {
        self.cursor.seek(key);
    }

    #[inline]
    fn last_key(&mut self) -> Option<&K> {
        self.cursor.last_key()
    }

    #[inline]
    fn step_val(&mut self) {
        self.cursor.child.step();
    }

    #[inline]
    fn seek_val(&mut self, val: &V) {
        self.cursor.child.seek_key(val);
    }

    #[inline]
    fn rewind_keys(&mut self) {
        self.cursor.rewind();
    }

    #[inline]
    fn rewind_vals(&mut self) {
        self.cursor.child.rewind();
    }
}

type IndexBuilder<K, V, R, O> = OrderedBuilder<K, OrderedColumnLeafBuilder<V, R>, O>;

/// A builder for creating layers from unsorted update tuples.
pub struct OrdIndexedZSetBuilder<K, V, R, O>
where
    K: Ord,
    V: Ord,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    builder: IndexBuilder<K, V, R, O>,
}

impl<K, V, R, O> Builder<(K, V), (), R, OrdIndexedZSet<K, V, R, O>>
    for OrdIndexedZSetBuilder<K, V, R, O>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone,
    R: MonoidValue,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    #[inline]
    fn new(_time: ()) -> Self {
        Self {
            builder: IndexBuilder::<K, V, R, O>::new(),
        }
    }

    #[inline]
    fn with_capacity(_time: (), capacity: usize) -> Self {
        Self {
            builder: <IndexBuilder<K, V, R, O> as TupleBuilder>::with_capacity(capacity),
        }
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.builder.reserve(additional);
    }

    #[inline]
    fn push(&mut self, ((key, val), diff): ((K, V), R)) {
        self.builder.push_tuple((key, (val, diff)));
    }

    #[inline(never)]
    fn done(self) -> OrdIndexedZSet<K, V, R, O> {
        OrdIndexedZSet {
            layer: self.builder.done(),
            lower: Antichain::from_elem(()),
            upper: Antichain::new(),
        }
    }
}
