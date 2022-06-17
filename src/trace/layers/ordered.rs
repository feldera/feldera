//! Implementation using ordered keys and exponential search.

use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    trace::layers::{advance, Builder, Cursor, MergeBuilder, Trie, TrieSlice, TupleBuilder},
    NumEntries, SharedRef,
};
use deepsize::DeepSizeOf;
use std::{
    cmp::{min, Ordering},
    convert::{TryFrom, TryInto},
    fmt::{Debug, Display, Formatter},
    marker::PhantomData,
    ops::{Add, AddAssign, Neg, Sub},
};
use textwrap::indent;

/// Trait for types used as offsets into an ordered layer.
/// This is usually `usize`, but `u32` can also be used in applications
/// where huge batches do not occur to reduce metadata size.
pub trait OrdOffset:
    Copy + PartialEq + Add<Output = Self> + Sub<Output = Self> + TryFrom<usize> + TryInto<usize>
{
}

impl<O> OrdOffset for O where
    O: Copy + PartialEq + Add<Output = Self> + Sub<Output = Self> + TryFrom<usize> + TryInto<usize>
{
}

/// A level of the trie, with keys and offsets into a lower layer.
///
/// In this representation, the values for `keys[i]` are found at `vals[offs[i]
/// .. offs[i+1]]`.
#[derive(Debug, DeepSizeOf, PartialEq, Eq, Clone)]
pub struct OrderedLayer<K, L, O = usize>
where
    K: Ord,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    /// The keys of the layer.
    pub keys: Vec<K>,
    /// The offsets associate with each key.
    ///
    /// The bounds for `keys[i]` are `(offs[i], offs[i+1]`). The offset array is
    /// guaranteed to be one element longer than the keys array, ensuring
    /// that these accesses do not panic.
    pub offs: Vec<O>,
    /// The ranges of values associated with the keys.
    pub vals: L,
}

impl<K, L, O> Display for OrderedLayer<K, L, O>
where
    K: Ord + Clone + Display,
    L: Trie,
    <Self as Trie>::Cursor: Clone,
    L::Cursor: Clone,
    for<'a> TrieSlice<'a, L>: Display,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        TrieSlice(self, self.cursor()).fmt(f)
    }
}

impl<'a, K, L, O> Display for TrieSlice<'a, OrderedLayer<K, L, O>>
where
    K: Ord + Clone + Display,
    L: Trie,
    <OrderedLayer<K, L, O> as Trie>::Cursor: Clone,
    L::Cursor: Clone,
    for<'b> TrieSlice<'b, L>: Display,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let TrieSlice(storage, cursor) = self;
        let mut cursor: OrderedCursor<L> = cursor.clone();

        while cursor.valid(storage) {
            let key = cursor.key(storage);
            writeln!(f, "{}:", key)?;
            let (val_storage, val_cursor) = cursor.values(storage);

            f.write_str(&indent(
                &TrieSlice(val_storage, val_cursor).to_string(),
                "    ",
            ))?;
            cursor.step(storage);
        }

        Ok(())
    }
}

impl<K, L, O> SharedRef for OrderedLayer<K, L, O>
where
    K: Ord + Clone,
    L: Clone,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Target = Self;

    fn try_into_owned(self) -> Result<Self::Target, Self> {
        Ok(self)
    }
}

impl<K, L, O> NumEntries for OrderedLayer<K, L, O>
where
    K: Ord + Clone,
    L: Trie,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn num_entries_shallow(&self) -> usize {
        self.keys()
    }

    fn num_entries_deep(&self) -> usize {
        self.tuples()
    }

    const CONST_NUM_ENTRIES: Option<usize> = None;
}

impl<K, L, O> NegByRef for OrderedLayer<K, L, O>
where
    K: Ord + Clone,
    L: Trie + NegByRef,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            offs: self.offs.clone(),
            // We assume that offsets in `vals` don't change after negation;
            // otherwise `self.offs` will be invalid.
            vals: self.vals.neg_by_ref(),
        }
    }
}

impl<K, L, O> Neg for OrderedLayer<K, L, O>
where
    K: Ord + Clone,
    L: Trie + Neg<Output = L>,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            keys: self.keys,
            offs: self.offs,
            // We assume that offsets in `vals` don't change after negation;
            // otherwise `self.offs` will be invalid.
            vals: self.vals.neg(),
        }
    }
}

// TODO: by-value merge
impl<K, L, O> Add<Self> for OrderedLayer<K, L, O>
where
    K: Ord + Clone,
    L: Trie,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        if self.is_empty() {
            rhs
        } else if rhs.is_empty() {
            self
        } else {
            self.merge(&rhs)
        }
    }
}

impl<K, L, O> AddAssign<Self> for OrderedLayer<K, L, O>
where
    K: Ord + Clone,
    L: Trie,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn add_assign(&mut self, rhs: Self) {
        if self.is_empty() {
            *self = rhs;
        } else if !rhs.is_empty() {
            *self = self.merge(&rhs);
        }
    }
}

impl<K, L, O> AddAssignByRef for OrderedLayer<K, L, O>
where
    K: Ord + Clone,
    L: Trie,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        if !other.is_empty() {
            *self = self.merge(other);
        }
    }
}

impl<K, L, O> AddByRef for OrderedLayer<K, L, O>
where
    K: Ord + Clone,
    L: Trie,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, L, O> Trie for OrderedLayer<K, L, O>
where
    K: Ord + Clone,
    L: Trie,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Item = (K, L::Item);
    type Cursor = OrderedCursor<L>;
    type MergeBuilder = OrderedBuilder<K, L::MergeBuilder, O>;
    type TupleBuilder = UnorderedBuilder<K, L::TupleBuilder, O>;

    fn keys(&self) -> usize {
        self.keys.len()
    }
    fn tuples(&self) -> usize {
        self.vals.tuples()
    }
    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor {
        if lower < upper {
            let child_lower = self.offs[lower];
            let child_upper = self.offs[lower + 1];
            OrderedCursor {
                bounds: (lower, upper),
                child: self.vals.cursor_from(
                    child_lower.try_into().unwrap(),
                    child_upper.try_into().unwrap(),
                ),
                pos: lower,
            }
        } else {
            OrderedCursor {
                bounds: (0, 0),
                child: self.vals.cursor_from(0, 0),
                pos: 0,
            }
        }
    }
}

/// Assembles a layer of this
pub struct OrderedBuilder<K, L, O = usize>
where
    K: Ord,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    /// Keys
    pub keys: Vec<K>,
    /// Offsets
    pub offs: Vec<O>,
    /// The next layer down
    pub vals: L,
}

impl<K, L, O> Builder for OrderedBuilder<K, L, O>
where
    K: Ord + Clone,
    L: Builder,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Trie = OrderedLayer<K, L::Trie, O>;
    fn boundary(&mut self) -> usize {
        self.offs[self.keys.len()] = O::try_from(self.vals.boundary()).unwrap();
        self.keys.len()
    }
    fn done(mut self) -> Self::Trie {
        if !self.keys.is_empty() && self.offs[self.keys.len()].try_into().unwrap() == 0 {
            self.offs[self.keys.len()] = O::try_from(self.vals.boundary()).unwrap();
        }
        OrderedLayer {
            keys: self.keys,
            offs: self.offs,
            vals: self.vals.done(),
        }
    }
}

impl<K, L, O> MergeBuilder for OrderedBuilder<K, L, O>
where
    K: Ord + Clone,
    L: MergeBuilder,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    fn with_capacity(other1: &Self::Trie, other2: &Self::Trie) -> Self {
        let mut offs = Vec::with_capacity(other1.keys() + other2.keys() + 1);
        offs.push(O::try_from(0_usize).unwrap());
        OrderedBuilder {
            keys: Vec::with_capacity(other1.keys() + other2.keys()),
            offs,
            vals: L::with_capacity(&other1.vals, &other2.vals),
        }
    }
    fn with_key_capacity(cap: usize) -> Self {
        let mut offs = Vec::with_capacity(cap + 1);
        offs.push(O::try_from(0_usize).unwrap());
        OrderedBuilder {
            keys: Vec::with_capacity(cap),
            offs,
            vals: L::with_key_capacity(cap),
        }
    }

    #[inline]
    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        debug_assert!(lower < upper);
        let other_basis = other.offs[lower];
        let self_basis = self
            .offs
            .last()
            .copied()
            .unwrap_or_else(|| O::try_from(0).unwrap());

        self.keys.extend_from_slice(&other.keys[lower..upper]);
        for index in lower..upper {
            self.offs
                .push((other.offs[index + 1] + self_basis) - other_basis);
        }
        self.vals.copy_range(
            &other.vals,
            other_basis.try_into().unwrap(),
            other.offs[upper].try_into().unwrap(),
        );
    }

    fn push_merge(
        &mut self,
        other1: (&Self::Trie, <Self::Trie as Trie>::Cursor),
        other2: (&Self::Trie, <Self::Trie as Trie>::Cursor),
    ) -> usize {
        let (trie1, cursor1) = other1;
        let (trie2, cursor2) = other2;
        let mut lower1 = cursor1.bounds.0;
        let upper1 = cursor1.bounds.1;
        let mut lower2 = cursor2.bounds.0;
        let upper2 = cursor2.bounds.1;

        self.keys.reserve((upper1 - lower1) + (upper2 - lower2));

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            self.merge_step((trie1, &mut lower1, upper1), (trie2, &mut lower2, upper2));
        }

        if lower1 < upper1 {
            self.copy_range(trie1, lower1, upper1);
        }
        if lower2 < upper2 {
            self.copy_range(trie2, lower2, upper2);
        }

        self.keys.len()
    }
}

impl<K, L, O> OrderedBuilder<K, L, O>
where
    K: Ord + Clone,
    L: MergeBuilder,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    /// Performs one step of merging.
    #[inline]
    pub fn merge_step(
        &mut self,
        other1: (&<Self as Builder>::Trie, &mut usize, usize),
        other2: (&<Self as Builder>::Trie, &mut usize, usize),
    ) {
        let (trie1, lower1, upper1) = other1;
        let (trie2, lower2, upper2) = other2;

        match trie1.keys[*lower1].cmp(&trie2.keys[*lower2]) {
            Ordering::Less => {
                // determine how far we can advance lower1 until we reach/pass lower2
                let step = 1 + advance(&trie1.keys[(1 + *lower1)..upper1], |x| {
                    x < &trie2.keys[*lower2]
                });
                let step = min(step, 1_000);
                self.copy_range(trie1, *lower1, *lower1 + step);
                *lower1 += step;
            }
            Ordering::Equal => {
                let lower = self.vals.boundary();
                // record vals_length so we can tell if anything was pushed.
                let upper = self.vals.push_merge(
                    (
                        &trie1.vals,
                        trie1.vals.cursor_from(
                            trie1.offs[*lower1].try_into().unwrap(),
                            trie1.offs[*lower1 + 1].try_into().unwrap(),
                        ),
                    ),
                    (
                        &trie2.vals,
                        trie2.vals.cursor_from(
                            trie2.offs[*lower2].try_into().unwrap(),
                            trie2.offs[*lower2 + 1].try_into().unwrap(),
                        ),
                    ),
                );
                if upper > lower {
                    self.keys.push(trie1.keys[*lower1].clone());
                    self.offs.push(O::try_from(upper).unwrap());
                }

                *lower1 += 1;
                *lower2 += 1;
            }
            Ordering::Greater => {
                // determine how far we can advance lower2 until we reach/pass lower1
                let step = 1 + advance(&trie2.keys[(1 + *lower2)..upper2], |x| {
                    x < &trie1.keys[*lower1]
                });
                let step = min(step, 1_000);
                self.copy_range(trie2, *lower2, *lower2 + step);
                *lower2 += step;
            }
        }
    }
}

impl<K, L, O> TupleBuilder for OrderedBuilder<K, L, O>
where
    K: Ord + Clone,
    L: TupleBuilder,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Item = (K, L::Item);
    fn new() -> Self {
        OrderedBuilder {
            keys: Vec::new(),
            offs: vec![O::try_from(0).unwrap()],
            vals: L::new(),
        }
    }
    fn with_capacity(cap: usize) -> Self {
        let mut offs = Vec::with_capacity(cap + 1);
        offs.push(O::try_from(0).unwrap());
        OrderedBuilder {
            keys: Vec::with_capacity(cap),
            offs,
            vals: L::with_capacity(cap),
        }
    }
    #[inline]
    fn push_tuple(&mut self, (key, val): (K, L::Item)) {
        // if first element, prior element finish, or different element, need to push
        // and maybe punctuate.
        if self.keys.is_empty()
            || self.offs[self.keys.len()].try_into().unwrap() != 0
            || self.keys[self.keys.len() - 1] != key
        {
            if !self.keys.is_empty() && self.offs[self.keys.len()].try_into().unwrap() == 0 {
                self.offs[self.keys.len()] = O::try_from(self.vals.boundary()).unwrap();
            }
            self.keys.push(key);
            self.offs.push(O::try_from(0).unwrap()); // <-- indicates
                                                     // "unfinished".
        }
        self.vals.push_tuple(val);
    }

    fn tuples(&self) -> usize {
        self.vals.tuples()
    }
}

pub struct UnorderedBuilder<K, L, O = usize>
where
    K: Ord,
    L: TupleBuilder,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    pub vals: Vec<(K, L::Item)>,
    _phantom: PhantomData<O>,
}

impl<K, L, O> Builder for UnorderedBuilder<K, L, O>
where
    K: Ord + Clone,
    L: TupleBuilder,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Trie = OrderedLayer<K, L::Trie, O>;
    fn boundary(&mut self) -> usize {
        self.vals.len()
    }
    fn done(mut self) -> Self::Trie {
        // Don't use `sort_unstable_by_key` to avoid cloning the key.
        self.vals
            .sort_unstable_by(|(k1, _), (k2, _)| K::cmp(k1, k2));
        let mut builder = <OrderedBuilder<K, L, O> as TupleBuilder>::with_capacity(self.vals.len());

        for (k, v) in self.vals.into_iter() {
            builder.push_tuple((k, v))
        }
        builder.done()
    }
}

impl<K, L, O> TupleBuilder for UnorderedBuilder<K, L, O>
where
    K: Ord + Clone,
    L: TupleBuilder,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Item = (K, L::Item);
    fn new() -> Self {
        UnorderedBuilder {
            vals: Vec::new(),
            _phantom: PhantomData,
        }
    }
    fn with_capacity(cap: usize) -> Self {
        UnorderedBuilder {
            vals: Vec::with_capacity(cap),
            _phantom: PhantomData,
        }
    }
    #[inline]
    fn push_tuple(&mut self, kv: Self::Item) {
        self.vals.push(kv);
    }

    fn tuples(&self) -> usize {
        self.vals.len()
    }
}

/// A cursor with a child cursor that is updated as we move.
#[derive(Debug, Clone)]
pub struct OrderedCursor<L: Trie> {
    // keys: OwningRef<Rc<Erased>, [K]>,
    // offs: OwningRef<Rc<Erased>, [usize]>,
    pos: usize,
    bounds: (usize, usize),
    /// The cursor for the trie layer below this one.
    pub child: L::Cursor,
}

impl<K, L, O> Cursor<OrderedLayer<K, L, O>> for OrderedCursor<L>
where
    K: Ord,
    L: Trie,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
{
    type Key = K;
    type ValueStorage = L;

    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }
    fn key<'a>(&self, storage: &'a OrderedLayer<K, L, O>) -> &'a Self::Key {
        &storage.keys[self.pos]
    }
    fn values<'a>(&self, storage: &'a OrderedLayer<K, L, O>) -> (&'a L, L::Cursor) {
        let child_cursor = if self.valid(storage) {
            storage.vals.cursor_from(
                storage.offs[self.pos].try_into().unwrap(),
                storage.offs[self.pos + 1].try_into().unwrap(),
            )
        } else {
            storage.vals.cursor_from(0, 0)
        };
        (&storage.vals, child_cursor)
    }
    fn step(&mut self, storage: &OrderedLayer<K, L, O>) {
        self.pos += 1;
        if self.valid(storage) {
            self.child.reposition(
                &storage.vals,
                storage.offs[self.pos].try_into().unwrap(),
                storage.offs[self.pos + 1].try_into().unwrap(),
            );
        } else {
            self.pos = self.bounds.1;
        }
    }
    fn seek(&mut self, storage: &OrderedLayer<K, L, O>, key: &Self::Key) {
        self.pos += advance(&storage.keys[self.pos..self.bounds.1], |k| k.lt(key));
        if self.valid(storage) {
            self.child.reposition(
                &storage.vals,
                storage.offs[self.pos].try_into().unwrap(),
                storage.offs[self.pos + 1].try_into().unwrap(),
            );
        }
    }
    // fn size(&self) -> usize { self.bounds.1 - self.bounds.0 }
    fn valid(&self, _storage: &OrderedLayer<K, L, O>) -> bool {
        self.pos < self.bounds.1
    }
    fn rewind(&mut self, storage: &OrderedLayer<K, L, O>) {
        self.pos = self.bounds.0;
        if self.valid(storage) {
            self.child.reposition(
                &storage.vals,
                storage.offs[self.pos].try_into().unwrap(),
                storage.offs[self.pos + 1].try_into().unwrap(),
            );
        }
    }
    fn reposition(&mut self, storage: &OrderedLayer<K, L, O>, lower: usize, upper: usize) {
        self.pos = lower;
        self.bounds = (lower, upper);
        if self.valid(storage) {
            self.child.reposition(
                &storage.vals,
                storage.offs[self.pos].try_into().unwrap(),
                storage.offs[self.pos + 1].try_into().unwrap(),
            );
        }
    }
}
