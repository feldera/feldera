//! Implementation using ordered keys and exponential search.

//mod consumer;
//mod tests;

use rand::Rng;
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    dynamic::{DataTrait, DynVec, Erase, Factory, LeanVec, WithFactory},
    utils::{advance, assume},
    DBData, NumEntries,
};
use size_of::SizeOf;
use std::{
    cmp::{min, Ordering},
    fmt::{self, Debug, Display, Formatter},
    ops::{Add, AddAssign, Neg},
};
use textwrap::indent;

use super::{Builder, Cursor, MergeBuilder, OrdOffset, Trie, TupleBuilder};

pub struct LayerFactories<K: DataTrait + ?Sized, C> {
    pub key: &'static dyn Factory<K>,
    pub keys: &'static dyn Factory<DynVec<K>>,
    pub child: C,
}

impl<K: DataTrait + ?Sized, C> LayerFactories<K, C> {
    pub fn new<KType>(child: C) -> Self
    where
        KType: DBData + Erase<K>,
    {
        Self {
            key: WithFactory::<KType>::FACTORY,
            keys: WithFactory::<LeanVec<KType>>::FACTORY,
            child,
        }
    }
}

impl<K: DataTrait + ?Sized, C: Clone> Clone for LayerFactories<K, C> {
    fn clone(&self) -> Self {
        Self {
            key: self.key,
            keys: self.keys,
            child: self.child.clone(),
        }
    }
}

/// A level of the trie, with keys and offsets into a lower layer.
///
/// In this representation, the values for `keys[i]` are found at `vals[offs[i]
/// .. offs[i+1]]`.
// False positive from clippy
#[derive(SizeOf, Archive, Serialize, Deserialize)]
pub struct Layer<K, L, O = usize>
where
    K: DataTrait + ?Sized,
    O: 'static,
    L: Trie,
{
    #[size_of(skip)]
    pub(crate) factories: LayerFactories<K, <L as Trie>::Factories>,

    /// The keys of the layer.
    pub(crate) keys: Box<DynVec<K>>,
    /// The offsets associated with each key.
    ///
    /// The bounds for `keys[i]` are `(offs[i], offs[i+1]`). The offset array is
    /// guaranteed to be one element longer than the keys array, ensuring
    /// that these accesses do not panic.
    pub(crate) offs: Vec<O>,
    /// The ranges of values associated with the keys.
    pub(crate) vals: L,
    pub(crate) lower_bound: usize,
}

impl<K, L, O> PartialEq for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    O: OrdOffset,
    L: Trie + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.keys.eq(&other.keys)
            && self.offs == other.offs
            && self.vals == other.vals
            && self.lower_bound == other.lower_bound
    }
}

impl<K, L, O> Eq for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    O: OrdOffset,
    L: Trie + Eq,
{
}

impl<K, L, O> Clone for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie + Clone,
    O: Clone,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            keys: self.keys.clone(),
            offs: self.offs.clone(),
            vals: self.vals.clone(),
            lower_bound: self.lower_bound,
        }
    }
}

impl<K, L, O> Debug for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie + Debug,
    O: 'static + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Layer")
            .field("keys", &self.keys)
            .field("offs", &self.offs)
            .field("vals", &self.vals)
            .field("lower_bound", &self.lower_bound)
            .finish()
    }
}

impl<K: DataTrait + ?Sized, L, O: 'static> Layer<K, L, O>
where
    L: Trie,
{
    /*
    /// Break down an `OrderedLayer` into its constituent parts
    pub fn into_parts(self) -> (ErasedVec<K>, Vec<O>, L, usize) {
        (self.keys, self.offs, self.vals, self.lower_bound)
    }

    /// Expose an `OrderedLayer` as its constituent parts
    pub fn as_parts(&self) -> (&[K], &[O], &L, usize) {
        (&self.keys, &self.offs, &self.vals, self.lower_bound)
    }

    /// Create a new `OrderedLayer` from its component parts
    ///
    /// # Safety
    ///
    /// - `keys` must have a length of `offs.len() + 1` and every offset within
    ///   `offs` must be a valid value index into `vals`.
    /// - Every key's offset range must also be a valid range within `vals`
    /// - Every value range must be non-empty
    pub unsafe fn from_parts(keys: Vec<K>, offs: Vec<O>, vals: L, lower_bound: usize) -> Self {
        // TODO: Maybe validate indices into `vals` when debug assertions are enabled
        // TODO: Maybe validate that value ranges are all valid
        debug_assert_eq!(keys.len() + 1, offs.len());
        debug_assert!(lower_bound <= keys.len());

        Self {
            keys,
            offs,
            vals,
            lower_bound,
        }
    }
    */

    /// Assume the invariants of the current builder
    ///
    /// # Safety
    ///
    /// Requires that `offs` has a length of `keys + 1`
    unsafe fn assume_invariants(&self) {
        assume(self.offs.len() == self.keys.len() + 1);
        assume(self.lower_bound <= self.keys.len());
    }

    /// Compute a random sample of size `sample_size` of keys in `self.keys`.
    ///
    /// Pushes the random sample of keys to the `output` vector in ascending
    /// order.
    pub fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut DynVec<K>)
    where
        RG: Rng,
    {
        self.keys
            .sample_slice(self.lower_bound, self.keys.len(), rng, sample_size, output);
    }
}

impl<K, L, O> Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie,
    O: OrdOffset,
{
    /// Truncate layer at the first key greater than or equal to `lower_bound`.
    pub fn truncate_keys_below(&mut self, lower_bound: &K) {
        let index = self.keys.advance_to(0, self.keys.len(), lower_bound);
        self.truncate_below(index);
    }
}

impl<K, L, O> NumEntries for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie + NumEntries,
    O: OrdOffset,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.keys.len()
    }

    fn num_entries_deep(&self) -> usize {
        self.vals.num_entries_deep()
    }
}

impl<K, L, O> NegByRef for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie + NegByRef,
    O: OrdOffset,
{
    // TODO: We can eliminate elements from `0..lower_bound` when creating the
    // negated layer
    fn neg_by_ref(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            keys: self.keys.clone(),
            offs: self.offs.clone(),
            // We assume that offsets in `vals` don't change after negation;
            // otherwise `self.offs` will be invalid.
            vals: self.vals.neg_by_ref(),
            lower_bound: self.lower_bound,
        }
    }
}

impl<K, L, O> Neg for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie + Neg<Output = L>,
    O: OrdOffset,
{
    type Output = Self;

    // TODO: We can eliminate elements from `0..lower_bound` when creating the
    // negated layer
    fn neg(self) -> Self {
        Self {
            factories: self.factories,
            keys: self.keys,
            offs: self.offs,
            // We assume that offsets in `vals` don't change after negation;
            // otherwise `self.offs` will be invalid.
            vals: self.vals.neg(),
            lower_bound: self.lower_bound,
        }
    }
}

// TODO: by-value merge
impl<K, L, O> Add<Self> for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie,
    O: OrdOffset,
{
    type Output = Self;

    // TODO: In-place merge
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

impl<K, L, O> AddAssign<Self> for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie,
    O: OrdOffset,
{
    // TODO: In-place merge
    fn add_assign(&mut self, rhs: Self) {
        if self.is_empty() {
            *self = rhs;
        } else if !rhs.is_empty() {
            *self = self.merge(&rhs);
        }
    }
}

impl<K, L, O> AddAssignByRef for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie,
    O: OrdOffset,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        if !other.is_empty() {
            *self = self.merge(other);
        }
    }
}

impl<K, L, O> AddByRef for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie,
    O: OrdOffset,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, L, O> Trie for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie,
    O: OrdOffset,
{
    type Item<'a> = (&'a mut K, L::Item<'a>);
    type ItemRef<'a> = (&'a K, L::ItemRef<'a>);
    type Factories = LayerFactories<K, L::Factories>;
    type Cursor<'s> = LayerCursor<'s, K, L, O> where K: 's, O: 's, L: 's;
    type MergeBuilder = LayerBuilder<K, L::MergeBuilder, O>;
    type TupleBuilder = LayerBuilder<K, L::TupleBuilder, O>;

    fn keys(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.keys.len() - self.lower_bound
    }

    fn tuples(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.vals.tuples()
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        unsafe { self.assume_invariants() }

        if lower < upper {
            let child_lower = self.offs[lower];
            let child_upper = self.offs[lower + 1];

            LayerCursor {
                bounds: (lower, upper),
                storage: self,
                child: self
                    .vals
                    .cursor_from(child_lower.into_usize(), child_upper.into_usize()),
                pos: lower as isize,
            }
        } else {
            LayerCursor {
                bounds: (0, 0),
                storage: self,
                child: self.vals.cursor_from(0, 0),
                pos: 0,
            }
        }
    }

    fn lower_bound(&self) -> usize {
        self.lower_bound
    }

    fn truncate_below(&mut self, lower_bound: usize) {
        if lower_bound > self.lower_bound {
            self.lower_bound = min(lower_bound, self.keys.len());
        }

        let vals_bound = self.offs[self.lower_bound];
        self.vals.truncate_below(vals_bound.into_usize());
    }
}

impl<K, L, O> Display for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie,
    for<'a> L::Cursor<'a>: Clone + Display,
    O: OrdOffset,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.cursor().fmt(f)
    }
}

/// Assembles a layer of this
#[derive(SizeOf, Clone)]
pub struct LayerBuilder<K, L, O = usize>
where
    K: DataTrait + ?Sized,
    L: Builder,
{
    #[size_of(skip)]
    factories: LayerFactories<K, <L::Trie as Trie>::Factories>,
    /// Keys
    pub keys: Box<DynVec<K>>,
    /// Offsets
    pub offs: Vec<O>,
    /// The next layer down
    pub vals: L,
}

impl<K, L, O> Debug for LayerBuilder<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Builder + Debug,
    O: 'static + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LayerBuilder")
            .field("keys", &self.keys)
            .field("offs", &self.offs)
            .field("vals", &self.vals)
            .finish()
    }
}

impl<K, L, O> LayerBuilder<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Builder,
{
    /// Performs one step of merging.
    pub fn merge_step(
        &mut self,
        (trie1, lower1, upper1): (&Layer<K, L::Trie, O>, &mut usize, usize),
        (trie2, lower2, upper2): (&Layer<K, L::Trie, O>, &mut usize, usize),
    ) where
        L: MergeBuilder,
        O: OrdOffset,
    {
        match trie1.keys[*lower1].cmp(&trie2.keys[*lower2]) {
            Ordering::Less => {
                // determine how far we can advance lower1 until we reach/pass lower2
                let step = 1 + trie1
                    .keys
                    .advance_to(1 + *lower1, upper1, &trie2.keys[*lower2]);
                let step = min(step, 1_000);
                self.copy_range(trie1, *lower1, *lower1 + step);
                *lower1 += step;
            }

            Ordering::Equal => {
                let lower = self.vals.boundary();
                // record vals_length so we can tell if anything was pushed.
                self.vals.push_merge(
                    trie1.vals.cursor_from(
                        trie1.offs[*lower1].into_usize(),
                        trie1.offs[*lower1 + 1].into_usize(),
                    ),
                    trie2.vals.cursor_from(
                        trie2.offs[*lower2].into_usize(),
                        trie2.offs[*lower2 + 1].into_usize(),
                    ),
                );
                if self.vals.keys() > lower {
                    self.keys.push_ref(&trie1.keys[*lower1]);
                    self.offs.push(O::from_usize(self.vals.keys()));
                }

                *lower1 += 1;
                *lower2 += 1;
            }

            Ordering::Greater => {
                // determine how far we can advance lower2 until we reach/pass lower1
                let step = 1 + trie2
                    .keys
                    .advance_to(1 + *lower2, upper2, &trie1.keys[*lower1]);
                let step = min(step, 1_000);
                self.copy_range(trie2, *lower2, *lower2 + step);
                *lower2 += step;
            }
        }
    }

    pub fn merge_step_retain_keys<F>(
        &mut self,
        (trie1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (trie2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        filter: &F,
    ) where
        L: MergeBuilder,
        O: OrdOffset,
        F: Fn(&K) -> bool,
    {
        match trie1.keys[*lower1].cmp(&trie2.keys[*lower2]) {
            Ordering::Less => {
                // determine how far we can advance lower1 until we reach/pass lower2
                let step = 1 + trie1
                    .keys
                    .advance_to(1 + *lower1, upper1, &trie2.keys[*lower2]);
                let step = min(step, 1_000);
                self.copy_range_retain_keys(trie1, *lower1, *lower1 + step, filter);
                *lower1 += step;
            }

            Ordering::Equal => {
                let lower = self.vals.boundary();
                if filter(&trie1.keys[*lower1]) {
                    // record vals_length so we can tell if anything was pushed.
                    self.vals.push_merge(
                        trie1.vals.cursor_from(
                            trie1.offs[*lower1].into_usize(),
                            trie1.offs[*lower1 + 1].into_usize(),
                        ),
                        trie2.vals.cursor_from(
                            trie2.offs[*lower2].into_usize(),
                            trie2.offs[*lower2 + 1].into_usize(),
                        ),
                    );
                    if self.vals.keys() > lower {
                        self.keys.push_ref(&trie1.keys[*lower1]);
                        self.offs.push(O::from_usize(self.vals.keys()));
                    }
                }

                *lower1 += 1;
                *lower2 += 1;
            }

            Ordering::Greater => {
                // determine how far we can advance lower2 until we reach/pass lower1
                let step = 1 + trie2
                    .keys
                    .advance_to(1 + *lower2, upper2, &trie1.keys[*lower1]);
                let step = min(step, 1_000);
                self.copy_range_retain_keys(trie2, *lower2, *lower2 + step, filter);
                *lower2 += step;
            }
        }
    }

    /*
    /// Push a key and all of its associated values to the current builder
    ///
    /// Can be more efficient than repeatedly calling `.push_tuple()` because it
    /// doesn't require an owned key for every value+diff pair which can allow
    /// eliding unnecessary clones
    pub fn with_key<F>(&mut self, mut key: K, with: F)
    where
        L: DynTupleBuilder,
        O: OrdOffset,
        F: for<'a> FnOnce(ErasedOrderedBuilderVals<'a, K, I, L, O>),
    {
        let mut pushes = 0;
        let vals = ErasedOrderedBuilderVals {
            builder: self,
            pushes: &mut pushes,
        };
        with(vals);

        // If the user's closure actually added any elements, push the key
        if pushes != 0
            && (self.keys.is_empty()
                || !self.offs[self.keys.len()].is_zero()
                || !(self.vtables.key.eq)(&self.keys[self.keys.len() - 1], &key))
        {
            if !self.keys.is_empty() && self.offs[self.keys.len()].is_zero() {
                self.offs[self.keys.len()] = O::from_usize(self.vals.boundary());
            }
            // Safety: we own `key`, and we forget it, so `drop` won't get called.
            unsafe { self.keys.push(&mut key) };
            forget(key);
            self.offs.push(O::zero());
        }
    }*/
}

/*
pub struct ErasedOrderedBuilderVals<'a, K, I, L, O>
where
    K: 'static,
    I: 'static,
    O: 'static,
    L: DynBuilder,
{
    builder: &'a mut ErasedOrderedBuilder<K, I, L, O>,
    pushes: &'a mut usize,
}
*/

/*
impl<'a, K, I, L, O> ErasedOrderedBuilderVals<'a, K, I, L, O>
where
    K: 'static,
    L: DynTupleBuilder,
{
    pub fn push(&mut self, value: <L as DynTupleBuilder>::Item) {
        *self.pushes += 1;
        unsafe { self.builder.vals.push_tuple(value); }
    }
}
*/

impl<K, L, O> Builder for LayerBuilder<K, L, O>
where
    K: DataTrait + ?Sized,
    L: Builder,
    O: OrdOffset,
{
    type Trie = Layer<K, L::Trie, O>;

    fn boundary(&mut self) -> usize {
        self.offs[self.keys.len()] = O::from_usize(self.vals.boundary());
        self.keys.len()
    }

    fn done(mut self) -> Self::Trie {
        if !self.keys.is_empty() && self.offs[self.keys.len()].is_zero() {
            self.offs[self.keys.len()] = O::from_usize(self.vals.boundary());
        }

        Layer {
            factories: self.factories,
            keys: self.keys,
            offs: self.offs,
            vals: self.vals.done(),
            lower_bound: 0,
        }
    }
}

impl<K, L, O> LayerBuilder<K, L, O>
where
    K: DataTrait + ?Sized,
    L: MergeBuilder,
    O: OrdOffset,
{
    /// Like `push_merge`, but uses `fuel` to bound the amount of work.
    ///
    /// Builds at most `fuel` values plus values for one extra key.
    /// If `fuel > 0` when the method returns, this means that the merge is
    /// complete.
    pub fn push_merge_fueled(
        &mut self,
        (source1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (source2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        fuel: &mut isize,
    ) {
        let starting_updates = self.vals.keys();
        let mut effort = 0isize;

        // while both mergees are still active
        while *lower1 < upper1 && *lower2 < upper2 && effort < *fuel {
            self.merge_step((source1, lower1, upper1), (source2, lower2, upper2));
            effort = (self.vals.keys() - starting_updates) as isize;
        }

        // Merging is complete; only copying remains.
        if *lower1 == upper1 || *lower2 == upper2 {
            // Limit merging by remaining fuel.
            let mut remaining_fuel = *fuel - effort;
            if remaining_fuel > 0 {
                if *lower1 < upper1 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower1 =
                        self.copy_range_fueled(source1, *lower1, upper1, remaining_fuel as usize);
                }
                if *lower2 < upper2 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower2 =
                        self.copy_range_fueled(source2, *lower2, upper2, remaining_fuel as usize);
                }
            }
        }

        effort = (self.vals.keys() - starting_updates) as isize;

        *fuel -= effort;
    }

    pub fn push_merge_retain_keys_fueled<F>(
        &mut self,
        (source1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (source2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        filter: &F,
        fuel: &mut isize,
    ) where
        F: Fn(&K) -> bool,
    {
        let starting_updates = self.vals.keys();
        let mut effort = 0isize;

        // while both mergees are still active
        while *lower1 < upper1 && *lower2 < upper2 && effort < *fuel {
            self.merge_step_retain_keys(
                (source1, lower1, upper1),
                (source2, lower2, upper2),
                filter,
            );
            effort = (self.vals.keys() - starting_updates) as isize;
        }

        // Merging is complete; only copying remains.
        if *lower1 == upper1 || *lower2 == upper2 {
            // Limit merging by remaining fuel.
            let mut remaining_fuel = *fuel - effort;
            if remaining_fuel > 0 {
                if *lower1 < upper1 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower1 = self.copy_range_retain_keys_fueled(
                        source1,
                        *lower1,
                        upper1,
                        filter,
                        remaining_fuel as usize,
                    );
                }
                if *lower2 < upper2 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower2 = self.copy_range_retain_keys_fueled(
                        source2,
                        *lower2,
                        upper2,
                        filter,
                        remaining_fuel as usize,
                    );
                }
            }
        }

        effort = (self.vals.keys() - starting_updates) as isize;

        *fuel -= effort;
    }

    /// Like `copy_range`, but uses `fuel` to bound the amount of work.
    ///
    /// Invariants:
    /// - Copies at most `fuel` values plus values for one extra key.
    /// - If `fuel` is greater than or equal to the number of values, in the
    ///   `lower..upper` key range, copies the entire range.
    pub fn copy_range_fueled(
        &mut self,
        other: &<Self as Builder>::Trie,
        lower: usize,
        mut upper: usize,
        fuel: usize,
    ) -> usize {
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        let other_basis = other.offs[lower];
        let self_basis = self.offs.last().copied().unwrap_or_else(|| O::zero());

        // Number of keys in the `[lower..upper]` range that can be copied without
        // exceeding `fuel`.
        let keys = advance(&other.offs[lower..upper], |offset| {
            offset.into_usize() - other_basis.into_usize() <= fuel
        });

        upper = lower + keys;

        self.keys
            .extend_from_range(other.keys.as_ref(), lower, upper);
        for index in lower..upper {
            self.offs
                .push((other.offs[index + 1] + self_basis) - other_basis);
        }

        self.vals.copy_range(
            &other.vals,
            other_basis.into_usize(),
            other.offs[upper].into_usize(),
        );

        upper
    }

    fn copy_range_retain_keys_fueled<F>(
        &mut self,
        other: &<Self as Builder>::Trie,
        lower: usize,
        mut upper: usize,
        filter: &F,
        fuel: usize,
    ) -> usize
    where
        F: Fn(&K) -> bool,
    {
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        let other_basis = other.offs[lower];

        // Number of keys in the `[lower..upper]` range that can be copied without
        // exceeding `fuel`.
        let keys = advance(&other.offs[lower..upper], |offset| {
            offset.into_usize() - other_basis.into_usize() <= fuel
        });

        upper = lower + keys;

        self.copy_range_retain_keys(other, lower, upper, filter);

        upper
    }
}

impl<K, V, L, O> LayerBuilder<K, L, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    O: OrdOffset,
    L: MergeBuilder,
    <L as Builder>::Trie: 'static,
    for<'a, 'b> <<L as Builder>::Trie as Trie>::Cursor<'a>: Cursor<'a, Key = V>,
{
    /// Like `push_merge_fueled`, but also removes values that don't pass
    /// `filter` in both inputs.
    pub fn push_merge_retain_values_fueled<KF, VF>(
        &mut self,
        (source1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (source2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        key_filter: &KF,
        value_filter: &VF,
        fuel: &mut isize,
    ) where
        KF: Fn(&K) -> bool,
        VF: Fn(&V) -> bool,
    {
        let starting_updates = self.vals.keys();
        let mut effort = 0isize;

        // while both mergees are still active
        while *lower1 < upper1 && *lower2 < upper2 && effort < *fuel {
            self.merge_step_retain_values_fueled(
                (source1, lower1, upper1),
                (source2, lower2, upper2),
                key_filter,
                value_filter,
                usize::max_value(),
            );
            // TODO: account for filtered out keys.
            effort = (self.vals.keys() - starting_updates) as isize;
        }

        // Merging is complete; only copying remains.
        if *lower1 == upper1 || *lower2 == upper2 {
            // Limit merging by remaining fuel.
            let mut remaining_fuel = *fuel - effort;
            if remaining_fuel > 0 {
                if *lower1 < upper1 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower1 = self.copy_range_retain_values_fueled(
                        source1,
                        *lower1,
                        upper1,
                        key_filter,
                        value_filter,
                        remaining_fuel as usize,
                    );
                }
                if *lower2 < upper2 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower2 = self.copy_range_retain_values_fueled(
                        source2,
                        *lower2,
                        upper2,
                        key_filter,
                        value_filter,
                        remaining_fuel as usize,
                    );
                }
            }
        }

        effort = (self.vals.keys() - starting_updates) as isize;
        *fuel -= effort;
    }

    fn merge_step_retain_values_fueled<KF, VF>(
        &mut self,
        (trie1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (trie2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        key_filter: &KF,
        value_filter: &VF,
        fuel: usize,
    ) where
        KF: Fn(&K) -> bool,
        VF: Fn(&V) -> bool,
    {
        match trie1.keys[*lower1].cmp(&trie2.keys[*lower2]) {
            Ordering::Less => {
                // determine how far we can advance lower1 until we reach/pass lower2
                let step = 1 + trie1
                    .keys
                    .advance_to(1 + *lower1, upper1, &trie2.keys[*lower2]);
                let step = min(step, 1_000);
                *lower1 = self.copy_range_retain_values_fueled(
                    trie1,
                    *lower1,
                    *lower1 + step,
                    key_filter,
                    value_filter,
                    fuel,
                );
            }

            Ordering::Equal => {
                let lower = self.vals.boundary();
                let cursor1 = trie1.vals.cursor_from(
                    trie1.offs[*lower1].into_usize(),
                    trie1.offs[*lower1 + 1].into_usize(),
                );

                let cursor2 = trie2.vals.cursor_from(
                    trie2.offs[*lower2].into_usize(),
                    trie2.offs[*lower2 + 1].into_usize(),
                );

                if key_filter(&trie1.keys[*lower1]) {
                    // record vals_length so we can tell if anything was pushed.
                    self.vals
                        .push_merge_retain_keys(cursor1, cursor2, value_filter);

                    if self.vals.keys() > lower {
                        self.keys.push_ref(&trie1.keys[*lower1]);
                        self.offs.push(O::from_usize(self.vals.keys()));
                    }
                }
                *lower1 += 1;
                *lower2 += 1;
            }

            Ordering::Greater => {
                // determine how far we can advance lower2 until we reach/pass lower1
                let step = 1 + trie2
                    .keys
                    .advance_to(1 + *lower2, upper2, &trie1.keys[*lower1]);
                let step = min(step, 1_000);
                *lower2 = self.copy_range_retain_values_fueled(
                    trie2,
                    *lower2,
                    *lower2 + step,
                    key_filter,
                    value_filter,
                    fuel,
                );
            }
        }
    }

    fn copy_range_retain_values_fueled<KF, VF>(
        &mut self,
        other: &<Self as Builder>::Trie,
        lower: usize,
        upper: usize,
        key_filter: &KF,
        value_filter: &VF,
        fuel: usize,
    ) -> usize
    where
        KF: Fn(&K) -> bool,
        VF: Fn(&V) -> bool,
    {
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        let other_start = other.offs[lower].into_usize();

        for index in lower..upper {
            if key_filter(&other.keys[index]) {
                let self_basis = self.vals.boundary();
                let other_end = other.offs[index + 1].into_usize();

                let cursor = other
                    .vals
                    .cursor_from(other.offs[index].into_usize(), other_end);

                let other_basis = cursor.position();

                if other_end > other_basis {
                    self.vals.copy_range_retain_keys(
                        &other.vals,
                        other_basis,
                        other_end,
                        value_filter,
                    );
                    if self.vals.keys() > self_basis {
                        self.keys.push_ref(&other.keys[index]);
                        self.offs.push(O::from_usize(self.vals.keys()));
                    }
                }

                if other_end - other_start >= fuel {
                    return index + 1;
                }
            }
        }

        upper
    }
}

impl<K, L, O> MergeBuilder for LayerBuilder<K, L, O>
where
    K: DataTrait + ?Sized,
    L: MergeBuilder,
    O: OrdOffset,
{
    fn with_capacity(other1: &Self::Trie, other2: &Self::Trie) -> Self {
        let mut offs = Vec::with_capacity(other1.keys() + other2.keys() + 1);
        offs.push(O::zero());

        let mut keys = other1.factories.keys.default_box();
        keys.reserve(other1.keys() + other2.keys());

        Self {
            factories: other1.factories.clone(),
            keys,
            offs,
            vals: L::with_capacity(&other1.vals, &other2.vals),
        }
    }

    /*fn with_key_capacity(capacity: usize) -> Self {
        let mut offs = Vec::with_capacity(capacity + 1);
        offs.push(O::zero());

        Self {
            keys: Vec::with_capacity(capacity),
            offs,
            vals: L::with_key_capacity(capacity),
        }
    }*/

    fn reserve(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.offs.reserve(additional);
        self.vals.reserve(additional);
    }

    fn keys(&self) -> usize {
        self.keys.len()
    }

    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        let other_basis = other.offs[lower];
        let self_basis = self.offs.last().copied().unwrap_or_else(|| O::zero());

        self.keys
            .extend_from_range(other.keys.as_ref(), lower, upper);
        for index in lower..upper {
            self.offs
                .push((other.offs[index + 1] + self_basis) - other_basis);
        }

        self.vals.copy_range(
            &other.vals,
            other_basis.into_usize(),
            other.offs[upper].into_usize(),
        );
    }

    fn copy_range_retain_keys<'a, F>(
        &mut self,
        other: &'a Self::Trie,
        lower: usize,
        upper: usize,
        filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        for index in lower..upper {
            if filter(&other.keys[index]) {
                self.keys.push_ref(&other.keys[index]);

                self.vals.copy_range(
                    &other.vals,
                    other.offs[index].into_usize(),
                    other.offs[index + 1].into_usize(),
                );

                self.offs.push(O::from_usize(self.vals.keys()));
            }
        }
    }

    fn push_merge<'a>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
    ) {
        let (mut lower1, upper1) = cursor1.bounds;
        let (mut lower2, upper2) = cursor2.bounds;

        let capacity = (upper1 - lower1) + (upper2 - lower2);
        self.keys.reserve(capacity);
        self.offs.reserve(capacity);

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            self.merge_step(
                (cursor1.storage, &mut lower1, upper1),
                (cursor2.storage, &mut lower2, upper2),
            );
        }

        if lower1 < upper1 {
            self.copy_range(cursor1.storage, lower1, upper1);
        }
        if lower2 < upper2 {
            self.copy_range(cursor2.storage, lower2, upper2);
        }
    }

    fn push_merge_retain_keys<'a, F>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
        filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        let (mut lower1, upper1) = cursor1.bounds;
        let (mut lower2, upper2) = cursor2.bounds;

        let capacity = (upper1 - lower1) + (upper2 - lower2);
        self.keys.reserve(capacity);
        self.offs.reserve(capacity);

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            self.merge_step_retain_keys(
                (cursor1.storage, &mut lower1, upper1),
                (cursor2.storage, &mut lower2, upper2),
                filter,
            );
        }

        if lower1 < upper1 {
            self.copy_range_retain_keys(cursor1.storage, lower1, upper1, filter);
        }
        if lower2 < upper2 {
            self.copy_range_retain_keys(cursor2.storage, lower2, upper2, filter);
        }
    }
}

impl<K, L, O> TupleBuilder for LayerBuilder<K, L, O>
where
    K: DataTrait + ?Sized,
    L: TupleBuilder,
    O: OrdOffset,
{
    fn new(factories: &<Self::Trie as Trie>::Factories) -> Self {
        Self {
            factories: factories.clone(),
            keys: factories.keys.default_box(),
            offs: vec![O::zero()],
            vals: L::new(&factories.child),
        }
    }

    fn with_capacity(factories: &<Self::Trie as Trie>::Factories, cap: usize) -> Self {
        let mut offs = Vec::with_capacity(cap + 1);
        offs.push(O::zero());

        let mut keys = factories.keys.default_box();
        keys.reserve(cap);

        Self {
            factories: factories.clone(),
            keys,
            offs,
            vals: L::with_capacity(&factories.child, cap),
        }
    }

    fn reserve_tuples(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.offs.reserve(additional);
        self.vals.reserve_tuples(additional);
    }

    fn tuples(&self) -> usize {
        self.vals.tuples()
    }

    fn push_tuple<'a>(&mut self, (key, val): (&mut K, <L::Trie as Trie>::Item<'_>)) {
        // if first element, prior element finish, or different element, need to push
        // and maybe punctuate.
        if self.keys.is_empty()
            || !self.offs[self.keys.len()].is_zero()
            || &self.keys[self.keys.len() - 1] != key
        {
            if !self.keys.is_empty() && self.offs[self.keys.len()].is_zero() {
                self.offs[self.keys.len()] = O::from_usize(self.vals.boundary());
            }
            self.keys.push_val(key);
            self.offs.push(O::zero()); // <-- indicates "unfinished".
        }
        self.vals.push_tuple(val);
    }

    fn push_refs<'a>(&mut self, (key, val): (&K, <L::Trie as Trie>::ItemRef<'_>)) {
        // if first element, prior element finish, or different element, need to push
        // and maybe punctuate.
        if self.keys.is_empty()
            || !self.offs[self.keys.len()].is_zero()
            || &self.keys[self.keys.len() - 1] != (key)
        {
            if !self.keys.is_empty() && self.offs[self.keys.len()].is_zero() {
                self.offs[self.keys.len()] = O::from_usize(self.vals.boundary());
            }
            self.keys.push_ref(key);
            self.offs.push(O::zero()); // <-- indicates "unfinished".
        }
        self.vals.push_refs(val);
    }
}

/// A cursor with a child cursor that is updated as we move.
#[derive(Debug)]
pub struct LayerCursor<'s, K, L, O>
where
    K: DataTrait + ?Sized,
    O: 'static,
    L: Trie,
{
    pub storage: &'s Layer<K, L, O>,
    pos: isize,
    bounds: (usize, usize),
    /// The cursor for the trie layer below this one.
    pub child: L::Cursor<'s>,
}

impl<'s, K, L, O> Clone for LayerCursor<'s, K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie,
    O: OrdOffset,
    L::Cursor<'s>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            storage: self.storage,
            pos: self.pos,
            bounds: self.bounds,
            child: self.child.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.storage.clone_from(&source.storage);
        self.pos.clone_from(&source.pos);
        self.bounds.clone_from(&source.bounds);
        self.child.clone_from(&source.child);
    }
}

impl<'s, K, L, O> LayerCursor<'s, K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie,
    O: OrdOffset,
{
    pub fn seek_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool,
    {
        if self.valid() {
            self.pos +=
                self.storage
                    .keys
                    .advance_until(self.pos as usize, self.bounds.1, &predicate)
                    as isize;
        }

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    pub fn seek_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool,
    {
        if self.valid() {
            self.pos -=
                self.storage
                    .keys
                    .retreat_until(self.bounds.0, self.pos as usize, &predicate)
                    as isize;
        }

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }
}

impl<'s, K, L, O> Cursor<'s> for LayerCursor<'s, K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie,
    O: OrdOffset,
{
    type Key = K;

    type Item<'k> = &'k K
    where
        Self: 'k;

    type ValueCursor = L::Cursor<'s>;

    #[inline]
    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }

    #[inline]
    fn item(&self) -> Self::Item<'s> {
        &self.storage.keys[self.pos as usize]
    }

    fn values(&self) -> L::Cursor<'s> {
        if self.valid() {
            self.storage.vals.cursor_from(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            )
        } else {
            self.storage.vals.cursor_from(0, 0)
        }
    }

    fn step(&mut self) {
        self.pos += 1;

        if self.pos < self.bounds.1 as isize {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        } else {
            self.pos = self.bounds.1 as isize;
        }
    }

    fn seek(&mut self, key: &Self::Key) {
        if self.valid() {
            self.pos += self
                .storage
                .keys
                .advance_to(self.pos as usize, self.bounds.1, key) as isize;
        }

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    fn valid(&self) -> bool {
        self.pos >= self.bounds.0 as isize && self.pos < self.bounds.1 as isize
    }

    fn rewind(&mut self) {
        self.pos = self.bounds.0 as isize;

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    fn position(&self) -> usize {
        self.pos as usize
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        self.pos = lower as isize;
        self.bounds = (lower, upper);

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    fn step_reverse(&mut self) {
        self.pos -= 1;

        if self.pos >= self.bounds.0 as isize {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        } else {
            self.pos = self.bounds.0 as isize - 1;
        }
    }

    fn seek_reverse(&mut self, key: &Self::Key) {
        if self.valid() {
            self.pos -= self
                .storage
                .keys
                .retreat_to(self.bounds.0, self.pos as usize, key) as isize;
        }

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    fn fast_forward(&mut self) {
        self.pos = self.bounds.1 as isize - 1;

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }
}

impl<'a, K, L, O> Display for LayerCursor<'a, K, L, O>
where
    K: DataTrait + ?Sized,
    L: Trie,
    L::Cursor<'a>: Clone + Display,
    O: OrdOffset,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut cursor: LayerCursor<'_, K, L, O> = self.clone();

        while cursor.valid() {
            let key = cursor.item();
            key.fmt(f)?;
            let val_str = cursor.values().to_string();

            f.write_str(&indent(&val_str, "    "))?;
            cursor.step();
        }

        Ok(())
    }
}
