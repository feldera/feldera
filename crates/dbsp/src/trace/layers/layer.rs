//! Implementation using ordered keys and exponential search.

//mod consumer;
//mod tests;

use rand::Rng;
use rkyv::{Archive, Serialize};

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
    ops::Neg,
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
#[derive(SizeOf, Archive, Serialize)]
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
}

impl<K, L, O> PartialEq for Layer<K, L, O>
where
    K: DataTrait + ?Sized,
    O: OrdOffset,
    L: Trie + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.keys.eq(&other.keys) && self.offs == other.offs && self.vals == other.vals
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
            .finish()
    }
}

impl<K: DataTrait + ?Sized, L, O: 'static> Layer<K, L, O>
where
    L: Trie,
{
    /// Create a new `OrderedLayer` from its component parts
    pub fn from_parts(
        factories: &LayerFactories<K, <L as Trie>::Factories>,
        mut keys: Box<DynVec<K>>,
        mut offs: Vec<O>,
        vals: L,
    ) -> Self
    where
        O: OrdOffset,
    {
        debug_assert_eq!(keys.len() + 1, offs.len());
        debug_assert_eq!(offs.last().unwrap().into_usize(), vals.keys());

        if keys.spare_capacity() >= keys.len() / 10 {
            keys.shrink_to_fit();
        }
        if offs.capacity() - offs.len() >= offs.len() / 10 {
            offs.shrink_to_fit();
        }

        Self {
            factories: factories.clone(),
            keys,
            offs,
            vals,
        }
    }

    /// Assume the invariants of the current builder
    ///
    /// # Safety
    ///
    /// Requires that `offs` has a length of `keys + 1`
    unsafe fn assume_invariants(&self) {
        assume(self.offs.len() == self.keys.len() + 1);
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
            .sample_slice(0, self.keys.len(), rng, sample_size, output);
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
    fn neg_by_ref(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            keys: self.keys.clone(),
            offs: self.offs.clone(),
            // We assume that offsets in `vals` don't change after negation;
            // otherwise `self.offs` will be invalid.
            vals: self.vals.neg_by_ref(),
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

    fn neg(self) -> Self {
        Self {
            factories: self.factories,
            keys: self.keys,
            offs: self.offs,
            // We assume that offsets in `vals` don't change after negation;
            // otherwise `self.offs` will be invalid.
            vals: self.vals.neg(),
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
    type Cursor<'s>
        = LayerCursor<'s, K, L, O>
    where
        K: 's,
        O: 's,
        L: 's;
    type MergeBuilder = LayerBuilder<K, L::MergeBuilder, O>;
    type TupleBuilder = LayerBuilder<K, L::TupleBuilder, O>;
    type LeafKey = L::LeafKey;

    fn keys(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.keys.len()
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
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
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
                self.copy_range(trie1, *lower1, *lower1 + step, map_func);
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
                    map_func,
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
                self.copy_range(trie2, *lower2, *lower2 + step, map_func);
                *lower2 += step;
            }
        }
    }

    pub fn merge_step_retain_keys<F>(
        &mut self,
        (trie1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (trie2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        filter: &F,
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
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
                self.copy_range_retain_keys(trie1, *lower1, *lower1 + step, filter, map_func);
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
                        map_func,
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
                self.copy_range_retain_keys(trie2, *lower2, *lower2 + step, filter, map_func);
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

        if self.keys.spare_capacity() >= self.keys.len() / 10 {
            self.keys.shrink_to_fit();
            self.offs.shrink_to_fit();
        }

        Layer {
            factories: self.factories,
            keys: self.keys,
            offs: self.offs,
            vals: self.vals.done(),
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
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
        fuel: &mut isize,
    ) {
        let starting_updates = source1.offs[*lower1] + source2.offs[*lower2];
        let mut effort = 0isize;

        // while both mergees are still active
        while *lower1 < upper1 && *lower2 < upper2 && effort < *fuel {
            self.merge_step(
                (source1, lower1, upper1),
                (source2, lower2, upper2),
                map_func,
            );
            effort = (source1.offs[*lower1] + source2.offs[*lower2] - starting_updates).into_usize()
                as isize;
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
                    *lower1 = self.copy_range_fueled(
                        source1,
                        *lower1,
                        upper1,
                        map_func,
                        remaining_fuel as usize,
                    );
                }
                if *lower2 < upper2 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower2 = self.copy_range_fueled(
                        source2,
                        *lower2,
                        upper2,
                        map_func,
                        remaining_fuel as usize,
                    );
                }
            }
        }

        effort = (source1.offs[*lower1] + source2.offs[*lower2] - starting_updates).into_usize()
            as isize;
        *fuel -= effort;
    }

    pub fn push_merge_retain_keys_fueled<F>(
        &mut self,
        (source1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (source2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        filter: &F,
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
        fuel: &mut isize,
    ) where
        F: Fn(&K) -> bool,
    {
        // We measure effort exerted by this function in terms of the number of input
        // values processed rather than the number of produced outputs.
        let starting_updates = source1.offs[*lower1] + source2.offs[*lower2];
        let mut effort = 0isize;

        // while both mergees are still active
        while *lower1 < upper1 && *lower2 < upper2 && effort < *fuel {
            self.merge_step_retain_keys(
                (source1, lower1, upper1),
                (source2, lower2, upper2),
                filter,
                map_func,
            );
            effort = (source1.offs[*lower1] + source2.offs[*lower2] - starting_updates).into_usize()
                as isize;
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
                        map_func,
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
                        map_func,
                        remaining_fuel as usize,
                    );
                }
            }
        }

        effort = (source1.offs[*lower1] + source2.offs[*lower2] - starting_updates).into_usize()
            as isize;

        *fuel -= effort;
    }

    /// Copy the key at `index` and its associated values from `other` to `self`, applying
    /// `map_func` to times along the way. If all values get consolidated away, the
    /// key is not copied.
    fn map_times_and_copy_key(
        &mut self,
        other: &<Self as Builder>::Trie,
        index: usize,
        map_func: &dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey),
    ) {
        let val_start = self.vals.keys();

        self.vals.copy_range(
            &other.vals,
            other.offs[index].into_usize(),
            other.offs[index + 1].into_usize(),
            Some(map_func),
        );

        let val_end = self.vals.keys();

        if val_end > val_start {
            self.keys.push_ref(&other.keys[index]);
            self.offs.push(O::from_usize(val_end));
        }
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
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
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

        if let Some(map_func) = map_func {
            // Process keys one by one only keeping those keys whose values don't
            // get compacted away.
            for i in lower..upper {
                self.map_times_and_copy_key(other, i, map_func);
            }
        } else {
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
                None,
            );
        }
        upper
    }

    fn copy_range_retain_keys_fueled<F>(
        &mut self,
        other: &<Self as Builder>::Trie,
        lower: usize,
        mut upper: usize,
        filter: &F,
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
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

        self.copy_range_retain_keys(other, lower, upper, filter, map_func);

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
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
        fuel: &mut isize,
    ) where
        KF: Fn(&K) -> bool,
        VF: Fn(&V) -> bool,
    {
        let starting_updates = source1.offs[*lower1] + source2.offs[*lower2];

        let mut effort = 0isize;

        // while both mergees are still active
        while *lower1 < upper1 && *lower2 < upper2 && effort < *fuel {
            self.merge_step_retain_values_fueled(
                (source1, lower1, upper1),
                (source2, lower2, upper2),
                key_filter,
                value_filter,
                map_func,
                usize::MAX,
            );
            effort = (source1.offs[*lower1] + source2.offs[*lower2] - starting_updates).into_usize()
                as isize;
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
                        map_func,
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
                        map_func,
                        remaining_fuel as usize,
                    );
                }
            }
        }

        effort = (source1.offs[*lower1] + source2.offs[*lower2] - starting_updates).into_usize()
            as isize;
        *fuel -= effort;
    }

    fn merge_step_retain_values_fueled<KF, VF>(
        &mut self,
        (trie1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (trie2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        key_filter: &KF,
        value_filter: &VF,
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
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
                    map_func,
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
                        .push_merge_retain_keys(cursor1, cursor2, value_filter, map_func);

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
                    map_func,
                    fuel,
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_range_retain_values_fueled<KF, VF>(
        &mut self,
        other: &<Self as Builder>::Trie,
        lower: usize,
        upper: usize,
        key_filter: &KF,
        value_filter: &VF,
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
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
                        map_func,
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
        keys.reserve_exact(other1.keys() + other2.keys());

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

    fn copy_range(
        &mut self,
        other: &Self::Trie,
        lower: usize,
        upper: usize,
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
    ) {
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        let other_basis = other.offs[lower];
        let self_basis = self.offs.last().copied().unwrap_or_else(|| O::zero());

        if let Some(map_func) = map_func {
            for i in lower..upper {
                self.map_times_and_copy_key(other, i, map_func);
            }
        } else {
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
                None,
            );
        }
    }

    fn copy_range_retain_keys<'a, F>(
        &mut self,
        other: &'a Self::Trie,
        lower: usize,
        upper: usize,
        filter: &F,
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        for index in lower..upper {
            if filter(&other.keys[index]) {
                if let Some(map_func) = map_func {
                    self.map_times_and_copy_key(other, index, map_func);
                } else {
                    self.keys.push_ref(&other.keys[index]);

                    self.vals.copy_range(
                        &other.vals,
                        other.offs[index].into_usize(),
                        other.offs[index + 1].into_usize(),
                        None,
                    );

                    self.offs.push(O::from_usize(self.vals.keys()));
                }
            }
        }
    }

    fn push_merge<'a>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
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
                map_func,
            );
        }

        if lower1 < upper1 {
            self.copy_range(cursor1.storage, lower1, upper1, map_func);
        }
        if lower2 < upper2 {
            self.copy_range(cursor2.storage, lower2, upper2, map_func);
        }
    }

    fn push_merge_retain_keys<'a, F>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
        filter: &F,
        map_func: Option<&dyn Fn(&mut <<Self as Builder>::Trie as Trie>::LeafKey)>,
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
                map_func,
            );
        }

        if lower1 < upper1 {
            self.copy_range_retain_keys(cursor1.storage, lower1, upper1, filter, map_func);
        }
        if lower2 < upper2 {
            self.copy_range_retain_keys(cursor2.storage, lower2, upper2, filter, map_func);
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
        keys.reserve_exact(cap);

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

impl<K, L, O> LayerCursor<'_, K, L, O>
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

    type Item<'k>
        = &'k K
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
