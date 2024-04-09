use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    dynamic::{
        DataTrait, DynOpt, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory, LeanVec,
        WeightTrait, WeightTraitTyped, WithFactory,
    },
    storage::{
        backend::Backend,
        file::{
            reader::{Cursor as FileCursor, Error as ReaderError, Reader},
            writer::{Parameters, Writer2},
            Factories as FileFactories,
        },
    },
    time::AntichainRef,
    trace::{
        ord::merge_batcher::MergeBatcher, Batch, BatchFactories, BatchReader, BatchReaderFactories,
        Builder, Cursor, Filter, Merger, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries, Runtime,
};
use dyn_clone::clone_box;
use rand::{seq::index::sample, Rng};
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{cmp::min, ops::Neg};
use std::{cmp::Ordering, path::PathBuf};
use std::{
    fmt::{self, Debug},
    ops::{Add, AddAssign},
};

pub struct FileIndexedZSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    key_factory: &'static dyn Factory<K>,
    val_factory: &'static dyn Factory<V>,
    weight_factory: &'static dyn Factory<R>,
    keys_factory: &'static dyn Factory<DynVec<K>>,
    item_factory: &'static dyn Factory<DynPair<K, V>>,
    factories0: FileFactories<K, DynUnit>,
    factories1: FileFactories<V, R>,
    opt_key_factory: &'static dyn Factory<DynOpt<K>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, V, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>>,
}

impl<K, V, R> Clone for FileIndexedZSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            key_factory: self.key_factory,
            val_factory: self.val_factory,
            weight_factory: self.weight_factory,
            keys_factory: self.keys_factory,
            item_factory: self.item_factory,
            factories0: self.factories0.clone(),
            factories1: self.factories1.clone(),
            opt_key_factory: self.opt_key_factory,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
        }
    }
}

impl<K, V, R> BatchReaderFactories<K, V, (), R> for FileIndexedZSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            key_factory: WithFactory::<KType>::FACTORY,
            val_factory: WithFactory::<VType>::FACTORY,
            weight_factory: WithFactory::<RType>::FACTORY,
            keys_factory: WithFactory::<LeanVec<KType>>::FACTORY,
            item_factory: WithFactory::<Tup2<KType, VType>>::FACTORY,
            factories0: FileFactories::new::<KType, ()>(),
            factories1: FileFactories::new::<VType, RType>(),
            opt_key_factory: WithFactory::<Option<KType>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, VType>, RType>>::FACTORY,
            weighted_items_factory:
                WithFactory::<LeanVec<Tup2<Tup2<KType, VType>, RType>>>::FACTORY,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.key_factory
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.keys_factory
    }

    fn val_factory(&self) -> &'static dyn Factory<V> {
        self.val_factory
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.weight_factory
    }
}

impl<K, V, R> BatchFactories<K, V, (), R> for FileIndexedZSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, V>> {
        self.item_factory
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, V, R>> {
        self.weighted_item_factory
    }

    fn weighted_items_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>> {
        self.weighted_items_factory
    }
}

/// A batch of key-value weighted tuples without timing information.
///
/// Each tuple in `FileIndexedZSet<K, V, R>` has key type `K`, value type `V`,
/// weight type `R`, and time `()`.
pub struct FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FileIndexedZSetFactories<K, V, R>,
    #[allow(clippy::type_complexity)]
    file: Reader<Backend, (&'static K, &'static DynUnit, (&'static V, &'static R, ()))>,
    lower_bound: usize,
}

impl<K, V, R> Debug for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FileIndexedZSet {{ lower_bound: {}, data: ",
            self.lower_bound
        )?;
        let mut cursor = self.cursor();
        let mut n_keys = 0;
        while cursor.key_valid() {
            if n_keys > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}(", cursor.key())?;
            let mut n_values = 0;
            while cursor.val_valid() {
                if n_values > 0 {
                    write!(f, ", ")?;
                }
                let val = cursor.val();
                write!(f, "({val:?}")?;
                let diff = cursor.weight();
                write!(f, ", {diff:+?})")?;
                n_values += 1;
                cursor.step_val();
            }
            write!(f, ")")?;
            n_keys += 1;
            cursor.step_key();
        }
        write!(f, " }}")
    }
}

impl<K, V, R> Clone for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            file: self.file.clone(),
            lower_bound: self.lower_bound,
        }
    }
}

// This is `#[cfg(test)]` only because it would be surprisingly expensive in
// production.
#[cfg(test)]
impl<Other, K, V, R> PartialEq<Other> for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    Other: BatchReader<Key = K, Val = V, R = R, Time = ()>,
{
    fn eq(&self, other: &Other) -> bool {
        use crate::trace::eq_batch;
        eq_batch(self, other)
    }
}

#[cfg(test)]
impl<K, V, R> Eq for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K, V, R> NumEntries for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.file.rows().len() as usize
    }

    fn num_entries_deep(&self) -> usize {
        self.file.n_rows(1) as usize
    }
}

impl<K, V, R> NegByRef for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    #[inline]
    fn neg_by_ref(&self) -> Self {
        let mut writer = Writer2::new(
            &self.factories.factories0,
            &self.factories.factories1,
            &Runtime::storage(),
            Parameters::default(),
        )
        .unwrap();

        let mut cursor = self.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let diff = cursor.diff.neg_by_ref();
                writer.write1((cursor.val.as_ref(), diff.erase())).unwrap();
                cursor.step_val();
            }
            writer.write0((cursor.key.as_ref(), &())).unwrap();
            cursor.step_key();
        }
        Self {
            factories: self.factories.clone(),
            file: writer.into_reader().unwrap(),
            lower_bound: 0,
        }
    }
}

impl<K, V, R> Neg for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        self.neg_by_ref()
    }
}

impl<K, V, R> Add<Self> for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Output = Self;
    #[inline]

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

impl<K, V, R> AddAssign<Self> for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            *self = self.merge(&rhs);
        }
    }
}

impl<K, V, R> AddAssignByRef for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        if !rhs.is_empty() {
            *self = self.merge(rhs);
        }
    }
}

impl<K, V, R> AddByRef for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, V, R> BatchReader for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Factories = FileIndexedZSetFactories<K, V, R>;
    type Key = K;
    type Val = V;
    type Time = ();
    type R = R;
    type Cursor<'s> = FileIndexedZSetCursor<'s, K, V, R>
    where
        V: 's;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        FileIndexedZSetCursor::new(self)
    }

    #[inline]
    fn key_count(&self) -> usize {
        self.file.n_rows(0) as usize - self.lower_bound
    }

    #[inline]
    fn len(&self) -> usize {
        self.file.n_rows(1) as usize
    }

    #[inline]
    fn lower(&self) -> AntichainRef<'_, ()> {
        AntichainRef::new(&[()])
    }

    #[inline]
    fn upper(&self) -> AntichainRef<'_, ()> {
        AntichainRef::empty()
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        let mut cursor = self.file.rows().before();
        unsafe { cursor.advance_to_value_or_larger(lower_bound) }.unwrap();

        let lower_bound = cursor.absolute_position() as usize;
        if lower_bound > self.lower_bound {
            self.lower_bound = min(lower_bound, self.file.rows().len() as usize);
        }
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut DynVec<Self::Key>)
    where
        RG: Rng,
    {
        let size = self.key_count();
        let mut cursor = self.cursor();
        if sample_size >= size {
            output.reserve(size);

            while cursor.key_valid() {
                output.push_ref(cursor.key());
                cursor.step_key();
            }
        } else {
            output.reserve(sample_size);

            let mut indexes = sample(rng, size, sample_size).into_vec();
            indexes.sort_unstable();
            for index in indexes.into_iter() {
                cursor.move_key(|key_cursor| key_cursor.move_to_row(index as u64));
                output.push_ref(cursor.key());
            }
        }
    }
}

impl<K, V, R> Batch for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FileIndexedZSetBuilder<K, V, R>;
    type Merger = FileIndexedZSetMerger<K, V, R>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        FileIndexedZSetMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, _frontier: &()) {}

    fn dyn_empty(factories: &Self::Factories, _time: Self::Time) -> Self {
        Self {
            factories: factories.clone(),
            file: Reader::empty(&Runtime::storage()).unwrap(),
            lower_bound: 0,
        }
    }
    fn persistent_id(&self) -> Option<PathBuf> {
        Some(self.file.path())
    }
}

/// State for an in-progress merge.
pub struct FileIndexedZSetMerger<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FileIndexedZSetFactories<K, V, R>,

    // Position in first batch.
    lower1: usize,
    // Position in second batch.
    lower2: usize,

    // Output so far.
    writer: Writer2<Backend, K, DynUnit, V, R>,
}

impl<K, V, R> FileIndexedZSetMerger<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn copy_values_if(
        &mut self,
        cursor: &mut FileIndexedZSetCursor<K, V, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) {
        if filter(key_filter, cursor.key.as_ref()) {
            *fuel -= cursor.val_cursor.len() as isize;
            let mut n = 0;
            while cursor.val_valid() {
                let val = cursor.val();
                if filter(value_filter, val) {
                    let diff = cursor.diff.as_ref();
                    self.writer.write1((val, diff)).unwrap();
                    n += 1;
                }
                cursor.step_val();
            }
            if n > 0 {
                self.writer
                    .write0((cursor.key.as_ref(), ().erase()))
                    .unwrap();
            }
        } else {
            *fuel -= 1;
        }
        cursor.step_key();
    }

    fn copy_value(
        &mut self,
        cursor: &mut FileIndexedZSetCursor<K, V, R>,
        value_filter: &Option<Filter<V>>,
    ) -> u64 {
        let retval = if filter(value_filter, cursor.val.as_ref()) {
            self.writer
                .write1((cursor.val.as_ref(), cursor.diff.as_ref()))
                .unwrap();
            1
        } else {
            0
        };
        cursor.step_val();
        retval
    }

    fn merge_values<'a>(
        &mut self,
        cursor1: &mut FileIndexedZSetCursor<'a, K, V, R>,
        cursor2: &mut FileIndexedZSetCursor<'a, K, V, R>,
        value_filter: &Option<Filter<V>>,
    ) -> bool {
        let mut n = 0;
        let mut sum = self.factories.weight_factory.default_box();

        while cursor1.val_valid() && cursor2.val_valid() {
            let value1 = cursor1.val();
            let value2 = cursor2.val();
            let cmp = value1.cmp(value2);
            match cmp {
                Ordering::Less => {
                    n += self.copy_value(cursor1, value_filter);
                }
                Ordering::Equal => {
                    if filter(value_filter, value1) {
                        cursor1.diff.as_ref().add(cursor2.diff.as_ref(), &mut sum);
                        if !sum.is_zero() {
                            self.writer.write1((value1, &sum)).unwrap();
                            n += 1;
                        }
                    }
                    cursor1.step_val();
                    cursor2.step_val();
                }

                Ordering::Greater => {
                    n += self.copy_value(cursor2, value_filter);
                }
            }
        }

        while cursor1.val_valid() {
            n += self.copy_value(cursor1, value_filter);
        }
        while cursor2.val_valid() {
            n += self.copy_value(cursor2, value_filter);
        }
        n > 0
    }
}

impl<K, V, R> Merger<K, V, (), R, FileIndexedZSet<K, V, R>> for FileIndexedZSetMerger<K, V, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_merger(batch1: &FileIndexedZSet<K, V, R>, batch2: &FileIndexedZSet<K, V, R>) -> Self {
        Self {
            factories: batch1.factories.clone(),
            lower1: batch1.lower_bound,
            lower2: batch2.lower_bound,
            writer: Writer2::new(
                &batch1.factories.factories0,
                &batch1.factories.factories1,
                &Runtime::storage(),
                Parameters::default(),
            )
            .unwrap(),
        }
    }

    #[inline]
    fn done(self) -> FileIndexedZSet<K, V, R> {
        FileIndexedZSet {
            factories: self.factories.clone(),
            file: self.writer.into_reader().unwrap(),
            lower_bound: 0,
        }
    }

    fn work(
        &mut self,
        source1: &FileIndexedZSet<K, V, R>,
        source2: &FileIndexedZSet<K, V, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) {
        let mut cursor1 = FileIndexedZSetCursor::new_from(source1, self.lower1);
        let mut cursor2 = FileIndexedZSetCursor::new_from(source2, self.lower2);
        while cursor1.key_valid() && cursor2.key_valid() && *fuel > 0 {
            match cursor1.key.as_ref().cmp(cursor2.key.as_ref()) {
                Ordering::Less => {
                    self.copy_values_if(&mut cursor1, key_filter, value_filter, fuel);
                }
                Ordering::Equal => {
                    if filter(key_filter, cursor1.key.as_ref()) {
                        *fuel -= (cursor1.val_cursor.len() + cursor2.val_cursor.len()) as isize;
                        if self.merge_values(&mut cursor1, &mut cursor2, value_filter) {
                            self.writer
                                .write0((cursor1.key.as_ref(), ().erase()))
                                .unwrap();
                        }
                    } else {
                        *fuel -= 1;
                    }
                    cursor1.step_key();
                    cursor2.step_key();
                }

                Ordering::Greater => {
                    self.copy_values_if(&mut cursor2, key_filter, value_filter, fuel);
                }
            }
        }

        while cursor1.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut cursor1, key_filter, value_filter, fuel);
        }
        while cursor2.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut cursor2, key_filter, value_filter, fuel);
        }
        self.lower1 = cursor1.key_cursor.absolute_position() as usize;
        self.lower2 = cursor2.key_cursor.absolute_position() as usize;
    }
}

fn filter<T>(f: &Option<Filter<T>>, t: &T) -> bool
where
    T: ?Sized,
{
    f.as_ref().map_or(true, |f| f(t))
}

impl<K, V, R> SizeOf for FileIndexedZSetMerger<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

type KeyCursor<'s, K, V, R> = FileCursor<
    's,
    Backend,
    K,
    DynUnit,
    (&'static V, &'static R, ()),
    (&'static K, &'static DynUnit, (&'static V, &'static R, ())),
>;

type ValCursor<'s, K, V, R> =
    FileCursor<'s, Backend, V, R, (), (&'static K, &'static DynUnit, (&'static V, &'static R, ()))>;

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct FileIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    zset: &'s FileIndexedZSet<K, V, R>,

    key_cursor: KeyCursor<'s, K, V, R>,
    key: Box<K>,

    val_cursor: ValCursor<'s, K, V, R>,
    val: Box<V>,
    pub(crate) diff: Box<R>,
}

impl<'s, K, V, R> Clone for FileIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            zset: self.zset,
            key_cursor: self.key_cursor.clone(),
            key: clone_box(&self.key),
            val_cursor: self.val_cursor.clone(),
            val: clone_box(&self.val),
            diff: clone_box(&self.diff),
        }
    }
}

impl<'s, K, V, R> FileIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn new_from(zset: &'s FileIndexedZSet<K, V, R>, lower_bound: usize) -> Self {
        let key_cursor = zset
            .file
            .rows()
            .subset(lower_bound as u64..)
            .first()
            .unwrap();
        let mut key = zset.factories.key_factory.default_box();
        unsafe { key_cursor.key(&mut key) };

        let val_cursor = key_cursor.next_column().unwrap().first().unwrap();
        let mut val = zset.factories.val_factory.default_box();
        let mut diff = zset.factories.weight_factory.default_box();
        unsafe { val_cursor.item((&mut val, &mut diff)) };
        Self {
            zset,
            key_cursor,
            key,
            val_cursor,
            val,
            diff,
        }
    }

    pub fn new(zset: &'s FileIndexedZSet<K, V, R>) -> Self {
        Self::new_from(zset, zset.lower_bound)
    }

    fn move_key<F>(&mut self, op: F)
    where
        F: Fn(&mut KeyCursor<'s, K, V, R>) -> Result<(), ReaderError>,
    {
        op(&mut self.key_cursor).unwrap();
        unsafe { self.key_cursor.key(&mut self.key) };
        self.val_cursor = self.key_cursor.next_column().unwrap().first().unwrap();
        unsafe { self.val_cursor.item((&mut self.val, &mut self.diff)) };
    }

    fn move_val<F>(&mut self, op: F)
    where
        F: Fn(&mut ValCursor<'s, K, V, R>) -> Result<(), ReaderError>,
    {
        op(&mut self.val_cursor).unwrap();
        unsafe { self.val_cursor.item((&mut self.val, &mut self.diff)) };
    }
}

impl<'s, K, V, R> Cursor<K, V, (), R> for FileIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.zset.factories.weight_factory
    }

    fn key(&self) -> &K {
        debug_assert!(self.key_valid());
        self.key.as_ref()
    }

    fn val(&self) -> &V {
        debug_assert!(self.val_valid());
        self.val.as_ref()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        if self.val_valid() {
            logic(&(), self.diff.as_ref())
        }
    }

    fn map_times_through(&mut self, _upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.map_times(logic)
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R)) {
        while self.val_valid() {
            logic(self.val(), self.diff.as_ref());
            self.step_val();
        }
    }

    fn weight(&mut self) -> &R {
        debug_assert!(self.val_valid());
        self.diff.as_ref()
    }

    fn key_valid(&self) -> bool {
        self.key_cursor.has_value()
    }

    fn val_valid(&self) -> bool {
        self.val_cursor.has_value()
    }

    fn step_key(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_next());
    }

    fn step_key_reverse(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_prev());
    }

    fn seek_key(&mut self, key: &K) {
        self.move_key(|key_cursor| unsafe { key_cursor.advance_to_value_or_larger(key) });
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.move_key(|key_cursor| unsafe { key_cursor.seek_forward_until(predicate) });
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.move_key(|key_cursor| unsafe { key_cursor.seek_backward_until(predicate) });
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.move_key(|key_cursor| unsafe { key_cursor.rewind_to_value_or_smaller(key) });
    }

    fn step_val(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_next());
    }

    fn seek_val(&mut self, val: &V) {
        self.move_val(|val_cursor| unsafe { val_cursor.advance_to_value_or_larger(val) });
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.move_val(|val_cursor| unsafe { val_cursor.seek_forward_until(predicate) });
    }

    fn rewind_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_first());
    }

    fn fast_forward_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_last());
    }

    fn rewind_vals(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_first());
    }

    fn step_val_reverse(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_prev());
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.move_val(|val_cursor| unsafe { val_cursor.rewind_to_value_or_smaller(val) });
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.move_val(|val_cursor| unsafe { val_cursor.seek_backward_until(predicate) });
    }

    fn fast_forward_vals(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_last());
    }
}

/// A builder for batches from ordered update tuples.
pub struct FileIndexedZSetBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FileIndexedZSetFactories<K, V, R>,
    writer: Writer2<Backend, K, DynUnit, V, R>,
    key: Box<DynOpt<K>>,
}

impl<K, V, R> Builder<FileIndexedZSet<K, V, R>> for FileIndexedZSetBuilder<K, V, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_builder(factories: &FileIndexedZSetFactories<K, V, R>, _time: ()) -> Self {
        Self {
            factories: factories.clone(),
            writer: Writer2::new(
                &factories.factories0,
                &factories.factories1,
                &Runtime::storage(),
                Parameters::default(),
            )
            .unwrap(),
            key: factories.opt_key_factory.default_box(),
        }
    }

    #[inline]
    fn with_capacity(
        factories: &FileIndexedZSetFactories<K, V, R>,
        time: (),
        _capacity: usize,
    ) -> Self {
        Self::new_builder(factories, time)
    }

    #[inline]
    fn reserve(&mut self, _additional: usize) {}

    #[inline]
    fn push(&mut self, item: &mut DynPair<DynPair<K, V>, R>) {
        let (kv, r) = item.split();
        let (k, v) = kv.split();

        self.push_refs(k, v, r);
    }

    #[inline]
    fn push_refs(&mut self, key: &K, val: &V, weight: &R) {
        if let Some(cur_key) = self.key.get() {
            if cur_key != key {
                self.writer.write0((cur_key, ().erase())).unwrap();
                self.key.from_ref(key);
            }
        } else {
            self.key.from_ref(key);
        }
        self.writer.write1((val, weight)).unwrap();
    }

    #[inline]
    fn push_vals(&mut self, key: &mut K, val: &mut V, weight: &mut R) {
        self.push_refs(key, val, weight)
    }

    #[inline(never)]
    fn done(mut self) -> FileIndexedZSet<K, V, R> {
        if let Some(key) = self.key.get() {
            self.writer.write0((key, ().erase())).unwrap();
        }
        FileIndexedZSet {
            factories: self.factories,
            file: self.writer.into_reader().unwrap(),
            lower_bound: 0,
        }
    }
}

impl<K, V, R> SizeOf for FileIndexedZSetBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, V, R> SizeOf for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, V, R> Archive for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, V, R, S> Serialize<S> for FileIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, V, R, D> Deserialize<FileIndexedZSet<K, V, R>, D> for Archived<FileIndexedZSet<K, V, R>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileIndexedZSet<K, V, R>, D::Error> {
        unimplemented!();
    }
}
