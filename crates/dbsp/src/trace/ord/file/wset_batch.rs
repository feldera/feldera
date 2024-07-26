use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    dynamic::{
        DataTrait, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory, LeanVec,
        WeightTrait, WeightTraitTyped, WithFactory,
    },
    storage::file::{
        reader::{Cursor as FileCursor, Error as ReaderError, Reader},
        writer::{Parameters, Writer1},
        Factories as FileFactories,
    },
    time::{Antichain, AntichainRef},
    trace::{
        cursor::{HasTimeDiffCursor, SingletonTimeDiffCursor},
        ord::{filter, merge_batcher::MergeBatcher},
        Batch, BatchFactories, BatchLocation, BatchReader, BatchReaderFactories, Builder, Cursor,
        Deserializer, Filter, Merger, Serializer, TimedBuilder, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries, Runtime,
};
use dyn_clone::clone_box;
use rand::{seq::index::sample, Rng};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    cmp::{min, Ordering},
    fmt::{self, Debug},
    ops::Neg,
    path::{Path, PathBuf},
};

pub struct FileWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    key_factory: &'static dyn Factory<K>,
    weight_factory: &'static dyn Factory<R>,
    file_factories: FileFactories<K, R>,
    keys_factory: &'static dyn Factory<DynVec<K>>,
    item_factory: &'static dyn Factory<DynPair<K, DynUnit>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, DynUnit, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>>,
}

impl<K, R> Clone for FileWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            key_factory: self.key_factory,
            weight_factory: self.weight_factory,
            file_factories: self.file_factories.clone(),
            keys_factory: self.keys_factory,
            item_factory: self.item_factory,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
        }
    }
}

impl<K, R> BatchReaderFactories<K, DynUnit, (), R> for FileWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<DynUnit>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            key_factory: WithFactory::<KType>::FACTORY,
            weight_factory: WithFactory::<RType>::FACTORY,
            keys_factory: WithFactory::<LeanVec<KType>>::FACTORY,
            file_factories: FileFactories::new::<KType, RType>(),
            item_factory: WithFactory::<Tup2<KType, ()>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, ()>, RType>>::FACTORY,
            weighted_items_factory: WithFactory::<LeanVec<Tup2<Tup2<KType, ()>, RType>>>::FACTORY,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.key_factory
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.keys_factory
    }

    fn val_factory(&self) -> &'static dyn Factory<DynUnit> {
        WithFactory::<()>::FACTORY
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.weight_factory
    }
}

impl<K, R> BatchFactories<K, DynUnit, (), R> for FileWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    //type BatchItemFactory = BatchItemFactory<K, (), K, R>;

    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, DynUnit>> {
        self.item_factory
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, DynUnit, R>> {
        self.weighted_item_factory
    }

    fn weighted_items_factory(
        &self,
    ) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>> {
        self.weighted_items_factory
    }
}

/// A batch of weighted tuples without values or times.
///
/// Each tuple in `FileWSet<K, R>` has key type `K`, value type `()`, weight
/// type `R`, and time type `()`.
#[derive(SizeOf)]
pub struct FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileWSetFactories<K, R>,
    #[size_of(skip)]
    file: Reader<(&'static K, &'static R, ())>,
    lower_bound: usize,
}

impl<K, R> Debug for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileWSet {{ lower_bound: {}, data: ", self.lower_bound)?;
        let mut cursor = self.cursor();
        let mut n_keys = 0;
        while cursor.key_valid() {
            if n_keys > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}(", cursor.key())?;
            let diff = cursor.weight();
            write!(f, "({diff:+?})")?;
            n_keys += 1;
            cursor.step_key();
        }
        write!(f, " }}")
    }
}

impl<K, R> Clone for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
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

impl<K, R> FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    pub fn len(&self) -> usize {
        self.file.n_rows(0) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// This is `#[cfg(test)]` only because it would be surprisingly expensive in
// production.
impl<Other, K, R> PartialEq<Other> for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    Other: BatchReader<Key = K, Val = DynUnit, R = R, Time = ()>,
{
    fn eq(&self, other: &Other) -> bool {
        use crate::trace::eq_batch;
        eq_batch(self, other)
    }
}

impl<K, R> Eq for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K, R> NumEntries for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.file.rows().len() as usize
    }

    fn num_entries_deep(&self) -> usize {
        self.num_entries_shallow()
    }
}

impl<K, R> NegByRef for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    fn neg_by_ref(&self) -> Self {
        let mut writer = Writer1::new(
            &self.factories.file_factories,
            &Runtime::storage(),
            Parameters::default(),
        )
        .unwrap();

        let mut cursor = self.cursor();
        while cursor.key_valid() {
            let diff = cursor.diff.neg_by_ref();
            writer.write0((cursor.key.as_ref(), diff.erase())).unwrap();
            cursor.step_key();
        }
        Self {
            factories: self.factories.clone(),
            file: writer.into_reader().unwrap(),
            lower_bound: 0,
        }
    }
}

impl<K, R> Neg for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        self.neg_by_ref()
    }
}

impl<K, R> AddAssignByRef for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        if !rhs.is_empty() {
            *self = self.merge(rhs);
        }
    }
}

impl<K, R> AddByRef for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Deserialize<FileWSet<K, R>, Deserializer>
    for ()
{
    fn deserialize(
        &self,
        _deserializer: &mut Deserializer,
    ) -> Result<FileWSet<K, R>, <Deserializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Archive for FileWSet<K, R> {
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        todo!()
    }
}
impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Serialize<Serializer> for FileWSet<K, R> {
    fn serialize(
        &self,
        _serializer: &mut Serializer,
    ) -> Result<Self::Resolver, <Serializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K, R> BatchReader for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Factories = FileWSetFactories<K, R>;
    type Key = K;
    type Val = DynUnit;
    type Time = ();
    type R = R;
    type Cursor<'s> = FileWSetCursor<'s, K, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        FileWSetCursor::new(self)
    }

    #[inline]
    fn key_count(&self) -> usize {
        self.file.n_rows(0) as usize - self.lower_bound
    }

    #[inline]
    fn len(&self) -> usize {
        self.key_count()
    }

    fn approximate_byte_size(&self) -> usize {
        self.file.byte_size().unwrap() as usize
    }

    #[inline]
    fn location(&self) -> BatchLocation {
        BatchLocation::Storage
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

impl<K, R> Batch for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FileWSetBuilder<K, R>;
    type Merger = FileWSetMerger<K, R>;

    fn begin_merge(&self, other: &Self, dst_hint: Option<BatchLocation>) -> Self::Merger {
        FileWSetMerger::new_merger(self, other, dst_hint)
    }

    fn recede_to(&mut self, _frontier: &()) {}

    fn persistent_id(&self) -> Option<PathBuf> {
        Some(self.file.path())
    }

    fn from_path(factories: &Self::Factories, path: &Path) -> Result<Self, ReaderError> {
        let any_factory0 = factories.file_factories.any_factories();
        let file = Reader::open(&[&any_factory0], &Runtime::storage(), path)?;

        Ok(Self {
            factories: factories.clone(),
            file,
            lower_bound: 0,
        })
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct FileWSetMerger<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileWSetFactories<K, R>,

    // Position in first batch.
    lower1: usize,
    // Position in second batch.
    lower2: usize,

    // Output so far.
    #[size_of(skip)]
    writer: Writer1<K, R>,
}

impl<K, R> Merger<K, DynUnit, (), R, FileWSet<K, R>> for FileWSetMerger<K, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new_merger(
        batch1: &FileWSet<K, R>,
        batch2: &FileWSet<K, R>,
        _dst_hint: Option<BatchLocation>,
    ) -> Self {
        Self {
            factories: batch1.factories.clone(),
            lower1: batch1.lower_bound,
            lower2: batch2.lower_bound,
            writer: Writer1::new(
                &batch1.factories.file_factories,
                &Runtime::storage(),
                Parameters::default(),
            )
            .unwrap(),
        }
    }

    fn done(self) -> FileWSet<K, R> {
        FileWSet {
            factories: self.factories.clone(),
            file: self.writer.into_reader().unwrap(),
            lower_bound: 0,
        }
    }

    fn work(
        &mut self,
        source1: &FileWSet<K, R>,
        source2: &FileWSet<K, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<DynUnit>>,
        fuel: &mut isize,
    ) {
        if !filter(value_filter, &()) {
            return;
        }

        let mut cursor1 = FileWSetCursor::new_from(source1, self.lower1);
        let mut cursor2 = FileWSetCursor::new_from(source2, self.lower2);
        let mut sum = self.factories.weight_factory.default_box();
        while cursor1.key_valid() && cursor2.key_valid() && *fuel > 0 {
            match cursor1.key.as_ref().cmp(cursor2.key.as_ref()) {
                Ordering::Less => {
                    if filter(key_filter, cursor1.key.as_ref()) {
                        self.writer
                            .write0((cursor1.key.as_ref(), cursor1.diff.as_ref()))
                            .unwrap();
                    }
                    *fuel -= 1;
                    cursor1.step_key();
                }
                Ordering::Equal => {
                    if filter(key_filter, cursor1.key.as_ref()) {
                        cursor1.diff.as_ref().add(cursor2.diff.as_ref(), &mut sum);
                        if !sum.is_zero() {
                            self.writer.write0((cursor1.key.as_ref(), &sum)).unwrap();
                        }
                    }
                    *fuel -= 2;
                    cursor1.step_key();
                    cursor2.step_key();
                }

                Ordering::Greater => {
                    if filter(key_filter, cursor2.key.as_ref()) {
                        self.writer
                            .write0((cursor2.key.as_ref(), cursor2.diff.as_ref()))
                            .unwrap();
                    }
                    *fuel -= 1;
                    cursor2.step_key();
                }
            }
        }

        while cursor1.key_valid() && *fuel > 0 {
            if filter(key_filter, cursor1.key.as_ref()) {
                self.writer
                    .write0((cursor1.key.as_ref(), cursor1.diff.as_ref()))
                    .unwrap();
            }
            *fuel -= 1;
            cursor1.step_key();
        }
        while cursor2.key_valid() && *fuel > 0 {
            if filter(key_filter, cursor2.key.as_ref()) {
                self.writer
                    .write0((cursor2.key.as_ref(), cursor2.diff.as_ref()))
                    .unwrap();
            }
            *fuel -= 1;
            cursor2.step_key();
        }
        self.lower1 = cursor1.cursor.absolute_position() as usize;
        self.lower2 = cursor2.cursor.absolute_position() as usize;
    }
}

type RawCursor<'s, K, R> = FileCursor<'s, K, R, (), (&'static K, &'static R, ())>;

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct FileWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    wset: &'s FileWSet<K, R>,
    cursor: RawCursor<'s, K, R>,
    key: Box<K>,
    pub(crate) diff: Box<R>,
    valid: bool,
}

impl<'s, K, R> Clone for FileWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            wset: self.wset,
            cursor: self.cursor.clone(),
            key: clone_box(&self.key),
            diff: clone_box(&self.diff),
            valid: self.valid,
        }
    }
}

impl<'s, K, R> FileWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new_from(wset: &'s FileWSet<K, R>, lower_bound: usize) -> Self {
        let cursor = wset
            .file
            .rows()
            .subset(lower_bound as u64..)
            .first()
            .unwrap();
        let mut key = wset.factories.key_factory.default_box();
        let mut diff = wset.factories.weight_factory.default_box();
        let valid = unsafe { cursor.item((&mut key, &mut diff)) }.is_some();

        Self {
            wset,
            cursor,
            key,
            diff,
            valid,
        }
    }

    fn new(wset: &'s FileWSet<K, R>) -> Self {
        Self::new_from(wset, wset.lower_bound)
    }

    fn move_key<F>(&mut self, op: F)
    where
        F: Fn(&mut RawCursor<'s, K, R>) -> Result<(), ReaderError>,
    {
        op(&mut self.cursor).unwrap();
        self.valid = unsafe { self.cursor.item((&mut self.key, &mut self.diff)) }.is_some();
    }
}

impl<'s, K, R> Cursor<K, DynUnit, (), R> for FileWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn key(&self) -> &K {
        debug_assert!(self.valid);
        self.key.as_ref()
    }

    fn val(&self) -> &DynUnit {
        &()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        if self.valid {
            logic(&(), self.diff.as_ref());
        }
    }

    fn map_times_through(&mut self, _upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.map_times(logic)
    }

    fn weight(&mut self) -> &R {
        debug_assert!(self.valid);
        self.diff.as_ref()
    }

    fn key_valid(&self) -> bool {
        self.valid
    }

    fn val_valid(&self) -> bool {
        self.valid
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
        self.valid = false;
    }

    fn seek_val(&mut self, _val: &DynUnit) {}

    fn seek_val_with(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(&()) {
            self.valid = false;
        }
    }

    fn rewind_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_first());
    }

    fn fast_forward_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_last());
    }

    fn rewind_vals(&mut self) {
        self.valid = true;
    }

    fn step_val_reverse(&mut self) {
        self.valid = false;
    }

    fn seek_val_reverse(&mut self, _val: &DynUnit) {}

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(&()) {
            self.valid = false;
        }
    }

    fn fast_forward_vals(&mut self) {
        self.valid = true;
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.wset.factories.weight_factory
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&DynUnit, &R)) {
        if self.valid {
            logic(&(), self.diff.as_ref())
        }
    }
}

impl<'s, K, R> HasTimeDiffCursor<K, DynUnit, (), R> for FileWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type TimeDiffCursor<'a> = SingletonTimeDiffCursor<'a, R>
    where
        Self: 'a;

    fn time_diff_cursor(&self) -> Self::TimeDiffCursor<'_> {
        SingletonTimeDiffCursor::new(self.val_valid().then(|| self.diff.as_ref()))
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct FileWSetBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileWSetFactories<K, R>,
    #[size_of(skip)]
    writer: Writer1<K, R>,
}

impl<K, R> Builder<FileWSet<K, R>> for FileWSetBuilder<K, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_builder(factories: &<FileWSet<K, R> as BatchReader>::Factories, _time: ()) -> Self {
        Self {
            factories: factories.clone(),
            writer: Writer1::new(
                &factories.file_factories,
                &Runtime::storage(),
                Parameters::default(),
            )
            .unwrap(),
        }
    }

    #[inline]
    fn with_capacity(
        factories: &<FileWSet<K, R> as BatchReader>::Factories,
        time: (),
        _capacity: usize,
    ) -> Self {
        Self::new_builder(factories, time)
    }

    #[inline]
    fn reserve(&mut self, _additional: usize) {}

    #[inline]
    fn push(&mut self, item: &mut WeightedItem<K, DynUnit, R>) {
        let (kv, r) = item.split();
        let (k, v) = kv.split();

        self.push_refs(k, v, r);
    }

    #[inline(never)]
    fn done(self) -> FileWSet<K, R> {
        FileWSet {
            factories: self.factories,
            file: self.writer.into_reader().unwrap(),
            lower_bound: 0,
        }
    }

    fn push_refs(&mut self, key: &K, _val: &DynUnit, weight: &R) {
        self.writer.write0((key, weight)).unwrap();
    }

    fn push_vals(&mut self, key: &mut K, _val: &mut DynUnit, weight: &mut R) {
        self.push_refs(key, &(), weight)
    }
}

impl<K, R> TimedBuilder<FileWSet<K, R>> for FileWSetBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn push_time(&mut self, key: &K, val: &DynUnit, _time: &(), weight: &R) {
        self.push_refs(key, val, weight);
    }

    fn done_with_bounds(self, _lower: Antichain<()>, _upper: Antichain<()>) -> FileWSet<K, R> {
        self.done()
    }
}
