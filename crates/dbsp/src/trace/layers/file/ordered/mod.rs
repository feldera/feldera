use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    dynamic::{
        DataTrait, DynOpt, DynUnit, Erase, Factory, WeightTrait, WeightTraitTyped, WithFactory,
    },
    storage::{
        backend::Backend,
        file::{
            reader::{Cursor as FileCursor, FallibleEq, Reader},
            writer::{Parameters, Writer2},
            Factories as FileFactories,
        },
    },
    trace::layers::{Builder, Cursor, MergeBuilder, Trie, TupleBuilder},
    DBData, DBWeight, NumEntries, Runtime,
};

use crate::dynamic::DynVec;
use dyn_clone::clone_box;
use rand::{seq::index::sample, Rng};
use std::{
    cmp::{min, Ordering},
    fmt::Debug,
    ops::{Add, AddAssign, Neg, Range},
    path::PathBuf,
};

//mod tests;

pub struct FileOrderedLayerFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    opt_key_factory: &'static dyn Factory<DynOpt<K>>,
    pub(crate) diff_factory: &'static dyn Factory<R>,
    pub(crate) factories0: FileFactories<K, DynUnit>,
    pub(crate) factories1: FileFactories<V, R>,
}

impl<K, V, R> FileOrderedLayerFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub(crate) fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            opt_key_factory: WithFactory::<Option<KType>>::FACTORY,
            diff_factory: WithFactory::<RType>::FACTORY,
            factories0: FileFactories::new::<KType, ()>(),
            factories1: FileFactories::new::<VType, RType>(),
        }
    }
}

impl<K, V, R> Clone for FileOrderedLayerFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            opt_key_factory: self.opt_key_factory,
            diff_factory: self.diff_factory,
            factories0: self.factories0.clone(),
            factories1: self.factories1.clone(),
        }
    }
}

pub struct FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub(crate) factories: FileOrderedLayerFactories<K, V, R>,
    #[allow(clippy::type_complexity)]
    file: Reader<Backend, (&'static K, &'static DynUnit, (&'static V, &'static R, ()))>,
    lower_bound: usize,
}

impl<K, V, R> Clone for FileOrderedLayer<K, V, R>
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

impl<K, V, R> FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut DynVec<K>)
    where
        RG: Rng,
    {
        let size = self.keys();
        let mut cursor = self.cursor();
        if sample_size >= size {
            output.reserve(size);

            while let Some(key) = cursor.get_current_key() {
                output.push_ref(key);
                cursor.step();
            }
        } else {
            output.reserve(sample_size);

            let mut indexes = sample(rng, size, sample_size).into_vec();
            indexes.sort_unstable();
            for index in indexes.into_iter() {
                cursor.move_to_row(index);
                output.push_ref(cursor.current_key());
            }
        }
    }

    /// Remove keys smaller than `lower_bound` from the batch.
    pub fn truncate_keys_below(&mut self, lower_bound: &K) {
        let mut cursor = self.file.rows().before();
        unsafe { cursor.advance_to_value_or_larger(lower_bound) }.unwrap();
        self.truncate_below(cursor.absolute_position() as usize);
    }

    pub fn path(&self) -> PathBuf {
        self.file.path()
    }
}

impl<K, V, R> FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn empty(factories: &FileOrderedLayerFactories<K, V, R>) -> Self {
        Self {
            factories: factories.clone(),
            file: Reader::empty(&Runtime::storage()).unwrap(),
            lower_bound: 0,
        }
    }
}

impl<K, V, R> Debug for FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FileOrderedLayer {{ lower_bound: {}, data: ",
            self.lower_bound
        )?;
        let mut cursor = self.cursor();
        let mut n_keys = 0;
        while let Some((key, mut values)) = cursor.get_current_key_and_values() {
            if n_keys > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{key:?}(")?;
            let mut n_values = 0;
            while let Some((value, diff)) = values.get_current_item() {
                if n_values > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "({value:?}, {diff:+?})")?;
                n_values += 1;
                values.step();
            }
            write!(f, ")")?;
            n_keys += 1;
            cursor.step();
        }
        write!(f, " }}")
    }
}

impl<K, V, R> NumEntries for FileOrderedLayer<K, V, R>
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

impl<K, V, R> PartialEq for FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.file.equals(&other.file).unwrap()
    }
}

impl<K, V, R> Eq for FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K, V, R> Trie for FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Item<'a> = (&'a mut K, (&'a mut V, &'a mut R));
    type ItemRef<'a> = (&'a K, (&'a V, &'a R));
    type Factories = FileOrderedLayerFactories<K, V, R>;

    type Cursor<'s> = FileOrderedCursor<'s, K, V, R> where K: 's, V: 's, R: 's;
    type MergeBuilder = FileOrderedMergeBuilder<K, V, R>;
    type TupleBuilder = FileOrderedTupleBuilder<K, V, R>;

    fn keys(&self) -> usize {
        self.file.rows().len() as usize - self.lower_bound
    }

    fn tuples(&self) -> usize {
        self.file.n_rows(1) as usize
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        FileOrderedCursor::new(self, lower as u64..upper as u64)
    }

    fn lower_bound(&self) -> usize {
        self.lower_bound
    }

    fn truncate_below(&mut self, lower_bound: usize) {
        if lower_bound > self.lower_bound {
            self.lower_bound = min(lower_bound, self.file.rows().len() as usize);
        }
    }
}

pub struct FileOrderedMergeBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FileOrderedLayerFactories<K, V, R>,
    writer: Writer2<Backend, K, DynUnit, V, R>,
}

impl<K, V, R> FileOrderedMergeBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn copy_values_if<KF, VF>(
        &mut self,
        cursor: &mut FileOrderedCursor<K, V, R>,
        key_filter: &KF,
        value_filter: &VF,
    ) where
        KF: Fn(&K) -> bool,
        VF: Fn(&V) -> bool,
    {
        let key = cursor.current_key();
        if key_filter(key) {
            let mut value_cursor = cursor.values();
            let mut n = 0;
            while value_cursor.valid() {
                let (value, diff) = value_cursor.item();
                if value_filter(value) {
                    self.writer.write1((value, diff)).unwrap();
                    n += 1;
                }
                value_cursor.step();
            }
            if n > 0 {
                self.writer.write0((key, ().erase())).unwrap();
            }
        }
        cursor.step();
    }

    fn copy_value<VF>(
        &mut self,
        cursor: &mut FileOrderedValueCursor<K, V, R>,
        value_filter: &VF,
    ) -> u64
    where
        VF: Fn(&V) -> bool,
    {
        let (value, diff) = cursor.current_item();
        let retval = if value_filter(value) {
            self.writer.write1((value, diff)).unwrap();
            1
        } else {
            0
        };
        cursor.step();
        retval
    }

    fn merge_values<'a, VF>(
        &mut self,
        mut cursor1: FileOrderedValueCursor<'a, K, V, R>,
        mut cursor2: FileOrderedValueCursor<'a, K, V, R>,
        value_filter: &VF,
    ) -> bool
    where
        VF: Fn(&V) -> bool,
    {
        let mut n = 0;
        let mut sum = self.factories.diff_factory.default_box();

        while cursor1.valid() && cursor2.valid() {
            let value1 = cursor1.current_value();
            let value2 = cursor2.current_value();
            let cmp = value1.cmp(value2);
            match cmp {
                Ordering::Less => {
                    n += self.copy_value(&mut cursor1, value_filter);
                }
                Ordering::Equal => {
                    if value_filter(value1) {
                        cursor1.current_diff().add(cursor2.current_diff(), &mut sum);
                        if !sum.is_zero() {
                            self.writer.write1((value1, &sum)).unwrap();
                            n += 1;
                        }
                    }
                    cursor1.step();
                    cursor2.step();
                }

                Ordering::Greater => {
                    n += self.copy_value(&mut cursor2, value_filter);
                }
            }
        }

        while cursor1.valid() {
            n += self.copy_value(&mut cursor1, value_filter);
        }
        while cursor2.valid() {
            n += self.copy_value(&mut cursor2, value_filter);
        }
        n > 0
    }

    pub fn push_merge_retain_values<'a, KF, VF>(
        &'a mut self,
        mut cursor1: <<FileOrderedMergeBuilder<K, V, R> as Builder>::Trie as Trie>::Cursor<'a>,
        mut cursor2: <<FileOrderedMergeBuilder<K, V, R> as Builder>::Trie as Trie>::Cursor<'a>,
        key_filter: &KF,
        value_filter: &VF,
    ) where
        KF: Fn(&K) -> bool,
        VF: Fn(&V) -> bool,
    {
        while cursor1.valid() && cursor2.valid() {
            let key1 = cursor1.current_key();
            let key2 = cursor2.current_key();
            match key1.cmp(key2) {
                Ordering::Less => {
                    self.copy_values_if(&mut cursor1, key_filter, value_filter);
                }
                Ordering::Equal => {
                    if key_filter(key1)
                        && self.merge_values(cursor1.values(), cursor2.values(), value_filter)
                    {
                        self.writer.write0((key1, ().erase())).unwrap();
                    }
                    cursor1.step();
                    cursor2.step();
                }

                Ordering::Greater => {
                    self.copy_values_if(&mut cursor2, key_filter, value_filter);
                }
            }
        }

        while cursor1.valid() {
            self.copy_values_if(&mut cursor1, key_filter, value_filter);
        }
        while cursor2.valid() {
            self.copy_values_if(&mut cursor2, key_filter, value_filter);
        }
    }
}

impl<K, V, R> Builder for FileOrderedMergeBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Trie = FileOrderedLayer<K, V, R>;

    fn boundary(&mut self) -> usize {
        self.keys()
    }

    fn done(self) -> Self::Trie {
        FileOrderedLayer {
            factories: self.factories.clone(),
            file: self.writer.into_reader().unwrap(),
            lower_bound: 0,
        }
    }
}

impl<K, V, R> MergeBuilder for FileOrderedMergeBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn with_capacity(other1: &Self::Trie, _other2: &Self::Trie) -> Self {
        Self {
            factories: other1.factories.clone(),
            writer: Writer2::new(
                &other1.factories.factories0,
                &other1.factories.factories1,
                &Runtime::storage(),
                Parameters::default(),
            )
            .unwrap(),
        }
    }

    fn reserve(&mut self, _additional: usize) {}

    fn keys(&self) -> usize {
        self.writer.n_rows() as usize
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
        let mut cursor = other.cursor_from(lower, upper);
        while cursor.valid() {
            self.copy_values_if(&mut cursor, filter, &|_| true);
        }
    }

    fn push_merge_retain_keys<'a, F>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
        key_filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        self.push_merge_retain_values(cursor1, cursor2, key_filter, &|_| true);
    }
}

pub struct FileOrderedTupleBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FileOrderedLayerFactories<K, V, R>,
    writer: Writer2<Backend, K, DynUnit, V, R>,
    key: Box<DynOpt<K>>,
}

impl<K, V, R> Builder for FileOrderedTupleBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Trie = FileOrderedLayer<K, V, R>;

    fn boundary(&mut self) -> usize {
        if let Some(key) = self.key.get() {
            self.writer.write0((key, ().erase())).unwrap();
            self.key.set_none()
        }
        self.writer.n_rows() as usize
    }

    fn done(mut self) -> Self::Trie {
        self.boundary();
        FileOrderedLayer {
            factories: self.factories.clone(),
            file: self.writer.into_reader().unwrap(),
            lower_bound: 0,
        }
    }
}

impl<K, V, R> TupleBuilder for FileOrderedTupleBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new(factories: &FileOrderedLayerFactories<K, V, R>) -> Self {
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

    fn with_capacity(factories: &FileOrderedLayerFactories<K, V, R>, _capacity: usize) -> Self {
        Self::new(factories)
    }

    fn reserve_tuples(&mut self, _additional: usize) {}

    fn tuples(&self) -> usize {
        self.writer.n_rows() as usize
    }

    fn push_tuple(&mut self, (key, (val, w)): (&mut K, (&mut V, &mut R))) {
        self.push_refs((key, (val, w)))
    }

    fn push_refs<'a>(&mut self, (key, (val, w)): (&K, (&V, &R))) {
        if let Some(cur_key) = self.key.get() {
            if cur_key != key {
                self.writer.write0((cur_key, ().erase())).unwrap();
                self.key.from_ref(key);
            }
        } else {
            self.key.from_ref(key);
        }
        self.writer.write1((val, w)).unwrap();
    }
}

#[derive(Debug)]
pub struct FileOrderedCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub(crate) storage: &'s FileOrderedLayer<K, V, R>,
    key: Box<K>,
    valid: bool,
    #[allow(clippy::type_complexity)]
    cursor: FileCursor<
        's,
        Backend,
        K,
        DynUnit,
        (&'static V, &'static R, ()),
        (&'static K, &'static DynUnit, (&'static V, &'static R, ())),
    >,
}

impl<'s, K, V, R> Clone for FileOrderedCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            storage: self.storage,
            key: clone_box(&self.key),
            valid: self.valid,
            cursor: self.cursor.clone(),
        }
    }
}

impl<'s, K, V, R> FileOrderedCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn new(storage: &'s FileOrderedLayer<K, V, R>, bounds: Range<u64>) -> Self {
        let cursor = storage.file.rows().subset(bounds).first().unwrap();

        let mut key = storage.factories.factories0.key_factory.default_box();
        let valid = unsafe { cursor.key(&mut key) }.is_some();

        Self {
            cursor,
            storage,
            key,
            valid,
        }
    }

    pub fn current_key(&self) -> &K {
        debug_assert!(self.valid);
        self.key.as_ref()
    }

    pub fn get_current_key(&mut self) -> Option<&K> {
        if self.valid {
            Some(&self.key)
        } else {
            None
        }
    }

    pub fn get_current_key_and_values(
        &mut self,
    ) -> Option<(&K, FileOrderedValueCursor<'s, K, V, R>)> {
        if self.valid {
            let values = self.values();
            Some((&self.key, values))
        } else {
            None
        }
    }

    fn move_cursor(
        &mut self,
        f: impl FnOnce(
            &mut FileCursor<
                's,
                Backend,
                K,
                DynUnit,
                (&'static V, &'static R, ()),
                (&'static K, &'static DynUnit, (&'static V, &'static R, ())),
            >,
        ),
    ) {
        f(&mut self.cursor);
        self.valid = unsafe { self.cursor.key(&mut self.key) }.is_some();
    }

    pub fn seek_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.move_cursor(|cursor| unsafe { cursor.seek_forward_until(predicate) }.unwrap());
    }

    pub fn seek_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.move_cursor(|cursor| unsafe { cursor.seek_backward_until(predicate) }.unwrap());
    }

    pub fn move_to_row(&mut self, row: usize) {
        self.move_cursor(|cursor| cursor.move_to_row(row as u64).unwrap());
    }
}

impl<'s, K, V, R> Cursor<'s> for FileOrderedCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Key = K;

    type Item<'k> = &'k K
        where
            Self: 'k;

    type ValueCursor = FileOrderedValueCursor<'s, K, V, R>;

    fn keys(&self) -> usize {
        self.cursor.len() as usize
    }

    fn item(&self) -> Self::Item<'_> {
        self.current_key()
    }

    fn values<'a>(&'a self) -> FileOrderedValueCursor<'s, K, V, R> {
        FileOrderedValueCursor::new(&self.storage.factories, &self.cursor)
    }

    fn step(&mut self) {
        self.move_cursor(|cursor| cursor.move_next().unwrap());
    }

    fn seek(&mut self, key: &Self::Key) {
        self.move_cursor(|cursor| unsafe { cursor.advance_to_value_or_larger(key) }.unwrap());
    }

    fn valid(&self) -> bool {
        self.valid
    }

    fn rewind(&mut self) {
        self.move_cursor(|cursor| cursor.move_first().unwrap());
    }

    fn position(&self) -> usize {
        self.cursor.absolute_position() as usize
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        self.move_cursor(|cursor| {
            *cursor = self
                .storage
                .file
                .rows()
                .subset(lower as u64..upper as u64)
                .first()
                .unwrap()
        });
    }

    fn step_reverse(&mut self) {
        self.move_cursor(|cursor| cursor.move_prev().unwrap());
    }

    fn seek_reverse(&mut self, key: &Self::Key) {
        self.move_cursor(|cursor| unsafe { cursor.rewind_to_value_or_smaller(key) }.unwrap());
    }

    fn fast_forward(&mut self) {
        self.move_cursor(|cursor| cursor.move_last().unwrap());
    }
}

// FIXME: it can be inefficient to heap-allocate temporary `val` and `diff`
// values every time this cursor is instantiated especially for collections that
// only have few values per key.  Can we re-design this to keep the value
// cursor as a field inside the key cursor, so it doesn't have to be recreated
// for each key?
#[derive(Debug)]
pub struct FileOrderedValueCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[allow(clippy::type_complexity)]
    cursor: FileCursor<
        's,
        Backend,
        V,
        R,
        (),
        (&'static K, &'static DynUnit, (&'static V, &'static R, ())),
    >,
    val: Box<V>,
    diff: Box<R>,
    valid: bool,
}

impl<'s, K, V, R> Clone for FileOrderedValueCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            cursor: self.cursor.clone(),
            val: clone_box(&self.val),
            diff: clone_box(&self.diff),
            valid: self.valid,
        }
    }
}

impl<'s, K, V, R> FileOrderedValueCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[allow(clippy::type_complexity)]
    pub fn new(
        factories: &FileOrderedLayerFactories<K, V, R>,
        cursor: &FileCursor<
            's,
            Backend,
            K,
            DynUnit,
            (&'static V, &'static R, ()),
            (&'static K, &'static DynUnit, (&'static V, &'static R, ())),
        >,
    ) -> Self {
        let cursor = cursor.next_column().unwrap().first().unwrap();
        let mut val = factories.factories1.key_factory.default_box();
        let mut diff = factories.diff_factory.default_box();

        let valid = unsafe { cursor.item((&mut val, &mut diff)) }.is_some();
        Self {
            cursor,
            val,
            diff,
            valid,
        }
    }

    pub fn current_value(&self) -> &V {
        debug_assert!(self.valid);
        &self.val
    }

    pub fn current_diff(&self) -> &R {
        debug_assert!(self.valid);
        &self.diff
    }

    pub fn current_item(&self) -> (&V, &R) {
        debug_assert!(self.valid);
        (&self.val, &self.diff)
    }

    pub fn get_current_item(&mut self) -> Option<(&V, &R)> {
        if self.valid {
            Some((&self.val, &self.diff))
        } else {
            None
        }
    }

    /*
    pub fn take_current_item(&mut self) -> Option<(V, R)> {
        let item = self.item.take();
        self.step();
        item
    }*/

    fn move_cursor(
        &mut self,
        f: impl Fn(
            &mut FileCursor<
                's,
                Backend,
                V,
                R,
                (),
                (&'static K, &'static DynUnit, (&'static V, &'static R, ())),
            >,
        ),
    ) {
        f(&mut self.cursor);
        self.valid = unsafe { self.cursor.item((&mut self.val, &mut self.diff)) }.is_some();
    }

    pub fn remaining_rows(&self) -> u64 {
        self.cursor.remaining_rows()
    }

    pub fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.move_cursor(|cursor| unsafe { cursor.seek_forward_until(&predicate) }.unwrap());
    }

    pub fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.move_cursor(|cursor| unsafe { cursor.seek_backward_until(&predicate) }.unwrap());
    }
}

impl<'s, K, V, R> Cursor<'s> for FileOrderedValueCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Key = V;

    type Item<'k> = (&'k V, &'k R)
        where
            Self: 'k;

    type ValueCursor = ();

    fn keys(&self) -> usize {
        self.cursor.len() as usize
    }

    fn item(&self) -> Self::Item<'_> {
        self.current_item()
    }

    fn values(&self) {}

    fn step(&mut self) {
        self.move_cursor(|cursor| cursor.move_next().unwrap());
    }

    fn seek(&mut self, key: &Self::Key) {
        self.move_cursor(|cursor| unsafe { cursor.advance_to_value_or_larger(key) }.unwrap());
    }

    fn valid(&self) -> bool {
        self.valid
    }

    fn rewind(&mut self) {
        self.move_cursor(|cursor| cursor.move_first().unwrap());
    }

    fn position(&self) -> usize {
        self.cursor.absolute_position() as usize
    }

    fn reposition(&mut self, _lower: usize, _upper: usize) {
        todo!()
    }

    fn step_reverse(&mut self) {
        self.move_cursor(|cursor| cursor.move_prev().unwrap());
    }

    fn seek_reverse(&mut self, key: &Self::Key) {
        self.move_cursor(|cursor| unsafe { cursor.rewind_to_value_or_smaller(key) }.unwrap());
    }

    fn fast_forward(&mut self) {
        self.move_cursor(|cursor| cursor.move_last().unwrap());
    }
}

impl<K, V, R> Add<Self> for FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
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

impl<K, V, R> AddAssign<Self> for FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            *self = self.merge(&rhs);
        }
    }
}

impl<K, V, R> AddAssignByRef for FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        if !other.is_empty() {
            *self = self.merge(other);
        }
    }
}

impl<K, V, R> AddByRef for FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, V, R> NegByRef for FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    fn neg_by_ref(&self) -> Self {
        let mut tuple_builder = <Self as Trie>::TupleBuilder::new(&self.factories);
        let mut cursor = self.cursor();
        while let Some((key, mut values)) = cursor.get_current_key_and_values() {
            while let Some((value, diff)) = values.get_current_item() {
                let diff = diff.neg_by_ref();
                tuple_builder.push_refs((key, (value, diff.erase())));
                values.step();
            }
            cursor.step()
        }
        tuple_builder.done()
    }
}

impl<K, V, R> Neg for FileOrderedLayer<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        let mut tuple_builder = <Self as Trie>::TupleBuilder::new(&self.factories);
        let mut cursor = self.cursor();
        while let Some((key, mut values)) = cursor.get_current_key_and_values() {
            while let Some((value, diff)) = values.get_current_item() {
                let diff = diff.neg_by_ref();
                tuple_builder.push_refs((key, (value, diff.erase())));
                values.step()
            }
            cursor.step()
        }
        tuple_builder.done()
    }
}
