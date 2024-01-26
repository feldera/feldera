mod consumer;

pub use consumer::{FileOrderedLayerConsumer, FileOrderedLayerValues};
use feldera_storage::file::{
    reader::{Cursor as FileCursor, FallibleEq, Reader},
    writer::{Parameters, Writer2},
};
use rand::{seq::index::sample, Rng};

use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    trace::{
        layers::{Builder, Cursor, MergeBuilder, Trie, TupleBuilder},
        ord::file::StorageBackend,
    },
    DBData, DBWeight, NumEntries,
};
use std::{
    cmp::{min, Ordering},
    fmt::Debug,
    ops::{Add, AddAssign, Neg, Range},
};

#[derive(Clone)]
pub struct FileOrderedLayer<K, V, R>
where
    K: 'static,
    V: 'static,
    R: 'static,
{
    #[allow(clippy::type_complexity)]
    file: Reader<StorageBackend, (K, (), (V, R, ()))>,
    lower_bound: usize,
}

impl<K, V, R> FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    pub fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut Vec<K>)
    where
        K: DBData,
        R: DBWeight,
        RG: Rng,
    {
        let size = self.keys();
        let mut cursor = self.cursor();
        if sample_size >= size {
            output.reserve(size);

            while let Some(key) = cursor.take_current_key() {
                output.push(key);
            }
        } else {
            output.reserve(sample_size);

            let mut indexes = sample(rng, size, sample_size).into_vec();
            indexes.sort_unstable();
            for index in indexes.into_iter() {
                cursor.move_to_row(index);
                output.push(cursor.current_key().clone());
            }
        }
    }

    /// Remove keys smaller than `lower_bound` from the batch.
    pub fn truncate_keys_below(&mut self, lower_bound: &K)
    where
        K: DBData,
        R: DBWeight,
    {
        let mut cursor = self.file.rows().before();
        unsafe { cursor.advance_to_value_or_larger(lower_bound) }.unwrap();
        self.truncate_below(cursor.absolute_position() as usize);
    }
}

impl<K, V, R> FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    pub fn empty() -> Self {
        Self {
            file: Reader::empty(&StorageBackend::default_for_thread()).unwrap(),
            lower_bound: 0,
        }
    }
}

impl<K, V, R> Default for FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn default() -> Self {
        Self::empty()
    }
}

impl<K, V, R> Debug for FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FileOrderedLayer {{ lower_bound: {}, data: ",
            self.lower_bound
        )?;
        let mut cursor = self.cursor();
        let mut n_keys = 0;
        while let Some((key, mut values)) = cursor.take_current_key_and_values() {
            if n_keys > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{key:?}(")?;
            let mut n_values = 0;
            while let Some((value, diff)) = values.take_current_item() {
                if n_values > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "({value:?}, {diff:+?})")?;
                n_values += 1;
            }
            write!(f, ")")?;
            n_keys += 1;
        }
        write!(f, " }}")
    }
}

impl<K, V, R> NumEntries for FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
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
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn eq(&self, other: &Self) -> bool {
        self.file.equals(&other.file).unwrap()
    }
}

impl<K, V, R> Eq for FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
}

impl<K, V, R> Trie for FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Item = (K, (V, R));
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

pub struct FileOrderedMergeBuilder<K, V, R>(Writer2<StorageBackend, K, (), V, R>)
where
    K: DBData,
    V: DBData,
    R: DBWeight;

impl<K, V, R> FileOrderedMergeBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
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
                    self.0.write1((value, diff)).unwrap();
                    n += 1;
                }
                value_cursor.step();
            }
            if n > 0 {
                self.0.write0((&key, &())).unwrap();
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
            self.0.write1((value, diff)).unwrap();
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
                        let mut sum = cursor1.current_diff().clone();
                        sum.add_assign_by_ref(cursor2.current_diff());
                        if !sum.is_zero() {
                            self.0.write1((value1, &sum)).unwrap();
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
                        self.0.write0((key1, &())).unwrap();
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
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Trie = FileOrderedLayer<K, V, R>;

    fn boundary(&mut self) -> usize {
        self.keys()
    }

    fn done(self) -> Self::Trie {
        FileOrderedLayer {
            file: self.0.into_reader().unwrap(),
            lower_bound: 0,
        }
    }
}

impl<K, V, R> MergeBuilder for FileOrderedMergeBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn with_capacity(_other1: &Self::Trie, _other2: &Self::Trie) -> Self {
        Self::with_key_capacity(0)
    }

    fn with_key_capacity(_capacity: usize) -> Self {
        Self(Writer2::new(&StorageBackend::default_for_thread(), Parameters::default()).unwrap())
    }

    fn reserve(&mut self, _additional: usize) {}

    fn keys(&self) -> usize {
        self.0.n_rows() as usize
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
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    writer: Writer2<StorageBackend, K, (), V, R>,
    key: Option<K>,
}

impl<K, V, R> Builder for FileOrderedTupleBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Trie = FileOrderedLayer<K, V, R>;

    fn boundary(&mut self) -> usize {
        if let Some(key) = self.key.take() {
            self.writer.write0((&key, &())).unwrap()
        }
        self.writer.n_rows() as usize
    }

    fn done(mut self) -> Self::Trie {
        self.boundary();
        FileOrderedLayer {
            file: self.writer.into_reader().unwrap(),
            lower_bound: 0,
        }
    }
}

impl<K, V, R> TupleBuilder for FileOrderedTupleBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Item = (K, (V, R));

    fn new() -> Self {
        Self {
            writer: Writer2::new(&StorageBackend::default_for_thread(), Parameters::default())
                .unwrap(),
            key: None,
        }
    }

    fn with_capacity(_capacity: usize) -> Self {
        Self::new()
    }

    fn reserve_tuples(&mut self, _additional: usize) {}

    fn tuples(&self) -> usize {
        self.writer.n_rows() as usize
    }

    fn push_tuple(&mut self, (key, val): (K, (V, R))) {
        if let Some(ref cur_key) = self.key {
            if *cur_key != key {
                self.writer.write0((cur_key, &())).unwrap();
                self.key = Some(key);
            }
        } else {
            self.key = Some(key);
        }
        self.writer.write1((&val.0, &val.1)).unwrap();
    }
}

#[derive(Debug, Clone)]
pub struct FileOrderedCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    storage: &'s FileOrderedLayer<K, V, R>,
    key: Option<K>,
    #[allow(clippy::type_complexity)]
    cursor: FileCursor<'s, StorageBackend, K, (), (V, R, ()), (K, (), (V, R, ()))>,
}

impl<'s, K, V, R> FileOrderedCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    pub fn new(storage: &'s FileOrderedLayer<K, V, R>, bounds: Range<u64>) -> Self {
        let cursor = storage.file.rows().subset(bounds).first().unwrap();
        let key = unsafe { cursor.key() };
        Self {
            cursor,
            storage,
            key,
        }
    }

    pub fn current_key(&self) -> &K {
        self.key.as_ref().unwrap()
    }

    pub fn take_current_key(&mut self) -> Option<K> {
        let key = self.key.take();
        self.step();
        key
    }

    pub fn take_current_key_and_values(
        &mut self,
    ) -> Option<(K, FileOrderedValueCursor<'s, K, V, R>)> {
        if let Some(key) = self.key.take() {
            let values = self.values();
            self.step();
            Some((key, values))
        } else {
            None
        }
    }

    fn move_cursor(
        &mut self,
        f: impl FnOnce(&mut FileCursor<'s, StorageBackend, K, (), (V, R, ()), (K, (), (V, R, ()))>),
    ) {
        f(&mut self.cursor);
        self.key = unsafe { self.cursor.key() };
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
    K: DBData,
    V: DBData,
    R: DBWeight,
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
        FileOrderedValueCursor::new(&self.cursor)
    }

    fn step(&mut self) {
        self.move_cursor(|cursor| cursor.move_next().unwrap());
    }

    fn seek(&mut self, key: &Self::Key) {
        self.move_cursor(|cursor| unsafe { cursor.advance_to_value_or_larger(key) }.unwrap());
    }

    fn valid(&self) -> bool {
        self.cursor.has_value()
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

#[derive(Debug, Clone)]
pub struct FileOrderedValueCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    item: Option<(V, R)>,
    #[allow(clippy::type_complexity)]
    cursor: FileCursor<'s, StorageBackend, V, R, (), (K, (), (V, R, ()))>,
}

impl<'s, K, V, R> FileOrderedValueCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[allow(clippy::type_complexity)]
    pub fn new(
        cursor: &FileCursor<'s, StorageBackend, K, (), (V, R, ()), (K, (), (V, R, ()))>,
    ) -> Self {
        let cursor = cursor.next_column().unwrap().first().unwrap();
        let item = unsafe { cursor.item() };
        Self { cursor, item }
    }

    pub fn current_value(&self) -> &V {
        &self.item.as_ref().unwrap().0
    }

    pub fn current_diff(&self) -> &R {
        &self.item.as_ref().unwrap().1
    }

    pub fn current_item(&self) -> (&V, &R) {
        let item = self.item.as_ref().unwrap();
        (&item.0, &item.1)
    }

    pub fn take_current_item(&mut self) -> Option<(V, R)> {
        let item = self.item.take();
        self.step();
        item
    }

    fn move_cursor(
        &mut self,
        f: impl Fn(&mut FileCursor<'s, StorageBackend, V, R, (), (K, (), (V, R, ()))>),
    ) {
        f(&mut self.cursor);
        self.item = unsafe { self.cursor.item() };
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
    K: DBData,
    V: DBData,
    R: DBWeight,
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
        self.cursor.has_value()
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
    K: DBData,
    V: DBData,
    R: DBWeight,
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
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            *self = self.merge(&rhs);
        }
    }
}

impl<K, V, R> AddAssignByRef for FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        if !other.is_empty() {
            *self = self.merge(other);
        }
    }
}

impl<K, V, R> AddByRef for FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, V, R> NegByRef for FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight + NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        let mut tuple_builder = <Self as Trie>::TupleBuilder::new();
        let mut cursor = self.cursor();
        while let Some((key, mut values)) = cursor.take_current_key_and_values() {
            while let Some((value, diff)) = values.take_current_item() {
                tuple_builder.push_tuple((key.clone(), (value, diff.neg_by_ref())));
            }
        }
        tuple_builder.done()
    }
}

impl<K, V, R> Neg for FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight + Neg<Output = R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        let mut tuple_builder = <Self as Trie>::TupleBuilder::new();
        let mut cursor = self.cursor();
        while let Some((key, mut values)) = cursor.take_current_key_and_values() {
            while let Some((value, diff)) = values.take_current_item() {
                tuple_builder.push_tuple((key.clone(), (value, -diff)));
            }
        }
        tuple_builder.done()
    }
}
