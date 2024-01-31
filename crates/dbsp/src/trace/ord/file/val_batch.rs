use std::{
    cmp::Ordering,
    fmt::{Debug, Display, Formatter},
    marker::PhantomData,
};

use feldera_storage::{
    backend::{StorageControl, StorageExecutor, StorageRead},
    file::{
        reader::{ColumnSpec, Cursor as FileCursor, Reader},
        writer::{Parameters, Writer2},
    },
};
use itertools::Itertools;
use rand::{seq::index::sample, Rng};
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;

use crate::{
    algebra::HasZero,
    time::{Antichain, AntichainRef},
    trace::{
        ord::merge_batcher::MergeBatcher, Batch, BatchReader, Builder, Consumer, Cursor, Filter,
        Merger, ValueConsumer,
    },
    DBData, DBTimestamp, DBWeight, NumEntries, Rkyv,
};

use super::StorageBackend;

type RawValBatch<K, V, T, R> = Reader<StorageBackend, (K, (), (V, Vec<(T, R)>, ()))>;

type RawKeyCursor<'s, K, V, T, R> =
    FileCursor<'s, StorageBackend, K, (), (V, Vec<(T, R)>, ()), (K, (), (V, Vec<(T, R)>, ()))>;

type RawValCursor<'s, K, V, T, R> =
    FileCursor<'s, StorageBackend, V, Vec<(T, R)>, (), (K, (), (V, Vec<(T, R)>, ()))>;

/// An immutable collection of update tuples, from a contiguous interval of
/// logical times.
#[derive(Clone)]
pub struct FileValBatch<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    pub file: RawValBatch<K, V, T, R>,
    pub lower_bound: usize,
    pub lower: Antichain<T>,
    pub upper: Antichain<T>,
}

impl<K, V, T, R> NumEntries for FileValBatch<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.file.rows().len() as usize
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.file.n_rows(1) as usize
    }
}

impl<K, V, T, R> Display for FileValBatch<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(f, "lower: {:?}, upper: {:?}\n", self.lower, self.upper)
    }
}

impl<K, V, T, R> BatchReader for FileValBatch<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type R = R;

    type Cursor<'s> = FileValCursor<'s, K, V, T, R>;

    type Consumer = FileValConsumer<K, V, T, R>;

    fn cursor(&self) -> Self::Cursor<'_> {
        FileValCursor::new(self)
    }

    fn consumer(self) -> Self::Consumer {
        todo!()
    }

    fn key_count(&self) -> usize {
        self.file.rows().len() as usize - self.lower_bound
    }

    fn len(&self) -> usize {
        self.file.n_rows(1) as usize
    }

    fn lower(&self) -> AntichainRef<'_, T> {
        self.lower.as_ref()
    }

    fn upper(&self) -> AntichainRef<'_, T> {
        self.upper.as_ref()
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        let mut cursor = self.file.rows().before();
        unsafe { cursor.advance_to_value_or_larger(lower_bound) }.unwrap();
        let lower_bound = cursor.absolute_position() as usize;
        if lower_bound > self.lower_bound {
            self.lower_bound = lower_bound;
        }
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut Vec<Self::Key>)
    where
        RG: Rng,
    {
        let size = self.key_count();
        let mut cursor = self.file.rows().first().unwrap();
        if sample_size >= size {
            output.reserve(size);
            while let Some(key) = unsafe { cursor.key() } {
                output.push(key);
                cursor.move_next().unwrap();
            }
        } else {
            output.reserve(sample_size);

            let mut indexes = sample(rng, size, sample_size).into_vec();
            indexes.sort_unstable();
            for index in indexes {
                cursor.move_to_row(index as u64).unwrap();
                output.push(unsafe { cursor.key() }.unwrap());
            }
        }
    }
}

impl<K, V, T, R> Batch for FileValBatch<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    type Item = (K, V);
    type Batcher = MergeBatcher<(K, V), T, R, Self>;
    type Builder = FileValBuilder<K, V, T, R>;
    type Merger = FileValMerger<K, V, T, R>;

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
        FileValMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, frontier: &T) {
        // Nothing to do if the batch is entirely before the frontier.
        if !self.upper().less_equal(frontier) {
            let mut writer =
                Writer2::new(&StorageBackend::default_for_thread(), Parameters::default()).unwrap();
            let mut key_cursor = self.file.rows().first().unwrap();
            while key_cursor.has_value() {
                let mut val_cursor = key_cursor.next_column().unwrap().first().unwrap();
                let mut n_vals = 0;
                while val_cursor.has_value() {
                    let td = unsafe { val_cursor.aux() }.unwrap();
                    let td = recede_times(td, frontier);
                    if !td.is_empty() {
                        let val = unsafe { val_cursor.key() }.unwrap();
                        writer.write1((&val, &td)).unwrap();
                        n_vals += 1;
                    }
                    val_cursor.move_next().unwrap();
                }
                if n_vals > 0 {
                    let key = unsafe { key_cursor.key() }.unwrap();
                    writer.write0((&key, &())).unwrap();
                }
                key_cursor.move_next().unwrap();
            }
            self.file = writer.into_reader().unwrap();
        }
    }
}

fn recede_times<T, R>(mut td: Vec<(T, R)>, frontier: &T) -> Vec<(T, R)>
where
    T: DBTimestamp,
    R: DBWeight,
{
    for (time, _diff) in &mut td {
        time.meet_assign(frontier);
    }
    td.sort_unstable();

    td.iter()
        .cloned()
        .coalesce(|prev, cur| {
            let (prev_time, prev_diff) = prev;
            let (cur_time, cur_diff) = cur;
            if prev_time == cur_time {
                let mut sum = prev_diff.clone();
                sum.add_assign_by_ref(&cur_diff);
                Ok((cur_time, sum))
            } else {
                Err(((prev_time, prev_diff), (cur_time, cur_diff)))
            }
        })
        .filter(|(_time, diff)| !diff.is_zero())
        .collect()
}

/// State for an in-progress merge.
pub struct FileValMerger<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    result: Option<RawValBatch<K, V, T, R>>,
    lower: Antichain<T>,
    upper: Antichain<T>,
}

fn include<K>(x: &K, filter: &Option<Filter<K>>) -> bool {
    match filter {
        Some(filter) => filter(x),
        None => true,
    }
}

fn read_filtered<S, K, A, N, T>(
    cursor: &mut FileCursor<S, K, A, N, T>,
    filter: &Option<Filter<K>>,
) -> Option<K>
where
    S: StorageRead + StorageControl + StorageExecutor,
    K: Rkyv + Debug,
    A: Rkyv,
    T: ColumnSpec,
{
    while cursor.has_value() {
        let key = unsafe { cursor.key() }.unwrap();
        if include(&key, filter) {
            return Some(key);
        }
        cursor.move_next().unwrap();
    }
    None
}

fn merge_times<T, R>(mut a: &[(T, R)], mut b: &[(T, R)]) -> Vec<(T, R)>
where
    T: DBTimestamp,
    R: DBWeight,
{
    let mut output = Vec::with_capacity(a.len() + b.len());
    while !a.is_empty() && !b.is_empty() {
        match a[0].0.cmp(&b[0].0) {
            Ordering::Less => {
                output.push(a[0].clone());
                a = &a[1..];
            }
            Ordering::Equal => {
                let mut sum = a[0].1.clone();
                sum.add_assign_by_ref(&b[0].1);
                if !sum.is_zero() {
                    output.push((a[0].0.clone(), sum));
                }
                a = &a[1..];
                b = &b[1..];
            }
            Ordering::Greater => {
                output.push(b[0].clone());
                b = &b[1..];
            }
        }
    }
    output.extend_from_slice(a);
    output.extend_from_slice(b);
    output
}

impl<K, V, T, R> FileValMerger<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn copy_values_if(
        output: &mut Writer2<StorageBackend, K, (), V, Vec<(T, R)>>,
        key: &K,
        key_cursor: &mut RawKeyCursor<'_, K, V, T, R>,
        value_filter: &Option<Filter<V>>,
    ) {
        let mut value_cursor = key_cursor.next_column().unwrap().first().unwrap();
        let mut n = 0;
        while value_cursor.has_value() {
            let value = unsafe { value_cursor.key() }.unwrap();
            if include(&value, value_filter) {
                let aux = unsafe { value_cursor.aux() }.unwrap();
                output.write1((&value, &aux)).unwrap();
                n += 1;
            }
            value_cursor.move_next().unwrap();
        }
        if n > 0 {
            output.write0((&key, &())).unwrap();
        }
        key_cursor.move_next().unwrap();
    }

    fn copy_value(
        output: &mut Writer2<StorageBackend, K, (), V, Vec<(T, R)>>,
        cursor: &mut RawValCursor<'_, K, V, T, R>,
        value: &V,
    ) {
        let td = unsafe { cursor.aux() }.unwrap();
        output.write1((value, &td)).unwrap();
        cursor.move_next().unwrap();
    }

    fn merge_values(
        output: &mut Writer2<StorageBackend, K, (), V, Vec<(T, R)>>,
        cursor1: &mut RawValCursor<'_, K, V, T, R>,
        cursor2: &mut RawValCursor<'_, K, V, T, R>,
        value_filter: &Option<Filter<V>>,
    ) -> bool {
        let mut n = 0;
        loop {
            let Some(value1) = read_filtered(cursor1, value_filter) else {
                while let Some(value2) = read_filtered(cursor2, value_filter) {
                    Self::copy_value(output, cursor2, &value2);
                    n += 1;
                }
                return n > 0;
            };
            let Some(value2) = read_filtered(cursor2, value_filter) else {
                while let Some(value1) = read_filtered(cursor1, value_filter) {
                    Self::copy_value(output, cursor1, &value1);
                    n += 1;
                }
                return n > 0;
            };
            match value1.cmp(&value2) {
                Ordering::Less => Self::copy_value(output, cursor1, &value1),
                Ordering::Equal => {
                    let mut td1 = unsafe { cursor1.aux() }.unwrap();
                    td1.sort_unstable();
                    let mut td2 = unsafe { cursor2.aux() }.unwrap();
                    td2.sort_unstable();
                    let td = merge_times(&td1, &td2);
                    cursor1.move_next().unwrap();
                    cursor2.move_next().unwrap();
                    if td.is_empty() {
                        continue;
                    }
                    output.write1((&value1, &td)).unwrap();
                }
                Ordering::Greater => Self::copy_value(output, cursor2, &value2),
            }
            n += 1;
        }
    }

    fn merge(
        source1: &FileValBatch<K, V, T, R>,
        source2: &FileValBatch<K, V, T, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
    ) -> RawValBatch<K, V, T, R> {
        let mut output =
            Writer2::new(&StorageBackend::default_for_thread(), Parameters::default()).unwrap();
        let mut cursor1 = source1.file.rows().nth(source1.lower_bound as u64).unwrap();
        let mut cursor2 = source2.file.rows().nth(source2.lower_bound as u64).unwrap();
        loop {
            let Some(key1) = read_filtered(&mut cursor1, key_filter) else {
                while let Some(key2) = read_filtered(&mut cursor2, key_filter) {
                    Self::copy_values_if(&mut output, &key2, &mut cursor2, value_filter);
                }
                break;
            };
            let Some(key2) = read_filtered(&mut cursor2, key_filter) else {
                while let Some(key1) = read_filtered(&mut cursor1, key_filter) {
                    Self::copy_values_if(&mut output, &key1, &mut cursor1, value_filter);
                }
                break;
            };
            match key1.cmp(&key2) {
                Ordering::Less => {
                    Self::copy_values_if(&mut output, &key1, &mut cursor1, value_filter);
                }
                Ordering::Equal => {
                    if Self::merge_values(
                        &mut output,
                        &mut cursor1.next_column().unwrap().first().unwrap(),
                        &mut cursor2.next_column().unwrap().first().unwrap(),
                        value_filter,
                    ) {
                        output.write0((&key1, &())).unwrap();
                    }
                    cursor1.move_next().unwrap();
                    cursor2.move_next().unwrap();
                }

                Ordering::Greater => {
                    Self::copy_values_if(&mut output, &key2, &mut cursor2, value_filter);
                }
            }
        }
        output.into_reader().unwrap()
    }
}

impl<K, V, T, R> Merger<K, V, T, R, FileValBatch<K, V, T, R>> for FileValMerger<K, V, T, R>
where
    Self: SizeOf,
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn new_merger(batch1: &FileValBatch<K, V, T, R>, batch2: &FileValBatch<K, V, T, R>) -> Self {
        Self {
            result: None,
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
        }
    }

    fn done(mut self) -> FileValBatch<K, V, T, R> {
        FileValBatch {
            file: self
                .result
                .take()
                .unwrap_or(Reader::empty(&StorageBackend::default_for_thread()).unwrap()),
            lower_bound: 0,
            lower: self.lower,
            upper: self.upper,
        }
    }

    fn work(
        &mut self,
        source1: &FileValBatch<K, V, T, R>,
        source2: &FileValBatch<K, V, T, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) {
        debug_assert!(*fuel > 0);
        if self.result.is_none() {
            self.result = Some(Self::merge(source1, source2, key_filter, value_filter));
        }
    }
}

impl<K, V, T, R> SizeOf for FileValMerger<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

#[derive(Debug, SizeOf, Clone)]
pub struct FileValCursor<'s, K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    key_cursor: RawKeyCursor<'s, K, V, T, R>,
    val_cursor: RawValCursor<'s, K, V, T, R>,
    key: Option<K>,
    val: Option<V>,
}

impl<'s, K, V, T, R> FileValCursor<'s, K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn new(batch: &'s FileValBatch<K, V, T, R>) -> Self {
        let key_cursor = batch
            .file
            .rows()
            .subset(batch.lower_bound as u64..)
            .first()
            .unwrap();
        let val_cursor = key_cursor.next_column().unwrap().first().unwrap();
        let key = unsafe { key_cursor.key() };
        let val = unsafe { val_cursor.key() };
        Self {
            key_cursor,
            val_cursor,
            key,
            val,
        }
    }
    fn move_key<F>(&mut self, op: F)
    where
        F: Fn(&mut RawKeyCursor<'s, K, V, T, R>),
    {
        op(&mut self.key_cursor);
        self.val_cursor = self.key_cursor.next_column().unwrap().first().unwrap();
        self.key = unsafe { self.key_cursor.key() };
        self.val = unsafe { self.val_cursor.key() };
    }
    fn move_val<F>(&mut self, op: F)
    where
        F: Fn(&mut RawValCursor<'s, K, V, T, R>),
    {
        op(&mut self.val_cursor);
        self.val = unsafe { self.val_cursor.key() };
    }
    fn times(&self) -> Vec<(T, R)> {
        unsafe { self.val_cursor.aux() }.unwrap()
    }
}

impl<'s, K, V, T, R> Cursor<K, V, T, R> for FileValCursor<'s, K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn key(&self) -> &K {
        self.key.as_ref().unwrap()
    }

    fn val(&self) -> &V {
        self.val.as_ref().unwrap()
    }

    fn fold_times<F, U>(&mut self, mut init: U, mut fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        for (time, diff) in self.times() {
            init = fold(init, &time, &diff);
        }
        init
    }

    fn fold_times_through<F, U>(&mut self, upper: &T, mut init: U, mut fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        for (time, diff) in self.times() {
            if time.less_equal(upper) {
                init = fold(init, &time, &diff);
            }
        }
        init
    }

    fn weight(&mut self) -> R
    where
        T: PartialEq<()>,
    {
        debug_assert!(self.key_valid());
        debug_assert!(self.val_valid());
        let mut res: R = HasZero::zero();
        self.map_times(|_, w| res.add_assign_by_ref(w));
        res
    }

    fn key_valid(&self) -> bool {
        self.key_cursor.has_value()
    }
    fn val_valid(&self) -> bool {
        self.val_cursor.has_value()
    }
    fn step_key(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_next().unwrap());
    }

    fn step_key_reverse(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_prev().unwrap());
    }

    fn seek_key(&mut self, key: &K) {
        self.move_key(|key_cursor| unsafe { key_cursor.advance_to_value_or_larger(key) }.unwrap());
    }

    fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.move_key(|key_cursor| unsafe { key_cursor.seek_forward_until(&predicate) }.unwrap());
    }

    fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.move_key(|key_cursor| unsafe { key_cursor.seek_backward_until(&predicate) }.unwrap());
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.move_key(|key_cursor| unsafe { key_cursor.rewind_to_value_or_smaller(key) }.unwrap());
    }
    fn step_val(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_next().unwrap());
    }
    fn seek_val(&mut self, val: &V) {
        self.move_val(|val_cursor| unsafe { val_cursor.advance_to_value_or_larger(val) }.unwrap());
    }
    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.move_val(|val_cursor| unsafe { val_cursor.seek_forward_until(&predicate) }.unwrap());
    }
    fn rewind_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_first().unwrap());
    }
    fn fast_forward_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_last().unwrap());
    }
    fn rewind_vals(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_first().unwrap());
    }

    fn step_val_reverse(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_prev().unwrap());
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.move_val(|val_cursor| unsafe { val_cursor.rewind_to_value_or_smaller(val) }.unwrap());
    }

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.move_val(|val_cursor| unsafe { val_cursor.seek_backward_until(&predicate) }.unwrap());
    }

    fn fast_forward_vals(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_last().unwrap());
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct FileValBuilder<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    time: T,
    writer: Writer2<StorageBackend, K, (), V, Vec<(T, R)>>,
    cur_key: Option<K>,
}

impl<K, V, T, R> Builder<(K, V), T, R, FileValBatch<K, V, T, R>> for FileValBuilder<K, V, T, R>
where
    Self: SizeOf,
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn new_builder(time: T) -> Self {
        Self {
            time,
            writer: Writer2::new(&StorageBackend::default_for_thread(), Parameters::default())
                .unwrap(),
            cur_key: None,
        }
    }

    fn with_capacity(time: T, _cap: usize) -> Self {
        Self::new_builder(time)
    }

    fn reserve(&mut self, _additional: usize) {}

    fn push(&mut self, ((key, val), diff): ((K, V), R)) {
        if let Some(ref cur_key) = self.cur_key {
            if &key != cur_key {
                self.writer.write0((cur_key, &())).unwrap();
                self.cur_key = Some(key);
            }
        } else {
            self.cur_key = Some(key);
        }
        self.writer
            .write1((&val, &vec![(self.time.clone(), diff)]))
            .unwrap();
    }

    fn done(mut self) -> FileValBatch<K, V, T, R> {
        if let Some(ref cur_key) = self.cur_key {
            self.writer.write0((cur_key, &())).unwrap();
        }
        let time_next = self.time.advance(0);
        let upper = if time_next <= self.time {
            Antichain::new()
        } else {
            Antichain::from_elem(time_next)
        };
        FileValBatch {
            file: self.writer.into_reader().unwrap(),
            lower_bound: 0,
            lower: Antichain::from_elem(self.time),
            upper,
        }
    }
}

impl<K, V, T, R> SizeOf for FileValBuilder<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

pub struct FileValConsumer<K, V, T, R> {
    __type: PhantomData<(K, V, T, R)>,
}

impl<K, V, T, R> Consumer<K, V, R, T> for FileValConsumer<K, V, T, R> {
    type ValueConsumer<'a> = FileValValueConsumer<'a, K, V, T, R>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        todo!()
    }

    fn peek_key(&self) -> &K {
        todo!()
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        todo!()
    }

    fn seek_key(&mut self, _key: &K)
    where
        K: Ord,
    {
        todo!()
    }
}

pub struct FileValValueConsumer<'a, K, V, T, R> {
    __type: PhantomData<&'a (K, V, T, R)>,
}

impl<'a, K, V, T, R> ValueConsumer<'a, V, R, T> for FileValValueConsumer<'a, K, V, T, R> {
    fn value_valid(&self) -> bool {
        todo!()
    }

    fn next_value(&mut self) -> (V, R, T) {
        todo!()
    }

    fn remaining_values(&self) -> usize {
        todo!()
    }
}

impl<K, V, T, R> SizeOf for FileValBatch<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, V, T, R> Archive for FileValBatch<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, V, T, R, S> Serialize<S> for FileValBatch<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, V, T, R, D> Deserialize<FileValBatch<K, V, T, R>, D> for Archived<FileValBatch<K, V, T, R>>
where
    K: DBData,
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileValBatch<K, V, T, R>, D::Error> {
        unimplemented!();
    }
}
