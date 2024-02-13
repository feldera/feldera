use dyn_clone::clone_box;
use std::path::PathBuf;
use std::{
    cmp::Ordering,
    fmt,
    fmt::{Debug, Display, Formatter},
    ops::DerefMut,
};

use crate::{
    dynamic::{
        DataTrait, DynDataTyped, DynOpt, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase,
        Factory, LeanVec, WeightTrait, WithFactory,
    },
    storage::file::{
        reader::{ColumnSpec, Cursor as FileCursor, Reader},
        writer::{Parameters, Writer2},
        Factories as FileFactories,
    },
    time::{Antichain, AntichainRef},
    trace::{
        ord::merge_batcher::MergeBatcher, Batch, BatchFactories, BatchReader, BatchReaderFactories,
        Builder, Cursor, Filter, Merger, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries, Runtime, Timestamp,
};
use rand::{seq::index::sample, Rng};
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;

use super::StorageBackend;

pub struct FileValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories0: FileFactories<K, DynUnit>,
    factories1: FileFactories<V, DynWeightedPairs<DynDataTyped<T>, R>>,
    timediff_factory: &'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>,
    weight_factory: &'static dyn Factory<R>,
    optkey_factory: &'static dyn Factory<DynOpt<K>>,
    keys_factory: &'static dyn Factory<DynVec<K>>,
    item_factory: &'static dyn Factory<DynPair<K, V>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, V, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>>,
}

impl<K, V, T, R> Clone for FileValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories0: self.factories0.clone(),
            factories1: self.factories1.clone(),
            optkey_factory: self.optkey_factory,
            weight_factory: self.weight_factory,
            timediff_factory: self.timediff_factory,
            keys_factory: self.keys_factory,
            item_factory: self.item_factory,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
        }
    }
}

impl<K, V, T, R> BatchReaderFactories<K, V, T, R> for FileValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            factories0: FileFactories::new::<KType, ()>(),
            factories1: FileFactories::new::<VType, LeanVec<Tup2<T, RType>>>(),
            optkey_factory: WithFactory::<Option<KType>>::FACTORY,
            weight_factory: WithFactory::<RType>::FACTORY,
            timediff_factory: WithFactory::<LeanVec<Tup2<T, RType>>>::FACTORY,
            keys_factory: WithFactory::<LeanVec<KType>>::FACTORY,
            item_factory: WithFactory::<Tup2<KType, VType>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, VType>, RType>>::FACTORY,
            weighted_items_factory:
                WithFactory::<LeanVec<Tup2<Tup2<KType, VType>, RType>>>::FACTORY,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.factories0.key_factory
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.keys_factory
    }

    fn val_factory(&self) -> &'static dyn Factory<V> {
        self.factories1.key_factory
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.weight_factory
    }
}

impl<K, V, T, R> BatchFactories<K, V, T, R> for FileValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
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

type RawValBatch<K, V, T, R> = Reader<
    StorageBackend,
    (
        &'static K,
        &'static DynUnit,
        (
            &'static V,
            &'static DynWeightedPairs<DynDataTyped<T>, R>,
            (),
        ),
    ),
>;

type RawKeyCursor<'s, K, V, T, R> = FileCursor<
    's,
    StorageBackend,
    K,
    DynUnit,
    (
        &'static V,
        &'static DynWeightedPairs<DynDataTyped<T>, R>,
        (),
    ),
    (
        &'static K,
        &'static DynUnit,
        (
            &'static V,
            &'static DynWeightedPairs<DynDataTyped<T>, R>,
            (),
        ),
    ),
>;

type RawValCursor<'s, K, V, T, R> = FileCursor<
    's,
    StorageBackend,
    V,
    DynWeightedPairs<DynDataTyped<T>, R>,
    (),
    (
        &'static K,
        &'static DynUnit,
        (
            &'static V,
            &'static DynWeightedPairs<DynDataTyped<T>, R>,
            (),
        ),
    ),
>;

/// An immutable collection of update tuples, from a contiguous interval of
/// logical times.
pub struct FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FileValBatchFactories<K, V, T, R>,
    pub file: RawValBatch<K, V, T, R>,
    pub lower_bound: usize,
    pub lower: Antichain<T>,
    pub upper: Antichain<T>,
}

impl<K, V, T, R> Clone for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            file: self.file.clone(),
            lower_bound: self.lower_bound,
            lower: self.lower.clone(),
            upper: self.upper.clone(),
        }
    }
}

impl<K, V, T, R> NumEntries for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
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
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(f, "lower: {:?}, upper: {:?}\n", self.lower, self.upper)
    }
}

impl<K, V, T, R> BatchReader for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Factories = FileValBatchFactories<K, V, T, R>;
    type Key = K;
    type Val = V;
    type Time = T;
    type R = R;

    type Cursor<'s> = FileValCursor<'s, K, V, T, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        FileValCursor::new(self)
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

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut DynVec<Self::Key>)
    where
        RG: Rng,
    {
        self.factories.factories0.key_factory.with(&mut |key| {
            let size = self.key_count();
            let mut cursor = self.file.rows().first().unwrap();
            if sample_size >= size {
                output.reserve(size);
                while let Some(key) = unsafe { cursor.key(key) } {
                    output.push_ref(key);
                    cursor.move_next().unwrap();
                }
            } else {
                output.reserve(sample_size);

                let mut indexes = sample(rng, size, sample_size).into_vec();
                indexes.sort_unstable();
                for index in indexes {
                    cursor.move_to_row(index as u64).unwrap();
                    output.push_ref(unsafe { cursor.key(key) }.unwrap());
                }
            }
        })
    }
}

impl<K, V, T, R> Batch for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FileValBuilder<K, V, T, R>;
    type Merger = FileValMerger<K, V, T, R>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        FileValMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, frontier: &T) {
        self.factories.timediff_factory.with(&mut |td| {
            self.factories.factories1.key_factory.with(&mut |val| {
                self.factories.factories0.key_factory.with(&mut |key| {
                    // Nothing to do if the batch is entirely before the frontier.
                    if !self.upper().less_equal(frontier) {
                        let mut writer = Writer2::new(
                            &self.factories.factories0,
                            &self.factories.factories1,
                            &Runtime::storage(),
                            Parameters::default(),
                        )
                        .unwrap();
                        let mut key_cursor = self.file.rows().first().unwrap();
                        while key_cursor.has_value() {
                            let mut val_cursor = key_cursor.next_column().unwrap().first().unwrap();
                            let mut n_vals = 0;
                            while val_cursor.has_value() {
                                let td = unsafe { val_cursor.aux(td) }.unwrap();
                                recede_times(td, frontier);
                                if !td.is_empty() {
                                    let val = unsafe { val_cursor.key(val) }.unwrap();
                                    writer.write1((val, td)).unwrap();
                                    n_vals += 1;
                                }
                                val_cursor.move_next().unwrap();
                            }
                            if n_vals > 0 {
                                let key = unsafe { key_cursor.key(key) }.unwrap();
                                writer.write0((key, ().erase())).unwrap();
                            }
                            key_cursor.move_next().unwrap();
                        }
                        self.file = writer.into_reader().unwrap();
                    }
                })
            })
        })
    }

    fn persistent_id(&self) -> Option<PathBuf> {
        Some(self.file.path())
    }
}

fn recede_times<T, R>(td: &mut DynWeightedPairs<DynDataTyped<T>, R>, frontier: &T)
where
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    for timediff in td.dyn_iter_mut() {
        timediff.fst_mut().deref_mut().meet_assign(frontier);
    }
    td.consolidate();
}

/// State for an in-progress merge.
pub struct FileValMerger<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FileValBatchFactories<K, V, T, R>,
    result: Option<RawValBatch<K, V, T, R>>,
    lower: Antichain<T>,
    upper: Antichain<T>,
}

fn include<K: ?Sized>(x: &K, filter: &Option<Filter<K>>) -> bool {
    match filter {
        Some(filter) => filter(x),
        None => true,
    }
}

fn read_filtered<'a, K, A, N, T>(
    cursor: &mut FileCursor<StorageBackend, K, A, N, T>,
    filter: &Option<Filter<K>>,
    key: &'a mut K,
) -> Option<&'a K>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    while cursor.has_value() {
        unsafe { cursor.key(key) }.unwrap();
        if include(key, filter) {
            return Some(key);
        }
        cursor.move_next().unwrap();
    }
    None
}

fn merge_times<T, R>(
    a: &DynWeightedPairs<DynDataTyped<T>, R>,
    b: &DynWeightedPairs<DynDataTyped<T>, R>,
    output: &mut DynWeightedPairs<DynDataTyped<T>, R>,
    tmp_weight: &mut R,
) where
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    output.clear();
    output.reserve(a.len() + b.len());

    let mut i = 0;
    let mut j = 0;

    while i < a.len() && j < b.len() {
        match a[i].fst().cmp(b[j].fst()) {
            Ordering::Less => {
                output.push_ref(&a[i]);
                i += 1;
            }
            Ordering::Equal => {
                a[i].snd().add(b[j].snd(), tmp_weight);
                if !tmp_weight.is_zero() {
                    output.push_refs((a[i].fst(), tmp_weight));
                }
                i += 1;
                j += 1;
            }
            Ordering::Greater => {
                output.push_ref(&b[j]);
                j += 1;
            }
        }
    }
    output.extend_from_range(a.as_vec(), i, a.len());
    output.extend_from_range(b.as_vec(), j, b.len());
}

impl<K, V, T, R> FileValMerger<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn copy_values_if(
        &self,
        output: &mut Writer2<StorageBackend, K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
        key: &K,
        key_cursor: &mut RawKeyCursor<'_, K, V, T, R>,
        value_filter: &Option<Filter<V>>,
    ) {
        self.factories.factories1.key_factory.with(&mut |value| {
            self.factories.timediff_factory.with(&mut |aux| {
                let mut value_cursor = key_cursor.next_column().unwrap().first().unwrap();
                let mut n = 0;
                while value_cursor.has_value() {
                    let value = unsafe { value_cursor.key(value) }.unwrap();
                    if include(value, value_filter) {
                        let aux = unsafe { value_cursor.aux(aux) }.unwrap();
                        output.write1((value, aux)).unwrap();
                        n += 1;
                    }
                    value_cursor.move_next().unwrap();
                }
                if n > 0 {
                    output.write0((key, ().erase())).unwrap();
                }
                key_cursor.move_next().unwrap();
            })
        })
    }

    fn copy_value(
        &self,
        output: &mut Writer2<StorageBackend, K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
        cursor: &mut RawValCursor<'_, K, V, T, R>,
        value: &V,
    ) {
        self.factories.timediff_factory.with(&mut |td| {
            let td = unsafe { cursor.aux(td) }.unwrap();
            output.write1((value, td)).unwrap();
            cursor.move_next().unwrap();
        })
    }

    fn merge_values(
        &self,
        output: &mut Writer2<StorageBackend, K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
        cursor1: &mut RawValCursor<'_, K, V, T, R>,
        cursor2: &mut RawValCursor<'_, K, V, T, R>,
        value_filter: &Option<Filter<V>>,
    ) -> bool {
        let mut n = 0;

        self.factories.weight_factory.with(&mut |tmp_w| {
            self.factories.factories1.key_factory.with(&mut |tmp_v1| {
                self.factories.factories1.key_factory.with(&mut |tmp_v2| {
                    self.factories.timediff_factory.with(&mut |td| {
                        self.factories.timediff_factory.with(&mut |td1| {
                            self.factories.timediff_factory.with(&mut |td2| loop {
                                let Some(value1) = read_filtered(cursor1, value_filter, tmp_v1)
                                else {
                                    while let Some(value2) =
                                        read_filtered(cursor2, value_filter, tmp_v2)
                                    {
                                        self.copy_value(output, cursor2, value2);
                                        n += 1;
                                    }
                                    return;
                                };
                                let Some(value2) = read_filtered(cursor2, value_filter, tmp_v2)
                                else {
                                    while let Some(value1) =
                                        read_filtered(cursor1, value_filter, tmp_v1)
                                    {
                                        self.copy_value(output, cursor1, value1);
                                        n += 1;
                                    }
                                    return;
                                };
                                match value1.cmp(value2) {
                                    Ordering::Less => self.copy_value(output, cursor1, value1),
                                    Ordering::Equal => {
                                        let td1 = unsafe { cursor1.aux(td1) }.unwrap();
                                        td1.sort_unstable();
                                        let td2 = unsafe { cursor2.aux(td2) }.unwrap();
                                        td2.sort_unstable();
                                        merge_times(td1, td2, td, tmp_w);
                                        cursor1.move_next().unwrap();
                                        cursor2.move_next().unwrap();
                                        if td.is_empty() {
                                            continue;
                                        }
                                        output.write1((value1, td)).unwrap();
                                    }
                                    Ordering::Greater => self.copy_value(output, cursor2, value2),
                                }
                                n += 1;
                            })
                        })
                    })
                })
            })
        });

        n > 0
    }

    fn merge(
        &self,
        source1: &FileValBatch<K, V, T, R>,
        source2: &FileValBatch<K, V, T, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
    ) -> RawValBatch<K, V, T, R> {
        let mut output = Writer2::new(
            &source1.factories.factories0,
            &source1.factories().factories1,
            &Runtime::storage(),
            Parameters::default(),
        )
        .unwrap();
        let mut cursor1 = source1.file.rows().nth(source1.lower_bound as u64).unwrap();
        let mut cursor2 = source2.file.rows().nth(source2.lower_bound as u64).unwrap();
        self.factories.factories0.key_factory.with(&mut |tmp_key1| {
            self.factories
                .factories0
                .key_factory
                .with(&mut |tmp_key2| loop {
                    let Some(key1) = read_filtered(&mut cursor1, key_filter, tmp_key1) else {
                        while let Some(key2) = read_filtered(&mut cursor2, key_filter, tmp_key2) {
                            self.copy_values_if(&mut output, key2, &mut cursor2, value_filter);
                        }
                        break;
                    };
                    let Some(key2) = read_filtered(&mut cursor2, key_filter, tmp_key2) else {
                        while let Some(key1) = read_filtered(&mut cursor1, key_filter, tmp_key1) {
                            self.copy_values_if(&mut output, key1, &mut cursor1, value_filter);
                        }
                        break;
                    };
                    match key1.cmp(key2) {
                        Ordering::Less => {
                            self.copy_values_if(&mut output, key1, &mut cursor1, value_filter);
                        }
                        Ordering::Equal => {
                            if self.merge_values(
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
                            self.copy_values_if(&mut output, key2, &mut cursor2, value_filter);
                        }
                    }
                })
        });
        output.into_reader().unwrap()
    }
}

impl<K, V, T, R> Merger<K, V, T, R, FileValBatch<K, V, T, R>> for FileValMerger<K, V, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_merger(batch1: &FileValBatch<K, V, T, R>, batch2: &FileValBatch<K, V, T, R>) -> Self {
        Self {
            factories: batch1.factories.clone(),
            result: None,
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
        }
    }

    fn done(mut self) -> FileValBatch<K, V, T, R> {
        FileValBatch {
            factories: self.factories,
            file: self
                .result
                .take()
                .unwrap_or(Reader::empty(&Runtime::storage()).unwrap()),
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
            self.result = Some(self.merge(source1, source2, key_filter, value_filter));
        }
    }
}

impl<K, V, T, R> SizeOf for FileValMerger<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

#[derive(SizeOf)]
pub struct FileValCursor<'s, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    timediff_factory: &'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>,
    weight_factory: &'static dyn Factory<R>,
    key_cursor: RawKeyCursor<'s, K, V, T, R>,
    val_cursor: RawValCursor<'s, K, V, T, R>,
    key: Box<K>,
    key_valid: bool,
    val: Box<V>,
    val_valid: bool,
    weight: Box<R>,
}

impl<'s, K, V, T, R> Debug for FileValCursor<'s, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileValCursor")
            .field("key_cursor", &self.key_cursor)
            .field("val_cursor", &self.val)
            .field("key", &self.key)
            .field("key_valid", &self.key_valid)
            .field("val", &self.val)
            .field("val_valid", &self.val_valid)
            .field("weight", &self.weight)
            .finish()
    }
}

impl<'s, K, V, T, R> Clone for FileValCursor<'s, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            timediff_factory: self.timediff_factory,
            weight_factory: self.weight_factory,
            key_cursor: self.key_cursor.clone(),
            val_cursor: self.val_cursor.clone(),
            key: clone_box(&self.key),
            key_valid: self.key_valid,
            val: clone_box(&self.val),
            val_valid: self.val_valid,
            weight: clone_box(&self.weight),
        }
    }
}

impl<'s, K, V, T, R> FileValCursor<'s, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new(batch: &'s FileValBatch<K, V, T, R>) -> Self {
        let key_cursor = batch
            .file
            .rows()
            .subset(batch.lower_bound as u64..)
            .first()
            .unwrap();
        let val_cursor = key_cursor.next_column().unwrap().first().unwrap();
        let mut key = batch.factories.factories0.key_factory.default_box();
        let mut val = batch.factories.factories1.key_factory.default_box();

        let key_valid = unsafe { key_cursor.key(&mut key) }.is_some();
        let val_valid = unsafe { val_cursor.key(&mut val) }.is_some();
        Self {
            timediff_factory: batch.factories.timediff_factory,
            weight_factory: batch.factories.weight_factory,
            key_cursor,
            val_cursor,
            key,
            key_valid,
            val,
            val_valid,
            weight: batch.factories.weight_factory.default_box(),
        }
    }
    fn move_key<F>(&mut self, op: F)
    where
        F: Fn(&mut RawKeyCursor<'s, K, V, T, R>),
    {
        op(&mut self.key_cursor);
        self.val_cursor = self.key_cursor.next_column().unwrap().first().unwrap();
        self.key_valid = unsafe { self.key_cursor.key(&mut self.key) }.is_some();
        self.val_valid = unsafe { self.val_cursor.key(&mut self.val) }.is_some();
    }
    fn move_val<F>(&mut self, op: F)
    where
        F: Fn(&mut RawValCursor<'s, K, V, T, R>),
    {
        op(&mut self.val_cursor);
        self.val_valid = unsafe { self.val_cursor.key(&mut self.val) }.is_some();
    }
    fn times<'a>(
        &self,
        times: &'a mut DynWeightedPairs<DynDataTyped<T>, R>,
    ) -> &'a mut DynWeightedPairs<DynDataTyped<T>, R> {
        unsafe { self.val_cursor.aux(times) }.unwrap()
    }
}

impl<'s, K, V, T, R> Cursor<K, V, T, R> for FileValCursor<'s, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.weight_factory
    }

    fn key(&self) -> &K {
        debug_assert!(self.key_valid);
        &self.key
    }

    fn val(&self) -> &V {
        debug_assert!(self.val_valid);
        &self.val
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        self.timediff_factory.with(&mut |timediffs| {
            for timediff in self.times(timediffs).dyn_iter() {
                let (time, weight) = timediff.split();
                logic(time, weight);
            }
        })
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        self.timediff_factory.with(&mut |timediffs| {
            for timediff in self.times(timediffs).dyn_iter() {
                let (time, weight) = timediff.split();

                if time.less_equal(upper) {
                    logic(time, weight);
                }
            }
        })
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>,
    {
        while self.val_valid() {
            self.weight();
            logic(self.val(), &self.weight);
            self.step_val()
        }
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        debug_assert!(self.key_valid());
        debug_assert!(self.val_valid());
        self.weight.set_zero();

        self.timediff_factory.with(&mut |timediffs| {
            for timediff in self.times(timediffs).dyn_iter() {
                self.weight.add_assign(timediff.snd());
            }
        });

        &self.weight
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

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.move_key(|key_cursor| unsafe { key_cursor.seek_forward_until(predicate) }.unwrap());
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.move_key(|key_cursor| unsafe { key_cursor.seek_backward_until(predicate) }.unwrap());
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
    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
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

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.move_val(|val_cursor| unsafe { val_cursor.seek_backward_until(&predicate) }.unwrap());
    }

    fn fast_forward_vals(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_last().unwrap());
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct FileValBuilder<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FileValBatchFactories<K, V, T, R>,
    time: T,
    writer: Writer2<StorageBackend, K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
    cur_key: Box<DynOpt<K>>,
}

impl<K, V, T, R> Builder<FileValBatch<K, V, T, R>> for FileValBuilder<K, V, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_builder(factories: &FileValBatchFactories<K, V, T, R>, time: T) -> Self {
        Self {
            factories: factories.clone(),
            time,
            writer: Writer2::new(
                &factories.factories0,
                &factories.factories1,
                &Runtime::storage(),
                Parameters::default(),
            )
            .unwrap(),
            cur_key: factories.optkey_factory.default_box(),
        }
    }

    fn with_capacity(factories: &FileValBatchFactories<K, V, T, R>, time: T, _cap: usize) -> Self {
        Self::new_builder(factories, time)
    }

    fn reserve(&mut self, _additional: usize) {}

    fn push(&mut self, item: &mut DynPair<DynPair<K, V>, R>) {
        let (kv, r) = item.split();
        let (k, v) = kv.split();

        self.push_refs(k, v, r);
    }

    fn push_refs(&mut self, key: &K, val: &V, diff: &R) {
        if let Some(cur_key) = self.cur_key.get() {
            if key != cur_key {
                self.writer.write0((cur_key, ().erase())).unwrap();
                self.cur_key.from_ref(key);
            }
        } else {
            self.cur_key.from_ref(key);
        }
        let mut timediffs = self.factories.timediff_factory.default_box();
        timediffs.push_refs((self.time.erase(), diff));
        self.writer.write1((val, timediffs.as_ref())).unwrap();
    }

    fn push_vals(&mut self, key: &mut K, val: &mut V, diff: &mut R) {
        self.push_refs(key, val, diff)
    }

    fn done(mut self) -> FileValBatch<K, V, T, R> {
        if let Some(cur_key) = self.cur_key.get() {
            self.writer.write0((cur_key, ().erase())).unwrap();
        }
        let time_next = self.time.advance(0);
        let upper = if time_next <= self.time {
            Antichain::new()
        } else {
            Antichain::from_elem(time_next)
        };
        FileValBatch {
            factories: self.factories,
            file: self.writer.into_reader().unwrap(),
            lower_bound: 0,
            lower: Antichain::from_elem(self.time),
            upper,
        }
    }
}

impl<K, V, T, R> SizeOf for FileValBuilder<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

/*
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
*/

impl<K, V, T, R> SizeOf for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, V, T, R> Archive for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, V, T, R, S> Serialize<S> for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, V, T, R, D> Deserialize<FileValBatch<K, V, T, R>, D> for Archived<FileValBatch<K, V, T, R>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileValBatch<K, V, T, R>, D::Error> {
        unimplemented!();
    }
}
