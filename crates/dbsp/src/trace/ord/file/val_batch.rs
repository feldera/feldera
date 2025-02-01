use crate::storage::buffer_cache::CacheStats;
use crate::trace::cursor::{HasTimeDiffCursor, TimeDiffCursor};
use crate::trace::{BatchLocation, TimedBuilder};
use crate::{
    dynamic::{
        DataTrait, DynDataTyped, DynOpt, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase,
        Factory, LeanVec, WeightTrait, WithFactory,
    },
    storage::file::{
        reader::{ColumnSpec, Cursor as FileCursor, Error as ReaderError, Reader},
        writer::Writer2,
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
use derive_more::Debug;
use dyn_clone::clone_box;
use rand::{seq::index::sample, Rng};
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::path::{Path, PathBuf};
use std::{
    cmp::Ordering,
    fmt,
    fmt::{Display, Formatter},
};

pub struct FileValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories0: FileFactories<K, DynUnit>,
    factories1: FileFactories<V, DynWeightedPairs<DynDataTyped<T>, R>>,
    pub timediff_factory: &'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>,
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

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>> {
        Some(self.timediff_factory)
    }
}

type RawValBatch<K, V, T, R> = Reader<(
    &'static K,
    &'static DynUnit,
    (
        &'static V,
        &'static DynWeightedPairs<DynDataTyped<T>, R>,
        (),
    ),
)>;

type RawKeyCursor<'s, K, V, T, R> = FileCursor<
    's,
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
#[derive(SizeOf, Debug)]
pub struct FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    #[debug(skip)]
    factories: FileValBatchFactories<K, V, T, R>,
    #[size_of(skip)]
    pub file: RawValBatch<K, V, T, R>,
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
        self.file.rows().len() as usize
    }

    fn len(&self) -> usize {
        self.file.n_rows(1) as usize
    }

    fn approximate_byte_size(&self) -> usize {
        self.file.byte_size().unwrap() as usize
    }

    #[inline]
    fn location(&self) -> BatchLocation {
        BatchLocation::Storage
    }

    fn cache_stats(&self) -> CacheStats {
        self.file.cache_stats()
    }

    fn lower(&self) -> AntichainRef<'_, T> {
        self.lower.as_ref()
    }

    fn upper(&self) -> AntichainRef<'_, T> {
        self.upper.as_ref()
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

    fn begin_merge(&self, other: &Self, dst_hint: Option<BatchLocation>) -> Self::Merger {
        FileValMerger::new_merger(self, other, dst_hint)
    }

    fn checkpoint_path(&self) -> Option<PathBuf> {
        self.file.mark_for_checkpoint();
        Some(self.file.path())
    }

    fn from_path(factories: &Self::Factories, path: &Path) -> Result<Self, ReaderError> {
        let any_factory0 = factories.factories0.any_factories();
        let any_factory1 = factories.factories1.any_factories();
        let file = Reader::open(
            &[&any_factory0, &any_factory1],
            Runtime::buffer_cache().unwrap(),
            &*Runtime::storage_backend().unwrap(),
            path,
        )?;
        Ok(Self {
            factories: factories.clone(),
            file,
            lower: Antichain::new(),
            upper: Antichain::new(),
        })
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct FileValMerger<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileValBatchFactories<K, V, T, R>,
    #[size_of(skip)]
    result: Option<RawValBatch<K, V, T, R>>,
    lower: Antichain<T>,
    upper: Antichain<T>,
}

fn include<K: ?Sized>(x: &K, filter: &Option<Filter<K>>) -> bool {
    match filter {
        Some(filter) => (filter.filter_func)(x),
        None => true,
    }
}

fn read_filtered<'a, K, A, N, T>(
    cursor: &mut FileCursor<K, A, N, T>,
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

// Like `merge_times`, but additionally applied `map_func` to each timestamp.
// Sorts and consolidates the resulting array of time/diff pairs.
fn merge_map_times<T, R>(
    a: &mut DynWeightedPairs<DynDataTyped<T>, R>,
    b: &mut DynWeightedPairs<DynDataTyped<T>, R>,
    map_func: &dyn Fn(&mut DynDataTyped<T>),
    output: &mut DynWeightedPairs<DynDataTyped<T>, R>,
) where
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    output.clear();
    output.append(a.as_vec_mut());
    output.append(b.as_vec_mut());

    for i in 0..output.len() {
        map_func(unsafe { output.index_mut_unchecked(i) }.fst_mut());
    }

    output.consolidate();
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
        output: &mut Writer2<K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
        key: &K,
        key_cursor: &mut RawKeyCursor<'_, K, V, T, R>,
        value_filter: &Option<Filter<V>>,
        map_func: Option<&dyn Fn(&mut DynDataTyped<T>)>,
    ) {
        self.factories.factories1.key_factory.with(&mut |value| {
            let mut value_cursor = key_cursor.next_column().unwrap().first().unwrap();
            let mut n = 0;
            while value_cursor.has_value() {
                let value = unsafe { value_cursor.key(value) }.unwrap();
                if include(value, value_filter) {
                    n += self.copy_value(output, &mut value_cursor, value, map_func);
                }
                value_cursor.move_next().unwrap();
            }
            if n > 0 {
                output.write0((key, ().erase())).unwrap();
            }
            key_cursor.move_next().unwrap();
        })
    }

    fn copy_value(
        &self,
        output: &mut Writer2<K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
        cursor: &mut RawValCursor<'_, K, V, T, R>,
        value: &V,
        map_func: Option<&dyn Fn(&mut DynDataTyped<T>)>,
    ) -> usize {
        let mut n = 0;

        self.factories.timediff_factory.with(&mut |td| {
            let td = unsafe { cursor.aux(td) }.unwrap();
            if let Some(map_func) = map_func {
                for i in 0..td.len() {
                    map_func(td[i].fst_mut());
                }
                td.consolidate();
                if !td.is_empty() {
                    output.write1((value, td)).unwrap();
                    n = 1;
                }
            } else {
                output.write1((value, td)).unwrap();
                n = 1;
            }
        });
        n
    }

    fn merge_values(
        &self,
        output: &mut Writer2<K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
        cursor1: &mut RawValCursor<'_, K, V, T, R>,
        cursor2: &mut RawValCursor<'_, K, V, T, R>,
        value_filter: &Option<Filter<V>>,
        map_func: Option<&dyn Fn(&mut DynDataTyped<T>)>,
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
                                        n += self.copy_value(output, cursor2, value2, map_func);
                                        cursor2.move_next().unwrap();
                                    }
                                    return;
                                };
                                let Some(value2) = read_filtered(cursor2, value_filter, tmp_v2)
                                else {
                                    while let Some(value1) =
                                        read_filtered(cursor1, value_filter, tmp_v1)
                                    {
                                        n += self.copy_value(output, cursor1, value1, map_func);
                                        cursor1.move_next().unwrap();
                                    }
                                    return;
                                };
                                match value1.cmp(value2) {
                                    Ordering::Less => {
                                        n += self.copy_value(output, cursor1, value1, map_func);
                                        cursor1.move_next().unwrap();
                                    }
                                    Ordering::Equal => {
                                        let td1 = unsafe { cursor1.aux(td1) }.unwrap();
                                        let td2 = unsafe { cursor2.aux(td2) }.unwrap();
                                        if let Some(map_func) = &map_func {
                                            merge_map_times(td1, td2, map_func, td);
                                        } else {
                                            //debug_assert!(td1.is_sorted());
                                            //debug_assert!(td2.is_sorted());

                                            merge_times(td1, td2, td, tmp_w);
                                        }
                                        cursor1.move_next().unwrap();
                                        cursor2.move_next().unwrap();
                                        if td.is_empty() {
                                            continue;
                                        }
                                        output.write1((value1, td)).unwrap();
                                        n += 1;
                                    }
                                    Ordering::Greater => {
                                        n += self.copy_value(output, cursor2, value2, map_func);
                                        cursor2.move_next().unwrap();
                                    }
                                }
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
        frontier: &T,
    ) -> RawValBatch<K, V, T, R> {
        let advance_func = |t: &mut DynDataTyped<T>| t.join_assign(frontier);

        let time_map_func = if frontier == &T::minimum() {
            None
        } else {
            Some(&advance_func as &dyn Fn(&mut DynDataTyped<T>))
        };

        let mut output = Writer2::new(
            &source1.factories.factories0,
            &source1.factories().factories1,
            Runtime::buffer_cache().unwrap(),
            &*Runtime::storage_backend().unwrap(),
            Runtime::file_writer_parameters(),
        )
        .unwrap();
        let mut cursor1 = source1.file.rows().nth(0).unwrap();
        let mut cursor2 = source2.file.rows().nth(0).unwrap();
        self.factories.factories0.key_factory.with(&mut |tmp_key1| {
            self.factories
                .factories0
                .key_factory
                .with(&mut |tmp_key2| loop {
                    let Some(key1) = read_filtered(&mut cursor1, key_filter, tmp_key1) else {
                        while let Some(key2) = read_filtered(&mut cursor2, key_filter, tmp_key2) {
                            self.copy_values_if(
                                &mut output,
                                key2,
                                &mut cursor2,
                                value_filter,
                                time_map_func,
                            );
                        }
                        break;
                    };
                    let Some(key2) = read_filtered(&mut cursor2, key_filter, tmp_key2) else {
                        while let Some(key1) = read_filtered(&mut cursor1, key_filter, tmp_key1) {
                            self.copy_values_if(
                                &mut output,
                                key1,
                                &mut cursor1,
                                value_filter,
                                time_map_func,
                            );
                        }
                        break;
                    };
                    match key1.cmp(key2) {
                        Ordering::Less => {
                            self.copy_values_if(
                                &mut output,
                                key1,
                                &mut cursor1,
                                value_filter,
                                time_map_func,
                            );
                        }
                        Ordering::Equal => {
                            if self.merge_values(
                                &mut output,
                                &mut cursor1.next_column().unwrap().first().unwrap(),
                                &mut cursor2.next_column().unwrap().first().unwrap(),
                                value_filter,
                                time_map_func,
                            ) {
                                output.write0((key1, &())).unwrap();
                            }
                            cursor1.move_next().unwrap();
                            cursor2.move_next().unwrap();
                        }

                        Ordering::Greater => {
                            self.copy_values_if(
                                &mut output,
                                key2,
                                &mut cursor2,
                                value_filter,
                                time_map_func,
                            );
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
    fn new_merger(
        batch1: &FileValBatch<K, V, T, R>,
        batch2: &FileValBatch<K, V, T, R>,
        _dst_hint: Option<BatchLocation>,
    ) -> Self {
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
            file: self.result.take().unwrap_or(
                Reader::empty(
                    Runtime::buffer_cache().unwrap(),
                    &*Runtime::storage_backend().unwrap(),
                )
                .unwrap(),
            ),
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
        frontier: &T,
        fuel: &mut isize,
    ) {
        debug_assert!(*fuel > 0);
        if self.result.is_none() {
            self.result = Some(self.merge(source1, source2, key_filter, value_filter, frontier));
        }
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

impl<K, V, T, R> Debug for FileValCursor<'_, K, V, T, R>
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

impl<K, V, T, R> Clone for FileValCursor<'_, K, V, T, R>
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
        let key_cursor = batch.file.rows().first().unwrap();
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

impl<K, V, T, R> Cursor<K, V, T, R> for FileValCursor<'_, K, V, T, R>
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

pub struct FileValTimeDiffCursor<T, R>
where
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    timediffs: Box<DynWeightedPairs<DynDataTyped<T>, R>>,
    index: usize,
}

impl<T, R> TimeDiffCursor<'_, T, R> for FileValTimeDiffCursor<T, R>
where
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn current(&mut self, _tmp: &mut R) -> Option<(&T, &R)> {
        if self.index < self.timediffs.len() {
            let (time, diff) = self.timediffs[self.index].split();
            Some((time, diff))
        } else {
            None
        }
    }

    fn step(&mut self) {
        self.index += 1;
    }
}

impl<K, V, T, R> HasTimeDiffCursor<K, V, T, R> for FileValCursor<'_, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type TimeDiffCursor<'a>
        = FileValTimeDiffCursor<T, R>
    where
        Self: 'a;

    fn time_diff_cursor(&self) -> Self::TimeDiffCursor<'_> {
        let mut timediffs = self.timediff_factory.default_box();
        self.times(timediffs.as_mut());
        FileValTimeDiffCursor {
            timediffs,
            index: 0,
        }
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct FileValBuilder<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileValBatchFactories<K, V, T, R>,
    time: T,
    #[size_of(skip)]
    writer: Writer2<K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
    cur: Option<BuilderState<K, V, T, R>>,
}

#[derive(SizeOf)]
struct BuilderState<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    key: Box<K>,
    val: Box<V>,
    timediffs: Box<DynWeightedPairs<DynDataTyped<T>, R>>,
}

impl<K, V, T, R> BuilderState<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new(
        key: &K,
        val: &V,
        time: &T,
        weight: &R,
        factories: &FileValBatchFactories<K, V, T, R>,
    ) -> Self {
        let mut timediffs = factories.timediff_factory.default_box();
        timediffs.push_refs((time.erase(), weight));
        Self {
            key: clone_box(key),
            val: clone_box(val),
            timediffs,
        }
    }

    fn flush_val(
        &mut self,
        writer: &mut Writer2<K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
    ) {
        writer.write1((&self.val, self.timediffs.as_ref())).unwrap();
        self.timediffs.clear();
    }

    fn flush_keyval(
        &mut self,
        writer: &mut Writer2<K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
    ) {
        self.flush_val(writer);
        writer.write0((&self.key, ().erase())).unwrap();
    }
}

impl<K, V, T, R> TimedBuilder<FileValBatch<K, V, T, R>> for FileValBuilder<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn push_time(&mut self, key: &K, val: &V, time: &T, weight: &R) {
        debug_assert!(!weight.is_zero());
        if let Some(cur) = &mut self.cur {
            if &*cur.key != key {
                // Key differs from previous, so write the old (V, timediffs) to
                // column 1 and the old key to column 0, and then save the new
                // key and value as the new current.
                cur.flush_keyval(&mut self.writer);
                key.clone_to(&mut cur.key);
                val.clone_to(&mut cur.val);
            } else if &*cur.val != val {
                // Value differs from previous, but the key is the same, so
                // write the old (V, timediffs) to column 1 and save the new
                // value as the new current.
                cur.flush_val(&mut self.writer);
                val.clone_to(&mut cur.val);
            } else {
                // Same key and value, so we're just appending the new timediff.
            }
            cur.timediffs.push_refs((time.erase(), weight));
        } else {
            // First (K, V, T, R).
            self.cur = Some(BuilderState::new(key, val, time, weight, &self.factories));
        }
    }

    fn done_with_bounds(
        mut self,
        lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> FileValBatch<K, V, T, R> {
        if let Some(cur) = &mut self.cur {
            cur.flush_keyval(&mut self.writer);
        }
        FileValBatch {
            factories: self.factories,
            file: self.writer.into_reader().unwrap(),
            lower,
            upper,
        }
    }
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
                Runtime::buffer_cache().unwrap(),
                &*Runtime::storage_backend().unwrap(),
                Runtime::file_writer_parameters(),
            )
            .unwrap(),
            cur: None,
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
        let time = self.time.clone();
        self.push_time(key, val, &time, diff);
    }

    fn push_vals(&mut self, key: &mut K, val: &mut V, diff: &mut R) {
        self.push_refs(key, val, diff)
    }

    fn done(mut self) -> FileValBatch<K, V, T, R> {
        if let Some(cur) = &mut self.cur {
            cur.flush_keyval(&mut self.writer);
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
            lower: Antichain::from_elem(self.time),
            upper,
        }
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
