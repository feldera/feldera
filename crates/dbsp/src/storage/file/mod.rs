//! File based data format for Feldera.
//!
//! A "layer file" stores `n > 0` columns of data, each of which has a key type
//! `K[i]` and an auxiliary data type `A[i]`.  Each column is arranged into
//! groups of rows, where column 0 forms a single group and each row in column
//! `i` is associated with a group of one or more rows in column `i + 1` (for
//! `i + 1 < n`).  A group contains sorted, unique values. A group cursor for
//! column `i` can move forward and backward by rows, seek forward and backward
//! by the key type `K[i]` or using a predicate based on `K[i]`, and (when `i +
//! 1 < n`) move to the row group in column `i + 1` associated with the cursor's
//! row.
//!
//! Thus, ignoring the auxiliary data in each column, a 1-column layer file is
//! analogous to `BTreeSet<K[0]>`, a 2-column layer file is analogous to
//! `BTreeMap<K[0], BTreeSet<K[1]>>`, and a 3-column layer file is analogous to
//! `BTreeMap<K[0], BTreeMap<K[1], BTreeSet<K[2]>>>`.
//!
//! For DBSP, it is likely that only 1-, 2, and 3-column layer files matter, and
//! maybe not even 3-column.
//!
//! Layer files are written once in their entirety and immutable thereafter.
//! Therefore, there are APIs for reading and writing layer files, but no API
//! for modifying them.
//!
//! Layer files use [`rkyv`] for serialization and deserialization.
//!
//! The "layer file" name comes from the `ColumnLayer` and `OrderedLayer` data
//! structures used in DBSP and inherited from Differential Dataflow.
//!
//! # Goals
//!
//! Layer files aim to balance read and write performance.  That is, neither
//! should be sacrificed to benefit the other.
//!
//! Row groups should implement indexing efficiently for `O(lg n)` seek by data
//! value and for sequential reads.  It should be possible to disable indexing
//! by data value for workloads that don't require it.[^0]
//!
//! Layer files support approximate set membership query in `~O(1)` time using
//! [a filter block](format::FilterBlock).
//!
//! Layer files should support 1 TB data size.
//!
//! Layer files should include data checksums to detect accidental corruption.
//!
//! # Design
//!
//! Layer files are stored as on-disk trees, one tree per column, with data
//! blocks as leaf nodes and index blocks as interior nodes.  Each tree's
//! branching factor is the number of values per data block and the number of
//! index entries per index block.  Block sizes and the branching factor can be
//! set as [parameters](`writer::Parameters`) at write time.
//!
//! Layer files support variable-length data in all columns.  The layer file
//! writer automatically detects fixed-length data and stores it slightly more
//! efficiently.
//!
//! Layer files index and compare data using [`Ord`] and [`Eq`], unlike many
//! data storage libraries that compare data lexicographically as byte arrays.
//! This convenience does prevent layer files from usefully storing only a
//! prefix of large data items plus a pointer to their full content.  In turn,
//! that means that, while layer files don't limit the size of data items, they
//! are always stored in full in index and data blocks, limiting performance for
//! large data.  This could be ameliorated if the layer file's clients were
//! permitted to provide a way to summarize data for comparisons.  The need for
//! this improvement is not yet clear, so it is not yet implemented.

// Warn about missing docs, but not for item declared with `#[cfg(test)]`.
#![cfg_attr(not(test), warn(missing_docs))]

use crate::dynamic::ArchivedDBData;
use crate::storage::buffer_cache::{FBuf, FBufSerializer};
use rkyv::de::deserializers::SharedDeserializeMap;
use rkyv::{
    ser::{
        serializers::{
            AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch, SharedSerializeMap,
        },
        Serializer as _,
    },
    Archive, Archived, Deserialize, Fallible, Serialize,
};
use std::cell::RefCell;
use std::fmt::Debug;
use std::{any::Any, sync::Arc};

pub mod format;
mod item;
pub mod reader;
pub mod writer;

use crate::{
    dynamic::{DataTrait, Erase, Factory, WithFactory},
    storage::file::item::RefTup2Factory,
    DBData,
};
pub use item::{ArchivedItem, Item, ItemFactory, WithItemFactory};

const BLOOM_FILTER_SEED: u128 = 42;
const BLOOM_FILTER_FALSE_POSITIVE_RATE: f64 = 0.001;

/// Factory objects used by file reader and writer.
pub struct Factories<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    /// Factory for creating instances of `K`.
    pub key_factory: &'static dyn Factory<K>,

    /// Factory for creating instances of `Item<K, A>`.
    pub item_factory: &'static dyn ItemFactory<K, A>,
}

impl<K, A> Clone for Factories<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            key_factory: self.key_factory,
            item_factory: self.item_factory,
        }
    }
}

impl<K, A> Factories<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    /// Create an instance of `Factories<K, A>` backed by concrete types
    /// `KType` and `AType`.
    pub fn new<KType, AType>() -> Self
    where
        KType: DBData + Erase<K>,
        AType: DBData + Erase<A>,
    {
        Self {
            key_factory: WithFactory::<KType>::FACTORY,
            item_factory: <RefTup2Factory<KType, AType> as WithItemFactory<K, A>>::ITEM_FACTORY,
        }
    }

    /// Convert `self` into an instance of [`AnyFactories`],
    /// which can be used in contexts where `K` and `A` traits
    /// are unknown.
    ///
    /// See documentation of [`AnyFactories`].
    pub(crate) fn any_factories(&self) -> AnyFactories {
        AnyFactories {
            key_factory: Arc::new(self.key_factory),
            item_factory: Arc::new(self.item_factory),
        }
    }
}

/// A type-erased version of [`Factories`].
///
/// File reader and writer objects have types that don't include their key and
/// aux types as type arguments, which allows us to have arrays of readers and
/// writers with different data types. This requires a type erased
/// representation of factory objects, which can be downcast to concrete factory
/// types on demand. This struct offers such a representation by casting key and
/// item factories to `dyn Any`.
#[derive(Clone)]
pub struct AnyFactories {
    key_factory: Arc<(dyn Any + Send + Sync + 'static)>,
    item_factory: Arc<(dyn Any + Send + Sync + 'static)>,
}

impl Debug for AnyFactories {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnyFactories").finish()
    }
}

impl AnyFactories {
    fn key_factory<K>(&self) -> &'static dyn Factory<K>
    where
        K: DataTrait + ?Sized,
    {
        *self
            .key_factory
            .as_ref()
            .downcast_ref::<&'static dyn Factory<K>>()
            .unwrap()
    }

    fn item_factory<K, A>(&self) -> &'static dyn ItemFactory<K, A>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        *self
            .item_factory
            .as_ref()
            .downcast_ref::<&'static dyn ItemFactory<K, A>>()
            .unwrap()
    }

    fn factories<K, A>(&self) -> Factories<K, A>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        Factories {
            key_factory: self.key_factory(),
            item_factory: self.item_factory(),
        }
    }
}

/// Trait for data that can be serialized and deserialized with [`rkyv`].
pub trait Rkyv: Archive + Serialize<Serializer> + Deserializable {}
impl<T> Rkyv for T where T: Archive + Serialize<Serializer> + Deserializable {}

/// Trait for data that can be deserialized with [`rkyv`].
pub trait Deserializable: Archive<Archived = Self::ArchivedDeser> + Sized {
    /// Deserialized type.
    type ArchivedDeser: Deserialize<Self, Deserializer>;
}
impl<T: Archive> Deserializable for T
where
    Archived<T>: Deserialize<T, Deserializer>,
{
    type ArchivedDeser = Archived<T>;
}

/// The particular [`rkyv::ser::Serializer`] that we use.
pub type Serializer = CompositeSerializer<FBufSerializer<FBuf>, DbspScratch, SharedSerializeMap>;

/// The particular [`rkyv::ser::ScratchSpace`] that we use.
pub type DbspScratch = FallbackScratch<HeapScratch<65536>, AllocScratch>;

/// The particular [`rkyv`] deserializer that we use.
pub type Deserializer = SharedDeserializeMap;

/// Creates an instance of [Serializer] that will serialize to `serializer` and
/// passes it to `f`. Returns a tuple of the `FBuf` from the [Serializer] and
/// the return value of `f`.
///
/// This is useful because it reuses the scratch space from one serializer to
/// the next, which is valuable because it saves an allocation and free per
/// serialization.
pub fn with_serializer<F, R>(serializer: FBufSerializer<FBuf>, f: F) -> (FBuf, R)
where
    F: FnOnce(&mut Serializer) -> R,
{
    thread_local! {
        static SCRATCH: RefCell<Option<DbspScratch>> = RefCell::new(Some(Default::default()));
    }

    let mut serializer = Serializer::new(serializer, SCRATCH.take().unwrap(), Default::default());
    let result = f(&mut serializer);
    let (serializer, scratch, _shared) = serializer.into_components();
    SCRATCH.replace(Some(scratch));
    (serializer.into_inner(), result)
}

/// Serializes the given value and returns the resulting bytes.
///
/// This is like [`rkyv::to_bytes`], but that function only works with one
/// particular serializer whereas this function works with our [`Serializer`].
pub fn to_bytes<T>(value: &T) -> Result<FBuf, <Serializer as Fallible>::Error>
where
    T: Serialize<Serializer>,
{
    let (bytes, result) = with_serializer(FBufSerializer::default(), |serializer| {
        serializer.serialize_value(value)
    });
    result?;
    Ok(bytes)
}

/// Serializes the given value and returns the resulting bytes.
///
/// This is like [`rkyv::to_bytes`], but that function only works with one
/// particular serializer whereas this function works with our [`Serializer`].
pub fn to_bytes_dyn<T>(value: &T) -> Result<FBuf, <Serializer as Fallible>::Error>
where
    T: ArchivedDBData,
{
    let (bytes, result) = with_serializer(FBufSerializer::default(), |serializer| {
        serializer.serialize_value(value)
    });
    result?;
    Ok(bytes)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::{
        storage::{
            backend::StorageBackend,
            buffer_cache::BufferCache,
            file::{format::Compression, reader::Reader},
            test::init_test_logger,
        },
        Runtime,
    };

    use super::{
        reader::{ColumnSpec, RowGroup},
        writer::{Parameters, Writer1, Writer2},
        Factories,
    };

    use crate::{
        dynamic::{DynData, Erase},
        DBData,
    };
    use feldera_types::config::{StorageConfig, StorageOptions};
    use rand::{seq::SliceRandom, thread_rng, Rng};
    use tempfile::tempdir;

    fn test_buffer_cache() -> Arc<BufferCache> {
        thread_local! {
            static BUFFER_CACHE: Arc<BufferCache> = Arc::new(BufferCache::new(1024 * 1024));
        }
        BUFFER_CACHE.with(|cache| cache.clone())
    }

    fn for_each_compression_type<F>(parameters: Parameters, f: F)
    where
        F: Fn(Parameters),
    {
        for compression in [None, Some(Compression::Snappy)] {
            print!("\n# testing with compression={compression:?}\n\n");
            f(parameters.clone().with_compression(compression));
        }
    }

    trait TwoColumns {
        type K0: DBData;
        type A0: DBData;
        type K1: DBData;
        type A1: DBData;

        fn n0() -> usize;
        fn key0(row0: usize) -> Self::K0;
        fn near0(row0: usize) -> (Self::K0, Self::K0);
        fn aux0(row0: usize) -> Self::A0;

        fn n1(row0: usize) -> usize;
        fn key1(row0: usize, row1: usize) -> Self::K1;
        fn near1(row0: usize, row1: usize) -> (Self::K1, Self::K1);
        fn aux1(row0: usize, row1: usize) -> Self::A1;
    }

    fn test_find<K, A, N, T>(
        row_group: &RowGroup<DynData, DynData, N, T>,
        before: &K,
        key: &K,
        after: &K,
        mut aux: A,
    ) where
        K: DBData,
        A: DBData,
        T: ColumnSpec,
    {
        let mut tmp_key = K::default();
        let mut tmp_aux = A::default();
        let (tmp_key, tmp_aux): (&mut DynData, &mut DynData) =
            (tmp_key.erase_mut(), tmp_aux.erase_mut());

        let mut key = key.clone();
        //let (key, aux): (&mut DynData, &mut DynData) = (key.erase_mut(),
        // aux.erase_mut());

        let mut cursor = row_group.first().unwrap();
        unsafe { cursor.advance_to_value_or_larger(key.erase()) }.unwrap();
        assert_eq!(
            unsafe { cursor.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );

        let mut cursor = row_group.first().unwrap();
        unsafe { cursor.advance_to_value_or_larger(before.erase()) }.unwrap();
        assert_eq!(
            unsafe { cursor.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );

        let mut cursor = row_group.first().unwrap();
        unsafe { cursor.seek_forward_until(|k| k >= key.erase()) }.unwrap();
        assert_eq!(
            unsafe { cursor.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );

        let mut cursor = row_group.first().unwrap();
        unsafe { cursor.seek_forward_until(|k| k >= before.erase()) }.unwrap();
        assert_eq!(
            unsafe { cursor.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );

        let mut cursor = row_group.last().unwrap();
        unsafe { cursor.rewind_to_value_or_smaller(key.erase()) }.unwrap();
        assert_eq!(
            unsafe { cursor.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );

        let mut cursor = row_group.last().unwrap();
        unsafe { cursor.rewind_to_value_or_smaller(after.erase()) }.unwrap();
        assert_eq!(
            unsafe { cursor.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );

        let mut cursor = row_group.last().unwrap();
        unsafe { cursor.seek_backward_until(|k| k <= key.erase()) }.unwrap();
        assert_eq!(
            unsafe { cursor.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );

        let mut cursor = row_group.last().unwrap();
        unsafe { cursor.seek_backward_until(|k| k <= after.erase()) }.unwrap();
        assert_eq!(
            unsafe { cursor.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );
    }

    fn test_out_of_range<K, A, N, T>(
        row_group: &RowGroup<DynData, DynData, N, T>,
        before: &K,
        after: &K,
    ) where
        K: DBData,
        A: DBData,
        T: ColumnSpec,
    {
        let mut tmp_key = K::default();
        let mut tmp_aux = A::default();
        let (tmp_key, tmp_aux): (&mut DynData, &mut DynData) =
            (tmp_key.erase_mut(), tmp_aux.erase_mut());

        let mut cursor = row_group.first().unwrap();
        unsafe { cursor.advance_to_value_or_larger(after.erase()) }.unwrap();
        assert_eq!(unsafe { cursor.item((tmp_key, tmp_aux)) }, None);

        cursor.move_first().unwrap();
        unsafe { cursor.seek_forward_until(|k| k >= after.erase()) }.unwrap();
        assert_eq!(unsafe { cursor.item((tmp_key, tmp_aux)) }, None);

        let mut cursor = row_group.last().unwrap();
        unsafe { cursor.rewind_to_value_or_smaller(before.erase()) }.unwrap();
        assert_eq!(unsafe { cursor.item((tmp_key, tmp_aux)) }, None);

        cursor.move_last().unwrap();
        unsafe { cursor.seek_backward_until(|k| k <= before.erase()) }.unwrap();
        assert_eq!(unsafe { cursor.item((tmp_key, tmp_aux)) }, None);
    }

    #[allow(clippy::len_zero)]
    fn test_cursor_helper<K, A, N, T>(
        rows: &RowGroup<DynData, DynData, N, T>,
        offset: u64,
        n: usize,
        expected: impl Fn(usize) -> (K, K, K, A),
    ) where
        K: DBData,
        A: DBData,
        T: ColumnSpec,
    {
        let mut tmp_key = K::default();
        let mut tmp_aux = A::default();
        let (tmp_key, tmp_aux): (&mut DynData, &mut DynData) =
            (tmp_key.erase_mut(), tmp_aux.erase_mut());

        assert_eq!(rows.len(), n as u64);

        assert_eq!(rows.len() == 0, rows.is_empty());
        assert_eq!(rows.before().len(), n as u64);
        assert_eq!(rows.after().len() == 0, rows.after().is_empty());

        let mut forward = rows.before();
        assert_eq!(unsafe { forward.item((tmp_key, tmp_aux)) }, None);
        forward.move_prev().unwrap();
        assert_eq!(unsafe { forward.item((tmp_key, tmp_aux)) }, None);
        forward.move_next().unwrap();
        for row in 0..n {
            let (_before, mut key, _after, mut aux) = expected(row);
            assert_eq!(
                unsafe { forward.item((tmp_key, tmp_aux)) },
                Some((key.erase_mut(), aux.erase_mut()))
            );
            forward.move_next().unwrap();
        }
        assert_eq!(unsafe { forward.item((tmp_key, tmp_aux)) }, None);
        forward.move_next().unwrap();
        assert_eq!(unsafe { forward.item((tmp_key, tmp_aux)) }, None);

        let mut backward = rows.after();
        assert_eq!(unsafe { backward.item((tmp_key, tmp_aux)) }, None);
        backward.move_next().unwrap();
        assert_eq!(unsafe { backward.item((tmp_key, tmp_aux)) }, None);
        backward.move_prev().unwrap();
        for row in (0..n).rev() {
            let (_before, mut key, _after, mut aux) = expected(row);
            assert_eq!(
                unsafe { backward.item((tmp_key, tmp_aux)) },
                Some((key.erase_mut(), aux.erase_mut()))
            );
            backward.move_prev().unwrap();
        }
        assert_eq!(unsafe { backward.item((tmp_key, tmp_aux)) }, None);
        backward.move_prev().unwrap();
        assert_eq!(unsafe { backward.item((tmp_key, tmp_aux)) }, None);

        for row in 0..n {
            let (before, key, after, aux) = expected(row);
            test_find(rows, &before, &key, &after, aux.clone());
        }

        let mut random = rows.before();
        let mut order: Vec<_> = (0..n + 10).collect();
        order.shuffle(&mut thread_rng());
        for row in order {
            random.move_to_row(row as u64).unwrap();
            assert_eq!(random.absolute_position(), offset + row.min(n) as u64);
            assert_eq!(random.remaining_rows(), (n - row.min(n)) as u64);
            if row < n {
                let (_before, mut key, _after, mut aux) = expected(row);
                assert_eq!(
                    unsafe { random.item((tmp_key, tmp_aux)) },
                    Some((key.erase_mut(), aux.erase_mut()))
                );
            } else {
                assert_eq!(unsafe { random.item((tmp_key, tmp_aux)) }, None);
            }
        }

        if n > 0 {
            let (before, _, _, _) = expected(0);
            let (_, _, after, _) = expected(n - 1);
            test_out_of_range::<K, A, N, T>(rows, &before, &after);
        }
    }

    fn test_cursor<K, A, N, T>(
        rows: &RowGroup<DynData, DynData, N, T>,
        n: usize,
        expected: impl Fn(usize) -> (K, K, K, A),
    ) where
        K: DBData,
        A: DBData,
        T: ColumnSpec,
    {
        let offset = rows.before().absolute_position();
        test_cursor_helper(rows, offset, n, &expected);

        let start = thread_rng().gen_range(0..n);
        let end = thread_rng().gen_range(start..=n);
        let subset = rows.subset(start as u64..end as u64);
        test_cursor_helper(&subset, offset + start as u64, end - start, |index| {
            expected(index + start)
        })
    }

    fn test_bloom<K, A, N>(
        reader: &Reader<(&'static DynData, &'static DynData, N)>,
        n: usize,
        expected: impl Fn(usize) -> (K, K, K, A),
    ) where
        K: DBData,
        A: DBData,
        N: ColumnSpec,
    {
        let mut false_positives = 0;
        for row in 0..n {
            let (before, key, after, _aux) = expected(row);
            assert!(reader.maybe_contains_key(&key));
            if reader.maybe_contains_key(&before) {
                false_positives += 1;
            }
            if reader.maybe_contains_key(&after) {
                false_positives += 1;
            }
        }
        if n >= 5 {
            // Note that, usually, `after` for row `i` is the same as `before`
            // for row `i + 1`, so the values in the data are not necessarily
            // *unique* values.
            assert!(false_positives < n,
                    "Out of {} values not in the data, {} appeared in the Bloom filter ({:.1}% false positive rate)",
                    2 * n, false_positives, false_positives as f64 / (2 * n) as f64);
        }
    }

    fn test_two_columns<T>(parameters: Parameters)
    where
        T: TwoColumns,
    {
        let factories0 = Factories::<DynData, DynData>::new::<T::K0, T::A0>();
        let factories1 = Factories::<DynData, DynData>::new::<T::K1, T::A1>();

        let tempdir = tempdir().unwrap();
        let storage_backend = <dyn StorageBackend>::new(
            &StorageConfig {
                path: tempdir.path().to_string_lossy().to_string(),
                cache: Default::default(),
            },
            &StorageOptions::default(),
        )
        .unwrap();
        let mut layer_file = Writer2::new(
            &factories0,
            &factories1,
            test_buffer_cache,
            &*storage_backend,
            parameters,
            T::n0(),
        )
        .unwrap();
        let n0 = T::n0();
        for row0 in 0..n0 {
            for row1 in 0..T::n1(row0) {
                layer_file
                    .write1((&T::key1(row0, row1), &T::aux1(row0, row1)))
                    .unwrap();
            }
            layer_file.write0((&T::key0(row0), &T::aux0(row0))).unwrap();
        }

        let reader = layer_file.into_reader().unwrap();
        reader.evict();
        let rows0 = reader.rows();
        let expected0 = |row0| {
            let key0 = T::key0(row0);
            let (before0, after0) = T::near0(row0);
            let aux0 = T::aux0(row0);
            (before0, key0, after0, aux0)
        };
        test_cursor(&rows0, n0, expected0);
        test_bloom(&reader, n0, expected0);

        for row0 in 0..n0 {
            let rows1 = rows0.nth(row0 as u64).unwrap().next_column().unwrap();
            let n1 = T::n1(row0);
            test_cursor(&rows1, n1, |row1| {
                let key1 = T::key1(row0, row1);
                let (before1, after1) = T::near1(row0, row1);
                let aux1 = T::aux1(row0, row1);
                (before1, key1, after1, aux1)
            });
        }
    }

    fn test_2_columns_helper(parameters: Parameters) {
        struct TwoInts;
        impl TwoColumns for TwoInts {
            type K0 = i32;
            type A0 = u64;
            type K1 = i32;
            type A1 = u64;

            fn n0() -> usize {
                500
            }
            fn key0(row0: usize) -> Self::K0 {
                row0 as i32 * 2
            }
            fn near0(row0: usize) -> (Self::K0, Self::K0) {
                let key0 = Self::key0(row0);
                (key0 - 1, key0 + 1)
            }
            fn aux0(_row0: usize) -> Self::A0 {
                0x1111
            }

            fn n1(_row0: usize) -> usize {
                7
            }
            fn key1(row0: usize, row1: usize) -> Self::K1 {
                (row0 + row1 * 2) as i32
            }
            fn near1(row0: usize, row1: usize) -> (Self::K1, Self::K1) {
                let key1 = Self::key1(row0, row1);
                (key1 - 1, key1 + 1)
            }
            fn aux1(_row0: usize, _row1: usize) -> Self::A1 {
                0x2222
            }
        }

        for_each_compression_type(parameters, |parameters| {
            test_two_columns::<TwoInts>(parameters)
        });
    }

    #[test]
    fn test_2_columns() {
        init_test_logger();
        test_2_columns_helper(Parameters::default());
    }

    #[test]
    fn test_2_columns_max_branch_2() {
        init_test_logger();
        test_2_columns_helper(Parameters::default().with_max_branch(2));
    }

    fn test_one_column<K, A>(
        n: usize,
        expected: impl Fn(usize) -> (K, K, K, A),
        parameters: Parameters,
    ) where
        K: DBData,
        A: DBData,
    {
        for_each_compression_type(parameters, |parameters| {
            for reopen in [false, true] {
                let factories = Factories::<DynData, DynData>::new::<K, A>();
                let tempdir = tempdir().unwrap();
                let storage_backend = <dyn StorageBackend>::new(
                    &StorageConfig {
                        path: tempdir.path().to_string_lossy().to_string(),
                        cache: Default::default(),
                    },
                    &StorageOptions::default(),
                )
                .unwrap();
                let mut writer = Writer1::new(
                    &factories,
                    test_buffer_cache,
                    &*storage_backend,
                    parameters.clone(),
                    n,
                )
                .unwrap();
                for row in 0..n {
                    let (_before, key, _after, aux) = expected(row);
                    writer.write0((&key, &aux)).unwrap();
                }

                let reader = if reopen {
                    println!("closing writer and reopening as reader");
                    let (_file_handle, path, _bloom_filter) = writer.close().unwrap();
                    Reader::open(
                        &[&factories.any_factories()],
                        Runtime::buffer_cache,
                        &*storage_backend,
                        &path,
                    )
                    .unwrap()
                } else {
                    println!("transforming writer into reader");
                    writer.into_reader().unwrap()
                };
                reader.evict();
                assert_eq!(reader.rows().len(), n as u64);
                test_cursor(&reader.rows(), n, &expected);
                test_bloom(&reader, n, &expected);
            }
        })
    }

    fn test_i64_helper(parameters: Parameters) {
        init_test_logger();
        test_one_column(
            1000,
            |row| (row as u64 * 2, row as u64 * 2 + 1, row as u64 * 2 + 2, ()),
            parameters,
        );
    }

    #[test]
    fn test_i64() {
        test_i64_helper(Parameters::default());
    }

    #[test]
    fn test_i64_max_branch_32() {
        test_i64_helper(Parameters::default().with_max_branch(32));
    }

    #[test]
    fn test_i64_max_branch_3() {
        test_i64_helper(Parameters::default().with_max_branch(3));
    }

    #[test]
    fn test_i64_max_branch_2() {
        test_i64_helper(Parameters::default().with_max_branch(2));
    }

    #[test]
    fn test_string() {
        fn f(x: usize) -> String {
            format!("{x:09}")
        }

        init_test_logger();
        test_one_column(
            1000,
            |row| (f(row * 2), f(row * 2 + 1), f(row * 2 + 2), ()),
            Parameters::default(),
        );
    }

    #[test]
    fn test_tuple() {
        init_test_logger();
        test_one_column(
            1000,
            |row| {
                (
                    (row as u64, 0),
                    (row as u64, 1),
                    (row as u64, 2),
                    row as u64,
                )
            },
            Parameters::default(),
        );
    }

    #[test]
    fn test_big_values() {
        fn v(row: usize) -> Vec<i64> {
            (0..row as i64).collect()
        }
        init_test_logger();
        test_one_column(
            500,
            |row| (v(row * 2), v(row * 2 + 1), v(row * 2 + 2), ()),
            Parameters::default(),
        );
    }
}
