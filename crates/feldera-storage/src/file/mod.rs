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
//! Layer files should support approximate set membership query in `~O(1)`
//! time.[^0]
//!
//! Layer files should support 1 TB data size.
//!
//! Layer files should include data checksums to detect accidental corruption.
//!
//! [^0]: Not yet implemented.
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

#![warn(missing_docs)]

#[cfg(doc)]
use crc32c::crc32c;
use rkyv::{
    ser::{
        serializers::{
            AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch, SharedSerializeMap,
        },
        Serializer as _,
    },
    Archive, Archived, Deserialize, Fallible, Infallible, Serialize,
};

use crate::buffer_cache::{FBuf, FBufSerializer};

pub mod format;
pub mod reader;
pub mod writer;

#[derive(Copy, Clone, Debug)]
struct InvalidBlockLocation {
    offset: u64,
    size: usize,
}

/// A block in a layer file.
///
/// Used for error reporting.
#[derive(Copy, Clone, Debug)]
struct BlockLocation {
    /// Byte offset, a multiple of 4096.
    offset: u64,

    /// Size in bytes, a power of 2 between 4096 and `2**31`.
    size: usize,
}

impl BlockLocation {
    fn new(offset: u64, size: usize) -> Result<Self, InvalidBlockLocation> {
        if (offset & 0xfff) != 0 || !(4096..=1 << 31).contains(&size) || !size.is_power_of_two() {
            Err(InvalidBlockLocation { offset, size })
        } else {
            Ok(Self { offset, size })
        }
    }
}

impl TryFrom<u64> for BlockLocation {
    type Error = InvalidBlockLocation;

    fn try_from(source: u64) -> Result<Self, Self::Error> {
        Self::new((source & !0x1f) << 7, 1 << (source & 0x1f))
    }
}

impl From<BlockLocation> for u64 {
    fn from(source: BlockLocation) -> Self {
        let shift = source.size.trailing_zeros() as u64;
        (source.offset >> 7) | shift
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
pub type Serializer = CompositeSerializer<
    FBufSerializer<FBuf>,
    FallbackScratch<HeapScratch<1024>, AllocScratch>,
    SharedSerializeMap,
>;

/// The particular [`rkyv`] deserializer that we use.
pub type Deserializer = Infallible;

/// Serializes the given value and returns the resulting bytes.
///
/// This is like [`rkyv::to_bytes`], but that function only works with one
/// particular serializer whereas this function works with our [`Serializer`].
pub fn to_bytes<T>(value: &T) -> Result<FBuf, <Serializer as Fallible>::Error>
where
    T: Serialize<Serializer>,
{
    let mut serializer = Serializer::default();
    serializer.serialize_value(value)?;
    Ok(serializer.into_serializer().into_inner())
}

#[cfg(test)]
mod test {
    use std::{fmt::Debug, rc::Rc};

    use crate::{
        backend::{DefaultBackend, StorageControl, StorageExecutor, StorageRead, StorageWrite},
        test::init_test_logger,
    };

    use super::{
        reader::{ColumnSpec, RowGroup},
        writer::{Parameters, Writer1, Writer2},
        Rkyv,
    };

    use rand::{seq::SliceRandom, thread_rng, Rng};

    trait TwoColumns {
        type K0: Rkyv + Debug + Ord + Eq + Clone;
        type A0: Rkyv + Debug + Eq + Clone;
        type K1: Rkyv + Debug + Ord + Eq + Clone;
        type A1: Rkyv + Debug + Eq + Clone;

        fn n0() -> usize;
        fn key0(row0: usize) -> Self::K0;
        fn near0(row0: usize) -> (Self::K0, Self::K0);
        fn aux0(row0: usize) -> Self::A0;

        fn n1(row0: usize) -> usize;
        fn key1(row0: usize, row1: usize) -> Self::K1;
        fn near1(row0: usize, row1: usize) -> (Self::K1, Self::K1);
        fn aux1(row0: usize, row1: usize) -> Self::A1;
    }

    fn test_find<S, K, A, N, T>(
        row_group: &RowGroup<S, K, A, N, T>,
        before: &K,
        key: &K,
        after: &K,
        aux: A,
    ) where
        S: StorageRead + StorageControl + StorageExecutor,
        K: Rkyv + Debug + Ord + Eq + Clone,
        A: Rkyv + Debug + Eq + Clone,
        T: ColumnSpec,
    {
        let mut cursor = row_group.first().unwrap();
        unsafe { cursor.advance_to_value_or_larger(key) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, Some((key.clone(), aux.clone())));

        let mut cursor = row_group.first().unwrap();
        unsafe { cursor.advance_to_value_or_larger(before) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, Some((key.clone(), aux.clone())));

        let mut cursor = row_group.first().unwrap();
        unsafe { cursor.seek_forward_until(|k| k >= key) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, Some((key.clone(), aux.clone())));

        let mut cursor = row_group.first().unwrap();
        unsafe { cursor.seek_forward_until(|k| k >= before) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, Some((key.clone(), aux.clone())));

        let mut cursor = row_group.last().unwrap();
        unsafe { cursor.rewind_to_value_or_smaller(key) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, Some((key.clone(), aux.clone())));

        let mut cursor = row_group.last().unwrap();
        unsafe { cursor.rewind_to_value_or_smaller(after) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, Some((key.clone(), aux.clone())));

        let mut cursor = row_group.last().unwrap();
        unsafe { cursor.seek_backward_until(|k| k <= key) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, Some((key.clone(), aux.clone())));

        let mut cursor = row_group.last().unwrap();
        unsafe { cursor.seek_backward_until(|k| k <= after) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, Some((key.clone(), aux.clone())));
    }

    fn test_out_of_range<S, K, A, N, T>(row_group: &RowGroup<S, K, A, N, T>, before: &K, after: &K)
    where
        S: StorageRead + StorageControl + StorageExecutor,
        K: Rkyv + Debug + Ord + Eq + Clone,
        A: Rkyv + Debug + Eq + Clone,
        T: ColumnSpec,
    {
        let mut cursor = row_group.first().unwrap();
        unsafe { cursor.advance_to_value_or_larger(after) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, None);

        cursor.move_first().unwrap();
        unsafe { cursor.seek_forward_until(|k| k >= after) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, None);

        let mut cursor = row_group.last().unwrap();
        unsafe { cursor.rewind_to_value_or_smaller(before) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, None);

        cursor.move_last().unwrap();
        unsafe { cursor.seek_backward_until(|k| k <= before) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, None);
    }

    #[allow(clippy::len_zero)]
    fn test_cursor_helper<S, K, A, N, T>(
        rows: &RowGroup<S, K, A, N, T>,
        offset: u64,
        n: usize,
        expected: impl Fn(usize) -> (K, K, K, A),
    ) where
        S: StorageRead + StorageControl + StorageExecutor,
        K: Rkyv + Debug + Ord + Eq + Clone,
        A: Rkyv + Debug + Eq + Clone,
        T: ColumnSpec,
    {
        assert_eq!(rows.len(), n as u64);

        assert_eq!(rows.len() == 0, rows.is_empty());
        assert_eq!(rows.before().len(), n as u64);
        assert_eq!(rows.after().len() == 0, rows.after().is_empty());

        let mut forward = rows.before();
        assert_eq!(unsafe { forward.item() }, None);
        forward.move_prev().unwrap();
        assert_eq!(unsafe { forward.item() }, None);
        forward.move_next().unwrap();
        for row in 0..n {
            let (_before, key, _after, aux) = expected(row);
            assert_eq!(unsafe { forward.item() }, Some((key, aux)));
            forward.move_next().unwrap();
        }
        assert_eq!(unsafe { forward.item() }, None);
        forward.move_next().unwrap();
        assert_eq!(unsafe { forward.item() }, None);

        let mut backward = rows.after();
        assert_eq!(unsafe { backward.item() }, None);
        backward.move_next().unwrap();
        assert_eq!(unsafe { backward.item() }, None);
        backward.move_prev().unwrap();
        for row in (0..n).rev() {
            let (_before, key, _after, aux) = expected(row);
            assert_eq!(unsafe { backward.item() }, Some((key, aux)));
            backward.move_prev().unwrap();
        }
        assert_eq!(unsafe { backward.item() }, None);
        backward.move_prev().unwrap();
        assert_eq!(unsafe { backward.item() }, None);

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
                let (_before, key, _after, aux) = expected(row);
                assert_eq!(unsafe { random.item() }, Some((key, aux)));
            } else {
                assert_eq!(unsafe { random.item() }, None);
            }
        }

        if n > 0 {
            let (before, _, _, _) = expected(0);
            let (_, _, after, _) = expected(n - 1);
            test_out_of_range(rows, &before, &after);
        }
    }

    fn test_cursor<S, K, A, N, T>(
        rows: &RowGroup<S, K, A, N, T>,
        n: usize,
        expected: impl Fn(usize) -> (K, K, K, A),
    ) where
        S: StorageRead + StorageControl + StorageExecutor,
        K: Rkyv + Debug + Ord + Eq + Clone,
        A: Rkyv + Debug + Eq + Clone,
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

    fn test_two_columns<S, T>(storage: &Rc<S>, parameters: Parameters)
    where
        S: StorageControl + StorageRead + StorageWrite + StorageExecutor,
        T: TwoColumns,
    {
        let mut layer_file = Writer2::new(storage, parameters).unwrap();
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
        let rows0 = reader.rows();
        test_cursor(&rows0, n0, |row0| {
            let key0 = T::key0(row0);
            let (before0, after0) = T::near0(row0);
            let aux0 = T::aux0(row0);
            (before0, key0, after0, aux0)
        });

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

    fn test_2_columns_helper<S>(storage: &Rc<S>, parameters: Parameters)
    where
        S: StorageControl + StorageRead + StorageWrite + StorageExecutor,
    {
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

        test_two_columns::<_, TwoInts>(storage, parameters);
    }

    #[test]
    fn test_2_columns() {
        init_test_logger();
        test_2_columns_helper(&DefaultBackend::default_for_thread(), Parameters::default());
    }

    #[cfg(feature = "glommio")]
    #[test]
    fn test_2_columns_glommio() {
        use crate::backend::glommio_impl::GlommioBackend;

        init_test_logger();
        test_2_columns_helper(&GlommioBackend::default_for_thread(), Parameters::default());
    }

    #[test]
    fn test_2_columns_max_branch_2() {
        init_test_logger();
        test_2_columns_helper(
            &DefaultBackend::default_for_thread(),
            Parameters::with_max_branch(2),
        );
    }

    fn test_one_column<S, K, A>(
        storage: &Rc<S>,
        n: usize,
        expected: impl Fn(usize) -> (K, K, K, A),
        parameters: Parameters,
    ) where
        S: StorageControl + StorageRead + StorageWrite + StorageExecutor,
        K: Rkyv + Debug + Ord + Eq + Clone,
        A: Rkyv + Debug + Eq + Clone,
    {
        let mut writer = Writer1::new(storage, parameters).unwrap();
        for row in 0..n {
            let (_before, key, _after, aux) = expected(row);
            writer.write0((&key, &aux)).unwrap();
        }

        let reader = writer.into_reader().unwrap();
        assert_eq!(reader.rows().len(), n as u64);
        test_cursor(&reader.rows(), n, expected);
    }

    fn test_i64_helper(parameters: Parameters) {
        init_test_logger();
        test_one_column(
            &DefaultBackend::default_for_thread(),
            1000,
            |row| (row * 2, row * 2 + 1, row * 2 + 2, ()),
            parameters,
        );
    }

    #[test]
    fn test_i64() {
        test_i64_helper(Parameters::default());
    }

    #[test]
    fn test_i64_max_branch_32() {
        test_i64_helper(Parameters::with_max_branch(32));
    }

    #[test]
    fn test_i64_max_branch_3() {
        test_i64_helper(Parameters::with_max_branch(3));
    }

    #[test]
    fn test_i64_max_branch_2() {
        test_i64_helper(Parameters::with_max_branch(2));
    }

    #[test]
    fn test_string() {
        fn f(x: usize) -> String {
            format!("{x:09}")
        }

        init_test_logger();
        test_one_column(
            &DefaultBackend::default_for_thread(),
            1000,
            |row| (f(row * 2), f(row * 2 + 1), f(row * 2 + 2), ()),
            Parameters::default(),
        );
    }

    #[test]
    fn test_tuple() {
        init_test_logger();
        test_one_column(
            &DefaultBackend::default_for_thread(),
            1000,
            |row| ((row, 0), (row, 1), (row, 2), row),
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
            &DefaultBackend::default_for_thread(),
            500,
            |row| (v(row * 2), v(row * 2 + 1), v(row * 2 + 2), ()),
            Parameters::default(),
        );
    }
}
