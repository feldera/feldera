use std::{
    io::Cursor,
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{
    DBWeight, Runtime,
    circuit::{CircuitConfig, CircuitStorageConfig},
    dynamic::{DataTrait, DowncastTrait, DynWeight, Factory, LeanVec, Vector, WithFactory},
    storage::{
        backend::{BlockLocation, StorageBackend},
        buffer_cache::BufferCache,
        file::{
            TouchedWindowCount,
            format::{
                BLOOM_FILTER_BLOCK_MAGIC, BatchMetadata, Compression, FileTrailer,
                INCOMPATIBLE_FEATURE_MODULAR_FILTERS, MODULAR_BLOOM_FILTER_BLOCK_MAGIC,
                ROARING_BITMAP_FILTER_BLOCK_MAGIC,
            },
            reader::{BulkRows, FilteredKeys, Reader},
        },
    },
    trace::{
        BatchReaderFactories, Builder, VecIndexedWSetFactories, VecWSetFactories,
        filter::BatchFilters,
        ord::vec::{indexed_wset_batch::VecIndexedWSetBuilder, wset_batch::VecWSetBuilder},
    },
    utils::{Tup1, test::init_test_logger},
};

use super::{
    Factories, FilterPlan,
    reader::{ColumnSpec, RowGroup},
    writer::{Parameters, Writer1, Writer2},
};

use crate::storage::file::FilterKind;
use crate::storage::{backend::StorageError, buffer_cache::FBuf};
use crate::{
    DBData,
    dynamic::{DynData, Erase},
};
use binrw::BinRead;
use feldera_storage::file::FileId;
use feldera_storage::{FileCommitter, FileReader, FileRw, StoragePath};
use feldera_types::config::{StorageConfig, StorageOptions};
use rand::{Rng, seq::SliceRandom, thread_rng};
use tempfile::tempdir;

feldera_macros::declare_tuple! {
    Tup65<
        T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18,
        T19, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29, T30, T31, T32, T33, T34, T35,
        T36, T37, T38, T39, T40, T41, T42, T43, T44, T45, T46, T47, T48, T49, T50, T51, T52,
        T53, T54, T55, T56, T57, T58, T59, T60, T61, T62, T63, T64
    >
}

type OptString = Option<String>;
type Tup65OptString = Tup65<
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
    OptString,
>;

// Map bits to fields in MSB->LSB order so row indices remain lexicographically sorted.
fn bit_set(bits: u128, idx: usize) -> bool {
    debug_assert!(idx < 65);
    ((bits >> (64 - idx)) & 1) != 0
}

fn opt_str(bit: bool) -> Option<String> {
    if bit { Some("abc".to_string()) } else { None }
}

fn tup65_from_bits(bits: u128) -> Tup65OptString {
    Tup65(
        opt_str(bit_set(bits, 0)),
        opt_str(bit_set(bits, 1)),
        opt_str(bit_set(bits, 2)),
        opt_str(bit_set(bits, 3)),
        opt_str(bit_set(bits, 4)),
        opt_str(bit_set(bits, 5)),
        opt_str(bit_set(bits, 6)),
        opt_str(bit_set(bits, 7)),
        opt_str(bit_set(bits, 8)),
        opt_str(bit_set(bits, 9)),
        opt_str(bit_set(bits, 10)),
        opt_str(bit_set(bits, 11)),
        opt_str(bit_set(bits, 12)),
        opt_str(bit_set(bits, 13)),
        opt_str(bit_set(bits, 14)),
        opt_str(bit_set(bits, 15)),
        opt_str(bit_set(bits, 16)),
        opt_str(bit_set(bits, 17)),
        opt_str(bit_set(bits, 18)),
        opt_str(bit_set(bits, 19)),
        opt_str(bit_set(bits, 20)),
        opt_str(bit_set(bits, 21)),
        opt_str(bit_set(bits, 22)),
        opt_str(bit_set(bits, 23)),
        opt_str(bit_set(bits, 24)),
        opt_str(bit_set(bits, 25)),
        opt_str(bit_set(bits, 26)),
        opt_str(bit_set(bits, 27)),
        opt_str(bit_set(bits, 28)),
        opt_str(bit_set(bits, 29)),
        opt_str(bit_set(bits, 30)),
        opt_str(bit_set(bits, 31)),
        opt_str(bit_set(bits, 32)),
        opt_str(bit_set(bits, 33)),
        opt_str(bit_set(bits, 34)),
        opt_str(bit_set(bits, 35)),
        opt_str(bit_set(bits, 36)),
        opt_str(bit_set(bits, 37)),
        opt_str(bit_set(bits, 38)),
        opt_str(bit_set(bits, 39)),
        opt_str(bit_set(bits, 40)),
        opt_str(bit_set(bits, 41)),
        opt_str(bit_set(bits, 42)),
        opt_str(bit_set(bits, 43)),
        opt_str(bit_set(bits, 44)),
        opt_str(bit_set(bits, 45)),
        opt_str(bit_set(bits, 46)),
        opt_str(bit_set(bits, 47)),
        opt_str(bit_set(bits, 48)),
        opt_str(bit_set(bits, 49)),
        opt_str(bit_set(bits, 50)),
        opt_str(bit_set(bits, 51)),
        opt_str(bit_set(bits, 52)),
        opt_str(bit_set(bits, 53)),
        opt_str(bit_set(bits, 54)),
        opt_str(bit_set(bits, 55)),
        opt_str(bit_set(bits, 56)),
        opt_str(bit_set(bits, 57)),
        opt_str(bit_set(bits, 58)),
        opt_str(bit_set(bits, 59)),
        opt_str(bit_set(bits, 60)),
        opt_str(bit_set(bits, 61)),
        opt_str(bit_set(bits, 62)),
        opt_str(bit_set(bits, 63)),
        opt_str(bit_set(bits, 64)),
    )
}

fn test_buffer_cache() -> Option<Arc<BufferCache>> {
    thread_local! {
        static BUFFER_CACHE: Arc<BufferCache> = Arc::new(BufferCache::new(1024 * 1024));
    }
    Some(BUFFER_CACHE.with(|cache| cache.clone()))
}

fn with_roaring_enabled<F>(f: F)
where
    F: FnOnce() + Clone + Send + 'static,
{
    let mut config = CircuitConfig::default();
    config.dev_tweaks.enable_roaring = Some(true);
    let (handle, ()) = Runtime::init_circuit(config, move |_| {
        f();
        Ok(())
    })
    .unwrap();
    handle.kill().unwrap();
}

/// Runs `f` inside a circuit configured with `rate`, which is what both the
/// writer and the filter loader consult.
///
/// The rate goes through the storage options rather than the deprecated dev
/// tweak, so the tests drive the same path production does. The circuit's own
/// storage is unused: `f` reaches its batch files through its own backend.
fn with_false_positive_rate<F>(rate: f64, f: F)
where
    F: FnOnce() + Clone + Send + 'static,
{
    let tempdir = tempdir().unwrap();
    let config = CircuitConfig::default().with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: tempdir.path().to_string_lossy().into_owned(),
                cache: Default::default(),
            },
            StorageOptions {
                bloom_false_positive_rate: Some(rate),
                ..StorageOptions::default()
            },
        )
        .unwrap(),
    ));
    let (handle, ()) = Runtime::init_circuit(config, move |_| {
        f();
        Ok(())
    })
    .unwrap();
    handle.kill().unwrap();
}

fn for_each_compression_type<F>(parameters: Parameters, f: F)
where
    F: Fn(Parameters),
{
    for compression in [
        None,
        Some(Compression::Snappy),
        Some(Compression::Lz4),
        Some(Compression::Zstd),
    ] {
        print!("\n# testing with compression={compression:?}\n\n");
        f(parameters.clone().with_compression(compression));
    }
}

trait TwoColumns {
    type K0: DBData;
    type A0: DBData;
    type K1: DBData;
    type A1: DBWeight;

    fn n0() -> usize;
    fn key0(row0: usize) -> Self::K0;
    fn near0(row0: usize) -> (Self::K0, Self::K0);
    fn aux0(row0: usize) -> Self::A0;

    fn n1(row0: usize) -> usize;
    fn key1(row0: usize, row1: usize) -> Self::K1;
    fn near1(row0: usize, row1: usize) -> (Self::K1, Self::K1);
    fn aux1(row0: usize, row1: usize) -> Self::A1;
}

struct Column0<T> {
    row: usize,
    _phantom: PhantomData<T>,
}

impl<T> Column0<T> {
    pub fn new() -> Self {
        Self {
            row: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T> Iterator for Column0<T>
where
    T: TwoColumns,
{
    type Item = (T::K0, T::A0);

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= T::n0() {
            None
        } else {
            let retval = Some((T::key0(self.row), T::aux0(self.row)));
            self.row += 1;
            retval
        }
    }
}

struct Column1<T> {
    row0: usize,
    row1: usize,
    _phantom: PhantomData<T>,
}

impl<T> Column1<T>
where
    T: TwoColumns,
{
    fn new() -> Self {
        Self {
            row0: 0,
            row1: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T> Iterator for Column1<T>
where
    T: TwoColumns,
{
    type Item = (T::K1, T::A1);

    fn next(&mut self) -> Option<Self::Item> {
        if self.row0 >= T::n0() {
            None
        } else {
            let retval = Some((T::key1(self.row0, self.row1), T::aux1(self.row0, self.row1)));
            self.row1 += 1;
            if self.row1 >= T::n1(self.row0) {
                self.row0 += 1;
                self.row1 = 0;
            }
            retval
        }
    }
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

    let mut cursor = unsafe { row_group.first().unwrap() };
    unsafe { cursor.advance_to_value_or_larger(key.erase()) }.unwrap();
    assert_eq!(
        unsafe { cursor.item((tmp_key, tmp_aux)) },
        Some((key.erase_mut(), aux.erase_mut()))
    );
    assert_eq!(cursor.key(), Some(key.erase()));

    let mut cursor = unsafe { row_group.first().unwrap() };
    unsafe { cursor.advance_to_value_or_larger(before.erase()) }.unwrap();
    assert_eq!(
        unsafe { cursor.item((tmp_key, tmp_aux)) },
        Some((key.erase_mut(), aux.erase_mut()))
    );
    assert_eq!(cursor.key(), Some(key.erase()));

    let mut cursor = unsafe { row_group.first().unwrap() };
    unsafe { cursor.seek_forward_until(|k| k >= key.erase()) }.unwrap();
    assert_eq!(
        unsafe { cursor.item((tmp_key, tmp_aux)) },
        Some((key.erase_mut(), aux.erase_mut()))
    );
    assert_eq!(cursor.key(), Some(key.erase()));

    let mut cursor = unsafe { row_group.first().unwrap() };
    unsafe { cursor.seek_forward_until(|k| k >= before.erase()) }.unwrap();
    assert_eq!(
        unsafe { cursor.item((tmp_key, tmp_aux)) },
        Some((key.erase_mut(), aux.erase_mut()))
    );
    assert_eq!(cursor.key(), Some(key.erase()));

    let mut cursor = unsafe { row_group.last().unwrap() };
    unsafe { cursor.rewind_to_value_or_smaller(key.erase()) }.unwrap();
    assert_eq!(
        unsafe { cursor.item((tmp_key, tmp_aux)) },
        Some((key.erase_mut(), aux.erase_mut()))
    );
    assert_eq!(cursor.key(), Some(key.erase()));

    let mut cursor = unsafe { row_group.last().unwrap() };
    unsafe { cursor.rewind_to_value_or_smaller(after.erase()) }.unwrap();
    assert_eq!(
        unsafe { cursor.item((tmp_key, tmp_aux)) },
        Some((key.erase_mut(), aux.erase_mut()))
    );
    assert_eq!(cursor.key(), Some(key.erase()));

    let mut cursor = unsafe { row_group.last().unwrap() };
    unsafe { cursor.seek_backward_until(|k| k <= key.erase()) }.unwrap();
    assert_eq!(
        unsafe { cursor.item((tmp_key, tmp_aux)) },
        Some((key.erase_mut(), aux.erase_mut()))
    );
    assert_eq!(cursor.key(), Some(key.erase()));

    let mut cursor = unsafe { row_group.last().unwrap() };
    unsafe { cursor.seek_backward_until(|k| k <= after.erase()) }.unwrap();
    assert_eq!(
        unsafe { cursor.item((tmp_key, tmp_aux)) },
        Some((key.erase_mut(), aux.erase_mut()))
    );
    assert_eq!(cursor.key(), Some(key.erase()));
}

/// Seeks one cursor again and again, rather than starting each seek from a
/// fresh cursor as [`test_find`] does.
///
/// A cursor that has already moved resumes its search from the path it holds:
/// it compares the row it is on, then the last row of its data block, and
/// only then climbs out through its index blocks and descends again.  A seek
/// from row 0 reaches those comparisons in one order and with one path; a
/// sequence of seeks walks a cursor whose path is partly consumed, which is
/// how a merge uses one.  The targets below are spaced so that consecutive
/// seeks land in the same data block, in the next one, and several blocks
/// further on.
///
/// A target the cursor has already passed must leave it where it is.  That is
/// the promise every one of these four makes, and the comparison against the
/// current row is what keeps it.
fn test_repeated_seeks<K, A, N, T>(
    rows: &RowGroup<DynData, DynData, N, T>,
    offset: u64,
    n: usize,
    expected: impl Fn(usize) -> (K, K, K, A),
) where
    K: DBData,
    A: DBData,
    T: ColumnSpec,
{
    if n == 0 {
        return;
    }
    let mut tmp_key = K::default();
    let mut tmp_aux = A::default();
    let (tmp_key, tmp_aux): (&mut DynData, &mut DynData) =
        (tmp_key.erase_mut(), tmp_aux.erase_mut());

    let targets: Vec<usize> = [
        0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987,
    ]
    .into_iter()
    .filter(|&row| row < n)
    .chain([n - 1])
    .collect();

    // Forward, by key and by predicate, each on a cursor of its own.
    for by_predicate in [false, true] {
        let mut cursor = unsafe { rows.first().unwrap() };
        let mut landed = 0;
        for &row in &targets {
            let (_before, mut key, _after, mut aux) = expected(row);
            if by_predicate {
                let target = key.clone();
                unsafe { cursor.seek_forward_until(|k| k >= target.erase()) }.unwrap();
            } else {
                unsafe { cursor.advance_to_value_or_larger(key.erase()) }.unwrap();
            }
            assert_eq!(cursor.absolute_position(), offset + row as u64);
            assert_eq!(
                unsafe { cursor.item((tmp_key, tmp_aux)) },
                Some((key.erase_mut(), aux.erase_mut()))
            );
            assert_eq!(cursor.key(), Some(key.erase()));

            // Seeking back to where it came from leaves it here.
            let (_before, mut behind, _after, _aux) = expected(landed);
            if by_predicate {
                let target = behind.clone();
                unsafe { cursor.seek_forward_until(|k| k >= target.erase()) }.unwrap();
            } else {
                unsafe { cursor.advance_to_value_or_larger(behind.erase_mut()) }.unwrap();
            }
            assert_eq!(cursor.absolute_position(), offset + row as u64);
            assert_eq!(cursor.key(), Some(key.erase()));
            landed = row;
        }
    }

    // Backward, the same way.
    for by_predicate in [false, true] {
        let mut cursor = unsafe { rows.last().unwrap() };
        let mut landed = n - 1;
        for &row in targets.iter().rev() {
            let (_before, mut key, _after, mut aux) = expected(row);
            if by_predicate {
                let target = key.clone();
                unsafe { cursor.seek_backward_until(|k| k <= target.erase()) }.unwrap();
            } else {
                unsafe { cursor.rewind_to_value_or_smaller(key.erase()) }.unwrap();
            }
            assert_eq!(cursor.absolute_position(), offset + row as u64);
            assert_eq!(
                unsafe { cursor.item((tmp_key, tmp_aux)) },
                Some((key.erase_mut(), aux.erase_mut()))
            );
            assert_eq!(cursor.key(), Some(key.erase()));

            // And a target ahead of it leaves it here.
            let (_before, mut ahead, _after, _aux) = expected(landed);
            if by_predicate {
                let target = ahead.clone();
                unsafe { cursor.seek_backward_until(|k| k <= target.erase()) }.unwrap();
            } else {
                unsafe { cursor.rewind_to_value_or_smaller(ahead.erase_mut()) }.unwrap();
            }
            assert_eq!(cursor.absolute_position(), offset + row as u64);
            assert_eq!(cursor.key(), Some(key.erase()));
            landed = row;
        }
    }
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

    let mut cursor = unsafe { row_group.first().unwrap() };
    unsafe { cursor.advance_to_value_or_larger(after.erase()) }.unwrap();
    assert_eq!(unsafe { cursor.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(cursor.key(), None);

    unsafe { cursor.move_first() }.unwrap();
    unsafe { cursor.seek_forward_until(|k| k >= after.erase()) }.unwrap();
    assert_eq!(unsafe { cursor.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(cursor.key(), None);

    let mut cursor = unsafe { row_group.last().unwrap() };
    unsafe { cursor.rewind_to_value_or_smaller(before.erase()) }.unwrap();
    assert_eq!(unsafe { cursor.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(cursor.key(), None);

    unsafe { cursor.move_last() }.unwrap();
    unsafe { cursor.seek_backward_until(|k| k <= before.erase()) }.unwrap();
    assert_eq!(unsafe { cursor.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(cursor.key(), None);
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
    assert_eq!(unsafe { rows.before() }.len(), n as u64);
    assert_eq!(
        unsafe { rows.after() }.len() == 0,
        unsafe { rows.after() }.is_empty()
    );

    if n > 0 {
        let first = unsafe { rows.first().unwrap() };
        let (_before, mut key, _after, mut aux) = expected(0);
        assert_eq!(
            unsafe { first.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );
        assert_eq!(first.key(), Some(key.erase()));

        let last = unsafe { rows.last().unwrap() };
        let (_before, mut key, _after, mut aux) = expected(n - 1);
        assert_eq!(
            unsafe { last.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );
        assert_eq!(last.key(), Some(key.erase()));
    }

    let mut forward = unsafe { rows.before() };
    assert_eq!(unsafe { forward.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(forward.key(), None);
    unsafe { forward.move_prev() }.unwrap();
    assert_eq!(unsafe { forward.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(forward.key(), None);
    unsafe { forward.move_next() }.unwrap();
    for row in 0..n {
        let (_before, mut key, _after, mut aux) = expected(row);
        assert_eq!(
            unsafe { forward.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );
        assert_eq!(forward.key(), Some(key.erase()));
        unsafe { forward.move_next() }.unwrap();
    }
    assert_eq!(unsafe { forward.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(forward.key(), None);
    unsafe { forward.move_next() }.unwrap();
    assert_eq!(unsafe { forward.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(forward.key(), None);

    let mut backward = unsafe { rows.after() };
    assert_eq!(unsafe { backward.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(backward.key(), None);
    unsafe { backward.move_next() }.unwrap();
    assert_eq!(unsafe { backward.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(backward.key(), None);
    unsafe { backward.move_prev() }.unwrap();
    for row in (0..n).rev() {
        let (_before, mut key, _after, mut aux) = expected(row);
        assert_eq!(
            unsafe { backward.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );
        assert_eq!(backward.key(), Some(key.erase()));
        unsafe { backward.move_prev() }.unwrap();
    }
    assert_eq!(unsafe { backward.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(backward.key(), None);
    unsafe { backward.move_prev() }.unwrap();
    assert_eq!(unsafe { backward.item((tmp_key, tmp_aux)) }, None);
    assert_eq!(backward.key(), None);

    for row in 0..n {
        let (before, key, after, aux) = expected(row);
        test_find(rows, &before, &key, &after, aux.clone());
    }

    test_repeated_seeks(rows, offset, n, &expected);

    let mut random = unsafe { rows.before() };
    let mut order: Vec<_> = (0..n + 10).collect();
    order.shuffle(&mut thread_rng());
    for row in order {
        unsafe { random.move_to_row(row as u64) }.unwrap();
        assert_eq!(random.absolute_position(), offset + row.min(n) as u64);
        assert_eq!(random.remaining_rows(), (n - row.min(n)) as u64);
        if row < n {
            let (_before, mut key, _after, mut aux) = expected(row);
            assert_eq!(
                unsafe { random.item((tmp_key, tmp_aux)) },
                Some((key.erase_mut(), aux.erase_mut()))
            );
            assert_eq!(random.key(), Some(key.erase()));
        } else {
            assert_eq!(unsafe { random.item((tmp_key, tmp_aux)) }, None);
            assert_eq!(random.key(), None);
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
    let offset = unsafe { rows.before().absolute_position() };
    test_cursor_helper(rows, offset, n, &expected);

    let start = thread_rng().gen_range(0..n);
    let end = thread_rng().gen_range(start..=n);
    let subset = rows.subset(start as u64..end as u64);
    test_cursor_helper(&subset, offset + start as u64, end - start, |index| {
        expected(index + start)
    })
}

fn test_bulk_rows<K, A, N, T>(
    mut bulk: BulkRows<DynData, DynData, N, T>,
    mut expected: impl Iterator<Item = (K, A)>,
) where
    K: DBData,
    A: DBData,
    T: ColumnSpec,
{
    let mut tmp_key = K::default();
    let mut tmp_aux = A::default();
    let (tmp_key, tmp_aux): (&mut DynData, &mut DynData) =
        (tmp_key.erase_mut(), tmp_aux.erase_mut());

    while !bulk.at_eof() {
        bulk.wait().unwrap();
        let (mut key, mut aux) = expected.next().unwrap();
        assert_eq!(
            unsafe { bulk.item((tmp_key, tmp_aux)) },
            Some((key.erase_mut(), aux.erase_mut()))
        );
        bulk.step();
    }
    assert!(expected.next().is_none());
}

fn test_multifetch_zset<K, A>(
    reader: &Reader<(&'static DynData, &'static DynWeight, ())>,
    n: usize,
    expected_fn: impl Fn(usize) -> (K, K, K, A),
) where
    K: DBData,
    A: DBWeight,
{
    let keys_factory: &dyn Factory<dyn Vector<DynData>> = WithFactory::<LeanVec<K>>::FACTORY;

    let vec_wset_factories = VecWSetFactories::new::<K, (), A>();
    let mut expected = VecWSetBuilder::new_builder(&vec_wset_factories);

    let mut keys = keys_factory.default_box();
    for i in 0..n {
        // `before` sorts between this key and the one before it, so it is in
        // the file's range and in the block a search would look in, and it is
        // not in the file.  Asking for it says that a key the file does not
        // hold is reported as missing rather than as its neighbour.
        let (before, key, _after, diff) = (expected_fn)(i);
        if rand::random() {
            keys.push_ref(&before);
        }
        if rand::random() {
            keys.push_ref(&key);
            expected.push_val_diff(().erase(), diff.erase());
            expected.push_key(key.erase());
        }
    }
    let expected = expected.done();

    let mut multifetch = reader.fetch_zset(FilteredKeys::all(&*keys)).unwrap();
    while !multifetch.is_done() {
        multifetch.wait().unwrap();
    }
    let output = multifetch.results(vec_wset_factories);
    assert_eq!(&output, &expected);
}

fn test_multifetch_two_columns<T>(
    reader: &Reader<(
        &'static DynData,
        &'static DynData,
        (&'static DynData, &'static DynWeight, ()),
    )>,
) where
    T: TwoColumns,
{
    let keys_factory: &dyn Factory<dyn Vector<DynData>> = WithFactory::<LeanVec<T::K0>>::FACTORY;

    let vec_indexed_wset_factories = VecIndexedWSetFactories::new::<T::K0, T::K1, T::A1>();
    let mut expected = VecIndexedWSetBuilder::new_builder(&vec_indexed_wset_factories);

    let mut keys = keys_factory.default_box();
    for i in 0..T::n0() {
        if rand::random() {
            keys.push_ref(&T::key0(i));

            for j in 0..T::n1(i) {
                let val = T::key1(i, j);
                let weight = T::aux1(i, j);
                expected.push_val_diff(val.erase(), weight.erase());
            }
            let key = T::key0(i);
            expected.push_key(key.erase());
        }
    }
    let expected = expected.done();

    let mut multifetch = reader
        .fetch_indexed_zset(FilteredKeys::all(&*keys))
        .unwrap();
    while !multifetch.is_done() {
        multifetch.wait().unwrap();
    }
    let output = multifetch.results(vec_indexed_wset_factories);
    assert_eq!(&output, &expected);
}

fn test_bloom<K, A>(
    filters: &BatchFilters<DynData>,
    n: usize,
    expected: impl Fn(usize) -> (K, K, K, A),
) where
    K: DBData + Erase<DynData>,
    A: DBData,
{
    let mut false_positives = 0;
    for row in 0..n {
        let (before, key, after, _aux) = expected(row);
        assert!(filters.maybe_contains_key(key.erase(), None));
        if filters.maybe_contains_key(before.erase(), None) {
            false_positives += 1;
        }
        if filters.maybe_contains_key(after.erase(), None) {
            false_positives += 1;
        }
    }
    if n >= 5 {
        // Note that, usually, `after` for row `i` is the same as `before`
        // for row `i + 1`, so the values in the data are not necessarily
        // *unique* values.
        assert!(
            false_positives < n,
            "Out of {} values not in the data, {} appeared in the Bloom filter ({:.1}% false positive rate)",
            2 * n,
            false_positives,
            false_positives as f64 / (2 * n) as f64
        );
    }
}

fn test_key_range<K, A, Aux, N>(
    reader: &Reader<(&'static DynData, &'static Aux, N)>,
    n: usize,
    expected: impl Fn(usize) -> (K, K, K, A),
) where
    K: DBData,
    A: DBData,
    Aux: DataTrait + ?Sized,
    N: ColumnSpec,
{
    let key_range = reader.key_range().unwrap();
    if n == 0 {
        assert!(key_range.is_none());
        return;
    }

    let Some((min, max)) = key_range else {
        panic!("expected non-empty key range");
    };
    let (_, expected_min, _, _) = expected(0);
    let (_, expected_max, _, _) = expected(n - 1);
    assert_eq!(min.downcast_checked::<K>(), &expected_min);
    assert_eq!(max.downcast_checked::<K>(), &expected_max);
}

fn filter_block_magic<K, A, N>(reader: &Reader<(&'static K, &'static A, N)>) -> Option<[u8; 4]>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    (&'static K, &'static A, N): ColumnSpec,
{
    let file_size = reader.byte_size().unwrap() as usize;
    let trailer_block = reader
        .file_handle()
        .read_block(BlockLocation::new((file_size - 512) as u64, 512).unwrap())
        .unwrap();
    let trailer = FileTrailer::read_le(&mut Cursor::new(trailer_block.as_slice())).unwrap();
    let offset = if trailer.has_filter64() {
        trailer.filter_offset64
    } else {
        trailer.filter_offset
    };
    let size = if trailer.has_filter64() {
        trailer.filter_size64 as usize
    } else {
        trailer.filter_size as usize
    };
    if offset == 0 {
        return None;
    }

    let filter_block = reader
        .file_handle()
        .read_block(BlockLocation::new(offset, size).unwrap())
        .unwrap();
    let mut magic = [0u8; 4];
    magic.copy_from_slice(&filter_block[4..8]);
    Some(magic)
}

fn incompatible_features<K, A, N>(reader: &Reader<(&'static K, &'static A, N)>) -> u64
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    (&'static K, &'static A, N): ColumnSpec,
{
    let file_size = reader.byte_size().unwrap() as usize;
    let trailer_block = reader
        .file_handle()
        .read_block(BlockLocation::new((file_size - 512) as u64, 512).unwrap())
        .unwrap();
    let trailer = FileTrailer::read_le(&mut Cursor::new(trailer_block.as_slice())).unwrap();
    trailer.incompatible_features
}

fn filter_plan_from_keys<K>(keys: &[K]) -> FilterPlan<DynData>
where
    K: DBData + Erase<DynData>,
{
    FilterPlan::from_bounds(keys.first().unwrap().erase(), keys.last().unwrap().erase())
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
        FilterPlan::<DynData>::decide_filter(None, T::n0()),
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

    let (reader, filters) = layer_file.into_reader(BatchMetadata::default()).unwrap();
    reader.evict();
    let rows0 = reader.rows();
    let expected0 = |row0| {
        let key0 = T::key0(row0);
        let (before0, after0) = T::near0(row0);
        let aux0 = T::aux0(row0);
        (before0, key0, after0, aux0)
    };
    test_cursor(&rows0, n0, expected0);
    test_bloom(&filters, n0, expected0);
    test_key_range(&reader, n0, expected0);

    let expected1 = |row0, row1| {
        let key1 = T::key1(row0, row1);
        let (before1, after1) = T::near1(row0, row1);
        let aux1 = T::aux1(row0, row1);
        (before1, key1, after1, aux1)
    };
    for row0 in 0..n0 {
        let rows1 = rows0.nth(row0 as u64).unwrap().next_column().unwrap();
        let n1 = T::n1(row0);
        test_cursor(&rows1, n1, |row1| expected1(row0, row1));
    }
    test_bulk_rows(reader.bulk_rows().unwrap(), Column0::<T>::new());
    test_bulk_rows(
        reader.bulk_rows().unwrap().next_column().unwrap(),
        Column1::<T>::new(),
    );
}

fn test_two_columns_multifetch<T>(parameters: Parameters)
where
    T: TwoColumns,
{
    let factories0 = Factories::<DynData, DynData>::new::<T::K0, T::A0>();
    let factories1 = Factories::<DynData, DynWeight>::new::<T::K1, T::A1>();

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
        FilterPlan::<DynData>::decide_filter(None, T::n0()),
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

    let (reader, _filters) = layer_file.into_reader(BatchMetadata::default()).unwrap();
    reader.evict();
    test_multifetch_two_columns::<T>(&reader);
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
    test_two_columns::<TwoInts>(parameters.clone());
    test_two_columns_multifetch::<TwoInts>(parameters);
}

#[test]
fn two_columns_uncompressed() {
    init_test_logger();
    test_2_columns_helper(Parameters::default().with_compression(None));
}

#[test]
fn two_columns_snappy() {
    init_test_logger();
    test_2_columns_helper(Parameters::default().with_compression(Some(Compression::Snappy)));
}

#[test]
fn two_columns_lz4() {
    init_test_logger();
    test_2_columns_helper(Parameters::default().with_compression(Some(Compression::Lz4)));
}

#[test]
fn two_columns_zstd() {
    init_test_logger();
    test_2_columns_helper(Parameters::default().with_compression(Some(Compression::Zstd)));
}

/// Zstd levels are a writer-side choice that must not change what reads back.
#[test]
fn two_columns_zstd_levels() {
    init_test_logger();
    for level in [1, 3, 9] {
        test_2_columns_helper(
            Parameters::default()
                .with_compression(Some(Compression::Zstd))
                .with_compression_level(Some(level)),
        );
    }
}

/// The level reaches us from user configuration, so an absurd one must still
/// produce a readable file.
///
/// Note this passes with or without the explicit clamp in `BlockWriter::new`,
/// because zstd also clamps the level itself. The clamp is kept so the
/// behaviour does not depend on that, but this test does not prove it is
/// there; it pins the observable property that any configured level works.
#[test]
fn two_columns_zstd_level_out_of_range() {
    init_test_logger();
    for level in [i32::MIN, -1000000, 1000000, i32::MAX] {
        test_2_columns_helper(
            Parameters::default()
                .with_compression(Some(Compression::Zstd))
                .with_compression_level(Some(level)),
        );
    }
}

#[test]
fn two_columns_max_branch_2_uncompressed() {
    init_test_logger();
    test_2_columns_helper(
        Parameters::default()
            .with_max_branch(2)
            .with_compression(None),
    );
}

#[test]
fn two_columns_max_branch_2_snappy() {
    init_test_logger();
    test_2_columns_helper(
        Parameters::default()
            .with_max_branch(2)
            .with_compression(Some(Compression::Snappy)),
    );
}

struct OneColumn<'a, T> {
    expected: &'a T,
    row: usize,
    n: usize,
}

impl<'a, T> OneColumn<'a, T> {
    pub fn new(expected: &'a T, n: usize) -> Self {
        Self {
            expected,
            row: 0,
            n,
        }
    }
}

impl<'a, T, K, A> Iterator for OneColumn<'a, T>
where
    T: Fn(usize) -> (K, K, K, A),
{
    type Item = (K, A);

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.n {
            None
        } else {
            let (_before, key, _after, aux) = (self.expected)(self.row);
            let retval = Some((key, aux));
            self.row += 1;
            retval
        }
    }
}

fn test_one_column<K, A>(n: usize, expected: impl Fn(usize) -> (K, K, K, A), parameters: Parameters)
where
    K: DBData,
    A: DBData,
{
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
            FilterPlan::<DynData>::decide_filter(None, n),
        )
        .unwrap();
        for row in 0..n {
            let (_before, key, _after, aux) = expected(row);
            writer.write0((&key, &aux)).unwrap();
        }

        let (reader, filters) = if reopen {
            println!("closing writer and reopening as reader");
            let path = writer.path().clone();
            let (_file_handle, _key_filter, _key_bounds) =
                writer.close(BatchMetadata::default()).unwrap();
            let (reader, membership_filter) = Reader::open_with_filter(
                &[&factories.any_factories()],
                test_buffer_cache,
                &*storage_backend,
                &path,
            )
            .unwrap();
            let key_range = reader.key_range().unwrap().map(Into::into);
            let filters = BatchFilters::from_file(key_range, membership_filter);
            (reader, filters)
        } else {
            println!("transforming writer into reader");
            let (reader, filters) = writer.into_reader(BatchMetadata::default()).unwrap();
            (reader, filters)
        };
        reader.evict();
        assert_eq!(reader.rows().len(), n as u64);
        test_cursor(&reader.rows(), n, &expected);
        test_bloom(&filters, n, &expected);
        test_key_range(&reader, n, &expected);
        test_bulk_rows(reader.bulk_rows().unwrap(), OneColumn::new(&expected, n));
    }
}

fn test_one_column_zset<K, A>(
    n: usize,
    expected: impl Fn(usize) -> (K, K, K, A),
    parameters: Parameters,
) where
    K: DBData,
    A: DBWeight,
{
    for reopen in [false, true] {
        let factories = Factories::<DynData, DynWeight>::new::<K, A>();
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
            FilterPlan::<DynData>::decide_filter(None, n),
        )
        .unwrap();
        for row in 0..n {
            let (_before, key, _after, aux) = expected(row);
            writer.write0((&key, &aux)).unwrap();
        }

        let reader = if reopen {
            println!("closing writer and reopening as reader");
            let path = writer.path().clone();
            let (_file_handle, _key_filter, _key_bounds) =
                writer.close(BatchMetadata::default()).unwrap();
            Reader::open(
                &[&factories.any_factories()],
                test_buffer_cache,
                &*storage_backend,
                &path,
            )
            .unwrap()
        } else {
            println!("transforming writer into reader");
            let (reader, _filters) = writer.into_reader(BatchMetadata::default()).unwrap();
            reader
        };
        reader.evict();
        assert_eq!(reader.rows().len(), n as u64);
        test_key_range(&reader, n, &expected);
        test_multifetch_zset(&reader, n, &expected);
    }
}

#[test]
fn one_column_key_range() {
    init_test_logger();

    for reopen in [false, true] {
        for (label, keys) in [
            ("negative", [-30_i64, -20, -10]),
            ("positive", [10_i64, 20, 30]),
            ("mixed", [-30_i64, 0, 20]),
        ] {
            let factories = Factories::<DynData, DynData>::new::<i64, ()>();
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
                Parameters::default(),
                FilterPlan::<DynData>::decide_filter(None, keys.len()),
            )
            .unwrap();
            for key in keys {
                writer.write0((&key, &())).unwrap();
            }

            let reader = if reopen {
                let path = writer.path().clone();
                let (_file_handle, _key_filter, _key_bounds) =
                    writer.close(BatchMetadata::default()).unwrap();
                Reader::open(
                    &[&factories.any_factories()],
                    test_buffer_cache,
                    &*storage_backend,
                    &path,
                )
                .unwrap()
            } else {
                let (reader, _filters) = writer.into_reader(BatchMetadata::default()).unwrap();
                reader
            };

            let Some((min, max)) = reader.key_range().unwrap() else {
                panic!("expected non-empty key range for {label}");
            };
            assert_eq!(*min.downcast_checked::<i64>(), keys[0], "{label}");
            assert_eq!(
                *max.downcast_checked::<i64>(),
                keys[keys.len() - 1],
                "{label}"
            );
        }
    }
}

#[test]
fn persists_touched_window_count_in_metadata() {
    let factories = Factories::<DynData, DynData>::new::<u32, ()>();
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
        Parameters::default(),
        FilterPlan::<DynData>::decide_filter(None, 6),
    )
    .unwrap();

    for key in [0u32, 1, 1 << 16, (1 << 16) + 1, 3 << 16, (3 << 16) + 1] {
        writer.write0((&key, &())).unwrap();
    }

    let metadata = BatchMetadata {
        touched_window_count: TouchedWindowCount::new(3),
        ..BatchMetadata::default()
    };
    let (reader, _filters) = writer.into_reader(metadata).unwrap();
    assert_eq!(
        reader.metadata().touched_window_count,
        TouchedWindowCount::new(3)
    );
}

#[test]
fn bloom_filter_roundtrip_and_block_kind() {
    init_test_logger();

    for reopen in [false, true] {
        let factories = Factories::<DynData, DynData>::new::<i64, ()>();
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
            Parameters::default(),
            FilterPlan::<DynData>::decide_filter(None, 3),
        )
        .unwrap();
        for key in [1i64, 3, 7] {
            writer.write0((&key, &())).unwrap();
        }

        let (reader, filters) = if reopen {
            let path = writer.path().clone();
            let (_file_handle, _key_filter, _key_bounds) =
                writer.close(BatchMetadata::default()).unwrap();
            let (reader, membership_filter) = Reader::open_with_filter(
                &[&factories.any_factories()],
                test_buffer_cache,
                &*storage_backend,
                &path,
            )
            .unwrap();
            let key_range = reader.key_range().unwrap().map(Into::into);
            let filters = BatchFilters::from_file(key_range, membership_filter);
            (reader, filters)
        } else {
            writer.into_reader(BatchMetadata::default()).unwrap()
        };

        for key in [1i64, 3, 7] {
            assert!(filters.maybe_contains_key(key.erase(), None));
        }
        assert_eq!(
            filter_block_magic(&reader),
            Some(MODULAR_BLOOM_FILTER_BLOCK_MAGIC)
        );
        // A file carrying more than one module is deliberately unreadable by
        // binaries that predate them, so they fail loudly rather than mis-parse.
        assert_eq!(
            incompatible_features(&reader),
            INCOMPATIBLE_FEATURE_MODULAR_FILTERS
        );
    }
}

#[test]
fn roaring_u32_filter_roundtrip_exact_and_block_kind() {
    init_test_logger();

    with_roaring_enabled(|| {
        for reopen in [false, true] {
            let factories = Factories::<DynData, DynData>::new::<u32, ()>();
            let tempdir = tempdir().unwrap();
            let storage_backend = <dyn StorageBackend>::new(
                &StorageConfig {
                    path: tempdir.path().to_string_lossy().to_string(),
                    cache: Default::default(),
                },
                &StorageOptions::default(),
            )
            .unwrap();

            let filter_plan = filter_plan_from_keys(&[1u32, 3, 7]);
            let mut writer = Writer1::new(
                &factories,
                test_buffer_cache,
                &*storage_backend,
                Parameters::default(),
                FilterPlan::decide_filter(Some(&filter_plan), 3),
            )
            .unwrap();
            for key in [1u32, 3, 7] {
                writer.write0((&key, &())).unwrap();
            }

            let (reader, filters) = if reopen {
                let path = writer.path().clone();
                let (_file_handle, _key_filter, _key_bounds) =
                    writer.close(BatchMetadata::default()).unwrap();
                let (reader, membership_filter) = Reader::open_with_filter(
                    &[&factories.any_factories()],
                    test_buffer_cache,
                    &*storage_backend,
                    &path,
                )
                .unwrap();
                let key_range = reader.key_range().unwrap().map(Into::into);
                let filters = BatchFilters::from_file(key_range, membership_filter);
                (reader, filters)
            } else {
                writer.into_reader(BatchMetadata::default()).unwrap()
            };

            for key in [1u32, 3, 7] {
                assert!(filters.maybe_contains_key(key.erase(), None));
            }
            for key in [0u32, 2, 9] {
                assert!(!filters.maybe_contains_key(key.erase(), None));
            }
            assert_eq!(
                filter_block_magic(&reader),
                Some(ROARING_BITMAP_FILTER_BLOCK_MAGIC)
            );
            assert_ne!(incompatible_features(&reader), 0);
        }
    });
}

#[test]
fn roaring_tup1_i32_filter_roundtrip_exact_and_block_kind() {
    init_test_logger();

    with_roaring_enabled(|| {
        for reopen in [false, true] {
            let factories = Factories::<DynData, DynData>::new::<Tup1<i32>, ()>();
            let tempdir = tempdir().unwrap();
            let storage_backend = <dyn StorageBackend>::new(
                &StorageConfig {
                    path: tempdir.path().to_string_lossy().to_string(),
                    cache: Default::default(),
                },
                &StorageOptions::default(),
            )
            .unwrap();

            let filter_plan = filter_plan_from_keys(&[Tup1(-7i32), Tup1(1), Tup1(3)]);
            let mut writer = Writer1::new(
                &factories,
                test_buffer_cache,
                &*storage_backend,
                Parameters::default(),
                FilterPlan::decide_filter(Some(&filter_plan), 3),
            )
            .unwrap();
            for key in [Tup1(-7i32), Tup1(1), Tup1(3)] {
                writer.write0((&key, &())).unwrap();
            }

            let (reader, filters) = if reopen {
                let path = writer.path().clone();
                let (_file_handle, _key_filter, _key_bounds) =
                    writer.close(BatchMetadata::default()).unwrap();
                let (reader, membership_filter) = Reader::open_with_filter(
                    &[&factories.any_factories()],
                    test_buffer_cache,
                    &*storage_backend,
                    &path,
                )
                .unwrap();
                let key_range = reader.key_range().unwrap().map(Into::into);
                let filters = BatchFilters::from_file(key_range, membership_filter);
                (reader, filters)
            } else {
                writer.into_reader(BatchMetadata::default()).unwrap()
            };

            for key in [Tup1(-7i32), Tup1(1), Tup1(3)] {
                assert!(filters.maybe_contains_key(key.erase(), None));
            }
            for key in [Tup1(-8i32), Tup1(0), Tup1(9)] {
                assert!(!filters.maybe_contains_key(key.erase(), None));
            }
            assert_eq!(
                filter_block_magic(&reader),
                Some(ROARING_BITMAP_FILTER_BLOCK_MAGIC)
            );
            assert_ne!(incompatible_features(&reader), 0);
        }
    });
}

#[test]
fn writer_without_filter_plan_uses_bloom_filter() {
    init_test_logger();

    with_roaring_enabled(|| {
        let factories = Factories::<DynData, DynData>::new::<u32, ()>();
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
            Parameters::default(),
            FilterPlan::<DynData>::decide_filter(None, 2),
        )
        .unwrap();
        for key in [5u32, 8] {
            writer.write0((&key, &())).unwrap();
        }

        let (reader, _filters) = writer.into_reader(BatchMetadata::default()).unwrap();
        assert_eq!(
            filter_block_magic(&reader),
            Some(MODULAR_BLOOM_FILTER_BLOCK_MAGIC)
        );
    });
}

#[test]
fn roaring_i64_filter_roundtrip_uses_batch_min_offset() {
    init_test_logger();

    with_roaring_enabled(|| {
        for reopen in [false, true] {
            let factories = Factories::<DynData, DynData>::new::<i64, ()>();
            let tempdir = tempdir().unwrap();
            let storage_backend = <dyn StorageBackend>::new(
                &StorageConfig {
                    path: tempdir.path().to_string_lossy().to_string(),
                    cache: Default::default(),
                },
                &StorageOptions::default(),
            )
            .unwrap();

            let min = (i64::from(u32::MAX) * 4) + 10;
            let keys = [min, min + 3, min + 7];
            let filter_plan = filter_plan_from_keys(&keys);
            let mut writer = Writer1::new(
                &factories,
                test_buffer_cache,
                &*storage_backend,
                Parameters::default(),
                FilterPlan::decide_filter(Some(&filter_plan), keys.len()),
            )
            .unwrap();
            for key in keys {
                writer.write0((&key, &())).unwrap();
            }

            let (reader, filters) = if reopen {
                let path = writer.path().clone();
                let (_file_handle, _key_filter, _key_bounds) =
                    writer.close(BatchMetadata::default()).unwrap();
                let (reader, membership_filter) = Reader::open_with_filter(
                    &[&factories.any_factories()],
                    test_buffer_cache,
                    &*storage_backend,
                    &path,
                )
                .unwrap();
                let key_range = reader.key_range().unwrap().map(Into::into);
                let filters = BatchFilters::from_file(key_range, membership_filter);
                (reader, filters)
            } else {
                writer.into_reader(BatchMetadata::default()).unwrap()
            };

            for key in keys {
                assert!(filters.maybe_contains_key((&key) as &DynData, None));
            }
            for key in [min - 1, min + 4, min + 9] {
                assert!(!filters.maybe_contains_key((&key) as &DynData, None));
            }
            assert_eq!(
                filter_block_magic(&reader),
                Some(ROARING_BITMAP_FILTER_BLOCK_MAGIC)
            );
        }
    });
}

#[test]
fn roaring_u64_filter_roundtrip_uses_batch_min_offset() {
    init_test_logger();

    with_roaring_enabled(|| {
        let factories = Factories::<DynData, DynData>::new::<u64, ()>();
        let tempdir = tempdir().unwrap();
        let storage_backend = <dyn StorageBackend>::new(
            &StorageConfig {
                path: tempdir.path().to_string_lossy().to_string(),
                cache: Default::default(),
            },
            &StorageOptions::default(),
        )
        .unwrap();

        let base = (u64::from(u32::MAX) << 8) + 11;
        let keys = [base, base + 2, base + 9];
        let filter_plan = filter_plan_from_keys(&keys);
        let mut writer = Writer1::new(
            &factories,
            test_buffer_cache,
            &*storage_backend,
            Parameters::default(),
            FilterPlan::decide_filter(Some(&filter_plan), keys.len()),
        )
        .unwrap();
        for key in keys {
            writer.write0((&key, &())).unwrap();
        }

        let (reader, filters) = writer.into_reader(BatchMetadata::default()).unwrap();
        for key in keys {
            assert!(filters.maybe_contains_key((&key) as &DynData, None));
        }
        for key in [base - 1, base + 3, base + 20] {
            assert!(!filters.maybe_contains_key((&key) as &DynData, None));
        }
        assert_eq!(
            filter_block_magic(&reader),
            Some(ROARING_BITMAP_FILTER_BLOCK_MAGIC)
        );
    });
}

#[test]
fn i64_keys_fallback_to_bloom_when_span_exceeds_u32() {
    init_test_logger();

    with_roaring_enabled(|| {
        let factories = Factories::<DynData, DynData>::new::<i64, ()>();
        let tempdir = tempdir().unwrap();
        let storage_backend = <dyn StorageBackend>::new(
            &StorageConfig {
                path: tempdir.path().to_string_lossy().to_string(),
                cache: Default::default(),
            },
            &StorageOptions::default(),
        )
        .unwrap();

        let max = i64::from(u32::MAX) + 1;
        let filter_plan = FilterPlan::from_bounds((&0i64) as &DynData, (&max) as &DynData);
        let mut writer = Writer1::new(
            &factories,
            test_buffer_cache,
            &*storage_backend,
            Parameters::default(),
            FilterPlan::decide_filter(Some(&filter_plan), 2),
        )
        .unwrap();
        for key in [0i64, max] {
            writer.write0((&key, &())).unwrap();
        }

        let (reader, _filters) = writer.into_reader(BatchMetadata::default()).unwrap();
        assert_eq!(
            filter_block_magic(&reader),
            Some(MODULAR_BLOOM_FILTER_BLOCK_MAGIC)
        );
    });
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
    for_each_compression_type(Parameters::default(), test_i64_helper);
}

#[test]
fn test_i64_max_branch_32() {
    for_each_compression_type(Parameters::default().with_max_branch(32), test_i64_helper);
}

#[test]
fn test_i64_max_branch_3_uncompressed() {
    test_i64_helper(
        Parameters::default()
            .with_max_branch(3)
            .with_compression(None),
    );
}

#[test]
fn test_i64_max_branch_3_snappy() {
    test_i64_helper(
        Parameters::default()
            .with_max_branch(3)
            .with_compression(Some(Compression::Snappy)),
    );
}

#[test]
fn test_i64_max_branch_2_uncompressed() {
    test_i64_helper(
        Parameters::default()
            .with_max_branch(2)
            .with_compression(None),
    );
}

#[test]
fn test_i64_max_branch_2_snappy() {
    test_i64_helper(
        Parameters::default()
            .with_max_branch(2)
            .with_compression(Some(Compression::Snappy)),
    );
}

#[test]
fn test_string() {
    fn f(x: usize) -> String {
        format!("{x:09}")
    }

    init_test_logger();
    for_each_compression_type(Parameters::default(), |parameters| {
        test_one_column(
            1000,
            |row| (f(row * 2), f(row * 2 + 1), f(row * 2 + 2), ()),
            parameters,
        )
    });
}

#[test]
fn test_tuple() {
    init_test_logger();
    for_each_compression_type(Parameters::default(), |parameters| {
        let expected = |row| {
            (
                (row as u64, 0),
                (row as u64, 1),
                (row as u64, 2),
                row as u64 + 1,
            )
        };
        test_one_column(1000, &expected, parameters.clone());
        test_one_column_zset(1000, &expected, parameters);
    });
}

#[test]
fn test_tup65_option_string() {
    init_test_logger();
    for_each_compression_type(Parameters::default(), |parameters| {
        test_one_column(
            1_000usize,
            |row| {
                let bits = row as u128 * 2 + 1;
                let before = tup65_from_bits(bits - 1);
                let key = tup65_from_bits(bits);
                let after = tup65_from_bits(bits + 1);
                (before, key, after, ())
            },
            parameters,
        );
    });
}

#[test]
fn test_big_values() {
    fn v(row: usize) -> Vec<i64> {
        (0..row as i64).collect()
    }
    init_test_logger();
    for_each_compression_type(Parameters::default(), |parameters| {
        test_one_column(
            500,
            |row| (v(row * 2), v(row * 2 + 1), v(row * 2 + 2), ()),
            parameters,
        )
    });
}

/// Builds a storage backend rooted at `dir`.
fn backend_at(dir: &str) -> Arc<dyn StorageBackend> {
    <dyn StorageBackend>::new(
        &StorageConfig {
            path: dir.to_string(),
            cache: Default::default(),
        },
        &StorageOptions::default(),
    )
    .unwrap()
}

/// Writes `keys` into a batch file under `dir`.
///
/// # Arguments
///
/// - `dir`: directory to write the batch into.
/// - `keys`: keys to write, in order.
///
/// # Returns
///
/// The path of the finished file, which the tests reopen through a fresh
/// backend.
fn write_u32_batch(dir: &str, keys: &[u32]) -> StoragePath {
    let factories = Factories::<DynData, DynData>::new::<u32, ()>();
    let storage_backend = backend_at(dir);
    let mut writer = Writer1::new(
        &factories,
        test_buffer_cache,
        &*storage_backend,
        Parameters::default(),
        FilterPlan::<DynData>::decide_filter(None, keys.len()),
    )
    .unwrap();
    for key in keys {
        writer.write0((key, &())).unwrap();
    }
    let tmp_path = writer.path().clone();
    // The handle must outlive the copy: dropping it removes the file.
    let (_file_handle, _filter, _bounds) = writer.close(BatchMetadata::default()).unwrap();

    // The writer picks its own file name, which a later backend instance has
    // no way to learn, so the batch is renamed to something the tests can ask
    // for by name.
    let stable_path = StoragePath::from("batch.feldera".to_string());
    let content = storage_backend.read(&tmp_path).unwrap();
    storage_backend
        .write(&stable_path, (*content).clone())
        .unwrap();
    storage_backend.delete(&tmp_path).unwrap();
    stable_path
}

/// Opens a batch file written by [`write_u32_batch`].
///
/// # Arguments
///
/// - `dir`: directory holding the batch.
/// - `path`: the batch's path, as returned by [`write_u32_batch`].
///
/// # Returns
///
/// - the batch's filter chain;
/// - the magic of the filter block on disk, or `None` when it has no filter;
/// - the file's incompatible feature bits.
fn open_u32_batch(dir: &str, path: &StoragePath) -> (BatchFilters<DynData>, Option<[u8; 4]>, u64) {
    let factories = Factories::<DynData, DynData>::new::<u32, ()>();
    let storage_backend = backend_at(dir);
    let (reader, membership_filter): (Reader<(&'static DynData, &'static DynData, ())>, _) =
        Reader::open_with_filter(
            &[&factories.any_factories()],
            test_buffer_cache,
            &*storage_backend,
            path,
        )
        .unwrap();
    let magic = filter_block_magic(&reader);
    let features = incompatible_features(&reader);
    let key_range = reader.key_range().unwrap().map(Into::into);
    (
        BatchFilters::from_file(key_range, membership_filter),
        magic,
        features,
    )
}

/// The configured rate selects the module count, and a one-module filter is
/// written in the original encoding so older binaries can still read the file.
#[test]
fn rate_selects_the_module_count_and_the_block_encoding() {
    init_test_logger();

    for (rate, expected_magic) in [
        (1e-4, Some(MODULAR_BLOOM_FILTER_BLOCK_MAGIC)),
        (1e-3, Some(MODULAR_BLOOM_FILTER_BLOCK_MAGIC)),
        (1e-2, Some(MODULAR_BLOOM_FILTER_BLOCK_MAGIC)),
        (1e-1, Some(BLOOM_FILTER_BLOCK_MAGIC)),
        (1.0, None),
    ] {
        let tempdir = tempdir().unwrap();
        let dir = tempdir.path().to_string_lossy().to_string();
        let keys: Vec<u32> = (0..2_000u32).map(|k| k * 2).collect();

        with_false_positive_rate(rate, move || {
            let path = write_u32_batch(&dir, &keys);
            let (filters, magic, features) = open_u32_batch(&dir, &path);
            assert_eq!(magic, expected_magic, "rate {rate:e} wrote the wrong block");

            for key in &keys {
                assert!(
                    filters.maybe_contains_key(key as &DynData, None),
                    "rate {rate:e}: filter rejected stored key {key}"
                );
            }

            // Only a multi-module file is closed to older binaries. A file with
            // one module, or none, stays readable by them.
            let expected_features = if rate < 0.05 {
                INCOMPATIBLE_FEATURE_MODULAR_FILTERS
            } else {
                0
            };
            assert_eq!(
                features, expected_features,
                "rate {rate:e} set the wrong incompatible features"
            );

            let kind = filters.membership_filter_kind();
            if expected_magic.is_none() {
                assert_eq!(
                    kind,
                    FilterKind::None,
                    "rate {rate:e} should write no filter"
                );
            } else {
                assert_eq!(kind, FilterKind::Bloom, "rate {rate:e} should write Bloom");
            }
        });
    }
}

/// Lowering the rate sheds modules when the file is loaded, without rewriting
/// it and without ever losing a key.
#[test]
fn lowering_the_rate_evicts_modules_on_load() {
    init_test_logger();

    let tempdir = tempdir().unwrap();
    let dir = tempdir.path().to_string_lossy().to_string();
    let keys: Vec<u32> = (0..4_000u32).map(|k| k * 2).collect();
    let path = Arc::new(Mutex::new(None));

    // Write at the most accurate rate, so the file holds four modules.
    {
        let (dir, keys, path) = (dir.clone(), keys.clone(), path.clone());
        with_false_positive_rate(1e-4, move || {
            *path.lock().unwrap() = Some(write_u32_batch(&dir, &keys));
        });
    }
    let path = path.lock().unwrap().clone().unwrap();

    // Reload it under progressively coarser rates. Each one keeps fewer bytes
    // resident, and none of them may lose a key.
    let mut previous_bytes = usize::MAX;
    for rate in [1e-4, 1e-3, 1e-2, 1e-1] {
        let (dir, keys, path) = (dir.clone(), keys.clone(), path.clone());
        let bytes = Arc::new(Mutex::new(0));
        {
            let bytes = bytes.clone();
            with_false_positive_rate(rate, move || {
                let (filters, magic, _features) = open_u32_batch(&dir, &path);
                assert_eq!(
                    magic,
                    Some(MODULAR_BLOOM_FILTER_BLOCK_MAGIC),
                    "the file on disk is unchanged by loading it"
                );
                assert_eq!(filters.membership_filter_kind(), FilterKind::Bloom);
                for key in &keys {
                    assert!(
                        filters.maybe_contains_key(key as &DynData, None),
                        "rate {rate:e}: eviction lost key {key}"
                    );
                }
                *bytes.lock().unwrap() = filters.stats().membership_filter.size_byte;
            });
        }
        let bytes = *bytes.lock().unwrap();
        assert!(
            bytes < previous_bytes,
            "rate {rate:e} kept {bytes} bytes resident, no less than the {previous_bytes} before it"
        );
        previous_bytes = bytes;
    }
}

/// A rate of one drops the filter entirely at load time, even for a file that
/// was written with modules.
#[test]
fn a_rate_of_one_drops_an_existing_filter_on_load() {
    init_test_logger();

    let tempdir = tempdir().unwrap();
    let dir = tempdir.path().to_string_lossy().to_string();
    let keys: Vec<u32> = (0..1_000u32).collect();
    let path = Arc::new(Mutex::new(None));

    {
        let (dir, keys, path) = (dir.clone(), keys.clone(), path.clone());
        with_false_positive_rate(1e-4, move || {
            *path.lock().unwrap() = Some(write_u32_batch(&dir, &keys));
        });
    }
    let path = path.lock().unwrap().clone().unwrap();

    with_false_positive_rate(1.0, move || {
        let (filters, _magic, _features) = open_u32_batch(&dir, &path);
        assert_eq!(filters.membership_filter_kind(), FilterKind::None);
        assert_eq!(filters.stats().membership_filter.size_byte, 0);
        // Without a membership filter every in-range key must still be accepted.
        for key in &keys {
            assert!(filters.maybe_contains_key(key as &DynData, None));
        }
    });
}

/// A [`FileReader`] that tallies the bytes the reads it forwards ask for.
#[derive(Debug)]
struct CountingFileReader {
    inner: Arc<dyn FileReader>,
    bytes: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}

impl FileRw for CountingFileReader {
    fn file_id(&self) -> FileId {
        self.inner.file_id()
    }

    fn path(&self) -> &StoragePath {
        self.inner.path()
    }
}

impl FileCommitter for CountingFileReader {
    fn commit(&self) -> Result<(), StorageError> {
        self.inner.commit()
    }
}

impl FileReader for CountingFileReader {
    fn mark_for_checkpoint(&self) {
        self.inner.mark_for_checkpoint();
    }

    fn read_block(&self, location: BlockLocation) -> Result<Arc<FBuf>, StorageError> {
        self.bytes.fetch_add(location.size, Ordering::Relaxed);
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.read_block(location)
    }

    fn get_size(&self) -> Result<u64, StorageError> {
        self.inner.get_size()
    }
}

/// Opens a batch file, counting what loading its membership filter reads.
///
/// The filter block is read on demand, so counting around that one call leaves
/// out the trailer and index reads and measures the filter alone.
///
/// # Arguments
///
/// - `dir`: directory holding the batch.
/// - `path`: the batch's path, as returned by [`write_u32_batch`].
///
/// # Returns
///
/// - the batch's filter chain;
/// - bytes the reads asked for, filter block only;
/// - number of reads issued.
fn open_u32_batch_counting(dir: &str, path: &StoragePath) -> (BatchFilters<DynData>, usize, usize) {
    let factories = Factories::<DynData, DynData>::new::<u32, ()>();
    let storage_backend = backend_at(dir);
    let bytes = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let counting = Arc::new(CountingFileReader {
        inner: storage_backend.open(path).unwrap(),
        bytes: bytes.clone(),
        calls: calls.clone(),
    });
    let reader: Reader<(&'static DynData, &'static DynData, ())> =
        Reader::new(&[&factories.any_factories()], test_buffer_cache, counting).unwrap();
    let key_range = reader.key_range().unwrap().map(Into::into);

    let before = (bytes.load(Ordering::Relaxed), calls.load(Ordering::Relaxed));
    let membership_filter = reader.membership_filter().unwrap();
    let filter_bytes = bytes.load(Ordering::Relaxed) - before.0;
    let filter_calls = calls.load(Ordering::Relaxed) - before.1;

    (
        BatchFilters::from_file(key_range, membership_filter),
        filter_bytes,
        filter_calls,
    )
}

/// A coarse rate reads only the modules it needs, not the whole filter block.
///
/// This is what the recorded per-module bit counts buy. Without them a reader
/// would have to read every module to learn what a prefix of them is worth, and
/// dropping modules would save memory but no I/O.
#[test]
fn a_coarse_rate_reads_only_the_modules_it_keeps() {
    init_test_logger();

    let tempdir = tempdir().unwrap();
    let dir = tempdir.path().to_string_lossy().to_string();
    // Enough keys that a module spans many sectors, so the saving cannot hide
    // in the rounding up to the 512-byte read granularity.
    let keys: Vec<u32> = (0..20_000u32).map(|k| k * 3).collect();
    let path = Arc::new(Mutex::new(None));

    {
        let (dir, keys, path) = (dir.clone(), keys.clone(), path.clone());
        with_false_positive_rate(1e-4, move || {
            *path.lock().unwrap() = Some(write_u32_batch(&dir, &keys));
        });
    }
    let path = path.lock().unwrap().clone().unwrap();

    // Coarse first: a shared cache could only make the later, finer open
    // cheaper, so it cannot manufacture the difference asserted below.
    let measure = |rate: f64| {
        let (dir, keys, path) = (dir.clone(), keys.clone(), path.clone());
        let result = Arc::new(Mutex::new((0usize, 0usize, 0usize)));
        {
            let result = result.clone();
            with_false_positive_rate(rate, move || {
                let (filters, bytes, calls) = open_u32_batch_counting(&dir, &path);
                for key in &keys {
                    assert!(
                        filters.maybe_contains_key(key as &DynData, None),
                        "rate {rate:e}: a prefix read lost key {key}"
                    );
                }
                *result.lock().unwrap() =
                    (bytes, filters.stats().membership_filter.size_byte, calls);
            });
        }
        *result.lock().unwrap()
    };

    let (coarse_read, coarse_resident, _coarse_calls) = measure(1e-1);
    let (fine_read, fine_resident, fine_calls) = measure(1e-4);

    // The finest rate cannot shed a module, so it must not pay to find that
    // out: one read of the block, as before any of this existed.
    assert_eq!(
        fine_calls, 1,
        "1e-4 read the filter block in {fine_calls} calls, not the one it needs"
    );

    assert!(
        coarse_resident < fine_resident,
        "1e-1 kept {coarse_resident} bytes resident, 1e-4 kept {fine_resident}"
    );
    assert!(
        coarse_read < fine_read,
        "1e-1 read {coarse_read} bytes of filter, no fewer than the {fine_read} that 1e-4 read"
    );

    // Reads are issued in 512-byte sectors, so a module dropped is a module not
    // read, give or take the sector the header shares with the first of them.
    let unread = fine_read - coarse_read;
    let evicted = fine_resident - coarse_resident;
    assert!(
        unread + 512 >= evicted,
        "evicting {evicted} bytes of modules only avoided reading {unread}"
    );
}

/// A filter written in the original encoding loads whole whatever the rate.
///
/// One module is written as `LFFB`, the encoding that predates modular filters,
/// and such a filter has no ladder to descend: the rate can drop it entirely or
/// leave it alone, and nothing in between. The modular path has its own tests
/// for shedding; this pins that the legacy path does not try to.
#[test]
fn a_legacy_filter_has_no_ladder_to_descend() {
    init_test_logger();

    let tempdir = tempdir().unwrap();
    let dir = tempdir.path().to_string_lossy().to_string();
    let keys: Vec<u32> = (0..4_000u32).map(|k| k * 2).collect();
    let path = Arc::new(Mutex::new(None));

    // 1e-1 is one module, so the writer uses the original encoding.
    {
        let (dir, keys, path) = (dir.clone(), keys.clone(), path.clone());
        with_false_positive_rate(1e-1, move || {
            *path.lock().unwrap() = Some(write_u32_batch(&dir, &keys));
        });
    }
    let path = path.lock().unwrap().clone().unwrap();

    // Loading it at any rate that wants a filter gives back all of it, whether
    // the rate is coarser or finer than the one it was written at.
    let mut sizes = Vec::new();
    for rate in [1e-1, 1e-2, 1e-4] {
        let (dir, keys, path) = (dir.clone(), keys.clone(), path.clone());
        let size = Arc::new(Mutex::new(0));
        {
            let size = size.clone();
            with_false_positive_rate(rate, move || {
                let (filters, magic, _features) = open_u32_batch(&dir, &path);
                assert_eq!(
                    magic,
                    Some(BLOOM_FILTER_BLOCK_MAGIC),
                    "rate {rate:e}: the file on disk is unchanged by loading it"
                );
                assert_eq!(filters.membership_filter_kind(), FilterKind::Bloom);
                for key in &keys {
                    assert!(
                        filters.maybe_contains_key(key as &DynData, None),
                        "rate {rate:e}: a legacy filter lost key {key}"
                    );
                }
                *size.lock().unwrap() = filters.stats().membership_filter.size_byte;
            });
        }
        sizes.push(*size.lock().unwrap());
    }
    assert!(sizes[0] > 0, "the legacy filter measured zero bytes");
    assert!(
        sizes.iter().all(|size| *size == sizes[0]),
        "a legacy filter changed size with the rate: {sizes:?}"
    );

    // A rate of one drops it outright, the one thing the rate can do to it.
    with_false_positive_rate(1.0, move || {
        let (filters, _magic, _features) = open_u32_batch(&dir, &path);
        assert_eq!(filters.membership_filter_kind(), FilterKind::None);
        assert_eq!(filters.stats().membership_filter.size_byte, 0);
        for key in &keys {
            assert!(filters.maybe_contains_key(key as &DynData, None));
        }
    });
}

/// Copying a layer file by splicing its values, rather than rewriting them.
///
/// This is the whole point of [`Cursor::raw_run`] and [`Writer2::write1_raw`]:
/// a merge that finds one input holding a stretch of the output to itself can
/// move those bytes instead of decoding and re-encoding them.  The copy here
/// is the simplest case of that, one input and no merging at all, so anything
/// that comes back different is the splice's fault and nothing else's.
///
/// Such a merge reads its inputs without decoding them, too: it compares keys
/// as they are stored, with [`Cursor::archived_key`], and counts the negative
/// weights of the values it copies where they lie, with [`Cursor::aux_at`].
/// The tests of those two check that they read what decoding would.
mod splice_layer_file {
    use std::{
        cmp::Ordering,
        collections::{BTreeMap, HashSet},
        ops::Range,
    };

    use feldera_types::config::{StorageConfig, StorageOptions};
    use rand::{Rng, SeedableRng, seq::SliceRandom};
    use rand_chacha::ChaCha8Rng;

    use crate::{
        DBData,
        algebra::{F32, F64},
        dynamic::{DataTrait, DynData, DynWeight, Erase},
        hash::default_hash,
        storage::{
            backend::StorageBackend,
            file::{
                BatchKeyFilter, Factories, FilterPlan,
                filter::FilterKind,
                format::{BatchMetadata, Compression},
                reader::{ColumnSpec, Cursor, Reader, RowGroup},
                writer::{Parameters, Writer2},
            },
        },
        time::Product,
        trace::filter::BatchFilters,
        utils::{Tup1, Tup2, Tup3, Tup4, Tup10},
    };

    use super::{backend_at, test_buffer_cache};
    use tempfile::tempdir;

    type K0 = String;
    type A0 = ();
    type K1 = u64;
    type A1 = i64;

    /// Keys of uneven length, so items do not all encode to the same size and
    /// the runs being copied are not a uniform stride.
    fn key0(row: usize) -> K0 {
        format!("key-{row:05}-{}", "y".repeat(row % 23))
    }

    fn values(row: usize) -> Vec<(K1, A1)> {
        (0..1 + row % 5)
            .map(|i| ((row * 10 + i) as u64, (row as i64) - (i as i64) * 3))
            .collect()
    }

    /// Every way a block can be stored: as is, and with each codec.  A copy
    /// takes items from a block once it is decompressed and puts them in a
    /// block that is compressed afresh, so it has to come out the same under
    /// each.
    const COMPRESSIONS: [Option<Compression>; 4] = [
        None,
        Some(Compression::Snappy),
        Some(Compression::Lz4),
        Some(Compression::Zstd),
    ];

    fn parameters(compression: Option<Compression>) -> Parameters {
        Parameters {
            // Small blocks so a run runs off the end of one and the splice has
            // to carry on into the next, which is the case that gets the row
            // numbering wrong if anything does.
            min_data_block: 4096,
            min_index_block: 4096,
            compression,
            ..Parameters::default()
        }
    }

    /// A reader for the files these tests write, which hold keys in their
    /// first column and values with their weights in the second.
    type TwoColumnReader = Reader<(
        &'static DynData,
        &'static DynData,
        (&'static DynData, &'static DynWeight, ()),
    )>;

    /// A writer for those files.
    type TwoColumnWriter = Writer2<DynData, DynData, DynData, DynWeight>;

    /// Creates a writer for a file whose keys are of type `K`.
    ///
    /// # Arguments
    ///
    /// * `backend` - where to write the file.
    /// * `parameters` - how to write it.
    /// * `filter` - the membership filter to build for its keys, if any.
    ///
    /// # Returns
    ///
    /// The writer.
    fn new_writer<K: DBData>(
        backend: &dyn StorageBackend,
        parameters: Parameters,
        filter: Option<BatchKeyFilter>,
    ) -> TwoColumnWriter {
        Writer2::new(
            &Factories::<DynData, DynData>::new::<K, A0>(),
            &Factories::<DynData, DynWeight>::new::<K1, A1>(),
            test_buffer_cache,
            backend,
            parameters,
            filter,
        )
        .unwrap()
    }

    /// Writes a file whose first column holds `keys` and whose second holds,
    /// under the key at index `row`, the [`values`] of `row`.
    ///
    /// # Arguments
    ///
    /// * `keys` - the keys, in ascending order and without repeats.
    /// * `backend` - where to write the file.
    /// * `parameters` - how to write it.
    /// * `filter` - the membership filter to build for the keys, if any.
    ///
    /// # Returns
    ///
    /// A reader for the file, and the filters that go with it.
    fn write_keys<K: DBData>(
        keys: &[K],
        backend: &dyn StorageBackend,
        parameters: Parameters,
        filter: Option<BatchKeyFilter>,
    ) -> (TwoColumnReader, BatchFilters<DynData>) {
        let mut writer = new_writer::<K>(backend, parameters, filter);
        for (row, key) in keys.iter().enumerate() {
            for (mut k, mut a) in values(row) {
                writer.write1((k.erase_mut(), a.erase_mut())).unwrap();
            }
            writer.write0((key.erase(), ().erase())).unwrap();
        }
        writer.into_reader(BatchMetadata::default()).unwrap()
    }

    fn build(
        n: usize,
        backend: &dyn StorageBackend,
        compression: Option<Compression>,
    ) -> TwoColumnReader {
        let keys: Vec<K0> = (0..n).map(key0).collect();
        let filter = crate::storage::file::filter::FilterPlan::<DynData>::decide_filter(None, n);
        write_keys(&keys, backend, parameters(compression), filter).0
    }

    /// Asserts that `cursor` is on a row whose key is `expected`, both decoded
    /// and as stored, or on no row when `expected` is `None`.
    ///
    /// The key as stored is asked for twice: the first ask looks it up and
    /// keeps it, and the second answers from what the first kept.
    ///
    /// # Arguments
    ///
    /// * `cursor` - the cursor to check.
    /// * `expected` - the key the cursor should be on, if any.
    /// * `after` - what moved the cursor there, for the failure message.
    ///
    /// # Panics
    ///
    /// If either form of the key is not `expected`.
    fn assert_on_key<A, N, T>(
        cursor: &Cursor<'_, DynData, A, N, T>,
        expected: Option<&DynData>,
        after: &str,
    ) where
        A: DataTrait + ?Sized,
        T: ColumnSpec,
    {
        assert_eq!(cursor.key(), expected, "after {after}, the decoded key");
        for ask in ["first", "second"] {
            let archived = cursor.archived_key();
            match expected {
                Some(expected) => assert_eq!(
                    archived.map(|archived| archived.cmp_target(expected)),
                    Some(Ordering::Equal),
                    "after {after}, the key as stored, asked for the {ask} time",
                ),
                None => assert!(
                    archived.is_none(),
                    "after {after}, a key as stored off the rows, asked for the {ask} time",
                ),
            }
        }
    }

    /// Copies one key's values from row `from` on, a run at a time, until the
    /// copy has reached row `until`.
    ///
    /// A run ends where its data block or the key's values end, so the copy
    /// stops at the first such end at or past `until`.
    ///
    /// # Arguments
    ///
    /// * `writer` - the writer to copy into.
    /// * `values` - the key's values in the source.
    /// * `from` - the first row to copy, counted from the key's first value.
    /// * `until` - the row the copy has to reach.
    ///
    /// # Returns
    ///
    /// The row after the last one copied, which is `from` when `until` is not
    /// past it.
    fn copy_runs<T: ColumnSpec>(
        writer: &mut TwoColumnWriter,
        values: &RowGroup<'_, DynData, DynWeight, (), T>,
        from: u64,
        until: u64,
    ) -> u64 {
        let mut at = from;
        let mut cursor = values.nth(at).unwrap();
        while at < until {
            at += {
                let run = cursor
                    .raw_run()
                    .expect("a block this build wrote can be copied");
                writer.write1_raw(&run).unwrap();
                run.len() as u64
            };
            // SAFETY: the cursor reads a file these tests wrote, through the
            // factories they wrote it with.
            unsafe { cursor.move_to_row(at) }.unwrap();
        }
        at
    }

    /// Encodes some of the values of the key at index `row`, as [`values`]
    /// gives them.
    ///
    /// # Arguments
    ///
    /// * `writer` - the writer to encode into.
    /// * `row` - the key's index.
    /// * `rows` - which of its values to encode, counted from its first.
    fn encode_values(writer: &mut TwoColumnWriter, row: usize, rows: Range<u64>) {
        for (value, weight) in &values(row)[rows.start as usize..rows.end as usize] {
            writer.write1((value.erase(), weight.erase())).unwrap();
        }
    }

    /// Copies `source` into `writer` a key at a time, as a merge copies a key
    /// that no other input holds: its values as bytes, then the key as bytes,
    /// or decoded where the writer refuses the bytes.
    ///
    /// # Arguments
    ///
    /// * `source` - the file to copy.
    /// * `writer` - the writer to copy into.
    ///
    /// # Returns
    ///
    /// How many keys the writer refused as bytes.
    fn copy_key_by_key(source: &TwoColumnReader, writer: &mut TwoColumnWriter) -> usize {
        let rows0 = source.rows();
        let mut keys = rows0.nth(0).unwrap();
        let mut refused = 0;
        while keys.has_value() {
            let values = keys.next_column().unwrap();
            copy_runs(writer, &values, 0, values.len());
            let taken = {
                let item = keys.raw_item().unwrap();
                writer.write0_raw(&item).unwrap()
            };
            if !taken {
                refused += 1;
                writer.write0((keys.key().unwrap(), ().erase())).unwrap();
            }
            // SAFETY: as in `copy_runs`.
            unsafe { keys.move_next() }.unwrap();
        }
        refused
    }

    /// Writes a file of `keys` and copies it key by key into one that builds
    /// `filter`.
    ///
    /// # Arguments
    ///
    /// * `keys` - the keys, in ascending order and without repeats.
    /// * `backend` - where to write both files.
    /// * `filter` - the membership filter the copy builds, if any.
    ///
    /// # Returns
    ///
    /// A reader for the copy, its filters, and how many keys it refused as
    /// bytes.
    fn copy_keys<K: DBData>(
        keys: &[K],
        backend: &dyn StorageBackend,
        filter: Option<BatchKeyFilter>,
    ) -> (TwoColumnReader, BatchFilters<DynData>, usize) {
        let (source, _) = write_keys(keys, backend, parameters(None), None);
        source.evict();
        let mut writer = new_writer::<K>(backend, parameters(None), filter);
        let refused = copy_key_by_key(&source, &mut writer);
        let (copy, filters) = writer.into_reader(BatchMetadata::default()).unwrap();
        copy.evict();
        (copy, filters, refused)
    }

    /// Asserts that `file` holds exactly the keys of `expected`, in order,
    /// each owning the [`values`] of the row it is paired with.
    ///
    /// Every key and value is read decoded and as stored, every value also at
    /// its position under its key, and from every value each weight that
    /// `aux_at` can reach in the same run.
    ///
    /// # Arguments
    ///
    /// * `file` - the file to read.
    /// * `expected` - each key the file should hold, after the row whose
    ///   values it should own.
    ///
    /// # Panics
    ///
    /// If the file holds anything else.
    fn assert_holds<K: DBData>(
        file: &TwoColumnReader,
        expected: impl IntoIterator<Item = (usize, K)>,
    ) {
        let expected: Vec<(usize, K)> = expected.into_iter().collect();
        let rows0 = file.rows();
        assert_eq!(
            rows0.len(),
            expected.len() as u64,
            "the file holds the wrong number of keys",
        );
        for (at, (row, key)) in expected.iter().enumerate() {
            let keys = rows0.nth(at as u64).unwrap();
            assert_on_key(&keys, Some(key.erase()), &format!("nth({at})"));
            let rows1 = keys.next_column().unwrap();
            let owned = values(*row);
            assert_eq!(
                rows1.len(),
                owned.len() as u64,
                "key {at} owns the wrong number of values",
            );
            let mut cursor = rows1.nth(0).unwrap();
            for (index, &(value, weight)) in owned.iter().enumerate() {
                let place = format!("key {at}, value {index}");
                assert_eq!(cursor.relative_position(), index as u64, "{place}");
                assert_on_key(&cursor, Some(value.erase()), &place);
                let reach = cursor
                    .raw_run()
                    .expect("a block this build wrote can be copied")
                    .len() as u64;
                let (mut got_value, mut got_weight) = (K1::default(), A1::default());
                // SAFETY: the cursor reads a file these tests wrote, through
                // the factories they wrote it with.
                unsafe {
                    assert!(
                        cursor
                            .item((got_value.erase_mut(), got_weight.erase_mut()))
                            .is_some(),
                        "{place}: no item",
                    );
                    assert_eq!((got_value, got_weight), (value, weight), "{place}");
                    for offset in 0..reach {
                        assert!(
                            cursor.aux_at(offset, got_weight.erase_mut()),
                            "{place}: aux_at({offset}) read nothing",
                        );
                        assert_eq!(
                            got_weight,
                            owned[index + offset as usize].1,
                            "{place}: aux_at({offset}) read the wrong weight",
                        );
                    }
                    assert!(
                        !cursor.aux_at(reach, got_weight.erase_mut()),
                        "{place}: aux_at({reach}) read past the run",
                    );
                    cursor.move_next().unwrap();
                }
            }
        }
    }

    /// Asserts that seeking `file` for the key of any row of its source, kept
    /// or not, lands on the first key at or past it when seeking forward, and
    /// on the last key at or before it when seeking backward.
    ///
    /// # Arguments
    ///
    /// * `file` - a copy of a file that held the keys of rows `0..n`.
    /// * `kept` - the rows whose keys the copy holds, in order.
    /// * `n` - how many keys the source held.
    ///
    /// # Panics
    ///
    /// If a seek lands anywhere else.
    fn assert_seeks_land(file: &TwoColumnReader, kept: &[usize], n: usize) {
        let rows0 = file.rows();
        let last = rows0.len() - 1;
        for row in 0..n {
            let target = key0(row);
            let ahead = kept
                .get(kept.partition_point(|&kept| kept < row))
                .map(|&kept| key0(kept));
            let behind = kept
                .partition_point(|&kept| kept <= row)
                .checked_sub(1)
                .map(|previous| key0(kept[previous]));
            let mut forward = rows0.nth(0).unwrap();
            let mut forward_until = rows0.nth(0).unwrap();
            let mut backward = rows0.nth(last).unwrap();
            let mut backward_until = rows0.nth(last).unwrap();
            // SAFETY: the cursors read a file these tests wrote, through the
            // factories they wrote it with.
            unsafe {
                forward.advance_to_value_or_larger(target.erase()).unwrap();
                forward_until
                    .seek_forward_until(|key| key >= target.erase())
                    .unwrap();
                backward.rewind_to_value_or_smaller(target.erase()).unwrap();
                backward_until
                    .seek_backward_until(|key| key <= target.erase())
                    .unwrap();
            }
            let ahead = ahead.as_ref().map(|key| key.erase());
            let behind = behind.as_ref().map(|key| key.erase());
            let to = format!("to key {row}");
            assert_on_key(&forward, ahead, &format!("advance_to_value_or_larger {to}"));
            assert_on_key(&forward_until, ahead, &format!("seek_forward_until {to}"));
            assert_on_key(
                &backward,
                behind,
                &format!("rewind_to_value_or_smaller {to}"),
            );
            assert_on_key(
                &backward_until,
                behind,
                &format!("seek_backward_until {to}"),
            );
        }
    }

    /// Copies a file of `keys` key by key into one with a Bloom filter, and
    /// checks that each key went into the filter by the hash its decoded form
    /// has.
    ///
    /// # Arguments
    ///
    /// * `keys` - the keys, in any order and possibly repeated.
    /// * `hashable` - whether the archived form of `K` can be hashed.  If it
    ///   can, every key has to go in as bytes and hash as stored the way it
    ///   hashes decoded; if not, every key has to be refused as bytes, offer no
    ///   hash as stored, and go in decoded.
    ///
    /// # Panics
    ///
    /// If a key goes in some other way, reads back different, or is missing
    /// from the filter.
    fn assert_copied_keys_hash_like_decoded<K: DBData>(mut keys: Vec<K>, hashable: bool) {
        keys.sort();
        keys.dedup();
        let what = std::any::type_name::<K>();
        let tempdir = tempdir().unwrap();
        let backend = backend_at(&tempdir.path().to_string_lossy());
        let filter = FilterPlan::<DynData>::decide_filter(None, keys.len());
        let (copy, filters, refused) = copy_keys(&keys, &*backend, filter);
        assert_eq!(
            filters.membership_filter_kind(),
            FilterKind::Bloom,
            "{what}"
        );
        assert_eq!(
            refused,
            if hashable { 0 } else { keys.len() },
            "{what}: the keys refused as bytes",
        );
        assert_holds(&copy, keys.iter().cloned().enumerate());

        let rows0 = copy.rows();
        let mut cursor = rows0.nth(0).unwrap();
        for key in &keys {
            assert_eq!(
                cursor.archived_key().unwrap().archived_hash(),
                hashable.then(|| default_hash(key)),
                "{what}: {key:?} hashes differently as stored",
            );
            assert!(
                filters.maybe_contains_key(key.erase(), None),
                "{what}: {key:?} is missing from the filter",
            );
            // SAFETY: the cursor reads the file this function wrote, through
            // the factories it wrote it with.
            unsafe { cursor.move_next() }.unwrap();
        }
    }

    /// A splice onto a file that already holds rows has to move every row
    /// group by the distance between where the run sat and where it now sits.
    ///
    /// A straight copy cannot show this: there the two coincide and a shift of
    /// zero is correct.  Here the destination is given a few keys of its own
    /// first, so the distance is not zero and getting it wrong is visible.
    #[test]
    fn a_splice_onto_a_non_empty_file_shifts_row_groups() {
        let n = 200;
        let head = 3; // keys written the ordinary way before any splicing
        let tempdir = tempdir().unwrap();
        let backend = <dyn StorageBackend>::new(
            &StorageConfig {
                path: tempdir.path().to_string_lossy().to_string(),
                cache: Default::default(),
            },
            &StorageOptions::default(),
        )
        .unwrap();
        let source = build(n, &*backend, None);
        source.evict();

        let factories0 = Factories::<DynData, DynData>::new::<K0, A0>();
        let factories1 = Factories::<DynData, DynWeight>::new::<K1, A1>();
        let mut writer = Writer2::new(
            &factories0,
            &factories1,
            test_buffer_cache,
            &*backend,
            parameters(None),
            None,
        )
        .unwrap();

        // Keys of the destination's own, sorting before every key of the
        // source and carrying a different number of values each, so that the
        // source's rows land somewhere they have never been.
        let mut shift = 0u64;
        for i in 0..head {
            for j in 0..(2 + i) {
                let (mut k, mut a) = ((900 + 10 * i + j) as K1, -(j as A1) - 1);
                writer.write1((k.erase_mut(), a.erase_mut())).unwrap();
                shift += 1;
            }
            let (mut k, mut a) = (format!("aaa-{i}"), ());
            writer.write0((k.erase_mut(), a.erase_mut())).unwrap();
        }
        assert!(shift > 0, "the destination has to start somewhere else");

        // Now the whole source, spliced, landing `shift` rows further along.
        let rows0 = source.rows();
        let mut keys = unsafe { rows0.first() }.unwrap();
        let mut at = 0u64;
        while keys.has_value() {
            // One key at a time, which is the granularity a merge splices at: it
            // owns the output only as far as the next key of another input.  A
            // key's record carries the range of value rows it owns, so those
            // values have to be written before it is.
            let value_rows = keys.next_column().unwrap();
            let mut value_cursor = unsafe { value_rows.first() }.unwrap();
            let mut values_at = 0u64;
            while values_at < value_rows.len() {
                let run = value_cursor.raw_run().unwrap();
                writer.write1_raw(&run).unwrap();
                values_at += run.len() as u64;
                unsafe { value_cursor.move_to_row(values_at) }.unwrap();
            }
            let took = {
                let item = keys.raw_item().unwrap();
                writer.write0_raw(&item).unwrap()
            };
            assert!(took, "the key splice stalled at {at}");
            at += 1;
            unsafe { keys.move_next() }.unwrap();
        }
        assert_eq!(at, n as u64);

        let copy = writer.into_reader(BatchMetadata::default()).unwrap().0;
        copy.evict();
        let copy0 = copy.rows();
        assert_eq!(copy0.len(), (n + head) as u64);
        for row in 0..n {
            let cursor = copy0.nth((row + head) as u64).unwrap();
            let rows1 = cursor.next_column().unwrap();
            let expected = values(row);
            assert_eq!(rows1.len(), expected.len() as u64, "row {row} value count");
            let mut c1 = unsafe { rows1.first() }.unwrap();
            let (mut got_k, mut got_a) = (K1::default(), A1::default());
            for (i, (k, a)) in expected.iter().enumerate() {
                let (mut want_k, mut want_a) = (*k, *a);
                assert_eq!(
                    unsafe { c1.item((got_k.erase_mut(), got_a.erase_mut())) },
                    Some((want_k.erase_mut() as &mut _, want_a.erase_mut() as &mut _)),
                    "row {row} value {i} came back changed"
                );
                unsafe { c1.move_next() }.unwrap();
            }
        }
    }

    /// A copied key has to own at least one row of the next column, as an
    /// encoded one does.
    ///
    /// A key written with none would leave the file's columns disagreeing on
    /// how many rows there are, which only shows when the file is read back;
    /// the writer refuses it on the spot instead, in every build.
    #[test]
    #[should_panic(expected = "a key needs at least one row of the next column")]
    fn a_copied_key_that_owns_no_rows_is_refused() {
        let tempdir = tempdir().unwrap();
        let backend = <dyn StorageBackend>::new(
            &StorageConfig {
                path: tempdir.path().to_string_lossy().to_string(),
                cache: Default::default(),
            },
            &StorageOptions::default(),
        )
        .unwrap();
        let source = build(10, &*backend, None);
        let factories0 = Factories::<DynData, DynData>::new::<K0, A0>();
        let factories1 = Factories::<DynData, DynWeight>::new::<K1, A1>();
        let mut writer = Writer2::new(
            &factories0,
            &factories1,
            test_buffer_cache,
            &*backend,
            parameters(None),
            crate::storage::file::filter::FilterPlan::<DynData>::decide_filter(None, 10),
        )
        .unwrap();
        let rows0 = source.rows();
        let keys = unsafe { rows0.first() }.unwrap();
        let item = keys.raw_item().unwrap();
        // No values written first.
        let _ = writer.write0_raw(&item);
    }

    /// A copied key goes into the copy's membership filter by its own hash,
    /// wherever it sits in the archived item it was copied as.
    ///
    /// An archived item is the key together with its auxiliary data, laid out
    /// the way Rust chooses, so the key starts at the item's root only when the
    /// auxiliary data takes no space.  Beside a `u64`, a `u8` key sits eight
    /// bytes in: hashing from the item's root would record part of the
    /// auxiliary data instead, and those keys would be missing from the filter.
    #[test]
    fn a_copied_key_beside_auxiliary_data_is_found_in_the_filter() {
        type Key = u8;
        type Aux = u64;
        let n = 200;
        let key = |row: usize| row as Key;
        let aux = |row: usize| 1_000_000 + row as Aux;

        let tempdir = tempdir().unwrap();
        let backend = <dyn StorageBackend>::new(
            &StorageConfig {
                path: tempdir.path().to_string_lossy().to_string(),
                cache: Default::default(),
            },
            &StorageOptions::default(),
        )
        .unwrap();
        let factories0 = Factories::<DynData, DynData>::new::<Key, Aux>();
        let factories1 = Factories::<DynData, DynWeight>::new::<K1, A1>();
        let new_writer = || {
            Writer2::new(
                &factories0,
                &factories1,
                test_buffer_cache,
                &*backend,
                parameters(None),
                crate::storage::file::filter::FilterPlan::<DynData>::decide_filter(None, n),
            )
            .unwrap()
        };

        let mut writer = new_writer();
        for row in 0..n {
            for (mut k, mut a) in values(row) {
                writer.write1((k.erase_mut(), a.erase_mut())).unwrap();
            }
            let (mut k, mut a) = (key(row), aux(row));
            writer.write0((k.erase_mut(), a.erase_mut())).unwrap();
        }
        let source: Reader<(
            &'static DynData,
            &'static DynData,
            (&'static DynData, &'static DynWeight, ()),
        )> = writer.into_reader(BatchMetadata::default()).unwrap().0;
        source.evict();

        let mut writer = new_writer();
        let rows0 = source.rows();
        let mut keys = unsafe { rows0.first() }.unwrap();
        while keys.has_value() {
            let value_rows = keys.next_column().unwrap();
            let mut value_cursor = unsafe { value_rows.first() }.unwrap();
            let mut values_at = 0u64;
            while values_at < value_rows.len() {
                let run = value_cursor.raw_run().unwrap();
                writer.write1_raw(&run).unwrap();
                values_at += run.len() as u64;
                unsafe { value_cursor.move_to_row(values_at) }.unwrap();
            }
            let took = {
                let item = keys.raw_item().unwrap();
                writer.write0_raw(&item).unwrap()
            };
            assert!(took, "the copy refused a key");
            unsafe { keys.move_next() }.unwrap();
        }
        let (copy, filters) = writer.into_reader(BatchMetadata::default()).unwrap();
        copy.evict();

        assert_eq!(filters.membership_filter_kind(), FilterKind::Bloom);
        let missing: Vec<usize> = (0..n)
            .filter(|&row| {
                let mut k = key(row);
                !filters.maybe_contains_key(k.erase_mut(), None)
            })
            .collect();
        assert!(
            missing.is_empty(),
            "copied keys are missing from the filter at rows {missing:?}"
        );
    }

    /// Copies the file by splicing both columns, which is what a merge that
    /// found a run of keys to itself would do.
    ///
    /// The copy carries a membership filter, which is the ordinary case: a
    /// merge builds one for whatever it writes.  The filter is fed from the
    /// archived keys as they go by, since a splice never decodes one, and the
    /// check at the end is the one that matters -- a key that went in has to
    /// be found again, or a query that looks for it is silently wrong.
    #[test]
    fn a_two_column_spliced_copy_matches_the_original() {
        for compression in COMPRESSIONS {
            let n = 400;
            let tempdir = tempdir().unwrap();
            let backend = <dyn StorageBackend>::new(
                &StorageConfig {
                    path: tempdir.path().to_string_lossy().to_string(),
                    cache: Default::default(),
                },
                &StorageOptions::default(),
            )
            .unwrap();
            let source = build(n, &*backend, compression);
            source.evict();

            let factories0 = Factories::<DynData, DynData>::new::<K0, A0>();
            let factories1 = Factories::<DynData, DynWeight>::new::<K1, A1>();
            let mut writer = Writer2::new(
                &factories0,
                &factories1,
                test_buffer_cache,
                &*backend,
                parameters(compression),
                crate::storage::file::filter::FilterPlan::<DynData>::decide_filter(None, n),
            )
            .unwrap();

            let rows0 = source.rows();
            let mut keys = unsafe { rows0.first() }.unwrap();
            let mut at = 0u64;
            while keys.has_value() {
                // One key at a time, which is the granularity a merge splices at: it
                // owns the output only as far as the next key of another input.  A
                // key's record carries the range of value rows it owns, so those
                // values have to be written before it is.
                let value_rows = keys.next_column().unwrap();
                let mut value_cursor = unsafe { value_rows.first() }.unwrap();
                let mut values_at = 0u64;
                while values_at < value_rows.len() {
                    let run = value_cursor.raw_run().unwrap();
                    writer.write1_raw(&run).unwrap();
                    values_at += run.len() as u64;
                    unsafe { value_cursor.move_to_row(values_at) }.unwrap();
                }
                let took = {
                    let item = keys.raw_item().unwrap();
                    writer.write0_raw(&item).unwrap()
                };
                assert!(took, "the key splice stalled at {at}");
                at += 1;
                unsafe { keys.move_next() }.unwrap();
            }
            assert_eq!(at, n as u64);

            let (copy, filters) = writer.into_reader(BatchMetadata::default()).unwrap();
            copy.evict();
            assert_eq!(copy.rows().len(), n as u64);

            // Every key the splice copied has to be in the filter.  A filter
            // fed from decoded keys would pass this too, so the assertion
            // above it is what says the filter came from the spliced bytes:
            // the splice could not have run at all had it refused, and the
            // rows would not be here.
            assert_ne!(filters.membership_filter_kind(), FilterKind::None);
            for row in 0..n {
                let mut key = key0(row);
                assert!(
                    filters.maybe_contains_key(key.erase_mut(), None),
                    "row {row} is missing from the membership filter"
                );
            }

            let copy0 = copy.rows();
            for row in 0..n {
                let cursor = copy0.nth(row as u64).unwrap();
                let mut want0 = key0(row);
                assert_eq!(cursor.key(), Some(want0.erase_mut() as &_), "row {row} key");
                let rows1 = cursor.next_column().unwrap();
                let expected = values(row);
                assert_eq!(rows1.len(), expected.len() as u64, "row {row} value count");
                let mut c1 = unsafe { rows1.first() }.unwrap();
                let (mut got_k, mut got_a) = (K1::default(), A1::default());
                for (i, (k, a)) in expected.iter().enumerate() {
                    let (mut want_k, mut want_a) = (*k, *a);
                    assert_eq!(
                        unsafe { c1.item((got_k.erase_mut(), got_a.erase_mut())) },
                        Some((want_k.erase_mut() as &mut _, want_a.erase_mut() as &mut _)),
                        "row {row} value {i} came back changed"
                    );
                    unsafe { c1.move_next() }.unwrap();
                }
            }
        }
    }

    #[test]
    fn a_spliced_copy_matches_the_original() {
        for compression in COMPRESSIONS {
            let n = 400;
            let tempdir = tempdir().unwrap();
            let backend = <dyn StorageBackend>::new(
                &StorageConfig {
                    path: tempdir.path().to_string_lossy().to_string(),
                    cache: Default::default(),
                },
                &StorageOptions::default(),
            )
            .unwrap();
            let source = build(n, &*backend, compression);
            source.evict();

            let factories0 = Factories::<DynData, DynData>::new::<K0, A0>();
            let factories1 = Factories::<DynData, DynWeight>::new::<K1, A1>();
            let mut writer = Writer2::new(
                &factories0,
                &factories1,
                test_buffer_cache,
                &*backend,
                parameters(compression),
                crate::storage::file::filter::FilterPlan::<DynData>::decide_filter(None, n),
            )
            .unwrap();

            let rows0 = source.rows();
            let mut spliced = 0usize;
            for row in 0..n {
                let keys = rows0.nth(row as u64).unwrap();
                let rows1 = keys.next_column().unwrap();
                let mut cursor = unsafe { rows1.first() }.unwrap();
                let mut taken = 0u64;
                while cursor.has_value() {
                    let run = cursor
                        .raw_run()
                        .expect("a block this writer just wrote can be spliced");
                    writer.write1_raw(&run).unwrap();
                    spliced += run.len();
                    taken += run.len() as u64;
                    unsafe { cursor.move_to_row(taken) }.unwrap();
                }
                let mut k = key0(row);
                let mut a = ();
                writer.write0((k.erase_mut(), a.erase_mut())).unwrap();
            }
            let total: usize = (0..n).map(|row| values(row).len()).sum();
            assert_eq!(spliced, total, "not every value was spliced");

            let copy = writer.into_reader(BatchMetadata::default()).unwrap().0;
            copy.evict();
            assert_eq!(copy.rows().len(), n as u64);
            let copy0 = copy.rows();
            for row in 0..n {
                let cursor = copy0.nth(row as u64).unwrap();
                let mut want0 = key0(row);
                assert_eq!(cursor.key(), Some(want0.erase_mut() as &_), "row {row} key");
                let rows1 = cursor.next_column().unwrap();
                let expected = values(row);
                assert_eq!(rows1.len(), expected.len() as u64, "row {row} value count");
                let mut c1 = unsafe { rows1.first() }.unwrap();
                let (mut got_k, mut got_a) = (K1::default(), A1::default());
                for (i, (k, a)) in expected.iter().enumerate() {
                    let (mut want_k, mut want_a) = (*k, *a);
                    assert_eq!(
                        unsafe { c1.item((got_k.erase_mut(), got_a.erase_mut())) },
                        Some((want_k.erase_mut() as &mut _, want_a.erase_mut() as &mut _)),
                        "row {row} value {i} came back changed"
                    );
                    unsafe { c1.move_next() }.unwrap();
                }
            }
        }
    }

    /// Copies the rows of `source` at `rows`, in that order, keys and values
    /// alike, into a file that has no filter, so no key is refused.
    ///
    /// # Arguments
    ///
    /// * `source` - the file to copy from, written by [`build`].
    /// * `rows` - the rows to copy.
    #[cfg(debug_assertions)]
    fn copy_rows(source: &TwoColumnReader, rows: &[u64]) {
        let tempdir = tempdir().unwrap();
        let backend = backend_at(&tempdir.path().to_string_lossy());
        let mut writer = new_writer::<K0>(&*backend, parameters(None), None);
        let rows0 = source.rows();
        for &row in rows {
            let keys = rows0.nth(row).unwrap();
            let rows1 = keys.next_column().unwrap();
            let values = unsafe { rows1.first() }.unwrap();
            writer.write1_raw(&values.raw_run().unwrap()).unwrap();
            assert!(writer.write0_raw(&keys.raw_item().unwrap()).unwrap());
        }
    }

    /// A debug build checks the order of keys that are copied, as it does
    /// for keys written decoded, though a copy never needs a key decoded.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "to column 0")]
    fn a_debug_build_rejects_copied_keys_out_of_order() {
        let tempdir = tempdir().unwrap();
        let backend = backend_at(&tempdir.path().to_string_lossy());
        copy_rows(&build(2, &*backend, None), &[1, 0]);
    }

    /// A debug build checks that a run of values copied under a key follows
    /// the values already under it: here the run's first value repeats an
    /// earlier one, because the run is the one copied just before it.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "to column 1")]
    fn a_debug_build_rejects_copied_values_out_of_order() {
        let tempdir = tempdir().unwrap();
        let backend = backend_at(&tempdir.path().to_string_lossy());
        let source = build(2, &*backend, None);
        let mut writer = new_writer::<K0>(&*backend, parameters(None), None);
        let rows0 = source.rows();
        let key = rows0.nth(1).unwrap();
        let rows1 = key.next_column().unwrap();
        let values = unsafe { rows1.first() }.unwrap();
        let run = values.raw_run().unwrap();
        writer.write1_raw(&run).unwrap();
        writer.write1_raw(&run).unwrap();
    }

    /// A debug build refuses a row group of another file as the model for
    /// crossing to the next column, which would make the row group it hands
    /// back read that file instead.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "another file")]
    fn next_column_like_rejects_a_row_group_of_another_file() {
        let tempdir = tempdir().unwrap();
        let backend = backend_at(&tempdir.path().to_string_lossy());
        let (one, other) = (build(1, &*backend, None), build(1, &*backend, None));
        let (rows0, other_rows0) = (one.rows(), other.rows());
        let key = rows0.nth(0).unwrap();
        let other_key = other_rows0.nth(0).unwrap();
        let other_values = other_key.next_column().unwrap();
        let _ = key.next_column_like(&other_values);
    }

    /// A cursor offers the key of whatever row it moves to as stored, and no
    /// key off the rows, however it moves.
    ///
    /// The cursor keeps the stored key it found until it moves, so every way
    /// of moving has to forget it; one that did not would hand out the key of
    /// the row it left, and a merge comparing keys that way would order its
    /// output wrongly.  Every way of moving starts, at least once, from a row
    /// whose key was just asked for, so that there is something to forget.
    /// Small blocks and a branching factor of three spread the rows over many
    /// data blocks under several levels of index, so the moves cross both.
    #[test]
    fn the_key_as_stored_follows_every_move() {
        let n = 200;
        let tempdir = tempdir().unwrap();
        let backend = backend_at(&tempdir.path().to_string_lossy());
        let keys: Vec<K0> = (0..n).map(key0).collect();
        let parameters = Parameters {
            max_branch: 3,
            ..parameters(None)
        };
        let (file, _) = write_keys(&keys, &*backend, parameters, None);
        let key_at = |row: usize| Some(keys[row].erase());
        // Strings that sort after and before every key.
        let (after_all, before_all) = (K0::from("z"), K0::new());

        let rows0 = file.rows();
        let mut cursor = rows0.nth(0).unwrap();
        assert_on_key(&cursor, key_at(0), "nth(0)");
        // SAFETY: every move below reads the file this test wrote, through
        // the factories it wrote it with.
        unsafe {
            cursor.move_next().unwrap();
            assert_on_key(&cursor, key_at(1), "move_next");
            cursor.move_prev().unwrap();
            assert_on_key(&cursor, key_at(0), "move_prev");
            cursor.move_prev().unwrap();
            assert_on_key(&cursor, None, "move_prev from the first row");
            cursor.move_next().unwrap();
            assert_on_key(&cursor, key_at(0), "move_next from before the rows");
            cursor.move_to_row(150).unwrap();
            assert_on_key(&cursor, key_at(150), "move_to_row(150)");

            // A clone moves on its own.
            let mut clone = cursor.clone();
            assert_on_key(&clone, key_at(150), "clone");
            clone.move_prev().unwrap();
            assert_on_key(&clone, key_at(149), "move_prev on the clone");
            assert_on_key(&cursor, key_at(150), "move_prev on its clone");

            cursor
                .advance_to_value_or_larger(keys[170].erase())
                .unwrap();
            assert_on_key(&cursor, key_at(170), "advance_to_value_or_larger");
            cursor.advance_to_value_or_larger(keys[10].erase()).unwrap();
            assert_on_key(
                &cursor,
                key_at(170),
                "advance_to_value_or_larger to a passed key",
            );
            let target = keys[180].clone();
            cursor
                .seek_forward_until(|key| key >= target.erase())
                .unwrap();
            assert_on_key(&cursor, key_at(180), "seek_forward_until");
            cursor.rewind_to_value_or_smaller(keys[20].erase()).unwrap();
            assert_on_key(&cursor, key_at(20), "rewind_to_value_or_smaller");
            cursor
                .rewind_to_value_or_smaller(keys[190].erase())
                .unwrap();
            assert_on_key(
                &cursor,
                key_at(20),
                "rewind_to_value_or_smaller to a passed key",
            );
            let target = keys[10].clone();
            cursor
                .seek_backward_until(|key| key <= target.erase())
                .unwrap();
            assert_on_key(&cursor, key_at(10), "seek_backward_until");

            cursor.move_last().unwrap();
            assert_on_key(&cursor, key_at(n - 1), "move_last");
            cursor.move_next().unwrap();
            assert_on_key(&cursor, None, "move_next from the last row");
            cursor.move_prev().unwrap();
            assert_on_key(&cursor, key_at(n - 1), "move_prev from past the rows");
            cursor.move_to_row(n as u64).unwrap();
            assert_on_key(&cursor, None, "move_to_row just past the rows");
            cursor.move_to_row(5).unwrap();
            assert_on_key(&cursor, key_at(5), "move_to_row from past the rows");
            cursor.move_first().unwrap();
            assert_on_key(&cursor, key_at(0), "move_first");
            cursor.move_to_row(u64::MAX).unwrap();
            assert_on_key(&cursor, None, "move_to_row(u64::MAX)");
            cursor.move_to_row(7).unwrap();
            assert_on_key(&cursor, key_at(7), "move_to_row(7)");
            cursor
                .advance_to_value_or_larger(after_all.erase())
                .unwrap();
            assert_on_key(&cursor, None, "advance_to_value_or_larger past every key");
            cursor.move_prev().unwrap();
            assert_on_key(&cursor, key_at(n - 1), "move_prev from past every key");
            cursor
                .rewind_to_value_or_smaller(before_all.erase())
                .unwrap();
            assert_on_key(&cursor, None, "rewind_to_value_or_smaller before every key");
        }

        // The values of one key are a row group in the middle of their
        // column, so moving off either end of it leaves the rows although the
        // column goes on.  This key's five values span data blocks.
        let row = 4;
        let values: Vec<K1> = values(row).into_iter().map(|(value, _)| value).collect();
        let value_at = |index: usize| Some(values[index].erase());
        let rows1 = rows0.nth(row as u64).unwrap().next_column().unwrap();
        let mut cursor = rows1.nth(0).unwrap();
        assert_on_key(&cursor, value_at(0), "nth(0) of the values");
        // SAFETY: as above.
        unsafe {
            cursor.move_next().unwrap();
            assert_on_key(&cursor, value_at(1), "move_next over the values");
            cursor.move_last().unwrap();
            assert_on_key(&cursor, value_at(4), "move_last over the values");
            cursor.move_next().unwrap();
            assert_on_key(&cursor, None, "move_next from the last value");
            cursor.move_prev().unwrap();
            assert_on_key(&cursor, value_at(4), "move_prev from past the values");
            cursor.move_first().unwrap();
            assert_on_key(&cursor, value_at(0), "move_first over the values");
            cursor.move_prev().unwrap();
            assert_on_key(&cursor, None, "move_prev from the first value");
            cursor
                .advance_to_value_or_larger(values[3].erase())
                .unwrap();
            assert_on_key(
                &cursor,
                value_at(3),
                "advance_to_value_or_larger over the values",
            );
            cursor
                .rewind_to_value_or_smaller(values[1].erase())
                .unwrap();
            assert_on_key(
                &cursor,
                value_at(1),
                "rewind_to_value_or_smaller over the values",
            );
            cursor.move_to_row(u64::MAX).unwrap();
            assert_on_key(&cursor, None, "move_to_row(u64::MAX) over the values");
        }
    }

    /// `aux_at` reads the weights of rows ahead of the cursor as far as the
    /// end of its data block or of its row group, whichever comes first, and
    /// no further.
    ///
    /// A merge counts the negative weights of a run it copies this way, so a
    /// read past either end would count a row that is not in the run.  Every
    /// data block of values here holds exactly `MAX_BRANCH` rows, which says
    /// where blocks end without asking the reader, and the keys own one to
    /// five values each, so some row groups end inside a block and some run
    /// across several.
    #[test]
    fn aux_at_stops_at_the_end_of_the_block_and_of_the_row_group() {
        const MAX_BRANCH: u64 = 4;
        let n = 40;
        let tempdir = tempdir().unwrap();
        let backend = backend_at(&tempdir.path().to_string_lossy());
        let keys: Vec<K0> = (0..n).map(key0).collect();
        let parameters = Parameters {
            max_branch: MAX_BRANCH as usize,
            ..parameters(None)
        };
        let (file, _) = write_keys(&keys, &*backend, parameters, None);

        // Runs that end where a block does, before the key's values do, and
        // runs that end where the key's values do, inside a block.
        let (mut ended_by_block, mut ended_by_row_group) = (0, 0);
        let mut weight = A1::default();
        // The row of the current key's first value, counted from the top of
        // the column.
        let mut first_row = 0;
        for row in 0..n {
            let expected = values(row);
            let end = first_row + expected.len() as u64;
            let rows1 = file.rows().nth(row as u64).unwrap().next_column().unwrap();
            let mut cursor = rows1.nth(0).unwrap();
            // SAFETY: the cursor reads the file this test wrote, through the
            // factories it wrote it with.
            unsafe {
                for index in 0..expected.len() {
                    let at = first_row + index as u64;
                    let block_end = (at / MAX_BRANCH + 1) * MAX_BRANCH;
                    let reach = block_end.min(end) - at;
                    ended_by_block += usize::from(block_end < end);
                    ended_by_row_group += usize::from(end < block_end);
                    let place = format!("key {row}, value {index}");
                    assert_eq!(
                        cursor.raw_run().map(|run| run.len() as u64),
                        Some(reach),
                        "{place}: the run does not end where the block or the key does",
                    );
                    for offset in 0..reach {
                        assert!(
                            cursor.aux_at(offset, weight.erase_mut()),
                            "{place}: aux_at({offset}) read nothing",
                        );
                        assert_eq!(
                            weight,
                            expected[index + offset as usize].1,
                            "{place}: aux_at({offset}) read the wrong weight",
                        );
                    }
                    assert!(
                        !cursor.aux_at(reach, weight.erase_mut()),
                        "{place}: aux_at({reach}) read past the end of the run",
                    );
                    assert!(
                        !cursor.aux_at(u64::MAX, weight.erase_mut()),
                        "{place}: aux_at(u64::MAX) read something",
                    );
                    cursor.move_next().unwrap();
                }
                assert!(
                    !cursor.aux_at(0, weight.erase_mut()),
                    "key {row}: aux_at read past the last value",
                );
                assert!(
                    !rows1.before().aux_at(0, weight.erase_mut()),
                    "key {row}: aux_at read before the first value",
                );
            }
            first_row = end;
        }
        assert!(
            ended_by_block > 0 && ended_by_row_group > 0,
            "the runs ended {ended_by_block} times at a block's end and \
             {ended_by_row_group} times at a key's, but both have to happen",
        );
    }

    /// The way [`a_file_interleaves_copied_and_encoded_rows`] writes one key's
    /// values into its copy.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    enum Way {
        /// Leaves the key out, values and all.
        Skipped,
        /// Encodes every value.
        Encoded,
        /// Copies every value, a run at a time.
        Copied,
        /// Encodes the values up to a row picked at random and copies the
        /// rest.
        EncodedThenCopied,
        /// Copies runs until it reaches a row picked at random and encodes
        /// the rest.
        CopiedThenEncoded,
    }

    impl Way {
        /// Every way, to pick from.
        const ALL: [Way; 5] = [
            Way::Skipped,
            Way::Encoded,
            Way::Copied,
            Way::EncodedThenCopied,
            Way::CopiedThenEncoded,
        ];
    }

    /// A file can mix copied rows with encoded ones, in any order and under
    /// one key, and reads back as if every row had been encoded.
    ///
    /// A merge copies what one input holds alone and encodes the rest, so a
    /// block of its output can hold encoded items after copied ones and the
    /// reverse, a copied key can own values that were encoded, and a copy can
    /// begin in the middle of a source block.  Each key of the source here
    /// goes into the copy a way picked at random: its values encoded, copied,
    /// or some of each, and the key itself copied or encoded; or the key is
    /// left out, so that the copy's rows do not line up with the source's.
    /// The files have data blocks of two, three and seven items, and of as
    /// many as fit, stored as is and with each codec, with and without a Bloom
    /// filter.
    #[test]
    fn a_file_interleaves_copied_and_encoded_rows() {
        let n = 300;
        let keys: Vec<K0> = (0..n).map(key0).collect();
        // Every way a key went in, with whether the key itself was copied.
        let mut ways = HashSet::new();
        let mut seed = 0;
        for max_branch in [2, 3, 7, usize::MAX] {
            for compression in COMPRESSIONS {
                for bloom in [false, true] {
                    println!("max_branch {max_branch}, {compression:?}, Bloom filter {bloom}");
                    seed += 1;
                    let mut rng = ChaCha8Rng::seed_from_u64(seed);
                    let tempdir = tempdir().unwrap();
                    let backend = backend_at(&tempdir.path().to_string_lossy());
                    let parameters = Parameters {
                        max_branch,
                        ..parameters(compression)
                    };
                    let (source, _) = write_keys(&keys, &*backend, parameters.clone(), None);
                    source.evict();
                    let filter = if bloom {
                        FilterPlan::<DynData>::decide_filter(None, n)
                    } else {
                        None
                    };
                    let mut writer = new_writer::<K0>(&*backend, parameters, filter);

                    let rows0 = source.rows();
                    let mut kept = Vec::new();
                    for row in 0..n {
                        let key = rows0.nth(row as u64).unwrap();
                        let values = key.next_column().unwrap();
                        let len = values.len();
                        let way = *Way::ALL.choose(&mut rng).unwrap();
                        match way {
                            Way::Skipped => {
                                ways.insert((way, false));
                                continue;
                            }
                            Way::Encoded => encode_values(&mut writer, row, 0..len),
                            Way::Copied => {
                                copy_runs(&mut writer, &values, 0, len);
                            }
                            Way::EncodedThenCopied => {
                                let head = rng.gen_range(0..=len);
                                encode_values(&mut writer, row, 0..head);
                                copy_runs(&mut writer, &values, head, len);
                            }
                            Way::CopiedThenEncoded => {
                                let until = rng.gen_range(0..=len);
                                let copied = copy_runs(&mut writer, &values, 0, until);
                                encode_values(&mut writer, row, copied..len);
                            }
                        }
                        let copy_key = rng.gen_bool(0.5);
                        if copy_key {
                            let item = key.raw_item().unwrap();
                            assert!(
                                writer.write0_raw(&item).unwrap(),
                                "key {row} was refused as bytes",
                            );
                        } else {
                            writer.write0((key.key().unwrap(), ().erase())).unwrap();
                        }
                        ways.insert((way, copy_key));
                        kept.push(row);
                    }
                    let (copy, filters) = writer.into_reader(BatchMetadata::default()).unwrap();
                    copy.evict();

                    assert_holds(&copy, kept.iter().map(|&row| (row, key0(row))));
                    assert_seeks_land(&copy, &kept, n);
                    let kind = if bloom {
                        FilterKind::Bloom
                    } else {
                        FilterKind::None
                    };
                    assert_eq!(filters.membership_filter_kind(), kind);
                    for &row in &kept {
                        assert!(
                            filters.maybe_contains_key(keys[row].erase(), None),
                            "key {row} is missing from the filter",
                        );
                    }
                }
            }
        }
        assert_eq!(
            ways.len(),
            2 * Way::ALL.len() - 1,
            "some way of writing a key never came up: {ways:?}",
        );
    }

    /// A key that the copy's membership filter cannot take as bytes is refused
    /// without taking the rows written for it, and the same key written
    /// decoded owns them.
    ///
    /// A roaring filter needs every key itself rather than a hash of it, and a
    /// `Product` key cannot hash its archived form the way it hashes decoded,
    /// so a Bloom filter cannot take one as bytes either; without a filter,
    /// the same keys go in as bytes.  The keys own one to five values each,
    /// and in every case each key has to own its own after the copy, and the
    /// filter has to find every key.
    #[test]
    fn a_refused_key_leaves_its_rows_for_the_decoded_key() {
        let tempdir = tempdir().unwrap();
        let backend = backend_at(&tempdir.path().to_string_lossy());

        let keys: Vec<u32> = (0..300).map(|i| i * 3).collect();
        let roaring = BatchKeyFilter::new_roaring_u32::<DynData>(0u32.erase());
        let (copy, filters, refused) = copy_keys(&keys, &*backend, Some(roaring));
        assert_eq!(refused, keys.len(), "a roaring filter took keys as bytes");
        assert_eq!(filters.membership_filter_kind(), FilterKind::Roaring);
        assert_holds(&copy, keys.iter().copied().enumerate());
        // A roaring filter is exact, so it finds every key and nothing else.
        for &key in &keys {
            assert!(
                filters.maybe_contains_key(key.erase(), None),
                "{key} is missing from the filter",
            );
            assert!(
                !filters.maybe_contains_key((key + 1).erase(), None),
                "the filter holds {}, which is not a key",
                key + 1,
            );
        }

        let keys: Vec<Product<u32, u32>> = (0..300).map(|i| Product::new(i / 7, i)).collect();
        for filter in [FilterPlan::<DynData>::decide_filter(None, keys.len()), None] {
            let bloom = filter.is_some();
            let (copy, filters, refused) = copy_keys(&keys, &*backend, filter);
            assert_eq!(
                refused,
                if bloom { keys.len() } else { 0 },
                "the keys refused as bytes, with a Bloom filter: {bloom}",
            );
            assert_holds(&copy, keys.iter().cloned().enumerate());
            for key in &keys {
                assert!(
                    filters.maybe_contains_key(key.erase(), None),
                    "{key:?} is missing from the filter",
                );
            }
        }
    }

    /// A copied key goes into a Bloom filter by the hash of its archived form,
    /// which for every key type that has one is the hash of the decoded key,
    /// and a key of a type without one goes in decoded.
    ///
    /// A key recorded by any other hash would be missing from the filter when
    /// a lookup hashes the decoded key, so the lookup would find nothing.  The
    /// types cover text short enough to be kept inline and long enough not to
    /// be, floats including a negative zero and NaN, integers of several
    /// widths, options, tuples, vectors and maps, and these nested.  A tuple
    /// of ten fields is stored in a wider layout, whose archived form cannot
    /// be hashed, so its keys go in decoded.
    #[test]
    fn copied_keys_hash_like_decoded_keys() {
        type Wide = Tup10<
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
        >;
        let n = 200i64;
        let text = |i: i64| format!("s{i:+05}{}", "x".repeat((i.unsigned_abs() % 23) as usize));
        // Field `bit` of a wide tuple, present where bit `bit` of `i` is set.
        let field = |i: i64, bit: i64| ((i >> bit) & 1 == 1).then_some((i * bit) as i32);

        assert_copied_keys_hash_like_decoded((0..n).map(text).collect::<Vec<_>>(), true);
        assert_copied_keys_hash_like_decoded(
            (0..n)
                .map(|i| (i as f64 - 100.5) * 0.37)
                .chain([-0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY])
                .map(F64::new)
                .collect(),
            true,
        );
        assert_copied_keys_hash_like_decoded(
            (0..n)
                .map(|i| (i as f32 - 100.5) * 0.37)
                .chain([-0.0, f32::NAN])
                .map(F32::new)
                .collect(),
            true,
        );
        assert_copied_keys_hash_like_decoded((-100..100).map(|i: i64| i as i8).collect(), true);
        assert_copied_keys_hash_like_decoded(
            (0..n).map(|i| (i as i128 - 100) << 70).collect(),
            true,
        );
        assert_copied_keys_hash_like_decoded(
            (0..n).map(|i| ((i as u128) << 90) | 7).collect(),
            true,
        );
        assert_copied_keys_hash_like_decoded(
            (0..n).map(|i| (i % 7 != 0).then(|| text(i))).collect(),
            true,
        );
        assert_copied_keys_hash_like_decoded((0..n).map(|i| Tup1(text(i))).collect(), true);
        assert_copied_keys_hash_like_decoded(
            (0..n).map(|i| Tup2(i as i32 / 3, text(i))).collect(),
            true,
        );
        assert_copied_keys_hash_like_decoded(
            (0..n)
                .map(|i| Tup3((i % 3 != 0).then(|| text(i)), i / 2, i % 2 == 0))
                .collect(),
            true,
        );
        assert_copied_keys_hash_like_decoded(
            (0..n)
                .map(|i| Tup4(i as i32, text(i), i % 2 == 0, F64::new(i as f64 / 3.0)))
                .collect(),
            true,
        );
        assert_copied_keys_hash_like_decoded(
            (0..n)
                .map(|i| {
                    (0..i % 4)
                        .map(|j| (j != 1).then_some((j * i) as i32))
                        .collect::<Vec<_>>()
                })
                .collect(),
            true,
        );
        assert_copied_keys_hash_like_decoded(
            (0..n)
                .map(|i| (0..i % 4).map(|j| (text(j), i)).collect::<BTreeMap<_, _>>())
                .collect(),
            true,
        );
        assert_copied_keys_hash_like_decoded(
            (0..n)
                .map(|i| {
                    Tup2(
                        (i % 3 != 0).then(|| (0..i % 3).map(text).collect::<Vec<_>>()),
                        i,
                    )
                })
                .collect(),
            true,
        );
        assert_copied_keys_hash_like_decoded(
            (0..n)
                .map(|i| (0..i % 3).map(|j| (j * i) as u128).collect::<Vec<_>>())
                .collect(),
            true,
        );

        assert_copied_keys_hash_like_decoded::<Wide>(
            (0..n)
                .map(|i| {
                    Tup10::new(
                        field(i, 0),
                        field(i, 1),
                        field(i, 2),
                        field(i, 3),
                        field(i, 4),
                        field(i, 5),
                        field(i, 6),
                        field(i, 7),
                        field(i, 8),
                        None,
                    )
                })
                .collect(),
            false,
        );
    }
}
