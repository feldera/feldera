use std::{io::Cursor, marker::PhantomData, sync::Arc};

use crate::{
    DBWeight,
    dynamic::{DataTrait, DowncastTrait, DynWeight, Factory, LeanVec, Vector, WithFactory},
    storage::{
        backend::{BlockLocation, StorageBackend},
        buffer_cache::BufferCache,
        file::{
            format::{
                BLOOM_FILTER_BLOCK_MAGIC, BatchMetadata, Compression, FileTrailer,
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

use crate::{
    DBData,
    dynamic::{DynData, Erase},
};
use binrw::BinRead;
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
        if rand::random() {
            let (_before, key, _after, diff) = (expected_fn)(i);
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

fn sampled_filter_plan<K>(
    factories: &Factories<DynData, DynData>,
    keys: &[K],
) -> FilterPlan<DynData>
where
    K: DBData + Erase<DynData>,
{
    let mut sampled_keys = factories.keys_factory.default_box();
    sampled_keys.reserve(keys.len());
    for key in keys {
        sampled_keys.push_ref(key.erase());
    }

    FilterPlan::from_bounds(keys.first().unwrap().erase(), keys.last().unwrap().erase())
        .with_sampled_keys(sampled_keys)
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
fn test_bloom_filter_roundtrip_and_block_kind() {
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
        assert_eq!(filter_block_magic(&reader), Some(BLOOM_FILTER_BLOCK_MAGIC));
        assert_eq!(incompatible_features(&reader), 0);
    }
}

#[test]
fn test_roaring_u32_filter_roundtrip_exact_and_block_kind() {
    init_test_logger();

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

        let filter_plan = sampled_filter_plan(&factories, &[1u32, 3, 7]);
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
}

#[test]
fn test_roaring_tup1_i32_filter_roundtrip_exact_and_block_kind() {
    init_test_logger();

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

        let filter_plan = sampled_filter_plan(&factories, &[Tup1(-7i32), Tup1(1), Tup1(3)]);
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
}

#[test]
fn test_writer_without_filter_plan_uses_bloom_filter() {
    init_test_logger();

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
    assert_eq!(filter_block_magic(&reader), Some(BLOOM_FILTER_BLOCK_MAGIC));
}

#[test]
fn test_filter_plan_without_sample_falls_back_to_bloom() {
    init_test_logger();

    let filter_plan = FilterPlan::from_bounds((&1u32) as &DynData, (&7u32) as &DynData);
    assert!(matches!(
        FilterPlan::decide_filter(Some(&filter_plan), 3),
        Some(super::BatchKeyFilter::Bloom(_))
    ));
}

#[test]
fn test_filter_plan_predictor_prefers_roaring_for_dense_sample() {
    init_test_logger();

    let factories = Factories::<DynData, DynData>::new::<u32, ()>();
    let keys: Vec<u32> = (0..50_000).collect();
    let filter_plan = sampled_filter_plan(&factories, keys.as_slice());

    assert!(matches!(
        FilterPlan::decide_filter(Some(&filter_plan), keys.len()),
        Some(super::BatchKeyFilter::RoaringU32(_))
    ));
}

#[test]
fn test_filter_plan_predictor_prefers_bloom_for_sparse_wide_sample() {
    init_test_logger();

    let factories = Factories::<DynData, DynData>::new::<u32, ()>();
    let keys: Vec<u32> = (0..50_000).map(|index| index << 16).collect();
    let filter_plan = sampled_filter_plan(&factories, keys.as_slice());

    assert!(matches!(
        FilterPlan::decide_filter(Some(&filter_plan), keys.len()),
        Some(super::BatchKeyFilter::Bloom(_))
    ));
}

#[test]
fn test_roaring_i64_filter_roundtrip_uses_batch_min_offset() {
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

        let min = (i64::from(u32::MAX) * 4) + 10;
        let keys = [min, min + 3, min + 7];
        let filter_plan = sampled_filter_plan(&factories, &keys);
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
}

#[test]
fn test_roaring_u64_filter_roundtrip_uses_batch_min_offset() {
    init_test_logger();

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
    let filter_plan = sampled_filter_plan(&factories, &keys);
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
}

#[test]
fn test_i64_keys_fallback_to_bloom_when_span_exceeds_u32() {
    init_test_logger();

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
    assert_eq!(filter_block_magic(&reader), Some(BLOOM_FILTER_BLOCK_MAGIC));
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
