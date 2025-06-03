
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

fn test_one_column<K, A>(n: usize, expected: impl Fn(usize) -> (K, K, K, A), parameters: Parameters)
where
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
