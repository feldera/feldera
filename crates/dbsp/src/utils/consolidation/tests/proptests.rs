#![cfg_attr(miri, ignore)]

use crate::{
    dynamic::LeanVec,
    utils::{
        consolidation::{
            consolidate, consolidate_from, consolidate_paired_slices, consolidate_payload_from,
            consolidate_slice,
            quicksort::quicksort,
            utils::{dedup_payload_starting_at, retain_starting_at},
        },
        Tup2, VecExt,
    },
};
use proptest::{collection::vec, prelude::*};
use std::collections::BTreeMap;

prop_compose! {
    /// Create a batch data tuple
    fn tuple()(key in 0..10_000usize, value in 0..10_000usize, diff in -10_000..=10_000isize) -> ((usize, usize), isize) {
        ((key, value), diff)
    }
}

prop_compose! {
    /// Generate a random batch of data
    fn batch()(batch in vec(tuple().prop_map(|((k,v), w)| Tup2(Tup2(k, v), w)), 0..50_000)) -> LeanVec<Tup2<Tup2<usize, usize>, isize>> {
        LeanVec::from(batch)
    }
}

prop_compose! {
    fn random_vec()(batch in vec(any::<u16>(), 0..5000)) -> Vec<u16> {
        batch
    }
}

prop_compose! {
    fn random_paired_vecs()
        (len in 0..=5000usize)
        (left in vec(any::<u16>(), len), right in vec(any::<i16>(), len))
    -> (Vec<u16>, Vec<i16>) {
        debug_assert_eq!(left.len(), right.len());
        (left, right)
    }
}

prop_compose! {
    fn multiple_payloads()
        (len in 0..=5000usize)
        (
            left in vec(any::<u8>(), len),
            values1 in vec(any::<i16>(), len),
            values2 in vec(any::<u8>(), len),
            values3 in vec(any::<u32>(), len),
        )
    -> (Vec<u8>, Vec<i16>, Vec<u8>, Vec<u32>) {
        debug_assert_eq!(left.len(), values1.len());
        debug_assert_eq!(left.len(), values2.len());
        debug_assert_eq!(left.len(), values3.len());
        (left, values1, values2, values3)
    }
}

fn batch_data(batch: &[Tup2<Tup2<usize, usize>, isize>]) -> BTreeMap<Tup2<usize, usize>, i64> {
    let mut values = BTreeMap::new();
    for &Tup2(tuple, diff) in batch {
        values
            .entry(tuple)
            .and_modify(|acc| *acc += diff as i64)
            .or_insert(diff as i64);
    }

    // Elements with a value of zero are removed in consolidation
    values.retain(|_, &mut diff| diff != 0);
    values
}

fn paired_batch_data(
    keys: &[Tup2<usize, usize>],
    diffs: &[isize],
) -> BTreeMap<Tup2<usize, usize>, i64> {
    let mut values = BTreeMap::new();
    for (&tuple, &diff) in keys.iter().zip(diffs) {
        values
            .entry(tuple)
            .and_modify(|acc| *acc += diff as i64)
            .or_insert(diff as i64);
    }

    // Elements with a value of zero are removed in consolidation
    values.retain(|_, &mut diff| diff != 0);
    values
}

proptest! {
    #[test]
    fn consolidate_batch(mut batch in batch()) {
        let expected = batch_data(batch.as_slice());
        consolidate(&mut batch);
        let output = batch_data(batch.as_slice());

        // Ensure the batch is sorted
        prop_assert!(batch.is_sorted_by(|Tup2(a, _), Tup2(b, _)| a.cmp(b)));
        // Ensure no diff values are zero
        prop_assert!(batch.as_slice().iter().all(|&Tup2(_, diff)| diff != 0));
        // Ensure the aggregated data is the same
        prop_assert_eq!(expected, output);
    }

    #[test]
    fn consolidate_impls_are_equivalent(batch in batch()) {
        let expected = batch_data(batch.as_slice());

        let mut vec = batch.clone();
        consolidate(&mut vec);
        prop_assert!(vec.as_slice().iter().all(|&Tup2(_, diff)| diff != 0));
        prop_assert!(vec.is_sorted_by(|Tup2(a, _), Tup2(b, _)| a.cmp(b)));
        prop_assert_eq!(&expected, &batch_data(vec.as_slice()));

        let mut vec_offset = batch.clone();
        consolidate_from(&mut vec_offset, 0);
        prop_assert!(vec_offset.as_slice().iter().all(|&Tup2(_, diff)| diff != 0));
        prop_assert!(vec_offset.is_sorted_by(|Tup2(a, _), Tup2(b, _)| a.cmp(b)));
        prop_assert_eq!(&expected, &batch_data(vec.as_slice()));
        prop_assert_eq!(&vec, &vec_offset);

        let mut slice = batch;
        let len = consolidate_slice(slice.as_mut_slice());
        slice.truncate(len);
        prop_assert!(slice.as_slice().iter().all(|&Tup2(_, diff)| diff != 0));
        prop_assert!(slice.is_sorted_by(|Tup2(a, _), Tup2(b, _)| a.cmp(b)));
        prop_assert_eq!(&expected, &batch_data(slice.as_slice()));
        prop_assert_eq!(&vec, &slice);
    }

    #[test]
    fn consolidate_pair_is_equivalent(batch in batch()) {
        let expected = batch_data(batch.as_slice());

        let mut consolidated = batch.clone();
        consolidate(&mut consolidated);

        let (mut keys, mut diffs): (Vec<_>, Vec<_>) = batch.as_slice().iter().cloned().map(|Tup2(x,y)| (x,y)).unzip();
        let len = consolidate_paired_slices(&mut keys, &mut diffs);
        keys.truncate(len);
        diffs.truncate(len);

        prop_assert!(diffs.iter().all(|&diff| diff != 0));
        prop_assert!(keys.is_sorted_by(|a, b| a.partial_cmp(b)));
        prop_assert_eq!(expected, paired_batch_data(&keys, &diffs));

        let (consolidated_keys, consolidated_diffs): (Vec<_>, Vec<_>) = consolidated.as_slice().iter().cloned().map(|Tup2(x,y)| (x,y)).unzip();
        prop_assert_eq!(consolidated_keys, keys);
        prop_assert_eq!(consolidated_diffs, diffs);
    }

    #[test]
    fn consolidate_payload_from_is_equivalent(batch in batch()) {
        let expected = batch_data(batch.as_slice());

        let mut consolidated = batch.clone();
        consolidate(&mut consolidated);

        let (mut keys, mut diffs): (Vec<_>, Vec<_>) = batch.as_slice().iter().cloned().map(|Tup2(x,y)| (x,y)).unzip();
        consolidate_payload_from(&mut keys, &mut diffs, 0);

        prop_assert!(diffs.iter().all(|&diff| diff != 0));
        prop_assert!(keys.is_sorted_by(|a, b| a.partial_cmp(b)));
        prop_assert_eq!(expected, paired_batch_data(&keys, &diffs));

        let (consolidated_keys, consolidated_diffs): (Vec<_>, Vec<_>) = consolidated.as_slice().iter().cloned().map(|Tup2(x,y)| (x,y)).unzip();
        prop_assert_eq!(consolidated_keys, keys);
        prop_assert_eq!(consolidated_diffs, diffs);
    }

    #[test]
    fn dual_quicksort_smoke(mut data in vec(any::<(u32, u32)>(), 0..=5000)) {
        let (mut keys, mut values): (Vec<_>, Vec<_>) = data.clone().into_iter().unzip();
        quicksort(&mut keys, &mut values);

        data.sort_unstable_by_key(|&(key, _)| key);

        super::assert_sorted_eq(& data, &keys, &values);
    }

    #[test]
    fn retain_equivalence(mut expected in random_vec()) {
        let mut output = expected.clone();
        retain_starting_at(&mut output, 0, |a| *a % 5 == 0);
        expected.retain(|a| *a % 5 == 0);
        prop_assert_eq!(output, expected);
    }

    #[test]
    fn dedup_equivalence(mut expected in random_vec()) {
        let mut output = expected.clone();
        dedup_payload_starting_at(&mut output, (), 0, |a, (), b, ()| *a == *b);
        expected.dedup_by(|a, b| *a == *b);
        prop_assert_eq!(output, expected);
    }

    #[test]
    fn dedup_payload_equivalence((mut keys, mut values) in random_paired_vecs()) {
        keys.sort_unstable();

        let mut expected: Vec<(_, _)> = keys.iter().copied().zip(values.iter().copied()).collect();
        expected.dedup_by(|(a, _), (b, _)| a == b);
        let (expected_keys, expected_values): (Vec<_>, Vec<_>) = expected.into_iter().unzip();

        dedup_payload_starting_at(&mut keys, &mut values, 0, |a, _, b, _| *a == *b);
        prop_assert_eq!(keys, expected_keys);
        prop_assert_eq!(values, expected_values);
    }

    #[test]
    fn dedup_multiple_payloads((mut keys, mut values1, mut values2, mut values3) in multiple_payloads()) {
        let mut expected = Vec::with_capacity(keys.len());
        for idx in 0..keys.len() {
            expected.push((keys[idx], values1[idx], values2[idx], values3[idx]));
        }
        expected.dedup_by(|(a, ..), (b, ..)| a == b);

        let (mut expected_keys, mut expected_values1, mut expected_values2, mut expected_values3) = (
            Vec::with_capacity(expected.len()),
            Vec::with_capacity(expected.len()),
            Vec::with_capacity(expected.len()),
            Vec::with_capacity(expected.len()),
        );
        for (key, value1, value2, value3) in expected{
            expected_keys.push(key);
            expected_values1.push(value1);
            expected_values2.push(value2);
            expected_values3.push(value3);
        }

        dedup_payload_starting_at(&mut keys, (&mut values1, (&mut values2, &mut values3)), 0, |a, _, b, _| *a == *b);
        prop_assert_eq!(keys, expected_keys);
        prop_assert_eq!(values1, expected_values1);
        prop_assert_eq!(values2, expected_values2);
        prop_assert_eq!(values3, expected_values3);
    }

    #[test]
    fn quicksort_correctness(mut batch in vec(any::<(u16, u16)>(), 0..50_000)) {
        // Split the data into keys and values
        let (mut keys, mut values): (Vec<_>, Vec<_>) = batch.clone().into_iter().unzip();

        // Sort the given data
        quicksort(&mut keys, &mut values);

        // Make sure we didn't lose any elements
        prop_assert_eq!(keys.len(), values.len());
        prop_assert_eq!(keys.len(), batch.len());

        // Recombine the keys and values into a single vector
        let mut results: Vec<_> = keys.into_iter().zip(values).collect();

        // Ensure that the results vec is properly sorted
        prop_assert!(results.is_sorted_by_key(|&(key, _)| key));

        // Sort both the input batch and the results the same way
        // so that if their contents are equal (that is, if `quicksort()`
        // didn't drop any elements) then both vectors will be equal
        batch.sort();
        results.sort();

        // Ensure that no elements were lost during sorting
        prop_assert_eq!(batch, results);
    }
}
