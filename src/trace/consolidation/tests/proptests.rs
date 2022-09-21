#![cfg_attr(miri, ignore)]

use crate::{
    trace::consolidation::{
        consolidate, consolidate_from, consolidate_slice,
        utils::{
            dedup_starting_at, retain_starting_at, shuffle_by_indices, shuffle_by_indices_bitvec,
        },
    },
    utils::VecExt,
};
use bitvec::vec::BitVec;
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
    fn batch()(batch in vec(tuple(), 0..50_000)) -> Vec<((usize, usize), isize)> {
        batch
    }
}

prop_compose! {
    fn random_vec()(batch in vec(any::<u16>(), 0..5000)) -> Vec<u16> {
        batch
    }
}

prop_compose! {
    fn random_paired_vecs()
        (len in 0..5000usize)
        (left in vec(any::<u16>(), len), right in vec(any::<i16>(), len))
    -> (Vec<u16>, Vec<i16>) {
        assert_eq!(left.len(), right.len());
        (left, right)
    }
}

fn batch_data(batch: &[((usize, usize), isize)]) -> BTreeMap<(usize, usize), i64> {
    let mut values = BTreeMap::new();
    for &(tuple, diff) in batch {
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
        let expected = batch_data(&batch);
        consolidate(&mut batch);
        let output = batch_data(&batch);

        // Ensure the batch is sorted
        prop_assert!(batch.is_sorted_by(|(a, _), (b, _)| a.partial_cmp(b)));
        // Ensure no diff values are zero
        prop_assert!(batch.iter().all(|&(_, diff)| diff != 0));
        // Ensure the aggregated data is the same
        prop_assert_eq!(expected, output);
    }

    #[test]
    fn consolidate_impls_are_equivalent(batch in batch()) {
        let expected = batch_data(&batch);

        let mut vec = batch.clone();
        consolidate(&mut vec);
        prop_assert!(vec.iter().all(|&(_, diff)| diff != 0));
        prop_assert!(vec.is_sorted_by(|(a, _), (b, _)| a.partial_cmp(b)));
        prop_assert!(vec.iter().all(|&(_, diff)| diff != 0));
        prop_assert_eq!(&expected, &batch_data(&vec));

        let mut vec_offset = batch.clone();
        consolidate_from(&mut vec_offset, 0);
        prop_assert!(vec_offset.iter().all(|&(_, diff)| diff != 0));
        prop_assert!(vec_offset.is_sorted_by(|(a, _), (b, _)| a.partial_cmp(b)));
        prop_assert!(vec_offset.iter().all(|&(_, diff)| diff != 0));
        prop_assert_eq!(&expected, &batch_data(&vec));
        prop_assert_eq!(&vec, &vec_offset);

        let mut slice = batch;
        let len = consolidate_slice(&mut slice);
        prop_assert!(slice[..len].iter().all(|&(_, diff)| diff != 0));
        // prop_assert!(slice[..len].is_sorted_by(|(a, _), (b, _)| a.partial_cmp(b)));
        prop_assert!(slice[..len].iter().all(|&(_, diff)| diff != 0));
        prop_assert_eq!(&expected, &batch_data(&slice[..len]));
        prop_assert_eq!(&vec, &slice[..len]);
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
        dedup_starting_at(&mut output, 0, |a, b| *a == *b);
        expected.dedup_by(|a, b| *a == *b);
        prop_assert_eq!(output, expected);
    }

    #[test]
    fn shuffle_by_indices_equivalence((mut values, mut diffs) in random_paired_vecs()) {
        let mut expected_indices: Vec<_> = (0..values.len()).collect();
        expected_indices.sort_by_key(|&idx| values[idx]);

        let (mut output_values, mut output_diffs) = (vec![0; values.len()], vec![0; values.len()]);
        for (current, &idx) in expected_indices.iter().enumerate() {
            output_values[current] = values[idx];
            output_diffs[current] = diffs[idx];
        }

        unsafe { shuffle_by_indices(&mut values, &mut diffs, &mut expected_indices) };
        prop_assert_eq!(values, output_values);
        prop_assert_eq!(diffs, output_diffs);
    }

    #[test]
    fn shuffle_by_indices_bitvec_equivalence((mut values, mut diffs) in random_paired_vecs()) {
        let mut visited = BitVec::repeat(false, values.len());
        let mut expected_indices: Vec<_> = (0..values.len()).collect();
        expected_indices.sort_by_key(|&idx| values[idx]);

        let (mut output_values, mut output_diffs) = (vec![0; values.len()], vec![0; values.len()]);
        for (current, &idx) in expected_indices.iter().enumerate() {
            output_values[current] = values[idx];
            output_diffs[current] = diffs[idx];
        }

        unsafe { shuffle_by_indices_bitvec(&mut values, &mut diffs, &mut expected_indices, &mut visited) };
        prop_assert_eq!(values, output_values);
        prop_assert_eq!(diffs, output_diffs);
    }
}
