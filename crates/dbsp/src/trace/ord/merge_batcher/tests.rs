#![cfg(test)]

use crate::{
    algebra::MonoidValue,
    trace::{
        ord::{
            merge_batcher::{MergeBatcher, MergeSorter},
            OrdValBatch,
        },
        Batcher,
    },
};
use std::marker::PhantomData;

fn preallocated_stashes<K, V>(stashes: usize) -> Vec<Vec<(K, V)>>
where
    K: Ord,
    V: MonoidValue,
{
    (0..stashes)
        .map(|_| Vec::with_capacity(<MergeSorter<K, V>>::BUFFER_ELEMENTS))
        .collect()
}

// Merging empty lists should produce an empty list
#[test]
fn merge_empty_inputs() {
    let mut merger: MergeSorter<usize, isize> = MergeSorter::new();
    let merged = merger.merge_by(Vec::new(), Vec::new());
    assert!(merged.is_empty());
}

#[test]
fn small_push() {
    let mut merger = MergeSorter::<(usize, usize), isize> {
        queue: vec![vec![vec![((0, 0), 1)], vec![((45, 0), -1)]]],
        stash: Vec::new(),
    };

    let mut batch = vec![((45, 1), 1)];
    merger.push(&mut batch);

    let mut output = Vec::new();
    merger.finish_into(&mut output);

    assert_eq!(
        output,
        vec![vec![((0, 0), 1), ((45, 0), -1)], vec![((45, 1), 1)]],
    );
}

#[test]
fn merge_by() {
    let mut merger: MergeSorter<usize, isize> = MergeSorter::new();

    let left = vec![vec![(0, 1), (1, 6), (24, 5), (54, -23)]];
    let right = vec![vec![(0, 7), (1, 6), (24, -5), (25, 12), (89, 1)]];
    let merged = merger.merge_by(left, right);

    // TODO: Not entirely sure if this is an optimal result, we always leave one
    // element trailing
    let expected = vec![vec![(0, 8), (1, 12), (25, 12), (54, -23)], vec![(89, 1)]];
    assert_eq!(merged, expected);
}

#[test]
fn push_with_excess_stashes() {
    let mut merger: MergeSorter<usize, isize> = MergeSorter {
        queue: Vec::new(),
        stash: preallocated_stashes(5),
    };
    merger.push(&mut vec![(0, 1), (1, 6), (24, 5), (54, -23)]);

    let mut output = Vec::new();
    merger.finish_into(&mut output);

    assert_eq!(output, vec![vec![(0, 1), (1, 6), (24, 5), (54, -23)]]);
}

#[test]
fn force_finish_merge() {
    let mut merger: MergeSorter<usize, isize> = MergeSorter {
        queue: vec![
            vec![vec![(0, 1), (1, 6), (24, 5), (54, -23)]],
            vec![vec![(89, 1)]],
            vec![vec![(0, 8), (1, 12), (25, 12), (54, -23)]],
            vec![vec![(23, 54), (97, -102)]],
        ],
        stash: preallocated_stashes(5),
    };

    let mut output = Vec::new();
    merger.finish_into(&mut output);

    let expected = vec![
        vec![(0, 9), (1, 18), (23, 54), (24, 5), (25, 12), (54, -46)],
        vec![(89, 1)],
        vec![(97, -102)],
    ];
    assert_eq!(output, expected);
}

#[test]
fn force_merge_on_push() {
    let mut merger: MergeSorter<usize, isize> = MergeSorter {
        queue: vec![
            vec![vec![(0, 1), (1, 6), (24, 5), (54, -23)]],
            vec![vec![(89, 1)]],
            vec![vec![(0, 8), (1, 12), (25, 12), (54, -23)]],
        ],
        stash: preallocated_stashes(5),
    };

    merger.push(&mut vec![(23, 54), (97, -102)]);

    let mut output = Vec::new();
    merger.finish_into(&mut output);

    let expected = vec![
        vec![(0, 9), (1, 18), (23, 54), (24, 5), (25, 12), (54, -46)],
        vec![(89, 1)],
        vec![(97, -102)],
    ];
    assert_eq!(output, expected);
}

#[test]
fn count_tuples() {
    let empty = <MergeSorter<usize, isize>>::new();
    assert_eq!(empty.tuples(), 0);

    let still_empty: MergeSorter<u64, i64> = MergeSorter {
        queue: Vec::new(),
        stash: preallocated_stashes(100),
    };
    assert_eq!(still_empty.tuples(), 0);

    let has_tuples: MergeSorter<(u64, u64), i64> = MergeSorter {
        queue: vec![
            vec![Vec::new(), vec![((0, 0), 0); 1000], vec![((1, 1), 1)]],
            Vec::new(),
            vec![vec![((10, 10), 10); 256]],
        ],
        stash: vec![Vec::new(); 100],
    };
    assert_eq!(has_tuples.tuples(), 1257);

    #[allow(clippy::type_complexity)]
    let batcher: MergeBatcher<(u64, u64), u32, i64, OrdValBatch<u64, u64, u32, i64>> =
        MergeBatcher {
            sorter: has_tuples,
            time: 0,
            phantom: PhantomData,
        };
    assert_eq!(batcher.tuples(), 1257);
}

// These tests will utterly destroy miri's performance
#[cfg_attr(miri, ignore)]
mod proptests {
    use super::preallocated_stashes;
    use crate::{
        trace::{consolidation::consolidate, ord::merge_batcher::MergeSorter},
        utils::VecExt,
    };
    use proptest::{collection::vec, prelude::*};
    use std::collections::BTreeMap;

    type AggregatedData = BTreeMap<(usize, usize), i64>;

    prop_compose! {
        /// Create a batch data tuple
        fn tuple()(
            key in 0..1000usize,
            value in 0..1000usize,
            diff in -1000..=1000isize,
        ) -> ((usize, usize), isize) {
            ((key, value), diff)
        }
    }

    prop_compose! {
        /// Generate a random batch of data
        fn batch()
            (length in 0..500usize)
            (batch in vec(tuple(), 0..=length))
        -> Vec<((usize, usize), isize)> {
            batch
        }
    }

    /// Generate a random, consolidated batch of data
    fn consolidated_batch() -> impl Strategy<Value = Vec<((usize, usize), isize)>> {
        batch().prop_map(|mut batch| {
            consolidate(&mut batch);
            batch
        })
    }

    prop_compose! {
        /// Generate multiple random batches of data
        fn batches()
            (length in 0..150usize)
            (batches in vec(batch(), 0..=length))
        -> Vec<Vec<((usize, usize), isize)>> {
            batches
        }
    }

    prop_compose! {
        /// Generate multiple random consolidated batches of data
        fn consolidated_batches()
            (length in 0..150usize)
            (batches in vec(consolidated_batch(), 0..=length))
        -> Vec<Vec<((usize, usize), isize)>> {
            batches
        }
    }

    prop_compose! {
        // Create an initialized merge sorter with some stashes and already queued data
        fn merge_sorter()
            (queue_len in 0..10usize)
            (
                stashes in 0..5usize,
                // Each batch within the merge sorter's queue must already be consolidated
                queue in vec(consolidated_batches(), 0..=queue_len),
            )
        -> MergeSorter<(usize, usize), isize> {
            MergeSorter {
                queue,
                stash: preallocated_stashes(stashes),
            }
        }
    }

    fn empty_merge_sorter() -> MergeSorter<(usize, usize), isize> {
        MergeSorter {
            queue: Vec::new(),
            stash: Vec::new(),
        }
    }

    fn expected_data(
        merger: &MergeSorter<(usize, usize), isize>,
        batch: &[((usize, usize), isize)],
    ) -> AggregatedData {
        for stash in &merger.stash {
            assert!(stash.is_empty());
        }

        // Collect all previously queued values
        let mut values = BTreeMap::new();
        for queued in &merger.queue {
            for batch in queued {
                for &(tuple, diff) in batch {
                    values
                        .entry(tuple)
                        .and_modify(|acc| *acc += diff as i64)
                        .or_insert(diff as i64);
                }
            }
        }

        // Collect all tuples within the batch
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

    fn batches_data(batches: &[Vec<((usize, usize), isize)>]) -> AggregatedData {
        let mut values = BTreeMap::new();
        for batch in batches {
            for &(tuple, diff) in batch {
                values
                    .entry(tuple)
                    .and_modify(|acc| *acc += diff as i64)
                    .or_insert(diff as i64);
            }
        }

        // Elements with a value of zero are removed in consolidation
        values.retain(|_, &mut diff| diff != 0);
        values
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]

        #[test]
        #[ignore = "Currently broken, merge_sorter() doesn't ensure that the queue is sorted"]
        fn push_multiple_batches(mut merger in merge_sorter(), mut batch in batch()) {
            let input = expected_data(&merger, &batch);

            // Push the new batch
            merger.push(&mut batch);

            let mut output = Vec::new();
            merger.finish_into(&mut output);

            // Ensure all output batches are sorted
            for batch in &output {
                prop_assert!(
                    batch.is_sorted_by(|(a, _), (b, _)| Some(a.cmp(b))),
                    "unsorted batch: {batch:?}",
                );
            }

            let merged = batches_data(&output);
            prop_assert_eq!(input, merged);
        }

        #[test]
        fn push_batches_into_empty(batches in batches()) {
            let input = batches_data(&batches);
            let mut merger = empty_merge_sorter();

            // Push all batches
            for mut batch in batches {
                merger.push(&mut batch);
            }

            let mut output = Vec::new();
            merger.finish_into(&mut output);

            // Ensure all output batches are sorted
            for batch in &output {
                prop_assert!(batch.is_sorted_by(|(a, _), (b, _)| Some(a.cmp(b))));
            }

            let merged = batches_data(&output);
            prop_assert_eq!(input, merged);
        }
    }
}
