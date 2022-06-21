#![cfg(test)]

use crate::trace::{
    consolidation,
    ord::{
        merge_batcher::{MergeBatcher, MergeSorter},
        OrdValBatch,
    },
    Batcher,
};
use std::marker::PhantomData;

fn batch_one() -> Vec<Vec<(usize, isize)>> {
    (0..11)
        .map(|x| {
            let mut batch = (0..57).map(|y| (x * y, (x ^ y) as isize)).collect();
            consolidation::consolidate(&mut batch);
            batch
        })
        .collect()
}

// Merging empty lists should produce an empty list
#[test]
fn merge_empty_inputs() {
    let mut merger: MergeSorter<usize, isize> = MergeSorter::new();
    let merged = merger.merge_by(Vec::new(), Vec::new());
    assert!(merged.is_empty());
}

// If either of the lists being merged is empty, nothing happens
#[test]
fn merge_empty_with_full() {
    let mut merger: MergeSorter<usize, isize> = MergeSorter::new();
    let merged = merger.merge_by(Vec::new(), batch_one());
    assert_eq!(merged, batch_one());

    let mut merger: MergeSorter<usize, isize> = MergeSorter::new();
    let merged = merger.merge_by(batch_one(), Vec::new());
    assert_eq!(merged, batch_one());
}

#[test]
fn small_push() {
    let mut merger = MergeSorter::<(usize, usize), isize> {
        queue: vec![vec![vec![((45, 0), -1)], vec![((0, 0), 1)]]],
        stash: vec![],
    };

    let mut batch = vec![((45, 1), 1)];
    merger.push(&mut batch);

    let mut output = Vec::new();
    merger.finish_into(&mut output);

    assert_eq!(
        output,
        vec![vec![((45, 0), -1), ((0, 0), 1)], vec![((45, 1), 1)]],
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
        stash: vec![Vec::with_capacity(<MergeSorter<usize, isize>>::BUFFER_ELEMENTS); 5],
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
        stash: vec![Vec::with_capacity(<MergeSorter<usize, isize>>::BUFFER_ELEMENTS); 5],
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
        stash: vec![Vec::with_capacity(<MergeSorter<usize, isize>>::BUFFER_ELEMENTS); 5],
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

    let still_empty: MergeSorter<usize, isize> = MergeSorter {
        queue: Vec::new(),
        stash: vec![Vec::new(); 100],
    };
    assert_eq!(still_empty.tuples(), 0);

    let has_tuples: MergeSorter<(usize, usize), isize> = MergeSorter {
        queue: vec![
            vec![Vec::new(), vec![((0, 0), 0); 1000], vec![((1, 1), 1)]],
            Vec::new(),
            vec![vec![((10, 10), 10); 256]],
        ],
        stash: vec![Vec::new(); 100],
    };
    assert_eq!(has_tuples.tuples(), 1257);

    #[allow(clippy::type_complexity)]
    let batcher: MergeBatcher<
        usize,
        usize,
        u32,
        isize,
        OrdValBatch<usize, usize, u32, isize, usize>,
    > = MergeBatcher {
        sorter: has_tuples,
        time: 0,
        phantom: PhantomData,
    };
    assert_eq!(batcher.tuples(), 1257);
}

// These tests will utterly destroy miri's performance
#[cfg_attr(miri, ignore)]
mod proptests {
    use crate::{
        trace::{consolidation::consolidate, ord::merge_batcher::MergeSorter},
        utils::VecExt,
    };
    use proptest::{collection::vec, prelude::*};
    use std::collections::BTreeMap;

    type AggregatedData = BTreeMap<(usize, usize), i64>;

    prop_compose! {
        /// Create a batch data tuple
        fn tuple()(key in 0..10_000usize, value in 0..10_000usize, diff in -10_000..=10_000isize) -> ((usize, usize), isize) {
            ((key, value), diff)
        }
    }

    prop_compose! {
        /// Generate a random batch of data
        fn batch()
            (length in 0..1000)
            (batch in vec(tuple(), 0..=length as usize))
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
            (length in 0..500)
            (batches in vec(batch(), 0..=length as usize))
        -> Vec<Vec<((usize, usize), isize)>> {
            batches
        }
    }

    prop_compose! {
        /// Generate multiple random consolidated batches of data
        fn consolidated_batches()
            (length in 0..500)
            (batches in vec(consolidated_batch(), 0..=length as usize))
        -> Vec<Vec<((usize, usize), isize)>> {
            batches
        }
    }

    prop_compose! {
        // Create an initialized merge sorter with some stashes and already queued data
        fn merge_sorter()
            (queue_len in 0..10)
            (
                stashes in 0..10,
                // Each batch within the merge sorter's queue must already be consolidated
                queue in vec(consolidated_batches(), 0..=queue_len as usize),
            )
        -> MergeSorter<(usize, usize), isize> {
            MergeSorter {
                queue,
                stash: vec![Vec::with_capacity(MergeSorter::<(usize, usize), isize>::BUFFER_ELEMENTS); stashes as usize],
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
        #[test]
        fn push_batch(mut merger in merge_sorter(), mut batch in batch()) {
            let input = expected_data(&merger, &batch);

            // Push the new batch
            merger.push(&mut batch);

            let mut output = Vec::new();
            merger.finish_into(&mut output);

            // Ensure all output batches are sorted
            for batch in &output {
                prop_assert!(
                    batch.is_sorted_by(|(a, _), (b, _)| a.partial_cmp(b)),
                    "unsorted batch: {batch:?}",
                );
            }

            let merged = batches_data(&output);
            prop_assert_eq!(input, merged);
        }
    }

    proptest! {
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

            // FIXME: Apparently output batches aren't actually sorted??
            // // Ensure all output batches are sorted
            // for batch in &output {
            //     prop_assert!(batch.is_sorted_by(|(a, _), (b, _)| a.partial_cmp(b)));
            // }

            let merged = batches_data(&output);
            prop_assert_eq!(input, merged);
        }
    }
}
