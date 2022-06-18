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
    (0..14)
        .map(|x| {
            let mut batch = (0..264).map(|y| (x * y, (x ^ y) as isize)).collect();
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
fn merge_by() {
    let mut merger: MergeSorter<usize, isize> = MergeSorter::new();

    let left = vec![vec![(0, 1), (1, 6), (24, 5), (54, -23)]];
    let right = vec![vec![(0, 7), (1, 6), (24, -5), (25, 12), (89, 1)]];
    let merged = merger.merge_by(left, right);

    // TODO: Not entirely sure if this is an optimal result, we always leave one element trailing
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
