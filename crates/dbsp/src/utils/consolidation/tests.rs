#![cfg(test)]

mod proptests;

use std::fmt::Debug;

use crate::lean_vec;
use itertools::Itertools;

use crate::utils::{
    consolidation::{
        consolidate, consolidate_from, consolidate_paired_slices, consolidate_payload_from,
        consolidate_slice, dedup_payload_starting_at, quicksort::quicksort,
        utils::retain_starting_at,
    },
    Tup2,
};

#[test]
fn test_consolidate() {
    let test_cases = vec![
        (
            lean_vec![Tup2("a", -1), Tup2("b", -2), Tup2("a", 1)],
            lean_vec![Tup2("b", -2)],
        ),
        (
            lean_vec![Tup2("a", -1), Tup2("b", 0), Tup2("a", 1)],
            lean_vec![],
        ),
        (lean_vec![Tup2("a", 0)], lean_vec![]),
        (lean_vec![Tup2("a", 0), Tup2("b", 0)], lean_vec![]),
        (
            lean_vec![Tup2("a", 1), Tup2("b", 1)],
            lean_vec![Tup2("a", 1), Tup2("b", 1)],
        ),
    ];

    for (mut input, output) in test_cases {
        consolidate(&mut input);
        assert_eq!(input, output);
    }
}

#[test]
fn test_consolidate_from_start() {
    let test_cases = vec![
        (
            lean_vec![Tup2("a", -1), Tup2("b", -2), Tup2("a", 1)],
            lean_vec![Tup2("b", -2)],
        ),
        (
            lean_vec![Tup2("a", -1), Tup2("b", 0), Tup2("a", 1)],
            lean_vec![],
        ),
        (lean_vec![Tup2("a", 0)], lean_vec![]),
        (lean_vec![Tup2("a", 0), Tup2("b", 0)], lean_vec![]),
        (
            lean_vec![Tup2("a", 1), Tup2("b", 1)],
            lean_vec![Tup2("a", 1), Tup2("b", 1)],
        ),
    ];

    for (mut input, output) in test_cases {
        consolidate_from(&mut input, 0);
        assert_eq!(input, output);
    }
}

#[test]
fn test_consolidate_from() {
    let test_cases = vec![
        (
            lean_vec![Tup2("a", -1), Tup2("b", -2), Tup2("a", 1)],
            lean_vec![Tup2("a", -1), Tup2("a", 1), Tup2("b", -2)],
        ),
        (
            lean_vec![Tup2("a", -1), Tup2("b", 0), Tup2("a", 1)],
            lean_vec![Tup2("a", -1), Tup2("a", 1)],
        ),
        (lean_vec![Tup2("a", 0)], lean_vec![Tup2("a", 0)]),
        (
            lean_vec![Tup2("a", 0), Tup2("b", 0)],
            lean_vec![Tup2("a", 0)],
        ),
        (
            lean_vec![Tup2("a", 1), Tup2("b", 1)],
            lean_vec![Tup2("a", 1), Tup2("b", 1)],
        ),
    ];

    for (mut input, output) in test_cases {
        consolidate_from(&mut input, 1);
        assert_eq!(input, output);
    }
}

#[test]
fn test_consolidate_slice() {
    let test_cases = vec![
        (
            lean_vec![Tup2("a", -1), Tup2("b", -2), Tup2("a", 1)],
            lean_vec![Tup2("b", -2)],
        ),
        (
            lean_vec![Tup2("a", -1), Tup2("b", 0), Tup2("a", 1)],
            lean_vec![],
        ),
        (lean_vec![Tup2("a", 0)], lean_vec![]),
        (lean_vec![Tup2("a", 0), Tup2("b", 0)], lean_vec![]),
        (
            lean_vec![Tup2("a", 1), Tup2("b", 1)],
            lean_vec![Tup2("a", 1), Tup2("b", 1)],
        ),
    ];

    for (mut input, output) in test_cases {
        let length = consolidate_slice(&mut input[..]);
        assert_eq!(input[..length], output[..]);
    }
}

#[test]
fn test_consolidate_paired_slices() {
    let test_cases = vec![
        (
            (vec!["a", "b", "a"], vec![-1, -2, 1]),
            (vec!["b"], vec![-2]),
        ),
        ((vec!["a", "b", "a"], vec![-1, 0, 1]), (vec![], vec![])),
        ((vec!["a"], vec![0]), (vec![], vec![])),
        ((vec!["a", "b"], vec![0, 0]), (vec![], vec![])),
        ((vec!["a", "b"], vec![1, 1]), (vec!["a", "b"], vec![1, 1])),
    ];

    for ((mut keys, mut values), (output_keys, output_values)) in test_cases {
        let length = consolidate_paired_slices(&mut keys, &mut values);
        assert_eq!(keys[..length], output_keys);
        assert_eq!(values[..length], output_values);
    }
}

#[test]
fn consolidate_paired_slices_corpus() {
    let (mut keys, mut values): (Vec<_>, Vec<_>) = vec![
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((1107, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((1107, 0), 0),
        ((0, 0), 1),
        ((0, 0), 1),
        ((0, 0), -1),
        ((0, 0), -1),
        ((0, 0), 1),
    ]
    .into_iter()
    .unzip();
    let length = consolidate_paired_slices(&mut keys, &mut values);
    assert_eq!(&keys[..length], &[(0, 0)]);
    assert_eq!(&values[..length], &[1]);

    let (mut keys, mut values): (Vec<_>, Vec<_>) = vec![
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((1107, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((0, 0), 0),
        ((1107, 0), 0),
        ((0, 0), 1),
        ((0, 0), 1),
        ((0, 0), -1),
        ((0, 0), -1),
        ((0, 0), 1),
    ]
    .into_iter()
    .unzip();
    let length = consolidate_paired_slices(&mut keys, &mut values);
    assert_eq!(&keys[..length], &[(0, 0)]);
    assert_eq!(&values[..length], &[1]);
}

/// Checks that the given vectors are equivalent, even if their sorting
/// algorithms have different orderings for identical keys
#[track_caller]
fn assert_sorted_eq<T, U>(expected: &[(T, U)], keys: &[T], values: &[U])
where
    T: Clone + Ord + Debug,
    U: Clone + Ord + Debug,
{
    assert_eq!(keys.len(), values.len());
    assert_eq!(expected.len(), keys.len());

    let mut buffer = Vec::with_capacity(keys.len());
    buffer.extend(keys.iter().cloned().zip(values.iter().cloned()));

    for ((idx, (key, value)), (_, (next_key, next_value))) in
        buffer.iter().enumerate().tuple_windows()
    {
        if key > next_key {
            panic!("tuple at index {idx} isn't sorted: ({key:?}, {value:?}) > ({next_key:?}, {next_value:?})");
        }
    }

    let mut sorted_buffer: Vec<_> = buffer.iter().cloned().enumerate().collect();
    sorted_buffer.sort_by(|(_, lhs), (_, rhs)| lhs.cmp(rhs));

    let mut sorted_expected = expected.to_vec();
    sorted_expected.sort();

    for ((index, (key, value)), (expected_key, expected_value)) in
        sorted_buffer.into_iter().zip(sorted_expected)
    {
        if key != expected_key {
            panic!("key is incorrect at index {index}: {key:?} != {expected_key:?}\n left: `{buffer:?}`\nright: `{expected:?}`");
        }

        if value != expected_value {
            panic!("value is incorrect at index {index}: {value:?} != {expected_value:?}\n left: `{buffer:?}`\nright: `{expected:?}`");
        }
    }
}

#[test]
fn quicksort_corpus() {
    let corpus: &[&[(u32, u32)]] = &[
        &[
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 1),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (1, 0),
            (0, 0),
        ],
        &[
            (2355195229, 0),
            (2355195229, 0),
            (0, 0),
            (2806675676, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (10797240, 0),
            (2043475975, 0),
            (0, 0),
            (10797240, 0),
            (1697570063, 0),
            (630495336, 0),
            (10797240, 0),
            (797216936, 0),
            (797216936, 0),
            (797216936, 0),
            (797216936, 0),
            (997754872, 0),
            (997754872, 0),
            (997754872, 0),
            (997754872, 0),
            (997754872, 0),
            (997754872, 0),
            (1103036604, 0),
            (1103036604, 0),
            (1103036604, 0),
            (1103036604, 0),
            (1103036604, 0),
            (1103036604, 0),
            (1103036604, 0),
            (1103036604, 0),
            (1103036604, 0),
            (1103036604, 0),
            (1103036604, 0),
            (301201423, 0),
            (2060381626, 0),
            (301201423, 0),
            (10797240, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (0, 0),
            (2355195229, 1),
            (10797240, 0),
        ],
    ];

    for batch in corpus {
        let (mut keys, mut values): (Vec<_>, Vec<_>) = batch.iter().copied().unzip();
        quicksort(&mut keys, &mut values);

        let mut expected = batch.to_vec();
        expected.sort_unstable_by_key(|&(key, _)| key);

        assert_sorted_eq(&expected, &keys, &values);
    }
}

#[test]
fn offset_dedup() {
    let test_cases = vec![
        (vec![], 0, vec![]),
        (vec![1, 2, 3, 4], 0, vec![1, 2, 3, 4]),
        (vec![1, 2, 3, 4], 2, vec![1, 2, 3, 4]),
        (vec![1, 2, 3, 4, 4, 4, 4, 4, 4], 3, vec![1, 2, 3, 4]),
        (
            vec![1, 2, 3, 4, 4, 4, 4, 4, 4, 6, 5, 4, 4, 6, 6, 6, 6, 7, 2, 3],
            3,
            vec![1, 2, 3, 4, 6, 5, 4, 6, 7, 2, 3],
        ),
        (
            vec![1, 2, 3, 4, 4, 4, 4, 4, 4, 6, 5, 4, 4, 6, 6, 6, 6, 7, 2, 3],
            5,
            vec![1, 2, 3, 4, 4, 4, 6, 5, 4, 6, 7, 2, 3],
        ),
    ];

    for (mut input, starting_point, output) in test_cases {
        dedup_payload_starting_at(&mut input, (), starting_point, |a, (), b, ()| *a == *b);
        assert_eq!(input, output);
    }
}

#[test]
fn offset_retain() {
    let test_cases = vec![
        (vec![], 0, vec![]),
        (
            vec![(1, true), (2, true), (3, true), (4, true)],
            0,
            vec![(1, true), (2, true), (3, true), (4, true)],
        ),
        (
            vec![(1, false), (2, true), (3, true), (4, true)],
            2,
            vec![(1, false), (2, true), (3, true), (4, true)],
        ),
        (
            vec![
                (1, true),
                (2, false),
                (3, false),
                (4, true),
                (5, true),
                (6, false),
                (7, true),
                (8, false),
                (9, false),
            ],
            3,
            vec![
                (1, true),
                (2, false),
                (3, false),
                (4, true),
                (5, true),
                (7, true),
            ],
        ),
    ];

    for (mut input, starting_point, output) in test_cases {
        retain_starting_at(&mut input, starting_point, |(_, cond)| *cond);
        assert_eq!(input, output);
    }
}

#[test]
fn consolidate_payload_corpus() {
    let (mut keys, mut diffs): (Vec<_>, Vec<_>) = vec![
        (0, 0),
        (0, 0),
        (2, 0),
        (3, 0),
        (8, 0),
        (3, 0),
        (2, 0),
        (3, 0),
        (3, 0),
        (3, 0),
        (3, 0),
        (0, 0),
        (2, 0),
        (3, 0),
        (3, 0),
        (8, 0),
        (5, 0),
        (0, 0),
        (5, 0),
        (2, 0),
        (0, 0),
        (2, 0),
        (5, 0),
        (5, 0),
        (5, 0),
        (0, 0),
        (0, 0),
        (5, 0),
        (5, 0),
        (5, 0),
        (0, 0),
        (5, 0),
        (5, 0),
        (5, 0),
        (5, 0),
        (0, 0),
        (0, 0),
        (5, 1),
        (0, 0),
        (0, 0),
        (0, 0),
        (0, 0),
        (0, 0),
        (0, 0),
        (0, 0),
        (5, 0),
        (5, 0),
        (0, 0),
        (0, 0),
        (0, 0),
        (5, 0),
        (5, 0),
        (0, 0),
        (5, 0),
        (0, 0),
        (5, 0),
        (5, 0),
        (0, 0),
        (0, 0),
        (5, 0),
        (5, 0),
        (0, 0),
        (5, 0),
        (0, 0),
    ]
    .into_iter()
    .unzip();

    consolidate_payload_from(&mut keys, &mut diffs, 0);

    assert_eq!(&keys, &[5]);
    assert_eq!(&diffs, &[1]);
}
