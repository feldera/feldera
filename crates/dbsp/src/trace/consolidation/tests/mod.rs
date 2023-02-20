#![cfg(test)]

mod proptests;

use crate::trace::consolidation::{
    consolidate, consolidate_from, consolidate_paired_slices, consolidate_slice,
    dedup_payload_starting_at, quicksort::quicksort, retain_starting_at,
};

#[test]
fn test_consolidate() {
    let test_cases = vec![
        (vec![("a", -1), ("b", -2), ("a", 1)], vec![("b", -2)]),
        (vec![("a", -1), ("b", 0), ("a", 1)], vec![]),
        (vec![("a", 0)], vec![]),
        (vec![("a", 0), ("b", 0)], vec![]),
        (vec![("a", 1), ("b", 1)], vec![("a", 1), ("b", 1)]),
    ];

    for (mut input, output) in test_cases {
        consolidate(&mut input);
        assert_eq!(input, output);
    }
}

#[test]
fn test_consolidate_from_start() {
    let test_cases = vec![
        (vec![("a", -1), ("b", -2), ("a", 1)], vec![("b", -2)]),
        (vec![("a", -1), ("b", 0), ("a", 1)], vec![]),
        (vec![("a", 0)], vec![]),
        (vec![("a", 0), ("b", 0)], vec![]),
        (vec![("a", 1), ("b", 1)], vec![("a", 1), ("b", 1)]),
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
            vec![("a", -1), ("b", -2), ("a", 1)],
            vec![("a", -1), ("a", 1), ("b", -2)],
        ),
        (
            vec![("a", -1), ("b", 0), ("a", 1)],
            vec![("a", -1), ("a", 1)],
        ),
        (vec![("a", 0)], vec![("a", 0)]),
        (vec![("a", 0), ("b", 0)], vec![("a", 0)]),
        (vec![("a", 1), ("b", 1)], vec![("a", 1), ("b", 1)]),
    ];

    for (mut input, output) in test_cases {
        consolidate_from(&mut input, 1);
        assert_eq!(input, output);
    }
}

#[test]
fn test_consolidate_slice() {
    let test_cases = vec![
        (vec![("a", -1), ("b", -2), ("a", 1)], vec![("b", -2)]),
        (vec![("a", -1), ("b", 0), ("a", 1)], vec![]),
        (vec![("a", 0)], vec![]),
        (vec![("a", 0), ("b", 0)], vec![]),
        (vec![("a", 1), ("b", 1)], vec![("a", 1), ("b", 1)]),
    ];

    for (mut input, output) in test_cases {
        let length = consolidate_slice(&mut input);
        assert_eq!(input[..length], output);
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

#[test]
fn quicksort_corpus() {
    let corpus: &[&[(u32, u32)]] = &[&[
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
    ]];

    for batch in corpus {
        let (mut keys, mut values): (Vec<_>, Vec<_>) = batch.iter().copied().unzip();
        quicksort(&mut keys, &mut values);

        let mut expected = batch.to_vec();
        expected.sort_unstable_by_key(|&(key, _)| key);
        let (expected_keys, expected_values): (Vec<_>, Vec<_>) = expected.into_iter().unzip();

        assert_eq!(keys, expected_keys);
        assert_eq!(values, expected_values);
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
