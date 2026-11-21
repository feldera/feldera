//! Sort/merge costs for the bounded fixed and mixed key prototypes.
//! Archive sources and raw output before running; see ../README.md.

use feldera_native_probe::{Batch, KeyColumn, KeyField, KeyLayout, NormalizedKeys};
use std::{cmp::Ordering, hint::black_box, time::Instant};

#[derive(Clone, Copy, Debug)]
enum Distribution {
    Uniform,
    CommonPrefix,
    Duplicates,
}

const DISTRIBUTIONS: [Distribution; 3] = [
    Distribution::Uniform,
    Distribution::CommonPrefix,
    Distribution::Duplicates,
];

fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn input(rows: usize, columns: usize, distribution: Distribution, seed: u64) -> Batch {
    let mut state = seed;
    let rows: Vec<_> = (0..rows)
        .map(|_| {
            let duplicate = next(&mut state) % 64;
            (0..columns)
                .map(|column| {
                    let bits = next(&mut state);
                    match distribution {
                        Distribution::CommonPrefix if column + 1 < columns => Some(0),
                        Distribution::Duplicates => (duplicate != 0)
                            .then_some((duplicate as i64 - 32) * (column as i64 + 1)),
                        _ => (!bits.is_multiple_of(8)).then_some(bits as i64),
                    }
                })
                .collect()
        })
        .collect();
    Batch::from_rows(columns, &rows).expect("bounded benchmark input")
}

fn compare_value<T: Ord>(left: Option<T>, right: Option<T>, field: KeyField) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) if field.descending => right.cmp(&left),
        (Some(left), Some(right)) => left.cmp(&right),
        (left, right) if field.nulls_first => left.cmp(&right),
        (left, right) => right.cmp(&left),
    }
}

fn compare(batch: &Batch, fields: &[KeyField], left: usize, right: usize) -> Ordering {
    for field in fields {
        let order = compare_value(
            batch.get(left, field.column).expect("valid coordinate"),
            batch.get(right, field.column).expect("valid coordinate"),
            *field,
        );
        if order != Ordering::Equal {
            return order;
        }
    }
    Ordering::Equal
}

/// Merge two already sorted runs. Equal keys retain left-run precedence.
fn merge(
    left: &[usize],
    right: &[usize],
    compare: impl Fn(usize, usize) -> Ordering,
) -> Vec<usize> {
    let mut output = Vec::with_capacity(left.len() + right.len());
    let (mut l, mut r) = (0, 0);
    while l < left.len() && r < right.len() {
        if compare(left[l], right[r]) != Ordering::Greater {
            output.push(left[l]);
            l += 1;
        } else {
            output.push(right[r]);
            r += 1;
        }
    }
    output.extend_from_slice(&left[l..]);
    output.extend_from_slice(&right[r..]);
    output
}

fn measure<T>(operation: impl FnOnce() -> T) -> (T, u128) {
    let start = Instant::now();
    let value = black_box(operation());
    (value, start.elapsed().as_nanos())
}

struct Trial {
    sorted: Vec<usize>,
    merged: Vec<usize>,
    encode_ns: u128,
    sort_ns: u128,
    merge_ns: u128,
    key_bytes: usize,
}

fn run_operations(
    rows: usize,
    left: &[usize],
    right: &[usize],
    compare: impl Fn(usize, usize) -> Ordering,
) -> Trial {
    // Both paths start with the same index array; allocation is outside sort
    // timing. Stable-sort scratch allocation and merge allocation are included.
    let mut sorted: Vec<_> = (0..rows).collect();
    let (_, sort_ns) = measure(|| sorted.sort_by(|&l, &r| compare(l, r)));
    let (merged, merge_ns) = measure(|| merge(left, right, compare));
    Trial {
        sorted,
        merged,
        encode_ns: 0,
        sort_ns,
        merge_ns,
        key_bytes: 0,
    }
}

fn normalized(
    rows: usize,
    left: &[usize],
    right: &[usize],
    encode: impl FnOnce() -> NormalizedKeys,
) -> Trial {
    let (keys, encode_ns) = measure(encode);
    let mut trial = run_operations(rows, left, right, |l, r| {
        keys.key(l)
            .expect("valid row")
            .cmp(keys.key(r).expect("valid row"))
    });
    trial.encode_ns = encode_ns;
    trial.key_bytes = keys.byte_len();
    trial
}

#[derive(Clone, Copy)]
struct Workload {
    rows: usize,
    columns: usize,
    distribution: Distribution,
    key_type: &'static str,
}

fn run_trial(
    workload: Workload,
    iteration: usize,
    seed: u64,
    compare: impl Fn(usize, usize) -> Ordering,
    encode: impl FnOnce() -> NormalizedKeys,
) {
    let Workload {
        rows,
        columns,
        distribution,
        key_type,
    } = workload;
    // Independent sort supplies the reference and ordered half-input runs.
    let mut expected: Vec<_> = (0..rows).collect();
    expected.sort_by(|&l, &r| compare(l, r));
    let (left, right): (Vec<_>, Vec<_>) = expected.iter().copied().partition(|r| *r < rows / 2);
    let direct_first = iteration.is_multiple_of(2);
    let direct = || run_operations(rows, &left, &right, &compare);
    let encoded = || normalized(rows, &left, &right, encode);
    let (direct, normalized) = if direct_first {
        (direct(), encoded())
    } else {
        let result = encoded();
        (direct(), result)
    };
    for trial in [&direct, &normalized] {
        assert_eq!(trial.sorted, expected, "sort mismatch");
        assert_eq!(trial.merged, expected, "merge mismatch");
    }
    println!(
        concat!(
            "{{\"rows\":{},\"columns\":{},\"distribution\":\"{:?}\",\"key_type\":\"{}\",",
            "\"seed\":{},\"iteration\":{},\"warmup\":{},\"direct_first\":{},",
            "\"direct_sort_ns\":{},\"direct_merge_ns\":{},\"encode_ns\":{},",
            "\"normalized_sort_ns\":{},\"normalized_merge_ns\":{},\"key_bytes\":{}}}"
        ),
        rows,
        columns,
        distribution,
        key_type,
        seed,
        iteration,
        iteration < 2,
        direct_first,
        direct.sort_ns,
        direct.merge_ns,
        normalized.encode_ns,
        normalized.sort_ns,
        normalized.merge_ns,
        normalized.key_bytes
    );
}

fn mixed_trial(
    workload: Workload,
    fields: &[KeyField],
    layout: &KeyLayout,
    iteration: usize,
    seed: u64,
) {
    let mut state = seed;
    let mut strings = Vec::with_capacity(workload.rows);
    let mut decimals = Vec::with_capacity(workload.rows);
    let mut integers = Vec::with_capacity(workload.rows);
    for _ in 0..workload.rows {
        let a = next(&mut state);
        let b = next(&mut state);
        let c = next(&mut state);
        let group = a % 64;
        let text = match workload.distribution {
            Distribution::Uniform => format!("{:016x}", a),
            Distribution::CommonPrefix => format!("common-prefix-🦀-with\0separator-{group:02}"),
            Distribution::Duplicates => format!("{group:02}"),
        };
        strings.push((!a.is_multiple_of(8)).then_some(text));
        if matches!(workload.distribution, Distribution::Duplicates) {
            decimals.push((!group.is_multiple_of(5)).then_some(i128::from(group)));
            integers.push((!group.is_multiple_of(7)).then_some(group as i64));
        } else {
            decimals.push((!b.is_multiple_of(8)).then_some(i128::from(b as i64 % 1_000_000)));
            integers.push((!c.is_multiple_of(8)).then_some(c as i64));
        }
    }
    let text: Vec<_> = strings.iter().map(|value| value.as_deref()).collect();
    let columns = [
        KeyColumn::Text(&text),
        KeyColumn::WideInt(&decimals),
        KeyColumn::BigInt(&integers),
    ];
    run_trial(
        workload,
        iteration,
        seed,
        |l, r| {
            compare_value(text[l], text[r], fields[0])
                .then_with(|| compare_value(decimals[l], decimals[r], fields[1]))
                .then_with(|| compare_value(integers[l], integers[r], fields[2]))
        },
        || {
            layout
                .encode_columns(workload.rows, black_box(&columns))
                .expect("bounded mixed layout")
        },
    );
}

fn key_bits(state: &mut u64, distribution: Distribution, column: usize) -> u64 {
    let bits = next(state);
    match distribution {
        Distribution::Uniform => bits,
        Distribution::CommonPrefix if column == 0 => (0xffff_ffff_ffff_u64 << 16) | (bits & 0xffff),
        Distribution::CommonPrefix => bits,
        Distribution::Duplicates => bits % 64,
    }
}

fn corpus_layout(columns: usize) -> KeyLayout {
    let fields = (0..columns)
        .map(|column| KeyField {
            column,
            descending: false,
            nulls_first: true,
        })
        .collect();
    KeyLayout::new(columns, fields).expect("bounded key layout")
}

fn corpus_integer_trial<const N: usize>(workload: Workload, iteration: usize, seed: u64) {
    let mut state = seed;
    let values: Vec<[i32; N]> = (0..workload.rows)
        .map(|_| {
            std::array::from_fn(|column| key_bits(&mut state, workload.distribution, column) as i32)
        })
        .collect();
    let rows: Vec<_> = values
        .iter()
        .map(|value| {
            value
                .iter()
                .map(|&number| Some(i64::from(number)))
                .collect()
        })
        .collect();
    let batch = Batch::from_rows(N, &rows).expect("bounded integer key input");
    let layout = corpus_layout(N);
    run_trial(
        workload,
        iteration,
        seed,
        |l, r| values[l].cmp(&values[r]),
        || {
            layout
                .encode(black_box(&batch))
                .expect("bounded integer keys")
        },
    );
}

fn corpus_wide_trial<T: Copy + Ord>(
    workload: Workload,
    iteration: usize,
    seed: u64,
    values: Vec<T>,
    slot_value: impl Fn(T) -> Option<i128>,
) {
    let rows: Vec<_> = values
        .iter()
        .map(|&value| vec![slot_value(value)])
        .collect();
    let layout = corpus_layout(1);
    run_trial(
        workload,
        iteration,
        seed,
        |l, r| values[l].cmp(&values[r]),
        || {
            layout
                .encode_wide(black_box(&rows))
                .expect("bounded wide keys")
        },
    );
}

fn corpus_trials(samples: usize) {
    for rows in [4096, 65536] {
        for (key_type, columns) in [
            ("unit", 0),
            ("integer", 1),
            ("integer_pair", 2),
            ("nullable_decimal", 1),
            ("unsigned_time", 1),
        ] {
            for distribution in DISTRIBUTIONS {
                if key_type == "unit" && !matches!(distribution, Distribution::Uniform) {
                    continue;
                }
                let workload = Workload {
                    rows,
                    columns,
                    distribution,
                    key_type,
                };
                for iteration in 0..samples + 2 {
                    let seed = 0x9e3779b97f4a7c15_u64.wrapping_add(iteration as u64);
                    let mut state = seed;
                    match key_type {
                        "unit" => corpus_integer_trial::<0>(workload, iteration, seed),
                        "integer" => corpus_integer_trial::<1>(workload, iteration, seed),
                        "integer_pair" => corpus_integer_trial::<2>(workload, iteration, seed),
                        "nullable_decimal" => {
                            let values = (0..rows)
                                .map(|_| {
                                    let bits = key_bits(&mut state, distribution, 0);
                                    (!bits.is_multiple_of(8))
                                        .then_some(i128::from(bits as i64 % 1_000_000))
                                })
                                .collect();
                            corpus_wide_trial(workload, iteration, seed, values, |value| value);
                        }
                        "unsigned_time" => {
                            let values = (0..rows)
                                .map(|_| key_bits(&mut state, distribution, 0))
                                .collect();
                            corpus_wide_trial(workload, iteration, seed, values, |value| {
                                Some(i128::from(value))
                            });
                        }
                        _ => unreachable!("fixed key workload list"),
                    }
                }
            }
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let samples: usize = args.next().map_or(31, |s| s.parse().expect("sample count"));
    assert!((5..=1000).contains(&samples), "expected 5..=1000 samples");
    let profile = args.next();
    assert!(args.next().is_none(), "unexpected key benchmark arguments");
    let mixed = match profile.as_deref() {
        None => false,
        Some("--mixed") => true,
        Some("--corpus") => {
            corpus_trials(samples);
            return;
        }
        _ => panic!("usage: key_benchmark [SAMPLES [--mixed|--corpus]]"),
    };
    for rows in [4096, 65536] {
        for &columns in if mixed { &[3][..] } else { &[1, 4, 12][..] } {
            let fields: Vec<_> = (0..columns)
                .map(|column| KeyField {
                    column,
                    descending: column % 2 == 1,
                    nulls_first: column % 3 == 0,
                })
                .collect();
            let layout = KeyLayout::new(columns, fields.clone()).expect("valid layout");
            for distribution in DISTRIBUTIONS {
                let workload = Workload {
                    rows,
                    columns,
                    distribution,
                    key_type: if mixed { "mixed" } else { "bigint" },
                };
                for iteration in 0..samples + 2 {
                    let seed = 0x9e3779b97f4a7c15_u64.wrapping_add(iteration as u64);
                    if mixed {
                        mixed_trial(workload, &fields, &layout, iteration, seed);
                    } else {
                        let batch = input(rows, columns, distribution, seed);
                        run_trial(
                            workload,
                            iteration,
                            seed,
                            |l, r| compare(&batch, &fields, l, r),
                            || layout.encode(black_box(&batch)).expect("valid key layout"),
                        );
                    }
                }
            }
        }
    }
}
