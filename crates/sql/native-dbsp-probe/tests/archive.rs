//! N2 subset: bytes written by native code vs the actual generated Tup layouts.
//! These types occur in the frozen ten-program inventory; no circuit runs here.
#![cfg(target_os = "linux")]

use dbsp::{
    dynamic::{HashRepr, OrdRepr},
    utils::{Tup1, Tup2},
};
use feldera_native_probe::{
    ArchiveField, ArchiveInteger, ArchiveLayout, Batch, CompiledArchive, Error,
};
use rkyv::{
    AlignedVec, Archive, Archived, Deserialize, Infallible, Serialize,
    ser::serializers::AllocSerializer,
};
use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
    mem::{offset_of, size_of},
};

fn required(integer: ArchiveInteger, payload_offset: usize) -> ArchiveField {
    ArchiveField {
        integer,
        payload_offset,
        null_tag_offset: None,
    }
}

fn optional<T: Default>(integer: ArchiveInteger, field_offset: usize) -> ArchiveField {
    let example = rkyv::option::ArchivedOption::Some(T::default());
    let rkyv::option::ArchivedOption::Some(value) = &example else {
        unreachable!()
    };
    let payload_offset = std::ptr::from_ref(value) as usize - std::ptr::from_ref(&example) as usize;
    ArchiveField {
        integer,
        payload_offset: field_offset + payload_offset,
        null_tag_offset: Some(field_offset),
    }
}

fn check_archives<T>(layout: ArchiveLayout, values: Vec<T>, rows: Vec<Vec<Option<i64>>>)
where
    T: Archive + Serialize<AllocSerializer<256>> + Ord + Hash + Debug,
    Archived<T>: Deserialize<T, Infallible> + Ord + OrdRepr<T> + HashRepr,
{
    let encoder = CompiledArchive::compile(layout).unwrap();
    let empty = Batch::from_rows(rows[0].len(), &[]).unwrap();
    assert_eq!(encoder.encode(&empty).unwrap().rows().len(), 0);
    let native = encoder
        .encode(&Batch::from_rows(rows[0].len(), &rows).unwrap())
        .unwrap();
    drop(encoder);
    // Every archive byte, including option tags and padding, must match before
    // we ever interpret it as a typed archive. Decode after library destruction.
    assert_eq!(native.rows().len(), values.len());
    let archives: Vec<AlignedVec> = native
        .rows()
        .zip(&values)
        .map(|(bytes, value)| {
            let reference = rkyv::to_bytes::<_, 256>(value).unwrap();
            assert_eq!(bytes, reference.as_slice(), "archive of {value:?}");
            let mut aligned = AlignedVec::new();
            aligned.extend_from_slice(bytes);
            aligned
        })
        .collect();
    assert!(<Archived<T> as HashRepr>::FAITHFUL);
    for (bytes, value) in archives.iter().zip(&values) {
        // SAFETY: Exact equality with rkyv's serialization of this T was checked
        // above. AlignedVec supplies rkyv alignment and retains the complete,
        // initialized archive. These integer-only tuples have no external data.
        let archived = unsafe { rkyv::archived_root::<T>(bytes) };
        let decoded: T = archived.deserialize(&mut Infallible).unwrap();
        assert_eq!(&decoded, value);
        // Match crates/dbsp/src/hash.rs: Xxh3Default with its default seed.
        let mut live_hash = xxhash_rust::xxh3::Xxh3Default::new();
        value.hash(&mut live_hash);
        let mut archive_hash = xxhash_rust::xxh3::Xxh3Default::new();
        archived.hash_repr(&mut archive_hash);
        assert_eq!(live_hash.finish(), archive_hash.finish());
        for (other_bytes, other) in archives.iter().zip(&values).take(25) {
            // SAFETY: Same equality, alignment, initialization and lifetime
            // proof as above, independently established for each archive row.
            let other_archived = unsafe { rkyv::archived_root::<T>(other_bytes) };
            assert_eq!(archived.cmp(other_archived), value.cmp(other));
            assert_eq!(archived.ord_cmp(other), value.cmp(other));
        }
    }
}

fn integers() -> Vec<i64> {
    let mut values = vec![
        i64::MIN,
        i32::MIN as i64,
        -1,
        0,
        0,
        1,
        i32::MAX as i64,
        i64::MAX,
    ];
    let mut state = 0x39540d5a_i64;
    for _ in 0..256 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        values.push(state);
    }
    values
}

#[test]
fn integer_join_keys_match_generated_tuple_archives() {
    type Key = Tup1<i32>;
    let values: Vec<_> = integers().into_iter().map(|value| value as i32).collect();
    check_archives(
        ArchiveLayout::new(
            size_of::<Archived<Key>>(),
            vec![required(ArchiveInteger::I32, offset_of!(Archived<Key>, 0))],
        )
        .unwrap(),
        values.iter().map(|value| Tup1(*value)).collect(),
        values
            .iter()
            .map(|value| vec![Some(i64::from(*value))])
            .collect(),
    );
}

#[test]
fn nullable_integer_tuple_matches_generated_option_archive() {
    type Key = Tup1<Option<i32>>;
    let mut values: Vec<_> = integers()
        .into_iter()
        .map(|value| Some(value as i32))
        .collect();
    values.insert(0, None);
    values.push(None);
    check_archives(
        ArchiveLayout::new(
            size_of::<Archived<Key>>(),
            vec![optional::<i32>(
                ArchiveInteger::I32,
                offset_of!(Archived<Key>, 0),
            )],
        )
        .unwrap(),
        values.iter().map(|value| Tup1(*value)).collect(),
        values
            .iter()
            .map(|value| vec![value.map(i64::from)])
            .collect(),
    );
}

#[test]
fn bigint_tuple_matches_generated_archive() {
    type Row = Tup1<i64>;
    let values = integers();
    check_archives(
        ArchiveLayout::new(
            size_of::<Archived<Row>>(),
            vec![required(ArchiveInteger::I64, offset_of!(Archived<Row>, 0))],
        )
        .unwrap(),
        values.iter().map(|value| Tup1(*value)).collect(),
        values.iter().map(|value| vec![Some(*value)]).collect(),
    );
}

#[test]
fn empty_aggregate_output_matches_generated_archive() {
    type Row = Tup2<i64, i64>;
    let values = integers();
    let rows: Vec<_> = values
        .iter()
        .zip(values.iter().rev())
        .map(|(a, b)| vec![Some(*a), Some(*b)])
        .collect();
    check_archives(
        ArchiveLayout::new(
            size_of::<Archived<Row>>(),
            vec![
                required(ArchiveInteger::I64, offset_of!(Archived<Row>, 0)),
                required(ArchiveInteger::I64, offset_of!(Archived<Row>, 1)),
            ],
        )
        .unwrap(),
        values
            .iter()
            .zip(values.iter().rev())
            .map(|(a, b)| Tup2(*a, *b))
            .collect(),
        rows,
    );
}

#[test]
fn decimal_window_integer_output_matches_generated_archive() {
    type Row = Tup2<i32, i64>;
    let values = integers();
    let rows: Vec<_> = values
        .iter()
        .map(|value| vec![Some(i64::from(*value as i32)), Some(*value)])
        .collect();
    check_archives(
        ArchiveLayout::new(
            size_of::<Archived<Row>>(),
            vec![
                required(ArchiveInteger::I32, offset_of!(Archived<Row>, 0)),
                required(ArchiveInteger::I64, offset_of!(Archived<Row>, 1)),
            ],
        )
        .unwrap(),
        values
            .iter()
            .map(|value| Tup2(*value as i32, *value))
            .collect(),
        rows,
    );
}

#[test]
fn numeric_test_integer_join_key_matches_generated_archive() {
    type Key = Tup2<i32, i32>;
    let values = integers();
    let pairs: Vec<_> = values
        .iter()
        .zip(values.iter().rev())
        .map(|(a, b)| (*a as i32, *b as i32))
        .collect();
    check_archives(
        ArchiveLayout::new(
            size_of::<Archived<Key>>(),
            vec![
                required(ArchiveInteger::I32, offset_of!(Archived<Key>, 0)),
                required(ArchiveInteger::I32, offset_of!(Archived<Key>, 1)),
            ],
        )
        .unwrap(),
        pairs.iter().map(|(a, b)| Tup2(*a, *b)).collect(),
        pairs
            .iter()
            .map(|(a, b)| vec![Some(i64::from(*a)), Some(i64::from(*b))])
            .collect(),
    );
}

#[test]
fn invalid_layouts_and_values_fail_before_native_execution() {
    let i64_field = required(ArchiveInteger::I64, 0);
    assert!(ArchiveLayout::new(0, vec![i64_field]).is_err());
    assert!(ArchiveLayout::new(257, vec![i64_field]).is_err());
    assert!(ArchiveLayout::new(8, vec![]).is_err());
    assert!(ArchiveLayout::new(72, vec![i64_field; 9]).is_err());
    assert!(ArchiveLayout::new(8, vec![required(ArchiveInteger::I64, usize::MAX)]).is_err());
    assert!(ArchiveLayout::new(8, vec![i64_field, i64_field]).is_err());
    assert!(
        ArchiveLayout::new(
            8,
            vec![ArchiveField {
                null_tag_offset: Some(7),
                ..i64_field
            }]
        )
        .is_err()
    );
    assert!(
        ArchiveLayout::new(
            8,
            vec![ArchiveField {
                null_tag_offset: Some(8),
                ..i64_field
            }]
        )
        .is_err()
    );
    let encoder = CompiledArchive::compile(
        ArchiveLayout::new(4, vec![required(ArchiveInteger::I32, 0)]).unwrap(),
    )
    .unwrap();
    for row in [
        vec![None],
        vec![Some(i32::MAX as i64 + 1)],
        vec![Some(i32::MIN as i64 - 1)],
        vec![Some(1), Some(2)],
    ] {
        assert!(matches!(
            encoder.encode(&Batch::from_rows(row.len(), &[row]).unwrap()),
            Err(Error::InvalidBatch(_))
        ));
    }
}

// Adversarial extra shape, not an additional frozen-corpus coverage claim.
#[test]
fn mixed_nullable_fields_keep_distinct_tags_payloads_and_padding() {
    type Row = Tup2<Option<i32>, Option<i64>>;
    let values = integers();
    let mut pairs = Vec::new();
    for (index, value) in values.iter().enumerate() {
        for mask in 0..4 {
            pairs.push((
                (mask & 1 == 0).then_some(*value as i32),
                (mask & 2 == 0).then_some(values[values.len() - index - 1]),
            ));
        }
    }
    check_archives(
        ArchiveLayout::new(
            size_of::<Archived<Row>>(),
            vec![
                optional::<i32>(ArchiveInteger::I32, offset_of!(Archived<Row>, 0)),
                optional::<i64>(ArchiveInteger::I64, offset_of!(Archived<Row>, 1)),
            ],
        )
        .unwrap(),
        pairs.iter().map(|(a, b)| Tup2(*a, *b)).collect(),
        pairs
            .iter()
            .map(|(a, b)| vec![a.map(i64::from), *b])
            .collect(),
    );
}
