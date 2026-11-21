//! Native wide-slot writes of actual Fixed<P,S> archive shapes in the frozen
//! ten-program inventory. This is a row-type contract test, not SQL circuit C7.
#![cfg(target_os = "linux")]
use dbsp::{
    dynamic::{HashRepr, OrdRepr},
    utils::{Tup0, Tup1, Tup2, Tup3, Tup4},
};
use feldera_fxp::Fixed;
use feldera_native_probe::{ArchiveField, ArchiveInteger, ArchiveLayout, Batch, CompiledArchive};
use rkyv::{
    Archive, Archived, Deserialize, Infallible, Serialize, ser::serializers::AllocSerializer,
};
use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
    mem::{offset_of, size_of},
};
use xxhash_rust::xxh3::Xxh3Default;

fn verify_rows<T>(
    fields: Vec<ArchiveField>,
    values: &[T],
    slots: impl FnMut(&T) -> Vec<Option<i128>>,
) where
    T: Archive + Serialize<AllocSerializer<256>> + Eq + Debug,
    Archived<T>: Deserialize<T, Infallible>,
{
    let layout = ArchiveLayout::new(size_of::<Archived<T>>(), fields).unwrap();
    let encoder = CompiledArchive::compile(layout).unwrap();
    let inputs: Vec<_> = values.iter().map(slots).collect();
    let result = encoder.encode_wide(&inputs).unwrap();
    drop(encoder);
    assert_eq!(result.rows().len(), values.len());
    for (encoded, expected) in result.rows().zip(values) {
        let reference = rkyv::to_bytes::<_, 256>(expected).unwrap();
        assert_eq!(encoded, reference.as_slice());
        let mut aligned = rkyv::AlignedVec::new();
        aligned.extend_from_slice(encoded);
        // SAFETY: the encoded bytes match the ordinary serializer, and the
        // relocated copy provides the required alignment.
        let archived = unsafe { rkyv::archived_root::<T>(&aligned) };
        let actual: T = archived.deserialize(&mut Infallible).unwrap();
        assert_eq!(&actual, expected);
    }
}

fn optional<T: Default>(integer: ArchiveInteger, offset: usize) -> ArchiveField {
    let sample = rkyv::option::ArchivedOption::Some(T::default());
    let rkyv::option::ArchivedOption::Some(value) = &sample else {
        unreachable!()
    };
    let payload = std::ptr::from_ref(value) as usize - std::ptr::from_ref(&sample) as usize;
    ArchiveField {
        integer,
        payload_offset: offset + payload,
        null_tag_offset: Some(offset),
    }
}
fn required(integer: ArchiveInteger, offset: usize) -> ArchiveField {
    ArchiveField {
        integer,
        payload_offset: offset,
        null_tag_offset: None,
    }
}

#[test]
fn linear_aggregate_accumulators_match_runtime_archives() {
    use ArchiveInteger::{I64, I128};

    // Contributions and stored weights in the selected nullable SUM circuit.
    type Sum = Tup3<i64, i64, i64>;
    let contributions: [Sum; 3] = [Tup3(0, 0, 0), Tup3(0, 0, 1), Tup3(i64::MIN, -1, -2)];
    verify_rows(
        vec![
            required(I64, offset_of!(Archived<Sum>, 0)),
            required(I64, offset_of!(Archived<Sum>, 1)),
            required(I64, offset_of!(Archived<Sum>, 2)),
        ],
        &contributions,
        |row| vec![Some(row.0.into()), Some(row.1.into()), Some(row.2.into())],
    );

    // The global COUNT and COUNT DISTINCT share one nested linear weight.
    type RowCount = Tup2<i128, i64>;
    type KeyCount = Tup2<i64, i64>;
    type Counts = Archived<Tup2<RowCount, KeyCount>>;
    let rows = offset_of!(Counts, 0);
    let keys = offset_of!(Counts, 1);
    verify_rows(
        vec![
            required(I128, rows + offset_of!(Archived<RowCount>, 0)),
            required(I64, rows + offset_of!(Archived<RowCount>, 1)),
            required(I64, keys + offset_of!(Archived<KeyCount>, 0)),
            required(I64, keys + offset_of!(Archived<KeyCount>, 1)),
        ],
        &[
            Tup2(Tup2(0_i128, 0_i64), Tup2(0_i64, 0_i64)),
            Tup2(Tup2(1_i128 << 96, 2_i64), Tup2(1_i64, 2_i64)),
            Tup2(Tup2(i128::MIN, -2_i64), Tup2(-1_i64, -2_i64)),
        ],
        |Tup2(Tup2(rows, row_count), Tup2(keys, key_count))| {
            vec![
                Some(*rows),
                Some((*row_count).into()),
                Some((*keys).into()),
                Some((*key_count).into()),
            ]
        },
    );
}

#[test]
fn partitioned_rolling_count_batches_match_runtime_archives() {
    use ArchiveInteger::{I64, U64};

    // The rolling result is indexed by the unit partition and uses an
    // unsigned timestamp paired with an optional COUNT result.
    type Count = Tup1<i64>;
    type TimedCount = Tup2<u64, Option<Count>>;
    type Row = Archived<Tup2<Tup0, TimedCount>>;
    let timed = offset_of!(Row, 1);
    verify_rows(
        vec![
            required(U64, timed + offset_of!(Archived<TimedCount>, 0)),
            optional::<i64>(I64, timed + offset_of!(Archived<TimedCount>, 1)),
        ],
        &[
            Tup2(Tup0::new(), Tup2(0_u64, None)),
            Tup2(Tup0::new(), Tup2(0_u64, Some(Tup1(0_i64)))),
            Tup2(Tup0::new(), Tup2(u64::MAX, Some(Tup1(i64::MIN)))),
        ],
        |Tup2(_, Tup2(timestamp, count))| {
            vec![Some((*timestamp).into()), count.map(|count| count.0.into())]
        },
    );
}

#[test]
fn partitioned_rolling_count_tree_rows_match_runtime_archives() {
    use ArchiveInteger::{I64, U32, U64};
    use dbsp::operator::dynamic::time_series::archive_testing::{
        rolling_count_tree_offsets, rolling_count_tree_row,
    };

    let offsets = rolling_count_tree_offsets();
    let mut fields = vec![
        required(U64, offsets.prefix_key),
        required(U32, offsets.prefix_len),
    ];
    for child in offsets.children {
        fields.extend(
            [
                (U64, child.prefix_key),
                (U32, child.prefix_len),
                (I64, child.count),
            ]
            .map(|(integer, payload_offset)| ArchiveField {
                integer,
                payload_offset,
                null_tag_offset: Some(child.tag),
            }),
        );
    }
    let fixtures = [
        ((0, 0), [None, None]),
        ((0, 0), [None, Some((u64::MAX, 64, 2))]),
        ((0, 0), [Some((0, 64, 0)), None]),
        (
            (0, 0),
            [Some((0, 1, i64::MIN)), Some((1 << 63, 1, i64::MAX))],
        ),
        ((1 << 63, 1), [Some((1 << 63, 2, 7)), Some((3 << 62, 2, 9))]),
    ];
    let values = fixtures.map(|(prefix, children)| rolling_count_tree_row(prefix, children));
    let mut lanes = fixtures.iter().map(|(prefix, children)| {
        let mut row = vec![Some(i128::from(prefix.0)), Some(i128::from(prefix.1))];
        for child in children {
            row.extend(match child {
                Some((key, len, count)) => [
                    Some(i128::from(*key)),
                    Some(i128::from(*len)),
                    Some(i128::from(*count)),
                ],
                None => [None; 3],
            });
        }
        row
    });
    verify_rows(fields, &values, |_| lanes.next().unwrap());
}

#[test]
fn optional_accumulator_shares_one_archive_discriminant() {
    type Sum = Tup3<i64, i64, i64>;
    let sample: Sum = Tup3(i64::MIN, 2, 3);
    let reference = rkyv::to_bytes::<_, 256>(&Some(sample)).unwrap();
    // SAFETY: The aligned buffer comes directly from rkyv's serializer for
    // Option<Sum> and remains alive while we read the payload position.
    let archived = unsafe { rkyv::archived_root::<Option<Sum>>(&reference) };
    let payload = archived.as_ref().unwrap();
    let offset = std::ptr::from_ref(payload) as usize - std::ptr::from_ref(archived) as usize;
    let fields = [
        offset_of!(Archived<Sum>, 0),
        offset_of!(Archived<Sum>, 1),
        offset_of!(Archived<Sum>, 2),
    ]
    .into_iter()
    .map(|field| ArchiveField {
        integer: ArchiveInteger::I64,
        payload_offset: offset + field,
        null_tag_offset: Some(0),
    })
    .collect::<Vec<_>>();
    verify_rows(
        fields.clone(),
        &[None, Some(Tup3(0, 0, 0)), Some(sample)],
        |row| match row {
            None => vec![None; 3],
            Some(Tup3(sum, non_null, count)) => {
                vec![
                    Some((*sum).into()),
                    Some((*non_null).into()),
                    Some((*count).into()),
                ]
            }
        },
    );

    let encoder = CompiledArchive::compile(
        ArchiveLayout::new(size_of::<Archived<Option<Sum>>>(), fields).unwrap(),
    )
    .unwrap();
    let batch = Batch::from_rows(3, &[vec![Some(i64::MIN), Some(2), Some(3)]]).unwrap();
    let encoded = encoder.encode(&batch).unwrap();
    assert_eq!(encoded.rows().next(), Some(reference.as_slice()));
    let partial = vec![Some(1), None, Some(2)];
    let batch = Batch::from_rows(3, std::slice::from_ref(&partial)).unwrap();
    assert!(encoder.encode(&batch).is_err());
    let wide = partial
        .into_iter()
        .map(|value| value.map(i128::from))
        .collect();
    assert!(encoder.encode_wide(&[wide]).is_err());
    let mut row = vec![0xff; reference.len()];
    assert!(
        encoder
            .encode_wide_row_into(&[Some(1), None, Some(3)], &mut row)
            .is_err()
    );
    assert!(row.iter().all(|&byte| byte == 0xff));
    encoder
        .encode_wide_row_into(&[Some(i64::MIN.into()), Some(2), Some(3)], &mut row)
        .unwrap();
    assert_eq!(row, reference.as_slice());
}

#[test]
fn boolean_tuple_shapes_match_the_runtime_archives() {
    use ArchiveInteger::{Bool, I32};
    type Single = Archived<Tup1<bool>>;
    verify_rows(
        vec![required(Bool, offset_of!(Single, 0))],
        &[Tup1(false), Tup1(true)],
        |row| vec![Some(row.0.into())],
    );
    verify_rows(
        vec![optional::<bool>(Bool, 0)],
        &[None, Some(Tup1(false)), Some(Tup1(true))],
        |row| vec![row.map(|tuple| tuple.0.into())],
    );

    type Pair = Archived<Tup2<bool, Option<bool>>>;
    verify_rows(
        vec![
            required(Bool, offset_of!(Pair, 0)),
            optional::<bool>(Bool, offset_of!(Pair, 1)),
        ],
        &[
            Tup2(false, None),
            Tup2(true, Some(false)),
            Tup2(false, Some(true)),
        ],
        |row| vec![Some(row.0.into()), row.1.map(i128::from)],
    );
    type NullablePair = Archived<Tup2<Option<bool>, Option<bool>>>;
    verify_rows(
        vec![
            optional::<bool>(Bool, offset_of!(NullablePair, 0)),
            optional::<bool>(Bool, offset_of!(NullablePair, 1)),
        ],
        &[
            Tup2(None, None),
            Tup2(None, Some(false)),
            Tup2(Some(true), None),
            Tup2(Some(false), Some(true)),
        ],
        |row| vec![row.0.map(i128::from), row.1.map(i128::from)],
    );
    type Three = Archived<Tup3<i32, bool, Option<i32>>>;
    verify_rows(
        vec![
            required(I32, offset_of!(Three, 0)),
            required(Bool, offset_of!(Three, 1)),
            optional::<i32>(I32, offset_of!(Three, 2)),
        ],
        &[
            Tup3(i32::MIN, false, Some(i32::MAX)),
            Tup3(0, true, None),
            Tup3(i32::MAX, false, Some(i32::MIN)),
        ],
        |row| {
            vec![
                Some(row.0.into()),
                Some(row.1.into()),
                row.2.map(i128::from),
            ]
        },
    );
}

#[test]
fn unit_and_unsigned_time_shapes_match_the_runtime_archives() {
    use ArchiveInteger::{I64, U64};
    verify_rows(vec![], &[Tup0::new(); 3], |_| vec![]);
    verify_rows(
        vec![required(U64, 0)],
        &[0_u64, u64::MAX, i64::MAX as u64 + 1],
        |value| vec![Some((*value).into())],
    );

    type A = Archived<Tup2<u64, Option<Tup1<i64>>>>;
    verify_rows(
        vec![
            required(U64, offset_of!(A, 0)),
            optional::<i64>(I64, offset_of!(A, 1)),
        ],
        &[
            Tup2(0, None),
            Tup2(u64::MAX, Some(Tup1(i64::MIN))),
            Tup2(u64::MAX - 1, Some(Tup1(i64::MAX))),
        ],
        |row| vec![Some(row.0.into()), row.1.map(|tuple| tuple.0.into())],
    );
}

#[test]
fn selected_decimal_row_shapes_match_the_runtime_archives() {
    use ArchiveInteger::{I32, I128};
    type Decimal = Fixed<38, 10>;
    type Four = Archived<Tup4<i32, i32, Option<Decimal>, Option<Decimal>>>;
    let min = Some(Decimal::MIN);
    let max = Some(Decimal::MAX);
    let zero = Some(Decimal::default());
    let mantissa = |decimal: Option<Decimal>| decimal.map(|number| number.mantissa());
    verify_rows(
        vec![
            required(I32, offset_of!(Four, 0)),
            required(I32, offset_of!(Four, 1)),
            optional::<Decimal>(I128, offset_of!(Four, 2)),
            optional::<Decimal>(I128, offset_of!(Four, 3)),
        ],
        &[
            Tup4(i32::MIN, i32::MAX, min, None),
            Tup4(0, i32::MIN, None, max),
            Tup4(-1, 0, zero, min),
        ],
        |row| {
            vec![
                Some(row.0.into()),
                Some(row.1.into()),
                mantissa(row.2),
                mantissa(row.3),
            ]
        },
    );
    type Three = Archived<Tup3<Option<i32>, Option<i32>, Option<Decimal>>>;
    verify_rows(
        vec![
            optional::<i32>(I32, offset_of!(Three, 0)),
            optional::<i32>(I32, offset_of!(Three, 1)),
            optional::<Decimal>(I128, offset_of!(Three, 2)),
        ],
        &[
            Tup3(None, Some(i32::MAX), min),
            Tup3(Some(i32::MIN), None, max),
            Tup3(Some(1), Some(0), None),
        ],
        |row| {
            vec![
                row.0.map(i128::from),
                row.1.map(i128::from),
                mantissa(row.2),
            ]
        },
    );
    type NullableFour = Archived<Tup4<Option<i32>, Option<i32>, Option<Decimal>, Option<Decimal>>>;
    verify_rows(
        vec![
            optional::<i32>(I32, offset_of!(NullableFour, 0)),
            optional::<i32>(I32, offset_of!(NullableFour, 1)),
            optional::<Decimal>(I128, offset_of!(NullableFour, 2)),
            optional::<Decimal>(I128, offset_of!(NullableFour, 3)),
        ],
        &[
            Tup4(None, Some(i32::MAX), min, None),
            Tup4(Some(i32::MIN), None, None, max),
            Tup4(Some(0), Some(-1), zero, min),
        ],
        |row| {
            vec![
                row.0.map(i128::from),
                row.1.map(i128::from),
                mantissa(row.2),
                mantissa(row.3),
            ]
        },
    );
    type Alternating = Archived<Tup4<Option<i32>, Option<Decimal>, Option<i32>, Option<Decimal>>>;
    verify_rows(
        vec![
            optional::<i32>(I32, offset_of!(Alternating, 0)),
            optional::<Decimal>(I128, offset_of!(Alternating, 1)),
            optional::<i32>(I32, offset_of!(Alternating, 2)),
            optional::<Decimal>(I128, offset_of!(Alternating, 3)),
        ],
        &[
            Tup4(None, min, Some(i32::MAX), None),
            Tup4(Some(i32::MIN), None, None, max),
            Tup4(Some(0), zero, Some(-1), min),
        ],
        |row| {
            vec![
                row.0.map(i128::from),
                mantissa(row.1),
                row.2.map(i128::from),
                mantissa(row.3),
            ]
        },
    );
}

#[test]
fn decimal_window_values_and_nested_pair_match_the_runtime_archives() {
    use ArchiveInteger::{I32, I128};
    type Decimal = Fixed<6, 2>;
    type Pair = Tup2<i32, Option<Decimal>>;
    type A = Archived<Pair>;
    let values = [
        Tup2(i32::MIN, None),
        Tup2(i32::MAX, Some(Decimal::MAX)),
        Tup2(0, Some(Decimal::MIN)),
    ];
    let slots = |row: &Pair| vec![Some(row.0.into()), row.1.map(|value| value.mantissa())];
    verify_rows(
        vec![
            required(I32, offset_of!(A, 0)),
            optional::<Decimal>(I128, offset_of!(A, 1)),
        ],
        &values,
        slots,
    );

    type Nested = Archived<Tup2<Tup0, Pair>>;
    let base = offset_of!(Nested, 1);
    let nested = values.map(|row| Tup2(Tup0::new(), row));
    verify_rows(
        vec![
            required(I32, base + offset_of!(A, 0)),
            optional::<Decimal>(I128, base + offset_of!(A, 1)),
        ],
        &nested,
        |row| slots(&row.1),
    );
}
fn compare<T>(layout: ArchiveLayout, values: Vec<T>, rows: Vec<Vec<Option<i128>>>)
where
    T: Archive + Serialize<AllocSerializer<256>> + Ord + Hash + Debug,
    Archived<T>: Deserialize<T, Infallible> + Ord + OrdRepr<T> + HashRepr,
{
    let encoder = CompiledArchive::compile(layout).unwrap();
    let bytes = encoder.encode_wide(&rows).unwrap();
    drop(encoder);
    for (encoded, value) in bytes.rows().zip(&values) {
        let reference = rkyv::to_bytes::<_, 256>(value).unwrap();
        assert_eq!(encoded, reference.as_slice());
        let mut aligned = rkyv::AlignedVec::new();
        aligned.extend_from_slice(encoded);
        // SAFETY: complete native bytes equal the trusted reference archive;
        // AlignedVec provides its alignment and the full archived object.
        let archived = unsafe { rkyv::archived_root::<T>(&aligned) };
        let decoded: T = archived.deserialize(&mut Infallible).unwrap();
        assert_eq!(&decoded, value);
        let mut actual = Xxh3Default::default();
        archived.hash_repr(&mut actual);
        let mut expected = Xxh3Default::default();
        value.hash(&mut expected);
        assert_eq!(actual.finish(), expected.finish());
        for other in &values {
            assert_eq!(archived.ord_cmp(other), value.cmp(other));
        }
    }
}
fn nullable<const P: usize, const S: usize>(texts: &[&str]) {
    type Row<const P: usize, const S: usize> = Tup1<Option<Fixed<P, S>>>;
    let values: Vec<Option<Fixed<P, S>>> = std::iter::once(None)
        .chain(texts.iter().map(|s| Some(s.parse().unwrap())))
        .collect();
    let layout = ArchiveLayout::new(
        size_of::<Archived<Row<P, S>>>(),
        vec![optional::<Fixed<P, S>>(
            ArchiveInteger::I128,
            offset_of!(Archived<Row<P, S>>, 0),
        )],
    )
    .unwrap();
    compare(
        layout,
        values.iter().map(|v| Tup1(*v)).collect(),
        values
            .iter()
            .map(|v| vec![v.map(|d| d.mantissa())])
            .collect(),
    );
}
#[test]
fn decimal_window_nullable_key_preserves_full_mantissa_and_nulls() {
    // overDecimalTest: Tup1<Option<SqlDecimal<6,2>>> index key, not BIGINT.
    nullable::<6, 2>(&["-9999.99", "-0.01", "0", "0.01", "9999.99"]);
}
#[test]
fn decimal38_inputs_and_multiplication_scale_are_not_narrowed() {
    nullable::<38, 10>(&[
        "-9999999999999999999999999999.9999999999",
        "-0.0000000001",
        "0",
        "0.0000000001",
        "9999999999999999999999999999.9999999999",
    ]);
    // Scalar result type required by testMultiply; this single-field tuple is
    // an additional layout stress case, not a claimed observed corpus row.
    nullable::<38, 20>(&[
        "-999999999999999999.99999999999999999999",
        "-0.00000000000000000001",
        "0",
        "0.00000000000000000001",
        "999999999999999999.99999999999999999999",
    ]);
}
#[test]
fn numeric_input_row_mixes_nullable_integer_and_nullable_decimal() {
    type D = Fixed<38, 10>;
    type Row = Tup2<Option<i32>, Option<D>>;
    type A = Archived<Row>;
    let layout = ArchiveLayout::new(
        size_of::<A>(),
        vec![
            optional::<i32>(ArchiveInteger::I32, offset_of!(A, 0)),
            optional::<D>(ArchiveInteger::I128, offset_of!(A, 1)),
        ],
    )
    .unwrap();
    let decimals = [
        None,
        Some(
            "-9999999999999999999999999999.9999999999"
                .parse::<D>()
                .unwrap(),
        ),
        Some(D::default()),
        Some("0.0000000001".parse::<D>().unwrap()),
    ];
    let mut values = Vec::new();
    let mut rows = Vec::new();
    for integer in [None, Some(i32::MIN), Some(0), Some(i32::MAX)] {
        for decimal in decimals {
            values.push(Tup2(integer, decimal));
            rows.push(vec![integer.map(i128::from), decimal.map(|d| d.mantissa())]);
        }
    }
    compare(layout, values, rows);
}
#[test]
fn numeric_expected_row_keeps_both_nonnullable_decimal_payloads() {
    type D = Fixed<38, 10>;
    type Row = Tup4<i32, i64, D, D>;
    type A = Archived<Row>;
    let layout = ArchiveLayout::new(
        size_of::<A>(),
        vec![
            required(ArchiveInteger::I32, offset_of!(A, 0)),
            required(ArchiveInteger::I64, offset_of!(A, 1)),
            required(ArchiveInteger::I128, offset_of!(A, 2)),
            required(ArchiveInteger::I128, offset_of!(A, 3)),
        ],
    )
    .unwrap();
    let a = "9999999999999999999999999999.9999999999"
        .parse::<D>()
        .unwrap();
    let b = "-0.0000000001".parse::<D>().unwrap();
    compare(
        layout,
        vec![Tup4(i32::MIN, i64::MAX, a, b), Tup4(0, 0, b, a)],
        vec![
            vec![
                Some(i32::MIN.into()),
                Some(i64::MAX.into()),
                Some(a.mantissa()),
                Some(b.mantissa()),
            ],
            vec![Some(0), Some(0), Some(b.mantissa()), Some(a.mantissa())],
        ],
    );
}
#[test]
fn wide_archive_rejects_narrowing_null_and_shape_errors() {
    let encoder = CompiledArchive::compile(
        ArchiveLayout::new(4, vec![required(ArchiveInteger::I32, 0)]).unwrap(),
    )
    .unwrap();
    assert!(encoder.encode_wide(&[vec![Some(i128::MAX)]]).is_err());
    assert!(encoder.encode_wide(&[vec![None]]).is_err());
    assert!(encoder.encode_wide(&[vec![]]).is_err());
    assert!(encoder.encode_wide(&vec![vec![Some(0)]; 65_537]).is_err());
    assert_eq!(encoder.encode_wide(&[]).unwrap().rows().len(), 0);
    // Precision is checked by the actual SQL decimal constructor, not inferred
    // from an untyped i128 archive slot.
    assert!("10000.00".parse::<Fixed<6, 2>>().is_err());
}

#[test]
fn nullable_decimal_window_keys_preserve_order_for_each_null_policy() {
    use feldera_native_probe::{KeyField, KeyLayout};
    use std::cmp::Ordering;
    let values = [
        None,
        Some("-9999.99".parse::<Fixed<6, 2>>().unwrap()),
        Some("-0.01".parse().unwrap()),
        Some(Fixed::default()),
        Some("0.01".parse().unwrap()),
        Some("9999.99".parse().unwrap()),
    ];
    let rows: Vec<_> = values
        .iter()
        .map(|v| vec![v.map(|d| d.mantissa())])
        .collect();
    for descending in [false, true] {
        for nulls_first in [false, true] {
            let layout = KeyLayout::new(
                1,
                vec![KeyField {
                    column: 0,
                    descending,
                    nulls_first,
                }],
            )
            .unwrap();
            let keys = layout.encode_wide(&rows).unwrap();
            for (i, left) in values.iter().enumerate() {
                for (j, right) in values.iter().enumerate() {
                    let expected = match (left, right) {
                        (None, None) => Ordering::Equal,
                        (None, Some(_)) => {
                            if nulls_first {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                        (Some(_), None) => {
                            if nulls_first {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                        (Some(left), Some(right)) => {
                            if descending {
                                right.cmp(left)
                            } else {
                                left.cmp(right)
                            }
                        }
                    };
                    assert_eq!(keys.key(i).unwrap().cmp(keys.key(j).unwrap()), expected);
                }
            }
        }
    }
    assert!(
        KeyLayout::new(1, vec![])
            .unwrap()
            .encode_wide(&[vec![]])
            .is_err()
    );
}
