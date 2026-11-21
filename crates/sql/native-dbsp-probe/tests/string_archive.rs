//! Actual SqlString tuple archive bytes, including the inline/relative-pointer
//! transition. Corpus program execution is a separate obligation.
#![cfg(target_os = "linux")]
use dbsp::{
    dynamic::{HashRepr, OrdRepr},
    utils::{Tup1, Tup3, Tup5, Tup7},
};
use feldera_fxp::Fixed;
use feldera_native_probe::{
    ArchiveField, ArchiveInteger, CompiledMixedArchive, CompiledStringArchive, StringArchiveField,
    StringArchiveLayout, StringArchives,
};
use feldera_sqllib::{F64, SqlString};
use rkyv::{Archived, Deserialize, Infallible, Serialize, ser::serializers::AllocSerializer};
use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
    mem::{align_of, offset_of, size_of},
};
use xxhash_rust::xxh3::Xxh3Default;

fn compare_archive_rows<Row>(actual: &StringArchives, rows: &[Row])
where
    Row: Serialize<AllocSerializer<256>> + Eq + Debug,
    Archived<Row>: Deserialize<Row, Infallible>,
{
    assert_eq!(actual.rows().len(), rows.len());
    for ((bytes, root), row) in actual.rows().zip(rows) {
        assert_eq!(bytes, rkyv::to_bytes::<_, 256>(row).unwrap().as_slice());
        assert_eq!(root, bytes.len() - size_of::<Archived<Row>>());
        let mut relocated = rkyv::AlignedVec::new();
        relocated.extend_from_slice(bytes);
        // SAFETY: exact equality to the aligned reference archive establishes
        // this root and the relative pointers in the relocated copy.
        let archived = unsafe { rkyv::archived_root::<Row>(&relocated) };
        let restored: Row = archived.deserialize(&mut Infallible).unwrap();
        assert_eq!(&restored, row);
    }
}

fn strings() -> Vec<String> {
    vec![
        "".into(),
        "🐍x".into(),
        "a\0b".into(),
        "a".repeat(15),
        "a".repeat(16),
        "🐍".repeat(4),
        "é".repeat(15),
        "a".repeat(1024),
        "𐀀".into(),
        "\u{e000}".into(),
    ]
}
#[test]
fn native_string_headers_payloads_and_relocation_equal_actual_sqlstring() {
    type Row = Tup1<SqlString>;
    type A = Archived<Row>;
    assert_eq!(size_of::<rkyv::string::ArchivedString>(), 16);
    let encoder = CompiledStringArchive::compile(
        StringArchiveLayout::new(size_of::<A>(), offset_of!(A, 0), None).unwrap(),
    )
    .unwrap();
    let texts = strings();
    let rows: Vec<_> = texts.iter().map(|s| Some(s.as_str())).collect();
    let native = encoder.encode(&rows).unwrap();
    drop(encoder);
    for ((bytes, root), text) in native.rows().zip(&texts) {
        let value = Tup1(SqlString::from_ref(text));
        let reference = rkyv::to_bytes::<_, 256>(&value).unwrap();
        assert_eq!(bytes, reference.as_slice());
        assert_eq!(root, bytes.len() - size_of::<A>());
        let mut aligned = rkyv::AlignedVec::new();
        aligned.extend_from_slice(bytes);
        // SAFETY: complete byte equality establishes the reference archive and
        // its relative pointer bounds; the owned aligned copy preserves offsets.
        let archived = unsafe { rkyv::archived_root::<Row>(&aligned) };
        let decoded: Row = archived.deserialize(&mut Infallible).unwrap();
        assert_eq!(decoded, value);
        let mut expected = Xxh3Default::default();
        value.hash(&mut expected);
        let mut actual = Xxh3Default::default();
        archived.hash_repr(&mut actual);
        assert_eq!(expected.finish(), actual.finish());
        for other in &texts {
            assert_eq!(
                archived.ord_cmp(&Tup1(SqlString::from_ref(other))),
                text.cmp(other)
            );
        }
    }
}
#[test]
fn nullable_string_archives_distinguish_null_empty_and_long_payload() {
    type Row = Tup1<Option<SqlString>>;
    type A = Archived<Row>;
    // The actual archived payload's offset is obtained from a valid Some,
    // rather than assuming a Rust enum layout for the string option.
    let reference = rkyv::to_bytes::<_, 256>(&Tup1(Some(SqlString::from_ref("x")))).unwrap();
    // SAFETY: this is the serializer's own aligned archive of exactly Row.
    let archived = unsafe { rkyv::archived_root::<Row>(&reference) };
    let payload = archived.0.as_ref().unwrap();
    let offset = std::ptr::from_ref(payload) as usize - std::ptr::from_ref(archived) as usize;
    let encoder = CompiledStringArchive::compile(
        StringArchiveLayout::new(size_of::<A>(), offset, Some(offset_of!(A, 0))).unwrap(),
    )
    .unwrap();
    let texts = strings();
    let rows: Vec<_> = std::iter::once(None)
        .chain(texts.iter().map(|s| Some(s.as_str())))
        .collect();
    let native = encoder.encode(&rows).unwrap();
    for ((bytes, root), text) in native.rows().zip(&rows) {
        let value = Tup1(text.map(SqlString::from_ref));
        assert_eq!(bytes, rkyv::to_bytes::<_, 256>(&value).unwrap().as_slice());
        assert_eq!(root, bytes.len() - size_of::<A>());
        let payload = text.filter(|text| text.len() > 15).map(|_| 0);
        let mut single_root = [0x5a; size_of::<A>()];
        encoder
            .encode_single_root_into(*text, root, payload, &mut single_root)
            .unwrap();
        assert_eq!(&single_root, &bytes[root..]);
        let mut aligned = rkyv::AlignedVec::new();
        aligned.extend_from_slice(bytes);
        // SAFETY: complete reference byte equality establishes tags, payloads
        // and relative pointer bounds; copying the whole archive preserves them.
        let archived = unsafe { rkyv::archived_root::<Row>(&aligned) };
        let decoded: Row = archived.deserialize(&mut Infallible).unwrap();
        assert_eq!(decoded, value);
        let mut expected = Xxh3Default::default();
        value.hash(&mut expected);
        let mut actual = Xxh3Default::default();
        archived.hash_repr(&mut actual);
        assert_eq!(expected.finish(), actual.finish());
        for other in &rows {
            let other = Tup1(other.map(SqlString::from_ref));
            assert_eq!(archived.ord_cmp(&other), value.cmp(&other));
        }
    }
    let mut untouched = [0x5a; size_of::<A>()];
    assert!(
        encoder
            .encode_single_root_into(Some(&texts[4]), 16, Some(8), &mut untouched)
            .is_err()
    );
    assert_eq!(untouched, [0x5a; size_of::<A>()]);
}

#[test]
fn three_string_row_matches_runtime_field_order_and_archive_bytes() {
    type Row = Tup3<SqlString, SqlString, SqlString>;
    type A = Archived<Row>;

    let fields = [offset_of!(A, 0), offset_of!(A, 1), offset_of!(A, 2)]
        .into_iter()
        .map(|header_offset| StringArchiveField {
            header_offset,
            null_tag_offset: None,
        })
        .collect();
    let encoder =
        CompiledStringArchive::compile(StringArchiveLayout::tuple(size_of::<A>(), fields).unwrap())
            .unwrap();
    let texts = strings();
    let mut first = Vec::new();
    let mut second = Vec::new();
    let mut third = Vec::new();
    for a in &texts {
        for b in &texts {
            for c in &texts {
                first.push(Some(a.as_str()));
                second.push(Some(b.as_str()));
                third.push(Some(c.as_str()));
            }
        }
    }
    let actual = encoder.encode_columns(&[&first, &second, &third]).unwrap();
    drop(encoder);
    let rows: Vec<_> = (0..first.len())
        .map(|index| {
            Tup3(
                SqlString::from_ref(first[index].unwrap()),
                SqlString::from_ref(second[index].unwrap()),
                SqlString::from_ref(third[index].unwrap()),
            )
        })
        .collect();
    assert_eq!(rows.len(), 1000);
    compare_archive_rows(&actual, &rows);
}

#[test]
fn string_and_integer_tuple_matches_runtime_archive_bytes() {
    type Row = Tup5<Option<SqlString>, SqlString, Option<i32>, i32, Option<SqlString>>;
    type A = Archived<Row>;
    let witness = Tup5(
        Some(SqlString::from_ref("é")),
        SqlString::from_ref("a"),
        Some(19_i32),
        -10_i32,
        Some(SqlString::from_ref("long value with a payload")),
    );
    let reference = rkyv::to_bytes::<_, 256>(&witness).unwrap();
    // SAFETY: this is the serializer's own aligned archive of exactly Row.
    let archived = unsafe { rkyv::archived_root::<Row>(&reference) };
    let offset = |pointer: *const u8| pointer as usize - std::ptr::from_ref(archived) as usize;
    let strings = vec![
        StringArchiveField {
            header_offset: offset(std::ptr::from_ref(archived.0.as_ref().unwrap()).cast()),
            null_tag_offset: Some(offset_of!(A, 0)),
        },
        StringArchiveField {
            header_offset: offset_of!(A, 1),
            null_tag_offset: None,
        },
        StringArchiveField {
            header_offset: offset(std::ptr::from_ref(archived.4.as_ref().unwrap()).cast()),
            null_tag_offset: Some(offset_of!(A, 4)),
        },
    ];
    let integers = vec![
        ArchiveField {
            integer: ArchiveInteger::I32,
            payload_offset: offset(std::ptr::from_ref(archived.2.as_ref().unwrap()).cast()),
            null_tag_offset: Some(offset_of!(A, 2)),
        },
        ArchiveField {
            integer: ArchiveInteger::I32,
            payload_offset: offset_of!(A, 3),
            null_tag_offset: None,
        },
    ];
    let mut overlap = integers.clone();
    overlap[1].payload_offset = strings[0].header_offset;
    assert!(CompiledMixedArchive::compile(size_of::<A>(), strings.clone(), overlap).is_err());
    let encoder = CompiledMixedArchive::compile(size_of::<A>(), strings, integers).unwrap();

    let mut rows = Vec::new();
    for first in [None, Some(""), Some("a"), Some("é"), Some("🐍")] {
        for second in ["", "a", "é", "\0", "🦀"] {
            for (index, number) in [None, Some(i32::MIN), Some(0), Some(i32::MAX)]
                .into_iter()
                .enumerate()
            {
                for last in [
                    None,
                    Some(""),
                    Some("abcdefghijklmno"),
                    Some("abcdefghijklmnop"),
                    Some("🦀and\0an outlined string"),
                ] {
                    rows.push(Tup5(
                        first.map(SqlString::from_ref),
                        SqlString::from_ref(second),
                        number,
                        [i32::MAX, 0, -1, i32::MIN][index],
                        last.map(SqlString::from_ref),
                    ));
                }
            }
        }
    }
    let first: Vec<_> = rows
        .iter()
        .map(|row| row.0.as_ref().map(SqlString::str))
        .collect();
    let second: Vec<_> = rows.iter().map(|row| Some(row.1.str())).collect();
    let third: Vec<_> = rows
        .iter()
        .map(|row| row.4.as_ref().map(SqlString::str))
        .collect();
    let integers: Vec<_> = rows
        .iter()
        .map(|row| vec![row.2.map(i128::from), Some(row.3.into())])
        .collect();
    let actual = encoder
        .encode(&[&first, &second, &third], &integers)
        .unwrap();
    drop(encoder);
    assert_eq!(rows.len(), 500);
    compare_archive_rows(&actual, &rows);
}

#[test]
fn string_decimal_tuple_matches_runtime_with_decimal_alignment() {
    type Decimal = Fixed<6, 2>;
    type Row = Tup7<i32, F64, bool, SqlString, Option<i32>, Option<Decimal>, Option<u32>>;
    type A = Archived<Row>;
    let witness = Tup7(
        0,
        F64::new(0.0),
        false,
        SqlString::new(),
        Some(0),
        Some(Decimal::MAX),
        Some(0),
    );
    let reference = rkyv::to_bytes::<_, 256>(&witness).unwrap();
    // SAFETY: the ordinary serializer produced an aligned archive for Row.
    let archived = unsafe { rkyv::archived_root::<Row>(&reference) };
    let offset = |pointer: *const u8| pointer as usize - std::ptr::from_ref(archived) as usize;
    let string_fields = vec![StringArchiveField {
        header_offset: offset_of!(A, 3),
        null_tag_offset: None,
    }];
    let integers = vec![
        ArchiveField {
            integer: ArchiveInteger::I32,
            payload_offset: offset_of!(A, 0),
            null_tag_offset: None,
        },
        ArchiveField {
            integer: ArchiveInteger::I64,
            payload_offset: offset_of!(A, 1),
            null_tag_offset: None,
        },
        ArchiveField {
            integer: ArchiveInteger::Bool,
            payload_offset: offset_of!(A, 2),
            null_tag_offset: None,
        },
        ArchiveField {
            integer: ArchiveInteger::I32,
            payload_offset: offset(std::ptr::from_ref(archived.4.as_ref().unwrap()).cast()),
            null_tag_offset: Some(offset_of!(A, 4)),
        },
        ArchiveField {
            integer: ArchiveInteger::I128,
            payload_offset: offset(std::ptr::from_ref(archived.5.as_ref().unwrap()).cast()),
            null_tag_offset: Some(offset_of!(A, 5)),
        },
        ArchiveField {
            integer: ArchiveInteger::U32,
            payload_offset: offset(std::ptr::from_ref(archived.6.as_ref().unwrap()).cast()),
            null_tag_offset: Some(offset_of!(A, 6)),
        },
    ];
    let encoder = CompiledMixedArchive::compile_aligned(
        size_of::<A>(),
        align_of::<A>(),
        string_fields,
        integers,
    )
    .unwrap();

    let mut rows = Vec::new();
    for text in strings() {
        for float in [
            f64::NEG_INFINITY,
            -1.0,
            -0.0,
            1.0,
            f64::MAX,
            f64::INFINITY,
            f64::NAN,
        ] {
            for enabled in [false, true] {
                rows.push(Tup7(
                    i32::MIN,
                    F64::new(float),
                    enabled,
                    SqlString::from_ref(&text),
                    Some(i32::MAX),
                    None,
                    Some(u32::MAX),
                ));
                rows.push(Tup7(
                    i32::MAX,
                    F64::new(float),
                    enabled,
                    SqlString::from_ref(&text),
                    None,
                    Some(Decimal::MIN),
                    None,
                ));
            }
        }
    }
    let text: Vec<_> = rows.iter().map(|row| Some(row.3.str())).collect();
    let integers: Vec<_> = rows
        .iter()
        .map(|row| {
            vec![
                Some(row.0.into()),
                Some((row.1.into_inner().to_bits() as i64).into()),
                Some(row.2.into()),
                row.4.map(i128::from),
                row.5.map(|decimal| decimal.mantissa()),
                row.6.map(i128::from),
            ]
        })
        .collect();
    let mut invalid = integers[0].clone();
    invalid[2] = Some(2);
    assert!(encoder.encode(&[&text[..1]], &[invalid]).is_err());
    let mut invalid = integers[0].clone();
    invalid[5] = Some(-1);
    assert!(encoder.encode(&[&text[..1]], &[invalid]).is_err());
    let actual = encoder.encode(&[&text], &integers).unwrap();
    drop(encoder);
    assert_eq!(rows.len(), 280);
    compare_archive_rows(&actual, &rows);
}

#[test]
fn nullable_nested_string_tuple_matches_runtime_archive_bytes() {
    type Address = Tup3<Option<SqlString>, Option<SqlString>, Option<i32>>;
    type Row = Tup1<Option<Address>>;
    type A = Archived<Row>;
    let witness = Tup1(Some(Tup3(
        Some(SqlString::from_ref("city")),
        Some(SqlString::from_ref("street")),
        Some(1_i32),
    )));
    let reference = rkyv::to_bytes::<_, 256>(&witness).unwrap();
    // SAFETY: the ordinary serializer produced an aligned archive for Row.
    let archived = unsafe { rkyv::archived_root::<Row>(&reference) };
    let address = archived.0.as_ref().unwrap();
    let offset = |pointer: *const u8| pointer as usize - std::ptr::from_ref(archived) as usize;
    let string_fields = vec![
        StringArchiveField {
            header_offset: offset(std::ptr::from_ref(address.0.as_ref().unwrap()).cast()),
            null_tag_offset: Some(offset(std::ptr::from_ref(&address.0).cast())),
        },
        StringArchiveField {
            header_offset: offset(std::ptr::from_ref(address.1.as_ref().unwrap()).cast()),
            null_tag_offset: Some(offset(std::ptr::from_ref(&address.1).cast())),
        },
    ];
    let integer_fields = vec![
        ArchiveField {
            integer: ArchiveInteger::Bool,
            payload_offset: offset_of!(A, 0),
            null_tag_offset: None,
        },
        ArchiveField {
            integer: ArchiveInteger::I32,
            payload_offset: offset(std::ptr::from_ref(address.2.as_ref().unwrap()).cast()),
            null_tag_offset: Some(offset(std::ptr::from_ref(&address.2).cast())),
        },
    ];
    let encoder =
        CompiledMixedArchive::compile(size_of::<A>(), string_fields, integer_fields).unwrap();

    let mut rows = vec![Tup1(None)];
    let names = [
        None,
        Some(""),
        Some("φ\0"),
        Some("a longer field that uses a relative pointer"),
    ];
    for city in names {
        for street in names {
            for number in [None, Some(i32::MIN), Some(i32::MAX)] {
                rows.push(Tup1(Some(Tup3(
                    city.map(SqlString::from_ref),
                    street.map(SqlString::from_ref),
                    number,
                ))));
            }
        }
    }
    let cities: Vec<_> = rows
        .iter()
        .map(|row| {
            row.0
                .as_ref()
                .and_then(|address| address.0.as_ref().map(SqlString::str))
        })
        .collect();
    let streets: Vec<_> = rows
        .iter()
        .map(|row| {
            row.0
                .as_ref()
                .and_then(|address| address.1.as_ref().map(SqlString::str))
        })
        .collect();
    let integers: Vec<_> = rows
        .iter()
        .map(|row| {
            vec![
                Some(row.0.is_some().into()),
                row.0.as_ref().and_then(|address| address.2.map(i128::from)),
            ]
        })
        .collect();
    let actual = encoder.encode(&[&cities, &streets], &integers).unwrap();
    drop(encoder);
    assert_eq!(rows.len(), 49);
    compare_archive_rows(&actual, &rows);
}

#[test]
fn string_layout_and_allocation_limits_fail_before_native_execution() {
    assert!(StringArchiveLayout::new(16, 1, None).is_err());
    assert!(StringArchiveLayout::new(16, 0, Some(8)).is_err());
    assert!(StringArchiveLayout::new(16, usize::MAX, None).is_err());
    let field = StringArchiveField {
        header_offset: 0,
        null_tag_offset: None,
    };
    assert!(StringArchiveLayout::tuple(32, vec![field, field]).is_err());
    let tagged = StringArchiveField {
        header_offset: 16,
        null_tag_offset: Some(4),
    };
    assert!(StringArchiveLayout::tuple(32, vec![field, tagged]).is_err());
    let encoder =
        CompiledStringArchive::compile(StringArchiveLayout::new(16, 0, None).unwrap()).unwrap();
    assert!(encoder.encode(&[None]).is_err());
    assert!(encoder.encode_columns(&[&[], &[]]).is_err());
    assert!(encoder.encode(&vec![Some(""); 65_537]).is_err());
    assert!(
        encoder
            .encode(&[Some(&"a".repeat(1024 * 1024 + 1))])
            .is_err()
    );
    let big = "a".repeat(1024 * 1024);
    assert!(encoder.encode(&vec![Some(big.as_str()); 16]).is_err());
    assert_eq!(encoder.encode(&[]).unwrap().rows().len(), 0);
}
