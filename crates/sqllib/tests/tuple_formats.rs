use dbsp::utils::tuple::TupleFormat;
use feldera_sqllib::SqlString;

feldera_macros::declare_tuple! { Tup1<T1> }
feldera_macros::declare_tuple! { Tup2<T1, T2> }
feldera_macros::declare_tuple! { Tup7<T1, T2, T3, T4, T5, T6, T7> }
feldera_macros::declare_tuple! { Tup8<T1, T2, T3, T4, T5, T6, T7, T8> }
feldera_macros::declare_tuple! { Tup12<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> }

type Tup1OptStr = Tup1<Option<SqlString>>;
type Tup2Sql = Tup2<SqlString, Option<u16>>;
type Tup2OptInt = Tup2<Option<i8>, Option<u8>>;
type Tup8Base = Tup8<i8, i16, i32, i64, u8, u16, u32, u64>;
type Tup7Opt =
    Tup7<Option<i8>, Option<i16>, Option<i32>, Option<i64>, Option<u8>, Option<u16>, Option<u32>>;
type Tup8Opt = Tup8<
    Option<i8>,
    Option<i16>,
    Option<i32>,
    Option<i64>,
    Option<u8>,
    Option<u16>,
    Option<u32>,
    Option<u64>,
>;
type Tup12Opt = Tup12<
    Option<i8>,
    Option<i16>,
    Option<i32>,
    Option<i64>,
    Option<u8>,
    Option<u16>,
    Option<u32>,
    Option<u64>,
    Option<bool>,
    Option<f32>,
    Option<f64>,
    Option<SqlString>,
>;

#[repr(C)]
struct ArchivedFormatHeader {
    format: TupleFormat,
}

fn serialize_bytes<T>(value: &T) -> dbsp::storage::buffer_cache::FBuf
where
    T: rkyv::Archive + rkyv::Serialize<dbsp::storage::file::Serializer>,
{
    dbsp::storage::file::to_bytes(value).expect("serialize tuple")
}

fn roundtrip<T>(value: &T)
where
    T: rkyv::Archive
        + rkyv::Serialize<dbsp::storage::file::Serializer>
        + PartialEq
        + core::fmt::Debug,
    rkyv::Archived<T>: rkyv::Deserialize<T, dbsp::storage::file::Deserializer>,
{
    let bytes = serialize_bytes(value);
    let restored: T = dbsp::trace::unaligned_deserialize(&bytes[..]);
    assert_eq!(&restored, value);
}

fn archived_format<T>(bytes: &[u8]) -> TupleFormat
where
    T: rkyv::Archive,
{
    let archived = unsafe { rkyv::archived_root::<T>(bytes) };
    unsafe { (*(archived as *const _ as *const ArchivedFormatHeader)).format }
}

/// Makes sure we can serialize and deserialize the Tup's and
/// we have the same content before and after
#[test]
fn roundtrip_various_sizes() {
    let tup1 = Tup1OptStr::new(Some(SqlString::from_ref("hello")));
    let tup2 = Tup2Sql::new(SqlString::from_ref("world"), Some(42u16));
    let tup8 = Tup8Base::new(-1, -2, -3, -4, 1, 2, 3, 4);
    let tup12 = Tup12Opt::new(
        Some(-8),
        Some(-16),
        Some(-32),
        Some(-64),
        Some(8),
        Some(16),
        Some(32),
        Some(64),
        Some(true),
        Some(1.25),
        Some(2.5),
        Some(SqlString::from_ref("tuple")),
    );

    roundtrip(&tup1);
    roundtrip(&tup2);
    roundtrip(&tup8);
    roundtrip(&tup12);
}

#[test]
fn legacy_small_tuple_sizes() {
    // We use the regular rkyv Archive impl for TupN where N < 8:
    // (because our optimized Tuples have some overhead that only
    // gets amortized when Tuples are large)

    // This means the Tup2
    let tup2_none: Tup2OptInt = Tup2OptInt::new(None, None);
    let bytes2 = serialize_bytes(&tup2_none);
    assert_eq!(bytes2.len(), 4);

    // This is 24 bytes mostly because a None in the archived version of
    // SqlString doesnt get optimized away since it's first member is a [u8; X]
    let tup1_none: Tup1OptStr = Tup1OptStr::new(None);
    let bytes1 = serialize_bytes(&tup1_none);
    assert_eq!(bytes1.len(), 24);

    // A Tup7 with all None is serialized in the regular rkyv derived version
    // therefor it's size is the size of all the option types (44) aligned
    // to a multiple of 8: 48 bytes
    let tup7_none: Tup7Opt = Tup7Opt::new(None, None, None, None, None, None, None);
    let bytes7 = serialize_bytes(&tup7_none);
    let option_sizes = std::mem::size_of::<Option<i8>>()
        + std::mem::size_of::<Option<i16>>()
        + std::mem::size_of::<Option<i32>>()
        + std::mem::size_of::<Option<i64>>()
        + std::mem::size_of::<Option<u8>>()
        + std::mem::size_of::<Option<u16>>()
        + std::mem::size_of::<Option<u32>>();
    let archived_alignment = std::mem::align_of::<
        ArchivedTup7<
            Option<i8>,
            Option<i16>,
            Option<i32>,
            Option<i64>,
            Option<u8>,
            Option<u16>,
            Option<u32>,
        >,
    >();
    assert_eq!(
        bytes7.len(),
        option_sizes.next_multiple_of(archived_alignment)
    );

    // In comparison here is a Tup12 we have optimized:
    // With InlineSparse format, an all-None tuple is just the
    // envelope header + a 2-byte bitmap (no inline body needed).
    let tup12_none: Tup12Opt = Tup12Opt::new(
        None, None, None, None, None, None, None, None, None, None, None, None,
    );
    let bytes12 = serialize_bytes(&tup12_none);
    let archived_size = core::mem::size_of::<
        ArchivedTup12<
            Option<i8>,
            Option<i16>,
            Option<i32>,
            Option<i64>,
            Option<u8>,
            Option<u16>,
            Option<u32>,
            Option<u64>,
            Option<bool>,
            Option<f32>,
            Option<f64>,
            Option<SqlString>,
        >,
    >();
    // InlineSparse blob is just 2 bytes (bitmap for 12 fields, ceil(12/8) = 2).
    // The envelope is archived_size bytes (format + RawRelPtrI32 + phantom).
    // Plus alignment padding between the blob and the envelope.
    let bitmap_bytes = 12usize.div_ceil(8);
    let envelope_align = core::mem::align_of::<
        ArchivedTup12<
            Option<i8>,
            Option<i16>,
            Option<i32>,
            Option<i64>,
            Option<u8>,
            Option<u16>,
            Option<u32>,
            Option<u64>,
            Option<bool>,
            Option<f32>,
            Option<f64>,
            Option<SqlString>,
        >,
    >();
    let padded_blob = bitmap_bytes.next_multiple_of(envelope_align);
    assert_eq!(bytes12.len(), padded_blob + archived_size);
}

/// Tests that we pick the "right" storage format based on the data
#[test]
fn choose_appropriate_format() {
    let tup8_dense: Tup8Opt = Tup8Opt::new(
        Some(-1),
        Some(-2),
        Some(-3),
        Some(-4),
        Some(1),
        Some(2),
        None,
        None,
    );
    // A Tuple with just a few None's should be stored in the
    // Dense format
    let bytes_dense = serialize_bytes(&tup8_dense);
    assert_eq!(
        archived_format::<Tup8Opt>(&bytes_dense[..]),
        TupleFormat::Dense
    );

    // A Tup with many (>1/3) None's should be stored
    // as Sparse
    let tup8_sparse: Tup8Opt =
        Tup8Opt::new(Some(-1), Some(-2), None, None, None, None, Some(3), Some(4));
    let bytes_sparse = serialize_bytes(&tup8_sparse);
    assert_eq!(
        archived_format::<Tup8Opt>(&bytes_sparse[..]),
        TupleFormat::Sparse
    );
}

#[test]
fn choose_format_tup12() {
    let tup12_all_none = Tup12Opt::new(
        None, None, None, None, None, None, None, None, None, None, None, None,
    );
    let bytes_none = serialize_bytes(&tup12_all_none);
    assert_eq!(
        archived_format::<Tup12Opt>(&bytes_none[..]),
        TupleFormat::InlineSparse
    );

    let tup12_dense = Tup12Opt::new(
        Some(-1),
        Some(-2),
        Some(-3),
        Some(-4),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        None,
        None,
        Some(2.5),
        Some(SqlString::from_ref("dense")),
    );
    let bytes_dense = serialize_bytes(&tup12_dense);
    assert_eq!(
        archived_format::<Tup12Opt>(&bytes_dense[..]),
        TupleFormat::Dense
    );

    let tup12_sparse = Tup12Opt::new(
        Some(-1),
        None,
        Some(-3),
        None,
        Some(1),
        None,
        Some(3),
        None,
        None,
        None,
        None,
        None,
    );
    let bytes_sparse = serialize_bytes(&tup12_sparse);
    assert_eq!(
        archived_format::<Tup12Opt>(&bytes_sparse[..]),
        TupleFormat::InlineSparse
    );
    assert!(bytes_none.len() < bytes_dense.len());
    assert!(bytes_sparse.len() < bytes_dense.len());
}

/// Tests InlineSparse roundtrip with variable-size fields (String).
#[test]
fn inline_sparse_with_strings() {
    // All None — minimal InlineSparse blob.
    let all_none = Tup12Opt::new(
        None, None, None, None, None, None, None, None, None, None, None, None,
    );
    roundtrip(&all_none);

    // Mix of Some and None including the String field.
    let mixed = Tup12Opt::new(
        Some(1),
        None,
        Some(42),
        None,
        None,
        None,
        None,
        None,
        None,
        Some(3.14),
        None,
        Some(SqlString::from_ref("hello inline sparse")),
    );
    roundtrip(&mixed);

    // All fields present.
    let all_some = Tup12Opt::new(
        Some(-8),
        Some(-16),
        Some(-32),
        Some(-64),
        Some(8),
        Some(16),
        Some(32),
        Some(64),
        Some(true),
        Some(1.25),
        Some(2.5),
        Some(SqlString::from_ref("all present")),
    );
    roundtrip(&all_some);

    // Only the String field present, rest None.
    let string_only = Tup12Opt::new(
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(SqlString::from_ref("only string")),
    );
    roundtrip(&string_only);

    // String field is None, some fixed fields present.
    let no_string = Tup12Opt::new(
        Some(10),
        Some(20),
        None,
        None,
        None,
        None,
        None,
        None,
        Some(false),
        None,
        None,
        None,
    );
    roundtrip(&no_string);
}

feldera_macros::declare_tuple! { Tup9<T1, T2, T3, T4, T5, T6, T7, T8, T9> }
feldera_macros::declare_tuple! { Tup10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> }

/// Tests cross-format comparison (Dense vs Sparse, Dense vs InlineSparse, etc.)
/// to exercise envelope walking_read_tN dispatching to different format walkers.
#[test]
fn cross_format_archived_comparison() {
    use core::cmp::Ordering;

    // Tup8 can be Dense or Sparse (never InlineSparse since N < 9).
    // Dense: few Nones; Sparse: many Nones.
    let tup8_dense: Tup8Opt =
        Tup8Opt::new(Some(1), Some(2), Some(3), Some(4), Some(5), Some(6), None, None);
    let tup8_sparse: Tup8Opt =
        Tup8Opt::new(Some(1), Some(2), None, None, None, None, None, None);

    let bytes_dense = serialize_bytes(&tup8_dense);
    let bytes_sparse = serialize_bytes(&tup8_sparse);
    assert_eq!(archived_format::<Tup8Opt>(&bytes_dense[..]), TupleFormat::Dense);
    assert_eq!(archived_format::<Tup8Opt>(&bytes_sparse[..]), TupleFormat::Sparse);

    let ad = unsafe { rkyv::archived_root::<Tup8Opt>(&bytes_dense[..]) };
    let as_ = unsafe { rkyv::archived_root::<Tup8Opt>(&bytes_sparse[..]) };

    // Dense has more non-null fields, so dense > sparse (field t2: Some(3) vs None).
    // None < Some(_) for Option, so sparse < dense.
    assert_eq!(ad.cmp(as_), Ordering::Greater);
    assert_eq!(as_.cmp(ad), Ordering::Less);
    assert!(!ad.eq(as_));

    // Same first two fields → comparison continues past them.
    let tup8_sparse2: Tup8Opt =
        Tup8Opt::new(Some(1), Some(2), None, None, None, None, None, None);
    let bytes_sparse2 = serialize_bytes(&tup8_sparse2);
    let as2 = unsafe { rkyv::archived_root::<Tup8Opt>(&bytes_sparse2[..]) };
    assert!(as_.eq(as2));
    assert_eq!(as_.cmp(as2), Ordering::Equal);

    // Tup12 can be Dense or InlineSparse.
    // Dense: at most 2 Nones out of 12 (< 40%).
    // InlineSparse: 5+ Nones out of 12 (≥ 40%).
    type Tup10Cmp = Tup10<
        Option<i32>, Option<i32>, Option<i32>, Option<i32>, Option<i32>,
        Option<i32>, Option<i32>, Option<i32>, Option<i32>, Option<i32>,
    >;

    let t10_dense = Tup10Cmp::new(
        Some(10), Some(20), Some(30), Some(40), Some(50),
        Some(60), Some(70), Some(80), None, None,
    );
    let t10_is = Tup10Cmp::new(
        Some(10), Some(20), Some(30), Some(40), None,
        None, None, None, None, None,
    );

    let bytes_d10 = serialize_bytes(&t10_dense);
    let bytes_is10 = serialize_bytes(&t10_is);
    assert_eq!(archived_format::<Tup10Cmp>(&bytes_d10[..]), TupleFormat::Dense);
    assert_eq!(archived_format::<Tup10Cmp>(&bytes_is10[..]), TupleFormat::InlineSparse);

    let ad10 = unsafe { rkyv::archived_root::<Tup10Cmp>(&bytes_d10[..]) };
    let ais10 = unsafe { rkyv::archived_root::<Tup10Cmp>(&bytes_is10[..]) };

    // First 4 fields match, then dense has Some(50) vs IS has None.
    // None < Some(50), so IS < Dense.
    assert_eq!(ad10.cmp(ais10), Ordering::Greater);
    assert_eq!(ais10.cmp(ad10), Ordering::Less);
    assert!(!ad10.eq(ais10));

    // Two InlineSparse with different bitmaps but different values.
    let t10_is2 = Tup10Cmp::new(
        Some(10), Some(20), Some(30), Some(40), None,
        None, None, None, None, Some(99),
    );
    let bytes_is2 = serialize_bytes(&t10_is2);
    assert_eq!(archived_format::<Tup10Cmp>(&bytes_is2[..]), TupleFormat::InlineSparse);
    let ais2 = unsafe { rkyv::archived_root::<Tup10Cmp>(&bytes_is2[..]) };

    // Different bitmaps: t10_is has None at t9, t10_is2 has Some(99) at t9.
    // Fields t4..t8 are None in both. t9: None vs Some(99) → IS < IS2.
    assert_eq!(ais10.cmp(ais2), Ordering::Less);
    assert!(!ais10.eq(ais2));

    // IS↔Dense where IS has a larger first field (tests .reverse() in cmp).
    let t10_is_big = Tup10Cmp::new(
        Some(999), None, None, None, None,
        None, None, None, None, None,
    );
    let bytes_is_big = serialize_bytes(&t10_is_big);
    assert_eq!(archived_format::<Tup10Cmp>(&bytes_is_big[..]), TupleFormat::InlineSparse);
    let ais_big = unsafe { rkyv::archived_root::<Tup10Cmp>(&bytes_is_big[..]) };

    // IS(999,...) vs Dense(10,20,...): IS > Dense on first field.
    assert_eq!(ais_big.cmp(ad10), Ordering::Greater);
    assert_eq!(ad10.cmp(ais_big), Ordering::Less);
    assert!(!ais_big.eq(ad10));

    // IS↔Dense where all present fields match but NULL patterns differ.
    let t10_dense_few_none = Tup10Cmp::new(
        Some(10), Some(20), Some(30), Some(40), Some(50),
        Some(60), Some(70), Some(80), Some(90), None,
    );
    let t10_is_match = Tup10Cmp::new(
        Some(10), Some(20), Some(30), Some(40), Some(50),
        Some(60), None, None, None, None,
    );
    let bytes_dfn = serialize_bytes(&t10_dense_few_none);
    let bytes_ism = serialize_bytes(&t10_is_match);
    assert_eq!(archived_format::<Tup10Cmp>(&bytes_dfn[..]), TupleFormat::Dense);
    assert_eq!(archived_format::<Tup10Cmp>(&bytes_ism[..]), TupleFormat::InlineSparse);
    let adfn = unsafe { rkyv::archived_root::<Tup10Cmp>(&bytes_dfn[..]) };
    let aism = unsafe { rkyv::archived_root::<Tup10Cmp>(&bytes_ism[..]) };

    // First 6 fields match. Field t6: Dense has Some(70), IS has None → IS < Dense.
    assert_eq!(aism.cmp(adfn), Ordering::Less);
    assert_eq!(adfn.cmp(aism), Ordering::Greater);
    assert!(!aism.eq(adfn));
}

/// Tests InlineSparse equality and ordering via archived forms.
/// Uses integer-only types (no floats) so Archived impls have Ord.
#[test]
fn inline_sparse_archived_comparison() {
    use core::cmp::Ordering;

    type Tup9Cmp = Tup9<
        Option<i32>, Option<i32>, Option<i32>,
        Option<i32>, Option<i32>, Option<i32>,
        Option<i32>, Option<i32>, Option<SqlString>,
    >;

    let a = Tup9Cmp::new(
        Some(1), None, None, None, None, None, None, None,
        Some(SqlString::from_ref("alpha")),
    );
    let b = Tup9Cmp::new(
        Some(2), None, None, None, None, None, None, None,
        Some(SqlString::from_ref("alpha")),
    );
    let c = Tup9Cmp::new(
        Some(1), None, None, None, None, None, None, None,
        Some(SqlString::from_ref("beta")),
    );

    let bytes_a = serialize_bytes(&a);
    let bytes_b = serialize_bytes(&b);
    let bytes_c = serialize_bytes(&c);

    let archived_a = unsafe { rkyv::archived_root::<Tup9Cmp>(&bytes_a[..]) };
    let archived_b = unsafe { rkyv::archived_root::<Tup9Cmp>(&bytes_b[..]) };
    let archived_c = unsafe { rkyv::archived_root::<Tup9Cmp>(&bytes_c[..]) };

    // a < b because first field 1 < 2.
    assert_eq!(archived_a.cmp(archived_b), Ordering::Less);
    // a < c because same first field but "alpha" < "beta".
    assert_eq!(archived_a.cmp(archived_c), Ordering::Less);
    // a == a
    assert_eq!(archived_a.cmp(archived_a), Ordering::Equal);
    assert!(archived_a.eq(archived_a));
}
