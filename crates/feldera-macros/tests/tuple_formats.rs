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
    // the size is the header plus the sparse payload and doesn't include any fields.
    let tup12_none: Tup12Opt =
        Tup12Opt::new(None, None, None, None, None, None, None, None, None, None, None, None);
    let bytes12 = serialize_bytes(&tup12_none);
    let archived_sparse_size = core::mem::size_of::<
        ArchivedTup12Sparse<
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
    assert_eq!(bytes12.len(), archived_size + archived_sparse_size);
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
        TupleFormat::Sparse
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
        TupleFormat::Sparse
    );
    assert!(bytes_none.len() < bytes_dense.len());
    assert!(bytes_sparse.len() < bytes_dense.len());
}
