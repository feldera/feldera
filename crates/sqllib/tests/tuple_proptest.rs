use std::collections::BTreeMap;
use std::sync::Arc;

use dbsp::DBData;
use dbsp::algebra::{F32, F64};
use dbsp::dynamic::{DynData, Erase};
use dbsp::storage::backend::memory_impl::MemoryBackend;
use dbsp::storage::buffer_cache::BufferCache;
use dbsp::storage::file::Factories;
use dbsp::storage::file::writer::{Parameters, Writer1};
use feldera_sqllib::{
    Array, ByteArray, Date, GeoPoint, LongInterval, Map, ShortInterval, SqlDecimal, SqlString,
    Time, Timestamp, Uuid, Variant, to_array, to_map,
};
use proptest::prelude::*;

use dbsp::utils::{Tup0, Tup1, Tup2, Tup3, Tup4, Tup5, Tup6, Tup7, Tup8, Tup9, Tup10};
feldera_macros::declare_tuple! { Tup11<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> }
feldera_macros::declare_tuple! { Tup12<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> }
feldera_macros::declare_tuple! { Tup13<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> }
feldera_macros::declare_tuple! { Tup14<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> }
feldera_macros::declare_tuple! { Tup15<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> }
feldera_macros::declare_tuple! { Tup16<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> }
feldera_macros::declare_tuple! { Tup17<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> }
feldera_macros::declare_tuple! { Tup65<
    T1, T2, T3, T4, T5, T6, T7, T8, T9, T10,
    T11, T12, T13, T14, T15, T16, T17, T18, T19, T20,
    T21, T22, T23, T24, T25, T26, T27, T28, T29, T30,
    T31, T32, T33, T34, T35, T36, T37, T38, T39, T40,
    T41, T42, T43, T44, T45, T46, T47, T48, T49, T50,
    T51, T52, T53, T54, T55, T56, T57, T58, T59, T60,
    T61, T62, T63, T64, T65
> }

type Dec12_2 = SqlDecimal<12, 2>;
type Dec10_0 = SqlDecimal<10, 0>;
type Dec18_4 = SqlDecimal<18, 4>;

type Tup0Ty = Tup0;
type Tup1Ty = Tup1<SqlString>;
type Tup2Ty = Tup2<i8, Option<i16>>;
type Tup3Ty = Tup3<SqlString, ByteArray, GeoPoint>;
type Tup4Ty = Tup4<ShortInterval, LongInterval, Timestamp, Date>;
type Tup5Ty = Tup5<Time, Uuid, Variant, Dec12_2, Option<SqlString>>;
type Tup6Ty = Tup6<
    Option<ByteArray>,
    Option<GeoPoint>,
    Option<ShortInterval>,
    Option<LongInterval>,
    Option<Timestamp>,
    Option<Date>,
>;
type Tup7Ty = Tup7<
    Option<Time>,
    Option<Uuid>,
    Option<Variant>,
    Option<Dec12_2>,
    Dec10_0,
    Option<Dec10_0>,
    Dec18_4,
>;
type Tup8Ty = Tup8<
    Option<Dec18_4>,
    Array<SqlString>,
    Option<Array<SqlString>>,
    Map<SqlString, SqlString>,
    Option<Map<SqlString, SqlString>>,
    Vec<u8>,
    u64,
    bool,
>;
type Tup9Ty = Tup9<i8, i16, i32, i64, i128, u8, u16, u32, u128>;
type Tup10Ty =
    Tup10<isize, usize, F32, F64, char, String, Option<bool>, Option<i8>, Option<i16>, Option<i32>>;
type Tup11Ty = Tup11<
    Option<i64>,
    Option<i128>,
    Option<u8>,
    Option<u16>,
    Option<u32>,
    Option<u64>,
    Option<u128>,
    Option<isize>,
    Option<usize>,
    Option<F32>,
    Option<F64>,
>;
type Tup12Ty = Tup12<
    Option<char>,
    Option<String>,
    SqlString,
    ByteArray,
    GeoPoint,
    ShortInterval,
    LongInterval,
    Timestamp,
    Date,
    Time,
    Uuid,
    Variant,
>;
type Tup13Ty = Tup13<
    Dec12_2,
    Option<SqlString>,
    Option<ByteArray>,
    Option<GeoPoint>,
    Option<ShortInterval>,
    Option<LongInterval>,
    Option<Timestamp>,
    Option<Date>,
    Option<Time>,
    Option<Uuid>,
    Option<Variant>,
    Option<Dec12_2>,
    Vec<u8>,
>;
type Tup14Ty = Tup14<
    Dec10_0,
    Option<Dec10_0>,
    Dec18_4,
    Option<Dec18_4>,
    Array<SqlString>,
    Option<Array<SqlString>>,
    Map<SqlString, SqlString>,
    Option<Map<SqlString, SqlString>>,
    u64,
    bool,
    i8,
    i16,
    i32,
    i64,
>;
type Tup15Ty = Tup15<
    i128,
    u8,
    u16,
    u32,
    u128,
    isize,
    usize,
    F32,
    F64,
    char,
    String,
    Option<bool>,
    Option<i8>,
    Option<i16>,
    Option<i32>,
>;
type Tup16Ty = Tup16<
    Option<i64>,
    Option<i128>,
    Option<u8>,
    Option<u16>,
    Option<u32>,
    Option<u64>,
    Option<u128>,
    Option<isize>,
    Option<usize>,
    Option<F32>,
    Option<F64>,
    Option<char>,
    Option<String>,
    SqlString,
    ByteArray,
    GeoPoint,
>;
type Tup17Ty = Tup17<
    ShortInterval,
    LongInterval,
    Timestamp,
    Date,
    Time,
    Uuid,
    Variant,
    Dec12_2,
    Dec10_0,
    Dec18_4,
    Array<SqlString>,
    Map<SqlString, SqlString>,
    Vec<u8>,
    Option<SqlString>,
    Option<ByteArray>,
    Option<GeoPoint>,
    Option<Uuid>,
>;
type Tup65Ty = Tup65<
    u8,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
    Option<u8>,
>;

const PROPTEST_CASES: u32 = 128;
const STORAGE_CASES: u32 = 128;
const STORAGE_MAX_ROWS: usize = 8;
const MAX_STRING_LEN: usize = 4096;
const MAX_BYTES_LEN: usize = 8192;
const MAX_COLLECTION_LEN: usize = 24;

fn roundtrip_eq<T>(value: &T) -> Result<(), TestCaseError>
where
    T: rkyv::Archive
        + rkyv::Serialize<dbsp::storage::file::Serializer>
        + PartialEq
        + core::fmt::Debug,
    rkyv::Archived<T>: rkyv::Deserialize<T, dbsp::storage::file::Deserializer>,
{
    let bytes = dbsp::storage::file::to_bytes(value)
        .map_err(|err| TestCaseError::fail(format!("serialize failed: {err:?}")))?;
    let restored: T = dbsp::trace::unaligned_deserialize(&bytes[..]);
    prop_assert_eq!(value, &restored);
    Ok(())
}

fn roundtrip_all<T>(values: &[T]) -> Result<(), TestCaseError>
where
    T: rkyv::Archive
        + rkyv::Serialize<dbsp::storage::file::Serializer>
        + PartialEq
        + core::fmt::Debug,
    rkyv::Archived<T>: rkyv::Deserialize<T, dbsp::storage::file::Deserializer>,
{
    for value in values {
        roundtrip_eq(value)?;
    }
    Ok(())
}

fn buffer_cache() -> Arc<BufferCache> {
    Arc::new(BufferCache::new(1024 * 1024))
}

fn storage_roundtrip_eq<T>(mut values: Vec<T>) -> Result<(), TestCaseError>
where
    T: DBData + Clone + Ord + Default + Erase<DynData>,
{
    values.sort();
    values.dedup();

    let backend = MemoryBackend::new();
    let factories = Factories::<DynData, DynData>::new::<T, ()>();
    let parameters = Parameters::default();
    let mut writer = Writer1::new(&factories, buffer_cache, &backend, parameters, values.len())
        .map_err(|err| TestCaseError::fail(format!("writer init failed: {err:?}")))?;

    let aux = ();
    for value in &values {
        writer
            .write0((value.erase(), aux.erase()))
            .map_err(|err| TestCaseError::fail(format!("write failed: {err:?}")))?;
    }

    let reader = writer
        .into_reader()
        .map_err(|err| TestCaseError::fail(format!("reader init failed: {err:?}")))?;
    prop_assert_eq!(reader.n_rows(0) as usize, values.len());

    let mut bulk = reader
        .bulk_rows()
        .map_err(|err| TestCaseError::fail(format!("bulk rows failed: {err:?}")))?;

    let mut key_buf = T::default();
    let mut aux_buf = ();
    let mut idx = 0usize;

    while !bulk.at_eof() {
        bulk.wait()
            .map_err(|err| TestCaseError::fail(format!("bulk wait failed: {err:?}")))?;
        let item = unsafe {
            let key_dyn = key_buf.erase_mut();
            let aux_dyn = aux_buf.erase_mut();
            bulk.item((key_dyn, aux_dyn))
        };
        prop_assert!(item.is_some());
        prop_assert_eq!(&key_buf, &values[idx]);
        prop_assert_eq!(aux_buf, ());
        idx += 1;
        bulk.step();
    }

    prop_assert_eq!(idx, values.len());
    Ok(())
}

fn vec_strategy<T>(element: BoxedStrategy<T>) -> BoxedStrategy<Vec<T>>
where
    T: core::fmt::Debug + 'static,
{
    prop::collection::vec(element, 0..=STORAGE_MAX_ROWS).boxed()
}

fn bool_val() -> BoxedStrategy<bool> {
    prop_oneof![Just(false), Just(true)].boxed()
}

fn i8_val() -> BoxedStrategy<i8> {
    prop_oneof![Just(i8::MIN), Just(i8::MAX), Just(0i8), any::<i8>()].boxed()
}

fn i16_val() -> BoxedStrategy<i16> {
    prop_oneof![Just(i16::MIN), Just(i16::MAX), any::<i16>()].boxed()
}

fn i32_val() -> BoxedStrategy<i32> {
    prop_oneof![Just(i32::MIN), Just(i32::MAX), any::<i32>()].boxed()
}

fn i64_val() -> BoxedStrategy<i64> {
    prop_oneof![Just(i64::MIN), Just(i64::MAX), any::<i64>()].boxed()
}

fn i128_val() -> BoxedStrategy<i128> {
    prop_oneof![Just(i128::MIN), Just(i128::MAX), any::<i128>()].boxed()
}

fn isize_val() -> BoxedStrategy<isize> {
    prop_oneof![Just(isize::MIN), Just(isize::MAX), any::<isize>()].boxed()
}

fn u8_val() -> BoxedStrategy<u8> {
    prop_oneof![Just(u8::MIN), Just(u8::MAX), any::<u8>()].boxed()
}

fn u16_val() -> BoxedStrategy<u16> {
    prop_oneof![Just(u16::MIN), Just(u16::MAX), any::<u16>()].boxed()
}

fn u32_val() -> BoxedStrategy<u32> {
    prop_oneof![Just(u32::MIN), Just(u32::MAX), any::<u32>()].boxed()
}

fn u64_val() -> BoxedStrategy<u64> {
    prop_oneof![Just(u64::MIN), Just(u64::MAX), any::<u64>()].boxed()
}

fn u128_val() -> BoxedStrategy<u128> {
    prop_oneof![Just(u128::MIN), Just(u128::MAX), any::<u128>()].boxed()
}

fn usize_val() -> BoxedStrategy<usize> {
    prop_oneof![Just(usize::MIN), Just(usize::MAX), any::<usize>()].boxed()
}

fn opt<T>(inner: T) -> impl Strategy<Value = Option<T::Value>>
where
    T: Strategy,
{
    proptest::option::of(inner)
}

fn string_value() -> BoxedStrategy<String> {
    let max = "X".repeat(MAX_STRING_LEN);
    prop_oneof![
        Just(String::new()),
        Just(max),
        prop::collection::vec(any::<char>(), 0..=MAX_STRING_LEN)
            .prop_map(|v| v.into_iter().collect::<String>()),
    ]
    .boxed()
}

fn sql_string() -> BoxedStrategy<SqlString> {
    string_value().prop_map(SqlString::from).boxed()
}

fn byte_array() -> BoxedStrategy<ByteArray> {
    let max = vec![0xFF; MAX_BYTES_LEN];
    prop_oneof![
        Just(ByteArray::new(&[])),
        Just(ByteArray::new(&max)),
        prop::collection::vec(any::<u8>(), 0..=MAX_BYTES_LEN).prop_map(|v| ByteArray::new(&v)),
    ]
    .boxed()
}

fn f32_val() -> BoxedStrategy<f32> {
    prop_oneof![
        Just(f32::MIN),
        Just(f32::MAX),
        Just(f32::NEG_INFINITY),
        Just(f32::INFINITY),
        Just(f32::NAN),
        any::<f32>(),
    ]
    .boxed()
}

fn f64_val() -> BoxedStrategy<f64> {
    prop_oneof![
        Just(f64::MIN),
        Just(f64::MAX),
        Just(f64::NEG_INFINITY),
        Just(f64::INFINITY),
        Just(f64::NAN),
        any::<f64>(),
    ]
    .boxed()
}

fn f32_wrap() -> BoxedStrategy<F32> {
    f32_val().prop_map(F32::from).boxed()
}

fn f64_wrap() -> BoxedStrategy<F64> {
    f64_val().prop_map(F64::from).boxed()
}

fn geo_point() -> BoxedStrategy<GeoPoint> {
    (f64_val(), f64_val())
        .prop_map(|(x, y)| GeoPoint::new(x, y))
        .boxed()
}

fn short_interval() -> BoxedStrategy<ShortInterval> {
    i64_val().prop_map(ShortInterval::from_microseconds).boxed()
}

fn long_interval() -> BoxedStrategy<LongInterval> {
    i32_val().prop_map(LongInterval::from_months).boxed()
}

fn timestamp() -> BoxedStrategy<Timestamp> {
    i64_val().prop_map(Timestamp::from_microseconds).boxed()
}

fn date() -> BoxedStrategy<Date> {
    i32_val().prop_map(Date::from_days).boxed()
}

fn time() -> BoxedStrategy<Time> {
    u64_val().prop_map(Time::from_nanoseconds).boxed()
}

fn uuid() -> BoxedStrategy<Uuid> {
    let min = [0u8; 16];
    let max = [0xFFu8; 16];
    prop_oneof![
        Just(Uuid::from_bytes(min)),
        Just(Uuid::from_bytes(max)),
        prop::array::uniform16(any::<u8>()).prop_map(Uuid::from_bytes),
    ]
    .boxed()
}

fn dec12_2() -> BoxedStrategy<Dec12_2> {
    decimal_strategy::<12, 2>()
}

fn dec10_0() -> BoxedStrategy<Dec10_0> {
    decimal_strategy::<10, 0>()
}

fn dec18_4() -> BoxedStrategy<Dec18_4> {
    decimal_strategy::<18, 4>()
}

fn decimal_strategy<const P: usize, const S: usize>() -> BoxedStrategy<SqlDecimal<P, S>> {
    let min = SqlDecimal::<P, S>::MIN.mantissa();
    let max = SqlDecimal::<P, S>::MAX.mantissa();
    prop_oneof![
        Just(SqlDecimal::<P, S>::MIN),
        Just(SqlDecimal::<P, S>::MAX),
        (min..=max).prop_map(|value| SqlDecimal::<P, S>::new(value, S as i32).unwrap()),
    ]
    .boxed()
}

fn array_sql_string() -> BoxedStrategy<Array<SqlString>> {
    let max = to_array(
        (0..MAX_COLLECTION_LEN)
            .map(|i| SqlString::from(format!("s{i}")))
            .collect(),
    );
    prop_oneof![
        Just(to_array(Vec::new())),
        Just(max),
        prop::collection::vec(sql_string(), 0..=MAX_COLLECTION_LEN).prop_map(to_array),
    ]
    .boxed()
}

fn map_sql_string() -> BoxedStrategy<Map<SqlString, SqlString>> {
    let mut max_map = BTreeMap::new();
    for i in 0..MAX_COLLECTION_LEN {
        max_map.insert(
            SqlString::from(format!("k{i}")),
            SqlString::from(format!("v{i}")),
        );
    }
    let max = to_map(max_map);
    prop_oneof![
        Just(to_map(BTreeMap::new())),
        Just(max),
        prop::collection::btree_map(sql_string(), sql_string(), 0..=MAX_COLLECTION_LEN)
            .prop_map(to_map),
    ]
    .boxed()
}

fn variant_leaf() -> BoxedStrategy<Variant> {
    prop_oneof![
        Just(Variant::SqlNull),
        Just(Variant::VariantNull),
        bool_val().prop_map(Variant::Boolean),
        i8_val().prop_map(Variant::TinyInt),
        i16_val().prop_map(Variant::SmallInt),
        i32_val().prop_map(Variant::Int),
        i64_val().prop_map(Variant::BigInt),
        u8_val().prop_map(Variant::UTinyInt),
        u16_val().prop_map(Variant::USmallInt),
        u32_val().prop_map(Variant::UInt),
        u64_val().prop_map(Variant::UBigInt),
        f32_wrap().prop_map(Variant::Real),
        f64_wrap().prop_map(Variant::Double),
        sql_string().prop_map(Variant::String),
        date().prop_map(Variant::Date),
        time().prop_map(Variant::Time),
        timestamp().prop_map(Variant::Timestamp),
        short_interval().prop_map(Variant::ShortInterval),
        long_interval().prop_map(Variant::LongInterval),
        byte_array().prop_map(Variant::Binary),
        geo_point().prop_map(Variant::Geometry),
        uuid().prop_map(Variant::Uuid),
        dec12_2().prop_map(Variant::from),
    ]
    .boxed()
}

fn variant() -> BoxedStrategy<Variant> {
    let array = prop::collection::vec(variant_leaf(), 0..=MAX_COLLECTION_LEN)
        .prop_map(|v| Variant::Array(to_array(v)));
    let map = prop::collection::btree_map(variant_leaf(), variant_leaf(), 0..=MAX_COLLECTION_LEN)
        .prop_map(|m| Variant::Map(to_map(m)));
    prop_oneof![variant_leaf(), array, map].boxed()
}

fn char_val() -> BoxedStrategy<char> {
    prop_oneof![Just(char::MIN), Just(char::MAX), any::<char>()].boxed()
}

fn string_val() -> BoxedStrategy<String> {
    string_value()
}

fn vec_u8() -> BoxedStrategy<Vec<u8>> {
    let max = vec![0xFF; MAX_BYTES_LEN];
    prop_oneof![
        Just(Vec::new()),
        Just(max),
        prop::collection::vec(any::<u8>(), 0..=MAX_BYTES_LEN),
    ]
    .boxed()
}

fn tup0_strategy() -> BoxedStrategy<Tup0Ty> {
    Just(Tup0::new()).boxed()
}

fn tup1_strategy() -> BoxedStrategy<Tup1Ty> {
    sql_string().prop_map(Tup1::new).boxed()
}

fn tup2_strategy() -> BoxedStrategy<Tup2Ty> {
    (i8_val(), opt(i16_val()))
        .prop_map(|(a, b)| Tup2::new(a, b))
        .boxed()
}

fn tup3_strategy() -> BoxedStrategy<Tup3Ty> {
    (sql_string(), byte_array(), geo_point())
        .prop_map(|(a, b, c)| Tup3::new(a, b, c))
        .boxed()
}

fn tup4_strategy() -> BoxedStrategy<Tup4Ty> {
    (short_interval(), long_interval(), timestamp(), date())
        .prop_map(|(a, b, c, d)| Tup4::new(a, b, c, d))
        .boxed()
}

fn tup5_strategy() -> BoxedStrategy<Tup5Ty> {
    (time(), uuid(), variant(), dec12_2(), opt(sql_string()))
        .prop_map(|(a, b, c, d, e)| Tup5::new(a, b, c, d, e))
        .boxed()
}

fn tup6_strategy() -> BoxedStrategy<Tup6Ty> {
    (
        opt(byte_array()),
        opt(geo_point()),
        opt(short_interval()),
        opt(long_interval()),
        opt(timestamp()),
        opt(date()),
    )
        .prop_map(|(a, b, c, d, e, f)| Tup6::new(a, b, c, d, e, f))
        .boxed()
}

fn tup7_strategy() -> BoxedStrategy<Tup7Ty> {
    (
        opt(time()),
        opt(uuid()),
        opt(variant()),
        opt(dec12_2()),
        dec10_0(),
        opt(dec10_0()),
        dec18_4(),
    )
        .prop_map(|(a, b, c, d, e, f, g)| Tup7::new(a, b, c, d, e, f, g))
        .boxed()
}

fn tup8_strategy() -> BoxedStrategy<Tup8Ty> {
    (
        opt(dec18_4()),
        array_sql_string(),
        opt(array_sql_string()),
        map_sql_string(),
        opt(map_sql_string()),
        vec_u8(),
        u64_val(),
        bool_val(),
    )
        .prop_map(|(a, b, c, d, e, f, g, h)| Tup8::new(a, b, c, d, e, f, g, h))
        .boxed()
}

fn tup9_strategy() -> BoxedStrategy<Tup9Ty> {
    (
        i8_val(),
        i16_val(),
        i32_val(),
        i64_val(),
        i128_val(),
        u8_val(),
        u16_val(),
        u32_val(),
        u128_val(),
    )
        .prop_map(|(a, b, c, d, e, f, g, h, i)| Tup9::new(a, b, c, d, e, f, g, h, i))
        .boxed()
}

fn tup10_strategy() -> BoxedStrategy<Tup10Ty> {
    (
        isize_val(),
        usize_val(),
        f32_wrap(),
        f64_wrap(),
        char_val(),
        string_val(),
        opt(bool_val()),
        opt(i8_val()),
        opt(i16_val()),
        opt(i32_val()),
    )
        .prop_map(|(a, b, c, d, e, f, g, h, i, j)| Tup10::new(a, b, c, d, e, f, g, h, i, j))
        .boxed()
}

fn tup11_strategy() -> BoxedStrategy<Tup11Ty> {
    (
        opt(i64_val()),
        opt(i128_val()),
        opt(u8_val()),
        opt(u16_val()),
        opt(u32_val()),
        opt(u64_val()),
        opt(u128_val()),
        opt(isize_val()),
        opt(usize_val()),
        opt(f32_wrap()),
        opt(f64_wrap()),
    )
        .prop_map(|(a, b, c, d, e, f, g, h, i, j, k)| Tup11::new(a, b, c, d, e, f, g, h, i, j, k))
        .boxed()
}

fn tup12_strategy() -> BoxedStrategy<Tup12Ty> {
    (
        opt(char_val()),
        opt(string_val()),
        sql_string(),
        byte_array(),
        geo_point(),
        short_interval(),
        long_interval(),
        timestamp(),
        date(),
        time(),
        uuid(),
        variant(),
    )
        .prop_map(|(a, b, c, d, e, f, g, h, i, j, k, l)| {
            Tup12::new(a, b, c, d, e, f, g, h, i, j, k, l)
        })
        .boxed()
}

fn tup13_strategy() -> BoxedStrategy<Tup13Ty> {
    let head = (
        dec12_2(),
        opt(sql_string()),
        opt(byte_array()),
        opt(geo_point()),
        opt(short_interval()),
        opt(long_interval()),
        opt(timestamp()),
        opt(date()),
        opt(time()),
        opt(uuid()),
        opt(variant()),
        opt(dec12_2()),
    );
    (head, vec_u8())
        .prop_map(|(head, m)| {
            let (a, b, c, d, e, f, g, h, i, j, k, l) = head;
            Tup13::new(a, b, c, d, e, f, g, h, i, j, k, l, m)
        })
        .boxed()
}

fn tup14_strategy() -> BoxedStrategy<Tup14Ty> {
    let head = (
        dec10_0(),
        opt(dec10_0()),
        dec18_4(),
        opt(dec18_4()),
        array_sql_string(),
        opt(array_sql_string()),
        map_sql_string(),
        opt(map_sql_string()),
        u64_val(),
        bool_val(),
        i8_val(),
        i16_val(),
    );
    (head, i32_val(), i64_val())
        .prop_map(|(head, m, n)| {
            let (a, b, c, d, e, f, g, h, i, j, k, l) = head;
            Tup14::new(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
        })
        .boxed()
}

fn tup15_strategy() -> BoxedStrategy<Tup15Ty> {
    let head = (
        i128_val(),
        u8_val(),
        u16_val(),
        u32_val(),
        u128_val(),
        isize_val(),
        usize_val(),
        f32_wrap(),
        f64_wrap(),
        char_val(),
        string_val(),
        opt(bool_val()),
    );
    (head, opt(i8_val()), opt(i16_val()), opt(i32_val()))
        .prop_map(|(head, m, n, o)| {
            let (a, b, c, d, e, f, g, h, i, j, k, l) = head;
            Tup15::new(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
        })
        .boxed()
}

fn tup16_strategy() -> BoxedStrategy<Tup16Ty> {
    let head = (
        opt(i64_val()),
        opt(i128_val()),
        opt(u8_val()),
        opt(u16_val()),
        opt(u32_val()),
        opt(u64_val()),
        opt(u128_val()),
        opt(isize_val()),
        opt(usize_val()),
        opt(f32_wrap()),
        opt(f64_wrap()),
        opt(char_val()),
    );
    (
        head,
        opt(string_val()),
        sql_string(),
        byte_array(),
        geo_point(),
    )
        .prop_map(|(head, m, n, o, p)| {
            let (a, b, c, d, e, f, g, h, i, j, k, l) = head;
            Tup16::new(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
        })
        .boxed()
}

fn tup17_strategy() -> BoxedStrategy<Tup17Ty> {
    let head = (
        short_interval(),
        long_interval(),
        timestamp(),
        date(),
        time(),
        uuid(),
        variant(),
        dec12_2(),
        dec10_0(),
        dec18_4(),
        array_sql_string(),
        map_sql_string(),
    );
    (
        head,
        vec_u8(),
        opt(sql_string()),
        opt(byte_array()),
        opt(geo_point()),
        opt(uuid()),
    )
        .prop_map(|(head, m, n, o, p, q)| {
            let (a, b, c, d, e, f, g, h, i, j, k, l) = head;
            Tup17::new(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
        })
        .boxed()
}

fn tup65_strategy() -> BoxedStrategy<Tup65Ty> {
    (u8_val(), prop::collection::vec(opt(u8_val()), 64))
        .prop_map(|(first, opts)| {
            Tup65::new(
                first, opts[0], opts[1], opts[2], opts[3], opts[4], opts[5], opts[6], opts[7],
                opts[8], opts[9], opts[10], opts[11], opts[12], opts[13], opts[14], opts[15],
                opts[16], opts[17], opts[18], opts[19], opts[20], opts[21], opts[22], opts[23],
                opts[24], opts[25], opts[26], opts[27], opts[28], opts[29], opts[30], opts[31],
                opts[32], opts[33], opts[34], opts[35], opts[36], opts[37], opts[38], opts[39],
                opts[40], opts[41], opts[42], opts[43], opts[44], opts[45], opts[46], opts[47],
                opts[48], opts[49], opts[50], opts[51], opts[52], opts[53], opts[54], opts[55],
                opts[56], opts[57], opts[58], opts[59], opts[60], opts[61], opts[62], opts[63],
            )
        })
        .boxed()
}

fn min_string() -> String {
    String::new()
}

fn max_string() -> String {
    "X".repeat(MAX_STRING_LEN)
}

fn min_sql_string() -> SqlString {
    SqlString::from(min_string())
}

fn max_sql_string() -> SqlString {
    SqlString::from(max_string())
}

fn min_byte_array() -> ByteArray {
    ByteArray::new(&[])
}

fn max_byte_array() -> ByteArray {
    ByteArray::new(&vec![0xFF; MAX_BYTES_LEN])
}

fn min_vec_u8() -> Vec<u8> {
    Vec::new()
}

fn max_vec_u8() -> Vec<u8> {
    vec![0xFF; MAX_BYTES_LEN]
}

fn min_geo_point() -> GeoPoint {
    GeoPoint::new(f64::MIN, f64::MIN)
}

fn max_geo_point() -> GeoPoint {
    GeoPoint::new(f64::MAX, f64::MAX)
}

fn min_uuid() -> Uuid {
    Uuid::from_bytes([0u8; 16])
}

fn max_uuid() -> Uuid {
    Uuid::from_bytes([0xFFu8; 16])
}

fn min_array_sql_string() -> Array<SqlString> {
    to_array(Vec::new())
}

fn max_array_sql_string() -> Array<SqlString> {
    to_array(
        (0..MAX_COLLECTION_LEN)
            .map(|i| SqlString::from(format!("s{i}")))
            .collect(),
    )
}

fn min_map_sql_string() -> Map<SqlString, SqlString> {
    to_map(BTreeMap::new())
}

fn max_map_sql_string() -> Map<SqlString, SqlString> {
    let mut map = BTreeMap::new();
    for i in 0..MAX_COLLECTION_LEN {
        map.insert(
            SqlString::from(format!("k{i}")),
            SqlString::from(format!("v{i}")),
        );
    }
    to_map(map)
}

fn min_variant() -> Variant {
    Variant::SqlNull
}

fn max_variant() -> Variant {
    Variant::Double(F64::new(f64::MAX))
}

fn nan_variant() -> Variant {
    Variant::Double(F64::new(f64::NAN))
}

fn min_f32() -> F32 {
    F32::new(f32::MIN)
}

fn max_f32() -> F32 {
    F32::new(f32::MAX)
}

fn nan_f32() -> F32 {
    F32::new(f32::NAN)
}

fn min_f64() -> F64 {
    F64::new(f64::MIN)
}

fn max_f64() -> F64 {
    F64::new(f64::MAX)
}

fn nan_f64() -> F64 {
    F64::new(f64::NAN)
}

fn edge_values_tup0() -> Vec<Tup0Ty> {
    vec![Tup0::new()]
}

fn edge_values_tup1() -> Vec<Tup1Ty> {
    vec![Tup1::new(min_sql_string()), Tup1::new(max_sql_string())]
}

fn edge_values_tup2() -> Vec<Tup2Ty> {
    vec![Tup2::new(i8::MIN, None), Tup2::new(i8::MAX, Some(i16::MAX))]
}

fn edge_values_tup3() -> Vec<Tup3Ty> {
    vec![
        Tup3::new(min_sql_string(), min_byte_array(), min_geo_point()),
        Tup3::new(max_sql_string(), max_byte_array(), max_geo_point()),
    ]
}

fn edge_values_tup4() -> Vec<Tup4Ty> {
    vec![
        Tup4::new(
            ShortInterval::from_microseconds(i64::MIN),
            LongInterval::from_months(i32::MIN),
            Timestamp::from_microseconds(i64::MIN),
            Date::from_days(i32::MIN),
        ),
        Tup4::new(
            ShortInterval::from_microseconds(i64::MAX),
            LongInterval::from_months(i32::MAX),
            Timestamp::from_microseconds(i64::MAX),
            Date::from_days(i32::MAX),
        ),
    ]
}

fn edge_values_tup5() -> Vec<Tup5Ty> {
    vec![
        Tup5::new(
            Time::from_nanoseconds(u64::MIN),
            min_uuid(),
            min_variant(),
            Dec12_2::MIN,
            None,
        ),
        Tup5::new(
            Time::from_nanoseconds(u64::MAX),
            max_uuid(),
            max_variant(),
            Dec12_2::MAX,
            Some(max_sql_string()),
        ),
        Tup5::new(
            Time::from_nanoseconds(0),
            min_uuid(),
            nan_variant(),
            Dec12_2::ZERO,
            Some(min_sql_string()),
        ),
    ]
}

fn edge_values_tup6() -> Vec<Tup6Ty> {
    vec![
        Tup6::new(None, None, None, None, None, None),
        Tup6::new(
            Some(max_byte_array()),
            Some(max_geo_point()),
            Some(ShortInterval::from_microseconds(i64::MAX)),
            Some(LongInterval::from_months(i32::MAX)),
            Some(Timestamp::from_microseconds(i64::MAX)),
            Some(Date::from_days(i32::MAX)),
        ),
    ]
}

fn edge_values_tup7() -> Vec<Tup7Ty> {
    vec![
        Tup7::new(None, None, None, None, Dec10_0::MIN, None, Dec18_4::MIN),
        Tup7::new(
            Some(Time::from_nanoseconds(u64::MAX)),
            Some(max_uuid()),
            Some(max_variant()),
            Some(Dec12_2::MAX),
            Dec10_0::MAX,
            Some(Dec10_0::MAX),
            Dec18_4::MAX,
        ),
    ]
}

fn edge_values_tup8() -> Vec<Tup8Ty> {
    vec![
        Tup8::new(
            None,
            min_array_sql_string(),
            None,
            min_map_sql_string(),
            None,
            min_vec_u8(),
            u64::MIN,
            false,
        ),
        Tup8::new(
            Some(Dec18_4::MAX),
            max_array_sql_string(),
            Some(max_array_sql_string()),
            max_map_sql_string(),
            Some(max_map_sql_string()),
            max_vec_u8(),
            u64::MAX,
            true,
        ),
    ]
}

fn edge_values_tup9() -> Vec<Tup9Ty> {
    vec![
        Tup9::new(
            i8::MIN,
            i16::MIN,
            i32::MIN,
            i64::MIN,
            i128::MIN,
            u8::MIN,
            u16::MIN,
            u32::MIN,
            u128::MIN,
        ),
        Tup9::new(
            i8::MAX,
            i16::MAX,
            i32::MAX,
            i64::MAX,
            i128::MAX,
            u8::MAX,
            u16::MAX,
            u32::MAX,
            u128::MAX,
        ),
    ]
}

fn edge_values_tup10() -> Vec<Tup10Ty> {
    vec![
        Tup10::new(
            isize::MIN,
            usize::MIN,
            min_f32(),
            min_f64(),
            char::MIN,
            min_string(),
            None,
            None,
            None,
            None,
        ),
        Tup10::new(
            isize::MAX,
            usize::MAX,
            max_f32(),
            max_f64(),
            char::MAX,
            max_string(),
            Some(true),
            Some(i8::MAX),
            Some(i16::MAX),
            Some(i32::MAX),
        ),
        Tup10::new(
            0,
            0,
            nan_f32(),
            nan_f64(),
            char::MIN,
            min_string(),
            Some(false),
            Some(0),
            Some(0),
            Some(0),
        ),
    ]
}

fn edge_values_tup11() -> Vec<Tup11Ty> {
    vec![
        Tup11::new(
            None, None, None, None, None, None, None, None, None, None, None,
        ),
        Tup11::new(
            Some(i64::MAX),
            Some(i128::MAX),
            Some(u8::MAX),
            Some(u16::MAX),
            Some(u32::MAX),
            Some(u64::MAX),
            Some(u128::MAX),
            Some(isize::MAX),
            Some(usize::MAX),
            Some(max_f32()),
            Some(max_f64()),
        ),
        Tup11::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(nan_f32()),
            Some(nan_f64()),
        ),
    ]
}

fn edge_values_tup12() -> Vec<Tup12Ty> {
    vec![
        Tup12::new(
            None,
            None,
            min_sql_string(),
            min_byte_array(),
            min_geo_point(),
            ShortInterval::from_microseconds(i64::MIN),
            LongInterval::from_months(i32::MIN),
            Timestamp::from_microseconds(i64::MIN),
            Date::from_days(i32::MIN),
            Time::from_nanoseconds(u64::MIN),
            min_uuid(),
            min_variant(),
        ),
        Tup12::new(
            Some(char::MAX),
            Some(max_string()),
            max_sql_string(),
            max_byte_array(),
            max_geo_point(),
            ShortInterval::from_microseconds(i64::MAX),
            LongInterval::from_months(i32::MAX),
            Timestamp::from_microseconds(i64::MAX),
            Date::from_days(i32::MAX),
            Time::from_nanoseconds(u64::MAX),
            max_uuid(),
            max_variant(),
        ),
    ]
}

fn edge_values_tup13() -> Vec<Tup13Ty> {
    vec![
        Tup13::new(
            Dec12_2::MIN,
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
            min_vec_u8(),
        ),
        Tup13::new(
            Dec12_2::MAX,
            Some(max_sql_string()),
            Some(max_byte_array()),
            Some(max_geo_point()),
            Some(ShortInterval::from_microseconds(i64::MAX)),
            Some(LongInterval::from_months(i32::MAX)),
            Some(Timestamp::from_microseconds(i64::MAX)),
            Some(Date::from_days(i32::MAX)),
            Some(Time::from_nanoseconds(u64::MAX)),
            Some(max_uuid()),
            Some(max_variant()),
            Some(Dec12_2::MAX),
            max_vec_u8(),
        ),
    ]
}

fn edge_values_tup14() -> Vec<Tup14Ty> {
    vec![
        Tup14::new(
            Dec10_0::MIN,
            None,
            Dec18_4::MIN,
            None,
            min_array_sql_string(),
            None,
            min_map_sql_string(),
            None,
            u64::MIN,
            false,
            i8::MIN,
            i16::MIN,
            i32::MIN,
            i64::MIN,
        ),
        Tup14::new(
            Dec10_0::MAX,
            Some(Dec10_0::MAX),
            Dec18_4::MAX,
            Some(Dec18_4::MAX),
            max_array_sql_string(),
            Some(max_array_sql_string()),
            max_map_sql_string(),
            Some(max_map_sql_string()),
            u64::MAX,
            true,
            i8::MAX,
            i16::MAX,
            i32::MAX,
            i64::MAX,
        ),
    ]
}

fn edge_values_tup15() -> Vec<Tup15Ty> {
    vec![
        Tup15::new(
            i128::MIN,
            u8::MIN,
            u16::MIN,
            u32::MIN,
            u128::MIN,
            isize::MIN,
            usize::MIN,
            min_f32(),
            min_f64(),
            char::MIN,
            min_string(),
            None,
            None,
            None,
            None,
        ),
        Tup15::new(
            i128::MAX,
            u8::MAX,
            u16::MAX,
            u32::MAX,
            u128::MAX,
            isize::MAX,
            usize::MAX,
            max_f32(),
            max_f64(),
            char::MAX,
            max_string(),
            Some(true),
            Some(i8::MAX),
            Some(i16::MAX),
            Some(i32::MAX),
        ),
        Tup15::new(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            nan_f32(),
            nan_f64(),
            char::MIN,
            min_string(),
            Some(false),
            Some(0),
            Some(0),
            Some(0),
        ),
    ]
}

fn edge_values_tup16() -> Vec<Tup16Ty> {
    vec![
        Tup16::new(
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
            None,
            None,
            min_sql_string(),
            min_byte_array(),
            min_geo_point(),
        ),
        Tup16::new(
            Some(i64::MAX),
            Some(i128::MAX),
            Some(u8::MAX),
            Some(u16::MAX),
            Some(u32::MAX),
            Some(u64::MAX),
            Some(u128::MAX),
            Some(isize::MAX),
            Some(usize::MAX),
            Some(max_f32()),
            Some(max_f64()),
            Some(char::MAX),
            Some(max_string()),
            max_sql_string(),
            max_byte_array(),
            max_geo_point(),
        ),
        Tup16::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(nan_f32()),
            Some(nan_f64()),
            None,
            None,
            min_sql_string(),
            min_byte_array(),
            min_geo_point(),
        ),
    ]
}

fn edge_values_tup17() -> Vec<Tup17Ty> {
    vec![
        Tup17::new(
            ShortInterval::from_microseconds(i64::MIN),
            LongInterval::from_months(i32::MIN),
            Timestamp::from_microseconds(i64::MIN),
            Date::from_days(i32::MIN),
            Time::from_nanoseconds(u64::MIN),
            min_uuid(),
            min_variant(),
            Dec12_2::MIN,
            Dec10_0::MIN,
            Dec18_4::MIN,
            min_array_sql_string(),
            min_map_sql_string(),
            min_vec_u8(),
            None,
            None,
            None,
            None,
        ),
        Tup17::new(
            ShortInterval::from_microseconds(i64::MAX),
            LongInterval::from_months(i32::MAX),
            Timestamp::from_microseconds(i64::MAX),
            Date::from_days(i32::MAX),
            Time::from_nanoseconds(u64::MAX),
            max_uuid(),
            max_variant(),
            Dec12_2::MAX,
            Dec10_0::MAX,
            Dec18_4::MAX,
            max_array_sql_string(),
            max_map_sql_string(),
            max_vec_u8(),
            Some(max_sql_string()),
            Some(max_byte_array()),
            Some(max_geo_point()),
            Some(max_uuid()),
        ),
    ]
}

fn edge_values_tup65() -> Vec<Tup65Ty> {
    vec![
        Tup65::new(
            u8::MIN,
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
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        Tup65::new(
            u8::MAX,
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
            Some(u8::MAX),
        ),
        Tup65::new(
            0,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
            Some(u8::MIN),
            None,
        ),
    ]
}

macro_rules! tuple_roundtrip_test {
    ($name:ident, $ty:ty, $strategy:expr) => {
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(PROPTEST_CASES))]
            #[test]
            fn $name(value in $strategy) {
                roundtrip_eq::<$ty>(&value)?;
            }
        }
    };
}

macro_rules! tuple_storage_roundtrip_test {
    ($name:ident, $ty:ty, $strategy:expr) => {
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(STORAGE_CASES))]
            #[test]
            fn $name(values in $strategy) {
                storage_roundtrip_eq::<$ty>(values)?;
            }
        }
    };
}

tuple_roundtrip_test!(tup0_roundtrip, Tup0Ty, tup0_strategy());
tuple_roundtrip_test!(tup1_roundtrip, Tup1Ty, tup1_strategy());
tuple_roundtrip_test!(tup2_roundtrip, Tup2Ty, tup2_strategy());
tuple_roundtrip_test!(tup3_roundtrip, Tup3Ty, tup3_strategy());
tuple_roundtrip_test!(tup4_roundtrip, Tup4Ty, tup4_strategy());
tuple_roundtrip_test!(tup5_roundtrip, Tup5Ty, tup5_strategy());
tuple_roundtrip_test!(tup6_roundtrip, Tup6Ty, tup6_strategy());
tuple_roundtrip_test!(tup7_roundtrip, Tup7Ty, tup7_strategy());
tuple_roundtrip_test!(tup8_roundtrip, Tup8Ty, tup8_strategy());
tuple_roundtrip_test!(tup9_roundtrip, Tup9Ty, tup9_strategy());
tuple_roundtrip_test!(tup10_roundtrip, Tup10Ty, tup10_strategy());
tuple_roundtrip_test!(tup11_roundtrip, Tup11Ty, tup11_strategy());
tuple_roundtrip_test!(tup12_roundtrip, Tup12Ty, tup12_strategy());
tuple_roundtrip_test!(tup13_roundtrip, Tup13Ty, tup13_strategy());
tuple_roundtrip_test!(tup14_roundtrip, Tup14Ty, tup14_strategy());
tuple_roundtrip_test!(tup15_roundtrip, Tup15Ty, tup15_strategy());
tuple_roundtrip_test!(tup16_roundtrip, Tup16Ty, tup16_strategy());
tuple_roundtrip_test!(tup17_roundtrip, Tup17Ty, tup17_strategy());
tuple_roundtrip_test!(tup65_roundtrip, Tup65Ty, tup65_strategy());

tuple_storage_roundtrip_test!(
    tup0_storage_roundtrip,
    Tup0Ty,
    vec_strategy(tup0_strategy())
);
tuple_storage_roundtrip_test!(
    tup1_storage_roundtrip,
    Tup1Ty,
    vec_strategy(tup1_strategy())
);
tuple_storage_roundtrip_test!(
    tup2_storage_roundtrip,
    Tup2Ty,
    vec_strategy(tup2_strategy())
);
tuple_storage_roundtrip_test!(
    tup3_storage_roundtrip,
    Tup3Ty,
    vec_strategy(tup3_strategy())
);
tuple_storage_roundtrip_test!(
    tup4_storage_roundtrip,
    Tup4Ty,
    vec_strategy(tup4_strategy())
);
tuple_storage_roundtrip_test!(
    tup5_storage_roundtrip,
    Tup5Ty,
    vec_strategy(tup5_strategy())
);
tuple_storage_roundtrip_test!(
    tup6_storage_roundtrip,
    Tup6Ty,
    vec_strategy(tup6_strategy())
);
tuple_storage_roundtrip_test!(
    tup7_storage_roundtrip,
    Tup7Ty,
    vec_strategy(tup7_strategy())
);
tuple_storage_roundtrip_test!(
    tup8_storage_roundtrip,
    Tup8Ty,
    vec_strategy(tup8_strategy())
);
tuple_storage_roundtrip_test!(
    tup9_storage_roundtrip,
    Tup9Ty,
    vec_strategy(tup9_strategy())
);
tuple_storage_roundtrip_test!(
    tup10_storage_roundtrip,
    Tup10Ty,
    vec_strategy(tup10_strategy())
);
tuple_storage_roundtrip_test!(
    tup11_storage_roundtrip,
    Tup11Ty,
    vec_strategy(tup11_strategy())
);
tuple_storage_roundtrip_test!(
    tup12_storage_roundtrip,
    Tup12Ty,
    vec_strategy(tup12_strategy())
);
tuple_storage_roundtrip_test!(
    tup13_storage_roundtrip,
    Tup13Ty,
    vec_strategy(tup13_strategy())
);
tuple_storage_roundtrip_test!(
    tup14_storage_roundtrip,
    Tup14Ty,
    vec_strategy(tup14_strategy())
);
tuple_storage_roundtrip_test!(
    tup15_storage_roundtrip,
    Tup15Ty,
    vec_strategy(tup15_strategy())
);
tuple_storage_roundtrip_test!(
    tup16_storage_roundtrip,
    Tup16Ty,
    vec_strategy(tup16_strategy())
);
tuple_storage_roundtrip_test!(
    tup17_storage_roundtrip,
    Tup17Ty,
    vec_strategy(tup17_strategy())
);
tuple_storage_roundtrip_test!(
    tup65_storage_roundtrip,
    Tup65Ty,
    vec_strategy(tup65_strategy())
);

#[test]
fn edge_case_roundtrip() -> Result<(), TestCaseError> {
    roundtrip_all(&edge_values_tup0())?;
    roundtrip_all(&edge_values_tup1())?;
    roundtrip_all(&edge_values_tup2())?;
    roundtrip_all(&edge_values_tup3())?;
    roundtrip_all(&edge_values_tup4())?;
    roundtrip_all(&edge_values_tup5())?;
    roundtrip_all(&edge_values_tup6())?;
    roundtrip_all(&edge_values_tup7())?;
    roundtrip_all(&edge_values_tup8())?;
    roundtrip_all(&edge_values_tup9())?;
    roundtrip_all(&edge_values_tup10())?;
    roundtrip_all(&edge_values_tup11())?;
    roundtrip_all(&edge_values_tup12())?;
    roundtrip_all(&edge_values_tup13())?;
    roundtrip_all(&edge_values_tup14())?;
    roundtrip_all(&edge_values_tup15())?;
    roundtrip_all(&edge_values_tup16())?;
    roundtrip_all(&edge_values_tup17())?;
    roundtrip_all(&edge_values_tup65())?;
    Ok(())
}

#[test]
fn edge_case_storage_roundtrip_small() -> Result<(), TestCaseError> {
    storage_roundtrip_eq(edge_values_tup0())?;
    storage_roundtrip_eq(edge_values_tup1())?;
    storage_roundtrip_eq(edge_values_tup2())?;
    storage_roundtrip_eq(edge_values_tup3())?;
    storage_roundtrip_eq(edge_values_tup4())?;
    storage_roundtrip_eq(edge_values_tup5())?;
    storage_roundtrip_eq(edge_values_tup6())?;
    storage_roundtrip_eq(edge_values_tup7())?;
    storage_roundtrip_eq(edge_values_tup8())?;
    storage_roundtrip_eq(edge_values_tup9())?;
    storage_roundtrip_eq(edge_values_tup10())?;
    Ok(())
}

#[test]
fn edge_case_storage_roundtrip_medium() -> Result<(), TestCaseError> {
    storage_roundtrip_eq(edge_values_tup11())?;
    storage_roundtrip_eq(edge_values_tup12())?;
    storage_roundtrip_eq(edge_values_tup13())?;
    storage_roundtrip_eq(edge_values_tup14())?;
    storage_roundtrip_eq(edge_values_tup15())?;
    storage_roundtrip_eq(edge_values_tup16())?;
    storage_roundtrip_eq(edge_values_tup17())?;
    Ok(())
}

#[test]
fn edge_case_storage_roundtrip_large() -> Result<(), TestCaseError> {
    storage_roundtrip_eq(edge_values_tup65())?;
    Ok(())
}
