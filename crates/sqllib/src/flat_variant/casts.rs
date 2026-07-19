//! The `FV` runtime function grid for [`FlatVariant`]: casts, indexing, and the
//! JSON functions the SQL compiler emits when the flat_variant mode is on.
//!
//! Every function delegates through the enum [`Variant`] implementation, so
//! cast semantics (string fallbacks, numeric coercion, error text) stay
//! identical to the enum grid by construction. Casts are cold next to
//! parse/store, so the conversion cost is acceptable; hot paths (parsing,
//! storage, indexing) run natively on the flat encoding.

use std::error::Error;

use crate::casts::*;
use crate::error::{SqlResult, SqlRuntimeError};
use crate::flat_variant::FlatVariant;
use crate::variant::Variant;
use crate::{Array, ByteArray, Map, SqlDecimal, SqlString};

/// `Ok(t)` to `Ok(Some(t))`, like `r2o` in casts.rs.
fn r2o<T>(result: SqlResult<T>) -> SqlResult<Option<T>> {
    result.map(Some)
}

// Scalar casts, delegating through the enum grid

macro_rules! cast_flat_variant {
    ($name: ident $(< $( const $var:ident : $ty: ty),* >)?, $type: ty) => {
        ::paste::paste! {
            // cast_to_FV_i32
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ FV_ $name >] $(< $( const $var : $ty),* >)? ( value: $type ) -> SqlResult<FlatVariant> {
                Ok(FlatVariant::from(&[<cast_to_ V_ $name >] $(:: < $($var),* >)? (value)?))
            }

            // cast_to_FVN_i32
            #[doc(hidden)]
            pub fn [<cast_to_ FVN_ $name >] $(< $( const $var : $ty),* >)? ( value: $type ) -> SqlResult<Option<FlatVariant>> {
                r2o([<cast_to_ FV_ $name >] $(:: < $($var),* >)? (value))
            }

            // cast_to_FV_i32N
            #[doc(hidden)]
            pub fn [<cast_to_ FV_ $name N>] $(< $( const $var : $ty),* >)? ( value: Option<$type> ) -> SqlResult<FlatVariant> {
                Ok(FlatVariant::from(&[<cast_to_ V_ $name N>] $(:: < $($var),* >)? (value)?))
            }

            // cast_to_FVN_i32N
            #[doc(hidden)]
            pub fn [<cast_to_ FVN_ $name N>] $(< $( const $var : $ty),* >)? ( value: Option<$type> ) -> SqlResult<Option<FlatVariant>> {
                r2o([<cast_to_ FV_ $name N>] $(:: < $($var),* >)? (value))
            }
        }
    };
}

macro_rules! cast_from_flat_variant {
    ($name: ident, $type: ty) => {
        ::paste::paste! {
            // cast_to_i32N_FV
            #[doc(hidden)]
            pub fn [< cast_to_ $name N _FV >](value: FlatVariant) -> SqlResult<Option<$type>> {
                [< cast_to_ $name N _V >](Variant::from(&value))
            }

            // cast_to_i32N_FVN
            #[doc(hidden)]
            pub fn [<cast_to_ $name N_ FVN >]( value: Option<FlatVariant> ) -> SqlResult<Option<$type>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_ $name N_FV >](value),
                }
            }
        }
    };
}

macro_rules! cast_flat_variant_both {
    ($name: ident, $type: ty) => {
        cast_flat_variant!($name, $type);
        cast_from_flat_variant!($name, $type);
    };
}

cast_flat_variant_both!(b, bool);
cast_flat_variant_both!(i8, i8);
cast_flat_variant_both!(i16, i16);
cast_flat_variant_both!(i32, i32);
cast_flat_variant_both!(i64, i64);
cast_flat_variant_both!(u8, u8);
cast_flat_variant_both!(u16, u16);
cast_flat_variant_both!(u32, u32);
cast_flat_variant_both!(u64, u64);
cast_flat_variant_both!(f, crate::F32);
cast_flat_variant_both!(d, crate::F64);
cast_flat_variant!(SqlDecimal<const P: usize, const S: usize>, SqlDecimal<P, S>);
cast_flat_variant!(s, SqlString);
cast_flat_variant!(bytes, ByteArray);
cast_flat_variant_both!(Date, crate::Date);
cast_flat_variant_both!(Time, crate::Time);
cast_flat_variant_both!(Uuid, crate::Uuid);
cast_flat_variant_both!(Timestamp, crate::Timestamp);
cast_flat_variant_both!(TimestampTz, crate::TimestampTz);
cast_flat_variant_both!(ShortInterval_DAYS, crate::ShortInterval);
cast_flat_variant_both!(ShortInterval_HOURS, crate::ShortInterval);
cast_flat_variant_both!(ShortInterval_DAYS_TO_HOURS, crate::ShortInterval);
cast_flat_variant_both!(ShortInterval_MINUTES, crate::ShortInterval);
cast_flat_variant_both!(ShortInterval_DAYS_TO_MINUTES, crate::ShortInterval);
cast_flat_variant_both!(ShortInterval_HOURS_TO_MINUTES, crate::ShortInterval);
cast_flat_variant_both!(ShortInterval_SECONDS, crate::ShortInterval);
cast_flat_variant_both!(ShortInterval_DAYS_TO_SECONDS, crate::ShortInterval);
cast_flat_variant_both!(ShortInterval_HOURS_TO_SECONDS, crate::ShortInterval);
cast_flat_variant_both!(ShortInterval_MINUTES_TO_SECONDS, crate::ShortInterval);
cast_flat_variant_both!(LongInterval_YEARS_TO_MONTHS, crate::LongInterval);
cast_flat_variant_both!(LongInterval_MONTHS, crate::LongInterval);
cast_flat_variant_both!(LongInterval_YEARS, crate::LongInterval);
cast_flat_variant_both!(GeoPoint, crate::GeoPoint);

// String and binary from-variant casts carry size arguments.

#[doc(hidden)]
pub fn cast_to_s_FV(value: FlatVariant, size: i32, fixed: bool) -> SqlResult<SqlString> {
    cast_to_s_V(Variant::from(&value), size, fixed)
}

#[doc(hidden)]
pub fn cast_to_s_FVN(value: Option<FlatVariant>, size: i32, fixed: bool) -> SqlResult<SqlString> {
    cast_to_s_VN(value.map(|v| Variant::from(&v)), size, fixed)
}

#[doc(hidden)]
pub fn cast_to_sN_FV(value: FlatVariant, size: i32, fixed: bool) -> SqlResult<Option<SqlString>> {
    cast_to_sN_V(Variant::from(&value), size, fixed)
}

#[doc(hidden)]
pub fn cast_to_sN_FVN(
    value: Option<FlatVariant>,
    size: i32,
    fixed: bool,
) -> SqlResult<Option<SqlString>> {
    cast_to_sN_VN(value.map(|v| Variant::from(&v)), size, fixed)
}

#[doc(hidden)]
pub fn cast_to_bytes_FV(value: FlatVariant, size: i32, fixed: bool) -> SqlResult<ByteArray> {
    cast_to_bytes_V(Variant::from(&value), size, fixed)
}

#[doc(hidden)]
pub fn cast_to_bytes_FVN(
    value: Option<FlatVariant>,
    size: i32,
    fixed: bool,
) -> SqlResult<ByteArray> {
    cast_to_bytes_VN(value.map(|v| Variant::from(&v)), size, fixed)
}

#[doc(hidden)]
pub fn cast_to_bytesN_FV(
    value: FlatVariant,
    size: i32,
    fixed: bool,
) -> SqlResult<Option<ByteArray>> {
    cast_to_bytesN_V(Variant::from(&value), size, fixed)
}

#[doc(hidden)]
pub fn cast_to_bytesN_FVN(
    value: Option<FlatVariant>,
    size: i32,
    fixed: bool,
) -> SqlResult<Option<ByteArray>> {
    cast_to_bytesN_VN(value.map(|v| Variant::from(&v)), size, fixed)
}

#[doc(hidden)]
pub fn cast_to_SqlDecimalN_FV<const P: usize, const S: usize>(
    value: FlatVariant,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    cast_to_SqlDecimalN_V::<P, S>(Variant::from(&value))
}

#[doc(hidden)]
pub fn cast_to_SqlDecimalN_FVN<const P: usize, const S: usize>(
    value: Option<FlatVariant>,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_SqlDecimalN_FV::<P, S>(value),
    }
}

// Array and map casts

#[doc(hidden)]
pub fn cast_to_FV_vec<T>(vec: Array<T>) -> SqlResult<FlatVariant>
where
    Variant: From<T>,
    T: Clone,
{
    Ok(FlatVariant::from(&cast_to_V_vec(vec)?))
}

#[doc(hidden)]
pub fn cast_to_FVN_vec<T>(vec: Array<T>) -> SqlResult<Option<FlatVariant>>
where
    Variant: From<T>,
    T: Clone,
{
    r2o(cast_to_FV_vec(vec))
}

#[doc(hidden)]
pub fn cast_to_FV_vecN<T>(vec: Option<Array<T>>) -> SqlResult<FlatVariant>
where
    Variant: From<T>,
    T: Clone,
{
    Ok(FlatVariant::from(&cast_to_V_vecN(vec)?))
}

#[doc(hidden)]
pub fn cast_to_FVN_vecN<T>(vec: Option<Array<T>>) -> SqlResult<Option<FlatVariant>>
where
    Variant: From<T>,
    T: Clone,
{
    r2o(cast_to_FV_vecN(vec))
}

#[doc(hidden)]
pub fn cast_to_vec_FV<T>(value: FlatVariant) -> SqlResult<Array<T>>
where
    Array<T>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    cast_to_vec_V(Variant::from(&value))
}

#[doc(hidden)]
pub fn cast_to_vec_FVN<T>(value: Option<FlatVariant>) -> SqlResult<Option<Array<T>>>
where
    Array<T>: TryFrom<Variant, Error = Box<dyn Error>>,
    T: std::fmt::Debug,
{
    cast_to_vec_VN(value.map(|v| Variant::from(&v)))
}

#[doc(hidden)]
pub fn cast_to_vecN_FV<T>(value: FlatVariant) -> SqlResult<Option<Array<T>>>
where
    Array<T>: TryFrom<Variant, Error = Box<dyn Error>>,
    T: std::fmt::Debug,
{
    cast_to_vecN_V(Variant::from(&value))
}

#[doc(hidden)]
pub fn cast_to_vecN_FVN<T>(value: Option<FlatVariant>) -> SqlResult<Option<Array<T>>>
where
    Array<T>: TryFrom<Variant, Error = Box<dyn Error>>,
    T: std::fmt::Debug,
{
    cast_to_vecN_VN(value.map(|v| Variant::from(&v)))
}

#[doc(hidden)]
pub fn cast_to_FV_FVN(value: Option<FlatVariant>) -> SqlResult<FlatVariant> {
    match value {
        None => Ok(FlatVariant::sql_null()),
        Some(x) => Ok(x),
    }
}

#[doc(hidden)]
pub fn cast_to_FV_map<K, V>(map: Map<K, V>) -> SqlResult<FlatVariant>
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    Ok(FlatVariant::from(&cast_to_V_map(map)?))
}

#[doc(hidden)]
pub fn cast_to_FVN_map<K, V>(map: Map<K, V>) -> SqlResult<Option<FlatVariant>>
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    r2o(cast_to_FV_map(map))
}

#[doc(hidden)]
pub fn cast_to_FV_mapN<K, V>(map: Option<Map<K, V>>) -> SqlResult<FlatVariant>
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    Ok(FlatVariant::from(&cast_to_V_mapN(map)?))
}

#[doc(hidden)]
pub fn cast_to_FVN_mapN<K, V>(map: Option<Map<K, V>>) -> SqlResult<Option<FlatVariant>>
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    r2o(cast_to_FV_mapN(map))
}

#[doc(hidden)]
pub fn cast_to_map_FV<K, V>(value: FlatVariant) -> SqlResult<Map<K, V>>
where
    Map<K, V>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    cast_to_map_V(Variant::from(&value))
}

#[doc(hidden)]
pub fn cast_to_map_FVN<K, V>(value: Option<FlatVariant>) -> SqlResult<Option<Map<K, V>>>
where
    Map<K, V>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    cast_to_map_VN(value.map(|v| Variant::from(&v)))
}

#[doc(hidden)]
pub fn cast_to_mapN_FV<K, V>(value: FlatVariant) -> SqlResult<Option<Map<K, V>>>
where
    Map<K, V>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    cast_to_mapN_V(Variant::from(&value))
}

#[doc(hidden)]
pub fn cast_to_mapN_FVN<K, V>(value: Option<FlatVariant>) -> SqlResult<Option<Map<K, V>>>
where
    Map<K, V>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    cast_to_mapN_VN(value.map(|v| Variant::from(&v)))
}

// Indexing (VARIANT_INDEX opcode), native on the flat encoding

// Return type is always Option<FlatVariant>, matching the indexV grid.
#[doc(hidden)]
pub fn indexFV__<T>(value: &FlatVariant, index: T) -> Option<FlatVariant>
where
    T: Into<FlatVariant>,
{
    value.index(&index.into())
}

#[doc(hidden)]
pub fn indexFV_N<T>(value: &FlatVariant, index: Option<T>) -> Option<FlatVariant>
where
    T: Into<FlatVariant>,
{
    let index = index?;
    indexFV__(value, index)
}

#[doc(hidden)]
pub fn indexFVN_<T>(value: &Option<FlatVariant>, index: T) -> Option<FlatVariant>
where
    T: Into<FlatVariant>,
{
    match value {
        None => None,
        Some(value) => indexFV__(value, index),
    }
}

#[doc(hidden)]
pub fn indexFVNN<T>(value: &Option<FlatVariant>, index: Option<T>) -> Option<FlatVariant>
where
    T: Into<FlatVariant>,
{
    match value {
        None => None,
        Some(value) => indexFV_N(value, index),
    }
}

// JSON functions and TYPEOF

#[doc(hidden)]
pub fn parse_json_fv_s(value: SqlString) -> FlatVariant {
    match serde_json::from_str::<FlatVariant>(value.str()) {
        Ok(v) => v,
        Err(_) => FlatVariant::sql_null(),
    }
}

#[doc(hidden)]
pub fn parse_json_fv_sN(value: Option<SqlString>) -> Option<FlatVariant> {
    value.map(parse_json_fv_s)
}

#[doc(hidden)]
pub fn parse_json_fv_nullN(_value: Option<()>) -> Option<FlatVariant> {
    None
}

#[doc(hidden)]
pub fn to_json_FV(value: FlatVariant) -> Option<SqlString> {
    match value.to_json_string() {
        Ok(s) => Some(SqlString::from(s)),
        _ => None,
    }
}

#[doc(hidden)]
pub fn to_json_FVN(value: Option<FlatVariant>) -> Option<SqlString> {
    value.and_then(to_json_FV)
}

#[doc(hidden)]
pub fn typeof_fv_(value: FlatVariant) -> SqlString {
    // Delegate so the type strings cannot drift from the enum's typeof_.
    crate::variant::typeof_(Variant::from(&value))
}

#[doc(hidden)]
pub fn typeof_fvN(value: Option<FlatVariant>) -> SqlString {
    match value {
        None => SqlString::from_ref("NULL"),
        Some(value) => typeof_fv_(value),
    }
}

#[doc(hidden)]
pub fn variantnull_fv() -> FlatVariant {
    FlatVariant::variant_null()
}

#[doc(hidden)]
pub fn from_json_string2<T>(value: &str) -> Option<T>
where
    T: for<'de> serde::Deserialize<'de>,
{
    serde_json::from_str::<T>(value).ok()
}

// Suppress an unused warning: SqlRuntimeError is used only when delegated
// functions construct errors.
#[allow(unused_imports)]
use SqlRuntimeError as _SqlRuntimeErrorUsed;
