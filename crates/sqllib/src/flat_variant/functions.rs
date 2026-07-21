//! The runtime functions for [`FlatVariant`] beyond casts: indexing
//! (the VARIANT_INDEX opcode), PARSE_JSON, TO_JSON, TYPEOF, and
//! VARIANTNULL, all emitted by the SQL compiler when the flat_variant
//! mode is on.

use crate::SqlString;
use crate::flat_variant::FlatVariant;
use crate::flat_variant::casts::type_string;

// Indexing (VARIANT_INDEX opcode), native on the flat encoding

// Return type is always Option<FlatVariant>, matching the indexV grid.
#[doc(hidden)]
pub fn indexFV__<T>(value: &FlatVariant, index: T) -> Option<FlatVariant>
where
    T: Into<FlatVariant>,
{
    value.index_from_one(&index.into())
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
    serde_json::from_str::<FlatVariant>(value.str()).unwrap_or_default()
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
    SqlString::from_ref(type_string(value.as_bytes()))
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

// No from_json_string2: the compiler emits `from_json_string` (variant.rs)
// in both variant modes; its AUX type parameter is generic on every
// implementing type, so FlatVariant programs use it unchanged.
