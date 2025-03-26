//! Implementation of various cast operations.

#![allow(non_snake_case)]

use crate::{
    array::Array,
    binary::ByteArray,
    decimal::Dec,
    error::{r2o, SqlResult, SqlRuntimeError},
    geopoint::*,
    interval::*,
    map::Map,
    timestamp::*,
    uuid::*,
    variant::*,
    SqlString, Weight,
};

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use dbsp::algebra::{HasOne, HasZero, F32, F64};
use num::{FromPrimitive, One, ToPrimitive, Zero};
use num_traits::cast::NumCast;
use regex::{Captures, Regex};
use rust_decimal::{Decimal, RoundingStrategy};
use std::cmp::{min, Ordering};
use std::error::Error;
use std::string::String;
use std::sync::LazyLock;

/// Maps a short type name to a SQL type name.
/// Maybe we should not have short type names at all...
// Needs to be pub(crate) because some macros are expanded in other files.
#[doc(hidden)]
pub(crate) fn type_name(name: &'static str) -> &'static str {
    match name {
        "i8" => "TINYINT",
        "i16" => "SHORTINT",
        "i32" => "INTEGER",
        "i64" => "BIGINT",
        "f" => "REAL",
        "d" => "FLOAT",
        "Timestamp" => "TIMESTAMP",
        "Date" => "DATE",
        "Time" => "TIME",
        "decimal" => "DECIMAL",
        "ShortInterval" => "INTERVAL",
        "LongInterval" => "INTERVAL",
        "s" => "(VAR)CHAR",
        _ => "Unexpected type",
    }
}

/// Maps a Rust type name to the corresponding SQL type name
#[doc(hidden)]
pub(crate) fn rust_type_name(name: &'static str) -> &'static str {
    match name {
        "i8" => "TINYINT",
        "i16" => "SHORTINT",
        "i32" => "INTEGER",
        "i64" => "BIGINT",
        "F32" => "REAL",
        "F64" => "FLOAT",
        "Timestamp" => "Timestamp",
        "Date" => "DATE",
        "Time" => "TIME",
        "Decimal" => "DECIMAL",
        "ShortInterval" => "INTERVAL",
        "LongInterval" => "INTERVAL",
        "String" => "(VAR)CHAR",
        _ => "Unexpected type",
    }
}

/// Sql type name from a type identifier
#[macro_export]
#[doc(hidden)]
macro_rules! tn {
    ($result_name: ident) => {
        type_name(stringify!($result_name))
    };
}

/// Sql type name from a Rust type
#[macro_export]
#[doc(hidden)]
macro_rules! ttn {
    ($result_type: ty) => {
        rust_type_name(stringify!($result_type))
    };
}

/// Cast to null
#[doc(hidden)]
macro_rules! cn {
    ($result_name: ident) => {
        cast_null(tn!($result_name))
    };
}

/// Standard cast implementation: if the result is Error, panic
#[doc(hidden)]
pub fn unwrap_cast<T>(value: SqlResult<T>) -> T {
    match value {
        Err(ce) => panic!("{}", *ce),
        Ok(v) => v,
    }
}

#[doc(hidden)]
pub fn unwrap_cast_position<T>(position: &str, value: SqlResult<T>) -> T {
    match value {
        Err(ce) => panic!("{}: {}", position, *ce),
        Ok(v) => v,
    }
}

/// Safe cast implementation: if the result is Error, return null (None).
// The result is always Option<T>.
#[doc(hidden)]
pub fn unwrap_safe_cast<T>(value: SqlResult<Option<T>>) -> Option<T> {
    value.unwrap_or_default()
}

fn cast_null(t: &str) -> Box<SqlRuntimeError> {
    SqlRuntimeError::from_string(format!("cast of NULL value to non-null type {}", t))
}

// Creates three cast functions based on an existing one
// The original function is
// cast_to_ $result_name _ $type_name( value: $arg_type ) -> SqlResult<$result_type>
macro_rules! cast_function {
    ($result_name: ident, $result_type: ty, $type_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_name N_ $type_name>]( value: $arg_type ) -> SqlResult<Option<$result_type>> {
                r2o([<cast_to_ $result_name _ $type_name>](value))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_name _ $type_name N >]( value: Option<$arg_type> ) -> SqlResult<$result_type> {
                match value {
                    None => Err(cn!($type_name)),
                    Some(value) => [<cast_to_ $result_name _ $type_name>](value),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_name N_ $type_name N >]( value: Option<$arg_type> ) -> SqlResult<Option<$result_type>> {
                match value {
                    None => Ok(None),
                    Some(v) => r2o([<cast_to_ $result_name _ $type_name >](v)),
                }
            }
        }
    };
}

/////////// cast to b

macro_rules! cast_to_b {
    ($type_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_b_ $type_name>]( value: $arg_type ) -> SqlResult<bool> {
                Ok(value != <$arg_type as num::Zero>::zero())
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_b_ $type_name N >]( value: Option<$arg_type> ) -> SqlResult<bool> {
                match value {
                    None => Err(cast_null("bool")),
                    Some(value) => [<cast_to_b_ $type_name>](value),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_bN_ $type_name >]( value: $arg_type ) -> SqlResult<Option<bool>> {
                r2o([< cast_to_b_ $type_name >](value))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_bN_ $type_name N >]( value: Option<$arg_type> ) -> SqlResult<Option<bool>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_bN_ $type_name >](value),
                }
            }
        }
    };
}

/// Generates 4 functions to convert floating point values to booleans
macro_rules! cast_to_b_fp {
    ($type_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            // Cast a FP numeric value to a bool: result is 'false' if value is 0
            #[inline]
            pub fn [<cast_to_b_ $type_name>]( value: $arg_type ) -> SqlResult<bool> {
                Ok(value != $arg_type::zero())
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_b_ $type_name N >]( value: Option<$arg_type> ) -> SqlResult<bool> {
                match value {
                    None => Err(cast_null("bool")),
                    Some(value) => [<cast_to_b_ $type_name>](value),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_bN_ $type_name >]( value: $arg_type ) -> SqlResult<Option<bool>> {
                r2o([< cast_to_b_ $type_name >](value))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_bN_ $type_name N >]( value: Option<$arg_type> ) -> SqlResult<Option<bool>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_bN_ $type_name >](value),
                }
            }
        }
    };
}

#[doc(hidden)]
#[inline]
pub fn cast_to_b_b(value: bool) -> SqlResult<bool> {
    Ok(value)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_b_bN(value: Option<bool>) -> SqlResult<bool> {
    match value {
        None => Err(cast_null("bool")),
        Some(value) => Ok(value),
    }
}

cast_to_b!(decimal, Decimal);
cast_to_b_fp!(d, F64);
cast_to_b_fp!(f, F32);
cast_to_b!(i8, i8);
cast_to_b!(i16, i16);
cast_to_b!(i32, i32);
cast_to_b!(i64, i64);
cast_to_b!(i, isize);
cast_to_b!(u, usize);

#[doc(hidden)]
#[inline]
pub fn cast_to_b_s(value: SqlString) -> SqlResult<bool> {
    Ok(value.str().trim().parse().unwrap_or(false))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_b_sN(value: Option<SqlString>) -> SqlResult<bool> {
    match value {
        None => Err(cast_null("bool")),
        Some(value) => cast_to_b_s(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bN_sN(value: Option<SqlString>) -> SqlResult<Option<bool>> {
    match value {
        None => Ok(None),
        Some(value) => r2o(cast_to_b_s(value)),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bN_s(value: SqlString) -> SqlResult<Option<bool>> {
    r2o(cast_to_b_s(value))
}

/////////// cast to bN

#[doc(hidden)]
#[inline]
pub fn cast_to_bN_nullN(_value: Option<()>) -> SqlResult<Option<bool>> {
    Ok(None)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bN_b(value: bool) -> SqlResult<Option<bool>> {
    Ok(Some(value))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bN_bN(value: Option<bool>) -> SqlResult<Option<bool>> {
    Ok(value)
}

/////////// cast to date

#[doc(hidden)]
#[inline]
pub fn cast_to_Date_s(value: SqlString) -> SqlResult<Date> {
    match NaiveDate::parse_from_str(value.str(), "%Y-%m-%d") {
        Ok(value) => Ok(Date::new(
            (value.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() / 86400) as i32,
        )),
        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
    }
}

cast_function!(Date, Date, s, SqlString);

#[doc(hidden)]
pub fn cast_to_Date_Timestamp(value: Timestamp) -> SqlResult<Date> {
    Ok(value.get_date())
}

cast_function!(Date, Date, Timestamp, Timestamp);

#[doc(hidden)]
#[inline]
pub fn cast_to_DateN_nullN(_value: Option<()>) -> SqlResult<Option<Date>> {
    Ok(None)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_Date_Date(value: Date) -> SqlResult<Date> {
    Ok(value)
}

cast_function!(Date, Date, Date, Date);

/////////// cast to Time

#[doc(hidden)]
#[inline]
pub fn cast_to_Time_s(value: SqlString) -> SqlResult<Time> {
    match NaiveTime::parse_from_str(value.str(), "%H:%M:%S%.f") {
        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
        Ok(value) => Ok(Time::from_time(value)),
    }
}

cast_function!(Time, Time, s, SqlString);

#[doc(hidden)]
#[inline]
pub fn cast_to_TimeN_nullN(_value: Option<()>) -> SqlResult<Option<Time>> {
    Ok(None)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_Time_Time(value: Time) -> SqlResult<Time> {
    Ok(value)
}

cast_function!(Time, Time, Time, Time);

#[doc(hidden)]
#[inline]
pub fn cast_to_Time_Timestamp(value: Timestamp) -> SqlResult<Time> {
    Ok(Time::from_time(value.to_dateTime().time()))
}

cast_function!(Time, Time, Timestamp, Timestamp);

/////////// cast to decimal

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_b(value: bool, precision: u32, scale: u32) -> SqlResult<Decimal> {
    let result = if value {
        <rust_decimal::Decimal as One>::one()
    } else {
        <rust_decimal::Decimal as Zero>::zero()
    };
    cast_to_decimal_decimal(result, precision, scale)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_bN(value: Option<bool>, precision: u32, scale: u32) -> SqlResult<Decimal> {
    match value {
        None => Err(cast_null("DECIMAL")),
        Some(value) => cast_to_decimal_b(value, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_decimal(value: Decimal, precision: u32, scale: u32) -> SqlResult<Decimal> {
    // make sure we can fit the left half of the number in the new wanted precision

    // '1234.5678' -> DECIMAL(6, 2) is fine as the integer part fits in 4 digits
    // but to DECIMAL(6, 3) would error as we can't fit '1234' in 3 digits
    // This is the rounding strategy used in Calcite
    let result = value.round_dp_with_strategy(scale, RoundingStrategy::ToZero);

    let int_part_precision = result
        .trunc()
        .mantissa()
        .checked_abs()
        .unwrap_or(i128::MAX) // i128::MIN and i128::MAX have the same number of digits
        .checked_ilog10()
        .map(|v| v + 1)
        .unwrap_or(0);
    let to_int_part_precision = precision - scale;
    if to_int_part_precision < int_part_precision {
        Err(SqlRuntimeError::from_string(
            format!("Cannot represent {value} as DECIMAL({precision}, {scale}): precision of DECIMAL type too small to represent value")))
    } else {
        Ok(result)
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_dec_decimal(value: Decimal) -> SqlResult<Decimal> {
    Ok(value)
}

cast_function!(dec, Dec, decimal, Decimal);

#[doc(hidden)]
#[inline]
pub fn cast_to_dec_f(value: F32) -> SqlResult<Decimal> {
    Ok(Decimal::from_f32(value.into_inner()).unwrap())
}

cast_function!(dec, Dec, f, F32);

#[doc(hidden)]
#[inline]
pub fn cast_to_dec_d(value: F64) -> SqlResult<Decimal> {
    Ok(Decimal::from_f64(value.into_inner()).unwrap())
}

cast_function!(dec, Dec, d, F64);

#[doc(hidden)]
#[inline]
pub fn cast_to_dec_i8(value: i8) -> SqlResult<Decimal> {
    Ok(value.into())
}

cast_function!(dec, Dec, i8, i8);

#[doc(hidden)]
#[inline]
pub fn cast_to_dec_i16(value: i16) -> SqlResult<Decimal> {
    Ok(value.into())
}

cast_function!(dec, Dec, i16, i16);

#[doc(hidden)]
#[inline]
pub fn cast_to_dec_i32(value: i32) -> SqlResult<Decimal> {
    Ok(value.into())
}

cast_function!(dec, Dec, i32, i32);

#[doc(hidden)]
#[inline]
pub fn cast_to_dec_i64(value: i64) -> SqlResult<Decimal> {
    Ok(value.into())
}

cast_function!(dec, Dec, i64, i64);

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_decimalN(
    value: Option<Decimal>,
    precision: u32,
    scale: u32,
) -> SqlResult<Decimal> {
    match value {
        None => Err(cast_null("DECIMAL")),
        Some(value) => cast_to_decimal_decimal(value, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_d(value: F64, precision: u32, scale: u32) -> SqlResult<Decimal> {
    match Decimal::from_f64(value.into_inner()) {
        None => Err(SqlRuntimeError::from_string(format!(
            "Value {} cannot be represented as a DECIMAL({}, {})",
            value, precision, scale
        ))),
        Some(result) => cast_to_decimal_decimal(result, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_dN(value: Option<F64>, precision: u32, scale: u32) -> SqlResult<Decimal> {
    match value {
        None => Err(cast_null("DECIMAL")),
        Some(value) => cast_to_decimal_d(value, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_f(value: F32, precision: u32, scale: u32) -> SqlResult<Decimal> {
    match Decimal::from_f32(value.into_inner()) {
        None => Err(SqlRuntimeError::from_string(format!(
            "Value {} cannot be represented as a DECIMAL({}, {})",
            value, precision, scale
        ))),
        Some(value) => cast_to_decimal_decimal(value, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_fN(value: Option<F32>, precision: u32, scale: u32) -> SqlResult<Decimal> {
    match value {
        None => Err(cast_null("DECIMAL")),
        Some(value) => cast_to_decimal_f(value, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_s(value: SqlString, precision: u32, scale: u32) -> SqlResult<Decimal> {
    match value.str().trim().parse::<Decimal>() {
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "While converting '{}' to decimal: {}",
            value, e
        ))),
        Ok(result) => cast_to_decimal_decimal(result, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_sN(
    value: Option<SqlString>,
    precision: u32,
    scale: u32,
) -> SqlResult<Decimal> {
    match value {
        None => Ok(<rust_decimal::Decimal as Zero>::zero()),
        Some(value) => cast_to_decimal_s(value, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_V(
    value: Variant,
    precision: u32,
    scale: u32,
) -> SqlResult<Option<Decimal>> {
    match value {
        Variant::TinyInt(i) => r2o(cast_to_decimal_i8(i, precision, scale)),
        Variant::SmallInt(i) => r2o(cast_to_decimal_i16(i, precision, scale)),
        Variant::Int(i) => r2o(cast_to_decimal_i32(i, precision, scale)),
        Variant::BigInt(i) => r2o(cast_to_decimal_i64(i, precision, scale)),
        Variant::Real(f) => r2o(cast_to_decimal_f(f, precision, scale)),
        Variant::Double(f) => r2o(cast_to_decimal_d(f, precision, scale)),
        Variant::Decimal(d) => r2o(cast_to_decimal_decimal(d, precision, scale)),
        _ => Ok(None),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_VN(
    value: Option<Variant>,
    precision: u32,
    scale: u32,
) -> SqlResult<Option<Decimal>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_decimalN_V(value, precision, scale),
    }
}

macro_rules! cast_to_decimal {
    ($type_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_decimal_ $type_name> ]( value: $arg_type, precision: u32, scale: u32 ) -> SqlResult<Decimal> {
                let result = Decimal::[<from_ $arg_type>](value).unwrap();
                cast_to_decimal_decimal(result, precision, scale)
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_decimal_ $type_name N> ]( value: Option<$arg_type>, precision: u32, scale: u32 ) -> SqlResult<Decimal> {
                match value {
                    None => Err(cast_null("DECIMAL")),
                    Some(value) => [<cast_to_decimal_ $type_name >](value, precision, scale),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_decimalN_ $type_name> ]( value: $arg_type, precision: u32, scale: u32 ) -> SqlResult<Option<Decimal>> {
                r2o([< cast_to_decimal_ $type_name >](value, precision, scale))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_decimalN_ $type_name N> ]( value: Option<$arg_type>, precision: u32, scale: u32 ) -> SqlResult<Option<Decimal>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_decimalN_ $type_name >](value, precision, scale),
                }
            }
        }
    }
}

cast_to_decimal!(i, isize);
cast_to_decimal!(i8, i8);
cast_to_decimal!(i16, i16);
cast_to_decimal!(i32, i32);
cast_to_decimal!(i64, i64);
cast_to_decimal!(u, usize);

/////////// cast to decimalN

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_nullN(
    _value: Option<()>,
    _precision: u32,
    _scale: i32,
) -> SqlResult<Option<Decimal>> {
    Ok(None)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_b(value: bool, precision: u32, scale: u32) -> SqlResult<Option<Decimal>> {
    r2o(cast_to_decimal_b(value, precision, scale))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_bN(
    value: Option<bool>,
    precision: u32,
    scale: u32,
) -> SqlResult<Option<Decimal>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_decimalN_b(value, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_decimal(
    value: Decimal,
    precision: u32,
    scale: u32,
) -> SqlResult<Option<Decimal>> {
    r2o(cast_to_decimal_decimal(value, precision, scale))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_decimalN(
    value: Option<Decimal>,
    precision: u32,
    scale: u32,
) -> SqlResult<Option<Decimal>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_decimalN_decimal(value, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_d(value: F64, precision: u32, scale: u32) -> SqlResult<Option<Decimal>> {
    r2o(cast_to_decimal_d(value, precision, scale))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_dN(
    value: Option<F64>,
    precision: u32,
    scale: u32,
) -> SqlResult<Option<Decimal>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_decimalN_d(value, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_f(value: F32, precision: u32, scale: u32) -> SqlResult<Option<Decimal>> {
    r2o(cast_to_decimal_f(value, precision, scale))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_fN(
    value: Option<F32>,
    precision: u32,
    scale: u32,
) -> SqlResult<Option<Decimal>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_decimalN_f(value, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_s(
    value: SqlString,
    precision: u32,
    scale: u32,
) -> SqlResult<Option<Decimal>> {
    match value.str().trim().parse::<Decimal>() {
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "While converting '{}' to decimal: {}",
            value, e
        ))),
        Ok(value) => r2o(cast_to_decimal_decimal(value, precision, scale)),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_sN(
    value: Option<SqlString>,
    precision: u32,
    scale: u32,
) -> SqlResult<Option<Decimal>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_decimalN_s(value, precision, scale),
    }
}

/////////// cast to double

macro_rules! cast_to_fp {
    ($type_name: ident, $arg_type: ty,
     $result_type_name: ident, $result_type: ty, $result_base_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_type_name _ $type_name >]( value: $arg_type ) -> SqlResult<$result_type> {
                Ok($result_type ::from(value as $result_base_type))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_type_name _ $type_name N >]( value: Option<$arg_type> ) -> SqlResult<$result_type> {
                match value {
                    None => Err(cn!($result_type)),
                    Some(value) => Ok($result_type::from(value as $result_base_type)),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_type_name N_ $type_name >]( value: $arg_type ) -> SqlResult<Option<$result_type>> {
                r2o([<cast_to_ $result_type_name _ $type_name >](value))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_type_name N_ $type_name N >]( value: Option<$arg_type> ) -> SqlResult<Option<$result_type>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_ $result_type_name N_ $type_name >](value),
                }
            }
        }
    }
}

macro_rules! cast_to_fps {
    ($type_name: ident, $arg_type: ty) => {
        cast_to_fp!($type_name, $arg_type, d, F64, f64);
        cast_to_fp!($type_name, $arg_type, f, F32, f32);
    };
}

#[doc(hidden)]
#[inline]
pub fn cast_to_d_b(value: bool) -> SqlResult<F64> {
    if value {
        Ok(F64::one())
    } else {
        Ok(F64::zero())
    }
}

cast_function!(d, F64, b, bool);

#[doc(hidden)]
#[inline]
pub fn cast_to_d_decimal(value: Decimal) -> SqlResult<F64> {
    match value.to_f64() {
        None => Err(SqlRuntimeError::from_string(format!(
            "Cannot convert {value} to DOUBLE"
        ))),
        Some(value) => Ok(F64::from(value)),
    }
}

cast_function!(d, F64, decimal, Decimal);

#[doc(hidden)]
#[inline]
pub fn cast_to_d_d(value: F64) -> SqlResult<F64> {
    Ok(value)
}

cast_function!(d, F64, d, F64);

#[doc(hidden)]
#[inline]
pub fn cast_to_d_f(value: F32) -> SqlResult<F64> {
    Ok(F64::from(value.into_inner()))
}

cast_function!(d, F64, f, F32);

#[doc(hidden)]
#[inline]
pub fn cast_to_d_s(value: SqlString) -> SqlResult<F64> {
    match value.str().trim().parse::<f64>() {
        Err(_) => Ok(F64::zero()),
        Ok(x) => Ok(F64::from(x)),
    }
}

cast_function!(d, F64, s, SqlString);

/////////// cast to doubleN

#[doc(hidden)]
#[inline]
pub fn cast_to_dN_nullN(_value: Option<()>) -> SqlResult<Option<F64>> {
    Ok(None)
}

cast_to_fps!(i, isize);
cast_to_fps!(i8, i8);
cast_to_fps!(i16, i16);
cast_to_fps!(i32, i32);
cast_to_fps!(i64, i64);
cast_to_fps!(u, usize);

/////////// Cast to float

#[doc(hidden)]
#[inline]
pub fn cast_to_f_b(value: bool) -> SqlResult<F32> {
    if value {
        Ok(F32::one())
    } else {
        Ok(F32::zero())
    }
}

cast_function!(f, F32, b, bool);

#[doc(hidden)]
#[inline]
pub fn cast_to_f_decimal(value: Decimal) -> SqlResult<F32> {
    match value.to_f32() {
        None => Err(SqlRuntimeError::from_string(format!(
            "Cannot convert {value} to REAL"
        ))),
        Some(value) => Ok(F32::from(value)),
    }
}

cast_function!(f, F32, decimal, Decimal);

#[doc(hidden)]
#[inline]
pub fn cast_to_f_d(value: F64) -> SqlResult<F32> {
    Ok(F32::from(value.into_inner() as f32))
}

cast_function!(f, F32, d, F64);

#[doc(hidden)]
#[inline]
pub fn cast_to_f_f(value: F32) -> SqlResult<F32> {
    Ok(value)
}

cast_function!(f, F32, f, F32);

#[doc(hidden)]
#[inline]
pub fn cast_to_f_s(value: SqlString) -> SqlResult<F32> {
    match value.str().trim().parse::<f32>() {
        Err(_) => Ok(F32::zero()),
        Ok(x) => Ok(F32::from(x)),
    }
}

cast_function!(f, F32, s, SqlString);

/////////// cast to floatN

#[doc(hidden)]
#[inline]
pub fn cast_to_fN_nullN(_value: Option<()>) -> SqlResult<Option<F32>> {
    Ok(None)
}

/////////// cast to GeoPoint

#[doc(hidden)]
#[inline]
pub fn cast_to_geopoint_geopoint(value: GeoPoint) -> SqlResult<GeoPoint> {
    Ok(value)
}

cast_function!(geopoint, GeoPoint, geopoint, GeoPoint);

/////////// cast to SqlString

// True if the size means "unlimited"
#[doc(hidden)]
fn is_unlimited_size(size: i32) -> bool {
    size < 0
}

#[inline(always)]
#[doc(hidden)]
fn truncate(value: &str, size: usize) -> String {
    let size = min(size, value.len());
    value[0..size].to_string()
}

/// Make sure the specified string has exactly the
/// specified size.
#[inline(always)]
#[doc(hidden)]
fn size_string(value: &str, size: i32) -> String {
    if is_unlimited_size(size) {
        value.trim_end().to_string()
    } else {
        let sz = size as usize;
        match value.len().cmp(&sz) {
            Ordering::Equal => value.to_string(),
            Ordering::Greater => truncate(value, sz),
            Ordering::Less => format!("{value:<sz$}"),
        }
    }
}

/// Make sure that the specified string does not exceed
/// the specified size.
#[inline(always)]
#[doc(hidden)]
fn limit_string(value: &str, size: i32) -> String {
    if is_unlimited_size(size) {
        value.trim_end().to_string()
    } else {
        let sz = size as usize;
        if value.len() < sz {
            value.to_string()
        } else {
            // TODO: this is legal only if all excess characters are spaces
            truncate(value, sz)
        }
    }
}

#[inline(always)]
#[doc(hidden)]
pub fn limit_or_size_string(value: &str, size: i32, fixed: bool) -> SqlResult<SqlString> {
    if fixed {
        Ok(SqlString::from(size_string(value, size)))
    } else {
        Ok(SqlString::from(limit_string(value, size)))
    }
}

macro_rules! cast_to_string {
    ($type_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_s_ $type_name N >]( value: Option<$arg_type>, size: i32, fixed: bool ) -> SqlResult<SqlString> {
                match value {
                    None => Err(cast_null("VARCHAR")),
                    Some(value) => [<cast_to_s_ $type_name>](value, size, fixed),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_sN_ $type_name >]( value: $arg_type, size: i32, fixed: bool ) -> SqlResult<Option<SqlString>> {
                r2o(([< cast_to_s_ $type_name >](value, size, fixed)))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_sN_ $type_name N >]( value: Option<$arg_type>, size: i32, fixed: bool ) -> SqlResult<Option<SqlString>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_sN_ $type_name >](value, size, fixed),
                }
            }
        }
    };
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_b(value: bool, size: i32, fixed: bool) -> SqlResult<SqlString> {
    // Calcite generates uppercase for boolean casts to string
    let result = value.to_string().to_uppercase();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_decimal(value: Decimal, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result = value.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_d(value: F64, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let v = value.into_inner();
    cast_to_s_fp(v, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_fp<T>(v: T, size: i32, fixed: bool) -> SqlResult<SqlString>
where
    T: num::Float + std::fmt::Display,
{
    let result = if v.is_infinite() {
        if v.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else if v.is_nan() {
        "NaN".to_string()
    } else {
        let result = format!("{}", v);
        let result = result.trim_end_matches('0').to_string();
        result.trim_end_matches('.').to_string()
    };
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_f(value: F32, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let v = value.into_inner();
    cast_to_s_fp(v, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_s(value: SqlString, size: i32, fixed: bool) -> SqlResult<SqlString> {
    limit_or_size_string(value.str(), size, fixed)
}

#[doc(hidden)]
pub fn cast_to_s_Timestamp(value: Timestamp, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let dt = value.to_dateTime();
    let month = dt.month();
    let day = dt.day();
    let year = dt.year();
    let hr = dt.hour();
    let min = dt.minute();
    let sec = dt.second();
    let result = format!(
        "{}-{:02}-{:02} {:02}:{:02}:{:02}",
        year, month, day, hr, min, sec
    );
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
pub fn cast_to_s_Date(value: Date, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let dt = value.to_date();
    let month = dt.month();
    let day = dt.day();
    let year = dt.year();
    let result = format!("{}-{:02}-{:02}", year, month, day);
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
pub fn cast_to_s_Time(value: Time, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let dt = value.to_time();
    let hr = dt.hour();
    let min = dt.minute();
    let sec = dt.second();
    let result = format!("{:02}:{:02}:{:02}", hr, min, sec);
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_i(value: isize, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result = value.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_i8(value: i8, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result = value.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_i16(value: i16, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result = value.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_i32(value: i32, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result = value.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_i64(value: i64, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result = value.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_u(value: usize, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result = value.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_V(value: Variant, size: i32, fixed: bool) -> SqlResult<SqlString> {
    // This function should never be called
    let result: SqlString = value.try_into().unwrap();
    limit_or_size_string(result.str(), size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_VN(value: Option<Variant>, size: i32, fixed: bool) -> SqlResult<SqlString> {
    // This function should never be called
    cast_to_s_V(value.unwrap(), size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_sN_V(value: Variant, size: i32, fixed: bool) -> SqlResult<Option<SqlString>> {
    let result: Result<SqlString, _> = value.try_into();
    match result {
        Err(_) => Ok(None),
        Ok(result) => r2o(limit_or_size_string(result.str(), size, fixed)),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_sN_VN(
    value: Option<Variant>,
    size: i32,
    fixed: bool,
) -> SqlResult<Option<SqlString>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_sN_V(value, size, fixed),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bytes_V(value: Variant, size: i32) -> SqlResult<ByteArray> {
    // Should never be called
    let result: Result<ByteArray, _> = value.try_into();
    match result {
        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
        Ok(result) => Ok(ByteArray::with_size(result.as_slice(), size)),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bytes_VN(value: Option<Variant>, size: i32) -> SqlResult<ByteArray> {
    match value {
        None => Err(cast_null("BINARY")),
        Some(value) => cast_to_bytes_V(value, size),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bytesN_V(value: Variant, size: i32) -> SqlResult<Option<ByteArray>> {
    let result: Result<ByteArray, _> = value.try_into();
    match result {
        Err(_) => Ok(None),
        Ok(value) => Ok(Some(ByteArray::with_size(value.as_slice(), size))),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bytesN_VN(value: Option<Variant>, size: i32) -> SqlResult<Option<ByteArray>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_bytesN_V(value, size),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_LongInterval_YEARS(
    interval: LongInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let years = interval.years();
    let negative = years < 0;
    let result = sign(negative) + &num::abs(years).to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
fn sign(negative: bool) -> String {
    (if negative { "-" } else { "+" }).to_string()
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_LongInterval_MONTHS(
    interval: LongInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let months = interval.months();
    let (months, negate) = if months < 0 {
        (-months, true)
    } else {
        (months, false)
    };
    let result: String = sign(negate) + &months.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_LongInterval_YEARS_TO_MONTHS(
    interval: LongInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let months = interval.months();
    let (months, negate) = if months < 0 {
        (-months, true)
    } else {
        (months, false)
    };
    let years = months / 12;
    let months = months % 12;
    let result: String = sign(negate) + &years.to_string() + "-" + &months.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_ShortInterval_DAYS(
    interval: ShortInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let negative = interval.milliseconds() < 0;
    let interval = if negative {
        ShortInterval::new(-interval.milliseconds())
    } else {
        interval
    };
    let days = extract_day_ShortInterval(interval);
    let result = format!("{}{}", sign(negative), days);
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_ShortInterval_HOURS(
    interval: ShortInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let negative = interval.milliseconds() < 0;
    let interval = if negative {
        ShortInterval::new(-interval.milliseconds())
    } else {
        interval
    };
    let days = extract_day_ShortInterval(interval);
    let hours = extract_hour_ShortInterval(interval);
    let result = format!("{}{}", sign(negative), 24 * days + hours);
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_ShortInterval_DAYS_TO_HOURS(
    interval: ShortInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let negative = interval.milliseconds() < 0;
    let interval = if negative {
        ShortInterval::new(-interval.milliseconds())
    } else {
        interval
    };
    let days = extract_day_ShortInterval(interval);
    let hours = extract_hour_ShortInterval(interval);
    let result = format!("{}{} {:02}", sign(negative), days, hours);
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_ShortInterval_MINUTES(
    interval: ShortInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let negative = interval.milliseconds() < 0;
    let interval = if negative {
        ShortInterval::new(-interval.milliseconds())
    } else {
        interval
    };
    let days = extract_day_ShortInterval(interval);
    let hours = extract_hour_ShortInterval(interval);
    let minutes = extract_minute_ShortInterval(interval);
    let result = format!("{}{}", sign(negative), (days * 24 + hours) * 60 + minutes);
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_ShortInterval_DAYS_TO_MINUTES(
    interval: ShortInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let negative = interval.milliseconds() < 0;
    let interval = if negative {
        ShortInterval::new(-interval.milliseconds())
    } else {
        interval
    };
    let days = extract_day_ShortInterval(interval);
    let hours = extract_hour_ShortInterval(interval);
    let minutes = extract_minute_ShortInterval(interval);
    let result = format!("{}{} {:02}:{:02}", sign(negative), days, hours, minutes);
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_ShortInterval_HOURS_TO_MINUTES(
    interval: ShortInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let negative = interval.milliseconds() < 0;
    let interval = if negative {
        ShortInterval::new(-interval.milliseconds())
    } else {
        interval
    };
    let days = extract_day_ShortInterval(interval);
    let hours = extract_hour_ShortInterval(interval);
    let minutes = extract_minute_ShortInterval(interval);
    let result = format!("{}{}:{:02}", sign(negative), days * 24 + hours, minutes);
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_ShortInterval_SECONDS(
    interval: ShortInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let negative = interval.milliseconds() < 0;
    let interval = if negative {
        ShortInterval::new(-interval.milliseconds())
    } else {
        interval
    };
    let days = extract_day_ShortInterval(interval);
    let hours = extract_hour_ShortInterval(interval);
    let minutes = extract_minute_ShortInterval(interval);
    let seconds = extract_second_ShortInterval(interval);
    let result = format!(
        "{}{}.{:06}",
        sign(negative),
        ((days * 24 + hours) * 60 + minutes) * 60 + seconds,
        0
    );
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_ShortInterval_DAYS_TO_SECONDS(
    interval: ShortInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let negative = interval.milliseconds() < 0;
    let interval = if negative {
        ShortInterval::new(-interval.milliseconds())
    } else {
        interval
    };
    let days = extract_day_ShortInterval(interval);
    let hours = extract_hour_ShortInterval(interval);
    let minutes = extract_minute_ShortInterval(interval);
    let seconds = extract_second_ShortInterval(interval);
    let result = format!(
        "{}{} {:02}:{:02}:{:02}.{:06}",
        sign(negative),
        days,
        hours,
        minutes,
        seconds,
        0
    );
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_ShortInterval_HOURS_TO_SECONDS(
    interval: ShortInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let negative = interval.milliseconds() < 0;
    let interval = if negative {
        ShortInterval::new(-interval.milliseconds())
    } else {
        interval
    };
    let days = extract_day_ShortInterval(interval);
    let hours = extract_hour_ShortInterval(interval);
    let minutes = extract_minute_ShortInterval(interval);
    let seconds = extract_second_ShortInterval(interval);
    let result = format!(
        "{}{}:{:02}:{:02}.{:06}",
        sign(negative),
        days * 24 + hours,
        minutes,
        seconds,
        0
    );
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_ShortInterval_MINUTES_TO_SECONDS(
    interval: ShortInterval,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
    let negative = interval.milliseconds() < 0;
    let interval = if negative {
        ShortInterval::new(-interval.milliseconds())
    } else {
        interval
    };
    let days = extract_day_ShortInterval(interval);
    let hours = extract_hour_ShortInterval(interval);
    let minutes = extract_minute_ShortInterval(interval);
    let seconds = extract_second_ShortInterval(interval);
    let result = format!(
        "{}{:02}:{:02}.{:06}",
        sign(negative),
        (days * 24 + hours) * 60 + minutes,
        seconds,
        0
    );
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
pub fn cast_to_s_Uuid(value: Uuid, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result: String = value.to_string();
    limit_or_size_string(&result, size, fixed)
}

cast_to_string!(b, bool);
cast_to_string!(decimal, Decimal);
cast_to_string!(f, F32);
cast_to_string!(d, F64);
cast_to_string!(s, SqlString);
cast_to_string!(i, isize);
cast_to_string!(u, usize);
cast_to_string!(i8, i8);
cast_to_string!(i16, i16);
cast_to_string!(i32, i32);
cast_to_string!(i64, i64);
cast_to_string!(Timestamp, Timestamp);
cast_to_string!(Time, Time);
cast_to_string!(Date, Date);
cast_to_string!(LongInterval_MONTHS, LongInterval);
cast_to_string!(LongInterval_YEARS, LongInterval);
cast_to_string!(LongInterval_YEARS_TO_MONTHS, LongInterval);
cast_to_string!(ShortInterval_DAYS, ShortInterval);
cast_to_string!(ShortInterval_HOURS, ShortInterval);
cast_to_string!(ShortInterval_DAYS_TO_HOURS, ShortInterval);
cast_to_string!(ShortInterval_MINUTES, ShortInterval);
cast_to_string!(ShortInterval_DAYS_TO_MINUTES, ShortInterval);
cast_to_string!(ShortInterval_HOURS_TO_MINUTES, ShortInterval);
cast_to_string!(ShortInterval_SECONDS, ShortInterval);
cast_to_string!(ShortInterval_DAYS_TO_SECONDS, ShortInterval);
cast_to_string!(ShortInterval_HOURS_TO_SECONDS, ShortInterval);
cast_to_string!(ShortInterval_MINUTES_TO_SECONDS, ShortInterval);
cast_to_string!(Uuid, Uuid);

#[doc(hidden)]
#[inline]
pub fn cast_to_sN_nullN(
    _value: Option<()>,
    _size: i32,
    _fixed: bool,
) -> SqlResult<Option<SqlString>> {
    Ok(None)
}

/////////// cast to integer

macro_rules! cast_to_i_i {
    ($result_type: ty, $arg_type_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_type _ $arg_type_name>]( value: $arg_type ) -> SqlResult<$result_type> {
                match $result_type::try_from(value) {
                    Ok(value) => Ok(value),
                    Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_type _ $arg_type_name N>]( value: Option<$arg_type> ) -> SqlResult<$result_type> {
                match value {
                    None => Err(cn!($result_type)),
                    Some(value) => [< cast_to_ $result_type _ $arg_type_name >](value),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_type N_ $arg_type_name >]( value: $arg_type ) -> SqlResult<Option<$result_type>> {
                r2o([< cast_to_ $result_type _ $arg_type_name >](value))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_type N_ $arg_type_name N>]( value: Option<$arg_type> ) -> SqlResult<Option<$result_type>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_ $result_type N_ $arg_type_name >](value),
                }
            }
        }
    }
}

macro_rules! cast_to_i {
    ($result_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [< cast_to_ $result_type N_nullN >](_value: Option<()>) -> SqlResult<Option<$result_type>> {
                Ok(None)
            }

            // From bool

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_type _ b >]( value: bool ) -> SqlResult<$result_type> {
                Ok(if value { 1 } else { 0 })
            }

            cast_function!($result_type, $result_type, b, bool);

            // From decimal

            #[doc(hidden)]
            #[inline]
            pub fn [< cast_to_ $result_type _decimal >](value: Decimal) -> SqlResult<$result_type> {
                match value.trunc().[<to_ $result_type >]() {
                    Some(value) => Ok(value),
                    None => Err(SqlRuntimeError::from_string(
                        format!("Cannot convert {value} to {}", tn!($result_type))
                    )),
                }
            }

            cast_function!($result_type, $result_type, decimal, Decimal);

            // F64

            #[doc(hidden)]
            #[inline]
            pub fn [< cast_to_ $result_type _d >](value: F64) -> SqlResult<$result_type> {
                let value = value.into_inner().trunc();
                match <$result_type as NumCast>::from(value) {
                    Some(value) => Ok(value),
                    None => Err(SqlRuntimeError::from_string(
                        format!("Cannot convert {value} to {}", tn!($result_type))
                    )),
                }
            }

            cast_function!($result_type, $result_type, d, F64);

            // F32

            #[doc(hidden)]
            #[inline]
            pub fn [< cast_to_ $result_type _f >](value: F32) -> SqlResult<$result_type> {
                let value = value.into_inner().trunc();
                match <$result_type as NumCast>::from(value) {
                    Some(value) => Ok(value),
                    None => Err(SqlRuntimeError::from_string(
                        format!("Cannot convert {value} to {}", tn!($result_type))
                    )),
                }
            }

            cast_function!($result_type, $result_type, f, F32);

            // From string

            #[doc(hidden)]
            #[inline]
            pub fn [< cast_to_ $result_type _s >](value: SqlString) -> SqlResult<$result_type> {
                match value.str().trim().parse::<$result_type>() {
                    Ok(value) => Ok(value),
                    Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
                }
            }

            cast_function!($result_type, $result_type, s, SqlString);

            // From other integers

            cast_to_i_i!($result_type, i8, i8);
            cast_to_i_i!($result_type, i16, i16);
            cast_to_i_i!($result_type, i32, i32);
            cast_to_i_i!($result_type, i64, i64);
            cast_to_i_i!($result_type, i, isize);
            cast_to_i_i!($result_type, u, usize);
        }
    }
}

cast_to_i!(i8);
cast_to_i!(i16);
cast_to_i!(i32);
cast_to_i!(i64);
cast_to_i!(u16);
cast_to_i!(u32);
cast_to_i!(u64);
cast_to_i!(u128);

#[doc(hidden)]
#[inline]
#[allow(clippy::unnecessary_cast)]
pub fn cast_to_i64_Weight(w: Weight) -> SqlResult<i64> {
    Ok(w as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i8_ShortInterval_DAYS(value: ShortInterval) -> SqlResult<i8> {
    cast_to_i8_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i16_ShortInterval_DAYS(value: ShortInterval) -> SqlResult<i16> {
    cast_to_i16_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i32_ShortInterval_DAYS(value: ShortInterval) -> SqlResult<i32> {
    cast_to_i32_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i64_ShortInterval_DAYS(value: ShortInterval) -> SqlResult<i64> {
    Ok(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i8_ShortInterval_HOURS(value: ShortInterval) -> SqlResult<i8> {
    cast_to_i8_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i16_ShortInterval_HOURS(value: ShortInterval) -> SqlResult<i16> {
    cast_to_i16_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i32_ShortInterval_HOURS(value: ShortInterval) -> SqlResult<i32> {
    cast_to_i32_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i64_ShortInterval_HOURS(value: ShortInterval) -> SqlResult<i64> {
    Ok(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i8_ShortInterval_MINUTES(value: ShortInterval) -> SqlResult<i8> {
    cast_to_i8_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i16_ShortInterval_MINUTES(value: ShortInterval) -> SqlResult<i16> {
    cast_to_i16_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i32_ShortInterval_MINUTES(value: ShortInterval) -> SqlResult<i32> {
    cast_to_i32_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i64_ShortInterval_MINUTES(value: ShortInterval) -> SqlResult<i64> {
    Ok(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i8_ShortInterval_SECONDS(value: ShortInterval) -> SqlResult<i8> {
    cast_to_i8_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i16_ShortInterval_SECONDS(value: ShortInterval) -> SqlResult<i16> {
    cast_to_i16_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i32_ShortInterval_SECONDS(value: ShortInterval) -> SqlResult<i32> {
    cast_to_i32_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i64_ShortInterval_SECONDS(value: ShortInterval) -> SqlResult<i64> {
    Ok(value.milliseconds())
}

cast_function!(i8, i8, ShortInterval_DAYS, ShortInterval);
cast_function!(i16, i16, ShortInterval_DAYS, ShortInterval);
cast_function!(i32, i32, ShortInterval_DAYS, ShortInterval);
cast_function!(i64, i64, ShortInterval_DAYS, ShortInterval);
cast_function!(i8, i8, ShortInterval_HOURS, ShortInterval);
cast_function!(i16, i16, ShortInterval_HOURS, ShortInterval);
cast_function!(i32, i32, ShortInterval_HOURS, ShortInterval);
cast_function!(i64, i64, ShortInterval_HOURS, ShortInterval);
cast_function!(i8, i8, ShortInterval_MINUTES, ShortInterval);
cast_function!(i16, i16, ShortInterval_MINUTES, ShortInterval);
cast_function!(i32, i32, ShortInterval_MINUTES, ShortInterval);
cast_function!(i64, i64, ShortInterval_MINUTES, ShortInterval);
cast_function!(i8, i8, ShortInterval_SECONDS, ShortInterval);
cast_function!(i16, i16, ShortInterval_SECONDS, ShortInterval);
cast_function!(i32, i32, ShortInterval_SECONDS, ShortInterval);
cast_function!(i64, i64, ShortInterval_SECONDS, ShortInterval);

#[doc(hidden)]
#[inline]
pub fn cast_to_i64_LongInterval_YEARS(value: LongInterval) -> SqlResult<i64> {
    Ok(value.months() as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_i64_LongInterval_MONTHS(value: LongInterval) -> SqlResult<i64> {
    Ok(value.months() as i64)
}

cast_function!(i64, i64, LongInterval_MONTHS, LongInterval);
cast_function!(i64, i64, LongInterval_YEARS, LongInterval);

//////// casts to Short interval

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_i8(value: i8) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_DAYS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_i16(value: i16) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_DAYS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_i32(value: i32) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_DAYS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_i64(value: i64) -> SqlResult<ShortInterval> {
    Ok(ShortInterval::new(value * 86400 * 1000))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_i8(value: i8) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_HOURS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_i16(value: i16) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_HOURS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_i32(value: i32) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_HOURS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_i64(value: i64) -> SqlResult<ShortInterval> {
    Ok(ShortInterval::new(value * 3600 * 1000))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_MINUTES_i8(value: i8) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_MINUTES_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_MINUTES_i16(value: i16) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_MINUTES_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_MINUTES_i32(value: i32) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_MINUTES_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_MINUTES_i64(value: i64) -> SqlResult<ShortInterval> {
    Ok(ShortInterval::new(value * 60 * 1000))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_SECONDS_i8(value: i8) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_SECONDS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_SECONDS_i16(value: i16) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_SECONDS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_SECONDS_i32(value: i32) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_SECONDS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_SECONDS_i64(value: i64) -> SqlResult<ShortInterval> {
    Ok(ShortInterval::new(value * 1000))
}

cast_function!(ShortInterval_DAYS, ShortInterval, i8, i8);
cast_function!(ShortInterval_DAYS, ShortInterval, i16, i16);
cast_function!(ShortInterval_DAYS, ShortInterval, i32, i32);
cast_function!(ShortInterval_DAYS, ShortInterval, i64, i64);
cast_function!(ShortInterval_HOURS, ShortInterval, i8, i8);
cast_function!(ShortInterval_HOURS, ShortInterval, i16, i16);
cast_function!(ShortInterval_HOURS, ShortInterval, i32, i32);
cast_function!(ShortInterval_HOURS, ShortInterval, i64, i64);
cast_function!(ShortInterval_MINUTES, ShortInterval, i8, i8);
cast_function!(ShortInterval_MINUTES, ShortInterval, i16, i16);
cast_function!(ShortInterval_MINUTES, ShortInterval, i32, i32);
cast_function!(ShortInterval_MINUTES, ShortInterval, i64, i64);
cast_function!(ShortInterval_SECONDS, ShortInterval, i8, i8);
cast_function!(ShortInterval_SECONDS, ShortInterval, i16, i16);
cast_function!(ShortInterval_SECONDS, ShortInterval, i32, i32);
cast_function!(ShortInterval_SECONDS, ShortInterval, i64, i64);

//////// casts to ShortIntervalN

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortIntervalN_nullN(_value: Option<()>) -> SqlResult<Option<ShortInterval>> {
    Ok(None)
}

//////// casts to LongInterval

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_i8(value: i8) -> SqlResult<LongInterval> {
    cast_to_LongInterval_YEARS_i32(value as i32)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_i16(value: i16) -> SqlResult<LongInterval> {
    cast_to_LongInterval_YEARS_i32(value as i32)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_i32(value: i32) -> SqlResult<LongInterval> {
    Ok(LongInterval::from(value * 12))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_i64(value: i64) -> SqlResult<LongInterval> {
    cast_to_LongInterval_YEARS_i32(value as i32)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_MONTHS_i8(value: i8) -> SqlResult<LongInterval> {
    cast_to_LongInterval_MONTHS_i32(value as i32)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_MONTHS_i16(value: i16) -> SqlResult<LongInterval> {
    cast_to_LongInterval_MONTHS_i32(value as i32)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_MONTHS_i32(value: i32) -> SqlResult<LongInterval> {
    Ok(LongInterval::from(value))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_MONTHS_i64(value: i64) -> SqlResult<LongInterval> {
    cast_to_LongInterval_MONTHS_i32(value as i32)
}

cast_function!(LongInterval_YEARS, LongInterval, i8, i8);
cast_function!(LongInterval_YEARS, LongInterval, i16, i16);
cast_function!(LongInterval_YEARS, LongInterval, i32, i32);
cast_function!(LongInterval_YEARS, LongInterval, i64, i64);
cast_function!(LongInterval_MONTHS, LongInterval, i8, i8);
cast_function!(LongInterval_MONTHS, LongInterval, i16, i16);
cast_function!(LongInterval_MONTHS, LongInterval, i32, i32);
cast_function!(LongInterval_MONTHS, LongInterval, i64, i64);

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_s(value: SqlString) -> SqlResult<LongInterval> {
    match value.str().parse::<i32>() {
        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
        Ok(years) => cast_to_LongInterval_YEARS_i32(years),
    }
}

static YEARS_TO_MONTHS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([-+]?\d+)(-(\d+))?$").unwrap());

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_TO_MONTHS_s(value: SqlString) -> SqlResult<LongInterval> {
    if let Some(captures) = YEARS_TO_MONTHS.captures(value.str()) {
        let yearcap = captures.get(1).unwrap().as_str();
        let mut years = match yearcap.parse::<i32>() {
            Err(e) => return Err(SqlRuntimeError::from_string(e.to_string())),
            Ok(years) => years,
        };
        let months: i32;
        match captures.get(2) {
            None => {
                months = years;
                years = 0;
            }
            _ => {
                let monthcap = captures.get(3).unwrap().as_str();
                months = match monthcap.parse() {
                    Ok(months) => months,
                    Err(e) => return Err(SqlRuntimeError::from_string(e.to_string())),
                }
            }
        }
        let months = if years < 0 {
            12 * years - months
        } else {
            12 * years + months
        };
        Ok(LongInterval::new(months))
    } else {
        Err(SqlRuntimeError::from_string(format!(
            "Interval '{value}' does not have format 'years-months'",
        )))
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_MONTHS_s(value: SqlString) -> SqlResult<LongInterval> {
    match value.str().parse::<i32>() {
        Ok(months) => cast_to_LongInterval_MONTHS_i32(months),
        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
    }
}

cast_function!(LongInterval_YEARS, LongInterval, s, SqlString);
cast_function!(LongInterval_YEARS_TO_MONTHS, LongInterval, s, SqlString);
cast_function!(LongInterval_MONTHS, LongInterval, s, SqlString);

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_LongInterval(value: LongInterval) -> SqlResult<LongInterval> {
    Ok(value)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_ShortInterval(value: ShortInterval) -> SqlResult<ShortInterval> {
    Ok(value)
}

cast_function!(LongInterval, LongInterval, LongInterval, LongInterval);
cast_function!(ShortInterval, ShortInterval, ShortInterval, ShortInterval);

static DAYS_TO_HOURS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([+-])?(\d+)\s+(\d{1,2})$").unwrap());
static DAYS_TO_MINUTES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([+-])?(\d+)\s+(\d{1,2}):(\d{1,2})$").unwrap());
static DAYS_TO_SECONDS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([+-])?(\d+)\s+(\d{1,2}):(\d{1,2}):(\d{1,2})([.](\d{1,6}))?$").unwrap()
});
static HOURS_TO_MINUTES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([+-])?(\d+):(\d{1,2})$").unwrap());
static HOURS_TO_SECONDS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([+-])?(\d+):(\d{1,2}):(\d{1,2})([.](\d{1,6}))?$").unwrap());
static MINUTES_TO_SECONDS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([+-])?(\d+):(\d{1,2})([.](\d{1,6}))?$").unwrap());
static SECONDS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([+-])?(\d+)([.](\d{1,6}))?$").unwrap());

fn validate_unit(value: i64, name: &str, max: i64) {
    if num::abs(value) >= max {
        panic!("{name} '{value}' must be between 0 and {max}");
    }
}

fn validate_hours(value: i64) {
    validate_unit(value, "hour", 24)
}

fn validate_minutes(value: i64) {
    validate_unit(value, "minute", 60)
}

fn validate_seconds(value: i64) {
    validate_unit(value, "second", 60)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_s(value: SqlString) -> SqlResult<ShortInterval> {
    match value.str().parse::<i64>() {
        Ok(value) => cast_to_ShortInterval_DAYS_i64(value),
        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_s(value: SqlString) -> SqlResult<ShortInterval> {
    match value.str().parse::<i64>() {
        Ok(value) => cast_to_ShortInterval_HOURS_i64(value),
        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
    }
}

#[doc(hidden)]
pub fn negative(captures: &Captures) -> bool {
    captures.get(1).is_some() && captures.get(1).unwrap().as_str() == "-"
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_TO_HOURS_s(value: SqlString) -> SqlResult<ShortInterval> {
    if let Some(captures) = DAYS_TO_HOURS.captures(value.str()) {
        let negative = negative(&captures);
        let daycap = captures.get(2).unwrap().as_str();
        let days = match daycap.parse::<i64>() {
            Err(e) => return Err(SqlRuntimeError::from_string(e.to_string())),
            Ok(days) => days,
        };
        let hourcap = captures.get(3).unwrap().as_str().trim();
        let hours = match hourcap.parse::<i64>() {
            Err(e) => return Err(SqlRuntimeError::from_string(e.to_string())),
            Ok(hours) => hours,
        };
        validate_hours(hours);
        let hours = if negative {
            -days * 24 - hours
        } else {
            days * 24 + hours
        };
        Ok(ShortInterval::new(hours * 3600 * 1000))
    } else {
        Err(SqlRuntimeError::from_string(format!(
            "Interval '{value}' does not have format 'days hours'",
        )))
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_MINUTES_s(value: SqlString) -> SqlResult<ShortInterval> {
    let value: i64 = value.str().parse().unwrap();
    cast_to_ShortInterval_MINUTES_i64(value)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_TO_MINUTES_s(value: SqlString) -> SqlResult<ShortInterval> {
    if let Some(captures) = DAYS_TO_MINUTES.captures(value.str()) {
        let negative = negative(&captures);
        let daycap = captures.get(2).unwrap().as_str();
        let days = match daycap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse DAYS: {}; {}",
                    daycap, e,
                )))
            }
            Ok(days) => days,
        };
        let hourcap = captures.get(3).unwrap().as_str().trim();
        let hours = match hourcap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse HOURS: {}; {}",
                    hourcap, e,
                )))
            }
            Ok(hours) => hours,
        };
        validate_hours(hours);
        let mincap = captures.get(4).unwrap().as_str().trim();
        let minutes = match mincap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse MINUTES: {}; {}",
                    mincap, e,
                )))
            }
            Ok(minutes) => minutes,
        };
        validate_minutes(minutes);
        let minutes = if negative {
            -(days * 24 + hours) * 60 - minutes
        } else {
            (days * 24 + hours) * 60 + minutes
        };
        Ok(ShortInterval::new(minutes * 60 * 1000))
    } else {
        Err(SqlRuntimeError::from_string(format!(
            "Interval '{value}' does not have format 'days hours:minutes'",
        )))
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_TO_MINUTES_s(value: SqlString) -> SqlResult<ShortInterval> {
    if let Some(captures) = HOURS_TO_MINUTES.captures(value.str()) {
        let negative = negative(&captures);
        let hourcap = captures.get(2).unwrap().as_str();
        let hours = match hourcap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse HOURS: {}; {}",
                    hourcap, e,
                )))
            }
            Ok(hours) => hours,
        };
        let mincap = captures.get(3).unwrap().as_str().trim();
        let minutes = match mincap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse MINUTES: {}; {}",
                    mincap, e,
                )))
            }
            Ok(minutes) => minutes,
        };
        validate_minutes(minutes);
        let minutes = if negative {
            -hours * 60 - minutes
        } else {
            hours * 60 + minutes
        };
        Ok(ShortInterval::new(minutes * 60 * 1000))
    } else {
        Err(SqlRuntimeError::from_string(format!(
            "Interval '{value}' does not have format 'hours:minutes'",
        )))
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_SECONDS_s(value: SqlString) -> SqlResult<ShortInterval> {
    if let Some(captures) = SECONDS.captures(value.str()) {
        let negative = negative(&captures);
        let seccap = captures.get(2).unwrap().as_str().trim();
        let seconds = match seccap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse SECONDS: {}; {}",
                    seccap, e,
                )))
            }
            Ok(seconds) => seconds,
        };
        let ms = match captures.get(3) {
            None => Ok(0i64),
            Some(_) => {
                let mscap = captures.get(4).unwrap().as_str();
                (mscap.to_owned() + "000000")[..3].parse::<i64>()
            }
        };
        let ms = match ms {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse MILLISECONDS: {}; {}",
                    captures.get(4).unwrap().as_str(),
                    e,
                )))
            }
            Ok(ms) => ms,
        };
        let ms = if negative {
            -seconds * 1000 - ms
        } else {
            seconds * 1000 + ms
        };
        Ok(ShortInterval::new(ms))
    } else {
        Err(SqlRuntimeError::from_string(format!(
            "Interval '{value}' does not have format 'seconds.fractions'",
        )))
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_TO_SECONDS_s(value: SqlString) -> SqlResult<ShortInterval> {
    if let Some(captures) = DAYS_TO_SECONDS.captures(value.str()) {
        let negative = negative(&captures);
        let daycap = captures.get(2).unwrap().as_str();
        let days = match daycap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse DAYS: {}; {}",
                    daycap, e,
                )))
            }
            Ok(days) => days,
        };
        let hourcap = captures.get(3).unwrap().as_str().trim();
        let hours = match hourcap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse HOURS: {}; {}",
                    hourcap, e,
                )))
            }
            Ok(hours) => hours,
        };
        validate_hours(hours);
        let mincap = captures.get(4).unwrap().as_str().trim();
        let minutes = match mincap.parse::<i64>() {
            Err(_) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "MINUTES is not a number: {}",
                    mincap
                )))
            }
            Ok(minutes) => minutes,
        };
        validate_minutes(minutes);
        let seccap = captures.get(5).unwrap().as_str().trim();
        let seconds = match seccap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse SECONDS: {}; {}",
                    seccap, e,
                )))
            }
            Ok(seconds) => seconds,
        };
        validate_seconds(seconds);
        let ms = match captures.get(6) {
            None => Ok(0i64),
            Some(_) => {
                let mscap = captures.get(7).unwrap().as_str();
                (mscap.to_owned() + "000000")[..3].parse::<i64>()
            }
        };
        let ms = match ms {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse MILLISECONDS: {}; {}",
                    captures.get(7).unwrap().as_str(),
                    e,
                )))
            }
            Ok(ms) => ms,
        };
        let ms = if negative {
            -((days * 24 + hours) * 60 + minutes) * 60000 - seconds * 1000 - ms
        } else {
            ((days * 24 + hours) * 60 + minutes) * 60000 + seconds * 1000 + ms
        };
        Ok(ShortInterval::new(ms))
    } else {
        Err(SqlRuntimeError::from_string(format!(
            "Interval '{value}' does not have format 'days hours:minutes:seconds'"
        )))
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_TO_SECONDS_s(value: SqlString) -> SqlResult<ShortInterval> {
    if let Some(captures) = HOURS_TO_SECONDS.captures(value.str()) {
        let negative = negative(&captures);
        let hourcap = captures.get(2).unwrap().as_str().trim();
        let hours = match hourcap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse HOURS: {}; {}",
                    hourcap, e,
                )))
            }
            Ok(hours) => hours,
        };
        let mincap = captures.get(3).unwrap().as_str().trim();
        let minutes = match mincap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse MINUTES: {}; {}",
                    mincap, e,
                )))
            }
            Ok(minutes) => minutes,
        };
        validate_minutes(minutes);
        let seccap = captures.get(4).unwrap().as_str().trim();
        let seconds = match seccap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse SECONDS: {}; {}",
                    seccap, e,
                )))
            }
            Ok(seconds) => seconds,
        };
        validate_seconds(seconds);
        let ms = match captures.get(5) {
            None => Ok(0i64),
            Some(_) => {
                let mscap = captures.get(6).unwrap().as_str();
                (mscap.to_owned() + "000000")[..3].parse::<i64>()
            }
        };
        let ms = match ms {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse MILLISECONDS: {}; {}",
                    captures.get(6).unwrap().as_str(),
                    e,
                )))
            }
            Ok(ms) => ms,
        };
        let ms = if negative {
            (-hours * 60 - minutes) * 60000 - seconds * 1000 - ms
        } else {
            (hours * 60 + minutes) * 60000 + seconds * 1000 + ms
        };
        Ok(ShortInterval::new(ms))
    } else {
        Err(SqlRuntimeError::from_string(format!(
            "Interval '{value}' does not have format 'hours:minutes:seconds.fractions'"
        )))
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_MINUTES_TO_SECONDS_s(value: SqlString) -> SqlResult<ShortInterval> {
    if let Some(captures) = MINUTES_TO_SECONDS.captures(value.str()) {
        let negative = negative(&captures);
        let mincap = captures.get(2).unwrap().as_str().trim();
        let minutes = match mincap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse MINUTES: {}; {}",
                    mincap, e,
                )))
            }
            Ok(minutes) => minutes,
        };
        let seccap = captures.get(3).unwrap().as_str().trim();
        let seconds = match seccap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse SECONDS: {}; {}",
                    seccap, e,
                )))
            }
            Ok(seconds) => seconds,
        };
        validate_seconds(seconds);
        let ms = match captures.get(4) {
            None => Ok(0i64),
            Some(_) => {
                let mscap = captures.get(5).unwrap().as_str();
                (mscap.to_owned() + "000000")[..3].parse::<i64>()
            }
        };
        let ms = match ms {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Could not parse MILLISECONDS: {}; {}",
                    captures.get(5).unwrap().as_str(),
                    e,
                )))
            }
            Ok(ms) => ms,
        };
        let ms = if negative {
            -minutes * 60000 - seconds * 1000 - ms
        } else {
            minutes * 60000 + seconds * 1000 + ms
        };
        Ok(ShortInterval::new(ms))
    } else {
        Err(SqlRuntimeError::from_string(format!(
            "Interval '{value}' does not have format 'minutes:seconds.fractions'"
        )))
    }
}

cast_function!(ShortInterval_DAYS, ShortInterval, s, SqlString);
cast_function!(ShortInterval_HOURS, ShortInterval, s, SqlString);
cast_function!(ShortInterval_DAYS_TO_HOURS, ShortInterval, s, SqlString);
cast_function!(ShortInterval_MINUTES, ShortInterval, s, SqlString);
cast_function!(ShortInterval_DAYS_TO_MINUTES, ShortInterval, s, SqlString);
cast_function!(ShortInterval_HOURS_TO_MINUTES, ShortInterval, s, SqlString);
cast_function!(ShortInterval_SECONDS, ShortInterval, s, SqlString);
cast_function!(ShortInterval_DAYS_TO_SECONDS, ShortInterval, s, SqlString);
cast_function!(ShortInterval_HOURS_TO_SECONDS, ShortInterval, s, SqlString);
cast_function!(
    ShortInterval_MINUTES_TO_SECONDS,
    ShortInterval,
    s,
    SqlString
);

//////// casts to Timestamp

#[doc(hidden)]
#[inline]
pub fn cast_to_Timestamp_s(value: SqlString) -> SqlResult<Timestamp> {
    if let Ok(v) = NaiveDateTime::parse_from_str(value.str(), "%Y-%m-%d %H:%M:%S%.f") {
        // round the number of microseconds
        let nanos = v.and_utc().timestamp_subsec_nanos();
        let nanos = (nanos + 500000) / 1000000;
        let result = Timestamp::new(v.and_utc().timestamp() * 1000 + (nanos as i64));
        //println!("Parsed successfully {} using {} into {:?} ({})",
        //         value, "%Y-%m-%d %H:%M:%S%.f", result, result.milliseconds());
        return Ok(result);
    }

    // Try just a date.
    // parse_from_str fails to parse a datetime if there is no time in the format!
    if let Ok(v) = NaiveDate::parse_from_str(value.str(), "%Y-%m-%d") {
        let dt = v.and_hms_opt(0, 0, 0).unwrap();
        let result = Timestamp::new(dt.and_utc().timestamp_millis());
        //println!("Parsed successfully {} using {} into {:?} ({})",
        //         value, "%Y-%m-%d", result, result.milliseconds());
        return Ok(result);
    }

    Err(SqlRuntimeError::from_string(format!(
        "Failed to parse '{value}' as a Timestamp"
    )))
}

cast_function!(Timestamp, Timestamp, s, SqlString);

#[doc(hidden)]
#[inline]
pub fn cast_to_Timestamp_Date(value: Date) -> SqlResult<Timestamp> {
    Ok(value.to_timestamp())
}

cast_function!(Timestamp, Timestamp, Date, Date);

#[doc(hidden)]
#[inline]
pub fn cast_to_Timestamp_Time(value: Time) -> SqlResult<Timestamp> {
    let dt = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
        value.to_time(),
    );
    Ok(Timestamp::from_naiveDateTime(dt))
}

cast_function!(Timestamp, Timestamp, Time, Time);

#[doc(hidden)]
#[inline]
pub fn cast_to_TimestampN_nullN(_value: Option<()>) -> SqlResult<Option<Timestamp>> {
    Ok(None)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_Timestamp_Timestamp(value: Timestamp) -> SqlResult<Timestamp> {
    Ok(value)
}

cast_function!(Timestamp, Timestamp, Timestamp, Timestamp);

#[doc(hidden)]
#[inline]
pub fn cast_to_Timestamp_i64(value: i64) -> SqlResult<Timestamp> {
    Ok(Timestamp::new(value))
}

cast_function!(Timestamp, Timestamp, i64, i64);

#[doc(hidden)]
#[inline]
pub fn cast_to_i64_Timestamp(value: Timestamp) -> SqlResult<i64> {
    Ok(value.milliseconds())
}

cast_function!(i64, i64, Timestamp, Timestamp);

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_Timestamp(
    value: Timestamp,
    precision: u32,
    scale: u32,
) -> SqlResult<Decimal> {
    cast_to_decimal_decimalN(Decimal::from_i64(value.milliseconds()), precision, scale)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_Timestamp(
    value: Timestamp,
    precision: u32,
    scale: u32,
) -> SqlResult<Option<Decimal>> {
    r2o(cast_to_decimal_Timestamp(value, precision, scale))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimal_TimestampN(
    value: Option<Timestamp>,
    precision: u32,
    scale: u32,
) -> SqlResult<Decimal> {
    match value {
        None => Err(cast_null("DECIMAL")),
        Some(value) => cast_to_decimal_Timestamp(value, precision, scale),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_decimalN_TimestampN(
    value: Option<Timestamp>,
    precision: u32,
    scale: u32,
) -> SqlResult<Option<Decimal>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_decimalN_Timestamp(value, precision, scale),
    }
}

macro_rules! cast_ts {
    ($type_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_Timestamp_ $type_name>](value: $arg_type) -> SqlResult<Timestamp> {
                match [< cast_to_i64_ $type_name >](value) {
                    Ok(value) => cast_to_Timestamp_i64(value),
                    Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
                }
            }

            cast_function!(Timestamp, Timestamp, $type_name, $arg_type);

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $type_name _Timestamp>](value: Timestamp) -> SqlResult<$arg_type> {
                match cast_to_i64_Timestamp(value) {
                    Ok(value) => [< cast_to_ $type_name _i64 >] (value),
                    Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
                }
            }

            cast_function!($type_name, $arg_type, Timestamp, Timestamp);
        }
    };
}

cast_ts!(i32, i32);
cast_ts!(i16, i16);
cast_ts!(i8, i8);
cast_ts!(f, F32);
cast_ts!(d, F64);

//////////////////// Other casts

#[doc(hidden)]
#[inline]
pub fn cast_to_u_i32(value: i32) -> SqlResult<usize> {
    match value.try_into() {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
    }
}

cast_function!(u, usize, i32, i32);

#[doc(hidden)]
#[inline]
pub fn cast_to_u_i64(value: i64) -> SqlResult<usize> {
    match value.try_into() {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
    }
}

cast_function!(u, usize, i64, i64);

#[doc(hidden)]
#[inline]
pub fn cast_to_i_i32(value: i32) -> SqlResult<isize> {
    Ok(value as isize)
}

cast_function!(i, isize, i32, i32);

#[doc(hidden)]
#[inline]
pub fn cast_to_i_i64(value: i64) -> SqlResult<isize> {
    Ok(value as isize)
}

cast_function!(i, isize, i64, i64);

pub fn cast_to_bytesN_nullN(_value: Option<()>, _precision: i32) -> SqlResult<Option<ByteArray>> {
    Ok(None)
}

#[doc(hidden)]
pub fn cast_to_bytes_s(value: SqlString, precision: i32) -> SqlResult<ByteArray> {
    let s = value.str();
    let array = s.as_bytes();
    Ok(ByteArray::with_size(array, precision))
}

#[doc(hidden)]
pub fn cast_to_bytesN_s(value: SqlString, precision: i32) -> SqlResult<Option<ByteArray>> {
    r2o(cast_to_bytes_s(value, precision))
}

#[doc(hidden)]
pub fn cast_to_bytes_sN(value: Option<SqlString>, precision: i32) -> SqlResult<ByteArray> {
    cast_to_bytes_s(value.unwrap(), precision)
}

#[doc(hidden)]
pub fn cast_to_bytesN_sN(value: Option<SqlString>, precision: i32) -> SqlResult<Option<ByteArray>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_bytesN_s(value, precision),
    }
}

#[doc(hidden)]
pub fn cast_to_bytes_bytes(value: ByteArray, precision: i32) -> SqlResult<ByteArray> {
    Ok(ByteArray::with_size(value.as_slice(), precision))
}

#[doc(hidden)]
pub fn cast_to_bytes_bytesN(value: Option<ByteArray>, precision: i32) -> SqlResult<ByteArray> {
    Ok(ByteArray::with_size(value.unwrap().as_slice(), precision))
}

#[doc(hidden)]
pub fn cast_to_bytesN_bytesN(
    value: Option<ByteArray>,
    precision: i32,
) -> SqlResult<Option<ByteArray>> {
    match value {
        None => Err(cast_null("BINARY")),
        Some(value) => Ok(Some(ByteArray::with_size(value.as_slice(), precision))),
    }
}

#[doc(hidden)]
pub fn cast_to_bytesN_bytes(value: ByteArray, precision: i32) -> SqlResult<Option<ByteArray>> {
    Ok(Some(ByteArray::with_size(value.as_slice(), precision)))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bytes_Uuid(value: Uuid, precision: i32) -> SqlResult<ByteArray> {
    Ok(ByteArray::with_size(value.to_bytes(), precision))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bytesN_Uuid(value: Uuid, precision: i32) -> SqlResult<Option<ByteArray>> {
    Ok(Some(ByteArray::with_size(value.to_bytes(), precision)))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bytes_UuidN(value: Option<Uuid>, precision: i32) -> SqlResult<ByteArray> {
    Ok(ByteArray::with_size(value.unwrap().to_bytes(), precision))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_bytesN_UuidN(value: Option<Uuid>, precision: i32) -> SqlResult<Option<ByteArray>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_bytesN_Uuid(value, precision),
    }
}

///////////////////// Cast to Variant

// Synthesizes 6 functions for the argument type, e.g.:
// cast_to_V_i32
// cast_to_VN_i32
// cast_to_V_i32N
// cast_to_VN_i32N
macro_rules! cast_to_variant {
    ($result_name: ident, $result_type: ty, $enum: ident) => {
        ::paste::paste! {
            // cast_to_V_i32
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ V_ $result_name >]( value: $result_type ) -> SqlResult<Variant> {
                Ok(Variant::from(value))
            }

            // cast_to_VN_i32
            #[doc(hidden)]
            pub fn [<cast_to_ VN_ $result_name >]( value: $result_type ) -> SqlResult<Option<Variant>> {
                Ok(Some(Variant::from(value)))
            }

            // cast_to_V_i32N
            #[doc(hidden)]
            pub fn [<cast_to_ V_ $result_name N>]( value: Option<$result_type> ) -> SqlResult<Variant> {
                match value {
                    None => Ok(Variant::SqlNull),
                    Some(value) => Ok(Variant::from(value)),
                }
            }

            // cast_to_VN_i32N
            #[doc(hidden)]
            pub fn [<cast_to_ VN_ $result_name N>]( value: Option<$result_type> ) -> SqlResult<Option<Variant>> {
                r2o([ <cast_to_ V_ $result_name N >](value))
            }
        }
    };
}

// Synthesizes 2 functions
// cast_to_i32N_VN
// cast_to_i32N_V
macro_rules! cast_from_variant {
    ($result_name: ident, $result_type: ty, $enum: ident) => {
        ::paste::paste! {
            // cast_to_i32N_V
            #[doc(hidden)]
            #[inline]
            pub fn [< cast_to_ $result_name N _V >](value: Variant) -> SqlResult<Option<$result_type>> {
                match value {
                    Variant::$enum(value) => Ok(Some(value)),
                    _            => Ok(None),
                }
            }

            // cast_to_i32N_VN
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_name N_ VN >]( value: Option<Variant> ) -> SqlResult<Option<$result_type>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_ $result_name N_V >](value),
                }
            }
        }
    };
}

macro_rules! cast_variant {
    ($result_name: ident, $result_type: ty, $enum: ident) => {
        cast_to_variant!($result_name, $result_type, $enum);
        cast_from_variant!($result_name, $result_type, $enum);
    };
}

macro_rules! cast_from_variant_numeric {
    ($result_name: ident, $result_type: ty) => {
        ::paste::paste! {
            // e.g., cast_to_i32N_V
            #[doc(hidden)]
            #[inline]
            pub fn [< cast_to_ $result_name N _V >](value: Variant) -> SqlResult<Option<$result_type>> {
                match value {
                    Variant::TinyInt(value) => r2o([< cast_to_ $result_name _i8 >](value)),
                    Variant::SmallInt(value) => r2o([< cast_to_ $result_name _i16 >](value)),
                    Variant::Int(value) => r2o([< cast_to_ $result_name _i32 >](value)),
                    Variant::BigInt(value) => r2o([< cast_to_ $result_name _i64 >](value)),
                    Variant::Real(value) => r2o([< cast_to_ $result_name _f >](value)),
                    Variant::Double(value) => r2o([< cast_to_ $result_name _d >](value)),
                    Variant::Decimal(value) => r2o([< cast_to_ $result_name _decimal >](value)),
                    _ => Ok(None),
                }
            }

            // e.g., cast_to_i32N_VN
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_name N_ VN >]( value: Option<Variant> ) -> SqlResult<Option<$result_type>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_ $result_name N_V >](value),
                }
            }
        }

    };
}

macro_rules! cast_variant_numeric {
    ($result_name: ident, $result_type: ty, $enum: ident) => {
        cast_to_variant!($result_name, $result_type, $enum);
        cast_from_variant_numeric!($result_name, $result_type);
    };
}

cast_variant!(b, bool, Boolean);
cast_variant_numeric!(i8, i8, TinyInt);
cast_variant_numeric!(i16, i16, SmallInt);
cast_variant_numeric!(i32, i32, Int);
cast_variant_numeric!(i64, i64, BigInt);
cast_variant_numeric!(f, F32, Real);
cast_variant_numeric!(d, F64, Double);
cast_to_variant!(decimal, Decimal, Decimal); // The other direction takes extra arguments
cast_to_variant!(s, SqlString, SqlString); // The other direction takes extra arguments
cast_to_variant!(bytes, ByteArray, Binary); // The other direction takes extra arguments
cast_variant!(Date, Date, Date);
cast_variant!(Time, Time, Time);
cast_variant!(Uuid, Uuid, Uuid);
cast_variant!(Timestamp, Timestamp, Timestamp);
cast_variant!(ShortInterval_DAYS, ShortInterval, ShortInterval);
cast_variant!(ShortInterval_HOURS, ShortInterval, ShortInterval);
cast_variant!(ShortInterval_DAYS_TO_HOURS, ShortInterval, ShortInterval);
cast_variant!(ShortInterval_MINUTES, ShortInterval, ShortInterval);
cast_variant!(ShortInterval_DAYS_TO_MINUTES, ShortInterval, ShortInterval);
cast_variant!(ShortInterval_HOURS_TO_MINUTES, ShortInterval, ShortInterval);
cast_variant!(ShortInterval_SECONDS, ShortInterval, ShortInterval);
cast_variant!(ShortInterval_DAYS_TO_SECONDS, ShortInterval, ShortInterval);
cast_variant!(ShortInterval_HOURS_TO_SECONDS, ShortInterval, ShortInterval);
cast_variant!(
    ShortInterval_MINUTES_TO_SECONDS,
    ShortInterval,
    ShortInterval
);
cast_variant!(LongInterval_YEARS_TO_MONTHS, LongInterval, LongInterval);
cast_variant!(LongInterval_MONTHS, LongInterval, LongInterval);
cast_variant!(LongInterval_YEARS, LongInterval, LongInterval);
cast_variant!(GeoPoint, GeoPoint, Geometry);

#[doc(hidden)]
pub fn cast_to_V_vec<T>(vec: Array<T>) -> SqlResult<Variant>
where
    Variant: From<T>,
    T: Clone,
{
    Ok(vec.into())
}

#[doc(hidden)]
pub fn cast_to_VN_vec<T>(vec: Array<T>) -> SqlResult<Option<Variant>>
where
    Variant: From<T>,
    T: Clone,
{
    Ok(Some(vec.into()))
}

#[doc(hidden)]
pub fn cast_to_V_vecN<T>(vec: Option<Array<T>>) -> SqlResult<Variant>
where
    Variant: From<T>,
    T: Clone,
{
    match vec {
        None => Ok(Variant::SqlNull),
        Some(vec) => Ok(vec.into()),
    }
}

#[doc(hidden)]
pub fn cast_to_VN_vecN<T>(vec: Option<Array<T>>) -> SqlResult<Option<Variant>>
where
    Variant: From<T>,
    T: Clone,
{
    r2o(cast_to_V_vecN(vec))
}

#[doc(hidden)]
pub fn cast_to_vec_V<T>(value: Variant) -> SqlResult<Array<T>>
where
    Array<T>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    match value.try_into() {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
    }
}

#[doc(hidden)]
pub fn cast_to_vec_VN<T>(value: Option<Variant>) -> SqlResult<Option<Array<T>>>
where
    Array<T>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    match value {
        None => Ok(None),
        Some(value) => cast_to_vecN_V(value),
    }
}

#[doc(hidden)]
pub fn cast_to_vecN_V<T>(value: Variant) -> SqlResult<Option<Array<T>>>
where
    Array<T>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    match value.try_into() {
        Ok(value) => Ok(Some(value)),
        Err(_) => Ok(None),
    }
}

#[doc(hidden)]
pub fn cast_to_vecN_VN<T>(value: Option<Variant>) -> SqlResult<Option<Array<T>>>
where
    Array<T>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    match value {
        None => Ok(None),
        Some(value) => cast_to_vecN_V(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_V_VN(value: Option<Variant>) -> SqlResult<Variant> {
    match value {
        None => Ok(Variant::SqlNull),
        Some(x) => Ok(x),
    }
}

/////// cast variant to map

#[doc(hidden)]
pub fn cast_to_V_map<K, V>(map: Map<K, V>) -> SqlResult<Variant>
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    Ok(map.into())
}

#[doc(hidden)]
pub fn cast_to_VN_map<K, V>(map: Map<K, V>) -> SqlResult<Option<Variant>>
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    r2o(cast_to_V_map(map))
}

pub fn cast_to_V_mapN<K, V>(map: Option<Map<K, V>>) -> SqlResult<Variant>
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    match map {
        None => Ok(Variant::SqlNull),
        Some(map) => Ok(map.into()),
    }
}

#[doc(hidden)]
pub fn cast_to_VN_mapN<K, V>(map: Option<Map<K, V>>) -> SqlResult<Option<Variant>>
where
    Variant: From<K> + From<V>,
    K: Clone + Ord,
    V: Clone,
{
    r2o(cast_to_V_mapN(map))
}

pub fn cast_to_map_V<K, V>(value: Variant) -> SqlResult<Map<K, V>>
where
    Map<K, V>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    match value.try_into() {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(e.to_string())),
    }
}

#[doc(hidden)]
pub fn cast_to_map_VN<K, V>(value: Option<Variant>) -> SqlResult<Option<Map<K, V>>>
where
    Map<K, V>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    match value {
        None => Ok(None),
        Some(value) => cast_to_mapN_V(value),
    }
}

#[doc(hidden)]
pub fn cast_to_mapN_V<K, V>(value: Variant) -> SqlResult<Option<Map<K, V>>>
where
    Map<K, V>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    match value.try_into() {
        Ok(value) => Ok(Some(value)),
        Err(_) => Ok(None),
    }
}

#[doc(hidden)]
pub fn cast_to_mapN_VN<K, V>(value: Option<Variant>) -> SqlResult<Option<Map<K, V>>>
where
    Map<K, V>: TryFrom<Variant, Error = Box<dyn Error>>,
{
    match value {
        None => Ok(None),
        Some(value) => cast_to_mapN_V(value),
    }
}

#[doc(hidden)]
pub fn cast_to_Uuid_s(value: SqlString) -> SqlResult<Uuid> {
    Ok(Uuid::from_ref(value.str()))
}

cast_function!(Uuid, Uuid, s, SqlString);

#[doc(hidden)]
pub fn cast_to_Uuid_bytes(value: ByteArray) -> SqlResult<Uuid> {
    if value.length() < 16 {
        Err(SqlRuntimeError::from_strng(
            "Need at least 16 bytes in BINARY value to create an UUID",
        ))
    } else {
        let slice = value.as_slice();
        let slice = unsafe { *(slice.as_ptr() as *const [u8; 16]) };
        Ok(Uuid::from_bytes(slice))
    }
}

cast_function!(Uuid, Uuid, bytes, ByteArray);
