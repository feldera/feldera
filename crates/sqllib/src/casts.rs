//! Implementation of various cast operations.

#![allow(non_snake_case)]

use crate::{
    array::Array,
    binary::{to_hex_, ByteArray},
    error::{r2o, SqlResult, SqlRuntimeError},
    geopoint::*,
    interval::*,
    map::Map,
    source::SourceMap,
    timestamp::*,
    uuid::*,
    variant::*,
    DynamicDecimal, SqlDecimal, SqlString, Weight,
};

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use dbsp::algebra::{HasOne, HasZero, F32, F64};
use num::{One, Zero};
use num_traits::cast::NumCast;
use regex::{Captures, Regex};
use std::cmp::{min, Ordering};
use std::error::Error;
use std::str::FromStr;
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
        "u8" => "TINYINT UNSIGNED",
        "u16" => "SHORTINT UNSIGNED",
        "u32" => "INTEGER UNSIGNED",
        "u64" => "BIGINT UNSIGNED",
        "f" => "REAL",
        "d" => "FLOAT",
        "Timestamp" => "TIMESTAMP",
        "Date" => "DATE",
        "Time" => "TIME",
        "SqlDecimal" => "DECIMAL",
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
        "u8" => "TINYINT UNSIGNED",
        "u16" => "SHORTINT UNSIGNED",
        "u32" => "INTEGER UNSIGNED",
        "u64" => "BIGINT UNSIGNED",
        "F32" => "REAL",
        "F64" => "FLOAT",
        "Timestamp" => "Timestamp",
        "Date" => "DATE",
        "Time" => "TIME",
        "SqlDecimal" => "DECIMAL",
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

/// Standard implementation: if the result is Error, panic
#[doc(hidden)]
pub fn handle_error<T>(value: SqlResult<T>) -> T {
    match value {
        Err(ce) => panic!("{}", *ce),
        Ok(v) => v,
    }
}

/// Panic, but report source position
#[doc(hidden)]
pub fn handle_error_with_position<T>(
    operator_hash: &'static str,
    id: u32,
    map: &'static SourceMap,
    value: SqlResult<T>,
) -> T {
    match value {
        Err(ce) => match map.getPosition(operator_hash, id) {
            None => panic!("{}", *ce),
            Some(position) => panic!("{}: {}", position, *ce),
        },
        Ok(v) => v,
    }
}

/// Safe cast implementation: if the result is Error, return null (None).
// The result is always Option<T>.
#[doc(hidden)]
pub fn handle_error_safe<T>(value: SqlResult<Option<T>>) -> Option<T> {
    value.unwrap_or_default()
}

fn cast_null(t: &str) -> Box<SqlRuntimeError> {
    SqlRuntimeError::from_string(format!("cast of NULL value to non-null type {}", t))
}

// Creates three cast functions based on an existing one
// The original function is
// cast_to_ $result_name _ $type_name( value: $arg_type ) -> SqlResult<$result_type>
macro_rules! cast_function {
    ($result_name: ident $(< $( const $var:ident : $ty: ty),* >)?, $result_type: ty, $type_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_name N_ $type_name>] $(< $( const $var : $ty),* >)? ( value: $arg_type ) -> SqlResult<Option<$result_type>> {
                r2o([<cast_to_ $result_name _ $type_name>] $(:: < $($var),* >)? (value))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_name _ $type_name N >] $(< $( const $var : $ty),* >)? ( value: Option<$arg_type> ) -> SqlResult<$result_type> {
                match value {
                    None => Err(cn!($type_name)),
                    Some(value) => [<cast_to_ $result_name _ $type_name>] $(:: < $($var),* >)? (value),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_name N_ $type_name N >] $(< $( const $var : $ty),* >)? ( value: Option<$arg_type> ) -> SqlResult<Option<$result_type>> {
                match value {
                    None => Ok(None),
                    Some(v) => r2o([<cast_to_ $result_name _ $type_name >] $(:: < $($var),* >)? (v)),
                }
            }
        }
    };
}

/////////// cast to b

macro_rules! cast_to_b {
    ($type_name: ident $(< $( const $var: ident : $ty: ty),* >)?, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_b_ $type_name>] $(< $( const $var : $ty ),* >)? ( value: $arg_type ) -> SqlResult<bool> {
                Ok(value != <$arg_type as num::Zero>::zero())
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_b_ $type_name N >] $(< $( const $var : $ty ),* >)? ( value: Option<$arg_type> ) -> SqlResult<bool> {
                match value {
                    None => Err(cast_null("bool")),
                    Some(value) => [<cast_to_b_ $type_name>] $(:: < $( $var ),* >)? (value),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_bN_ $type_name >] $(< $( const $var : $ty ),* >)? ( value: $arg_type ) -> SqlResult<Option<bool>> {
                r2o([< cast_to_b_ $type_name >] $(:: < $( $var ),* >)? (value))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_bN_ $type_name N >] $(< $( const $var : $ty ),* >)? ( value: Option<$arg_type> ) -> SqlResult<Option<bool>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_bN_ $type_name >] $(:: < $( $var ),* >)? (value),
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

cast_to_b!(SqlDecimal<const P: usize, const S: usize>, SqlDecimal<P, S>);
cast_to_b_fp!(d, F64);
cast_to_b_fp!(f, F32);
cast_to_b!(i8, i8);
cast_to_b!(i16, i16);
cast_to_b!(i32, i32);
cast_to_b!(i64, i64);
cast_to_b!(u8, u8);
cast_to_b!(u16, u16);
cast_to_b!(u32, u32);
cast_to_b!(u64, u64);
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
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to DATE: {}",
            e
        ))),
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
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to TIME: {}",
            e
        ))),
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

/////////// cast to SqlDecimal

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_b<const P: usize, const S: usize>(
    value: bool,
) -> SqlResult<SqlDecimal<P, S>> {
    if value {
        Ok(<SqlDecimal<P, S> as One>::one())
    } else {
        Ok(<SqlDecimal<P, S> as Zero>::zero())
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_bN<const P: usize, const S: usize>(
    value: Option<bool>,
) -> SqlResult<SqlDecimal<P, S>> {
    match value {
        None => Err(cast_null("DECIMAL")),
        Some(value) => cast_to_SqlDecimal_b::<P, S>(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
>(
    value: SqlDecimal<P1, S1>,
) -> SqlResult<SqlDecimal<P0, S0>> {
    let result = value.convert();
    match result {
        None => Err(SqlRuntimeError::from_string(
            format!("Cannot represent {value} as DECIMAL({P0}, {S0}): precision of DECIMAL type too small to represent value"))),
        Some(value) => Ok(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_SqlDecimalN<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
>(
    value: Option<SqlDecimal<P1, S1>>,
) -> SqlResult<SqlDecimal<P0, S0>> {
    match value {
        None => Err(cast_null("DECIMAL")),
        Some(value) => cast_to_SqlDecimal_SqlDecimal::<P0, S0, P1, S1>(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_d<const P: usize, const S: usize>(
    value: F64,
) -> SqlResult<SqlDecimal<P, S>> {
    match SqlDecimal::<P, S>::try_from(value.into_inner()) {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to DECIMAL({P}, {S}): {}",
            e
        ))),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_dN<const P: usize, const S: usize>(
    value: Option<F64>,
) -> SqlResult<SqlDecimal<P, S>> {
    match value {
        None => Err(cast_null("DECIMAL")),
        Some(value) => cast_to_SqlDecimal_d::<P, S>(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_f<const P: usize, const S: usize>(
    value: F32,
) -> SqlResult<SqlDecimal<P, S>> {
    match SqlDecimal::<P, S>::try_from(value.into_inner() as f64) {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to DECIMAL({P}, {S}): {}",
            e
        ))),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_fN<const P: usize, const S: usize>(
    value: Option<F32>,
) -> SqlResult<SqlDecimal<P, S>> {
    match value {
        None => Err(cast_null("DECIMAL")),
        Some(value) => cast_to_SqlDecimal_f::<P, S>(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_s<const P: usize, const S: usize>(
    value: SqlString,
) -> SqlResult<SqlDecimal<P, S>> {
    let str = value.str().trim();
    match str.parse() {
        Err(_) => Err(SqlRuntimeError::from_string(format!(
            "While converting '{}' to DECIMAL: parse error",
            value
        ))),
        Ok(result) => Ok(result),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_sN<const P: usize, const S: usize>(
    value: Option<SqlString>,
) -> SqlResult<SqlDecimal<P, S>> {
    match value {
        None => Ok(<SqlDecimal<P, S> as num_traits::Zero>::zero()),
        Some(value) => cast_to_SqlDecimal_s::<P, S>(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_V<const P: usize, const S: usize>(
    value: Variant,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    match value {
        Variant::TinyInt(i) => r2o(cast_to_SqlDecimal_i8::<P, S>(i)),
        Variant::SmallInt(i) => r2o(cast_to_SqlDecimal_i16::<P, S>(i)),
        Variant::Int(i) => r2o(cast_to_SqlDecimal_i32::<P, S>(i)),
        Variant::BigInt(i) => r2o(cast_to_SqlDecimal_i64::<P, S>(i)),
        Variant::UTinyInt(i) => r2o(cast_to_SqlDecimal_u8::<P, S>(i)),
        Variant::USmallInt(i) => r2o(cast_to_SqlDecimal_u16::<P, S>(i)),
        Variant::UInt(i) => r2o(cast_to_SqlDecimal_u32::<P, S>(i)),
        Variant::UBigInt(i) => r2o(cast_to_SqlDecimal_u64::<P, S>(i)),
        Variant::Real(f) => r2o(cast_to_SqlDecimal_f::<P, S>(f)),
        Variant::Double(f) => r2o(cast_to_SqlDecimal_d::<P, S>(f)),
        Variant::SqlDecimal(d) => {
            let dd = DynamicDecimal::new(d.0, d.1);
            match SqlDecimal::<P, S>::try_from(dd) {
                Err(e) => Err(SqlRuntimeError::from_string(format!(
                    "Error while converting 'VARIANT({:?})' to DECIMAL({P}, {S}): {}",
                    value, e
                ))),
                Ok(value) => Ok(Some(value)),
            }
        }
        _ => Ok(None),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_VN<const P: usize, const S: usize>(
    value: Option<Variant>,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_SqlDecimalN_V::<P, S>(value),
    }
}

macro_rules! cast_to_sqldecimal {
    ($type_name: ident, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_SqlDecimal_ $type_name> ]<const P: usize, const S: usize>( value: $arg_type ) -> SqlResult<SqlDecimal<P, S>> {
                match SqlDecimal::<P, S>::try_from(value) {
                    Ok(value) => Ok(value),
                    Err(e) => Err(SqlRuntimeError::from_string(
                        format!("Error converting {value} to DECIMAL({P}, {S}): {}", e.to_string())
                    )),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_SqlDecimal_ $type_name N> ]<const P: usize, const S: usize>( value: Option<$arg_type> ) -> SqlResult<SqlDecimal<P, S>> {
                match value {
                    None => Err(cast_null("DECIMAL")),
                    Some(value) => [<cast_to_SqlDecimal_ $type_name >]::<P, S>(value),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_SqlDecimalN_ $type_name> ]<const P: usize, const S: usize>( value: $arg_type ) -> SqlResult<Option<SqlDecimal<P, S>>> {
                r2o([< cast_to_SqlDecimal_ $type_name >]::<P, S>(value))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_SqlDecimalN_ $type_name N> ]<const P: usize, const S: usize>( value: Option<$arg_type> ) -> SqlResult<Option<SqlDecimal<P, S>>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_SqlDecimalN_ $type_name >]::<P, S>(value),
                }
            }
        }
    }
}

cast_to_sqldecimal!(i, isize);
cast_to_sqldecimal!(i8, i8);
cast_to_sqldecimal!(i16, i16);
cast_to_sqldecimal!(i32, i32);
cast_to_sqldecimal!(i64, i64);
cast_to_sqldecimal!(u8, u8);
cast_to_sqldecimal!(u16, u16);
cast_to_sqldecimal!(u32, u32);
cast_to_sqldecimal!(u64, u64);
cast_to_sqldecimal!(u, usize);

/////////// cast to decimalN

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_nullN<const P: usize, const S: usize>(
    _value: Option<()>,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    Ok(None)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_b<const P: usize, const S: usize>(
    value: bool,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    r2o(cast_to_SqlDecimal_b::<P, S>(value))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_bN<const P: usize, const S: usize>(
    value: Option<bool>,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_SqlDecimalN_b::<P, S>(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_SqlDecimal<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
>(
    value: SqlDecimal<P1, S1>,
) -> SqlResult<Option<SqlDecimal<P0, S0>>> {
    r2o(cast_to_SqlDecimal_SqlDecimal::<P0, S0, P1, S1>(value))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_SqlDecimalN<
    const P0: usize,
    const S0: usize,
    const P1: usize,
    const S1: usize,
>(
    value: Option<SqlDecimal<P1, S1>>,
) -> SqlResult<Option<SqlDecimal<P0, S0>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_SqlDecimalN_SqlDecimal::<P0, S0, P1, S1>(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_d<const P: usize, const S: usize>(
    value: F64,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    r2o(cast_to_SqlDecimal_d::<P, S>(value))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_dN<const P: usize, const S: usize>(
    value: Option<F64>,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_SqlDecimalN_d::<P, S>(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_f<const P: usize, const S: usize>(
    value: F32,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    r2o(cast_to_SqlDecimal_f::<P, S>(value))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_fN<const P: usize, const S: usize>(
    value: Option<F32>,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_SqlDecimalN_f::<P, S>(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_s<const P: usize, const S: usize>(
    value: SqlString,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    match SqlDecimal::<P, S>::from_str(value.str()) {
        Ok(value) => Ok(Some(value)),
        Err(_) => Err(SqlRuntimeError::from_string(format!(
            "Cannot parse {} into a DECIMAL({P}, {S})",
            value.str(),
        ))),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_sN<const P: usize, const S: usize>(
    value: Option<SqlString>,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_SqlDecimalN_s::<P, S>(value),
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
                let result: Option<$result_base_type> = NumCast::from(value);
                match result {
                    None => Err(SqlRuntimeError::from_string(format!("Cannot convert {value} to {}", tn!($result_type)))),
                    Some(value) => Ok($result_type::from(value)),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $result_type_name _ $type_name N >]( value: Option<$arg_type> ) -> SqlResult<$result_type> {
                match value {
                    None => Err(cn!($result_type)),
                    Some(value) => {
                        let result: Option<$result_base_type> = NumCast::from(value);
                        match result {
                            None => Err(SqlRuntimeError::from_string(format!("Cannot convert {value} to {}", tn!($result_type)))),
                            Some(value) => Ok($result_type::from(value)),
                        }
                    }
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
pub fn cast_to_d_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlResult<F64> {
    Ok(F64::from(
        <f64 as std::convert::From<SqlDecimal<P, S>>>::from(value),
    ))
}

cast_function!(d <const P: usize, const S: usize>, F64, SqlDecimal, SqlDecimal<P, S>);

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
cast_to_fps!(i128, i128);
cast_to_fps!(u8, u8);
cast_to_fps!(u16, u16);
cast_to_fps!(u32, u32);
cast_to_fps!(u64, u64);
cast_to_fps!(u128, u128);
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
pub fn cast_to_f_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlResult<F32> {
    Ok(F32::from(
        <f64 as std::convert::From<SqlDecimal<P, S>>>::from(value) as f32,
    ))
}

cast_function!(f <const P: usize, const S: usize>, F32, SqlDecimal, SqlDecimal<P, S>);

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
    assert!(!is_unlimited_size(size));
    let sz = size as usize;
    match value.len().cmp(&sz) {
        Ordering::Equal => value.to_string(),
        Ordering::Greater => truncate(value, sz),
        Ordering::Less => format!("{value:<sz$}"),
    }
}

/// Make sure that the specified string does not exceed
/// the specified size.
#[inline(always)]
#[doc(hidden)]
fn limit_string(value: &str, size: i32) -> String {
    if is_unlimited_size(size) {
        value.to_string()
    } else {
        let sz = size as usize;
        if value.len() < sz {
            value.to_string()
        } else {
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
    ($type_name: ident $(< $( const $var:ident : $ty: ty),* >)?, $arg_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_s_ $type_name N >] $(< $( const $var : $ty ),* >)? ( value: Option<$arg_type>, size: i32, fixed: bool ) -> SqlResult<SqlString> {
                match value {
                    None => Err(cast_null("VARCHAR")),
                    Some(value) => [<cast_to_s_ $type_name>] $(:: < $($var),* >)? (value, size, fixed),
                }
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_sN_ $type_name >] $(< $( const $var : $ty ),* >)? ( value: $arg_type, size: i32, fixed: bool ) -> SqlResult<Option<SqlString>> {
                r2o(([< cast_to_s_ $type_name >] $(:: < $($var),* >)? (value, size, fixed)))
            }

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_sN_ $type_name N >] $(< $( const $var : $ty ),* >)? ( value: Option<$arg_type>, size: i32, fixed: bool ) -> SqlResult<Option<SqlString>> {
                match value {
                    None => Ok(None),
                    Some(value) => [<cast_to_sN_ $type_name >] $(:: < $($var),* >)? (value, size, fixed),
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
pub fn cast_to_s_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
    size: i32,
    fixed: bool,
) -> SqlResult<SqlString> {
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
    T: num::Float + ryu::Float,
{
    let mut buffer = ryu::Buffer::new();
    let result = buffer.format(v);
    limit_or_size_string(result, size, fixed)
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
pub fn cast_to_s_u8(value: u8, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result = value.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_u16(value: u16, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result = value.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_u32(value: u32, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result = value.to_string();
    limit_or_size_string(&result, size, fixed)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_s_u64(value: u64, size: i32, fixed: bool) -> SqlResult<SqlString> {
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
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting VARIANT to BINARY: {}",
            e
        ))),
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

#[doc(hidden)]
pub fn cast_to_s_bytes(value: ByteArray, size: i32, fixed: bool) -> SqlResult<SqlString> {
    let result = to_hex_(value);
    limit_or_size_string(result.str(), size, fixed)
}

cast_to_string!(b, bool);
cast_to_string!(SqlDecimal<const P: usize, const S: usize>, SqlDecimal<P, S>);
cast_to_string!(f, F32);
cast_to_string!(d, F64);
cast_to_string!(s, SqlString);
cast_to_string!(i, isize);
cast_to_string!(u, usize);
cast_to_string!(i8, i8);
cast_to_string!(i16, i16);
cast_to_string!(i32, i32);
cast_to_string!(i64, i64);
cast_to_string!(u8, u8);
cast_to_string!(u16, u16);
cast_to_string!(u32, u32);
cast_to_string!(u64, u64);
cast_to_string!(Timestamp, Timestamp);
cast_to_string!(Time, Time);
cast_to_string!(Date, Date);
cast_to_string!(bytes, ByteArray);
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
                    Err(e) => Err(SqlRuntimeError::from_string(
                        format!("Error converting {value} to {}: {}", tn!($result_type), e)
                    )),
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
            pub fn [< cast_to_ $result_type _nullN >](_value: Option<()>) -> SqlResult<$result_type> {
                Err(SqlRuntimeError::from_string(
                    format!("Casting NULL value to {}", tn!($result_type))
                ))
            }

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
            pub fn [< cast_to_ $result_type _SqlDecimal >]<const P: usize, const S: usize>(value: SqlDecimal<P, S>) -> SqlResult<$result_type> {
                match value.try_into() {
                    Ok(value) => Ok(value),
                    Err(e) => Err(SqlRuntimeError::from_string(format!(
                        "Error converting DECIMAL({P},{S}) {value} to {}: {}",
                        tn!($result_type), e
                    ))),
                }
            }

            cast_function!($result_type <const P: usize, const S: usize>, $result_type, SqlDecimal, SqlDecimal<P, S>);

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
                    Err(e) => Err(SqlRuntimeError::from_string(
                        format!("Error converting {value} to {}: {}", tn!($result_type), e)
                    )),
                }
            }

            cast_function!($result_type, $result_type, s, SqlString);

            // From other integers

            cast_to_i_i!($result_type, i8, i8);
            cast_to_i_i!($result_type, i16, i16);
            cast_to_i_i!($result_type, i32, i32);
            cast_to_i_i!($result_type, i64, i64);
            cast_to_i_i!($result_type, i128, i128);
            cast_to_i_i!($result_type, u8, u8);
            cast_to_i_i!($result_type, u16, u16);
            cast_to_i_i!($result_type, u32, u32);
            cast_to_i_i!($result_type, u64, u64);
            cast_to_i_i!($result_type, u128, u128);
            cast_to_i_i!($result_type, i, isize);
            cast_to_i_i!($result_type, u, usize);
        }
    }
}

cast_to_i!(i8);
cast_to_i!(i16);
cast_to_i!(i32);
cast_to_i!(i64);
cast_to_i!(i128);
cast_to_i!(u8);
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

#[doc(hidden)]
#[inline]
pub fn cast_to_u8_ShortInterval_DAYS(value: ShortInterval) -> SqlResult<u8> {
    cast_to_u8_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u16_ShortInterval_DAYS(value: ShortInterval) -> SqlResult<u16> {
    cast_to_u16_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u32_ShortInterval_DAYS(value: ShortInterval) -> SqlResult<u32> {
    cast_to_u32_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u64_ShortInterval_DAYS(value: ShortInterval) -> SqlResult<u64> {
    cast_to_u64_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u8_ShortInterval_HOURS(value: ShortInterval) -> SqlResult<u8> {
    cast_to_u8_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u16_ShortInterval_HOURS(value: ShortInterval) -> SqlResult<u16> {
    cast_to_u16_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u32_ShortInterval_HOURS(value: ShortInterval) -> SqlResult<u32> {
    cast_to_u32_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u64_ShortInterval_HOURS(value: ShortInterval) -> SqlResult<u64> {
    cast_to_u64_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u8_ShortInterval_MINUTES(value: ShortInterval) -> SqlResult<u8> {
    cast_to_u8_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u16_ShortInterval_MINUTES(value: ShortInterval) -> SqlResult<u16> {
    cast_to_u16_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u32_ShortInterval_MINUTES(value: ShortInterval) -> SqlResult<u32> {
    cast_to_u32_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u64_ShortInterval_MINUTES(value: ShortInterval) -> SqlResult<u64> {
    cast_to_u64_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u8_ShortInterval_SECONDS(value: ShortInterval) -> SqlResult<u8> {
    cast_to_u8_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u16_ShortInterval_SECONDS(value: ShortInterval) -> SqlResult<u16> {
    cast_to_u16_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u32_ShortInterval_SECONDS(value: ShortInterval) -> SqlResult<u32> {
    cast_to_u32_i64(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_u64_ShortInterval_SECONDS(value: ShortInterval) -> SqlResult<u64> {
    cast_to_u64_i64(value.milliseconds())
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

cast_function!(u8, u8, ShortInterval_DAYS, ShortInterval);
cast_function!(u16, u16, ShortInterval_DAYS, ShortInterval);
cast_function!(u32, u32, ShortInterval_DAYS, ShortInterval);
cast_function!(u64, u64, ShortInterval_DAYS, ShortInterval);
cast_function!(u8, u8, ShortInterval_HOURS, ShortInterval);
cast_function!(u16, u16, ShortInterval_HOURS, ShortInterval);
cast_function!(u32, u32, ShortInterval_HOURS, ShortInterval);
cast_function!(u64, u64, ShortInterval_HOURS, ShortInterval);
cast_function!(u8, u8, ShortInterval_MINUTES, ShortInterval);
cast_function!(u16, u16, ShortInterval_MINUTES, ShortInterval);
cast_function!(u32, u32, ShortInterval_MINUTES, ShortInterval);
cast_function!(u64, u64, ShortInterval_MINUTES, ShortInterval);
cast_function!(u8, u8, ShortInterval_SECONDS, ShortInterval);
cast_function!(u16, u16, ShortInterval_SECONDS, ShortInterval);
cast_function!(u32, u32, ShortInterval_SECONDS, ShortInterval);
cast_function!(u64, u64, ShortInterval_SECONDS, ShortInterval);

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
    let val = value.checked_mul(86400 * 1000);
    match val {
        None => Err(SqlRuntimeError::from_string(format!(
            "Overflow during conversion of {value} to INTERVAL DAYS"
        ))),
        Some(value) => Ok(ShortInterval::new(value)),
    }
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
    let val = value.checked_mul(3600 * 1000);
    match val {
        None => Err(SqlRuntimeError::from_string(format!(
            "Overflow during conversion of {value} to INTERVAL HOURS"
        ))),
        Some(value) => Ok(ShortInterval::new(value)),
    }
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
    let val = value.checked_mul(60 * 1000);
    match val {
        None => Err(SqlRuntimeError::from_string(format!(
            "Overflow during conversion of {value} to INTERVAL MINUTES"
        ))),
        Some(value) => Ok(ShortInterval::new(value)),
    }
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
    let val = value.checked_mul(1000);
    match val {
        None => Err(SqlRuntimeError::from_string(format!(
            "Overflow during conversion of {value} to INTERVAL SECONDS"
        ))),
        Some(value) => Ok(ShortInterval::new(value)),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_u8(value: u8) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_DAYS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_u16(value: u16) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_DAYS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_u32(value: u32) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_DAYS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_DAYS_u64(value: u64) -> SqlResult<ShortInterval> {
    let value = match <i64 as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to INTERVAL DAYS"
            )))
        }
    };
    let val = value.checked_mul(86400 * 1000);
    match val {
        None => Err(SqlRuntimeError::from_string(format!(
            "Overflow during conversion of {value} to INTERVAL DAYS"
        ))),
        Some(value) => Ok(ShortInterval::new(value)),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_u8(value: u8) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_HOURS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_u16(value: u16) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_HOURS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_u32(value: u32) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_HOURS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_u64(value: u64) -> SqlResult<ShortInterval> {
    let value = match <i64 as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to INTERVAL HOURS"
            )))
        }
    };
    let val = value.checked_mul(3600 * 1000);
    match val {
        None => Err(SqlRuntimeError::from_string(format!(
            "Overflow during conversion of {value} to INTERVAL HOURS"
        ))),
        Some(value) => Ok(ShortInterval::new(value)),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_MINUTES_u8(value: u8) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_MINUTES_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_MINUTES_u16(value: u16) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_MINUTES_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_MINUTES_u32(value: u32) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_MINUTES_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_MINUTES_u64(value: u64) -> SqlResult<ShortInterval> {
    let value = match <i64 as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to INTERVAL MINUTES"
            )))
        }
    };
    let val = value.checked_mul(60 * 1000);
    match val {
        Some(value) => Ok(ShortInterval::new(value)),
        None => Err(SqlRuntimeError::from_string(format!(
            "Overflow during conversion of {value} to INTERVAL MINUTES"
        ))),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_SECONDS_u8(value: u8) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_SECONDS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_SECONDS_u16(value: u16) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_SECONDS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_SECONDS_u32(value: u32) -> SqlResult<ShortInterval> {
    cast_to_ShortInterval_SECONDS_i64(value as i64)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_SECONDS_u64(value: u64) -> SqlResult<ShortInterval> {
    let value = match <i64 as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to INTERVAL SECONDS"
            )))
        }
    };
    let val = value.checked_mul(1000);
    match val {
        Some(value) => Ok(ShortInterval::new(value)),
        None => Err(SqlRuntimeError::from_string(format!(
            "Overflow during conversion of {value} to INTERVAL SECONDS"
        ))),
    }
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

cast_function!(ShortInterval_DAYS, ShortInterval, u8, u8);
cast_function!(ShortInterval_DAYS, ShortInterval, u16, u16);
cast_function!(ShortInterval_DAYS, ShortInterval, u32, u32);
cast_function!(ShortInterval_DAYS, ShortInterval, u64, u64);
cast_function!(ShortInterval_HOURS, ShortInterval, u8, u8);
cast_function!(ShortInterval_HOURS, ShortInterval, u16, u16);
cast_function!(ShortInterval_HOURS, ShortInterval, u32, u32);
cast_function!(ShortInterval_HOURS, ShortInterval, u64, u64);
cast_function!(ShortInterval_MINUTES, ShortInterval, u8, u8);
cast_function!(ShortInterval_MINUTES, ShortInterval, u16, u16);
cast_function!(ShortInterval_MINUTES, ShortInterval, u32, u32);
cast_function!(ShortInterval_MINUTES, ShortInterval, u64, u64);
cast_function!(ShortInterval_SECONDS, ShortInterval, u8, u8);
cast_function!(ShortInterval_SECONDS, ShortInterval, u16, u16);
cast_function!(ShortInterval_SECONDS, ShortInterval, u32, u32);
cast_function!(ShortInterval_SECONDS, ShortInterval, u64, u64);

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
    let val = value.checked_mul(12);
    match val {
        Some(value) => Ok(LongInterval::from(value)),
        None => Err(SqlRuntimeError::from_string(format!(
            "Overflow during conversion of {value} to INTERVAL YEARS"
        ))),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_i64(value: i64) -> SqlResult<LongInterval> {
    let value = match <i32 as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to INTERVAL YEARS"
            )))
        }
    };
    cast_to_LongInterval_YEARS_i32(value)
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
    let value = match <i32 as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to INTERVAL MONTHS"
            )))
        }
    };
    cast_to_LongInterval_MONTHS_i32(value)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_u8(value: u8) -> SqlResult<LongInterval> {
    cast_to_LongInterval_YEARS_i32(value as i32)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_u16(value: u16) -> SqlResult<LongInterval> {
    cast_to_LongInterval_YEARS_i32(value as i32)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_u32(value: u32) -> SqlResult<LongInterval> {
    let value = match <i32 as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to INTERVAL YEARS"
            )))
        }
    };
    cast_to_LongInterval_YEARS_i32(value)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_u64(value: u64) -> SqlResult<LongInterval> {
    let value = match <i32 as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to INTERVAL YEARS"
            )))
        }
    };
    cast_to_LongInterval_YEARS_i32(value)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_MONTHS_u8(value: u8) -> SqlResult<LongInterval> {
    cast_to_LongInterval_MONTHS_i32(value as i32)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_MONTHS_u16(value: u16) -> SqlResult<LongInterval> {
    cast_to_LongInterval_MONTHS_i32(value as i32)
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_MONTHS_u32(value: u32) -> SqlResult<LongInterval> {
    let value = match <i32 as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to INTERVAL MONTHS"
            )))
        }
    };
    Ok(LongInterval::from(value))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_MONTHS_u64(value: u64) -> SqlResult<LongInterval> {
    let value = match <i32 as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to INTERVAL MONTHS"
            )))
        }
    };
    cast_to_LongInterval_MONTHS_i32(value)
}

cast_function!(LongInterval_YEARS, LongInterval, i8, i8);
cast_function!(LongInterval_YEARS, LongInterval, i16, i16);
cast_function!(LongInterval_YEARS, LongInterval, i32, i32);
cast_function!(LongInterval_YEARS, LongInterval, i64, i64);
cast_function!(LongInterval_MONTHS, LongInterval, i8, i8);
cast_function!(LongInterval_MONTHS, LongInterval, i16, i16);
cast_function!(LongInterval_MONTHS, LongInterval, i32, i32);
cast_function!(LongInterval_MONTHS, LongInterval, i64, i64);

cast_function!(LongInterval_YEARS, LongInterval, u8, u8);
cast_function!(LongInterval_YEARS, LongInterval, u16, u16);
cast_function!(LongInterval_YEARS, LongInterval, u32, u32);
cast_function!(LongInterval_YEARS, LongInterval, u64, u64);
cast_function!(LongInterval_MONTHS, LongInterval, u8, u8);
cast_function!(LongInterval_MONTHS, LongInterval, u16, u16);
cast_function!(LongInterval_MONTHS, LongInterval, u32, u32);
cast_function!(LongInterval_MONTHS, LongInterval, u64, u64);

#[doc(hidden)]
#[inline]
pub fn cast_to_LongInterval_YEARS_s(value: SqlString) -> SqlResult<LongInterval> {
    match value.str().parse::<i32>() {
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to INTERVAL YEARS: {}",
            e
        ))),
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
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Error converting {value} to INTERVAL YEARS TO MONTHS: {}",
                    e
                )))
            }
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
                    Err(e) => {
                        return Err(SqlRuntimeError::from_string(format!(
                            "Error converting {value} to INTERVAL YEARS TO MONTHS: {}",
                            e
                        )))
                    }
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
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to INTERVAL MONTHS: {}",
            e
        ))),
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
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to INTERVAL DAYS: {}",
            e
        ))),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_ShortInterval_HOURS_s(value: SqlString) -> SqlResult<ShortInterval> {
    match value.str().parse::<i64>() {
        Ok(value) => cast_to_ShortInterval_HOURS_i64(value),
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to INTERVAL HOURS: {}",
            e
        ))),
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
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Error converting {value} to INTERVAL DAYS TO HOURS: {}",
                    e
                )))
            }
            Ok(days) => days,
        };
        let hourcap = captures.get(3).unwrap().as_str().trim();
        let hours = match hourcap.parse::<i64>() {
            Err(e) => {
                return Err(SqlRuntimeError::from_string(format!(
                    "Error converting {value} to INTERVAL DAYS TO HOURS: {}",
                    e
                )))
            }
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
    // Calcite ignores sub-second units
    Ok(Timestamp::new((value / 1000) * 1000))
}

cast_function!(Timestamp, Timestamp, i64, i64);

#[doc(hidden)]
#[inline]
pub fn cast_to_Timestamp_SqlDecimal<const P: usize, const S: usize>(
    value: SqlDecimal<P, S>,
) -> SqlResult<Timestamp> {
    match TryInto::<i64>::try_into(value) {
        // Calcite ignores sub-second units
        Ok(value) => Ok(Timestamp::new((value / 1000) * 1000)),
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to TIMESTAMP: {}",
            e
        ))),
    }
}

cast_function!(Timestamp <const P: usize, const S: usize>, Timestamp, SqlDecimal, SqlDecimal<P, S>);

#[doc(hidden)]
#[inline]
pub fn cast_to_Timestamp_u64(value: u64) -> SqlResult<Timestamp> {
    let result: Result<i64, _> = value.try_into();
    match result {
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to TIMESTAMP: {}",
            e
        ))),
        Ok(result) => Ok(Timestamp::new(result)),
    }
}

cast_function!(Timestamp, Timestamp, u64, u64);

#[doc(hidden)]
#[inline]
pub fn cast_to_i64_Timestamp(value: Timestamp) -> SqlResult<i64> {
    Ok(value.milliseconds())
}

cast_function!(i64, i64, Timestamp, Timestamp);

#[doc(hidden)]
#[inline]
pub fn cast_to_u64_Timestamp(value: Timestamp) -> SqlResult<u64> {
    let ms = value.milliseconds();
    if ms < 0 {
        Err(SqlRuntimeError::from_string(format!(
            "Negative value converted to unsigned {}",
            value
        )))
    } else {
        Ok(ms as u64)
    }
}

cast_function!(u64, u64, Timestamp, Timestamp);

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_Timestamp<const P: usize, const S: usize>(
    value: Timestamp,
) -> SqlResult<SqlDecimal<P, S>> {
    cast_to_SqlDecimal_i64::<P, S>(value.milliseconds())
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_Timestamp<const P: usize, const S: usize>(
    value: Timestamp,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    r2o(cast_to_SqlDecimal_Timestamp::<P, S>(value))
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimal_TimestampN<const P: usize, const S: usize>(
    value: Option<Timestamp>,
) -> SqlResult<SqlDecimal<P, S>> {
    match value {
        None => Err(cast_null("DECIMAL")),
        Some(value) => cast_to_SqlDecimal_Timestamp::<P, S>(value),
    }
}

#[doc(hidden)]
#[inline]
pub fn cast_to_SqlDecimalN_TimestampN<const P: usize, const S: usize>(
    value: Option<Timestamp>,
) -> SqlResult<Option<SqlDecimal<P, S>>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_SqlDecimalN_Timestamp::<P, S>(value),
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
                    Err(e) => Err(SqlRuntimeError::from_string(format!(
                        "Error converting {value} to TIMESTAMP: {}",
                        e
                    ))),
                }
            }

            cast_function!(Timestamp, Timestamp, $type_name, $arg_type);

            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ $type_name _Timestamp>](value: Timestamp) -> SqlResult<$arg_type> {
                match cast_to_i64_Timestamp(value) {
                    Ok(value) => [< cast_to_ $type_name _i64 >] (value),
                    Err(e) => Err(SqlRuntimeError::from_string(format!(
                        "Error converting {value} to TIMESTAMP: {}",
                        e
                    ))),
                }
            }

            cast_function!($type_name, $arg_type, Timestamp, Timestamp);
        }
    };
}

// the 64-bit variants are defined separately
cast_ts!(i32, i32);
cast_ts!(i16, i16);
cast_ts!(i8, i8);
cast_ts!(u32, u32);
cast_ts!(u16, u16);
cast_ts!(u8, u8);
cast_ts!(f, F32);
cast_ts!(d, F64);

//////////////////// Other casts

#[doc(hidden)]
#[inline]
pub fn cast_to_u_i32(value: i32) -> SqlResult<usize> {
    match value.try_into() {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to unsigned: {}",
            e
        ))),
    }
}

cast_function!(u, usize, i32, i32);

#[doc(hidden)]
#[inline]
pub fn cast_to_u_i64(value: i64) -> SqlResult<usize> {
    match value.try_into() {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to usize: {}",
            e
        ))),
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
    let value = match <isize as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to isize"
            )))
        }
    };
    Ok(value)
}

cast_function!(i, isize, i64, i64);

#[doc(hidden)]
#[inline]
pub fn cast_to_u_u32(value: u32) -> SqlResult<usize> {
    match value.try_into() {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to usize: {}",
            e
        ))),
    }
}

cast_function!(u, usize, u32, u32);

#[doc(hidden)]
#[inline]
pub fn cast_to_u_u64(value: u64) -> SqlResult<usize> {
    match value.try_into() {
        Ok(value) => Ok(value),
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting {value} to usize: {}",
            e
        ))),
    }
}

cast_function!(u, usize, u64, u64);

#[doc(hidden)]
#[inline]
pub fn cast_to_i_u32(value: u32) -> SqlResult<isize> {
    Ok(value as isize)
}

cast_function!(i, isize, u32, u32);

#[doc(hidden)]
#[inline]
pub fn cast_to_i_u64(value: u64) -> SqlResult<isize> {
    let value = match <isize as NumCast>::from(value) {
        Some(value) => value,
        None => {
            return Err(SqlRuntimeError::from_string(format!(
                "Cannot convert {value} to INTERVAL MONTHS"
            )))
        }
    };
    Ok(value)
}

cast_function!(i, isize, u64, u64);

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
    match value {
        None => Err(cast_null("BINARY")),
        Some(value) => cast_to_bytes_bytes(value, precision),
    }
}

#[doc(hidden)]
pub fn cast_to_bytesN_bytes(value: ByteArray, precision: i32) -> SqlResult<Option<ByteArray>> {
    r2o(cast_to_bytes_bytes(value, precision))
}

#[doc(hidden)]
pub fn cast_to_bytesN_bytesN(
    value: Option<ByteArray>,
    precision: i32,
) -> SqlResult<Option<ByteArray>> {
    match value {
        None => Ok(None),
        Some(value) => cast_to_bytesN_bytes(value, precision),
    }
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

// Synthesizes 4 functions for the argument type, e.g.:
// cast_to_V_i32
// cast_to_VN_i32
// cast_to_V_i32N
// cast_to_VN_i32N
macro_rules! cast_to_variant {
    ($result_name: ident $(< $( const $var:ident : $ty: ty),* >)?, $result_type: ty, $enum: ident) => {
        ::paste::paste! {
            // cast_to_V_i32
            #[doc(hidden)]
            #[inline]
            pub fn [<cast_to_ V_ $result_name >] $(< $( const $var : $ty),* >)? ( value: $result_type ) -> SqlResult<Variant> {
                Ok(Variant::from(value))
            }

            // cast_to_VN_i32
            #[doc(hidden)]
            pub fn [<cast_to_ VN_ $result_name >] $(< $( const $var : $ty),* >)? ( value: $result_type ) -> SqlResult<Option<Variant>> {
                Ok(Some(Variant::from(value)))
            }

            // cast_to_V_i32N
            #[doc(hidden)]
            pub fn [<cast_to_ V_ $result_name N>] $(< $( const $var : $ty),* >)? ( value: Option<$result_type> ) -> SqlResult<Variant> {
                match value {
                    None => Ok(Variant::SqlNull),
                    Some(value) => Ok(Variant::from(value)),
                }
            }

            // cast_to_VN_i32N
            #[doc(hidden)]
            pub fn [<cast_to_ VN_ $result_name N>] $(< $( const $var : $ty),* >)? ( value: Option<$result_type> ) -> SqlResult<Option<Variant>> {
                r2o([ <cast_to_ V_ $result_name N >] $(:: < $($var),* >)? (value))
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
                    Variant::UTinyInt(value) => r2o([< cast_to_ $result_name _u8 >](value)),
                    Variant::USmallInt(value) => r2o([< cast_to_ $result_name _u16 >](value)),
                    Variant::UInt(value) => r2o([< cast_to_ $result_name _u32 >](value)),
                    Variant::UBigInt(value) => r2o([< cast_to_ $result_name _u64 >](value)),
                    Variant::Real(value) => r2o([< cast_to_ $result_name _f >](value)),
                    Variant::Double(value) => r2o([< cast_to_ $result_name _d >](value)),
                    Variant::SqlDecimal((value, scale)) => {
                        let dd = DynamicDecimal::new(value, scale);
                        let result = $result_type :: try_from(dd);
                        match result {
                            Ok(value) => Ok(Some(value)),
                            Err(e) => Err(SqlRuntimeError::from_string(format!(
                                "Error converting {value} to {}: {}",
                                tn!($result_name), e
                            ))),
                        }
                    },
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
cast_variant_numeric!(u8, u8, UTinyInt);
cast_variant_numeric!(u16, u16, USmallInt);
cast_variant_numeric!(u32, u32, UInt);
cast_variant_numeric!(u64, u64, UBigInt);
cast_variant_numeric!(f, F32, Real);
cast_variant_numeric!(d, F64, Double);
cast_to_variant!(SqlDecimal<const P: usize, const S: usize>, SqlDecimal<P, S>, SqlDecimal); // The other direction takes extra arguments
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
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting VARIANT to ARRAY: {}",
            e
        ))),
    }
}

#[doc(hidden)]
pub fn cast_to_vec_VN<T>(value: Option<Variant>) -> SqlResult<Option<Array<T>>>
where
    Array<T>: TryFrom<Variant, Error = Box<dyn Error>>,
    T: std::fmt::Debug,
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
    T: std::fmt::Debug,
{
    let val = value.try_into();
    match val {
        Ok(value) => Ok(Some(value)),
        Err(_) => Ok(None),
    }
}

#[doc(hidden)]
pub fn cast_to_vecN_VN<T>(value: Option<Variant>) -> SqlResult<Option<Array<T>>>
where
    Array<T>: TryFrom<Variant, Error = Box<dyn Error>>,
    T: std::fmt::Debug,
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
        Err(e) => Err(SqlRuntimeError::from_string(format!(
            "Error converting VARIANT to MAP: {}",
            e
        ))),
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
