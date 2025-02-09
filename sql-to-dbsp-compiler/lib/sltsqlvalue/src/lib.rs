//! SltSqlValue is a dynamically-typed object that can represent a subset
//! of the Tuple* values in a SQL program.  This is used by SQL Logic Test,
//! which has a particular way of formatting tuples.  The
//! Tuple* types are used for computations, and they are converted
//! to SqlRow objects when they need to be serialized as strings.

#![allow(non_snake_case)]

use dbsp::algebra::{F32, F64};
use feldera_sqllib::{casts::*, SqlString};
use rust_decimal::Decimal;

#[derive(Debug)]
pub enum SltSqlValue {
    Int(i32),
    Long(i64),
    Str(SqlString),
    Flt(f32),
    Dbl(f64),
    Bool(bool),
    Decimal(Decimal),

    OptInt(Option<i32>),
    OptLong(Option<i64>),
    OptStr(Option<SqlString>),
    OptFlt(Option<f32>),
    OptDbl(Option<f64>),
    OptBool(Option<bool>),
    OptDecimal(Option<Decimal>),
}

impl From<i32> for SltSqlValue {
    fn from(value: i32) -> Self {
        SltSqlValue::Int(value)
    }
}

impl From<i64> for SltSqlValue {
    fn from(value: i64) -> Self {
        SltSqlValue::Long(value)
    }
}

impl From<bool> for SltSqlValue {
    fn from(value: bool) -> Self {
        SltSqlValue::Bool(value)
    }
}

impl From<f32> for SltSqlValue {
    fn from(value: f32) -> Self {
        SltSqlValue::Flt(value)
    }
}

impl From<f64> for SltSqlValue {
    fn from(value: f64) -> Self {
        SltSqlValue::Dbl(value)
    }
}

impl From<F32> for SltSqlValue {
    fn from(value: F32) -> Self {
        SltSqlValue::Flt(value.into_inner())
    }
}

impl From<F64> for SltSqlValue {
    fn from(value: F64) -> Self {
        SltSqlValue::Dbl(value.into_inner())
    }
}

impl From<SqlString> for SltSqlValue {
    fn from(value: SqlString) -> Self {
        SltSqlValue::Str(value)
    }
}

impl From<Decimal> for SltSqlValue {
    fn from(value: Decimal) -> Self {
        SltSqlValue::Decimal(value)
    }
}

impl From<Option<i32>> for SltSqlValue {
    fn from(value: Option<i32>) -> Self {
        SltSqlValue::OptInt(value)
    }
}

impl From<Option<i64>> for SltSqlValue {
    fn from(value: Option<i64>) -> Self {
        SltSqlValue::OptLong(value)
    }
}

impl From<Option<bool>> for SltSqlValue {
    fn from(value: Option<bool>) -> Self {
        SltSqlValue::OptBool(value)
    }
}

impl From<Option<f32>> for SltSqlValue {
    fn from(value: Option<f32>) -> Self {
        SltSqlValue::OptFlt(value)
    }
}

impl From<Option<f64>> for SltSqlValue {
    fn from(value: Option<f64>) -> Self {
        SltSqlValue::OptDbl(value)
    }
}

impl From<Option<F32>> for SltSqlValue {
    fn from(value: Option<F32>) -> Self {
        match value {
            None => SltSqlValue::OptFlt(None),
            Some(x) => SltSqlValue::OptFlt(Some(x.into_inner())),
        }
    }
}

impl From<Option<F64>> for SltSqlValue {
    fn from(value: Option<F64>) -> Self {
        match value {
            None => SltSqlValue::OptDbl(None),
            Some(x) => SltSqlValue::OptDbl(Some(x.into_inner())),
        }
    }
}

impl From<Option<SqlString>> for SltSqlValue {
    fn from(value: Option<SqlString>) -> Self {
        SltSqlValue::OptStr(value)
    }
}

impl From<Option<Decimal>> for SltSqlValue {
    fn from(value: Option<Decimal>) -> Self {
        SltSqlValue::OptDecimal(value)
    }
}

#[derive(Default)]
pub struct SqlRow {
    values: Vec<SltSqlValue>,
}

pub trait ToSqlRow {
    fn to_row(&self) -> SqlRow;
}

impl SqlRow {
    pub fn new() -> Self {
        SqlRow {
            values: Vec::default(),
        }
    }

    /// Output the SqlRow value in the format expected by the tests
    /// in SqlLogicTest.
    /// # Arguments
    /// - 'format': a string with characters I, R, or T, standing
    ///   respectively for integers, real, or text.
    /// # Panics
    ///
    /// if format.let() != self.values.len()
    pub fn to_slt_strings(self, format: &str) -> Vec<SqlString> {
        if self.values.len() != format.len() {
            panic!(
                "Mismatched format {} vs len {}",
                format.len(),
                self.values.len()
            )
        }
        let mut result = Vec::<SqlString>::with_capacity(format.len());
        for elem in self.values.iter().zip(format.chars()) {
            result.push(SqlString::from(elem.0.format_slt(&elem.1)));
        }
        result
    }
}

impl SqlRow {
    pub fn push(&mut self, value: SltSqlValue) {
        self.values.push(value)
    }
}

pub trait SqlLogicTestFormat {
    fn format_slt(&self, arg: &char) -> String;
}

/// Convert a string result according to the SqlLogicTest rules:
/// empty strings are turned into (empty)
/// non-printable (ASCII) characters are turned into @
fn slt_translate_string(s: &str) -> String {
    if s.is_empty() {
        return String::from("(empty)");
    }
    let mut result = String::with_capacity(s.len());
    for mut c in s.chars() {
        if !(' '..='~').contains(&c) {
            c = '@';
        }
        result.push(c);
    }
    result
}

/// Format a SltSqlValue according to SqlLogicTest rules
/// the arg is one character of the form I - i32, R - f32, or T - String.
impl SqlLogicTestFormat for SltSqlValue {
    fn format_slt(&self, arg: &char) -> String {
        match (self, arg) {
            (SltSqlValue::Int(x), _) => format!("{}", x),
            (SltSqlValue::OptInt(None), _) => String::from("NULL"),
            (SltSqlValue::OptInt(Some(x)), _) => format!("{}", x),

            (SltSqlValue::Decimal(x), _) => format!("{}", x),
            (SltSqlValue::OptDecimal(None), _) => String::from("NULL"),
            (SltSqlValue::OptDecimal(Some(x)), _) => format!("{}", x),

            (SltSqlValue::Long(x), _) => format!("{}", x),
            (SltSqlValue::OptLong(None), _) => String::from("NULL"),
            (SltSqlValue::OptLong(Some(x)), _) => format!("{}", x),

            (SltSqlValue::OptFlt(None), _) => String::from("NULL"),
            (SltSqlValue::Flt(x), 'I') => format!("{}", *x as i32),
            (SltSqlValue::OptFlt(Some(x)), 'I') => format!("{}", *x as i32),
            (SltSqlValue::Flt(x), _) => format!("{:.3}", x),
            (SltSqlValue::OptFlt(Some(x)), _) => format!("{:.3}", x),

            (SltSqlValue::OptDbl(None), _) => String::from("NULL"),
            (SltSqlValue::Dbl(x), 'I') => format!("{}", *x as i32),
            (SltSqlValue::OptDbl(Some(x)), 'I') => format!("{}", *x as i32),
            (SltSqlValue::Dbl(x), _) => format!("{:.3}", x),
            (SltSqlValue::OptDbl(Some(x)), _) => format!("{:.3}", x),

            (SltSqlValue::Str(x), 'T') => slt_translate_string(x.str()),
            (SltSqlValue::OptStr(None), 'T') => String::from("NULL"),
            (SltSqlValue::OptStr(Some(x)), 'T') => slt_translate_string(x.str()),
            (SltSqlValue::OptStr(None), 'I') => String::from("NULL"),
            (SltSqlValue::OptStr(Some(x)), 'I') => {
                format!("{}", unwrap_cast(cast_to_i32_s(x.clone())))
            }

            (SltSqlValue::OptBool(None), _) => String::from("NULL"),
            (SltSqlValue::Bool(b), _) => format!("{}", b),
            (SltSqlValue::OptBool(Some(b)), _) => format!("{}", b),
            _ => panic!("Unexpected combination {:?} {:?}", self, arg),
        }
    }
}

#[macro_export]
macro_rules! to_sql_row_impl {
    (
        $(
            $tuple_name:ident<$($element:tt),* $(,)?>
        ),*
        $(,)?
    ) => {
        $(
            impl<$($element),*> ToSqlRow for $tuple_name<$($element,)*> where
                $(SltSqlValue: From<$element>,)*
                $($element: Clone, )*
            {
                fn to_row(&self) -> SqlRow  {
                    let mut result = SqlRow::new();
                    let ($($element),*,) = self.into();
                    $(result.push(SltSqlValue::from($element.clone()));)*
                    result
                }
            }
        )*
    };
}

use dbsp::utils::{Tup1, Tup10, Tup2, Tup3, Tup4, Tup5, Tup6, Tup7, Tup8, Tup9};
crate::to_sql_row_impl! {
    Tup1<T1>,
    Tup2<T1, T2>,
    Tup3<T1, T2, T3>,
    Tup4<T1, T2, T3, T4>,
    Tup5<T1, T2, T3, T4, T5>,
    Tup6<T1, T2, T3, T4, T5, T6>,
    Tup7<T1, T2, T3, T4, T5, T6, T7>,
    Tup8<T1, T2, T3, T4, T5, T6, T7, T8>,
    Tup9<T1, T2, T3, T4, T5, T6, T7, T8, T9>,
    Tup10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>,
}
