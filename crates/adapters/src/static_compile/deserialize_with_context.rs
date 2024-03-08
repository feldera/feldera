//! A framework for implementing configurable deserialization.
//!
//! ## Motivation
//!
//! In Feldera, we deal with different encodings of the same datatype, e.g.,
//! timestamps can be represented as strings formatted using different standards
//! or as numbers.  In the latter case, these numbers can represent times in
//! microseconds or milliseconds.  Different data sources can define their
//! own encodings; in fact even the same data source can support more than one,
//! e.g., Debezium uses a slightly different encoding for each source DB, and
//! that's just using the default configuration.
//!
//! To support this, we need to implement configurable (de)serializers that can
//! change their behavior based on a user-defined profile.  Unfortunately,
//! serde's `Deserialize` and `Serialize` traits don't allow this.  The only
//! support `serde` provides for passing state to deserializers is the
//! `DeserializeSeed` trait, but there is no support for automatically threading
//! it through nested types or to auto-derive implementations using it.
//!
//! ## Design
//!
//! * We define [`trait DeserializeWithContext`], which has the same signature
//!   as
//! `Deserialize` with the extra context argument.
//!
//! * [`deserialize_without_context`](`crate::deserialize_without_context`)
//!   macro
//! is used to implement [`DeserializeWithContext`] for types that implement
//! `serde::Deserialize` and don't support configurable deserialization.
//!
//! * We implement [`DeserializeWithContext`] for vectors, tuples, and options
//! by passing the context to each element.
//!
//! * The [`deserialize_struct`](`crate::deserialize_struct`) macro is used to
//! implement [`DeserializeWithContext`] for structs.
//!
//! * The [`deserialize_table_record`](`crate::deserialize_table_record`) macro
//! implements [`DeserializeWithContext`] for SQL table records.  It's similar
//! to [`deserialize_struct`](`crate::deserialize_struct`), but additionally
//! handles case-(in)sensitive SQL columns.

// TODOs:
// This could benefit from using procedural macros to auto-derive
// [`DeserializeWithContext`].

use std::{fmt, marker::PhantomData};

use crate::static_compile::catalog::NeighborhoodQuery;
use dbsp::{
    algebra::{F32, F64},
    operator::NeighborhoodDescr,
    utils::{Tup1, Tup10, Tup2, Tup3, Tup4, Tup5, Tup6, Tup7, Tup8, Tup9},
    DBData,
};
use rust_decimal::Decimal;
use serde::{
    de::{DeserializeSeed, Error, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};

/// Similar to [`Deserialize`], but takes an extra `context` argument and
/// threads it through all nested structures.
pub trait DeserializeWithContext<'de, C>: Sized {
    fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>;
}

/// Implement [`DeserializeWithContext`] for types that implement
/// [`Deserialize`] and don't support configurable deserialization.
///
/// The implementation invokes [`Deserialize::deserialize`], ignoring the
/// context.
#[macro_export]
macro_rules! deserialize_without_context {
    ($typ:tt) => {
        deserialize_without_context!($typ,);
    };

    ($typ:tt, $($arg:tt),*) => {
        impl<'de, C, $($arg),*> $crate::DeserializeWithContext<'de, C> for $typ<$($arg),*>
        where
            $typ<$($arg),*>: serde::Deserialize<'de>,
        {
            fn deserialize_with_context<D>(deserializer: D, _context: &'de C) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                serde::Deserialize::deserialize(deserializer)
            }
        }
    };
}

deserialize_without_context!(bool);
deserialize_without_context!(i8);
deserialize_without_context!(u8);
deserialize_without_context!(i16);
deserialize_without_context!(u16);
deserialize_without_context!(i32);
deserialize_without_context!(u32);
deserialize_without_context!(i64);
deserialize_without_context!(u64);
deserialize_without_context!(i128);
deserialize_without_context!(u128);
deserialize_without_context!(usize);
deserialize_without_context!(isize);
deserialize_without_context!(String);
deserialize_without_context!(char);
deserialize_without_context!(F32);
deserialize_without_context!(F64);
deserialize_without_context!(Decimal);
deserialize_without_context!(Tup1, T1);
deserialize_without_context!(Tup2, T1, T2);
deserialize_without_context!(Tup3, T1, T2, T3);
deserialize_without_context!(Tup4, T1, T2, T3, T4);
deserialize_without_context!(Tup5, T1, T2, T3, T4, T5);
deserialize_without_context!(Tup6, T1, T2, T3, T4, T5, T6);
deserialize_without_context!(Tup7, T1, T2, T3, T4, T5, T6, T7);
deserialize_without_context!(Tup8, T1, T2, T3, T4, T5, T6, T7, T8);
deserialize_without_context!(Tup9, T1, T2, T3, T4, T5, T6, T7, T8, T9);
deserialize_without_context!(Tup10, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);

// Used to pass the context to nested structures using the
// `DeserializeSeed` mechanism.  This is only public because it's
// use in a macro; it's not suppposed to be user-visible otherwise.
#[doc(hidden)]
pub struct DeserializationContext<'de, C, T> {
    context: &'de C,
    phantom: PhantomData<T>,
}

impl<'de, C, T> DeserializationContext<'de, C, T> {
    pub fn new(context: &'de C) -> Self {
        Self {
            context,
            phantom: PhantomData,
        }
    }
}

impl<'de, C, T> DeserializeSeed<'de> for DeserializationContext<'de, C, T>
where
    T: DeserializeWithContext<'de, C>,
{
    type Value = T;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize_with_context(deserializer, self.context)
    }
}

impl<'de, C, T> DeserializeWithContext<'de, C> for Vec<T>
where
    T: DeserializeWithContext<'de, C>,
{
    fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VecVisitor<'de, C, T> {
            context: &'de C,
            phantom: PhantomData<T>,
        }

        impl<'de, C, T> VecVisitor<'de, C, T> {
            fn new(context: &'de C) -> Self {
                Self {
                    context,
                    phantom: PhantomData,
                }
            }
        }

        impl<'de, C, T> Visitor<'de> for VecVisitor<'de, C, T>
        where
            T: DeserializeWithContext<'de, C>,
        {
            type Value = Vec<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "an array")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut result = Vec::new();

                while let Some(val) =
                    seq.next_element_seed(DeserializationContext::new(self.context))?
                {
                    result.push(val)
                }

                Ok(result)
            }
        }

        deserializer.deserialize_seq(VecVisitor::new(context))
    }
}

macro_rules! deserialize_tuple {
    ([$num_fields:expr]($($arg:tt),*)) => {
        #[allow(unused_variables)]
        #[allow(dead_code)]
        #[allow(unused_mut)]
        impl<'de, C, $($arg),*> DeserializeWithContext<'de, C> for ($($arg),*)
        where
            $($arg: DeserializeWithContext<'de, C>),*
        {
            fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct TupleVisitor<'de, C, $($arg),*> {
                    context: &'de C,
                    phantom: PhantomData<($($arg),*)>,
                }

                impl<'de, C, $($arg),*> TupleVisitor<'de, C, $($arg),*> {
                    fn new(context: &'de C) -> Self {
                        Self {
                            context,
                            phantom: PhantomData,
                        }
                    }
                }

                impl<'de, C, $($arg),*> Visitor<'de> for TupleVisitor<'de, C, $($arg),*>
                where
                    $($arg: DeserializeWithContext<'de, C>),*
                {
                    type Value = ($($arg),*);

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        write!(formatter, concat!("a tuple of size ", stringify!($num_fields)))
                    }

                    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                    where
                        A: SeqAccess<'de>,
                    {
                        let mut cnt: i32 = -1;
                        Ok(($({cnt+=1; seq
                            .next_element_seed(<$crate::DeserializationContext<C, $arg>>::new(self.context))?
                            .ok_or_else(|| {
                                A::Error::invalid_length(cnt as usize, &self)
                            })?}),*))
                    }
                }

                deserializer.deserialize_tuple($num_fields, TupleVisitor::new(context))
            }
        }


    }
}

deserialize_tuple!([0]());
deserialize_tuple!([2](T0, T1));
deserialize_tuple!([3](T0, T1, T2));
deserialize_tuple!([4](T0, T1, T2, T3));
deserialize_tuple!([5](T0, T1, T2, T3, T4));
deserialize_tuple!([6](T0, T1, T2, T3, T4, T5));
deserialize_tuple!([7](T0, T1, T2, T3, T4, T5, T6));
deserialize_tuple!([8](T0, T1, T2, T3, T4, T5, T6, T7));
deserialize_tuple!([9](T0, T1, T2, T3, T4, T5, T6, T7, T8));
deserialize_tuple!([10](T0, T1, T2, T3, T4, T5, T6, T7, T8, T9));
deserialize_tuple!([11](T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10));
deserialize_tuple!([12](T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11));

impl<'de, C, T> DeserializeWithContext<'de, C> for Option<T>
where
    T: DeserializeWithContext<'de, C>,
{
    fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct OptionVisitor<'de, C, T> {
            context: &'de C,
            phantom: std::marker::PhantomData<T>,
        }

        impl<'de, C, T> OptionVisitor<'de, C, T> {
            fn new(context: &'de C) -> Self {
                Self {
                    context,
                    phantom: std::marker::PhantomData,
                }
            }
        }

        impl<'de, C, T> Visitor<'de> for OptionVisitor<'de, C, T>
        where
            T: DeserializeWithContext<'de, C>,
        {
            type Value = Option<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "option")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(None)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Ok(Some(T::deserialize_with_context(
                    deserializer,
                    self.context,
                )?))
            }
        }

        deserializer.deserialize_option(OptionVisitor::new(context))
    }
}

/// Deserialization error type that includes field name,
/// which failed to parse correctly.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldParseError {
    pub field: String,
    pub description: String,
}

/// Generate [`DeserializeWithContext`] implementation parameterized by context
/// type for a struct.
///
/// # Arguments
///
/// * `$struct` - name of the struct type.
/// * `$arg` - type arguments.
/// * `$bound` - optional trait bound for type argument `$arg`.
/// * `$num_fields` - the number of struct fields.
/// * `$field_name` - field name.
/// * `$type` - field type.
/// * `$default` - `None`, or `Some(value)` for fields that have a default
/// value.
// TODO: This should be a procedural macro, but I don't have experience with those
// and it would take me too long to write one.  It should be possible to steal code
// from `serde_derive`.
#[macro_export]
macro_rules! deserialize_struct {
    ($struct:ident($($arg:tt $(: $bound:tt)?),*)[$num_fields:expr]{$($field_name:ident: $type:ty = $default:expr),* }) => {
        #[allow(unused_variables)]
        #[allow(unused_mut)]
        impl<'de, C, $($arg),*> $crate::DeserializeWithContext<'de, C> for $struct<$($arg),*>
        where
            $($arg: $crate::DeserializeWithContext<'de, C>,)*
            $($($arg : $bound,)?)*
        {
            fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct StructVisitor<'de, C, $($arg),*> {
                    context: &'de C,
                    #[allow(unused_parens)]
                    phantom: std::marker::PhantomData<($($arg),*)>
                }

                impl<'de, C, $($arg),*> StructVisitor<'de, C, $($arg),*> {
                    fn new(context: &'de C) -> Self {
                        Self {
                            context,
                            phantom: std::marker::PhantomData,
                        }
                    }
                }

                impl<'de, C, $($arg),*> serde::de::Visitor<'de> for StructVisitor<'de, C, $($arg),*>
                where
                    $($arg: $crate::DeserializeWithContext<'de, C>,)*
                    $($($arg : $bound,)?)*
                {

                    type Value = $struct<$($arg),*>;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, concat!("a struct of type ", stringify!($struct)))
                    }

                    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                    where A: serde::de::MapAccess<'de> {
                        $(let mut $field_name: Option<$type> = None;
                        )*

                        while let Some(field_name) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                            $(
                                if stringify!($field_name) == field_name.as_ref() {
                                    $field_name = Some(map.next_value_seed(<$crate::DeserializationContext<C, $type>>::new(self.context))?);
                                } else
                            )*
                            {let _ = map.next_value::<serde::de::IgnoredAny>()?;}
                        }
                        Ok($struct {
                            $($field_name: $field_name.or_else(|| $default).ok_or_else(|| serde::de::Error::missing_field(stringify!($field_name)))?,
                            )*
                        })
                    }

                    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                    where A: serde::de::SeqAccess<'de> {
                        let mut _cols: usize = 0;
                        let result = $struct {
                            $($field_name: {
                                _cols += 1;
                                seq.next_element_seed(<$crate::DeserializationContext<C, $type>>::new(self.context))?
                                    .ok_or_else(|| {
                                        serde::de::Error::invalid_length(_cols-1, &format!("{} fields", $num_fields).as_str())
                                    })?
                            },
                            )*
                        };

                        while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                        Ok(result)
                    }
                }

                let visitor = <StructVisitor<C, $($arg),*>>::new(context);

                deserializer.deserialize_struct(stringify!($struct), [$(stringify!($field_name),)*].as_slice(), visitor)
            }
        }
    }
}

deserialize_struct!(NeighborhoodDescr(K: DBData, V: DBData)[4]{
    anchor: Option<K> = Some(None),
    anchor_val: V = Some(Default::default()),
    before: u64 = None,
    after: u64 = None
});

deserialize_struct!(NeighborhoodQuery(K)[3]{
    anchor: Option<K> = Some(None),
    before: u64 = None,
    after: u64 = None
});

/// Generate a [`DeserializeWithContext`] impl for a SQL table row type.
///
/// A call to this macro is normally generated by the SQL compiler.
///
/// Unlike [`deserialize_struct`](`crate::deserialize_struct`), this macro:
/// - Supports case-insensitive parsing of field names.
/// - On error, reports the name of the field that failed to parse.
#[macro_export]
macro_rules! deserialize_table_record {
    ($table:ident[$sql_table:tt, $num_cols:expr]{$(($field_name:ident, $column_name:tt, $case_sensitive:tt, $type:ty, $init:expr $(, $postprocess:expr)*)),* }) => {
        #[allow(non_snake_case)]
        #[allow(unused_variables)]
        #[allow(unused_mut)]
        #[allow(dead_code)]
        impl<'de> $crate::DeserializeWithContext<'de, $crate::SqlSerdeConfig> for $table {
            fn deserialize_with_context<D>(deserializer: D, context: &'de $crate::SqlSerdeConfig) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct RecordVisitor<'de> {
                    context: &'de $crate::SqlSerdeConfig,
                }

                impl<'de> RecordVisitor<'de> {
                    fn new(context: &'de $crate::SqlSerdeConfig) -> Self {
                        Self {
                            context
                        }
                    }
                }

                impl<'de> serde::de::Visitor<'de> for RecordVisitor<'de> {
                    type Value = $table;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, concat!("a record of type ", stringify!($table)))
                    }

                    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                    where A: serde::de::MapAccess<'de> {
                        $(let mut $field_name: Option<$type> = None;
                        )*

                        while let Some(column_name) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                            let lowercase_column_name = column_name.to_lowercase();
                            $(
                                if $column_name == (if $case_sensitive { column_name.as_ref() } else { &lowercase_column_name } ) {
                                    // We don't have a way to return `FieldParseError` to the
                                    // user, since the error type is determined by the
                                    // deserializer type `D`, so we instead encode it as a JSON
                                    // object, which the client will have to parse.
                                    $field_name = Some(map.next_value_seed(<$crate::DeserializationContext<$crate::SqlSerdeConfig, $type>>::new(self.context))
                                                  $(.map($postprocess))*
                                                  .map_err(|e| serde::de::Error::custom(serde_json::to_string(&$crate::FieldParseError{field: $column_name.to_string(), description: e.to_string()}).unwrap()))?);
                                } else
                            )*
                            {let _ = map.next_value::<serde::de::IgnoredAny>()?;}
                        }
                        Ok($table {
                            $($field_name: $field_name.or_else(|| $init).ok_or_else(|| serde::de::Error::missing_field($column_name))?,
                            )*
                        })
                    }

                    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                    where A: serde::de::SeqAccess<'de> {
                        let mut _cols: usize = 0;
                        let result = $table {
                            $($field_name: {
                                _cols += 1;
                                seq.next_element_seed(<$crate::DeserializationContext<$crate::SqlSerdeConfig, $type>>::new(self.context))
                                    .map_err(|e| {
                                        serde::de::Error::custom(serde_json::to_string(&$crate::FieldParseError{field: $column_name.to_string(), description: e.to_string()}).unwrap())
                                    })?
                                    .ok_or_else(|| {
                                        serde::de::Error::invalid_length(_cols-1, &format!("{} columns", $num_cols).as_str())
                                    })?
                            },
                            )*
                        };

                        while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                        Ok(result)
                    }
                }

                let visitor = RecordVisitor::new(context);

                // XXX: At least with `serde_json::Deserializer`, `deserialize_struct` tells the
                // deserializer to work with either map or sequence representation
                // (while `deserialize_map` only deserializes from a map).  It seems that
                // `serde_json` ignores the actual field names provided, so this does not
                // break the case-insensitive behavior.  Not sure if this is true for all
                // deserializers.  In the future we may want to make the choice between map
                // and sequence representations configurable, so we won't need this trick.
                //deserializer.deserialize_map(visitor)
                deserializer.deserialize_struct($sql_table, [$($column_name,)*].as_slice(), visitor)
            }
        }
    }
}

#[cfg(test)]
mod test {

    use dbsp::algebra::F64;
    use lazy_static::lazy_static;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::{format::string_record_deserializer, DeserializeWithContext, SqlSerdeConfig};

    #[derive(Debug, Eq, PartialEq)]
    struct TUPLE0;
    deserialize_table_record!(TUPLE0["EmptyTable", 0] {});

    lazy_static! {
        static ref DEFAULT_CONFIG: SqlSerdeConfig = SqlSerdeConfig::default();
    }

    fn deserialize_with_default_context<'de, T>(json: &'de str) -> Result<T, serde_json::Error>
    where
        T: DeserializeWithContext<'de, SqlSerdeConfig>,
    {
        T::deserialize_with_context(
            &mut serde_json::Deserializer::from_str(json),
            &DEFAULT_CONFIG,
        )
    }

    #[test]
    fn deserialize_unit() {
        assert_eq!(
            deserialize_with_default_context::<TUPLE0>("{}").unwrap(),
            TUPLE0
        );
        assert_eq!(
            deserialize_with_default_context::<TUPLE0>(r#"{"extra_field": 5}"#).unwrap(),
            TUPLE0
        );
        assert_eq!(
            deserialize_with_default_context::<TUPLE0>("[]").unwrap(),
            TUPLE0
        );
    }

    #[derive(Debug, Eq, PartialEq)]
    #[allow(non_snake_case)]
    struct Struct2 {
        #[allow(non_snake_case)]
        cc_num: F64,
        #[allow(non_snake_case)]
        first: Option<String>,
        #[allow(non_snake_case)]
        dec: Decimal,
    }
    deserialize_table_record!(Struct2["Table.Name", 3] {(cc_num, "cc_num", false, F64, None), (first, "first", false, Option<String>, Some(None)), (dec, "dec", false, Decimal, None)});

    #[test]
    fn deserialize_struct2() {
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"{"cc_num": 100, "dec": "0.123"}"#)
                .unwrap(),
            Struct2 {
                cc_num: F64::from(100),
                first: None,
                dec: dec!(0.123),
            }
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"{"cc_num": 100, "dec": 0.123}"#)
                .unwrap(),
            Struct2 {
                cc_num: F64::from(100),
                first: None,
                dec: dec!(0.123),
            }
        );

        assert_eq!(
            deserialize_with_default_context::<Struct2>(
                r#"{"CC_NUM": 100, "first": null, "dec": "-1.40"}"#
            )
            .unwrap(),
            Struct2 {
                cc_num: F64::from(100),
                first: None,
                dec: dec!(-1.40),
            }
        );

        assert_eq!(
            deserialize_with_default_context::<Struct2>(
                r#"{"CC_NUM": 100, "first": null, "dec": -1.40}"#
            )
            .unwrap(),
            Struct2 {
                cc_num: F64::from(100),
                first: None,
                dec: dec!(-1.40),
            }
        );

        assert_eq!(
            deserialize_with_default_context::<Struct2>(
                r#"{"CC_NUM": 100, "first": "foo", "dec": "1e20"}"#
            )
            .unwrap(),
            Struct2 {
                cc_num: F64::from(100),
                first: Some("foo".to_string()),
                dec: dec!(1e20),
            }
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"{"first": "foo"}"#)
                .map_err(|e| e.to_string()),
            Err(r#"missing field `cc_num` at line 1 column 16"#.to_string())
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"[100, "foo", "-1e20"]"#).unwrap(),
            Struct2 {
                cc_num: F64::from(100),
                first: Some("foo".to_string()),
                dec: dec!(-1e20),
            }
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"[100, null, "2e-5"]"#).unwrap(),
            Struct2 {
                cc_num: F64::from(100),
                first: None,
                dec: dec!(0.00002),
            }
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"[100, null, "-3e-5"]"#).unwrap(),
            Struct2 {
                cc_num: F64::from(100),
                first: None,
                dec: dec!(-3e-5),
            }
        );
        assert_eq!(
            deserialize_with_default_context::<Struct2>(r#"[100, null, -3e-5]"#).unwrap(),
            Struct2 {
                cc_num: F64::from(100),
                first: None,
                dec: dec!(-3e-5),
            }
        );
    }

    #[derive(Debug, Eq, PartialEq)]
    #[allow(non_snake_case)]
    struct CaseSensitive {
        fIeLd1: bool,
        field2: String,
        field3: Option<u8>,
    }

    deserialize_table_record!(CaseSensitive["CaseSensitive", 3] {
        (fIeLd1, "fIeLd1", true, bool, None),
        (field2, "field2", true, String, None),
        (field3, "field3", false, Option<u8>, Some(None))
    });

    #[test]
    fn case_sensitive() {
        assert_eq!(
            deserialize_with_default_context::<CaseSensitive>(
                r#"{"fIeLd1": true, "field2": "foo"}"#
            )
            .unwrap(),
            CaseSensitive {
                fIeLd1: true,
                field2: "foo".to_string(),
                field3: None
            }
        );
        assert_eq!(
            deserialize_with_default_context::<CaseSensitive>(r#"[true, "foo", 100]"#).unwrap(),
            CaseSensitive {
                fIeLd1: true,
                field2: "foo".to_string(),
                field3: Some(100)
            }
        );
        assert_eq!(
            deserialize_with_default_context::<CaseSensitive>(
                r#"{"field1": true, "field2": "foo"}"#
            )
            .map_err(|e| e.to_string()),
            Err(r#"missing field `fIeLd1` at line 1 column 33"#.to_string())
        );
        assert_eq!(
            deserialize_with_default_context::<CaseSensitive>(
                r#"{"fIeLd1": true, "FIELD2": "foo"}"#
            )
            .map_err(|e| e.to_string()),
            Err(r#"missing field `field2` at line 1 column 33"#.to_string())
        );
    }

    #[test]
    fn error_reporting() {
        // Correctly report parsing errors for individual fields.
        assert_eq!(deserialize_with_default_context::<CaseSensitive>(r#"{"fIeLd1": 10, "field2": "foo"}"#).map_err(|e| e.to_string()), Err(r#"{"field":"fIeLd1","description":"invalid type: integer `10`, expected a boolean at line 1 column 13"} at line 1 column 13"#.to_string()));
        assert_eq!(deserialize_with_default_context::<CaseSensitive>(r#"[10, "foo", null]"#).map_err(|e| e.to_string()), Err(r#"{"field":"fIeLd1","description":"invalid type: integer `10`, expected a boolean at line 1 column 3"} at line 1 column 5"#.to_string()));
        assert_eq!(
            deserialize_with_default_context::<CaseSensitive>(r#"[true]"#)
                .map_err(|e| e.to_string()),
            Err(r#"invalid length 1, expected 3 columns at line 1 column 6"#.to_string())
        );
        assert_eq!(deserialize_with_default_context::<CaseSensitive>(r#"{"fIeLd1": null, "field2": "foo"}"#).map_err(|e| e.to_string()), Err(r#"{"field":"fIeLd1","description":"invalid type: null, expected a boolean at line 1 column 15"} at line 1 column 15"#.to_string()));
        assert_eq!(deserialize_with_default_context::<CaseSensitive>(r#"{"fIeLd1": false, "field2": "foo", "FIELD3": true}"#).map_err(|e| e.to_string()), Err(r#"{"field":"field3","description":"invalid type: boolean `true`, expected u8 at line 1 column 49"} at line 1 column 50"#.to_string()));
        assert_eq!(deserialize_with_default_context::<CaseSensitive>(r#"{"fIeLd1": 10, "field2": "foo"}"#).map_err(|e| e.to_string()), Err(r#"{"field":"fIeLd1","description":"invalid type: integer `10`, expected a boolean at line 1 column 13"} at line 1 column 13"#.to_string()));
    }

    #[derive(Debug, Eq, PartialEq)]
    #[allow(non_snake_case)]
    struct UnicodeStruct {
        f1: bool,
        f2: String,
        f3: Option<u8>,
    }

    deserialize_table_record!(UnicodeStruct["UnicodeStruct", 3] {
        (f1, "αΒβΓγδε", true, bool, None),
        (f2, "українська", false, String, None),
        (f3, "unicode⌛👏", false, Option<u8>, Some(None))
    });

    #[test]
    fn unicode() {
        assert_eq!(
            deserialize_with_default_context::<UnicodeStruct>(
                r#"{"αΒβΓγδε": true, "Українська": "foo", "unicode⌛👏": 100}"#
            )
            .unwrap(),
            UnicodeStruct {
                f1: true,
                f2: "foo".to_string(),
                f3: Some(100)
            }
        );
        assert_eq!(
            deserialize_with_default_context::<UnicodeStruct>(
                r#"{"αΒβΓγδε": true, "Українська": 10, "unicode⌛👏": 100}"#
            ).map_err(|e| e.to_string()),
            Err(r#"{"field":"українська","description":"invalid type: integer `10`, expected a string at line 1 column 51"} at line 1 column 51"#.to_string())
        );
        assert_eq!(
            deserialize_with_default_context::<UnicodeStruct>(
                r#"[true, 10, 100]"#
            ).map_err(|e| e.to_string()),
            Err(r#"{"field":"українська","description":"invalid type: integer `10`, expected a string at line 1 column 9"} at line 1 column 11"#.to_string())
        );
    }

    #[test]
    fn csv() {
        let data = r#"true,"foo",5
true,bar,buzz"#;
        let rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        let mut records = rdr.into_records();
        assert_eq!(
            CaseSensitive::deserialize_with_context(
                &mut string_record_deserializer(&records.next().unwrap().unwrap(), None),
                &SqlSerdeConfig::default()
            )
            .unwrap(),
            CaseSensitive {
                fIeLd1: true,
                field2: "foo".to_string(),
                field3: Some(5)
            }
        );
        assert_eq!(
            CaseSensitive::deserialize_with_context(
                &mut string_record_deserializer(&records.next().unwrap().unwrap(), None),
                &SqlSerdeConfig::default()
            )
            .map_err(|e| e.to_string()),
            Err(
                r#"{"field":"field3","description":"field 2: invalid digit found in string"}"#
                    .to_string()
            )
        );
    }
}
