//! A framework for implementing configurable serialization.
//!
//! ## Motivation
//!
//! See [`deserialize_with_context`](super::deserialize) module
//! documentation.
//!
//! ## Design
//!
//! * We define [`trait SerializeWithContext`], which has the same signature as
//!   `Serialize` with the extra context argument.
//!
//! * [`serialize_without_context`](`crate::serialize_without_context`) macro is
//!   used to implement [`SerializeWithContext`] for types that implement
//!   `serde::Serialize` and don't support configurable deserialization.
//!
//! * We implement [`SerializeWithContext`] for vectors, tuples, and options by
//!   passing the context to each element.
//!
//! * The [`serialize_struct`](`crate::serialize_struct`) macro is used to
//!   implement [`SerializeWithContext`] for structs.

// TODOs:
// This could benefit from procedural macros to auto-derive
// [`SerializeWithContext`].

use serde::{
    ser::{SerializeMap, SerializeSeq, SerializeTuple},
    Serialize, Serializer,
};
use std::{
    collections::{BTreeMap, HashSet},
    marker::PhantomData,
    sync::Arc,
};

/// Similar to [`Serialize`], but takes an extra `context` argument and
/// threads it through all nested structures.
pub trait SerializeWithContext<C>: Sized {
    fn serialize_with_context<S>(&self, serializer: S, context: &C) -> Result<S::Ok, S::Error>
    where
        S: Serializer;

    fn serialize_fields_with_context<S>(
        &self,
        serializer: S,
        context: &C,
        _fields: &HashSet<String>,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialize_with_context(serializer, context)
    }
}

pub struct SerializeWithContextWrapper<'a, C, T> {
    val: &'a T,
    context: &'a C,
}

impl<'a, C, T> SerializeWithContextWrapper<'a, C, T> {
    pub fn new(val: &'a T, context: &'a C) -> Self {
        Self { val, context }
    }
}

impl<C, T> Serialize for SerializeWithContextWrapper<'_, C, T>
where
    T: SerializeWithContext<C>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.val.serialize_with_context(serializer, self.context)
    }
}

pub struct SerializeFieldsWithContextWrapper<'a, C, T> {
    val: &'a T,
    context: &'a C,
    fields: &'a HashSet<String>,
}

impl<'a, C, T> SerializeFieldsWithContextWrapper<'a, C, T> {
    pub fn new(val: &'a T, context: &'a C, fields: &'a HashSet<String>) -> Self {
        Self {
            val,
            context,
            fields,
        }
    }
}

impl<C, T> Serialize for SerializeFieldsWithContextWrapper<'_, C, T>
where
    T: SerializeWithContext<C>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.val
            .serialize_fields_with_context(serializer, self.context, self.fields)
    }
}

/// Implement [`SerializeWithContext`] for types that implement
/// [`Serialize`] and don't support configurable serialization.
///
/// The implementation invokes [`Serialize::serialize`], ignoring the
/// context.
#[macro_export]
macro_rules! serialize_without_context {
    ($typ:tt) => {
        serialize_without_context!($typ,);
    };

    ($typ:tt, $($arg:tt),*) => {
        impl<C, $($arg),*> $crate::serde_with_context::SerializeWithContext<C> for $typ<$($arg),*>
        where
            $typ<$($arg),*>: serde::Serialize,
        {
            fn serialize_with_context<S>(&self, serializer: S, _context: &C) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serde::Serialize::serialize(self, serializer)
            }
        }
    };
}

serialize_without_context!(bool);
serialize_without_context!(i8);
serialize_without_context!(u8);
serialize_without_context!(i16);
serialize_without_context!(u16);
serialize_without_context!(i32);
serialize_without_context!(u32);
serialize_without_context!(i64);
serialize_without_context!(u64);
serialize_without_context!(i128);
serialize_without_context!(u128);
serialize_without_context!(usize);
serialize_without_context!(isize);
serialize_without_context!(String);
serialize_without_context!(char);

/// Used to pass the context to nested structures during serialization.
// This is only public because it's used in a macro; it's not supposed
// to be user-visible otherwise.
#[doc(hidden)]
pub struct SerializationContext<'se, C, T> {
    context: &'se C,
    value: &'se T,
    phantom: PhantomData<T>,
}

impl<'se, C, T> SerializationContext<'se, C, T> {
    pub fn new(context: &'se C, value: &'se T) -> Self {
        Self {
            context,
            value,
            phantom: PhantomData,
        }
    }
}

impl<C, T> Serialize for SerializationContext<'_, C, T>
where
    T: SerializeWithContext<C>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.value.serialize_with_context(serializer, self.context)
    }
}

impl<C, T> SerializeWithContext<C> for Vec<T>
where
    T: SerializeWithContext<C>,
{
    fn serialize_with_context<S>(&self, serializer: S, context: &C) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for element in self {
            seq.serialize_element(&SerializationContext::new(context, element))?;
        }

        seq.end()
    }
}

impl<C, K, V> SerializeWithContext<C> for BTreeMap<K, V>
where
    K: SerializeWithContext<C> + Ord,
    V: SerializeWithContext<C>,
{
    fn serialize_with_context<S>(&self, serializer: S, context: &C) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (k, v) in self {
            map.serialize_entry(
                &SerializationContext::new(context, k),
                &SerializationContext::new(context, v),
            )?;
        }
        map.end()
    }
}

impl<C, T> SerializeWithContext<C> for Arc<T>
where
    T: SerializeWithContext<C>,
{
    fn serialize_with_context<S>(&self, serializer: S, context: &C) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        T::serialize_with_context(&**self, serializer, context)
    }
}

impl<C, T> SerializeWithContext<C> for &T
where
    T: SerializeWithContext<C>,
{
    fn serialize_with_context<S>(&self, serializer: S, context: &C) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        T::serialize_with_context(*self, serializer, context)
    }

    fn serialize_fields_with_context<S>(
        &self,
        serializer: S,
        context: &C,
        fields: &HashSet<String>,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        T::serialize_fields_with_context(*self, serializer, context, fields)
    }
}

macro_rules! serialize_tuple {
    ([$num_fields:expr]($(($arg_name:ident, $arg_type:tt)),*)) => {
        #[allow(unused_variables)]
        #[allow(dead_code)]
        #[allow(unused_mut)]
        impl<C, $($arg_type),*> SerializeWithContext<C> for ($($arg_type),*)
        where
            $($arg_type: SerializeWithContext<C>),*
        {
            fn serialize_with_context<S>(&self, serializer: S, context: &C) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let ($($arg_name),*) = self;
                let mut tuple = serializer.serialize_tuple($num_fields)?;
                $(tuple.serialize_element(&SerializationContext::new(context, $arg_name))?;)*
                tuple.end()
            }
        }
    }
}

serialize_tuple!([0]());
serialize_tuple!([2]((v0, T0), (v1, T1)));
serialize_tuple!([3]((v0, T0), (v1, T1), (v2, T2)));
serialize_tuple!([4]((v0, T0), (v1, T1), (v2, T2), (v3, T3)));
serialize_tuple!([5]((v0, T0), (v1, T1), (v2, T2), (v3, T3), (v4, T4)));
serialize_tuple!([6](
    (v0, T0),
    (v1, T1),
    (v2, T2),
    (v3, T3),
    (v4, T4),
    (v5, T5)
));
serialize_tuple!([7](
    (v0, T0),
    (v1, T1),
    (v2, T2),
    (v3, T3),
    (v4, T4),
    (v5, T5),
    (v6, T6)
));
serialize_tuple!([8](
    (v0, T0),
    (v1, T1),
    (v2, T2),
    (v3, T3),
    (v4, T4),
    (v5, T5),
    (v6, T6),
    (v7, T7)
));
serialize_tuple!([9](
    (v0, T0),
    (v1, T1),
    (v2, T2),
    (v3, T3),
    (v4, T4),
    (v5, T5),
    (v6, T6),
    (v7, T7),
    (v8, T8)
));
serialize_tuple!([10](
    (v0, T0),
    (v1, T1),
    (v2, T2),
    (v3, T3),
    (v4, T4),
    (v5, T5),
    (v6, T6),
    (v7, T7),
    (v8, T8),
    (v9, T9)
));
serialize_tuple!([11](
    (v0, T0),
    (v1, T1),
    (v2, T2),
    (v3, T3),
    (v4, T4),
    (v5, T5),
    (v6, T6),
    (v7, T7),
    (v8, T8),
    (v9, T9),
    (v10, T10)
));
serialize_tuple!([12](
    (v0, T0),
    (v1, T1),
    (v2, T2),
    (v3, T3),
    (v4, T4),
    (v5, T5),
    (v6, T6),
    (v7, T7),
    (v8, T8),
    (v9, T9),
    (v10, T10),
    (v11, T11)
));

impl<C, T> SerializeWithContext<C> for Option<T>
where
    T: SerializeWithContext<C>,
{
    fn serialize_with_context<S>(&self, serializer: S, context: &C) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Some(x) => serializer.serialize_some(&SerializationContext::new(context, x)),
            None => serializer.serialize_none(),
        }
    }
}

/// Generate [`SerializeWithContext`] implementation parameterized by context
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
// TODO: This should be a procedural macro, but I don't have experience with those
// and it would take me too long to write one.  It should be possible to steal code
// from `serde_derive`.
#[macro_export]
macro_rules! serialize_struct {
    ($struct:ident($($arg:tt $(: $bound:tt)?),*)[$num_fields:expr]{$($field_name:ident[$column_name:tt]: $type:ty),* }) => {
        #[allow(unused_variables)]
        #[allow(unused_mut)]
        impl<C, $($arg),*> $crate::serde_with_context::SerializeWithContext<C> for $struct<$($arg),*>
        where
            $($arg: $crate::serde_with_context::SerializeWithContext<C>),*
            $($($arg : $bound,)?),*
        {
            #[inline(never)]
            fn serialize_with_context<S>(&self, serializer: S, context: &C) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut struct_serializer = serializer.serialize_struct(stringify!($struct), $num_fields)?;
                $(
                    serde::ser::SerializeStruct::serialize_field(&mut struct_serializer, $column_name, &$crate::serde_with_context::SerializationContext::new(context, &self.$field_name))?;
                )*
                serde::ser::SerializeStruct::end(struct_serializer)
            }

            #[inline(never)]
            fn serialize_fields_with_context<S>(
                &self,
                serializer: S,
                context: &C,
                fields: &std::collections::HashSet<String>,
            ) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut struct_serializer = serializer.serialize_struct(stringify!($struct), fields.len())?;
                $(
                    if fields.contains($column_name) {
                        serde::ser::SerializeStruct::serialize_field(&mut struct_serializer, $column_name, &$crate::serde_with_context::SerializationContext::new(context, &self.$field_name))?;
                    }
                )*
                serde::ser::SerializeStruct::end(struct_serializer)
            }

        }
    }
}

#[macro_export]
macro_rules! serialize_table_record {
    ($struct:ident[$num_fields:expr]{$($field_name:ident[$column_name:tt]: $type:ty),* }) => {
        #[allow(unused_variables)]
        #[allow(unused_mut)]
        impl $crate::serde_with_context::SerializeWithContext<$crate::serde_with_context::SqlSerdeConfig> for $struct {
            #[inline(never)]
            fn serialize_with_context<S>(&self, serializer: S, context: & $crate::serde_with_context::SqlSerdeConfig) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut struct_serializer = serializer.serialize_struct(stringify!($struct), $num_fields)?;
                $(
                    serde::ser::SerializeStruct::serialize_field(&mut struct_serializer, $column_name, &$crate::serde_with_context::SerializationContext::new(context, &self.$field_name))?;
                )*
                serde::ser::SerializeStruct::end(struct_serializer)
            }

            #[inline(never)]
            fn serialize_fields_with_context<S>(&self, serializer: S, context: & $crate::serde_with_context::SqlSerdeConfig, fields: &std::collections::HashSet<String>) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut struct_serializer = serializer.serialize_struct(stringify!($struct), fields.len())?;
                $(
                    if fields.contains($column_name) {
                        serde::ser::SerializeStruct::serialize_field(&mut struct_serializer, $column_name, &$crate::serde_with_context::SerializationContext::new(context, &self.$field_name))?;
                    }
                )*
                serde::ser::SerializeStruct::end(struct_serializer)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;

    use serde::{Deserialize, Serialize};

    use crate::serde_with_context::{SerializationContext, SerializeWithContext, SqlSerdeConfig};

    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct TUPLE0;
    serialize_struct!(TUPLE0()[0] {});

    static DEFAULT_CONFIG: LazyLock<SqlSerdeConfig> = LazyLock::new(SqlSerdeConfig::default);

    fn serialize_json_with_default_context<T>(val: &T) -> Result<String, serde_json::Error>
    where
        T: SerializeWithContext<SqlSerdeConfig>,
    {
        let mut result: Vec<u8> = Vec::new();
        val.serialize_with_context(
            &mut serde_json::Serializer::new(&mut result),
            &DEFAULT_CONFIG,
        )?;
        Ok(String::from_utf8(result).unwrap())
    }

    #[test]
    fn serialize_unit() {
        assert_eq!(serialize_json_with_default_context(&TUPLE0).unwrap(), "{}");
    }

    #[derive(Debug, Eq, PartialEq, Serialize)]
    #[allow(non_snake_case)]
    struct Struct2 {
        #[allow(non_snake_case)]
        cc_num: u64,
        #[allow(non_snake_case)]
        first: Option<String>,
    }
    serialize_struct!(Struct2()[3] {
        cc_num["cc_num"]: u64,
        first["FIRST"]: Option<String>
    });

    #[test]
    fn serialize_struct2() {
        assert_eq!(
            serialize_json_with_default_context(&Struct2 {
                cc_num: 100,
                first: None,
            })
            .unwrap(),
            r#"{"cc_num":100,"FIRST":null}"#
        );

        assert_eq!(
            serialize_json_with_default_context(&Struct2 {
                cc_num: 100,
                first: None,
            })
            .unwrap(),
            r#"{"cc_num":100,"FIRST":null}"#
        );

        assert_eq!(
            serialize_json_with_default_context(&Struct2 {
                cc_num: 100,
                first: Some("foo".to_string()),
            })
            .unwrap(),
            r#"{"cc_num":100,"FIRST":"foo"}"#
        );
    }

    #[derive(Debug, Eq, PartialEq, Serialize)]
    #[allow(non_snake_case)]
    struct UnicodeStruct {
        f1: bool,
        f2: String,
        f3: Option<u8>,
    }

    serialize_table_record!(UnicodeStruct[3] {
        f1["f1-bool"]: bool,
        f2["УКРАЇНСЬКА"]: String,
        f3["unicode⌛👏"]: Option<u8>
    });

    #[test]
    fn unicode() {
        assert_eq!(
            serialize_json_with_default_context(&UnicodeStruct {
                f1: true,
                f2: "foo".to_string(),
                f3: Some(100)
            })
            .unwrap(),
            r#"{"f1-bool":true,"УКРАЇНСЬКА":"foo","unicode⌛👏":100}"#
        );
    }

    fn serialize_csv_with_default_context<T>(val: &T) -> Result<String, csv::Error>
    where
        T: SerializeWithContext<SqlSerdeConfig>,
    {
        let mut result = Vec::new();
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(&mut result);
        wtr.serialize(SerializationContext::new(&SqlSerdeConfig::default(), val))
            .unwrap();
        Ok(std::str::from_utf8(wtr.into_inner().unwrap())
            .unwrap()
            .to_string())
    }

    #[test]
    fn csv() {
        let val = UnicodeStruct {
            f1: true,
            f2: "foo".to_string(),
            f3: Some(100),
        };

        assert_eq!(
            &serialize_csv_with_default_context(&val).unwrap(),
            r#"true,foo,100
"#
        );
        let val = UnicodeStruct {
            f1: true,
            f2: "x".to_string(),
            f3: None,
        };

        assert_eq!(
            &serialize_csv_with_default_context(&val).unwrap(),
            r#"true,x,
"#
        );
    }
}
