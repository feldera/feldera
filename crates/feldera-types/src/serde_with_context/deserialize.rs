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
//!   as `Deserialize` with the extra context argument.
//!
//! * [`deserialize_without_context`](`crate::deserialize_without_context`)
//!   macro is used to implement [`DeserializeWithContext`] for types that implement
//!   `serde::Deserialize` and don't support configurable deserialization.
//!
//! * We implement [`DeserializeWithContext`] for vectors, tuples, and options
//!   by passing the context to each element.
//!
//! * The [`deserialize_struct`](`crate::deserialize_struct`) macro is used to implement [`DeserializeWithContext`] for structs.
//!
//! * The [`deserialize_table_record`](`crate::deserialize_table_record`) macro implements [`DeserializeWithContext`] for SQL table records.  It's similar
//!   to [`deserialize_struct`](`crate::deserialize_struct`), but additionally
//!   handles case-(in)sensitive SQL columns.

// TODOs:
// This could benefit from using procedural macros to auto-derive
// [`DeserializeWithContext`].

use erased_serde::Deserializer as ErasedDeserializer;
use serde::de::Error as SerdeError;
use serde::{
    de::{DeserializeSeed, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::{collections::BTreeMap, fmt, marker::PhantomData, sync::Arc};

use crate::serde_with_context::SqlSerdeConfig;

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
        impl<'de, C, $($arg),*> $crate::serde_with_context::DeserializeWithContext<'de, C> for $typ<$($arg),*>
        where
            $typ<$($arg),*>: serde::Deserialize<'de>,
        {
            #[inline(never)]
            fn deserialize_with_context<D>(deserializer: D, _context: &'de C) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>
            {
                serde::Deserialize::deserialize(deserializer)
            }
        }
    };
}

impl<'de, C> DeserializeWithContext<'de, C> for &'de str {
    #[inline(never)]
    fn deserialize_with_context<D>(deserializer: D, _context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer)
    }
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

// Used to pass the context to nested structures using the
// `DeserializeSeed` mechanism.  This is only public because it's
// use in a macro; it's not supposed to be user-visible otherwise.
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

    #[inline(never)]
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
    #[inline(never)]
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

impl<'de, C, K, V> DeserializeWithContext<'de, C> for BTreeMap<K, V>
where
    K: DeserializeWithContext<'de, C> + Ord,
    V: DeserializeWithContext<'de, C>,
{
    #[inline(never)]
    fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MapVisitor<'de, C, K, V> {
            context: &'de C,
            phantom: PhantomData<(K, V)>,
        }

        impl<'de, C, K, V> MapVisitor<'de, C, K, V> {
            fn new(context: &'de C) -> Self {
                Self {
                    context,
                    phantom: PhantomData,
                }
            }
        }

        impl<'de, C, K, V> Visitor<'de> for MapVisitor<'de, C, K, V>
        where
            K: DeserializeWithContext<'de, C> + Ord,
            V: DeserializeWithContext<'de, C>,
        {
            type Value = BTreeMap<K, V>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a map")
            }

            fn visit_map<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut result = BTreeMap::new();

                while let Some((key, value)) = seq.next_entry_seed(
                    DeserializationContext::new(self.context),
                    DeserializationContext::new(self.context),
                )? {
                    result.insert(key, value);
                }

                Ok(result)
            }
        }

        deserializer.deserialize_map(MapVisitor::new(context))
    }
}

impl<'de, C, T> DeserializeWithContext<'de, C> for Arc<T>
where
    T: DeserializeWithContext<'de, C>,
{
    #[inline(never)]
    fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = T::deserialize_with_context(deserializer, context)?;
        Ok(Arc::new(val))
    }
}

#[macro_export]
macro_rules! deserialize_tuple {
    ([$num_fields:expr]($($arg:tt),*)) => {
        #[allow(unused_variables)]
        #[allow(dead_code)]
        #[allow(unused_mut)]
        impl<'de, C, $($arg),*> DeserializeWithContext<'de, C> for ($($arg),*)
        where
            $($arg: DeserializeWithContext<'de, C>),*
        {
            #[inline(never)]
            fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>
            {
                struct TupleVisitor<'de, C, $($arg),*> {
                    context: &'de C,
                    phantom: std::marker::PhantomData<($($arg),*)>,
                }

                impl<'de, C, $($arg),*> TupleVisitor<'de, C, $($arg),*> {
                    fn new(context: &'de C) -> Self {
                        Self {
                            context,
                            phantom: std::marker::PhantomData,
                        }
                    }
                }

                impl<'de, C, $($arg),*> serde::de::Visitor<'de> for TupleVisitor<'de, C, $($arg),*>
                where
                    $($arg: $crate::serde_with_context::DeserializeWithContext<'de, C>),*
                {
                    type Value = ($($arg),*);

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        write!(formatter, concat!("a tuple of size ", stringify!($num_fields)))
                    }

                    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                    where
                        A: serde::de::SeqAccess<'de>,
                    {
                        let mut cnt: i32 = -1;
                        Ok(($({cnt+=1; seq
                            .next_element_seed(<$crate::serde_with_context::DeserializationContext<C, $arg>>::new(self.context))?
                            .ok_or_else(|| {
                                use serde::de::Error;
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
    #[inline(never)]
    fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OptionVisitor<'de, C, T> {
            context: &'de C,
            phantom: PhantomData<T>,
        }

        impl<'de, C, T> OptionVisitor<'de, C, T> {
            fn new(context: &'de C) -> Self {
                Self {
                    context,
                    phantom: PhantomData,
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
                D: Deserializer<'de>,
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
/// * `$default` - `None`, or `Some(value)` for fields that have a default value.
// TODO: This should be a procedural macro, but I don't have experience with those
// and it would take me too long to write one.  It should be possible to steal code
// from `serde_derive`.
#[macro_export]
macro_rules! deserialize_struct {
    ($struct:ident($($arg:tt $(: $bound:tt)?),*)[$num_fields:expr]{$($field_name:ident: $type:ty = $default:expr),* }) => {
        #[allow(unused_variables)]
        #[allow(unused_mut)]
        impl<'de, C, $($arg),*> $crate::serde_with_context::DeserializeWithContext<'de, C> for $struct<$($arg),*>
        where
            $($arg: $crate::serde_with_context::DeserializeWithContext<'de, C>),*
            $($($arg : $bound,)?),*
        {
            #[inline(never)]
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
                    $($arg: $crate::serde_with_context::DeserializeWithContext<'de, C>),*
                    $($($arg : $bound,)?),*
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
                                    $field_name = Some(map.next_value_seed(<$crate::serde_with_context::DeserializationContext<C, $type>>::new(self.context))?);
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
                                seq.next_element_seed(<$crate::serde_with_context::DeserializationContext<C, $type>>::new(self.context))?
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

/// An object-safe version of `DeserializeWithContext`. Used to deserialize individual struct fields
/// via dynamic dispatch.
pub trait ErasedDeserializeWithContext {
    fn erased_deserialize_with_context<'de>(
        &mut self,
        deserializer: &mut dyn ErasedDeserializer<'de>,
        context: &'de SqlSerdeConfig,
    ) -> Result<(), erased_serde::Error>;

    fn check_missing(&mut self, column_name: &'static str) -> Result<(), erased_serde::Error>;
}

impl<T> ErasedDeserializeWithContext for Option<T>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + 'static,
{
    #[inline(never)]
    fn erased_deserialize_with_context<'de>(
        &mut self,
        deserializer: &mut dyn ErasedDeserializer<'de>,
        context: &'de SqlSerdeConfig,
    ) -> Result<(), erased_serde::Error> {
        *self = Some(DeserializeWithContext::deserialize_with_context(
            deserializer,
            context,
        )?);
        Ok(())
    }

    fn check_missing(&mut self, column_name: &'static str) -> Result<(), erased_serde::Error> {
        if self.is_none() {
            return Err(missing_field_error(column_name));
        }

        Ok(())
    }
}

#[doc(hidden)]
pub struct ErasedDeserializationContext<'de, 'a> {
    context: &'de SqlSerdeConfig,
    val: &'a mut dyn ErasedDeserializeWithContext,
}

impl<'de, 'a> ErasedDeserializationContext<'de, 'a> {
    pub fn new(
        val: &'a mut dyn ErasedDeserializeWithContext,
        context: &'de SqlSerdeConfig,
    ) -> Self {
        Self { val, context }
    }
}

impl<'de> DeserializeSeed<'de> for ErasedDeserializationContext<'de, '_> {
    type Value = ();

    #[inline(never)]
    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut deserializer = <dyn ErasedDeserializer>::erase(deserializer);
        self.val
            .erased_deserialize_with_context(&mut deserializer, self.context)
            .map_err(D::Error::custom)?;

        Ok(())
    }
}

#[inline(never)]
pub fn missing_field_error(column_name: &'static str) -> erased_serde::Error {
    erased_serde::Error::missing_field(column_name)
}

#[inline(never)]
pub fn field_parse_error<E: serde::de::Error>(column_name: &str, error: E) -> E {
    E::custom(
        serde_json::to_string(&FieldParseError {
            field: column_name.to_string(),
            description: error.to_string(),
        })
        .unwrap(),
    )
}

#[inline(never)]
pub fn invalid_length_error<E: serde::de::Error>(actual: usize, expected: usize) -> E {
    E::invalid_length(actual, &format!("{} columns", expected).as_str())
}

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
        impl<'de> $crate::serde_with_context::DeserializeWithContext<'de, $crate::serde_with_context::SqlSerdeConfig> for $table {
            #[inline(never)]
            fn deserialize_with_context<D>(deserializer: D, context: &'de $crate::serde_with_context::SqlSerdeConfig) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>
            {
                use $crate::serde_with_context::{SqlSerdeConfig, ErasedDeserializeWithContext, ErasedDeserializationContext, field_parse_error, invalid_length_error};
                use serde::de::{MapAccess, SeqAccess, Error as _};
                use std::{cell::LazyCell, collections::HashMap};

                struct RecordVisitor<'de> {
                    context: &'de SqlSerdeConfig,
                    $($field_name: Option<$type>,)*
                }

                thread_local! {
                    // maps LOWERCASE column names to field indexes.
                    static FIELD_MAP: LazyCell<HashMap<String, usize>> = LazyCell::new(|| {
                        let mut map = HashMap::new();
                        let mut _idx = 0usize;

                        $(
                            map.insert($column_name.to_string().to_lowercase(), _idx);
                            _idx += 1;
                        )*

                        map
                    });
                }

                impl<'de> RecordVisitor<'de> {
                    fn new(context: &'de SqlSerdeConfig) -> Self {
                        Self {
                            context,
                            $($field_name: None,)*
                        }
                    }

                    $(
                        #[inline(never)]
                        fn $field_name<E: serde::de::Error>(&mut self) -> Result<(), E> {
                            $(self.$field_name = self.$field_name.take().map($postprocess);)*
                            if self.$field_name.is_none() {
                                self.$field_name = $init;
                            }
                            self.$field_name.check_missing($column_name).map_err(|e| serde::de::Error::custom(e))
                        }
                    )*
                }

                impl<'de> serde::de::Visitor<'de> for RecordVisitor<'de> {
                    type Value = $table;

                    fn expecting(
                        &self,
                        formatter: &mut std::fmt::Formatter<'_>,
                    ) -> std::fmt::Result {
                        write!(formatter, concat!("a record of type ", stringify!($table)))
                    }

                    #[inline(never)]
                    fn visit_map<A>(mut self, mut map: A) -> Result<$table, A::Error>
                    where
                        A: MapAccess<'de>
                    {
                        let mut deserializers: [(&mut dyn ErasedDeserializeWithContext, bool, &'static str); $num_cols] = [
                            $((&mut self.$field_name as &mut dyn ErasedDeserializeWithContext, $case_sensitive, $column_name)
                            ),*
                        ];

                        while let Some(column_name) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                            let lowercase_column_name = column_name.to_lowercase();
                            if let Some(idx) = FIELD_MAP.with(|field_map| field_map.get(&lowercase_column_name).cloned()) {
                                let (deserializer, case_sensitive, expected_column_name) = &mut deserializers[idx];
                                if *case_sensitive {
                                    if column_name.as_ref() != *expected_column_name {
                                        return Err(A::Error::custom(format!("incorrect case-sensitive field name: expected: {expected_column_name}, found: {column_name}")));
                                    }
                                }
                                let res = map.next_value_seed(ErasedDeserializationContext::new(*deserializer, &self.context));

                                if let Err(e) = res {
                                    return Err(field_parse_error(column_name.as_ref(), e));
                                }
                            } else {
                                let _ = map.next_value::<serde::de::IgnoredAny>()?;
                            }
                        }

                        $(
                            self.$field_name()?;
                        )*

                        Ok($table {
                                $($field_name: self.$field_name.unwrap(),
                                )*
                            })
                    }

                    fn visit_seq<A>(mut self, mut seq: A) -> Result<$table, A::Error>
                    where
                        A: SeqAccess<'de>
                    {
                        let mut deserializers: [(&'static str, &mut dyn ErasedDeserializeWithContext); $num_cols] = [
                            $(($column_name, &mut self.$field_name as &mut dyn ErasedDeserializeWithContext)
                            ),*
                        ];

                        let mut _cols: usize = 0;

                        for (_cols, (column_name, deserializer)) in deserializers.iter_mut().enumerate()
                        {
                            let res = seq.next_element_seed(ErasedDeserializationContext::new(*deserializer, &self.context));
                            match res {
                                Err(e) => return Err(field_parse_error(column_name, e)),
                                Ok(None) => return Err(invalid_length_error(_cols, $num_cols)),
                                Ok(Some(_)) => (),
                            };
                        }

                        while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                        Ok($table {
                            $($field_name: self.$field_name.unwrap(),
                            )*
                        })

                    }
                }

                let mut visitor = RecordVisitor::new(context);

                // XXX: At least with `serde_json::Deserializer`, `deserialize_struct` tells the
                // deserializer to work with either map or sequence representation
                // (while `deserialize_map` only deserializes from a map).  It seems that
                // `serde_json` ignores the actual field names provided, so this does not
                // break the case-insensitive behavior.  Not sure if this is true for all
                // deserializers.  In the future we may want to make the choice between map
                // and sequence representations configurable, so we won't need this trick.
                //deserializer.deserialize_map(visitor)
                deserializer
                        .deserialize_struct($sql_table, [$($column_name,)*].as_slice(), visitor)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;

    use crate::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};

    #[derive(Debug, Eq, PartialEq)]
    struct TUPLE0;
    deserialize_table_record!(TUPLE0["EmptyTable", 0] {});

    static DEFAULT_CONFIG: LazyLock<SqlSerdeConfig> = LazyLock::new(SqlSerdeConfig::default);

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
            Err(r#"incorrect case-sensitive field name: expected: fIeLd1, found: field1 at line 1 column 9"#.to_string())
        );
        assert_eq!(
            deserialize_with_default_context::<CaseSensitive>(
                r#"{"fIeLd1": true, "FIELD2": "foo"}"#
            )
            .map_err(|e| e.to_string()),
            Err(r#"incorrect case-sensitive field name: expected: field2, found: FIELD2 at line 1 column 25"#.to_string())
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
        assert_eq!(deserialize_with_default_context::<CaseSensitive>(r#"{"fIeLd1": false, "field2": "foo", "FIELD3": true}"#).map_err(|e| e.to_string()), Err(r#"{"field":"FIELD3","description":"invalid type: boolean `true`, expected u8 at line 1 column 49"} at line 1 column 50"#.to_string()));
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
            Err(r#"{"field":"Українська","description":"invalid type: integer `10`, expected a string at line 1 column 51"} at line 1 column 51"#.to_string())
        );
        assert_eq!(
            deserialize_with_default_context::<UnicodeStruct>(
                r#"[true, 10, 100]"#
            ).map_err(|e| e.to_string()),
            Err(r#"{"field":"українська","description":"invalid type: integer `10`, expected a string at line 1 column 9"} at line 1 column 11"#.to_string())
        );
    }
}
