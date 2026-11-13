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
    Deserialize, Deserializer, Serialize,
    de::{DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use std::{
    cell::LazyCell,
    collections::{BTreeMap, HashMap},
    fmt,
    marker::PhantomData,
    sync::Arc,
    thread::LocalKey,
};

use crate::serde_with_context::SqlSerdeConfig;

/// Similar to [`Deserialize`], but takes an extra `context` argument and
/// threads it through all nested structures. The context can be used to
/// configure the deserialization behavior.
///
/// Additionally, the `AUX` argument can be used to pass additional metadata
/// to the deserializer, such as Kafka headers, topic name, etc.
pub trait DeserializeWithContext<'de, C, AUX = ()>: Sized {
    fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>;

    fn deserialize_with_context_aux<D>(
        deserializer: D,
        context: &'de C,
        _aux: &'de Option<AUX>,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize_with_context(deserializer, context)
    }
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
        impl<'de, C, AUX, $($arg),*> $crate::serde_with_context::DeserializeWithContext<'de, C, AUX> for $typ<$($arg),*>
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

impl<'de, C, AUX> DeserializeWithContext<'de, C, AUX> for &'de str {
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
pub struct DeserializationContext<'de, C, AUX, T> {
    context: &'de C,
    phantom: PhantomData<(AUX, T)>,
}

impl<'de, C, AUX, T> DeserializationContext<'de, C, AUX, T> {
    pub fn new(context: &'de C) -> Self {
        Self {
            context,
            phantom: PhantomData,
        }
    }
}

impl<'de, C, AUX, T> DeserializeSeed<'de> for DeserializationContext<'de, C, AUX, T>
where
    T: DeserializeWithContext<'de, C, AUX>,
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

/// Like [`DeserializationContext`], but also passes record metadata to the
/// deserialized value.
struct DeserializationContextAux<'de, C, AUX, T> {
    context: &'de C,
    aux: &'de Option<AUX>,
    phantom: PhantomData<T>,
}

impl<'de, C, AUX, T> DeserializeSeed<'de> for DeserializationContextAux<'de, C, AUX, T>
where
    T: DeserializeWithContext<'de, C, AUX>,
{
    type Value = T;

    #[inline(never)]
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize_with_context_aux(deserializer, self.context, self.aux)
    }
}

impl<'de, C, AUX: 'de, T> DeserializeWithContext<'de, C, AUX> for Vec<T>
where
    T: DeserializeWithContext<'de, C, AUX>,
{
    #[inline(never)]
    fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(VecVisitor::<C, AUX, T>::new(context, None))
    }

    /// Deserialize an array of records, passing `aux` to each of them.
    ///
    /// Formats that encode a batch of records in one payload, e.g., Arrow,
    /// deserialize the batch as a `Vec`, so each record in the batch must
    /// receive the metadata attached to the batch.
    #[inline(never)]
    fn deserialize_with_context_aux<D>(
        deserializer: D,
        context: &'de C,
        aux: &'de Option<AUX>,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(VecVisitor::<C, AUX, T>::new(context, Some(aux)))
    }
}

struct VecVisitor<'de, C, AUX, T> {
    context: &'de C,
    aux: Option<&'de Option<AUX>>,
    phantom: PhantomData<T>,
}

impl<'de, C, AUX, T> VecVisitor<'de, C, AUX, T> {
    fn new(context: &'de C, aux: Option<&'de Option<AUX>>) -> Self {
        Self {
            context,
            aux,
            phantom: PhantomData,
        }
    }
}

impl<'de, C, AUX: 'de, T> Visitor<'de> for VecVisitor<'de, C, AUX, T>
where
    T: DeserializeWithContext<'de, C, AUX>,
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

        // Both arms deserialize the same element type; they differ only in
        // whether each element also receives the batch metadata.
        match self.aux {
            Some(aux) => {
                while let Some(val) = seq.next_element_seed(DeserializationContextAux {
                    context: self.context,
                    aux,
                    phantom: PhantomData,
                })? {
                    result.push(val)
                }
            }
            None => {
                while let Some(val) =
                    seq.next_element_seed(DeserializationContext::new(self.context))?
                {
                    result.push(val)
                }
            }
        }

        Ok(result)
    }
}

impl<'de, C, AUX, K, V> DeserializeWithContext<'de, C, AUX> for BTreeMap<K, V>
where
    K: DeserializeWithContext<'de, C, AUX> + Ord,
    V: DeserializeWithContext<'de, C, AUX>,
{
    #[inline(never)]
    fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MapVisitor<'de, C, AUX, K, V> {
            context: &'de C,
            phantom: PhantomData<(AUX, K, V)>,
        }

        impl<'de, C, AUX, K, V> MapVisitor<'de, C, AUX, K, V> {
            fn new(context: &'de C) -> Self {
                Self {
                    context,
                    phantom: PhantomData,
                }
            }
        }

        impl<'de, C, AUX, K, V> Visitor<'de> for MapVisitor<'de, C, AUX, K, V>
        where
            K: DeserializeWithContext<'de, C, AUX> + Ord,
            V: DeserializeWithContext<'de, C, AUX>,
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

impl<'de, C, AUX, T> DeserializeWithContext<'de, C, AUX> for Arc<T>
where
    T: DeserializeWithContext<'de, C, AUX>,
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
    ([$num_fields:expr_2021]($($arg:tt),*)) => {
        #[allow(unused_variables)]
        #[allow(dead_code)]
        #[allow(unused_mut)]
        impl<'de, C, AUX, $($arg),*> DeserializeWithContext<'de, C, AUX> for ($($arg),*)
        where
            $($arg: DeserializeWithContext<'de, C, AUX>),*
        {
            #[inline(never)]
            fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>
            {
                struct TupleVisitor<'de, C, AUX, $($arg),*> {
                    context: &'de C,
                    phantom: std::marker::PhantomData<(AUX, $($arg),*)>,
                }

                impl<'de, C, AUX, $($arg),*> TupleVisitor<'de, C, AUX, $($arg),*> {
                    fn new(context: &'de C) -> Self {
                        Self {
                            context,
                            phantom: std::marker::PhantomData,
                        }
                    }
                }

                impl<'de, C, AUX, $($arg),*> serde::de::Visitor<'de> for TupleVisitor<'de, C, AUX, $($arg),*>
                where
                    $($arg: $crate::serde_with_context::DeserializeWithContext<'de, C, AUX>),*
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
                            .next_element_seed(<$crate::serde_with_context::DeserializationContext<C, AUX, $arg>>::new(self.context))?
                            .ok_or_else(|| {
                                use serde::de::Error;
                                A::Error::invalid_length(cnt as usize, &self)
                            })?}),*))
                    }
                }

                deserializer.deserialize_tuple($num_fields, TupleVisitor::<C, AUX, $($arg),*>::new(context))
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

impl<'de, C, AUX, T> DeserializeWithContext<'de, C, AUX> for Option<T>
where
    T: DeserializeWithContext<'de, C, AUX>,
{
    #[inline(never)]
    fn deserialize_with_context<D>(deserializer: D, context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OptionVisitor<'de, C, AUX, T> {
            context: &'de C,
            phantom: PhantomData<(AUX, T)>,
        }

        impl<'de, C, AUX, T> OptionVisitor<'de, C, AUX, T> {
            fn new(context: &'de C) -> Self {
                Self {
                    context,
                    phantom: PhantomData,
                }
            }
        }

        impl<'de, C, AUX, T> Visitor<'de> for OptionVisitor<'de, C, AUX, T>
        where
            T: DeserializeWithContext<'de, C, AUX>,
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
    ($struct:ident($($arg:tt $(: $bound:tt)?),*)[$num_fields:expr_2021]{$($field_name:ident: $type:ty = $default:expr_2021),* }) => {
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
pub trait ErasedDeserializeWithContext<AUX> {
    fn erased_deserialize_with_context<'de>(
        &mut self,
        deserializer: &mut dyn ErasedDeserializer<'de>,
        context: &'de SqlSerdeConfig,
    ) -> Result<(), erased_serde::Error>;

    fn check_missing(&mut self, column_name: &'static str) -> Result<(), erased_serde::Error>;
}

impl<T, AUX> ErasedDeserializeWithContext<AUX> for Option<T>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, AUX> + 'static,
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
pub struct ErasedDeserializationContext<'de, 'a, AUX> {
    context: &'de SqlSerdeConfig,
    val: &'a mut dyn ErasedDeserializeWithContext<AUX>,
}

impl<'de, 'a, AUX> ErasedDeserializationContext<'de, 'a, AUX> {
    pub fn new(
        val: &'a mut dyn ErasedDeserializeWithContext<AUX>,
        context: &'de SqlSerdeConfig,
    ) -> Self {
        Self { val, context }
    }
}

impl<'de, AUX> DeserializeSeed<'de> for ErasedDeserializationContext<'de, '_, AUX> {
    type Value = ();

    #[inline(never)]
    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut deserializer = deserializer;
        match as_erased(&mut deserializer) {
            Some(erased) => self
                .val
                .erased_deserialize_with_context(erased, self.context),
            None => self.val.erased_deserialize_with_context(
                &mut <dyn ErasedDeserializer>::erase(deserializer),
                self.context,
            ),
        }
        .map_err(D::Error::custom)
    }
}

/// Gives `deserializer` as a type-erased deserializer when it is one already.
///
/// The fields of a nested record, and of each record read from Arrow or Avro,
/// get a type-erased deserializer. To erase it again adds a virtual call to
/// each visitor call and makes `erased_serde` put each result on the heap.
fn as_erased<'r, 'de, D>(deserializer: &'r mut D) -> Option<&'r mut dyn ErasedDeserializer<'de>>
where
    D: Deserializer<'de>,
{
    if typeid::of::<D>() != typeid::of::<&mut dyn ErasedDeserializer<'static>>() {
        return None;
    }
    // SAFETY: The type IDs are equal and ignore only lifetimes, so `D` is
    // `&'a mut dyn ErasedDeserializer<'x>`. `'x` is `'de`, because that type
    // implements `Deserializer<'de>` for no other `'x`, and `'a` outlives the
    // borrow `'r` of a value that contains it.
    let erased = unsafe {
        &mut *std::ptr::from_mut(deserializer).cast::<&mut dyn ErasedDeserializer<'de>>()
    };
    Some(&mut **erased)
}

/// Deserializes a column name of a table record into the index of its field,
/// or `None` for a column the record does not have. A name spelled other than
/// the field leaves its spelling in `spelling`, for error messages.
///
/// Formats such as Arrow send the columns of each row in the same order, so
/// the column after the previous one is tried first, by exact name. Only a
/// miss pays for the lowercase copy and the hash lookup.
///
/// The value is two words, so `erased_serde` returns it without a heap
/// allocation.
#[doc(hidden)]
pub struct ColumnIndexSeed<'a> {
    /// Name and case sensitivity of each field, in field order.
    columns: &'a [(&'static str, bool)],
    /// Lowercase column name to field index.
    field_map: &'static LocalKey<LazyCell<HashMap<String, usize>>>,
    /// The field index to try first.
    hint: usize,
    /// Receives the spelling of a name that matched a field other than exactly.
    spelling: &'a mut Option<String>,
}

impl<'a> ColumnIndexSeed<'a> {
    pub fn new(
        columns: &'a [(&'static str, bool)],
        field_map: &'static LocalKey<LazyCell<HashMap<String, usize>>>,
        hint: usize,
        spelling: &'a mut Option<String>,
    ) -> Self {
        Self {
            columns,
            field_map,
            hint,
            spelling,
        }
    }

    fn lookup<E: SerdeError>(self, name: &str) -> Result<Option<usize>, E> {
        if let Some((expected, _)) = self.columns.get(self.hint)
            && *expected == name
        {
            return Ok(Some(self.hint));
        }

        let lowercase_name = name.to_lowercase();
        let Some((idx, ambiguous)) = self.field_map.with(|field_map| {
            let idx = field_map.get(&lowercase_name).copied()?;
            Some((idx, field_map.len() != self.columns.len()))
        }) else {
            return Ok(None);
        };
        // Fields that differ only in case, such as `a` and `"A"`, share an
        // entry of the map. The field with this spelling gets the value, as
        // with the hint, so the order of the columns in the input does not
        // change the result.
        let idx = if ambiguous {
            let exact = self.columns.iter().position(|(field, _)| *field == name);
            exact.unwrap_or(idx)
        } else {
            idx
        };
        let (expected, case_sensitive) = self.columns[idx];
        if case_sensitive && name != expected {
            return Err(E::custom(format!(
                "incorrect case-sensitive field name: expected: {expected}, found: {name}"
            )));
        }
        if name != expected {
            *self.spelling = Some(name.to_string());
        }
        Ok(Some(idx))
    }
}

impl<'de> DeserializeSeed<'de> for ColumnIndexSeed<'_> {
    type Value = Option<usize>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for ColumnIndexSeed<'_> {
    type Value = Option<usize>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a column name")
    }

    fn visit_str<E: SerdeError>(self, name: &str) -> Result<Self::Value, E> {
        self.lookup(name)
    }

    fn visit_bytes<E: SerdeError>(self, name: &[u8]) -> Result<Self::Value, E> {
        match std::str::from_utf8(name) {
            Ok(name) => self.lookup(name),
            Err(_) => Err(E::invalid_value(
                serde::de::Unexpected::Bytes(name),
                &"a UTF-8 column name",
            )),
        }
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
    ($table:ident[$sql_table:tt, $num_cols:expr_2021]{$(($field_name:ident, $column_name:tt, $case_sensitive:tt, $type:ty, $init:expr_2021 $(, $postprocess:expr_2021)*)),* }) => {
        deserialize_table_record!($table[$sql_table, (), $num_cols]{$(($field_name, $column_name, $case_sensitive, $type, $init $(,$postprocess)*)),* });
    };

    ($table:ident[$sql_table:tt, $aux_type:ty, $num_cols:expr_2021]{$(($field_name:ident, $column_name:tt, $case_sensitive:tt, $type:ty, $init:expr_2021 $(, $postprocess:expr_2021)*)),* }) => {
        #[allow(non_snake_case)]
        #[allow(unused_variables)]
        #[allow(unused_mut)]
        #[allow(dead_code)]
        impl<'de> $crate::serde_with_context::DeserializeWithContext<'de, $crate::serde_with_context::SqlSerdeConfig, $aux_type> for $table {
            #[inline(never)]
            fn deserialize_with_context<D>(deserializer: D, context: &'de $crate::serde_with_context::SqlSerdeConfig) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>
            {
                Self::deserialize_with_context_aux(deserializer, context, &None)
            }

            #[inline(never)]
            fn deserialize_with_context_aux<D>(deserializer: D, context: &'de $crate::serde_with_context::SqlSerdeConfig, __feldera_metadata: &'de Option<$aux_type>) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>
            {
                use $crate::serde_with_context::{SqlSerdeConfig, ErasedDeserializeWithContext, ErasedDeserializationContext, ColumnIndexSeed, field_parse_error, invalid_length_error};
                use serde::de::{MapAccess, SeqAccess};
                use std::{cell::LazyCell, collections::HashMap};

                struct RecordVisitor<'de> {
                    context: &'de SqlSerdeConfig,
                    // The columns that a positional input does not have.
                    absent: Option<&'de [bool]>,
                    __feldera_metadata: &'de Option<$aux_type>,
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
                    fn new(context: &'de SqlSerdeConfig, __feldera_metadata: &'de Option<$aux_type>) -> Self {
                        // The absent columns are those of this record only; the
                        // values of its columns use the configuration without them.
                        let (context, absent) = match &context.absent_columns {
                            Some(absent) => (
                                absent.field_config(),
                                // No flags when the input has all the columns.
                                (absent.num_present() != absent.absent().len()
                                    || absent.absent().len() != $num_cols)
                                    .then(|| absent.absent()),
                            ),
                            None => (context, None),
                        };
                        Self {
                            context,
                            absent,
                            __feldera_metadata,
                            $($field_name: None,)*
                        }
                    }

                    $(
                        #[inline(never)]
                        fn $field_name<E: serde::de::Error>(&mut self) -> Result<(), E> {
                            $(self.$field_name = self.$field_name.take().map($postprocess);)*
                            if self.$field_name.is_none() {
                                self.$field_name = $init(self.__feldera_metadata);
                            }
                            ErasedDeserializeWithContext::<$aux_type>::check_missing(&mut self.$field_name, $column_name).map_err(|e| serde::de::Error::custom(e))
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
                        const COLUMNS: [(&'static str, bool); $num_cols] = [$(($column_name, $case_sensitive)),*];

                        let mut deserializers: [&mut dyn ErasedDeserializeWithContext<$aux_type>; $num_cols] = [
                            $(&mut self.$field_name as &mut dyn ErasedDeserializeWithContext<$aux_type>
                            ),*
                        ];

                        let mut hint = 0;
                        let mut spelling = None;
                        while let Some(idx) = map.next_key_seed(ColumnIndexSeed::new(&COLUMNS, &FIELD_MAP, hint, &mut spelling))? {
                            let Some(idx) = idx else {
                                let _ = map.next_value::<serde::de::IgnoredAny>()?;
                                continue;
                            };
                            let res = map.next_value_seed(ErasedDeserializationContext::new(deserializers[idx], &self.context));

                            if let Err(e) = res {
                                return Err(field_parse_error(spelling.as_deref().unwrap_or(COLUMNS[idx].0), e));
                            }
                            spelling = None;
                            hint = idx + 1;
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
                        let mut deserializers: [(&'static str, &mut dyn ErasedDeserializeWithContext<$aux_type>); $num_cols] = [
                            $(($column_name, &mut self.$field_name as &mut dyn ErasedDeserializeWithContext<$aux_type>)
                            ),*
                        ];

                        let absent = self.absent;
                        if absent.is_some_and(|absent| absent.len() != $num_cols) {
                            return Err(invalid_length_error(absent.map_or(0, <[bool]>::len), $num_cols));
                        }
                        let mut _cols: usize = 0;

                        for (_cols, (column_name, deserializer)) in deserializers.iter_mut().enumerate()
                        {
                            if absent.is_some_and(|absent| absent[_cols]) {
                                continue;
                            }
                            let res = seq.next_element_seed(ErasedDeserializationContext::new(*deserializer, &self.context));
                            match res {
                                Err(e) => return Err(field_parse_error(column_name, e)),
                                Ok(None) => return Err(invalid_length_error(_cols, $num_cols)),
                                Ok(Some(_)) => (),
                            };
                        }

                        while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                        // A positional input gives each column of its source, as
                        // a read by name does, so a NULL gets the same value.
                        if self.context.positional_fields {
                            $($(self.$field_name = self.$field_name.take().map($postprocess);)*)*
                        }

                        // An absent column gets the value that the by-name path gives it.
                        if let Some(absent) = absent {
                            let mut _idx = 0usize;
                            $(
                                if absent[_idx] {
                                    self.$field_name()?;
                                }
                                _idx += 1;
                            )*
                        }

                        Ok($table {
                            $($field_name: self.$field_name.unwrap(),
                            )*
                        })

                    }
                }

                let mut visitor = RecordVisitor::new(context, __feldera_metadata);

                // XXX: At least with `serde_json::Deserializer`, `deserialize_struct` tells the
                // deserializer to work with either map or sequence representation
                // (while `deserialize_map` only deserializes from a map).  It seems that
                // `serde_json` ignores the actual field names provided, so this does not
                // break the case-insensitive behavior.  Not sure if this is true for all
                // deserializers.  In the future we may want to make the choice between map
                // and sequence representations configurable, so we won't need this trick.
                //deserializer.deserialize_map(visitor)
                match (context.positional_fields, &context.absent_columns) {
                    // The record does not have all the columns of the table, for example
                    // when the compiler removed the unused columns from it. Only the
                    // field names tell which column of the input each field reads.
                    (true, Some(absent)) if absent.absent().len() != $num_cols => deserializer
                        .deserialize_struct($sql_table, [$($column_name,)*].as_slice(), visitor),
                    // The input holds the columns in declaration order; skip the field names.
                    (true, absent) => {
                        let num_present =
                            absent.as_ref().map_or($num_cols, |absent| absent.num_present());
                        deserializer.deserialize_tuple(num_present, visitor)
                    }
                    (false, _) => deserializer
                        .deserialize_struct($sql_table, [$($column_name,)*].as_slice(), visitor),
                }
            }
        }
    };
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;

    use crate::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};

    #[derive(Debug, Eq, PartialEq)]
    struct TUPLE0;
    deserialize_table_record!(TUPLE0["EmptyTable", (), 0] {});

    static DEFAULT_CONFIG: LazyLock<SqlSerdeConfig> = LazyLock::new(SqlSerdeConfig::default);

    fn deserialize_with_default_context<'de, T>(json: &'de str) -> Result<T, serde_json::Error>
    where
        T: DeserializeWithContext<'de, SqlSerdeConfig, ()>,
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
    (fIeLd1, "fIeLd1", true, bool, |_| None),
    (field2, "field2", true, String, |_| None),
    (field3, "field3", false, Option<u8>, |_| Some(None))
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

    /// With `positional_fields`, a record is read as a sequence and never as a map.
    #[test]
    fn positional_fields() {
        let config = SqlSerdeConfig::default().with_positional_fields(true);
        let deserialize = |json: &str| {
            CaseSensitive::deserialize_with_context(
                &mut serde_json::Deserializer::from_str(json),
                &config,
            )
            .map_err(|e| e.to_string())
        };
        assert_eq!(
            deserialize(r#"[true, "foo", 100]"#),
            Ok(CaseSensitive {
                fIeLd1: true,
                field2: "foo".to_string(),
                field3: Some(100)
            })
        );
        assert_eq!(
            deserialize(r#"{"fIeLd1": true, "field2": "foo"}"#),
            Err(
                "invalid type: map, expected a record of type CaseSensitive at line 1 column 0"
                    .to_string()
            )
        );
    }

    #[derive(Debug, Eq, PartialEq)]
    struct Inner {
        x: u8,
        y: Option<u8>,
    }

    deserialize_table_record!(Inner["Inner", 2] {
    (x, "x", false, u8, |_| None),
    (y, "y", false, Option<u8>, |_| Some(None))
    });

    #[derive(Debug, Eq, PartialEq)]
    struct Outer {
        a: u8,
        b: Option<u8>,
        c: u8,
        d: Inner,
        e: u8,
    }

    deserialize_table_record!(Outer["Outer", 5] {
    (a, "a", false, u8, |_| None),
    (b, "b", false, Option<u8>, |_| Some(None)),
    (c, "c", false, u8, |_| Some(7)),
    (d, "d", false, Inner, |_| None),
    (e, "e", false, u8, |_| None)
    });

    /// With `absent_columns`, the sequence holds only the other columns. An
    /// absent column gets its `NULL` or its default, or is an error. A nested
    /// record holds all its fields.
    #[test]
    fn absent_columns() {
        let deserialize = |absent: Vec<bool>, json: &str| {
            let config = SqlSerdeConfig::default().with_absent_columns(absent);
            Outer::deserialize_with_context(&mut serde_json::Deserializer::from_str(json), &config)
                .map_err(|e| e.to_string())
        };
        assert_eq!(
            deserialize(vec![false, true, true, false, false], "[1, [2, 3], 4]"),
            Ok(Outer {
                a: 1,
                b: None,
                c: 7,
                d: Inner { x: 2, y: Some(3) },
                e: 4,
            })
        );
        assert_eq!(
            deserialize(vec![false; 5], "[1, 5, 6, [2, 3], 4]"),
            Ok(Outer {
                a: 1,
                b: Some(5),
                c: 6,
                d: Inner { x: 2, y: Some(3) },
                e: 4,
            })
        );
        // `e` has no `NULL` and no default.
        assert_eq!(
            deserialize(vec![false, false, false, false, true], "[1, 5, 6, [2, 3]]"),
            Err("missing field `e` at line 1 column 17".to_string())
        );
        // The flags are for another record type.
        assert!(deserialize(vec![false, true], "[1]").is_err());
    }

    #[derive(Debug, Eq, PartialEq)]
    struct Trimmed {
        c: u32,
        e: Option<u32>,
    }

    deserialize_table_record!(Trimmed["Trimmed", 2] {
        (c, "c", false, u32, |_| None),
        (e, "e", false, Option<u32>, |_| Some(None))
    });

    /// A record without some columns of the table, such as the unused columns
    /// that the compiler can remove, reads a positional input by name.
    #[test]
    fn record_without_all_columns() {
        let deserialize = |absent: Vec<bool>, json: &str| {
            let config = SqlSerdeConfig::default().with_absent_columns(absent);
            Trimmed::deserialize_with_context(
                &mut serde_json::Deserializer::from_str(json),
                &config,
            )
            .map_err(|e| e.to_string())
        };
        assert_eq!(
            deserialize(
                vec![false; 5],
                r#"{"a": 1, "b": 5, "c": 6, "d": [2, 3], "e": 4}"#
            ),
            Ok(Trimmed { c: 6, e: Some(4) })
        );
        assert_eq!(
            deserialize(vec![false, true, false, true, true], r#"{"a": 1, "c": 6}"#),
            Ok(Trimmed { c: 6, e: None })
        );
        // A sequence does not tell which columns it holds.
        assert!(deserialize(vec![false, true, true, true, false], "[6, 4]").is_err());
    }

    #[derive(Debug, Eq, PartialEq)]
    #[allow(non_snake_case)]
    struct SameNames {
        A: Option<u32>,
        a: Option<u32>,
        b: Option<u32>,
    }

    deserialize_table_record!(SameNames["SameNames", 3] {
        (A, "A", true, Option<u32>, |_| Some(None)),
        (a, "a", false, Option<u32>, |_| Some(None)),
        (b, "b", false, Option<u32>, |_| Some(None))
    });

    /// With fields that differ only in case, the field with the spelling of
    /// the column gets its value, wherever the column is in the input.
    #[test]
    fn names_that_differ_only_in_case() {
        let deserialize = |json| deserialize_with_default_context::<SameNames>(json).unwrap();
        let upper = SameNames {
            A: Some(1),
            a: None,
            b: Some(0),
        };
        assert_eq!(deserialize(r#"{"A": 1, "b": 0}"#), upper);
        assert_eq!(deserialize(r#"{"b": 0, "A": 1}"#), upper);
        let lower = SameNames {
            A: None,
            a: Some(1),
            b: Some(0),
        };
        assert_eq!(deserialize(r#"{"a": 1, "b": 0}"#), lower);
        assert_eq!(deserialize(r#"{"b": 0, "a": 1}"#), lower);
    }

    #[derive(Debug, Eq, PartialEq)]
    struct Update {
        id: u32,
        s: Option<Option<u32>>,
    }

    deserialize_table_record!(Update["Update", 2] {
        (id, "id", false, u32, |_| None),
        (s, "s", false, Option<Option<u32>>, |_| Some(None), |x| if x.is_none() { Some(None) } else { x })
    });

    /// A positional read gives a NULL of the input the same value as a read by
    /// name: here "set to NULL", and not "no change".
    #[test]
    fn positional_fields_postprocess() {
        let by_name = SqlSerdeConfig::default();
        let positional = SqlSerdeConfig::default().with_positional_fields(true);
        let absent = SqlSerdeConfig::default().with_absent_columns(vec![false, true]);
        let deserialize = |config: &SqlSerdeConfig, json: &str| {
            Update::deserialize_with_context(&mut serde_json::Deserializer::from_str(json), config)
                .unwrap()
        };
        let set_null = Update {
            id: 1,
            s: Some(None),
        };
        assert_eq!(deserialize(&by_name, r#"{"id": 1, "s": null}"#), set_null);
        assert_eq!(deserialize(&positional, "[1, null]"), set_null);
        let no_change = Update { id: 1, s: None };
        assert_eq!(deserialize(&by_name, r#"{"id": 1}"#), no_change);
        assert_eq!(deserialize(&absent, "[1]"), no_change);
    }

    /// A type-erased deserializer is passed on as it is, not erased again.
    #[test]
    fn as_erased() {
        let mut json = serde_json::Deserializer::from_str("1");
        assert!(super::as_erased(&mut &mut json).is_none());

        let mut erased = <dyn erased_serde::Deserializer>::erase(&mut json);
        let mut reference: &mut dyn erased_serde::Deserializer = &mut erased;
        let address = std::ptr::from_mut(reference).cast::<()>();
        let same = super::as_erased(&mut reference).unwrap();
        assert_eq!(std::ptr::from_mut(same).cast::<()>(), address);
        assert_eq!(erased_serde::deserialize::<u32>(same).unwrap(), 1);
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

    deserialize_table_record!(UnicodeStruct["UnicodeStruct", (), 3] {
    (f1, "αΒβΓγδε", true, bool, |_| None),
    (f2, "українська", false, String, |_| None),
    (f3, "unicode⌛👏", false, Option<u8>, |_| Some(None))
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

    /// Record whose `origin` column defaults to a metadata attribute, the way
    /// the SQL compiler compiles `DEFAULT CONNECTOR_METADATA()['origin']`.
    #[derive(Debug, Eq, PartialEq)]
    struct RecordWithMetadata {
        id: u8,
        origin: Option<String>,
    }

    deserialize_table_record!(RecordWithMetadata["RecordWithMetadata", String, 2] {
        (id, "id", false, u8, |_| None),
        (origin, "origin", false, Option<String>, |metadata: &Option<String>| Some(metadata.clone()))
    });

    /// A batch of records deserialized as a `Vec` must pass the batch metadata
    /// to every record in it.  Columnar formats, e.g., Arrow, deserialize a
    /// whole batch at a time, so dropping the metadata here leaves metadata
    /// columns NULL for all of them.
    #[test]
    fn vec_passes_metadata_to_each_record() {
        let metadata = Some("kafka".to_string());
        let records = Vec::<RecordWithMetadata>::deserialize_with_context_aux(
            &mut serde_json::Deserializer::from_str(r#"[{"id": 1}, {"id": 2, "origin": "file"}]"#),
            &DEFAULT_CONFIG,
            &metadata,
        )
        .unwrap();

        assert_eq!(
            records,
            vec![
                RecordWithMetadata {
                    id: 1,
                    origin: Some("kafka".to_string())
                },
                // A value in the record takes precedence over metadata.
                RecordWithMetadata {
                    id: 2,
                    origin: Some("file".to_string())
                },
            ]
        );
    }
}
