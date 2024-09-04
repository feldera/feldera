//! SqlValue is a dynamically-typed object that supports the Variant SQL type.

use core::cmp::Ordering;
use feldera_types::{deserialize_without_context, serialize_without_context};
use serde::{Deserialize, Serialize};
use size_of::*;

#[derive(
    Clone,
    Debug,
    Default,
    Deserialize,
    Eq,
    Hash,
    PartialEq,
    PartialOrd,
    Ord,
    Serialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
pub enum SqlValue {
    // Used for the NULL value with the NULL type
    #[default]
    Empty,

    I16(i16),
    I32(i32),

    OptI16(Option<i16>),
    OptI32(Option<i32>),
}

impl SizeOf for SqlValue {
    fn size_of_children(&self, _context: &mut size_of::Context) {}
}

impl PartialEq<()> for SqlValue {
    fn eq(&self, _other: &()) -> bool {
        false
    }
}

impl PartialEq<SqlValue> for () {
    fn eq(&self, _other: &SqlValue) -> bool {
        false
    }
}

impl PartialOrd<()> for SqlValue {
    fn partial_cmp(&self, _other: &()) -> Option<Ordering> {
        Some(Ordering::Greater)
    }
}

impl PartialOrd<SqlValue> for () {
    fn partial_cmp(&self, _other: &SqlValue) -> Option<Ordering> {
        Some(Ordering::Less)
    }
}

serialize_without_context!(SqlValue);
deserialize_without_context!(SqlValue);

macro_rules! make_froms {
    /*
    Example generated code:

    impl From<i8> for SqlValue {
        fn from(value: i8) -> Self {
            SqlValue::I8(value)
        }
    }

    impl From<SqlValue> for i8 {
        #[inline]
        fn from(value: SqlValue) -> Self {
            match value {
                SqlValue::I8(value) => value,
                _ => unreachable!("{:?} type is not 'i8'", value),
            }
        }
    }

    impl From<Option<i8>> for SqlValue {
        fn from(value: Option<i8>) -> Self {
            SqlValue::OptI8(value)
        }
    }

    impl From<SqlValue> for Option<i8> {
        #[inline]
        fn from(value: SqlValue) -> Self {
            match value {
                SqlValue::OptI8(value) => value,
                _ => unreachable!("{:?} type is not 'Option<i8>'", value),
            }
        }
    }
    */
    (
        $type: ty, $enum: tt
    ) => {
        ::paste::paste! {
            impl From<$type> for SqlValue {
                #[inline]
                fn from(value: $type) -> Self {
                    SqlValue::$enum(value)
                }
            }

            impl From<SqlValue> for $type {
                #[inline]
                fn from(value: SqlValue) -> Self {
                    match value {
                        SqlValue::$enum(value) => value,
                        _ => unreachable!(concat!("{:?} type is not '", stringify!($ty), "'"), value),
                    }
                }
            }

            impl From<Option<$type>> for SqlValue {
                #[inline]
                fn from(value: Option<$type>) -> Self {
                    SqlValue::[< Opt $enum >](value)
                }
            }

            impl From<SqlValue> for Option<$type> {
                #[inline]
                fn from(value: SqlValue) -> Self {
                    match value {
                        SqlValue::[< Opt $enum >](value) => value,
                        _ => unreachable!(concat!("{:?} type is not 'Option<", stringify!($ty), ">'"), value),
                    }
                }
            }
        }
    };
}

make_froms!(i16, I16);
make_froms!(i32, I32);
