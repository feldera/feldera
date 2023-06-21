use crate::ir::{
    pretty::{DocAllocator, DocBuilder, Pretty},
    ColumnType, RowLayoutCache,
};
use chrono::{NaiveDate, NaiveDateTime};
use dbsp::algebra::Decimal;
use schemars::{
    gen::SchemaGenerator,
    schema::{InstanceType, Schema, SchemaObject},
    JsonSchema,
};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{cmp::Ordering, fmt, mem};

/// A constant value
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub enum Constant {
    Unit,
    U8(u8),
    I8(i8),
    U16(u16),
    I16(i16),
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    Usize(usize),
    Isize(isize),
    F32(f32),
    F64(f64),
    Bool(bool),
    String(String),
    Date(NaiveDate),
    #[serde(deserialize_with = "deserialize_timestamp")]
    Timestamp(NaiveDateTime),
    #[schemars(schema_with = "decimal_schema")]
    Decimal(Decimal),
}

impl Constant {
    /// Returns `true` if the constant is an integer (signed or unsigned)
    #[must_use]
    pub const fn is_int(&self) -> bool {
        self.column_type().is_int()
    }

    /// Returns `true` if the constant is an unsigned integer
    #[must_use]
    pub const fn is_unsigned_int(&self) -> bool {
        self.column_type().is_unsigned_int()
    }

    /// Returns `true` if the constant is a signed integer
    #[must_use]
    pub const fn is_signed_int(&self) -> bool {
        self.column_type().is_signed_int()
    }

    /// Returns `true` if the constant is a floating point value ([`F32`] or
    /// [`F64`])
    ///
    /// [`F32`]: Constant::F32
    /// [`F64`]: Constant::F64
    #[must_use]
    pub const fn is_float(&self) -> bool {
        self.column_type().is_float()
    }

    /// Returns `true` if the constant is [`String`].
    ///
    /// [`String`]: Constant::String
    #[must_use]
    pub const fn is_string(&self) -> bool {
        self.column_type().is_string()
    }

    /// Returns `true` if the constant is [`Bool`].
    ///
    /// [`Bool`]: Constant::Bool
    #[must_use]
    pub const fn is_bool(&self) -> bool {
        self.column_type().is_bool()
    }

    /// Returns `true` if the constant is [`Unit`].
    ///
    /// [`Unit`]: Constant::Unit
    #[must_use]
    pub const fn is_unit(&self) -> bool {
        self.column_type().is_unit()
    }

    /// Returns `true` if the constant is a [`Date`].
    ///
    /// [`Date`]: Constant::Date
    #[must_use]
    pub const fn is_date(&self) -> bool {
        matches!(self, Self::Date(..))
    }

    /// Returns `true` if the constant is a [`Timestamp`].
    ///
    /// [`Timestamp`]: Constant::Timestamp
    #[must_use]
    pub const fn is_timestamp(&self) -> bool {
        matches!(self, Self::Timestamp(..))
    }

    /// Returns `true` if the constant is a [`Decimal`].
    ///
    /// [`Decimal`]: Constant::Decimal
    #[must_use]
    pub const fn is_decimal(&self) -> bool {
        matches!(self, Self::Decimal(..))
    }

    /// Returns the [`ColumnType`] of the current constant
    #[must_use]
    pub const fn column_type(&self) -> ColumnType {
        match self {
            Self::Unit => ColumnType::Unit,
            Self::U8(_) => ColumnType::U8,
            Self::I8(_) => ColumnType::I8,
            Self::U16(_) => ColumnType::U16,
            Self::I16(_) => ColumnType::I16,
            Self::U32(_) => ColumnType::U32,
            Self::I32(_) => ColumnType::I32,
            Self::U64(_) => ColumnType::U64,
            Self::I64(_) => ColumnType::I64,
            Self::Usize(_) => ColumnType::Usize,
            Self::Isize(_) => ColumnType::Isize,
            Self::F32(_) => ColumnType::F32,
            Self::F64(_) => ColumnType::F64,
            Self::Bool(_) => ColumnType::Bool,
            Self::String(_) => ColumnType::String,
            Self::Date(_) => ColumnType::Date,
            Self::Timestamp(_) => ColumnType::Timestamp,
            Self::Decimal(_) => ColumnType::Decimal,
        }
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Constant
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        if self.is_unit() {
            return alloc.text("unit");
        }

        let value = match self {
            Constant::U8(u8) => format!("{u8}"),
            Constant::I8(i8) => format!("{i8}"),
            Constant::U16(u16) => format!("{u16}"),
            Constant::I16(i16) => format!("{i16}"),
            Constant::U32(u32) => format!("{u32}"),
            Constant::I32(i32) => format!("{i32}"),
            Constant::U64(u64) => format!("{u64}"),
            Constant::I64(i64) => format!("{i64}"),
            Constant::Usize(usize) => format!("{usize}"),
            Constant::Isize(isize) => format!("{isize}"),
            Constant::F32(f32) => format!("{f32}"),
            Constant::F64(f64) => format!("{f64}"),
            Constant::Bool(bool) => format!("{bool}"),
            Constant::String(string) => format!("{string:?}"),
            // Formats as `2001-07-08`
            Constant::Date(date) => format!("{date:?}"),
            // Formats as `2001-07-08T00:34:60.026490+09:30`
            Constant::Timestamp(timestamp) => format!("{timestamp:?}"),
            Constant::Decimal(decimal) => format!("{decimal}"),
            Constant::Unit => unreachable!("already handled unit"),
        };

        self.column_type()
            .pretty(alloc, cache)
            .append(alloc.space())
            .append(alloc.text(value))
    }
}

impl PartialEq for Constant {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Unit, Self::Unit) => true,
            (Self::U8(lhs), Self::U8(rhs)) => lhs == rhs,
            (Self::I8(lhs), Self::I8(rhs)) => lhs == rhs,
            (Self::U16(lhs), Self::U16(rhs)) => lhs == rhs,
            (Self::I16(lhs), Self::I16(rhs)) => lhs == rhs,
            (Self::U32(lhs), Self::U32(rhs)) => lhs == rhs,
            (Self::I32(lhs), Self::I32(rhs)) => lhs == rhs,
            (Self::U64(lhs), Self::U64(rhs)) => lhs == rhs,
            (Self::I64(lhs), Self::I64(rhs)) => lhs == rhs,
            (Self::Usize(lhs), Self::Usize(rhs)) => lhs == rhs,
            (Self::Isize(lhs), Self::Isize(rhs)) => lhs == rhs,
            (Self::F32(lhs), Self::F32(rhs)) => {
                if lhs.is_nan() {
                    rhs.is_nan()
                } else {
                    lhs == rhs
                }
            }
            (Self::F64(lhs), Self::F64(rhs)) => {
                if lhs.is_nan() {
                    rhs.is_nan()
                } else {
                    lhs == rhs
                }
            }
            (Self::Bool(lhs), Self::Bool(rhs)) => lhs == rhs,
            (Self::String(lhs), Self::String(rhs)) => lhs == rhs,
            (Self::Date(lhs), Self::Date(rhs)) => lhs == rhs,
            (Self::Timestamp(lhs), Self::Timestamp(rhs)) => lhs == rhs,
            (Self::Decimal(lhs), Self::Decimal(rhs)) => lhs == rhs,

            _ => {
                debug_assert_ne!(mem::discriminant(self), mem::discriminant(other));
                false
            }
        }
    }
}

impl Eq for Constant {}

impl PartialOrd for Constant {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(match (self, other) {
            (Self::Unit, Self::Unit) => Ordering::Equal,
            (Self::U8(lhs), Self::U8(rhs)) => lhs.cmp(rhs),
            (Self::I8(lhs), Self::I8(rhs)) => lhs.cmp(rhs),
            (Self::U16(lhs), Self::U16(rhs)) => lhs.cmp(rhs),
            (Self::I16(lhs), Self::I16(rhs)) => lhs.cmp(rhs),
            (Self::U32(lhs), Self::U32(rhs)) => lhs.cmp(rhs),
            (Self::I32(lhs), Self::I32(rhs)) => lhs.cmp(rhs),
            (Self::U64(lhs), Self::U64(rhs)) => lhs.cmp(rhs),
            (Self::I64(lhs), Self::I64(rhs)) => lhs.cmp(rhs),
            (Self::Usize(lhs), Self::Usize(rhs)) => lhs.cmp(rhs),
            (Self::Isize(lhs), Self::Isize(rhs)) => lhs.cmp(rhs),
            (Self::F32(lhs), Self::F32(rhs)) => match lhs.partial_cmp(rhs) {
                Some(ordering) => ordering,
                None => {
                    if lhs.is_nan() {
                        if rhs.is_nan() {
                            Ordering::Equal
                        } else {
                            Ordering::Greater
                        }
                    } else {
                        Ordering::Less
                    }
                }
            },
            (Self::F64(lhs), Self::F64(rhs)) => match lhs.partial_cmp(rhs) {
                Some(ordering) => ordering,
                None => {
                    if lhs.is_nan() {
                        if rhs.is_nan() {
                            Ordering::Equal
                        } else {
                            Ordering::Greater
                        }
                    } else {
                        Ordering::Less
                    }
                }
            },
            (Self::Bool(lhs), Self::Bool(rhs)) => lhs.cmp(rhs),
            (Self::String(lhs), Self::String(rhs)) => lhs.cmp(rhs),
            (Self::Date(lhs), Self::Date(rhs)) => lhs.cmp(rhs),
            (Self::Timestamp(lhs), Self::Timestamp(rhs)) => lhs.cmp(rhs),
            (Self::Decimal(lhs), Self::Decimal(rhs)) => lhs.cmp(rhs),

            _ => {
                debug_assert_ne!(mem::discriminant(self), mem::discriminant(other));
                return None;
            }
        })
    }
}

impl Ord for Constant {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Unit, Self::Unit) => Ordering::Equal,
            (Self::U8(lhs), Self::U8(rhs)) => lhs.cmp(rhs),
            (Self::I8(lhs), Self::I8(rhs)) => lhs.cmp(rhs),
            (Self::U16(lhs), Self::U16(rhs)) => lhs.cmp(rhs),
            (Self::I16(lhs), Self::I16(rhs)) => lhs.cmp(rhs),
            (Self::U32(lhs), Self::U32(rhs)) => lhs.cmp(rhs),
            (Self::I32(lhs), Self::I32(rhs)) => lhs.cmp(rhs),
            (Self::U64(lhs), Self::U64(rhs)) => lhs.cmp(rhs),
            (Self::I64(lhs), Self::I64(rhs)) => lhs.cmp(rhs),
            (Self::Usize(lhs), Self::Usize(rhs)) => lhs.cmp(rhs),
            (Self::Isize(lhs), Self::Isize(rhs)) => lhs.cmp(rhs),
            (Self::F32(lhs), Self::F32(rhs)) => match lhs.partial_cmp(rhs) {
                Some(ordering) => ordering,
                None => {
                    if lhs.is_nan() {
                        if rhs.is_nan() {
                            Ordering::Equal
                        } else {
                            Ordering::Greater
                        }
                    } else {
                        Ordering::Less
                    }
                }
            },
            (Self::F64(lhs), Self::F64(rhs)) => match lhs.partial_cmp(rhs) {
                Some(ordering) => ordering,
                None => {
                    if lhs.is_nan() {
                        if rhs.is_nan() {
                            Ordering::Equal
                        } else {
                            Ordering::Greater
                        }
                    } else {
                        Ordering::Less
                    }
                }
            },
            (Self::Bool(lhs), Self::Bool(rhs)) => lhs.cmp(rhs),
            (Self::String(lhs), Self::String(rhs)) => lhs.cmp(rhs),
            (Self::Date(lhs), Self::Date(rhs)) => lhs.cmp(rhs),
            (Self::Timestamp(lhs), Self::Timestamp(rhs)) => lhs.cmp(rhs),
            (Self::Decimal(lhs), Self::Decimal(rhs)) => lhs.cmp(rhs),

            _ => {
                debug_assert_ne!(mem::discriminant(self), mem::discriminant(other));
                panic!();
            }
        }
    }
}

fn deserialize_timestamp<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(TimestampVisitor)
}

struct TimestampVisitor;

impl<'de> de::Visitor<'de> for TimestampVisitor {
    type Value = NaiveDateTime;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a formatted date and time string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        NaiveDateTime::parse_from_str(value, "%+").map_err(E::custom)
    }
}

fn decimal_schema(_gen: &mut SchemaGenerator) -> Schema {
    Schema::Object(SchemaObject {
        instance_type: Some(InstanceType::Number.into()),
        ..Default::default()
    })
}
