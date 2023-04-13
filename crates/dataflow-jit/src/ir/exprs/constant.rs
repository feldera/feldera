use crate::ir::ColumnType;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, mem};

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
    // TODO: Date, Timestamp
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
        }
    }
}

impl PartialEq for Constant {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
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
            (Self::F32(lhs), Self::F32(rhs)) => lhs.total_cmp(rhs).is_eq(),
            (Self::F64(lhs), Self::F64(rhs)) => lhs.total_cmp(rhs).is_eq(),
            (Self::Bool(lhs), Self::Bool(rhs)) => lhs == rhs,
            (Self::String(lhs), Self::String(rhs)) => lhs == rhs,

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
            (Self::F32(lhs), Self::F32(rhs)) => lhs.total_cmp(rhs),
            (Self::F64(lhs), Self::F64(rhs)) => lhs.total_cmp(rhs),
            (Self::Bool(lhs), Self::Bool(rhs)) => lhs.cmp(rhs),
            (Self::String(lhs), Self::String(rhs)) => lhs.cmp(rhs),

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
            (Self::F32(lhs), Self::F32(rhs)) => lhs.total_cmp(rhs),
            (Self::F64(lhs), Self::F64(rhs)) => lhs.total_cmp(rhs),
            (Self::Bool(lhs), Self::Bool(rhs)) => lhs.cmp(rhs),
            (Self::String(lhs), Self::String(rhs)) => lhs.cmp(rhs),

            _ => {
                debug_assert_ne!(mem::discriminant(self), mem::discriminant(other));
                panic!();
            }
        }
    }
}
