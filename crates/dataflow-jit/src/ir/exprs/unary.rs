use crate::ir::{ColumnType, ExprId};
use serde::{Deserialize, Serialize};

/// A unary operation, see [`UnaryOpKind`] for the possible kinds of
/// operations being performed
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct UnaryOp {
    /// The input value
    value: ExprId,
    /// The type of the input value
    value_ty: ColumnType,
    /// The unary operation being performed
    kind: UnaryOpKind,
}

impl UnaryOp {
    pub fn new(value: ExprId, value_ty: ColumnType, kind: UnaryOpKind) -> Self {
        Self {
            value,
            value_ty,
            kind,
        }
    }

    pub const fn value(&self) -> ExprId {
        self.value
    }

    pub fn value_mut(&mut self) -> &mut ExprId {
        &mut self.value
    }

    pub const fn value_ty(&self) -> ColumnType {
        self.value_ty
    }

    pub const fn kind(&self) -> UnaryOpKind {
        self.kind
    }
}

/// The kind of unary operation being performed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum UnaryOpKind {
    Abs,
    Neg,
    Not,
    Ceil,
    Floor,
    Trunc,
    Sqrt,
    CountOnes,
    CountZeroes,
    LeadingOnes,
    LeadingZeroes,
    TrailingOnes,
    TrailingZeroes,
    BitReverse,
    ByteReverse,
    /// Returns the length of the passed string as a u64
    // TODO: Should we expose `usize` to users?
    StringLen,
}
