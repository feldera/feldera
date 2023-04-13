use crate::ir::{ColumnType, ExprId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A binary operation, see [`BinaryOpKind`] for the possible kinds of
/// operations being performed
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct BinaryOp {
    /// The left hand value
    lhs: ExprId,
    /// The right hand value
    rhs: ExprId,
    /// The type of `lhs` and `rhs`
    // TODO: Should `lhs` and `rhs` be able to have different types?
    operand_ty: ColumnType,
    /// The kind of binary op being performed
    kind: BinaryOpKind,
}

impl BinaryOp {
    pub fn new(lhs: ExprId, rhs: ExprId, operand_ty: ColumnType, kind: BinaryOpKind) -> Self {
        Self {
            lhs,
            rhs,
            operand_ty,
            kind,
        }
    }

    pub const fn lhs(&self) -> ExprId {
        self.lhs
    }

    pub fn lhs_mut(&mut self) -> &mut ExprId {
        &mut self.lhs
    }

    pub const fn rhs(&self) -> ExprId {
        self.rhs
    }

    pub fn rhs_mut(&mut self) -> &mut ExprId {
        &mut self.rhs
    }

    pub const fn operand_ty(&self) -> ColumnType {
        self.operand_ty
    }

    pub const fn kind(&self) -> BinaryOpKind {
        self.kind
    }
}

/// The kind of binary operation being performed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub enum BinaryOpKind {
    /// Addition
    Add,
    /// Subtraction
    Sub,
    /// Multiplication
    Mul,
    /// Division
    Div,
    /// Floored division
    DivFloor,
    /// Remainder
    Rem,
    /// Modulus (Euclidean remainder)
    Mod,
    /// Floored modulus
    ModFloor,
    /// Equality (`==`)
    Eq,
    /// Inequality (`!=`)
    Neq,
    /// Less than (`<`)
    LessThan,
    /// Greater than (`>`)
    GreaterThan,
    /// Less than or equal to (`<=`)
    LessThanOrEqual,
    /// Greater than or equal to (`>=`)
    GreaterThanOrEqual,
    /// Binary and for integers, logical and for booleans
    And,
    /// Binary or for integers, logical or for booleans
    Or,
    /// Binary xor for integers, logical xor for booleans
    Xor,
    /// Minimum
    Min,
    /// Maximum
    Max,
    // TODO: shr, shl, rotl, rotr, pow
}
