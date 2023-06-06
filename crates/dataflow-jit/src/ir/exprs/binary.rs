use crate::ir::{
    pretty::{DocAllocator, DocBuilder, Pretty},
    ColumnType, ExprId, RowLayoutCache,
};
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

impl<'a, D, A> Pretty<'a, D, A> for &BinaryOp
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        self.kind
            .pretty(alloc, cache)
            .append(alloc.space())
            .append(self.operand_ty.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.lhs.pretty(alloc, cache))
            .append(alloc.text(","))
            .append(alloc.space())
            .append(self.rhs.pretty(alloc, cache))
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

impl BinaryOpKind {
    pub const fn to_str(self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Sub => "sub",
            Self::Mul => "mul",
            Self::Div => "div",
            Self::DivFloor => "div_floor",
            Self::Rem => "rem",
            Self::Mod => "mod",
            Self::ModFloor => "mod_floor",
            Self::Eq => "eq",
            Self::Neq => "neq",
            Self::LessThan => "lt",
            Self::GreaterThan => "gt",
            Self::LessThanOrEqual => "le",
            Self::GreaterThanOrEqual => "ge",
            Self::And => "and",
            Self::Or => "or",
            Self::Xor => "xor",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

impl<'a, D, A> Pretty<'a, D, A> for BinaryOpKind
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, _cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc.text(self.to_str())
    }
}
