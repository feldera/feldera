use crate::ir::{
    pretty::{DocAllocator, DocBuilder, Pretty},
    ColumnType, ExprId, RowLayoutCache,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A unary operation, see [`UnaryOpKind`] for the possible kinds of
/// operations being performed
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
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

impl<'a, D, A> Pretty<'a, D, A> for &UnaryOp
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        self.kind
            .pretty(alloc, cache)
            .append(alloc.space())
            .append(self.value_ty.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.value.pretty(alloc, cache))
    }
}

/// The kind of unary operation being performed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
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

impl UnaryOpKind {
    pub const fn to_str(self) -> &'static str {
        match self {
            Self::Abs => "abs",
            Self::Neg => "neg",
            Self::Not => "not",
            Self::Ceil => "ceil",
            Self::Floor => "floor",
            Self::Trunc => "trunc",
            Self::Sqrt => "sqrt",
            Self::CountOnes => "count_ones",
            Self::CountZeroes => "count_zeroes",
            Self::LeadingOnes => "leading_ones",
            Self::LeadingZeroes => "leading_zeroes",
            Self::TrailingOnes => "trailing_ones",
            Self::TrailingZeroes => "trailing_zeroes",
            Self::BitReverse => "bit_reverse",
            Self::ByteReverse => "byte_reverse",
            Self::StringLen => "string_len",
        }
    }
}

impl<'a, D, A> Pretty<'a, D, A> for UnaryOpKind
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, _cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc.text(self.to_str())
    }
}
