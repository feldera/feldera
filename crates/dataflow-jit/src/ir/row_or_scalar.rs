use crate::ir::{
    pretty::{DocAllocator, DocBuilder, Pretty},
    ColumnType, LayoutId, RowLayoutCache,
};
use derive_more::Unwrap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The type of a function argument
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Unwrap, JsonSchema)]
pub enum RowOrScalar {
    /// A row type
    Row(LayoutId),
    /// A scalar value's type
    Scalar(ColumnType),
}

impl RowOrScalar {
    /// Returns `true` if the arg type is a [`Row`].
    ///
    /// [`Row`]: ArgType::Row
    #[must_use]
    #[inline]
    pub const fn is_row(&self) -> bool {
        matches!(self, Self::Row(..))
    }

    /// Returns `true` if the arg type is a [`Scalar`].
    ///
    /// [`Scalar`]: ArgType::Scalar
    #[must_use]
    #[inline]
    pub const fn is_scalar(&self) -> bool {
        matches!(self, Self::Scalar(..))
    }

    #[inline]
    pub const fn as_row(self) -> Option<LayoutId> {
        if let Self::Row(layout) = self {
            Some(layout)
        } else {
            None
        }
    }

    #[inline]
    pub const fn as_scalar(self) -> Option<ColumnType> {
        if let Self::Scalar(scalar) = self {
            Some(scalar)
        } else {
            None
        }
    }

    #[track_caller]
    pub fn expect_row(self, message: &str) -> LayoutId {
        if let Self::Row(row) = self {
            row
        } else {
            panic!("called .expect_row() on a scalar type: {message}")
        }
    }

    #[track_caller]
    pub fn expect_scalar(self, message: &str) -> ColumnType {
        if let Self::Scalar(scalar) = self {
            scalar
        } else {
            panic!("called .expect_scalar() on a row type: {message}")
        }
    }

    pub fn needs_drop(self, cache: &RowLayoutCache) -> bool {
        match self {
            Self::Row(layout) => cache.get(layout).needs_drop(),
            Self::Scalar(scalar) => scalar.needs_drop(),
        }
    }
}

impl Default for RowOrScalar {
    #[inline]
    fn default() -> Self {
        Self::Scalar(ColumnType::Unit)
    }
}

impl From<ColumnType> for RowOrScalar {
    #[inline]
    fn from(column: ColumnType) -> Self {
        Self::Scalar(column)
    }
}

impl From<LayoutId> for RowOrScalar {
    #[inline]
    fn from(row: LayoutId) -> Self {
        Self::Row(row)
    }
}

impl<'a, D, A> Pretty<'a, D, A> for RowOrScalar
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        match self {
            Self::Row(row) => row.pretty(alloc, cache),
            Self::Scalar(scalar) => scalar.pretty(alloc, cache),
        }
    }
}
