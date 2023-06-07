use crate::ir::{
    exprs::{ExprId, RowOrScalar},
    pretty::{DocAllocator, DocBuilder, Pretty},
    ColumnType, LayoutId, RowLayoutCache,
};
use derive_more::From;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Drops the given scalar or row value
// TODO: Should this be able to deallocate a row's backing memory?
#[derive(Debug, Clone, From, PartialEq, Deserialize, Serialize, JsonSchema)]
#[allow(dead_code)]
pub struct Drop {
    value: ExprId,
    ty: RowOrScalar,
}

impl Drop {
    /// Create a new drop
    pub fn new(value: ExprId, ty: RowOrScalar) -> Self {
        Self { value, ty }
    }

    pub const fn value(&self) -> ExprId {
        self.value
    }

    pub fn value_mut(&mut self) -> &mut ExprId {
        &mut self.value
    }

    pub const fn ty(&self) -> RowOrScalar {
        self.ty
    }

    pub fn ty_mut(&mut self) -> &mut RowOrScalar {
        &mut self.ty
    }

    pub const fn is_scalar(&self) -> bool {
        self.ty.is_scalar()
    }

    pub const fn is_row(&self) -> bool {
        self.ty.is_row()
    }

    pub const fn as_scalar(&self) -> Option<ColumnType> {
        self.ty.as_scalar()
    }

    pub const fn as_row(&self) -> Option<LayoutId> {
        self.ty.as_row()
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Drop
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("drop")
            .append(alloc.space())
            .append(self.ty.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.value.pretty(alloc, cache))
    }
}
