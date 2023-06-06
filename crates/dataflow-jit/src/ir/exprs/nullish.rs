use crate::ir::{
    exprs::ExprId,
    pretty::{DocAllocator, DocBuilder, Pretty},
    LayoutId, RValue, RowLayoutCache,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Checks if the given column of the target row is null
///
/// Requires that `column` of the target layout is nullable.
/// Returns `true` if the `column`-th row of `target` is currently null and
/// `false` if it's not null
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct IsNull {
    /// The row we're checking
    target: ExprId,
    /// The layout of the target row
    target_layout: LayoutId,
    /// The column of `target` to fetch the null-ness of
    column: usize,
}

impl IsNull {
    /// Create a new `IsNull` expression
    ///
    /// - `target` must be a readable row type
    /// - `column` must be a valid column index into `target`
    pub fn new(target: ExprId, target_layout: LayoutId, column: usize) -> Self {
        Self {
            target,
            target_layout,
            column,
        }
    }

    pub const fn target(&self) -> ExprId {
        self.target
    }

    pub fn target_mut(&mut self) -> &mut ExprId {
        &mut self.target
    }

    pub const fn target_layout(&self) -> LayoutId {
        self.target_layout
    }

    pub fn target_layout_mut(&mut self) -> &mut LayoutId {
        &mut self.target_layout
    }

    pub const fn column(&self) -> usize {
        self.column
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &IsNull
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("is_null")
            .append(alloc.space())
            .append(self.target_layout.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.target.pretty(alloc, cache))
            .append(alloc.text(format!("[{}]", self.column)))
    }
}

/// Sets the nullness of the `column`-th row of the target row
///
/// Requires that `column` of the target layout is nullable and that `target` is
/// writeable. `is_null` determines
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct SetNull {
    /// The row that's being manipulated
    target: ExprId,
    /// The layout of the target row
    target_layout: LayoutId,
    /// The column of `target` that we're setting the null flag of
    column: usize,
    /// A boolean constant to be stored in `target.column`'s null flag
    is_null: RValue,
}

impl SetNull {
    /// Create a new `SetNull` expression
    ///
    /// - `target` must be a writeable row type
    /// - `column` must be a valid column index into `target`
    /// - `is_null` must be a boolean value
    pub fn new(target: ExprId, target_layout: LayoutId, column: usize, is_null: RValue) -> Self {
        Self {
            target,
            target_layout,
            column,
            is_null,
        }
    }

    /// Gets the target of the `SetNull`
    pub const fn target(&self) -> ExprId {
        self.target
    }

    pub fn target_mut(&mut self) -> &mut ExprId {
        &mut self.target
    }

    pub const fn target_layout(&self) -> LayoutId {
        self.target_layout
    }

    pub fn target_layout_mut(&mut self) -> &mut LayoutId {
        &mut self.target_layout
    }

    pub const fn column(&self) -> usize {
        self.column
    }

    pub const fn is_null(&self) -> &RValue {
        &self.is_null
    }

    pub fn is_null_mut(&mut self) -> &mut RValue {
        &mut self.is_null
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &SetNull
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("set_null")
            .append(alloc.space())
            .append(self.target_layout.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.target.pretty(alloc, cache))
            .append(alloc.text(format!("[{}]", self.column)))
            .append(alloc.text(","))
            .append(alloc.space())
            .append(self.is_null.pretty(alloc, cache))
    }
}
