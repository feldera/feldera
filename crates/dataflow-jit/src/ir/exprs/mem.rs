use crate::ir::{
    pretty::{DocAllocator, DocBuilder, Pretty},
    ColumnType, ExprId, LayoutId, RValue, RowLayoutCache,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Load a value from the given column of the given row
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Load {
    /// The row to load from
    source: ExprId,
    /// The layout of the target row
    source_layout: LayoutId,
    /// The index of the column to load from
    column: usize,
    /// The type of the value being loaded (the type of `source_layout[column]`)
    column_type: ColumnType,
}

impl Load {
    pub fn new(
        source: ExprId,
        source_layout: LayoutId,
        column: usize,
        column_type: ColumnType,
    ) -> Self {
        Self {
            source,
            source_layout,
            column,
            column_type,
        }
    }

    pub const fn source(&self) -> ExprId {
        self.source
    }

    pub fn source_mut(&mut self) -> &mut ExprId {
        &mut self.source
    }

    pub const fn source_layout(&self) -> LayoutId {
        self.source_layout
    }

    pub fn source_layout_mut(&mut self) -> &mut LayoutId {
        &mut self.source_layout
    }

    pub const fn column(&self) -> usize {
        self.column
    }

    pub const fn column_type(&self) -> ColumnType {
        self.column_type
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Load
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("load")
            .append(alloc.space())
            .append(self.source_layout.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.source.pretty(alloc, cache))
            .append(alloc.text(format!("[{}]", self.column)))
            // Comment for the loaded value's type
            .append(alloc.space())
            .append(alloc.text(";"))
            .append(alloc.space())
            .append(
                if let Some(ty) = cache.get(self.source_layout).try_column_type(self.column) {
                    ty.pretty(alloc, cache)
                } else {
                    alloc.text(format!("<column {} doesn't exist>", self.column))
                },
            )
    }
}

/// Store a value to the given column of the given row
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Store {
    /// The row to store into
    target: ExprId,
    /// The layout of the target row
    target_layout: LayoutId,
    /// The index of the column to store to
    column: usize,
    /// The value being stored
    value: RValue,
    /// The type of the value being stored
    value_type: ColumnType,
}

impl Store {
    pub fn new(
        target: ExprId,
        target_layout: LayoutId,
        column: usize,
        value: RValue,
        value_type: ColumnType,
    ) -> Self {
        Self {
            target,
            target_layout,
            column,
            value,
            value_type,
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

    pub const fn value(&self) -> &RValue {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut RValue {
        &mut self.value
    }

    pub const fn value_type(&self) -> ColumnType {
        self.value_type
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Store
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("store")
            .append(alloc.space())
            .append(self.target_layout.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.target.pretty(alloc, cache))
            .append(alloc.text(format!("[{}]", self.column)))
            .append(alloc.text(","))
            .append(alloc.space())
            .append(self.value_type.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.value.pretty(alloc, cache))
    }
}

/// Copies the contents of the source row into the destination row
///
/// Both the `src` and `dest` rows must have the same layout.
///
/// Semantically equivalent to [`Load`]-ing each column within `src`, calling
/// [`trait@struct@Copy`] on the loaded value and then [`Store`]-ing the copied value
/// to `dest` (along with any required [`crate::ir::exprs::IsNull`]/
/// [`crate::ir::exprs::SetNull`]
/// juggling that has to be done due to nullable columns)
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct CopyRowTo {
    src: ExprId,
    dest: ExprId,
    layout: LayoutId,
}

impl CopyRowTo {
    /// Creates a new `CopyRowTo` expression, both `src` and `dest` must be rows
    /// of `layout`'s layout
    pub fn new(src: ExprId, dest: ExprId, layout: LayoutId) -> Self {
        Self { src, dest, layout }
    }

    /// Returns the source row
    pub const fn src(&self) -> ExprId {
        self.src
    }

    pub fn src_mut(&mut self) -> &mut ExprId {
        &mut self.src
    }

    /// Returns the destination row
    pub const fn dest(&self) -> ExprId {
        self.dest
    }

    pub fn dest_mut(&mut self) -> &mut ExprId {
        &mut self.dest
    }

    /// Returns the layout of the `src` and `dest` rows
    pub const fn layout(&self) -> LayoutId {
        self.layout
    }

    pub fn layout_mut(&mut self) -> &mut LayoutId {
        &mut self.layout
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &CopyRowTo
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("copy_row_to")
            .append(alloc.space())
            .append(self.layout.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.dest.pretty(alloc, cache))
            .append(alloc.space())
            .append(alloc.text("from"))
            .append(alloc.space())
            .append(self.src.pretty(alloc, cache))
    }
}
