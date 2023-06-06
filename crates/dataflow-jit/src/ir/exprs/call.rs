use crate::ir::{
    pretty::{DocAllocator, DocBuilder, Pretty},
    ColumnType, ExprId, LayoutId, RowLayoutCache,
};
use derive_more::Unwrap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The type of a function argument
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Unwrap, JsonSchema)]
pub enum ArgType {
    /// A row type
    Row(LayoutId),
    /// A scalar value's type
    Scalar(ColumnType),
}

impl ArgType {
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

    pub fn needs_drop(self, cache: &RowLayoutCache) -> bool {
        match self {
            Self::Row(layout) => cache.get(layout).needs_drop(),
            Self::Scalar(scalar) => scalar.needs_drop(),
        }
    }
}

impl Default for ArgType {
    fn default() -> Self {
        Self::Scalar(ColumnType::Unit)
    }
}

impl<'a, D, A> Pretty<'a, D, A> for ArgType
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

/// The call instruction, calls a function
///
/// ### Functions
///
/// - `@dbsp.row.vec.push(vec: { ptr, ptr }, row: { .. })`
/// - `@dbsp.str.truncate(str, usize)`
/// - `@dbsp.str.truncate_clone(str, usize) -> str`
/// - `@dbsp.str.clear(str)`
/// - `@dbsp.str.concat(str, str)`
/// - `@dbsp.str.concat_clone(str, str) -> str`
/// - `@dbsp.timestamp.epoch(timestamp) -> i64`
/// - `@dbsp.date.second(date) -> i32`
/// - `@dbsp.date.minute(date) -> i32`
/// - `@dbsp.date.millisecond(date) -> i32`
/// - `@dbsp.date.microsecond(date) -> i32`
/// - `@dbsp.date.year(date) -> i32`
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct Call {
    /// The name of the function being called
    function: String,
    /// The arguments passed to the function
    args: Vec<ExprId>,
    /// The types of each argument
    arg_types: Vec<ArgType>,
    /// The function's return type
    // FIXME: Could return a row type as well
    ret_ty: ColumnType,
}

impl Call {
    pub fn new(
        function: String,
        args: Vec<ExprId>,
        arg_types: Vec<ArgType>,
        ret_ty: ColumnType,
    ) -> Self {
        Self {
            function,
            args,
            arg_types,
            ret_ty,
        }
    }

    pub fn function(&self) -> &str {
        &self.function
    }

    pub fn args(&self) -> &[ExprId] {
        &self.args
    }

    pub fn args_mut(&mut self) -> &mut Vec<ExprId> {
        &mut self.args
    }

    pub fn arg_types(&self) -> &[ArgType] {
        &self.arg_types
    }

    pub fn arg_types_mut(&mut self) -> &mut Vec<ArgType> {
        &mut self.arg_types
    }

    pub const fn ret_ty(&self) -> ColumnType {
        self.ret_ty
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Call
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized + 'a,
    DocBuilder<'a, D, A>: Clone,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("call")
            .append(alloc.space())
            .append(self.ret_ty.pretty(alloc, cache))
            .append(alloc.space())
            .append(alloc.text(self.function.clone()))
            .append(
                alloc
                    .intersperse(
                        self.arg_types.iter().zip(&self.args).map(|(ty, arg)| {
                            ty.pretty(alloc, cache)
                                .append(alloc.space())
                                .append(arg.pretty(alloc, cache))
                        }),
                        alloc.text(",").append(alloc.space()),
                    )
                    .parens(),
            )
    }
}
