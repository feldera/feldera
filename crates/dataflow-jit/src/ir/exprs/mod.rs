pub mod visit;

mod binary;
mod call;
mod constant;
mod drop;
mod mem;
mod nullish;
mod select;
mod unary;

pub use crate::ir::{ExprId, RowOrScalar};
pub use binary::{BinaryOp, BinaryOpKind};
pub use call::Call;
pub use constant::Constant;
pub use drop::Drop;
pub use mem::{CopyRowTo, Load, Store};
pub use nullish::{IsNull, SetNull};
pub use select::Select;
pub use unary::{UnaryOp, UnaryOpKind};

use crate::ir::{
    exprs::visit::MapLayouts,
    pretty::{DocAllocator, DocBuilder, Pretty},
    ColumnType, LayoutId, RowLayoutCache,
};
use derive_more::From;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// TODO: Put type info into more expressions

/// An expression within a basic block's body
#[derive(Debug, Clone, From, PartialEq, Deserialize, Serialize, JsonSchema)]
pub enum Expr {
    Call(Call),
    Cast(Cast),
    Load(Load),
    Store(Store),
    Select(Select),
    IsNull(IsNull),
    BinOp(BinaryOp),
    Copy(Copy),
    UnaryOp(UnaryOp),
    NullRow(NullRow),
    SetNull(SetNull),
    Constant(Constant),
    CopyRowTo(CopyRowTo),
    UninitRow(UninitRow),
    Uninit(Uninit),
    Nop(Nop),
    Drop(Drop),
}

impl Expr {
    pub fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId) + ?Sized,
    {
        self.apply(&mut MapLayouts::new(map));
    }

    pub fn map_layouts_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut LayoutId),
    {
        self.apply_mut(&mut MapLayouts::new(map));
    }

    pub(crate) fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.map_layouts_mut(&mut |layout| *layout = mappings[layout]);
    }

    /// Returns `true` if the expression "assigns" a value, used for pretty
    pub(crate) fn needs_assign(&self) -> bool {
        match self {
            Self::Call(call) => !call.ret_ty().is_unit(),

            Self::Cast(_)
            | Self::Load(_)
            | Self::Select(_)
            | Self::IsNull(_)
            | Self::BinOp(_)
            | Self::Copy(_)
            | Self::UnaryOp(_)
            | Self::NullRow(_)
            | Self::Constant(_)
            | Self::UninitRow(_)
            | Self::Uninit(_) => true,

            Self::Store(_)
            | Self::SetNull(_)
            | Self::CopyRowTo(_)
            | Self::Nop(_)
            | Self::Drop(_) => false,
        }
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Expr
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized + 'a,
    DocBuilder<'a, D, A>: Clone,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        match self {
            Expr::Call(call) => call.pretty(alloc, cache),
            Expr::Cast(cast) => cast.pretty(alloc, cache),
            Expr::Load(load) => load.pretty(alloc, cache),
            Expr::Store(store) => store.pretty(alloc, cache),
            Expr::Select(select) => select.pretty(alloc, cache),
            Expr::IsNull(is_null) => is_null.pretty(alloc, cache),
            Expr::BinOp(bin_op) => bin_op.pretty(alloc, cache),
            Expr::Copy(copy) => copy.pretty(alloc, cache),
            Expr::UnaryOp(unary_op) => unary_op.pretty(alloc, cache),
            Expr::NullRow(null_row) => null_row.pretty(alloc, cache),
            Expr::SetNull(set_null) => set_null.pretty(alloc, cache),
            Expr::Constant(constant) => constant.pretty(alloc, cache),
            Expr::CopyRowTo(copy_row_to) => copy_row_to.pretty(alloc, cache),
            Expr::UninitRow(uninit_row) => uninit_row.pretty(alloc, cache),
            Expr::Uninit(uninit) => uninit.pretty(alloc, cache),
            Expr::Nop(nop) => nop.pretty(alloc, cache),
            Expr::Drop(drop) => drop.pretty(alloc, cache),
        }
    }
}

/// An rvalue (right value) is either a reference to another expression or an
/// immediate constant
// TODO: Remove all uses of this
#[derive(Debug, Clone, From, PartialEq, Deserialize, Serialize, JsonSchema)]
pub enum RValue {
    /// An expression
    Expr(ExprId),
    /// An immediate constant value
    Imm(Constant),
}

impl RValue {
    /// Returns `true` if the current rvalue is an expression
    pub const fn is_expr(&self) -> bool {
        matches!(self, Self::Expr(_))
    }

    /// Returns `true` if the current rvalue is an immediate constant
    pub const fn is_immediate(&self) -> bool {
        matches!(self, Self::Imm(_))
    }

    pub const fn as_expr(&self) -> Option<&ExprId> {
        if let Self::Expr(expr) = self {
            Some(expr)
        } else {
            None
        }
    }

    pub const fn as_immediate(&self) -> Option<&Constant> {
        if let Self::Imm(immediate) = self {
            Some(immediate)
        } else {
            None
        }
    }
}

impl From<bool> for RValue {
    fn from(value: bool) -> Self {
        Self::Imm(Constant::Bool(value))
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &RValue
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        match self {
            RValue::Expr(expr) => expr.pretty(alloc, cache),
            RValue::Imm(imm) => imm.pretty(alloc, cache),
        }
    }
}

/// No operation
#[derive(Debug, Clone, From, PartialEq, Default, Deserialize, Serialize, JsonSchema)]
pub struct Nop;

impl Nop {
    pub const fn new() -> Self {
        Self
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Nop
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, _cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc.text("nop")
    }
}

/// A cast expression, changes the type of the given value
///
/// Changes the type of `value` from `from` to `to`
///
/// # Rules
///
/// - Casts between the same type are always valid (that is, a cast from
///   `String` to [`String`] is always valid)
/// - Strings and unit types are not castable (except casts from [`String`] to
///   `String` or from [`Unit`] to `Unit`)
/// - Integers (signed and unsigned) can be freely casted between ([`U16`] <->
///   [`I32`], etc.)
/// - Floats can be casted between themselves ([`F32`] <-> [`F64`])
/// - Integers can be casted to floats and vice versa
/// - Time types ([`Timestamp`] and [`Date`]) can be casted to and from integers
/// - Booleans ([`Bool`]) can be casted to integers (but *not* vice versa)
///
/// [`String`]: ColumnType::String
/// [`Unit`]: ColumnType::Unit
/// [`U16`]: ColumnType::U16
/// [`I32`]: ColumnType::I32
/// [`F32`]: ColumnType::F32
/// [`F64`]: ColumnType::F64
/// [`Timestamp`]: ColumnType::Timestamp
/// [`Date`]: ColumnType::Date
/// [`Bool`]: ColumnType::Bool
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Cast {
    /// The source value being casted
    value: ExprId,
    /// The source type of `value`
    from: ColumnType,
    /// The type to cast `value` into
    to: ColumnType,
}

impl Cast {
    pub fn new(value: ExprId, from: ColumnType, to: ColumnType) -> Self {
        Self { value, from, to }
    }

    pub const fn value(&self) -> ExprId {
        self.value
    }

    pub fn value_mut(&mut self) -> &mut ExprId {
        &mut self.value
    }

    pub const fn from(&self) -> ColumnType {
        self.from
    }

    pub const fn to(&self) -> ColumnType {
        self.to
    }

    /// Returns true if the current cast is between valid types
    pub fn is_valid_cast(&self) -> bool {
        const fn is_weird_float_cast(a: ColumnType, b: ColumnType) -> bool {
            a.is_float() && (b.is_bool() || b.is_date() || b.is_timestamp())
        }

        let Self { from, to, .. } = *self;

        // Casts between the same type are always valid
        if from == to {
            return true;
        }

        let is_invalid_cast =
            // Cannot cast unit types
            from.is_unit() || to.is_unit()
            // Cannot cast strings
            || from.is_string() || to.is_string()
            // Cannot cast from floats to bool, timestamp or date
            || is_weird_float_cast(from, to)
            || is_weird_float_cast(to, from)
            // Cannot cast from non-bool to bool
            || (!from.is_bool() && to.is_bool());

        !is_invalid_cast
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Cast
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("cast")
            .append(alloc.space())
            .append(self.from.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.value.pretty(alloc, cache))
            .append(alloc.space())
            .append(alloc.text("to"))
            .append(alloc.space())
            .append(self.to.pretty(alloc, cache))
    }
}

/// Copies a value
///
/// For most types this is a noop, however for [`String`] this clones the
/// underlying string and results in the cloned string
///
/// [`String`]: ColumnType::String
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Copy {
    /// The value to be copied
    value: ExprId,
    /// The type of the value being copied
    value_ty: ColumnType,
}

impl Copy {
    pub fn new(value: ExprId, value_ty: ColumnType) -> Self {
        Self { value, value_ty }
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
}

impl<'a, D, A> Pretty<'a, D, A> for &Copy
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("copy")
            .append(alloc.space())
            .append(self.value_ty.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.value.pretty(alloc, cache))
    }
}

/// Creates a row containing [uninitialized data]
///
/// Values within the resulting row should never be read from, only written to.
/// After writing a value to the row you may then read from it, but values
/// cannot be read before initialization without causing UB
///
/// [uninitialized data]: https://en.wikipedia.org/wiki/Uninitialized_variable
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct UninitRow {
    layout: LayoutId,
}

impl UninitRow {
    /// Create a new uninit row
    pub fn new(layout: LayoutId) -> Self {
        Self { layout }
    }

    /// Returns the layout of the produced row
    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &UninitRow
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("uninit_row")
            .append(alloc.space())
            .append(self.layout.pretty(alloc, cache))
    }
}

/// Creates a row containing all null values
///
/// If the given layout doesn't contain any nullable columns this behaves
/// identically to [`UninitRow`].
///
/// Somewhat counter-intuitively, this does not
/// necessarily mean that the row will contain any
/// particular values, only that all nullish flags
/// will be set to null. What "set" means also doesn't
/// necessarily mean that they'll all be set to `1`, we
/// reserve the right to assign `0` as our nullish
/// sigil value since that could potentially be more efficient.
/// In short: `NullRow` produces a row for which `IsNull` will
/// always return `true`
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct NullRow {
    layout: LayoutId,
}

impl NullRow {
    /// Create a new null row
    pub fn new(layout: LayoutId) -> Self {
        Self { layout }
    }

    /// Returns the layout of the produced row
    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &NullRow
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("null")
            .append(alloc.space())
            .append(self.layout.pretty(alloc, cache))
    }
}

/// Creates an uninitialized scalar or row value
#[derive(Debug, Clone, From, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Uninit {
    value: RowOrScalar,
}

impl Uninit {
    /// Create a new null row
    pub fn new(value: RowOrScalar) -> Self {
        Self { value }
    }

    pub const fn value(&self) -> RowOrScalar {
        self.value
    }

    pub fn value_mut(&mut self) -> &mut RowOrScalar {
        &mut self.value
    }

    pub const fn is_scalar(&self) -> bool {
        self.value.is_scalar()
    }

    pub const fn is_row(&self) -> bool {
        self.value.is_row()
    }

    pub const fn as_scalar(&self) -> Option<ColumnType> {
        self.value.as_scalar()
    }

    pub const fn as_row(&self) -> Option<LayoutId> {
        self.value.as_row()
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Uninit
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("uninit")
            .append(alloc.space())
            .append(self.value.pretty(alloc, cache))
    }
}
