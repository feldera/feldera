pub mod visit;

mod binary;
mod call;
mod constant;
mod select;
mod unary;

pub use binary::{BinaryOp, BinaryOpKind};
pub use call::{ArgType, Call};
pub use constant::Constant;
pub use select::Select;
pub use unary::{UnaryOp, UnaryOpKind};

use crate::ir::{exprs::visit::MapLayouts, ColumnType, ExprId, LayoutId};
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
}

impl Expr {
    pub(crate) fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        match self {
            Self::Load(load) => load.source_layout = mappings[&load.source_layout],
            Self::Store(store) => store.target_layout = mappings[&store.target_layout],
            Self::IsNull(is_null) => is_null.target_layout = mappings[&is_null.target_layout],
            Self::SetNull(set_null) => set_null.target_layout = mappings[&set_null.target_layout],
            Self::CopyRowTo(copy_row) => copy_row.layout = mappings[&copy_row.layout],
            Self::NullRow(null_row) => null_row.layout = mappings[&null_row.layout],
            Self::UninitRow(uninit_row) => uninit_row.layout = mappings[&uninit_row.layout],
            Self::Call(call) => {
                for arg in call.arg_types_mut() {
                    if let ArgType::Row(layout) = arg {
                        *layout = mappings[layout];
                    }
                }
            }

            // These expressions don't contain `LayoutId`s
            Self::Cast(_)
            | Self::BinOp(_)
            | Self::Select(_)
            | Self::Copy(_)
            | Self::UnaryOp(_)
            | Self::Constant(_) => {}
        }
    }

    pub(crate) fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        self.apply(&mut MapLayouts::new(map));
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

    pub const fn source_layout(&self) -> LayoutId {
        self.source_layout
    }

    pub const fn column(&self) -> usize {
        self.column
    }

    pub const fn column_type(&self) -> ColumnType {
        self.column_type
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

    pub const fn target_layout(&self) -> LayoutId {
        self.target_layout
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

    pub const fn target_layout(&self) -> LayoutId {
        self.target_layout
    }

    pub const fn column(&self) -> usize {
        self.column
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

/// Copies the contents of the source row into the destination row
///
/// Both the `src` and `dest` rows must have the same layout.
///
/// Semantically equivalent to [`Load`]-ing each column within `src`, calling
/// [`struct@Copy`] on the loaded value and then [`Store`]-ing the copied value
/// to `dest` (along with any required [`IsNull`]/[`SetNull`] juggling that has
/// to be done due to nullable columns)
// TODO: We need to offer a drop operator of some kind so that rows can be deinitialized if needed
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

    /// Returns the destination row
    pub const fn dest(&self) -> ExprId {
        self.dest
    }

    /// Returns the layout of the `src` and `dest` rows
    pub const fn layout(&self) -> LayoutId {
        self.layout
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
