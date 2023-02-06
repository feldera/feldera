use crate::ir::{BlockId, ColumnType, ExprId, LayoutId};
use derive_more::From;

#[derive(Debug, Clone, From, PartialEq)]
pub enum Terminator {
    Return(Return),
    Jump(Jump),
    Branch(Branch),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Return {
    value: RValue,
}

impl Return {
    pub fn new(value: RValue) -> Self {
        Self { value }
    }

    pub fn value(&self) -> &RValue {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut RValue {
        &mut self.value
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Jump {
    target: BlockId,
}

impl Jump {
    pub fn new(target: BlockId) -> Self {
        Self { target }
    }

    pub const fn target(&self) -> BlockId {
        self.target
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Branch {
    cond: RValue,
    truthy: BlockId,
    falsy: BlockId,
}

impl Branch {
    pub fn new(cond: RValue, truthy: BlockId, falsy: BlockId) -> Self {
        Self {
            cond,
            truthy,
            falsy,
        }
    }

    pub const fn cond(&self) -> &RValue {
        &self.cond
    }

    pub fn cond_mut(&mut self) -> &mut RValue {
        &mut self.cond
    }

    pub const fn truthy(&self) -> BlockId {
        self.truthy
    }

    pub const fn falsy(&self) -> BlockId {
        self.falsy
    }
}

#[derive(Debug, Clone, From, PartialEq)]
pub enum RValue {
    Expr(ExprId),
    Imm(Constant),
}

#[derive(Debug, Clone, From, PartialEq)]
pub enum Expr {
    Cast(Cast),
    Load(Load),
    Store(Store),
    // Select(Select),
    IsNull(IsNull),
    BinOp(BinaryOp),
    CopyVal(CopyVal),
    UnaryOp(UnaryOp),
    NullRow(NullRow),
    SetNull(SetNull),
    Constant(Constant),
    CopyRowTo(CopyRowTo),
    UninitRow(UninitRow),
    // TODO: Select, cast
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cast {
    value: ExprId,
    from: ColumnType,
    to: ColumnType,
}

impl Cast {
    pub fn new(value: ExprId, from: ColumnType, to: ColumnType) -> Self {
        Self { value, from, to }
    }

    pub const fn value(&self) -> ExprId {
        self.value
    }

    pub const fn from(&self) -> ColumnType {
        self.from
    }

    pub const fn to(&self) -> ColumnType {
        self.to
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryOp {
    // TODO: Allow for immediates
    lhs: ExprId,
    rhs: ExprId,
    kind: BinaryOpKind,
}

impl BinaryOp {
    pub fn new(lhs: ExprId, rhs: ExprId, kind: BinaryOpKind) -> Self {
        Self { lhs, rhs, kind }
    }

    pub const fn lhs(&self) -> ExprId {
        self.lhs
    }

    pub const fn rhs(&self) -> ExprId {
        self.rhs
    }

    pub const fn kind(&self) -> BinaryOpKind {
        self.kind
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOpKind {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Neq,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
    And,
    Or,
    Xor,
    Min,
    Max,
    // TODO: shr, shl, rem, mod, sqrt, rotl, rotr
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnaryOp {
    value: RValue,
    kind: UnaryOpKind,
}

impl UnaryOp {
    pub fn new(value: RValue, kind: UnaryOpKind) -> Self {
        Self { value, kind }
    }

    pub const fn value(&self) -> &RValue {
        &self.value
    }

    pub const fn kind(&self) -> UnaryOpKind {
        self.kind
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

/// Copies a value
#[derive(Debug, Clone, PartialEq)]
pub struct CopyVal {
    /// The value to be copied
    value: ExprId,
    ty: ColumnType,
}

impl CopyVal {
    pub fn new(value: ExprId, ty: ColumnType) -> Self {
        Self { value, ty }
    }

    pub const fn value(&self) -> ExprId {
        self.value
    }

    pub const fn ty(&self) -> ColumnType {
        self.ty
    }
}

/// Extract a value from a row
#[derive(Debug, Clone, PartialEq)]
pub struct Load {
    /// The row to extract from
    source: ExprId,
    /// The index of the column to extract from
    column: usize,
}

impl Load {
    pub fn new(target: ExprId, column: usize) -> Self {
        Self {
            source: target,
            column,
        }
    }

    pub const fn source(&self) -> ExprId {
        self.source
    }

    pub const fn column(&self) -> usize {
        self.column
    }
}

/// Insert a value into a row
#[derive(Debug, Clone, PartialEq)]
pub struct Store {
    /// The row to insert into
    target: ExprId,
    /// The index of the column to insert into
    column: usize,
    /// The value being inserted
    value: RValue,
}

impl Store {
    pub fn new(target: ExprId, column: usize, value: RValue) -> Self {
        Self {
            target,
            column,
            value,
        }
    }

    pub const fn target(&self) -> ExprId {
        self.target
    }

    pub const fn column(&self) -> usize {
        self.column
    }

    pub const fn value(&self) -> &RValue {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IsNull {
    /// The row we're checking
    target: ExprId,
    /// The column of `target` to fetch the null-ness of
    column: usize,
}

impl IsNull {
    /// Create a new `IsNull` expression
    ///
    /// - `target` must be a readable row type
    /// - `column` must be a valid column index into `target`
    pub fn new(target: ExprId, column: usize) -> Self {
        Self { target, column }
    }

    pub const fn target(&self) -> ExprId {
        self.target
    }

    pub const fn column(&self) -> usize {
        self.column
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetNull {
    /// The row that's being manipulated
    target: ExprId,
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
    pub fn new(target: ExprId, column: usize, is_null: RValue) -> Self {
        Self {
            target,
            column,
            is_null,
        }
    }

    /// Gets the target of the `SetNull`
    pub const fn target(&self) -> ExprId {
        self.target
    }

    pub const fn column(&self) -> usize {
        self.column
    }

    pub const fn is_null(&self) -> &RValue {
        &self.is_null
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Constant {
    Unit,
    U32(u32),
    U64(u64),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    String(String),
}

impl Constant {
    /// Returns `true` if the constant is a [`U32`], [`I32`], [`U64`] or
    /// [`I64`].
    ///
    /// [`U32`]: Constant::U32
    /// [`I32`]: Constant::I32
    /// [`U64`]: Constant::U64
    /// [`I64`]: Constant::I64
    #[must_use]
    pub const fn is_int(&self) -> bool {
        matches!(
            self,
            Self::U32(_) | Self::I32(_) | Self::U64(_) | Self::I64(_),
        )
    }

    /// Returns `true` if the constant is a [`F32`] or [`F64`].
    ///
    /// [`F32`]: Constant::F32
    /// [`F64`]: Constant::F64
    #[must_use]
    pub const fn is_float(&self) -> bool {
        matches!(self, Self::F32(_) | Self::F64(_))
    }

    /// Returns `true` if the constant is [`String`].
    ///
    /// [`String`]: Constant::String
    #[must_use]
    pub const fn is_string(&self) -> bool {
        matches!(self, Self::String(..))
    }

    /// Returns `true` if the constant is [`Bool`].
    ///
    /// [`Bool`]: Constant::Bool
    #[must_use]
    pub const fn is_bool(&self) -> bool {
        matches!(self, Self::Bool(..))
    }

    /// Returns `true` if the constant is [`Unit`].
    ///
    /// [`Unit`]: Constant::Unit
    #[must_use]
    pub const fn is_unit(&self) -> bool {
        matches!(self, Self::Unit)
    }

    pub const fn column_type(&self) -> ColumnType {
        match self {
            Self::Unit => ColumnType::Unit,
            Self::U32(_) => ColumnType::U32,
            Self::U64(_) => ColumnType::U64,
            Self::I32(_) => ColumnType::I32,
            Self::I64(_) => ColumnType::I64,
            Self::F32(_) => ColumnType::F32,
            Self::F64(_) => ColumnType::F64,
            Self::Bool(_) => ColumnType::Bool,
            Self::String(_) => ColumnType::String,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CopyRowTo {
    src: ExprId,
    dest: ExprId,
    layout: LayoutId,
}

impl CopyRowTo {
    pub fn new(src: ExprId, dest: ExprId, layout: LayoutId) -> Self {
        Self { src, dest, layout }
    }

    pub const fn src(&self) -> ExprId {
        self.src
    }

    pub const fn dest(&self) -> ExprId {
        self.dest
    }

    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UninitRow {
    layout: LayoutId,
}

impl UninitRow {
    pub fn new(layout: LayoutId) -> Self {
        Self { layout }
    }

    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

/// Create a row containing all null values
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
#[derive(Debug, Clone, PartialEq)]
pub struct NullRow {
    layout: LayoutId,
}

impl NullRow {
    pub fn new(layout: LayoutId) -> Self {
        Self { layout }
    }

    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}
