use crate::ir::{BlockId, ExprId, LayoutId};
use derive_more::From;

#[derive(Debug, From)]
pub enum Terminator {
    Return(Return),
    Jump(Jump),
    Branch(Branch),
}

#[derive(Debug)]
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
}

#[derive(Debug)]
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

#[derive(Debug)]
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

    pub const fn truthy(&self) -> BlockId {
        self.truthy
    }

    pub const fn falsy(&self) -> BlockId {
        self.falsy
    }
}

#[derive(Debug, From)]
pub enum RValue {
    Expr(ExprId),
    Imm(Constant),
}

#[derive(Debug, From)]
pub enum Expr {
    BinOp(BinOp),
    Insert(Insert),
    IsNull(IsNull),
    CopyVal(CopyVal),
    Extract(Extract),
    NullRow(NullRow),
    SetNull(SetNull),
    Constant(Constant),
    CopyRowTo(CopyRowTo),
    UninitRow(UninitRow),
}

#[derive(Debug)]
pub struct BinOp {
    // TODO: Allow for immediates
    lhs: ExprId,
    rhs: ExprId,
    kind: BinOpKind,
}

impl BinOp {
    pub fn new(lhs: ExprId, rhs: ExprId, kind: BinOpKind) -> Self {
        Self { lhs, rhs, kind }
    }

    pub const fn lhs(&self) -> ExprId {
        self.lhs
    }

    pub const fn rhs(&self) -> ExprId {
        self.rhs
    }

    pub const fn kind(&self) -> BinOpKind {
        self.kind
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BinOpKind {
    Add,
    Sub,
    Mul,
    Eq,
    Neq,
    And,
    Or,
}

/// Copies a value
#[derive(Debug)]
pub struct CopyVal {
    /// The value to be copied
    value: ExprId,
}

impl CopyVal {
    pub fn new(value: ExprId) -> Self {
        Self { value }
    }

    pub const fn value(&self) -> ExprId {
        self.value
    }
}

/// Extract a value from a row
#[derive(Debug)]
pub struct Extract {
    /// The row to extract from
    source: ExprId,
    /// The index of the row to extract from
    row: usize,
}

impl Extract {
    pub fn new(target: ExprId, row: usize) -> Self {
        Self {
            source: target,
            row,
        }
    }

    pub const fn source(&self) -> ExprId {
        self.source
    }

    pub const fn row(&self) -> usize {
        self.row
    }
}

/// Insert a value into a row
#[derive(Debug)]
pub struct Insert {
    /// The row to insert into
    target: ExprId,
    /// The index of the row to insert into
    row: usize,
    /// The value being inserted
    value: RValue,
}

impl Insert {
    pub fn new(target: ExprId, row: usize, value: RValue) -> Self {
        Self { target, row, value }
    }

    pub const fn target(&self) -> ExprId {
        self.target
    }

    pub const fn row(&self) -> usize {
        self.row
    }

    pub const fn value(&self) -> &RValue {
        &self.value
    }
}

#[derive(Debug)]
pub struct IsNull {
    value: ExprId,
    row: usize,
}

impl IsNull {
    pub fn new(value: ExprId, row: usize) -> Self {
        Self { value, row }
    }

    pub const fn value(&self) -> ExprId {
        self.value
    }

    pub const fn row(&self) -> usize {
        self.row
    }
}

#[derive(Debug)]
pub struct SetNull {
    target: ExprId,
    row: usize,
    is_null: RValue,
}

impl SetNull {
    pub fn new(target: ExprId, row: usize, is_null: RValue) -> Self {
        Self {
            target,
            row,
            is_null,
        }
    }

    pub const fn target(&self) -> ExprId {
        self.target
    }

    pub const fn row(&self) -> usize {
        self.row
    }

    pub const fn is_null(&self) -> &RValue {
        &self.is_null
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct CopyRowTo {
    from: ExprId,
    to: ExprId,
    layout: LayoutId,
}

impl CopyRowTo {
    pub fn new(from: ExprId, to: ExprId, layout: LayoutId) -> Self {
        Self { from, to, layout }
    }

    pub const fn from(&self) -> ExprId {
        self.from
    }

    pub const fn to(&self) -> ExprId {
        self.to
    }

    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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
