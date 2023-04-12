use crate::ir::{BlockId, ColumnType, Expr, ExprId, LayoutId, Terminator};
use serde::{Deserialize, Serialize};

/// A single basic block within a function
///
/// Each basic block has a unique id, zero or more expressions within its body
/// and a single terminator
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Block {
    /// The id of the block
    id: BlockId,
    /// The parameters passed to this basic block
    #[serde(default)]
    params: Vec<(ExprId, ParamType)>,
    /// The ids of all expressions within the block
    body: Vec<(ExprId, Expr)>,
    /// The block's terminator
    terminator: Terminator,
}

impl Block {
    /// Returns the block's id
    #[inline]
    pub const fn id(&self) -> BlockId {
        self.id
    }

    /// Returns the block's parameters
    #[inline]
    pub fn params(&self) -> &[(ExprId, ParamType)] {
        &self.params
    }

    /// Returns a mutable reference to the block's parameters
    #[inline]
    pub fn params_mut(&mut self) -> &mut Vec<(ExprId, ParamType)> {
        &mut self.params
    }

    /// Returns the ids of all expressions within the block
    #[inline]
    pub fn body(&self) -> &[(ExprId, Expr)] {
        &self.body
    }

    /// Returns a mutable reference to the ids of all expressions within the
    /// block
    #[inline]
    pub fn body_mut(&mut self) -> &mut [(ExprId, Expr)] {
        &mut self.body
    }

    /// Removes all expressions within the current block's body for which the
    /// predicate returns `false`
    #[inline]
    pub fn retain<F>(&mut self, mut retain: F)
    where
        F: FnMut(ExprId, &Expr) -> bool,
    {
        self.body.retain(|(expr_id, expr)| retain(*expr_id, expr));
    }

    /// Returns the block's terminator
    #[inline]
    pub fn terminator(&self) -> &Terminator {
        &self.terminator
    }

    /// Returns a mutable reference to the block's terminator
    #[inline]
    pub fn terminator_mut(&mut self) -> &mut Terminator {
        &mut self.terminator
    }
}

/// An unfinished basic block
pub(crate) struct UnsealedBlock {
    pub(crate) id: BlockId,
    pub(crate) params: Vec<(ExprId, ParamType)>,
    pub(crate) body: Vec<(ExprId, Expr)>,
    pub(crate) terminator: Option<Terminator>,
}

impl UnsealedBlock {
    #[inline]
    pub(crate) fn new(id: BlockId) -> Self {
        Self {
            id,
            params: Vec::new(),
            body: Vec::new(),
            terminator: None,
        }
    }

    #[track_caller]
    pub(crate) fn into_block(self) -> Block {
        let terminator = self.terminator.unwrap_or_else(|| {
            panic!(
                "Called `FunctionBuilder::build()` with unfinished blocks: {} has no terminator",
                self.id,
            )
        });

        Block {
            id: self.id,
            params: self.params,
            body: self.body,
            terminator,
        }
    }
}

/// The type of a parameter, either a layout or a single column's type
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub enum ParamType {
    /// A row type consisting of a [`LayoutId`]
    Row(LayoutId),
    /// A scalar column consisting of a [`ColumnType`]
    Column(ColumnType),
}

impl ParamType {
    /// Returns `true` if the param type is a [`Row`].
    ///
    /// [`Row`]: ParamType::Row
    #[must_use]
    #[inline]
    pub const fn is_row(&self) -> bool {
        matches!(self, Self::Row(..))
    }

    /// Returns `true` if the param type is a [`Column`].
    ///
    /// [`Column`]: ParamType::Column
    #[must_use]
    #[inline]
    pub const fn is_column(&self) -> bool {
        matches!(self, Self::Column(..))
    }

    #[inline]
    pub const fn as_row(&self) -> Option<&LayoutId> {
        if let Self::Row(row) = self {
            Some(row)
        } else {
            None
        }
    }

    #[inline]
    pub const fn as_column(&self) -> Option<&ColumnType> {
        if let Self::Column(column) = self {
            Some(column)
        } else {
            None
        }
    }

    #[inline]
    pub fn as_mut_row(&mut self) -> Option<&mut LayoutId> {
        if let Self::Row(row) = self {
            Some(row)
        } else {
            None
        }
    }

    #[inline]
    pub fn as_mut_column(&mut self) -> Option<&mut ColumnType> {
        if let Self::Column(column) = self {
            Some(column)
        } else {
            None
        }
    }

    #[inline]
    pub const fn try_into_row(self) -> Result<LayoutId, Self> {
        if let Self::Row(row) = self {
            Ok(row)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn try_into_column(self) -> Result<ColumnType, Self> {
        if let Self::Column(column) = self {
            Ok(column)
        } else {
            Err(self)
        }
    }

    #[track_caller]
    pub const fn unwrap_row(self) -> LayoutId {
        if let Self::Row(row) = self {
            row
        } else {
            panic!("called .unwrap_row() on a column type")
        }
    }

    #[track_caller]
    pub const fn unwrap_column(self) -> ColumnType {
        if let Self::Column(column) = self {
            column
        } else {
            panic!("called .unwrap_column() on a row type")
        }
    }

    #[track_caller]
    pub fn expect_row(self, message: &str) -> LayoutId {
        if let Self::Row(row) = self {
            row
        } else {
            panic!("called .expect_row() on a column type: {message}")
        }
    }

    #[track_caller]
    pub fn expect_column(self, message: &str) -> ColumnType {
        if let Self::Column(column) = self {
            column
        } else {
            panic!("called .expect_column() on a row type: {message}")
        }
    }
}

impl From<ColumnType> for ParamType {
    #[inline]
    fn from(column: ColumnType) -> Self {
        Self::Column(column)
    }
}

impl From<LayoutId> for ParamType {
    #[inline]
    fn from(row: LayoutId) -> Self {
        Self::Row(row)
    }
}
