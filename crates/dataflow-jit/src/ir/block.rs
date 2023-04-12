use crate::ir::{BlockId, ColumnType, Expr, ExprId, Terminator};
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
    params: Vec<(ExprId, ColumnType)>,
    /// The ids of all expressions within the block
    body: Vec<(ExprId, Expr)>,
    /// The block's terminator
    terminator: Terminator,
}

impl Block {
    /// Returns the block's id
    pub const fn id(&self) -> BlockId {
        self.id
    }

    /// Returns the block's parameters
    pub fn params(&self) -> &[(ExprId, ColumnType)] {
        &self.params
    }

    /// Returns a mutable reference to the block's parameters
    pub fn params_mut(&mut self) -> &mut Vec<(ExprId, ColumnType)> {
        &mut self.params
    }

    /// Returns the ids of all expressions within the block
    pub fn body(&self) -> &[(ExprId, Expr)] {
        &self.body
    }

    /// Returns a mutable reference to the ids of all expressions within the
    /// block
    pub fn body_mut(&mut self) -> &mut [(ExprId, Expr)] {
        &mut self.body
    }

    /// Removes all expressions within the current block's body for which the
    /// predicate returns `false`
    pub fn retain<F>(&mut self, mut retain: F)
    where
        F: FnMut(ExprId, &Expr) -> bool,
    {
        self.body.retain(|(expr_id, expr)| retain(*expr_id, expr));
    }

    /// Returns the block's terminator
    pub fn terminator(&self) -> &Terminator {
        &self.terminator
    }

    /// Returns a mutable reference to the block's terminator
    pub fn terminator_mut(&mut self) -> &mut Terminator {
        &mut self.terminator
    }
}

/// An unfinished basic block
pub(crate) struct UnsealedBlock {
    pub(crate) id: BlockId,
    pub(crate) params: Vec<(ExprId, ColumnType)>,
    pub(crate) body: Vec<(ExprId, Expr)>,
    pub(crate) terminator: Option<Terminator>,
}

impl UnsealedBlock {
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
