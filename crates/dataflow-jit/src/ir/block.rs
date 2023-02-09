use crate::ir::{BlockId, ExprId, Terminator};

/// A single basic block within a function
///
/// Each basic block has a unique id, zero or more expressions within its body
/// and a single terminator
#[derive(Debug, Clone, PartialEq)]
pub struct Block {
    /// The id of the block
    id: BlockId,
    /// The ids of all expressions within the block
    body: Vec<ExprId>,
    /// The block's terminator
    terminator: Terminator,
}

impl Block {
    /// Returns the block's id
    pub const fn id(&self) -> BlockId {
        self.id
    }

    /// Returns the ids of all expressions within the block
    pub fn body(&self) -> &[ExprId] {
        &self.body
    }

    /// Returns a mutable reference to the ids of all expressions within the
    /// block
    pub fn body_mut(&mut self) -> &mut [ExprId] {
        &mut self.body
    }

    /// Removes all expressions within the current block's body for which the
    /// predicate returns `false`
    pub fn retain<F>(&mut self, mut retain: F)
    where
        F: FnMut(ExprId) -> bool,
    {
        self.body.retain(|&expr_id| retain(expr_id));
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
    pub(crate) body: Vec<ExprId>,
    pub(crate) terminator: Option<Terminator>,
}

impl UnsealedBlock {
    pub(crate) fn new(id: BlockId) -> Self {
        Self {
            id,
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
            body: self.body,
            terminator,
        }
    }
}
