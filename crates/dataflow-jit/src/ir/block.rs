use crate::ir::{expr::Terminator, BlockId, ExprId};

#[derive(Debug, Clone, PartialEq)]
pub struct Block {
    id: BlockId,
    body: Vec<ExprId>,
    terminator: Terminator,
}

impl Block {
    pub const fn id(&self) -> BlockId {
        self.id
    }

    pub fn body(&self) -> &[ExprId] {
        &self.body
    }

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

    pub fn terminator(&self) -> &Terminator {
        &self.terminator
    }

    pub fn terminator_mut(&mut self) -> &mut Terminator {
        &mut self.terminator
    }
}

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
