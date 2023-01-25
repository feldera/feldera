use crate::ir::{expr::Terminator, BlockId, ExprId};

#[derive(Debug, PartialEq)]
pub struct Block {
    pub(crate) id: BlockId,
    pub(crate) body: Vec<ExprId>,
    pub(crate) terminator: Terminator,
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
}
