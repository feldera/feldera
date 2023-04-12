mod builder;
mod flags;
mod passes;

pub use builder::FunctionBuilder;
pub use flags::{InputFlags, InvalidInputFlag};

use crate::ir::{Block, BlockId, ColumnType, ExprId, LayoutId, Signature};
use petgraph::{
    algo::dominators::{self, Dominators},
    prelude::DiGraphMap,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[serde_with::serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Function {
    args: Vec<FuncArg>,
    ret: ColumnType,
    entry_block: BlockId,
    #[serde_as(as = "BTreeMap<serde_with::DisplayFromStr, _>")]
    blocks: BTreeMap<BlockId, Block>,
    #[serde(skip)]
    cfg: DiGraphMap<BlockId, ()>,
}

impl Function {
    pub fn args(&self) -> &[FuncArg] {
        &self.args
    }

    pub fn entry_block(&self) -> BlockId {
        self.entry_block
    }

    pub fn blocks(&self) -> &BTreeMap<BlockId, Block> {
        &self.blocks
    }

    pub const fn return_type(&self) -> ColumnType {
        self.ret
    }

    pub fn dominators(&self) -> Dominators<BlockId> {
        dominators::simple_fast(&self.cfg, self.entry_block)
    }

    pub fn signature(&self) -> Signature {
        Signature::new(
            self.args.iter().map(|arg| arg.layout).collect(),
            self.args.iter().map(|arg| arg.flags).collect(),
            self.ret,
        )
    }

    pub(crate) fn set_cfg(&mut self, cfg: DiGraphMap<BlockId, ()>) {
        self.cfg = cfg;
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FuncArg {
    /// The id that the pointer is associated with and the flags that are
    /// associated with the argument. All function arguments are passed by
    /// pointer since we can't know the type's exact size at compile time
    pub id: ExprId,
    /// The layout of the argument
    pub layout: LayoutId,
    /// The flags associated with the argument
    pub flags: InputFlags,
}

impl FuncArg {
    pub const fn new(id: ExprId, layout: LayoutId, flags: InputFlags) -> Self {
        Self { id, layout, flags }
    }
}
