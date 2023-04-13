mod builder;
mod flags;
mod passes;

pub use builder::FunctionBuilder;
pub use flags::{InputFlags, InvalidInputFlag};
use schemars::JsonSchema;

use crate::ir::{block::Block, BlockId, ColumnType, ExprId, LayoutId, Signature};
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

impl schemars::JsonSchema for Function {
    fn schema_name() -> std::string::String {
        "Function".to_owned()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        {
            let mut schema_object = schemars::schema::SchemaObject {
                instance_type: Some(schemars::schema::InstanceType::Object.into()),
                ..Default::default()
            };

            let object_validation = schema_object.object();

            object_validation
                .properties
                .insert("args".to_owned(), gen.subschema_for::<Vec<FuncArg>>());
            object_validation.required.insert("args".to_owned());

            object_validation
                .properties
                .insert("ret".to_owned(), gen.subschema_for::<ColumnType>());
            object_validation.required.insert("ret".to_owned());

            object_validation
                .properties
                .insert("entry_block".to_owned(), gen.subschema_for::<BlockId>());
            object_validation.required.insert("entry_block".to_owned());

            object_validation.properties.insert(
                "blocks".to_owned(),
                gen.subschema_for::<BTreeMap<BlockId, Block>>(),
            );
            object_validation.required.insert("blocks".to_owned());

            schemars::schema::Schema::Object(schema_object)
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
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
