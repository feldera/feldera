use crate::ir::{
    layout_cache::RowLayoutCache, types::Signature, Constant, DataflowNode, LayoutId, NodeId,
    StreamKind, StreamLayout,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum StreamLiteral {
    Set(Vec<(RowLiteral, i32)>),
    Map(Vec<(RowLiteral, RowLiteral, i32)>),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RowLiteral {
    rows: Vec<NullableConstant>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum NullableConstant {
    NonNull(Constant),
    Nullable(Option<Constant>),
}

impl RowLiteral {
    pub fn new(rows: Vec<NullableConstant>) -> Self {
        Self { rows }
    }

    pub fn rows(&self) -> &[NullableConstant] {
        &self.rows
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConstantStream {
    input: NodeId,
    value: StreamLiteral,
    layout: StreamLayout,
}

impl ConstantStream {
    pub fn new(input: NodeId, value: StreamLiteral, layout: StreamLayout) -> Self {
        Self {
            input,
            value,
            layout,
        }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub fn value(&self) -> &StreamLiteral {
        &self.value
    }

    pub const fn layout(&self) -> StreamLayout {
        self.layout
    }
}

impl DataflowNode for ConstantStream {
    fn inputs(&self, _inputs: &mut Vec<NodeId>) {}

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(self.layout.kind())
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        match self.layout {
            StreamLayout::Set(key) => layouts.push(key),
            StreamLayout::Map(key, value) => layouts.extend([key, value]),
        }
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        match &mut self.layout {
            StreamLayout::Set(key) => *key = mappings[key],
            StreamLayout::Map(key, value) => {
                *key = mappings[key];
                *value = mappings[value];
            }
        }
    }
}
