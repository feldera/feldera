use crate::ir::{
    layout_cache::RowLayoutCache, types::Signature, DataflowNode, LayoutId, NodeId, StreamKind,
    StreamLayout,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Differentiate {
    input: NodeId,
}

impl Differentiate {
    pub fn new(input: NodeId) -> Self {
        Self { input }
    }

    pub fn input(&self) -> NodeId {
        self.input
    }
}

impl DataflowNode for Differentiate {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(match inputs[0] {
            StreamLayout::Set(value) => StreamLayout::Set(value),
            StreamLayout::Map(key, value) => StreamLayout::Map(key, value),
        })
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}

    fn remap_layouts(&mut self, _mappings: &BTreeMap<LayoutId, LayoutId>) {}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Integrate {
    input: NodeId,
}

impl Integrate {
    pub fn new(input: NodeId) -> Self {
        Self { input }
    }

    pub fn input(&self) -> NodeId {
        self.input
    }
}

impl DataflowNode for Integrate {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(match inputs[0] {
            StreamLayout::Set(value) => StreamLayout::Set(value),
            StreamLayout::Map(key, value) => StreamLayout::Map(key, value),
        })
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}

    fn remap_layouts(&mut self, _mappings: &BTreeMap<LayoutId, LayoutId>) {}
}
