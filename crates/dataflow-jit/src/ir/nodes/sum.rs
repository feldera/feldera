use crate::ir::{
    layout_cache::RowLayoutCache,
    nodes::{DataflowNode, StreamLayout},
    LayoutId, NodeId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct Sum {
    inputs: Vec<NodeId>,
    layout: StreamLayout,
}

impl Sum {
    pub fn new(inputs: Vec<NodeId>, layout: StreamLayout) -> Self {
        Self { inputs, layout }
    }

    pub fn inputs(&self) -> &[NodeId] {
        &self.inputs
    }

    pub fn layout(&self) -> StreamLayout {
        self.layout
    }
}

impl DataflowNode for Sum {
    fn map_inputs<F>(&self, map: &mut F)
    where
        F: FnMut(NodeId),
    {
        self.inputs.iter().copied().for_each(map);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
        self.inputs.iter_mut().for_each(map);
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn validate(&self, inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        assert!(inputs.iter().all(|&layout| layout == self.layout));
    }

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        self.layout.map_layouts(map);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout.remap_layouts(mappings);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct Minus {
    lhs: NodeId,
    rhs: NodeId,
}

impl Minus {
    pub fn new(lhs: NodeId, rhs: NodeId) -> Self {
        Self { lhs, rhs }
    }

    pub fn lhs(&self) -> NodeId {
        self.lhs
    }

    pub fn rhs(&self) -> NodeId {
        self.rhs
    }
}

impl DataflowNode for Minus {
    fn map_inputs<F>(&self, map: &mut F)
    where
        F: FnMut(NodeId),
    {
        map(self.lhs);
        map(self.rhs);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
        map(&mut self.lhs);
        map(&mut self.rhs);
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(inputs[0])
    }

    fn validate(&self, inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0], inputs[1]);
    }

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, _map: &mut F)
    where
        F: FnMut(LayoutId),
    {
    }

    fn remap_layouts(&mut self, _mappings: &BTreeMap<LayoutId, LayoutId>) {}
}
