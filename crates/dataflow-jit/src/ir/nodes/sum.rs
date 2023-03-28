use crate::ir::{
    layout_cache::RowLayoutCache,
    nodes::{DataflowNode, StreamKind, StreamLayout},
    LayoutId, NodeId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Sum {
    inputs: Vec<NodeId>,
}

impl Sum {
    pub fn new(inputs: Vec<NodeId>) -> Self {
        Self { inputs }
    }

    pub fn inputs(&self) -> &[NodeId] {
        &self.inputs
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

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(inputs[0])
    }

    fn validate(&self, inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        assert!(inputs
            .iter()
            .all(|layout1| inputs.iter().all(|layout2| layout1 == layout2)));
    }

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, _map: &mut F)
    where
        F: FnMut(LayoutId),
    {
    }

    fn remap_layouts(&mut self, _mappings: &BTreeMap<LayoutId, LayoutId>) {}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
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
