use crate::ir::{
    layout_cache::RowLayoutCache, types::Signature, DataflowNode, LayoutId, NodeId, StreamKind,
    StreamLayout,
};
use serde::{Deserialize, Serialize};

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
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.extend(self.inputs.iter().copied());
    }

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(inputs[0])
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
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
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.extend([self.lhs, self.rhs]);
    }

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(inputs[0])
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0], inputs[1]);
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}
