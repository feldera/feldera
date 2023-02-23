use crate::ir::{
    layout_cache::RowLayoutCache, types::Signature, DataflowNode, LayoutId, NodeId, Stream,
    StreamKind,
};
use serde::{Deserialize, Serialize};

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

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(match inputs[0] {
            Stream::Set(value) => Stream::Set(value),
            Stream::Map(key, value) => Stream::Map(key, value),
        })
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
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

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(match inputs[0] {
            Stream::Set(value) => Stream::Set(value),
            Stream::Map(key, value) => Stream::Map(key, value),
        })
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}
