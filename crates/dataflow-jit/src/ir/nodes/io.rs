use crate::ir::{
    layout_cache::RowLayoutCache, types::Signature, DataflowNode, LayoutId, NodeId, StreamKind,
    StreamLayout,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Export {
    input: NodeId,
    layout: StreamLayout,
}

impl Export {
    pub fn new(input: NodeId, layout: StreamLayout) -> Self {
        Self { input, layout }
    }

    pub fn input(&self) -> NodeId {
        self.input
    }

    pub fn layout(&self) -> StreamLayout {
        self.layout
    }
}

impl DataflowNode for Export {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(self.layout.kind())
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExportedNode {
    subgraph: NodeId,
    input: NodeId,
    layout: StreamLayout,
}

impl ExportedNode {
    pub fn new(subgraph: NodeId, input: NodeId, layout: StreamLayout) -> Self {
        Self {
            subgraph,
            input,
            layout,
        }
    }

    pub fn subgraph(&self) -> NodeId {
        self.subgraph
    }

    pub fn input(&self) -> NodeId {
        self.input
    }

    pub fn layout(&self) -> StreamLayout {
        self.layout
    }
}

impl DataflowNode for ExportedNode {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.extend([self.subgraph]);
    }

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(self.layout.kind())
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Source {
    /// The type of the source's produced stream
    layout: LayoutId,
}

impl Source {
    pub const fn new(layout: LayoutId) -> Self {
        Self { layout }
    }

    /// The type of the source's produced stream
    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

impl DataflowNode for Source {
    fn inputs(&self, _inputs: &mut Vec<NodeId>) {}

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(StreamKind::Set)
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(StreamLayout::Set(self.layout))
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.layout);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceMap {
    key: LayoutId,
    value: LayoutId,
}

impl SourceMap {
    pub const fn new(key: LayoutId, value: LayoutId) -> Self {
        Self { key, value }
    }

    /// The key type of the source's produced stream
    pub const fn key(&self) -> LayoutId {
        self.key
    }

    /// The value type of the source's produced stream
    pub const fn value(&self) -> LayoutId {
        self.value
    }
}

impl DataflowNode for SourceMap {
    fn inputs(&self, _inputs: &mut Vec<NodeId>) {}

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(StreamKind::Map)
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(StreamLayout::Map(self.key, self.value))
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.extend([self.key, self.value]);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Sink {
    input: NodeId,
}

impl Sink {
    pub fn new(input: NodeId) -> Self {
        Self { input }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }
}

impl DataflowNode for Sink {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        None
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        None
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}
