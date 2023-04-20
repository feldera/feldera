use crate::ir::{
    layout_cache::RowLayoutCache,
    nodes::{DataflowNode, StreamLayout},
    LayoutId, NodeId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
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
    fn map_inputs<F>(&self, map: &mut F)
    where
        F: FnMut(NodeId),
    {
        map(self.input);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
        map(&mut self.input);
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

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
    fn map_inputs<F>(&self, map: &mut F)
    where
        F: FnMut(NodeId),
    {
        map(self.subgraph);
        map(self.input);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
        map(&mut self.subgraph);
        map(&mut self.input);
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

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
    fn map_inputs<F>(&self, _map: &mut F)
    where
        F: FnMut(NodeId),
    {
    }

    fn map_inputs_mut<F>(&mut self, _map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(StreamLayout::Set(self.layout))
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        map(self.layout);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout = mappings[&self.layout];
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SourceMap {
    key_layout: LayoutId,
    value_layout: LayoutId,
}

impl SourceMap {
    pub const fn new(key: LayoutId, value: LayoutId) -> Self {
        Self {
            key_layout: key,
            value_layout: value,
        }
    }

    /// The key type of the source's produced stream
    pub const fn key(&self) -> LayoutId {
        self.key_layout
    }

    /// The value type of the source's produced stream
    pub const fn value(&self) -> LayoutId {
        self.value_layout
    }
}

impl DataflowNode for SourceMap {
    fn map_inputs<F>(&self, _map: &mut F)
    where
        F: FnMut(NodeId),
    {
    }

    fn map_inputs_mut<F>(&mut self, _map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(StreamLayout::Map(self.key_layout, self.value_layout))
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        map(self.key_layout);
        map(self.value_layout);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.key_layout = mappings[&self.key_layout];
        self.value_layout = mappings[&self.value_layout];
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
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
    fn map_inputs<F>(&self, map: &mut F)
    where
        F: FnMut(NodeId),
    {
        map(self.input);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
        map(&mut self.input);
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        None
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, _map: &mut F)
    where
        F: FnMut(LayoutId),
    {
    }

    fn remap_layouts(&mut self, _mappings: &BTreeMap<LayoutId, LayoutId>) {}
}
