use crate::ir::{
    layout_cache::RowLayoutCache,
    nodes::{DataflowNode, StreamLayout},
    pretty::{DocAllocator, DocBuilder, Pretty},
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
        F: FnMut(NodeId) + ?Sized,
    {
        map(self.input);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId) + ?Sized,
    {
        map(&mut self.input);
    }

    fn output_stream(
        &self,
        _inputs: &[StreamLayout],
        _layout_cache: &RowLayoutCache,
    ) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId) + ?Sized,
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
        F: FnMut(NodeId) + ?Sized,
    {
        map(self.subgraph);
        map(self.input);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId) + ?Sized,
    {
        map(&mut self.subgraph);
        map(&mut self.input);
    }

    fn output_stream(
        &self,
        _inputs: &[StreamLayout],
        _layout_cache: &RowLayoutCache,
    ) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId) + ?Sized,
    {
        self.layout.map_layouts(map);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout.remap_layouts(mappings);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
pub enum SourceKind {
    /// A zset source
    ZSet,
    /// An upsert source
    Upsert,
}

impl SourceKind {
    /// Returns `true` if the source kind is [`ZSet`].
    ///
    /// [`ZSet`]: SourceKind::ZSet
    #[must_use]
    pub const fn is_zset(self) -> bool {
        matches!(self, Self::ZSet)
    }

    /// Returns `true` if the source kind is [`Upsert`].
    ///
    /// [`Upsert`]: SourceKind::Upsert
    #[must_use]
    pub const fn is_upsert(self) -> bool {
        matches!(self, Self::Upsert)
    }

    const fn to_str(self) -> &'static str {
        match self {
            Self::ZSet => "zset",
            Self::Upsert => "upsert",
        }
    }
}

impl Default for SourceKind {
    fn default() -> Self {
        Self::ZSet
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct Source {
    /// The type of the source's produced stream, implicitly determines whether the source produces a set or a map
    layout: StreamLayout,
    kind: SourceKind,
    #[serde(alias = "table")]
    name: Option<Box<str>>,
}

impl Source {
    pub const fn new(layout: StreamLayout, kind: SourceKind, name: Option<Box<str>>) -> Self {
        Self { layout, kind, name }
    }

    /// The type of the source's produced stream
    pub const fn layout(&self) -> StreamLayout {
        self.layout
    }

    pub const fn kind(&self) -> SourceKind {
        self.kind
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub const fn output_layout(&self) -> StreamLayout {
        self.layout()
    }

    pub const fn is_set(&self) -> bool {
        self.layout.is_set()
    }

    pub const fn is_map(&self) -> bool {
        self.layout.is_map()
    }
}

impl DataflowNode for Source {
    fn map_inputs<F>(&self, _map: &mut F)
    where
        F: FnMut(NodeId) + ?Sized,
    {
    }

    fn map_inputs_mut<F>(&mut self, _map: &mut F)
    where
        F: FnMut(&mut NodeId) + ?Sized,
    {
    }

    fn output_stream(
        &self,
        _inputs: &[StreamLayout],
        _layout_cache: &RowLayoutCache,
    ) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId) + ?Sized,
    {
        self.layout.map_layouts(map);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout.remap_layouts(mappings);
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Source
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("source")
            .append(alloc.space())
            .append(self.kind.to_str())
            .append(alloc.space())
            .append(self.layout.pretty(alloc, cache))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct Sink {
    input: NodeId,
    view: Box<str>,
    input_layout: StreamLayout,
}

impl Sink {
    pub fn new(input: NodeId, view: Box<str>, input_layout: StreamLayout) -> Self {
        Self {
            input,
            view,
            input_layout,
        }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn view(&self) -> &str {
        &self.view
    }

    pub const fn name(&self) -> &str {
        &self.view
    }

    pub const fn input_layout(&self) -> StreamLayout {
        self.input_layout
    }
}

impl DataflowNode for Sink {
    fn map_inputs<F>(&self, map: &mut F)
    where
        F: FnMut(NodeId) + ?Sized,
    {
        map(self.input);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId) + ?Sized,
    {
        map(&mut self.input);
    }

    fn output_stream(
        &self,
        _inputs: &[StreamLayout],
        _layout_cache: &RowLayoutCache,
    ) -> Option<StreamLayout> {
        None
    }

    fn validate(&self, inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        assert_eq!(inputs.len(), 1);
        assert_eq!(self.input_layout, inputs[0]);
    }

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId) + ?Sized,
    {
        self.input_layout.map_layouts(map);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.input_layout.remap_layouts(mappings);
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Sink
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .text("sink")
            .append(alloc.space())
            .append(self.input_layout.pretty(alloc, cache))
            .append(alloc.space())
            .append(self.input.pretty(alloc, cache))
    }
}
