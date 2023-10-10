use crate::ir::{
    function::{Function, FunctionBuilder},
    layout_cache::RowLayoutCache,
    nodes::{
        ConstantStream, DataflowNode, Differentiate, Distinct, Filter, IndexWith, Integrate,
        JoinCore, Map, Node, Sink, Source, SourceKind, StreamDistinct, StreamKind, StreamLayout,
        Subgraph as SubgraphNode,
    },
    visit::{MutNodeVisitor, NodeVisitor},
    LayoutId, NodeId,
};
use petgraph::prelude::DiGraphMap;
use std::collections::BTreeMap;

pub trait GraphExt {
    fn layout_cache(&self) -> &RowLayoutCache;

    fn nodes(&self) -> &BTreeMap<NodeId, Node>;

    fn nodes_mut(&mut self) -> &mut BTreeMap<NodeId, Node>;

    fn next_node(&self) -> NodeId;

    fn create_node<N>(&mut self, node_id: NodeId, node: N) -> NodeId
    where
        N: Into<Node>;

    fn add_node<N>(&mut self, node: N) -> NodeId
    where
        N: Into<Node>,
    {
        let node_id = self.next_node();
        self.create_node(node_id, node)
    }

    fn subgraph<F, T>(&mut self, build: F) -> (NodeId, T)
    where
        F: FnOnce(&mut SubgraphNode) -> T;

    fn optimize(&mut self);

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        for node in self.nodes().values() {
            node.functions(functions);
        }
    }

    fn map_layouts<F>(&self, mut map: F)
    where
        F: FnMut(LayoutId),
    {
        self.map_layouts_inner(&mut map);
    }

    #[doc(hidden)]
    fn map_layouts_inner<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId) + ?Sized,
    {
        self.nodes().values().for_each(|node| node.map_layouts(map));
    }

    fn map_inputs_mut<F>(&mut self, mut map: F)
    where
        F: FnMut(&mut NodeId),
    {
        self.map_inputs_mut_inner(&mut map);
    }

    #[doc(hidden)]
    fn map_inputs_mut_inner<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId) + ?Sized,
    {
        self.nodes_mut()
            .values_mut()
            .for_each(|node| node.map_inputs_mut(map));
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        self.map_layouts(|layout_id| layouts.push(layout_id));
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        for node in self.nodes_mut().values_mut() {
            node.remap_layouts(mappings);
        }
    }

    fn edges(&self) -> &DiGraphMap<NodeId, ()>;

    fn edges_mut(&mut self) -> &mut DiGraphMap<NodeId, ()>;

    fn accept<V>(&self, visitor: &mut V)
    where
        V: NodeVisitor + ?Sized,
    {
        for (&node_id, node) in self.nodes() {
            node.accept(node_id, visitor);
        }
    }

    fn accept_mut<V>(&mut self, visitor: &mut V)
    where
        V: MutNodeVisitor + ?Sized,
    {
        for (&node_id, node) in self.nodes_mut() {
            node.accept_mut(node_id, visitor);
        }
    }

    fn function_builder(&self) -> FunctionBuilder {
        FunctionBuilder::new(self.layout_cache().clone())
    }

    fn source<L>(&mut self, layout: L, kind: SourceKind) -> NodeId
    where
        L: Into<StreamLayout>,
    {
        self.add_node(Source::new(layout.into(), kind, None))
    }

    fn named_source<L, N>(&mut self, layout: L, kind: SourceKind, name: N) -> NodeId
    where
        L: Into<StreamLayout>,
        N: Into<Box<str>>,
    {
        self.add_node(Source::new(layout.into(), kind, Some(name.into())))
    }

    fn sink<N>(&mut self, input: NodeId, view: N, input_layout: StreamLayout) -> NodeId
    where
        N: Into<Box<str>>,
    {
        self.add_node(Sink::new(input, view.into(), input_layout))
    }

    fn filter(&mut self, input: NodeId, filter_fn: Function) -> NodeId {
        self.add_node(Filter::new(input, filter_fn))
    }

    fn map(
        &mut self,
        input: NodeId,
        input_layout: StreamLayout,
        output_layout: StreamLayout,
        map_fn: Function,
    ) -> NodeId {
        self.add_node(Map::new(input, map_fn, input_layout, output_layout))
    }

    fn distinct(&mut self, input: NodeId, layout: StreamLayout) -> NodeId {
        self.add_node(Distinct::new(input, layout))
    }

    fn stream_distinct(&mut self, input: NodeId, layout: StreamLayout) -> NodeId {
        self.add_node(StreamDistinct::new(input, layout))
    }

    fn index_with(
        &mut self,
        input: NodeId,
        key_layout: LayoutId,
        value_layout: LayoutId,
        index_fn: Function,
    ) -> NodeId {
        self.add_node(IndexWith::new(input, index_fn, key_layout, value_layout))
    }

    fn differentiate(&mut self, input: NodeId, layout: StreamLayout) -> NodeId {
        self.add_node(Differentiate::new(input, layout))
    }

    fn integrate(&mut self, input: NodeId, layout: StreamLayout) -> NodeId {
        self.add_node(Integrate::new(input, layout))
    }

    fn join_core(
        &mut self,
        lhs: NodeId,
        rhs: NodeId,
        join_fn: Function,
        key_layout: LayoutId,
        value_layout: LayoutId,
        output_kind: StreamKind,
    ) -> NodeId {
        self.add_node(JoinCore::new(
            lhs,
            rhs,
            join_fn,
            key_layout,
            value_layout,
            output_kind,
        ))
    }

    fn empty_set(&mut self, key_layout: LayoutId) -> NodeId {
        self.empty_stream(StreamLayout::Set(key_layout))
    }

    fn empty_map(&mut self, key_layout: LayoutId, value_layout: LayoutId) -> NodeId {
        self.empty_stream(StreamLayout::Map(key_layout, value_layout))
    }

    // TODO: Make a dedicated empty stream node?
    fn empty_stream(&mut self, layout: StreamLayout) -> NodeId {
        self.add_node(ConstantStream::empty(layout))
    }

    fn stats(&self) -> GraphStats {
        self.stats_inner(0)
    }

    #[doc(hidden)]
    fn stats_inner(&self, subgraph_level: usize) -> GraphStats {
        let mut stats = GraphStats::new();
        stats.nodes = self.nodes().len();
        stats.max_subgraph_nesting = subgraph_level;

        for node in self.nodes().values() {
            match node {
                Node::Source(_) => stats.sources += 1,
                Node::Sink(_) => stats.sinks += 1,
                Node::JoinCore(_) | Node::MonotonicJoin(_) => stats.joins += 1,
                Node::Subgraph(subgraph) => {
                    stats.subgraphs += 1;
                    stats.combine(subgraph.stats_inner(subgraph_level + 1));
                }
                _ => {}
            }
        }

        stats
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphStats {
    pub nodes: usize,
    pub sources: usize,
    pub sinks: usize,
    pub joins: usize,
    pub subgraphs: usize,
    pub max_subgraph_nesting: usize,
}

impl Default for GraphStats {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphStats {
    pub fn new() -> Self {
        Self {
            nodes: 0,
            sources: 0,
            sinks: 0,
            joins: 0,
            subgraphs: 0,
            max_subgraph_nesting: 0,
        }
    }

    fn combine(self, other: Self) -> Self {
        Self {
            nodes: self.nodes + other.nodes,
            sources: self.sources + other.sources,
            sinks: self.sinks + other.sinks,
            joins: self.joins + other.joins,
            subgraphs: self.subgraphs + other.subgraphs,
            max_subgraph_nesting: self.max_subgraph_nesting.max(other.max_subgraph_nesting),
        }
    }
}
