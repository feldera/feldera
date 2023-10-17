//! Contains the dataflow graph that users construct

// TODO: Changing things to operate around a port-oriented design should
// simplify rerouting edges and removing nodes

mod graph_ext;
mod subgraph;
mod tests;

pub use graph_ext::GraphExt;
pub use subgraph::Subgraph;

use crate::ir::{
    layout_cache::RowLayoutCache,
    nodes::{Node, StreamLayout, Subgraph as SubgraphNode},
    optimize,
    pretty::{DocAllocator, DocBuilder, Pretty},
    NodeId, NodeIdGen,
};
use petgraph::prelude::DiGraphMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, rc::Rc};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct Graph {
    graph: Subgraph,
}

impl Graph {
    pub fn new() -> Self {
        Self {
            graph: Subgraph::new(GraphContext::new()),
        }
    }

    pub const fn graph(&self) -> &Subgraph {
        &self.graph
    }

    pub fn graph_mut(&mut self) -> &mut Subgraph {
        &mut self.graph
    }

    /// Returns the node ids of every [`Node::Source`]
    /// within the graph
    pub fn source_nodes(&self) -> Vec<(NodeId, StreamLayout)> {
        self.nodes()
            .iter()
            .filter_map(|(&node_id, node)| {
                node.as_source()
                    .map(|source| (node_id, source.output_layout()))
            })
            .collect()
    }

    /// Returns the node ids of every [`Node::Sink`] within the graph
    pub fn sink_nodes(&self) -> Vec<(NodeId, StreamLayout)> {
        self.nodes()
            .iter()
            .filter_map(|(&node_id, node)| {
                node.as_sink().map(|sink| (node_id, sink.input_layout()))
            })
            .collect()
    }
}

impl GraphExt for Graph {
    fn layout_cache(&self) -> &RowLayoutCache {
        self.graph.layout_cache()
    }

    fn nodes(&self) -> &BTreeMap<NodeId, Node> {
        self.graph.nodes()
    }

    fn nodes_mut(&mut self) -> &mut BTreeMap<NodeId, Node> {
        self.graph.nodes_mut()
    }

    fn next_node(&self) -> NodeId {
        self.graph.next_node()
    }

    fn create_node<N>(&mut self, node_id: NodeId, node: N) -> NodeId
    where
        N: Into<Node>,
    {
        self.graph.create_node(node_id, node)
    }

    fn subgraph<F, T>(&mut self, build: F) -> (NodeId, T)
    where
        F: FnOnce(&mut SubgraphNode) -> T,
    {
        self.graph.subgraph(build)
    }

    fn optimize(&mut self) {
        optimize::optimize_graph(self);
    }

    fn edges(&self) -> &DiGraphMap<NodeId, ()> {
        self.graph.edges()
    }

    fn edges_mut(&mut self) -> &mut DiGraphMap<NodeId, ()> {
        self.graph.edges_mut()
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &Graph
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized + 'a,
    DocBuilder<'a, D, A>: Clone,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc.intersperse(
            self.nodes().iter().map(|(node_id, node)| {
                node_id
                    .pretty(alloc, cache)
                    .append(alloc.space())
                    .append(alloc.text("="))
                    .append(alloc.space())
                    .append(node.pretty(alloc, cache))
            }),
            alloc.hardline().append(alloc.hardline()),
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GraphContext {
    layout_cache: RowLayoutCache,
    node_id: Rc<NodeIdGen>,
}

impl GraphContext {
    pub(crate) fn new() -> Self {
        Self {
            layout_cache: RowLayoutCache::new(),
            node_id: Rc::new(NodeIdGen::new()),
        }
    }

    pub(crate) fn from_parts(layout_cache: RowLayoutCache, node_id: NodeIdGen) -> Self {
        Self {
            layout_cache,
            node_id: Rc::new(node_id),
        }
    }
}

impl Default for GraphContext {
    fn default() -> Self {
        Self::new()
    }
}
