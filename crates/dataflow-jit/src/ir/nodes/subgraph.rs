use crate::ir::{
    function::Function,
    graph::{self, GraphExt},
    layout_cache::RowLayoutCache,
    types::Signature,
    DataflowNode, DelayedFeedback, Delta0, Export, LayoutId, Node, NodeId, Stream, StreamKind,
};
use petgraph::prelude::DiGraphMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Subgraph {
    // A map of external nodes to their subgraph import nodes
    inputs: BTreeMap<NodeId, NodeId>,
    subgraph: graph::Subgraph,
    // A map of subgraph nodes to their export nodes
    outputs: BTreeMap<NodeId, NodeId>,
    feedback: BTreeSet<NodeId>,
    // Connections from streams to a feedback node within `feedback`
    feedback_connections: BTreeMap<NodeId, NodeId>,
}

impl Subgraph {
    pub fn new(subgraph: graph::Subgraph) -> Self {
        Self {
            inputs: BTreeMap::new(),
            subgraph,
            outputs: BTreeMap::new(),
            feedback: BTreeSet::new(),
            feedback_connections: BTreeMap::new(),
        }
    }

    pub fn subgraph(&self) -> &graph::Subgraph {
        &self.subgraph
    }

    pub fn delta0(&mut self, import: NodeId) -> NodeId {
        let delta0 = self.add_node(Delta0::new(import));
        self.inputs.insert(import, delta0);
        delta0
    }

    pub fn export(&mut self, output: NodeId, layout: Stream) -> NodeId {
        let export = self.add_node(Export::new(output, layout));
        self.outputs.insert(output, export);
        export
    }

    pub fn delayed_feedback(&mut self, layout: LayoutId) -> NodeId {
        let delay = self.add_node(DelayedFeedback::new(layout));
        self.feedback.insert(delay);
        delay
    }

    pub fn connect_feedback(&mut self, from: NodeId, feedback: NodeId) {
        debug_assert!(self.feedback.contains(&feedback));
        self.feedback_connections.insert(from, feedback);
    }

    pub(crate) fn input_nodes(&self) -> &BTreeMap<NodeId, NodeId> {
        &self.inputs
    }

    pub(crate) fn output_nodes(&self) -> &BTreeMap<NodeId, NodeId> {
        &self.outputs
    }

    pub(crate) fn feedback_connections(&self) -> &BTreeMap<NodeId, NodeId> {
        &self.feedback_connections
    }
}

impl GraphExt for Subgraph {
    fn layout_cache(&self) -> &RowLayoutCache {
        self.subgraph.layout_cache()
    }

    fn nodes(&self) -> &BTreeMap<NodeId, Node> {
        self.subgraph.nodes()
    }

    fn nodes_mut(&mut self) -> &mut BTreeMap<NodeId, Node> {
        self.subgraph.nodes_mut()
    }

    fn next_node(&self) -> NodeId {
        self.subgraph.next_node()
    }

    fn create_node<N>(&mut self, node_id: NodeId, node: N) -> NodeId
    where
        N: Into<Node>,
    {
        self.subgraph.create_node(node_id, node)
    }

    fn subgraph<F, T>(&mut self, build: F) -> (NodeId, T)
    where
        F: FnOnce(&mut Subgraph) -> T,
    {
        self.subgraph.subgraph(build)
    }

    fn optimize(&mut self) {
        self.subgraph.optimize();
    }

    fn edges(&self) -> &DiGraphMap<NodeId, ()> {
        self.subgraph.edges()
    }
}

impl DataflowNode for Subgraph {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.extend(self.inputs.keys().copied());
    }

    fn output_kind(&self, _inputs: &[Stream]) -> Option<StreamKind> {
        None
    }

    fn output_stream(&self, _inputs: &[Stream]) -> Option<Stream> {
        None
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {}

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        self.subgraph.functions(functions);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        self.subgraph.layouts(layouts);
    }
}
