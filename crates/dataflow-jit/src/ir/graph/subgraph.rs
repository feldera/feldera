use crate::ir::{
    graph::{GraphContext, GraphExt},
    layout_cache::RowLayoutCache,
    nodes::{DataflowNode, ExportedNode, Node, Subgraph as SubgraphNode},
    NodeId,
};
use petgraph::prelude::DiGraphMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[serde_with::serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Subgraph {
    #[serde(skip)]
    edges: DiGraphMap<NodeId, ()>,
    #[serde_as(as = "BTreeMap<serde_with::DisplayFromStr, _>")]
    nodes: BTreeMap<NodeId, Node>,
    #[serde(skip)]
    ctx: GraphContext,
}

impl Subgraph {
    pub(crate) fn new(ctx: GraphContext) -> Self {
        Self {
            edges: DiGraphMap::new(),
            nodes: BTreeMap::new(),
            ctx,
        }
    }

    pub(crate) fn set_context(&mut self, context: GraphContext) {
        self.ctx = context;
    }

    pub(crate) fn set_edges(&mut self, edges: DiGraphMap<NodeId, ()>) {
        self.edges = edges;
    }
}

impl GraphExt for Subgraph {
    fn layout_cache(&self) -> &RowLayoutCache {
        &self.ctx.layout_cache
    }

    fn nodes(&self) -> &BTreeMap<NodeId, Node> {
        &self.nodes
    }

    fn nodes_mut(&mut self) -> &mut BTreeMap<NodeId, Node> {
        &mut self.nodes
    }

    fn next_node(&self) -> NodeId {
        self.ctx.node_id.next()
    }

    fn create_node<N>(&mut self, node_id: NodeId, node: N) -> NodeId
    where
        N: Into<Node>,
    {
        let node = node.into();

        let mut inputs = Vec::new();
        node.inputs(&mut inputs);

        self.edges.add_node(node_id);
        for input in inputs {
            self.edges.add_edge(input, node_id, ());
        }

        self.nodes.insert(node_id, node);

        node_id
    }

    fn subgraph<F, T>(&mut self, build: F) -> (NodeId, T)
    where
        F: FnOnce(&mut SubgraphNode) -> T,
    {
        let mut subgraph = SubgraphNode::new(Subgraph::new(self.ctx.clone()));
        let subgraph_id = self.next_node();

        let result = build(&mut subgraph);

        // Add all exports to the containing graph
        // FIXME: Remove this clone
        for (&input, &exported) in subgraph.output_nodes() {
            let export = subgraph.nodes()[&exported].clone().unwrap_export();
            self.create_node(
                exported,
                ExportedNode::new(subgraph_id, input, export.layout()),
            );
        }

        (self.create_node(subgraph_id, subgraph), result)
    }

    fn optimize(&mut self) {
        // TODO: Validate before and after optimizing
        for node in self.nodes.values_mut() {
            node.optimize(&self.ctx.layout_cache);
        }
    }

    fn edges(&self) -> &DiGraphMap<NodeId, ()> {
        &self.edges
    }

    fn edges_mut(&mut self) -> &mut DiGraphMap<NodeId, ()> {
        &mut self.edges
    }
}

impl JsonSchema for Subgraph {
    fn schema_name() -> String {
        "Subgraph".to_owned()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema_object = schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::Object.into()),
            ..Default::default()
        };

        let object_validation = schema_object.object();
        object_validation.properties.insert(
            "nodes".to_owned(),
            gen.subschema_for::<BTreeMap<NodeId, Node>>(),
        );
        object_validation.required.insert("nodes".to_owned());

        schemars::schema::Schema::Object(schema_object)
    }
}
