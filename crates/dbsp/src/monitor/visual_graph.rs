//! Intermediate representation of a circuit graph, serialized as JSON in
//! circuit profiles.

use serde::Serialize;

type Id = String;

/// Visual representation of a circuit graph.
///
/// The graph consists of a tree of cluster nodes populated with simple nodes.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Graph {
    nodes: ClusterNode,
    edges: Vec<Edge>,
}

impl Graph {
    pub(super) fn new(nodes: ClusterNode, edges: Vec<Edge>) -> Self {
        Self { nodes, edges }
    }

    /// Convert graph to JSON format.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct SimpleNode {
    id: Id,
    label: String,
}

impl SimpleNode {
    pub(super) fn new(id: Id, label: String) -> Self {
        Self { id, label }
    }
}

/// A cluster node represents a subcircuit or a region.
#[derive(Clone, Debug, Default, Serialize)]
pub(super) struct ClusterNode {
    id: Id,
    label: String,
    nodes: Vec<Node>,
}

impl ClusterNode {
    pub(super) fn new(id: Id, label: String, nodes: Vec<Node>) -> Self {
        Self { id, label, nodes }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(super) enum Node {
    Simple(SimpleNode),
    Cluster(ClusterNode),
}

impl Node {
    pub(super) fn cluster(self) -> Option<ClusterNode> {
        match self {
            Self::Simple(_) => None,
            Self::Cluster(cluster_node) => Some(cluster_node),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct Edge {
    from_node: Id,
    // Is `from_node` a cluster?
    from_cluster: bool,
    to_node: Id,
    // Is `to_node` a cluster?
    to_cluster: bool,
}

impl Edge {
    pub(super) fn new(from_node: Id, from_cluster: bool, to_node: Id, to_cluster: bool) -> Self {
        Self {
            from_node,
            from_cluster,
            to_node,
            to_cluster,
        }
    }
}
