//! Intermediate representation of a circuit graph suitable for
//! conversion to a visual format like dot.

type Id = String;

/// Visual representation of a circuit graph.
///
/// The graph consists of a tree of cluster nodes populated with simple nodes.
pub struct Graph {
    nodes: ClusterNode,
    edges: Vec<Edge>,
}

impl Graph {
    pub(super) fn new(nodes: ClusterNode, edges: Vec<Edge>) -> Self {
        Self { nodes, edges }
    }

    /// Convert graph to dot format.
    pub fn to_dot(&self) -> String {
        let mut lines: Vec<String> = vec!["digraph {".to_string(), "node [shape=box]".to_string()];
        lines.append(&mut self.nodes.to_dot());
        for edge in self.edges.iter() {
            lines.push(edge.to_dot());
        }
        lines.push("}".to_string());
        lines.join("\n")
    }
}

pub(super) struct SimpleNode {
    id: Id,
    label: String,
}

impl SimpleNode {
    pub(super) fn new(id: Id, label: String) -> Self {
        Self { id, label }
    }

    fn to_dot(&self) -> Vec<String> {
        vec![format!("{}[label=\"{}\"]", self.id, self.label)]
    }
}

/// A cluster node represents a subcircuit or a region.
// TODO:
// * Visually distinguish subcircuits from regions (e.g., dashed vs solid
//   boundaries).
pub(super) struct ClusterNode {
    id: Id,
    label: String,
    nodes: Vec<Node>,
}

impl ClusterNode {
    pub(super) fn new(id: Id, label: String, nodes: Vec<Node>) -> Self {
        Self { id, label, nodes }
    }

    // TODO: We add a pair of enter/exit nodes to each cluster and connect all
    // incoming/outgoing edges whose destination/source is the entire cluster to
    // these nodes.  This does not look great.  We should instead connect each
    // edge to a specific simple node, which requires extending the circuit
    // builder API.
    fn to_dot(&self) -> Vec<String> {
        let mut lines: Vec<String> = Vec::new();
        lines.push(format!("subgraph cluster_{} {{", &self.id));
        lines.push(format!("label=\"{}\"", self.label));
        lines.push(format!("enter_{}[style=invis]", self.id));
        lines.push(format!("exit_{}[style=invis]", self.id));
        for node in self.nodes.iter() {
            lines.append(&mut node.to_dot());
        }
        lines.push("}".to_string());
        lines
    }
}

pub(super) enum Node {
    Simple(SimpleNode),
    Cluster(ClusterNode),
}

impl Node {
    fn to_dot(&self) -> Vec<String> {
        match self {
            Self::Simple(simple_node) => simple_node.to_dot(),
            Self::Cluster(cluster_node) => cluster_node.to_dot(),
        }
    }
}

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

    fn to_dot(&self) -> String {
        let start = if self.from_cluster {
            format!("exit_{}", self.from_node)
        } else {
            self.from_node.clone()
        };
        let end = if self.to_cluster {
            format!("enter_{}", self.to_node)
        } else {
            self.to_node.clone()
        };
        format!("{} -> {}", start, end)
    }
}
