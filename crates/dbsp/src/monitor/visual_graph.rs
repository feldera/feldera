//! Intermediate representation of a circuit graph suitable for
//! conversion to a visual format like dot.

use serde::{ser::SerializeStruct, Serialize};
use std::fmt::{self, Debug, Display, Write};

type Id = String;

/// Visual representation of a circuit graph.
///
/// The graph consists of a tree of cluster nodes populated with simple nodes.
#[derive(Clone, Default, Serialize)]
pub struct Graph {
    nodes: ClusterNode,
    edges: Vec<Edge>,
}

impl Debug for Graph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Graph {{ \"{}\" }}", self.to_dot())
    }
}

impl Graph {
    pub(super) fn new(nodes: ClusterNode, edges: Vec<Edge>) -> Self {
        Self { nodes, edges }
    }

    /// Convert graph to dot format.
    pub fn to_dot(&self) -> String {
        let mut output = String::with_capacity((self.nodes.nodes.len() + self.edges.len()) * 50);
        output.push_str("digraph {\nnode [shape=box]\n");

        self.nodes
            .to_dot(&mut output)
            .expect("writing to a string should never fail");

        for edge in self.edges.iter() {
            edge.to_dot(&mut output)
                .expect("writing to a string should never fail");
        }

        output.push_str("}\n");
        output
    }

    /// Convert graph to JSON format.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

impl Display for Graph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.nodes.to_plain(Indentation(0), f)
    }
}

#[derive(Clone)]
pub(super) struct SimpleNode {
    id: Id,
    label: String,
    color: f64,
}

impl Serialize for SimpleNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SimpleNode", 2)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("label", &self.label)?;
        state.end()
    }
}

impl SimpleNode {
    pub(super) fn new(id: Id, label: String, color: f64) -> Self {
        Self { id, label, color }
    }

    fn to_dot(&self, output: &mut dyn Write) -> fmt::Result {
        let r = 1.0 - self.color;
        let gb = (r * r * r * 255.0) as u8;

        writeln!(
            output,
            "{}[label=\"{}\" fillcolor=\"#ff{gb:02x}{gb:02x}\" style=filled]",
            self.id, self.label,
        )
    }
    fn to_plain(&self, indent: Indentation, f: &mut fmt::Formatter) -> fmt::Result {
        for line in self.label.split("\\l").filter(|line| !line.is_empty()) {
            writeln!(f, "{indent}{line}")?;
        }
        Ok(())
    }
}

/// A cluster node represents a subcircuit or a region.
// TODO:
// * Visually distinguish subcircuits from regions (e.g., dashed vs solid
//   boundaries).
#[derive(Clone, Default, Serialize)]
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
    fn to_dot(&self, output: &mut dyn Write) -> fmt::Result {
        writeln!(output, "subgraph cluster_{} {{", &self.id)?;
        writeln!(output, "label=\"{}\"", self.label)?;
        writeln!(output, "enter_{}[style=invis]", self.id)?;
        writeln!(output, "exit_{}[style=invis]", self.id)?;
        for node in self.nodes.iter() {
            node.to_dot(output)?;
        }
        writeln!(output, "}}")?;

        Ok(())
    }

    fn to_plain(&self, indent: Indentation, f: &mut fmt::Formatter) -> fmt::Result {
        for line in self.label.split("\\l").filter(|line| !line.is_empty()) {
            writeln!(f, "{indent}{line}")?;
        }
        for (index, node) in self.nodes.iter().enumerate() {
            if index > 0 {
                writeln!(f)?;
            }
            node.to_plain(Indentation(indent.0 + 4), f)?;
        }
        Ok(())
    }
}

#[derive(Clone, Serialize)]
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

    fn to_dot(&self, output: &mut dyn Write) -> fmt::Result {
        match self {
            Self::Simple(simple_node) => simple_node.to_dot(output),
            Self::Cluster(cluster_node) => cluster_node.to_dot(output),
        }
    }

    fn to_plain(&self, indent: Indentation, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Simple(simple_node) => simple_node.to_plain(indent, f),
            Self::Cluster(cluster_node) => cluster_node.to_plain(indent, f),
        }
    }
}

#[derive(Clone, Serialize)]
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

    fn to_dot(&self, output: &mut dyn Write) -> fmt::Result {
        if self.from_cluster {
            write!(output, "exit_")?;
        }
        write!(output, "{} -> ", self.from_node)?;

        if self.to_cluster {
            write!(output, "enter_")?;
        }
        writeln!(output, "{}", self.to_node)
    }
}

struct Indentation(usize);
impl Display for Indentation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for _ in 0..self.0 {
            f.write_char(' ')?;
        }
        Ok(())
    }
}
