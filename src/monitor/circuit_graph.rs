use std::{
    collections::{hash_map::Entry, HashMap},
    slice,
};

use crate::circuit::{trace::EdgeKind, GlobalNodeId, NodeId};

pub(super) enum NodeKind {
    /// Regular operator.
    Operator,
    /// Root circuit or subcircuit.
    Circuit {
        iterative: bool,
        children: HashMap<NodeId, Node>,
    },
    /// The input half of a strict operator.
    StrictInput { output: NodeId },
}

/// A node in a circuit graph represents an operator or a circuit.
pub(super) struct Node {
    #[allow(dead_code)]
    pub name: String,
    pub kind: NodeKind,
}

impl Node {
    pub(super) fn new(name: &str, kind: NodeKind) -> Self {
        Self {
            name: name.to_string(),
            kind,
        }
    }

    /// Lookup node in the subtree with the root in `self` by path.
    fn node_ref<'a>(&self, mut path: slice::Iter<'a, NodeId>) -> Option<&Node> {
        match path.next() {
            None => Some(self),
            Some(node_id) => match &self.kind {
                NodeKind::Circuit { children, .. } => children.get(node_id)?.node_ref(path),
                _ => None,
            },
        }
    }

    /// Lookup node in the subtree with the root in `self` by path.
    fn node_mut<'a>(&mut self, mut path: slice::Iter<'a, NodeId>) -> Option<&mut Node> {
        match path.next() {
            None => Some(self),
            Some(node_id) => match &mut self.kind {
                NodeKind::Circuit { children, .. } => children.get_mut(node_id)?.node_mut(path),
                _ => None,
            },
        }
    }

    /// `true` if `self` is a circuit node.
    pub(super) fn is_circuit(&self) -> bool {
        matches!(self.kind, NodeKind::Circuit { .. })
    }

    /// `true` if `self` is an iterative circuit (including the root
    /// circuit).
    pub(super) fn is_iterative(&self) -> bool {
        matches!(
            self.kind,
            NodeKind::Circuit {
                iterative: true,
                ..
            }
        )
    }

    /// Returns children of `self` if `self` is a circuit.
    pub(super) fn children(&self) -> Option<&HashMap<NodeId, Node>> {
        if let NodeKind::Circuit { children, .. } = &self.kind {
            Some(children)
        } else {
            None
        }
    }

    /// `true` if `self` is the input half of a circuit operaor.
    pub(super) fn is_strict_input(&self) -> bool {
        matches!(self.kind, NodeKind::StrictInput { .. })
    }

    /// Returns `self.output` if `self` is a strict input operator.
    pub(super) fn output_id(&self) -> Option<NodeId> {
        if let NodeKind::StrictInput { output } = &self.kind {
            Some(*output)
        } else {
            None
        }
    }
}

pub(super) struct CircuitGraph {
    /// Tree of nodes.
    nodes: Node,
    /// Matches a node to the vector of nodes that read from its output
    /// stream or have a dependency on it.
    /// A node can occur in this vector multiple times.
    edges: HashMap<GlobalNodeId, Vec<(GlobalNodeId, EdgeKind)>>,
}

impl CircuitGraph {
    pub(super) fn new() -> Self {
        Self {
            nodes: Node::new(
                "root",
                NodeKind::Circuit {
                    iterative: true,
                    children: HashMap::new(),
                },
            ),
            edges: HashMap::new(),
        }
    }

    /// Locate node by its global id.
    pub(super) fn node_ref(&self, id: &GlobalNodeId) -> Option<&Node> {
        self.nodes.node_ref(id.path().iter())
    }

    /// Locate node by its global id.
    pub(super) fn node_mut(&mut self, id: &GlobalNodeId) -> Option<&mut Node> {
        self.nodes.node_mut(id.path().iter())
    }

    pub(super) fn add_edge(&mut self, from: &GlobalNodeId, to: &GlobalNodeId, kind: &EdgeKind) {
        match self.edges.entry(from.clone()) {
            Entry::Occupied(mut oe) => {
                oe.get_mut().push((to.clone(), kind.clone()));
            }
            Entry::Vacant(ve) => {
                ve.insert(vec![(to.clone(), kind.clone())]);
            }
        }
    }
}
