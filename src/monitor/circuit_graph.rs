use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
    slice,
};

use super::visual_graph::{
    ClusterNode, Edge as VisEdge, Graph as VisGraph, Node as VisNode, SimpleNode,
};
use crate::circuit::{trace::EdgeKind, GlobalNodeId, NodeId};

/// A region is a named grouping of operators in a circuit.
///
/// Regions can be nested inside other regions, forming a tree.
/// A circuit is created with a single root region.
pub(super) struct Region {
    id: RegionId,
    pub(super) nodes: Vec<NodeId>,
    name: Cow<'static, str>,
    children: Vec<Region>,
}

/// Region id is a path from the root of the region tree.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RegionId(Vec<usize>);

impl RegionId {
    /// Root region.
    pub(super) fn root() -> Self {
        Self(Vec::new())
    }

    /// Pop the innermost child, transforming region id into its parent region
    /// id.
    pub(super) fn pop(&mut self) {
        self.0.pop();
    }

    pub(super) fn child(&self, child_id: usize) -> Self {
        let mut path = Vec::with_capacity(self.0.len() + 1);
        path.extend_from_slice(&self.0);
        path.push(child_id);
        Self(path)
    }
}

impl Region {
    pub(super) fn new(id: RegionId, name: Cow<'static, str>) -> Self {
        Self {
            id,
            nodes: Vec::new(),
            name,
            children: Vec::new(),
        }
    }

    /// Generate unique name for a region to use as a node label in a visual
    /// graph.
    fn region_identifier(node_id: &GlobalNodeId, region_id: &RegionId) -> String {
        let mut region_ident = format!(
            "{}{}",
            Node::node_identifier(node_id),
            if region_id.0.is_empty() { "" } else { "_r" }
        );

        for i in 0..region_id.0.len() {
            region_ident.push_str(&region_id.0[i].to_string());
            if i < region_id.0.len() - 1 {
                region_ident.push('_');
            }
        }
        region_ident
    }

    /// Output region as a cluster in a visual graph.
    fn visualize(&self, scope: &Node, annotate: &dyn Fn(&GlobalNodeId) -> String) -> ClusterNode {
        let mut nodes = Vec::new();
        for nodeid in self.nodes.iter() {
            if let Some(vnode) = scope
                .children()
                .unwrap()
                .get(nodeid)
                .unwrap()
                .visualize(annotate)
            {
                nodes.push(vnode)
            }
        }

        for child in self.children.iter() {
            nodes.push(VisNode::Cluster(child.visualize(scope, annotate)));
        }

        ClusterNode::new(
            Self::region_identifier(&scope.id, &self.id),
            self.name.to_string(),
            nodes,
        )
    }

    fn do_add_region(&mut self, path: &[usize], name: Cow<'static, str>) -> RegionId {
        match path.split_first() {
            None => {
                let new_region_id = self.id.child(self.children.len());
                self.children.push(Region::new(new_region_id.clone(), name));
                new_region_id
            }
            Some((id, ids)) => self.children[*id].do_add_region(ids, name),
        }
    }

    /// Add a subregion to `self`.
    ///
    /// * `self` - must be a root region.
    /// * `parent` - existing sub-region id.
    /// * `description` - name of a new region to add as child to `parent`.
    pub(super) fn add_region(&mut self, parent: &RegionId, name: Cow<'static, str>) -> RegionId {
        debug_assert_eq!(self.id, RegionId::root());

        self.do_add_region(parent.0.as_slice(), name)
    }

    fn do_get_region(&mut self, path: &[usize]) -> &mut Region {
        match path.split_first() {
            None => self,
            Some((id, ids)) => self.children[*id].do_get_region(ids),
        }
    }

    /// Get a mutable reference to a subregion of `self`.
    ///
    /// * `self` - must be a root region.
    /// * `region_id` - existing subregion id.
    pub(super) fn get_region(&mut self, region_id: &RegionId) -> &mut Region {
        debug_assert_eq!(self.id, RegionId::root());

        self.do_get_region(region_id.0.as_slice())
    }
}

pub(super) enum NodeKind {
    /// Regular operator.
    Operator,
    /// Root circuit or subcircuit.
    Circuit {
        iterative: bool,
        children: HashMap<NodeId, Node>,
        region: Region,
    },
    /// The input half of a strict operator.
    StrictInput { output: NodeId },
    /// The output half of a strict operator.
    StrictOutput,
}

/// A node in a circuit graph represents an operator or a circuit.
pub(super) struct Node {
    id: GlobalNodeId,
    pub name: String,
    #[allow(dead_code)]
    pub region_id: RegionId,
    pub kind: NodeKind,
}

impl Node {
    pub(super) fn new(id: GlobalNodeId, name: &str, region_id: RegionId, kind: NodeKind) -> Self {
        Self {
            id,
            name: name.to_string(),
            region_id,
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

    /// Returns a mutable reference to the root region of `self` if
    /// `self` is a circuit node.
    pub(super) fn region_mut(&mut self) -> Option<&mut Region> {
        if let NodeKind::Circuit { region, .. } = &mut self.kind {
            Some(region)
        } else {
            None
        }
    }

    /// Returns a reference to the root region of `self` if
    /// `self` is a circuit node.
    pub(super) fn region(&self) -> Option<&Region> {
        if let NodeKind::Circuit { region, .. } = &self.kind {
            Some(region)
        } else {
            None
        }
    }

    /// `true` if `self` is the input half of a strict operator.
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

    /// Generate unique name for the node to use as a node label in a visual
    /// graph.
    fn node_identifier(node_id: &GlobalNodeId) -> String {
        let mut node_ident = "n".to_string();

        for i in 0..node_id.path().len() {
            node_ident.push_str(&node_id.path()[i].to_string());
            if i < node_id.path().len() - 1 {
                node_ident.push('_');
            }
        }
        node_ident
    }

    /// Output circuit node as a node in a visual graph.
    fn visualize(&self, annotate: &dyn Fn(&GlobalNodeId) -> String) -> Option<VisNode> {
        match &self.kind {
            NodeKind::Operator => {
                let annotation = annotate(&self.id);
                Some(VisNode::Simple(SimpleNode::new(
                    Self::node_identifier(&self.id),
                    format!(
                        "{}{}{}",
                        self.name,
                        if annotation.is_empty() { "" } else { "\\l" },
                        annotation
                    ),
                )))
            }
            NodeKind::Circuit { region, .. } => {
                Some(VisNode::Cluster(region.visualize(self, annotate)))
            }
            NodeKind::StrictInput { output } => {
                let annotation = annotate(&self.id);
                Some(VisNode::Simple(SimpleNode::new(
                    Self::node_identifier(&self.id.parent_id().unwrap().child(*output)),
                    format!(
                        "{}{}{}",
                        self.name,
                        if annotation.is_empty() { "" } else { "\\l" },
                        annotation
                    ),
                )))
            }
            NodeKind::StrictOutput => None,
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
                GlobalNodeId::root(),
                "root",
                RegionId::root(),
                NodeKind::Circuit {
                    iterative: true,
                    children: HashMap::new(),
                    region: Region::new(RegionId::root(), Cow::Borrowed("root")),
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

    /// Output circuit graph as visual graph.
    pub(super) fn visualize(&self, annotate: &dyn Fn(&GlobalNodeId) -> String) -> VisGraph {
        let cluster = self
            .nodes
            .region()
            .unwrap()
            .visualize(&self.nodes, annotate);
        let mut edges = Vec::new();

        for (from_id, to) in self.edges.iter() {
            let from_node = self.node_ref(from_id).unwrap();

            for (to_id, kind) in to.iter() {
                let to_node = self.node_ref(to_id).unwrap();
                let to_id = match to_node.kind {
                    NodeKind::StrictInput { output } => to_id.parent_id().unwrap().child(output),
                    _ => to_id.clone(),
                };

                if kind.is_stream() {
                    edges.push(VisEdge::new(
                        Node::node_identifier(from_id),
                        from_node.is_circuit(),
                        Node::node_identifier(&to_id),
                        to_node.is_circuit(),
                    ));
                }
            }
        }

        VisGraph::new(cluster, edges)
    }
}
