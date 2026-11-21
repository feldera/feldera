//! Partitions the streams that the balancer manages into clusters, and the
//! streams of each cluster into layers.

use crate::circuit::NodeId;
use petgraph::{
    algo::kosaraju_scc,
    graph::{DiGraph, EdgeReference, NodeIndex},
    visit::EdgeRef,
};
use std::collections::BTreeMap;

/// A join that the balancer manages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BalancedJoin {
    /// The left input, named by the node that writes it.
    pub left: NodeId,

    /// The right input, named by the node that writes it.
    pub right: NodeId,

    /// Whether the join is a left join.
    pub is_left_join: bool,
}

/// The streams and joins of one cluster, and the layer of each stream.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ClusterLayout {
    /// Map from each stream to its layer.
    pub streams: BTreeMap<NodeId, usize>,

    /// Map from the node id of each join to the join.
    pub joins: BTreeMap<NodeId, BalancedJoin>,

    /// Map from each layer to its streams, in ascending order.
    pub layers: BTreeMap<usize, Vec<NodeId>>,
}

/// The kind of an edge in the graph whose strongly connected components are
/// the clusters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EdgeKind {
    /// A stream or dependency edge of the circuit.
    Dataflow,

    /// An edge between two streams connected by a join.  Each join adds one in
    /// each direction.
    Join,
}

/// Partitions `streams` into clusters and the streams of each cluster into
/// layers.
///
/// A stream is named by the node that writes it, as
/// [`Stream::local_node_id`](crate::Stream::local_node_id) names it, so each
/// stream is a node of the circuit's dataflow graph.  Two streams belong to the
/// same cluster if they lie in the same strongly connected component of that
/// graph after an edge in both directions is added between every two streams
/// connected by a join.  Streams connected by a join therefore share a cluster.
/// So do the streams of two clusters that each depend on the other, directly or
/// through other clusters.  A cluster depends on another if a dataflow path
/// leads from a stream of the other cluster to one of its own streams.  The
/// component of a cluster also holds every node on a dataflow path between two
/// of its streams, so a stream's layer, the number of streams in the longest
/// chain of streams of its cluster that precede it in the dataflow, follows
/// from the dataflow edges inside the component.  A node of the component that
/// writes none of `streams` gets no layer.  Takes time and memory linear in the
/// size of the circuit.
///
/// # Arguments
///
/// * `num_nodes` - The number of nodes in the circuit, whose node ids are
///   `0..num_nodes`.
/// * `edges` - The circuit's stream and dependency edges, as (source,
///   destination) pairs.  They must form an acyclic graph.
/// * `streams` - The streams that the balancer manages, each named by the
///   node that writes it.
/// * `joins` - Map from the node id of each join to the join.  Both inputs of
///   every join must be in `streams`.
///
/// # Returns
///
/// The clusters, in ascending order of their smallest stream.
pub(super) fn compute_clusters(
    num_nodes: usize,
    edges: impl IntoIterator<Item = (NodeId, NodeId)>,
    streams: impl IntoIterator<Item = NodeId>,
    joins: &BTreeMap<NodeId, BalancedJoin>,
) -> Vec<ClusterLayout> {
    let mut streams = streams.into_iter().peekable();
    if streams.peek().is_none() {
        return Vec::new();
    }
    let mut writes_stream = vec![false; num_nodes];
    for stream in streams {
        writes_stream[stream.id()] = true;
    }

    let graph = cluster_graph(num_nodes, edges, joins);
    let (component, num_components) = strongly_connected_components(&graph);
    let depth = depths(&graph, &component, &writes_stream);

    let mut cluster_of_component: Vec<Option<usize>> = vec![None; num_components];
    let mut clusters: Vec<ClusterLayout> = Vec::new();
    for node in (0..num_nodes).filter(|&node| writes_stream[node]) {
        let cluster = *cluster_of_component[component[node]].get_or_insert_with(|| {
            clusters.push(ClusterLayout::default());
            clusters.len() - 1
        });
        let stream = NodeId::new(node);
        let layer = depth[node];
        clusters[cluster].streams.insert(stream, layer);
        clusters[cluster]
            .layers
            .entry(layer)
            .or_default()
            .push(stream);
    }

    for (&join, &balanced_join) in joins {
        let BalancedJoin { left, right, .. } = balanced_join;
        assert!(
            writes_stream[left.id()] && writes_stream[right.id()],
            "join {join} reads a stream that the balancer does not manage"
        );
        let cluster = cluster_of_component[component[left.id()]].unwrap();
        clusters[cluster].joins.insert(join, balanced_join);
    }

    clusters
}

/// Builds the circuit's dataflow graph and adds an edge in both directions
/// between every two streams connected by a join.
///
/// # Arguments
///
/// * `num_nodes` - The number of nodes in the circuit.
/// * `edges` - The circuit's stream and dependency edges.
/// * `joins` - Map from the node id of each join to its inputs.
///
/// # Returns
///
/// A graph whose node with index `i` is the circuit node with id `i`.
fn cluster_graph(
    num_nodes: usize,
    edges: impl IntoIterator<Item = (NodeId, NodeId)>,
    joins: &BTreeMap<NodeId, BalancedJoin>,
) -> DiGraph<(), EdgeKind> {
    let index = |node: NodeId| NodeIndex::new(node.id());

    let mut graph = DiGraph::with_capacity(num_nodes, num_nodes + 2 * joins.len());
    for _ in 0..num_nodes {
        graph.add_node(());
    }
    for (from, to) in edges {
        graph.add_edge(index(from), index(to), EdgeKind::Dataflow);
    }
    for &BalancedJoin { left, right, .. } in joins.values() {
        graph.add_edge(index(left), index(right), EdgeKind::Join);
        graph.add_edge(index(right), index(left), EdgeKind::Join);
    }
    graph
}

/// Numbers the strongly connected components of `graph`.
///
/// # Arguments
///
/// * `graph` - The graph to partition.
///
/// # Returns
///
/// The component of each node, by node index, and the number of components.
fn strongly_connected_components(graph: &DiGraph<(), EdgeKind>) -> (Vec<usize>, usize) {
    // Unlike `petgraph::algo::tarjan_scc`, `kosaraju_scc` does not recurse, so
    // a deep circuit cannot overflow the stack.
    let components = kosaraju_scc(graph);
    let mut component = vec![0; graph.node_count()];
    for (index, nodes) in components.iter().enumerate() {
        for node in nodes {
            component[node.index()] = index;
        }
    }
    (component, components.len())
}

/// Computes the depth of every node.
///
/// The depth of a node is the largest number of nodes that write a stream the
/// balancer manages, not counting the node itself, on one dataflow path that
/// ends at the node and stays inside its component.  A stream's layer is the
/// depth of the node that writes it.  A node that writes no stream the balancer
/// manages gets no layer: its depth only carries the count along the dataflow
/// paths through it.  Every chain of streams of a cluster runs along dataflow
/// edges inside its component, so this visits the nodes in topological order of
/// those edges (Kahn's algorithm).
///
/// # Examples
///
/// In the two circuits below, A, B, C, and D write streams that the balancer
/// manages, and J1, J2, and J3 are joins; the exchanges and integrals in front
/// of the joins are left out.  Each box gives an operator and its depth.
/// Solid edges are dataflow edges inside a component, labeled with the count
/// they carry: the depth of their source, plus one if the source writes a
/// stream.  Dotted edges leave a component, and the walk ignores them.
///
/// In the first circuit, J1 joins the streams of A and B, J2 those of C and A,
/// and J3 those of D and B.  C and D lie downstream of A and B, so J2 and J3
/// close cycles that tie A, B, C, and D into one component, which also holds
/// J1 and J2, the operators on the dataflow paths between them.  J3 lies
/// outside it.  J2 takes the larger of the counts on its inputs, 2 from C
/// rather than 1 from A.  The streams of A and B get layer 0, the stream of C
/// layer 1, and the stream of D layer 2.
///
/// ```text
/// ┌───────┐             ┌───────┐
/// │  A: 0 │             │  B: 0 │
/// └─┬───┬─┘             └─┬───┬─┘
///   │   │ 1             1 │   ┆
///   │   └────┐   ┌────────┘   ┆
///   │        ▼   ▼            ┆
///   │      ┌───────┐          ┆
///   │      │ J1: 1 │          ┆
///   │      └───┬───┘          ┆
///   │          │ 1            ┆
///   │          ▼              ┆
///   │      ┌───────┐          ┆
///   │      │  C: 1 │          ┆
///   │      └───┬───┘          ┆
///   │          │ 2            ┆
///   │          ▼              ┆
///   │ 1    ┌───────┐          ┆
///   └─────►│ J2: 2 │          ┆
///          └───┬───┘          ┆
///              │ 2            ┆
///              ▼              ┆
///          ┌───────┐          ┆
///          │  D: 2 │          ┆
///          └───┬───┘          ┆
///              ┆              ┆
///              ▼              ┆
///          ┌───────┐          ┆
///          │ J3: 0 │◄┄┄┄┄┄┄┄┄┄┘
///          └───────┘
/// ```
///
/// In the second circuit, J1 joins the streams of A and B, J2 those of C and
/// D, and C reads the output of J1.  Nothing leads from C or D back to A or B,
/// so the joins form two clusters, and every edge leaves a component.  All
/// four streams get layer 0, though the stream of C lies downstream of those
/// of A and B.
///
/// ```text
/// ┌───────┐       ┌───────┐
/// │  A: 0 │       │  B: 0 │
/// └───┬───┘       └───┬───┘
///     ┆               ┆
///     └┄┄┄┄┄┐   ┌┄┄┄┄┄┘
///           ▼   ▼
///         ┌───────┐
///         │ J1: 0 │
///         └───┬───┘
///             ┆
///             ▼
///         ┌───────┐     ┌───────┐
///         │  C: 0 │     │  D: 0 │
///         └───┬───┘     └───┬───┘
///             ┆             ┆
///             └┄┄┄┄┐   ┌┄┄┄┄┘
///                  ▼   ▼
///                ┌───────┐
///                │ J2: 0 │
///                └───────┘
/// ```
///
/// # Arguments
///
/// * `graph` - The graph built by [`cluster_graph`].
/// * `component` - The strongly connected component of each node.
/// * `writes_stream` - Whether each node writes a stream that the balancer
///   manages.
///
/// # Returns
///
/// The depth of each node, by node index.
fn depths(
    graph: &DiGraph<(), EdgeKind>,
    component: &[usize],
    writes_stream: &[bool],
) -> Vec<usize> {
    let is_internal_dataflow = |edge: &EdgeReference<'_, EdgeKind>| {
        *edge.weight() == EdgeKind::Dataflow
            && component[edge.source().index()] == component[edge.target().index()]
    };

    let mut unvisited_inputs = vec![0usize; graph.node_count()];
    for edge in graph.edge_references().filter(is_internal_dataflow) {
        unvisited_inputs[edge.target().index()] += 1;
    }

    let mut depth = vec![0; graph.node_count()];
    let mut ready: Vec<NodeIndex> = graph
        .node_indices()
        .filter(|node| unvisited_inputs[node.index()] == 0)
        .collect();
    let mut num_visited = 0;
    while let Some(node) = ready.pop() {
        num_visited += 1;
        let successor_depth = depth[node.index()] + usize::from(writes_stream[node.index()]);
        for edge in graph.edges(node).filter(is_internal_dataflow) {
            let successor = edge.target();
            depth[successor.index()] = depth[successor.index()].max(successor_depth);
            unvisited_inputs[successor.index()] -= 1;
            if unvisited_inputs[successor.index()] == 0 {
                ready.push(successor);
            }
        }
    }
    assert_eq!(
        num_visited,
        graph.node_count(),
        "the circuit's dataflow graph has a cycle"
    );

    depth
}

#[cfg(test)]
mod tests {
    use super::{
        BalancedJoin, ClusterLayout, cluster_graph, compute_clusters, depths,
        strongly_connected_components,
    };
    use crate::circuit::NodeId;
    use petgraph::{algo::kosaraju_scc, graph::DiGraph, unionfind::UnionFind};
    use proptest::{collection::vec, prelude::*, sample::Index};
    use std::collections::{BTreeMap, BTreeSet};

    /// Map from the node id of each join to the join.
    type Joins = BTreeMap<NodeId, BalancedJoin>;

    /// An acyclic circuit and the streams and joins that the balancer manages
    /// in it.
    #[derive(Clone, Debug)]
    struct TestCircuit {
        num_nodes: usize,
        edges: Vec<(NodeId, NodeId)>,
        streams: BTreeSet<NodeId>,
        joins: Joins,
    }

    impl TestCircuit {
        /// Adds a node and returns its id.
        fn add_node(&mut self) -> NodeId {
            let node = NodeId::new(self.num_nodes);
            self.num_nodes += 1;
            node
        }

        fn clusters(&self) -> Vec<ClusterLayout> {
            compute_clusters(
                self.num_nodes,
                self.edges.iter().copied(),
                self.streams.iter().copied(),
                &self.joins,
            )
        }

        /// Computes the depth of every node, by node index, with [`depths`].
        fn node_depths(&self) -> Vec<usize> {
            let graph = cluster_graph(self.num_nodes, self.edges.iter().copied(), &self.joins);
            let (component, _) = strongly_connected_components(&graph);
            let writes_stream: Vec<bool> = (0..self.num_nodes)
                .map(|node| self.streams.contains(&NodeId::new(node)))
                .collect();
            depths(&graph, &component, &writes_stream)
        }

        /// Computes the transitive ancestors of every node, by node index.
        fn ancestors(&self) -> Vec<BTreeSet<NodeId>> {
            let mut inputs = vec![Vec::new(); self.num_nodes];
            for &(from, to) in &self.edges {
                inputs[to.id()].push(from);
            }
            (0..self.num_nodes)
                .map(|node| {
                    let mut ancestors = BTreeSet::new();
                    let mut queue = vec![NodeId::new(node)];
                    while let Some(current) = queue.pop() {
                        for &input in &inputs[current.id()] {
                            if ancestors.insert(input) {
                                queue.push(input);
                            }
                        }
                    }
                    ancestors
                })
                .collect()
        }

        /// Computes the clusters the way the balancer did before
        /// [`compute_clusters`], from the transitive ancestors of every node,
        /// in time and memory quadratic in the size of the circuit.
        ///
        /// Takes the connected components of the join graph, merges the ones
        /// that depend on each other in a cycle, and peels each cluster into
        /// layers: first the streams with no ancestors in the cluster, then
        /// the streams with no ancestors among the rest, and so on.  The old
        /// code left the clusters in the order in which `kosaraju_scc`
        /// returned them; this sorts them in ascending order of their smallest
        /// stream instead.
        fn reference_clusters(&self) -> Vec<ClusterLayout> {
            let ancestors = self.ancestors();

            let mut join_graph = UnionFind::<usize>::new(self.num_nodes);
            for &BalancedJoin { left, right, .. } in self.joins.values() {
                join_graph.union(left.id(), right.id());
            }
            let mut components = BTreeMap::<usize, BTreeSet<NodeId>>::new();
            for &stream in &self.streams {
                components
                    .entry(join_graph.find(stream.id()))
                    .or_default()
                    .insert(stream);
            }
            let components: Vec<BTreeSet<NodeId>> = components.into_values().collect();

            // An edge from `i` to `j` if a stream of component `j` is an
            // ancestor of a stream of component `i`.
            let mut dependencies = DiGraph::<usize, ()>::new();
            let vertices: Vec<_> = (0..components.len())
                .map(|i| dependencies.add_node(i))
                .collect();
            for (i, streams) in components.iter().enumerate() {
                let upstream: BTreeSet<NodeId> = streams
                    .iter()
                    .flat_map(|stream| ancestors[stream.id()].iter().copied())
                    .collect();
                for (j, other) in components.iter().enumerate() {
                    if i != j && !upstream.is_disjoint(other) {
                        dependencies.add_edge(vertices[i], vertices[j], ());
                    }
                }
            }

            let mut clusters: Vec<ClusterLayout> = kosaraju_scc(&dependencies)
                .into_iter()
                .map(|scc| {
                    let members: BTreeSet<NodeId> = scc
                        .iter()
                        .flat_map(|&vertex| components[dependencies[vertex]].iter().copied())
                        .collect();

                    let mut cluster = ClusterLayout::default();
                    let mut remaining = members.clone();
                    let mut layer = 0;
                    while !remaining.is_empty() {
                        let free: Vec<NodeId> = remaining
                            .iter()
                            .filter(|stream| ancestors[stream.id()].is_disjoint(&remaining))
                            .copied()
                            .collect();
                        for stream in &free {
                            remaining.remove(stream);
                            cluster.streams.insert(*stream, layer);
                        }
                        cluster.layers.insert(layer, free);
                        layer += 1;
                    }
                    cluster.joins = self
                        .joins
                        .iter()
                        .filter(|(_, balanced_join)| members.contains(&balanced_join.left))
                        .map(|(&join, &balanced_join)| (join, balanced_join))
                        .collect();
                    cluster
                })
                .collect();
            clusters.sort_by_key(|cluster| cluster.streams.keys().next().copied());
            clusters
        }

        /// Checks `clusters` against the requirements of the balancer, stated
        /// directly rather than computed the way either algorithm computes
        /// them.
        ///
        /// Two streams share a cluster exactly when each reaches the other
        /// along dataflow paths and joins, so no two clusters wait on each
        /// other.  Each join belongs to the cluster of its inputs.  A stream's
        /// layer is one more than the largest layer of its ancestors in its
        /// cluster, or 0 if it has none there.  The clusters come in ascending
        /// order of their smallest stream, and each lists the streams of every
        /// layer in ascending order.
        ///
        /// # Arguments
        ///
        /// * `clusters` - The clusters to check.
        ///
        /// # Returns
        ///
        /// An error that names the first requirement that `clusters` breaks.
        fn check_clusters(&self, clusters: &[ClusterLayout]) -> Result<(), TestCaseError> {
            let ancestors = self.ancestors();
            let streams: Vec<NodeId> = self.streams.iter().copied().collect();
            let position: BTreeMap<NodeId, usize> = streams
                .iter()
                .enumerate()
                .map(|(index, &stream)| (stream, index))
                .collect();

            // `reaches[i][j]` tells whether stream `j` can be reached from
            // stream `i` along dataflow paths and joins.
            let num_streams = streams.len();
            let mut reaches = vec![vec![false; num_streams]; num_streams];
            for (j, stream) in streams.iter().enumerate() {
                reaches[j][j] = true;
                for ancestor in &ancestors[stream.id()] {
                    if let Some(&i) = position.get(ancestor) {
                        reaches[i][j] = true;
                    }
                }
            }
            for &BalancedJoin { left, right, .. } in self.joins.values() {
                reaches[position[&left]][position[&right]] = true;
                reaches[position[&right]][position[&left]] = true;
            }
            for k in 0..num_streams {
                for i in 0..num_streams {
                    for j in 0..num_streams {
                        if reaches[i][k] && reaches[k][j] {
                            reaches[i][j] = true;
                        }
                    }
                }
            }

            let mut cluster_of = BTreeMap::new();
            for (number, cluster) in clusters.iter().enumerate() {
                prop_assert!(!cluster.streams.is_empty(), "cluster {} is empty", number);
                for &stream in cluster.streams.keys() {
                    prop_assert!(
                        cluster_of.insert(stream, number).is_none(),
                        "stream {} is in two clusters",
                        stream
                    );
                }
            }
            prop_assert_eq!(
                cluster_of.keys().copied().collect::<BTreeSet<_>>(),
                self.streams.clone()
            );
            for (i, first) in streams.iter().enumerate() {
                for (j, second) in streams.iter().enumerate() {
                    prop_assert_eq!(
                        cluster_of[first] == cluster_of[second],
                        reaches[i][j] && reaches[j][i],
                        "streams {} and {}",
                        first,
                        second
                    );
                }
            }

            prop_assert_eq!(
                clusters
                    .iter()
                    .map(|cluster| cluster.joins.len())
                    .sum::<usize>(),
                self.joins.len()
            );
            for (join, balanced_join) in &self.joins {
                prop_assert_eq!(
                    clusters[cluster_of[&balanced_join.left]].joins.get(join),
                    Some(balanced_join)
                );
            }

            for cluster in clusters {
                let mut layers = BTreeMap::<usize, Vec<NodeId>>::new();
                for (&stream, &layer) in &cluster.streams {
                    let expected = ancestors[stream.id()]
                        .iter()
                        .filter_map(|ancestor| cluster.streams.get(ancestor))
                        .map(|layer| layer + 1)
                        .max()
                        .unwrap_or(0);
                    prop_assert_eq!(layer, expected, "layer of stream {}", stream);
                    layers.entry(layer).or_default().push(stream);
                }
                prop_assert_eq!(&cluster.layers, &layers);
            }

            prop_assert!(
                clusters
                    .windows(2)
                    .all(|pair| pair[0].streams.keys().next() < pair[1].streams.keys().next()),
                "the clusters are out of order"
            );
            Ok(())
        }
    }

    /// Builds a circuit from generated parts.
    ///
    /// # Arguments
    ///
    /// * `num_nodes` - The number of nodes.
    /// * `order` - A topological order of the nodes, which ascending node ids
    ///   need not follow.
    /// * `pairs` - Pairs of nodes, each of which becomes an edge from the node
    ///   earlier in `order` to the later one, unless the two are the same.
    /// * `writes_stream` - Whether each node writes a stream.
    /// * `joins` - The left input, the right input, and whether it is a left
    ///   join, for each join, with the inputs picked among the streams.
    ///
    /// # Returns
    ///
    /// The circuit.  Its joins have the node ids that follow its nodes, since
    /// join ids only key the map of joins.
    fn assemble(
        num_nodes: usize,
        order: &[usize],
        pairs: &[(usize, usize)],
        writes_stream: &[bool],
        joins: &[(Index, Index, bool)],
    ) -> TestCircuit {
        let mut position = vec![0; num_nodes];
        for (index, &node) in order.iter().enumerate() {
            position[node] = index;
        }
        let edges = pairs
            .iter()
            .filter(|(a, b)| a != b)
            .map(|&(a, b)| {
                if position[a] < position[b] {
                    (NodeId::new(a), NodeId::new(b))
                } else {
                    (NodeId::new(b), NodeId::new(a))
                }
            })
            .collect();

        let streams: Vec<NodeId> = (0..num_nodes)
            .filter(|&node| writes_stream[node])
            .map(NodeId::new)
            .collect();
        let joins = if streams.is_empty() {
            Joins::new()
        } else {
            joins
                .iter()
                .enumerate()
                .map(|(index, (left, right, is_left_join))| {
                    let balanced_join = BalancedJoin {
                        left: *left.get(&streams),
                        right: *right.get(&streams),
                        is_left_join: *is_left_join,
                    };
                    (NodeId::new(num_nodes + index), balanced_join)
                })
                .collect()
        };

        TestCircuit {
            num_nodes,
            edges,
            streams: streams.into_iter().collect(),
            joins,
        }
    }

    /// Generates an acyclic circuit of up to 48 nodes with random edges,
    /// streams, and joins.  The number of edges per node and the share of
    /// nodes that write a stream vary from circuit to circuit, so the circuits
    /// range from sparse ones with many small clusters to dense ones with a
    /// few large, deeply layered clusters.
    fn random_circuit() -> impl Strategy<Value = TestCircuit> {
        (1..=48usize, 0..=3usize, 1..=9u32)
            .prop_flat_map(|(num_nodes, edges_per_node, stream_tenths)| {
                (
                    Just(num_nodes),
                    Just((0..num_nodes).collect::<Vec<_>>()).prop_shuffle(),
                    vec((0..num_nodes, 0..num_nodes), 0..=edges_per_node * num_nodes),
                    vec(
                        prop::bool::weighted(f64::from(stream_tenths) / 10.0),
                        num_nodes,
                    ),
                    vec(
                        (any::<Index>(), any::<Index>(), any::<bool>()),
                        0..=num_nodes,
                    ),
                )
            })
            .prop_map(|(num_nodes, order, pairs, writes_stream, joins)| {
                assemble(num_nodes, &order, &pairs, &writes_stream, &joins)
            })
    }

    /// Generates a circuit shaped like the plan of a SQL query: up to 16
    /// joins, each of which reads two streams picked among up to 4 circuit
    /// inputs and the outputs of the joins before it.  A join reads each input
    /// through an exchange and an integral, as `join_balanced` builds it, and
    /// an operator after each join copies its output, so a later join may
    /// read either.
    fn query_plan() -> impl Strategy<Value = TestCircuit> {
        (
            1..=4usize,
            vec((any::<Index>(), any::<Index>(), any::<bool>()), 1..=16),
        )
            .prop_map(|(num_inputs, choices)| {
                let mut circuit = TestCircuit {
                    num_nodes: num_inputs,
                    edges: Vec::new(),
                    streams: BTreeSet::new(),
                    joins: Joins::new(),
                };
                let mut outputs: Vec<NodeId> = (0..num_inputs).map(NodeId::new).collect();
                for (left, right, is_left_join) in choices {
                    let inputs = [*left.get(&outputs), *right.get(&outputs)];
                    let join = circuit.add_node();
                    for input in inputs {
                        let exchange = circuit.add_node();
                        let integral = circuit.add_node();
                        circuit.edges.extend([
                            (input, exchange),
                            (exchange, integral),
                            (integral, join),
                        ]);
                    }
                    let copy = circuit.add_node();
                    circuit.edges.push((join, copy));
                    circuit.streams.extend(inputs);
                    let balanced_join = BalancedJoin {
                        left: inputs[0],
                        right: inputs[1],
                        is_left_join,
                    };
                    circuit.joins.insert(join, balanced_join);
                    outputs.extend([join, copy]);
                }
                circuit
            })
    }

    /// Generates a random circuit or the plan of a SQL query.
    fn test_circuit() -> impl Strategy<Value = TestCircuit> {
        prop_oneof![random_circuit(), query_plan()]
    }

    /// The node ids `0..N`.
    fn node_ids<const N: usize>() -> [NodeId; N] {
        std::array::from_fn(NodeId::new)
    }

    /// An inner join of `left` and `right`.
    fn inner_join(left: NodeId, right: NodeId) -> BalancedJoin {
        BalancedJoin {
            left,
            right,
            is_left_join: false,
        }
    }

    /// A cluster with streams `layers[i]` in layer `i`, and inner joins given
    /// as (join, left, right) triples.
    fn cluster(layers: &[&[NodeId]], joins: &[(NodeId, NodeId, NodeId)]) -> ClusterLayout {
        ClusterLayout {
            streams: layers
                .iter()
                .enumerate()
                .flat_map(|(layer, streams)| streams.iter().map(move |&stream| (stream, layer)))
                .collect(),
            joins: joins
                .iter()
                .map(|&(join, left, right)| (join, inner_join(left, right)))
                .collect(),
            layers: layers
                .iter()
                .enumerate()
                .map(|(layer, streams)| (layer, streams.to_vec()))
                .collect(),
        }
    }

    /// The circuit of `circular_dependency_test_circuit` in the balancer
    /// tests.
    ///
    /// Joins connect s1, s2, and s3, and also s4, s5, and s6.  Each group
    /// feeds the other, since s1 joins s5 with s6 and s4 joins s2 with s3, so
    /// the two form one cluster, in which s1 and s4 follow the other streams.
    #[test]
    fn mutually_dependent_groups_merge() {
        let [left, right, s2, s3, s5, s6, s1, s4, s7, s8] = node_ids();
        let circuit = TestCircuit {
            num_nodes: 10,
            edges: vec![
                (left, s2),
                (right, s3),
                (left, s5),
                (right, s6),
                (s5, s1),
                (s6, s1),
                (s2, s4),
                (s3, s4),
                (s4, s7),
                (s5, s7),
                (s2, s8),
                (s1, s8),
            ],
            streams: BTreeSet::from([s1, s2, s3, s4, s5, s6]),
            joins: Joins::from([
                (s1, inner_join(s5, s6)),
                (s4, inner_join(s2, s3)),
                (s7, inner_join(s4, s5)),
                (s8, inner_join(s2, s1)),
            ]),
        };

        assert_eq!(
            circuit.clusters(),
            vec![cluster(
                &[&[s2, s3, s5, s6], &[s1, s4]],
                &[(s1, s5, s6), (s4, s2, s3), (s7, s4, s5), (s8, s2, s1)]
            )]
        );
    }

    /// A stream on a dataflow path between two streams of a cluster brings its
    /// own group into the cluster, while a group that only reads from the
    /// cluster stays apart.
    #[test]
    fn stream_between_streams_of_a_cluster_merges() {
        let [input, a1, b1, a2, b2, c1, c2, join_a, join_b, join_c] = node_ids();
        let circuit = TestCircuit {
            num_nodes: 10,
            edges: vec![
                (input, a1),
                (a1, b1),
                (b1, a2),
                (input, b2),
                (a2, c1),
                (input, c2),
                (a1, join_a),
                (a2, join_a),
                (b1, join_b),
                (b2, join_b),
                (c1, join_c),
                (c2, join_c),
            ],
            streams: BTreeSet::from([a1, b1, a2, b2, c1, c2]),
            joins: Joins::from([
                (join_a, inner_join(a1, a2)),
                (join_b, inner_join(b1, b2)),
                (join_c, inner_join(c1, c2)),
            ]),
        };

        assert_eq!(
            circuit.clusters(),
            vec![
                cluster(
                    &[&[a1, b2], &[b1], &[a2]],
                    &[(join_a, a1, a2), (join_b, b1, b2)]
                ),
                cluster(&[&[c1, c2]], &[(join_c, c1, c2)]),
            ]
        );
    }

    /// The first example in the documentation of [`depths`]: the joins tie
    /// three of them into one component, where layers follow the longest
    /// chain of streams.
    #[test]
    fn layers_follow_the_longest_chain_of_streams() {
        let [a, b, j1, c, j2, d, j3] = node_ids();
        let circuit = TestCircuit {
            num_nodes: 7,
            edges: vec![
                (a, j1),
                (b, j1),
                (j1, c),
                (c, j2),
                (a, j2),
                (j2, d),
                (d, j3),
                (b, j3),
            ],
            streams: BTreeSet::from([a, b, c, d]),
            joins: Joins::from([
                (j1, inner_join(a, b)),
                (j2, inner_join(c, a)),
                (j3, inner_join(d, b)),
            ]),
        };

        assert_eq!(circuit.node_depths(), vec![0, 0, 1, 1, 2, 2, 0]);
        assert_eq!(
            circuit.clusters(),
            vec![cluster(
                &[&[a, b], &[c], &[d]],
                &[(j1, a, b), (j2, c, a), (j3, d, b)]
            )]
        );
    }

    /// The second example in the documentation of [`depths`]: two clusters
    /// in sequence, both in layer 0.  The downstream cluster comes first,
    /// since its smallest stream has the smaller node id.
    #[test]
    fn cluster_downstream_of_another_starts_at_layer_zero() {
        let [c, d, j2, a, b, j1] = node_ids();
        let circuit = TestCircuit {
            num_nodes: 6,
            edges: vec![(a, j1), (b, j1), (j1, c), (c, j2), (d, j2)],
            streams: BTreeSet::from([a, b, c, d]),
            joins: Joins::from([(j1, inner_join(a, b)), (j2, inner_join(c, d))]),
        };

        assert_eq!(circuit.node_depths(), vec![0; 6]);
        assert_eq!(
            circuit.clusters(),
            vec![
                cluster(&[&[c, d]], &[(j2, c, d)]),
                cluster(&[&[a, b]], &[(j1, a, b)]),
            ]
        );
    }

    /// A join of a stream with itself forms a cluster of one stream.
    #[test]
    fn self_join_forms_a_cluster_of_one_stream() {
        let [a, join] = node_ids();
        let circuit = TestCircuit {
            num_nodes: 2,
            edges: vec![(a, join)],
            streams: BTreeSet::from([a]),
            joins: Joins::from([(join, inner_join(a, a))]),
        };

        assert_eq!(circuit.clusters(), vec![cluster(&[&[a]], &[(join, a, a)])]);
    }

    /// A stream that no join reads forms a cluster without joins, even
    /// downstream of another cluster.
    #[test]
    fn stream_without_a_join_forms_its_own_cluster() {
        let [a, b, join, c] = node_ids();
        let circuit = TestCircuit {
            num_nodes: 4,
            edges: vec![(a, join), (b, join), (join, c)],
            streams: BTreeSet::from([a, b, c]),
            joins: Joins::from([(join, inner_join(a, b))]),
        };

        assert_eq!(
            circuit.clusters(),
            vec![cluster(&[&[a, b]], &[(join, a, b)]), cluster(&[&[c]], &[])]
        );
    }

    /// Without streams there are no clusters.
    #[test]
    fn no_streams_no_clusters() {
        let [a, b] = node_ids();
        assert!(compute_clusters(2, [(a, b)], [], &Joins::new()).is_empty());
    }

    #[test]
    #[should_panic(expected = "the circuit's dataflow graph has a cycle")]
    fn cycle_in_the_dataflow_graph_panics() {
        let [a, b] = node_ids();
        let circuit = TestCircuit {
            num_nodes: 2,
            edges: vec![(a, b), (b, a)],
            streams: BTreeSet::from([a, b]),
            joins: Joins::new(),
        };
        let _ = circuit.clusters();
    }

    #[test]
    #[should_panic(expected = "reads a stream that the balancer does not manage")]
    fn join_of_an_unmanaged_stream_panics() {
        let [a, b, join] = node_ids();
        let circuit = TestCircuit {
            num_nodes: 3,
            edges: vec![(a, join), (b, join)],
            streams: BTreeSet::from([a]),
            joins: Joins::from([(join, inner_join(a, b))]),
        };
        let _ = circuit.clusters();
    }

    /// A chain of 200,000 nodes, in which every 100th node is a stream joined
    /// with the next stream, does not overflow the stack, and each stream
    /// lands one layer after the stream before it.
    #[test]
    fn deep_circuit() {
        const NUM_NODES: usize = 200_000;
        const STREAM_SPACING: usize = 100;

        let edges = (1..NUM_NODES).map(|node| (NodeId::new(node - 1), NodeId::new(node)));
        let streams: Vec<NodeId> = (0..NUM_NODES)
            .step_by(STREAM_SPACING)
            .map(NodeId::new)
            .collect();
        let joins: Joins = streams
            .windows(2)
            .enumerate()
            .map(|(index, pair)| (NodeId::new(NUM_NODES + index), inner_join(pair[0], pair[1])))
            .collect();

        let clusters = compute_clusters(NUM_NODES, edges, streams.iter().copied(), &joins);

        assert_eq!(clusters.len(), 1);
        for (layer, stream) in streams.iter().enumerate() {
            assert_eq!(clusters[0].streams[stream], layer);
        }
    }

    /// A chain of 50,000 joins, each of which joins the output of the join
    /// before it with a stream of its own, forms 50,000 clusters of two
    /// streams in layer 0.
    #[test]
    fn long_chain_of_joins() {
        const NUM_JOINS: usize = 50_000;

        // Join `i` reads the streams of nodes `3 * i` and `3 * i + 1`, and
        // node `3 * i + 3`, which writes the left input of the next join,
        // reads its output.
        let left = |i: usize| NodeId::new(3 * i);
        let right = |i: usize| NodeId::new(3 * i + 1);
        let join = |i: usize| NodeId::new(3 * i + 2);
        let edges = (0..NUM_JOINS).flat_map(|i| {
            [
                (left(i), join(i)),
                (right(i), join(i)),
                (join(i), left(i + 1)),
            ]
        });
        let streams = (0..NUM_JOINS).flat_map(|i| [left(i), right(i)]);
        let joins: Joins = (0..NUM_JOINS)
            .map(|i| (join(i), inner_join(left(i), right(i))))
            .collect();

        let clusters = compute_clusters(3 * NUM_JOINS + 1, edges, streams, &joins);

        assert_eq!(clusters.len(), NUM_JOINS);
        for (i, layout) in clusters.iter().enumerate() {
            assert_eq!(
                *layout,
                cluster(&[&[left(i), right(i)]], &[(join(i), left(i), right(i))])
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        #[test]
        fn matches_reference(circuit in test_circuit()) {
            prop_assert_eq!(circuit.clusters(), circuit.reference_clusters());
        }

        #[test]
        fn meets_the_requirements_of_the_balancer(circuit in test_circuit()) {
            circuit.check_clusters(&circuit.clusters())?;
        }
    }
}
