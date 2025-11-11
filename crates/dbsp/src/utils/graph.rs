use petgraph::graph::{EdgeIndex, NodeIndex, UnGraph};
use petgraph::visit::{Bfs, EdgeRef, VisitMap, Visitable};

/// Compute the connected components of an undirected graph.
///
/// Returns a vector of tuples, where each tuple contains a vector of node indices
/// and a vector of edge indices for a connected component.
pub fn components<N, E>(g: &UnGraph<N, E>) -> Vec<(Vec<NodeIndex>, Vec<EdgeIndex>)> {
    let mut visited_nodes = g.visit_map();
    let mut visited_edges = std::collections::HashSet::new();
    let mut result: Vec<(Vec<NodeIndex>, Vec<EdgeIndex>)> = Vec::new();

    for start in g.node_indices() {
        if visited_nodes.is_visited(&start) {
            continue;
        }

        let mut nodes = Vec::new();
        let mut bfs = Bfs::new(g, start);
        visited_nodes.visit(start);

        while let Some(n) = bfs.next(g) {
            visited_nodes.visit(n);
            nodes.push(n);
        }

        // Collect all edges where both endpoints are in this component
        let node_set: std::collections::HashSet<_> = nodes.iter().cloned().collect();
        let mut edges = Vec::new();
        for edge_ref in g.edge_references() {
            let edge_idx = edge_ref.id();
            if visited_edges.contains(&edge_idx) {
                continue;
            }
            let (a, b) = (edge_ref.source(), edge_ref.target());
            if node_set.contains(&a) && node_set.contains(&b) {
                visited_edges.insert(edge_idx);
                edges.push(edge_idx);
            }
        }

        result.push((nodes, edges));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::components;
    use petgraph::graph::UnGraph;

    #[test]
    fn test_empty_graph() {
        let g: UnGraph<(), ()> = UnGraph::new_undirected();
        let comps = components(&g);
        assert_eq!(comps.len(), 0);
    }

    #[test]
    fn test_single_node() {
        let mut g: UnGraph<i32, i32> = UnGraph::new_undirected();
        let n1 = g.add_node(1);
        let comps = components(&g);
        assert_eq!(comps.len(), 1);
        assert_eq!(comps[0].0.len(), 1);
        assert_eq!(comps[0].0[0], n1);
        assert_eq!(comps[0].1.len(), 0); // No edges for a single node
    }

    #[test]
    fn test_two_disconnected_nodes() {
        let mut g: UnGraph<i32, i32> = UnGraph::new_undirected();
        let n1 = g.add_node(1);
        let n2 = g.add_node(2);
        let comps = components(&g);
        assert_eq!(comps.len(), 2);
        // Each component should have one node
        assert_eq!(comps.iter().map(|c| c.0.len()).sum::<usize>(), 2);
        // Check that both nodes are present
        let all_nodes: Vec<_> = comps.iter().flat_map(|c| &c.0).collect();
        assert!(all_nodes.contains(&&n1));
        assert!(all_nodes.contains(&&n2));
        // No edges in disconnected components
        assert_eq!(comps.iter().map(|c| c.1.len()).sum::<usize>(), 0);
    }

    #[test]
    fn test_two_connected_nodes() {
        let mut g: UnGraph<i32, i32> = UnGraph::new_undirected();
        let n1 = g.add_node(1);
        let n2 = g.add_node(2);
        let e1 = g.add_edge(n1, n2, 0);
        let comps = components(&g);
        assert_eq!(comps.len(), 1);
        assert_eq!(comps[0].0.len(), 2);
        assert!(comps[0].0.contains(&n1));
        assert!(comps[0].0.contains(&n2));
        assert_eq!(comps[0].1.len(), 1);
        assert!(comps[0].1.contains(&e1));
    }

    #[test]
    fn test_three_nodes_two_components() {
        let mut g: UnGraph<i32, i32> = UnGraph::new_undirected();
        let n1 = g.add_node(1);
        let n2 = g.add_node(2);
        let n3 = g.add_node(3);
        // n1 and n2 are connected, n3 is isolated
        let e1 = g.add_edge(n1, n2, 0);
        let comps = components(&g);
        assert_eq!(comps.len(), 2);
        // One component with 2 nodes, one with 1 node
        let sizes: Vec<usize> = comps.iter().map(|c| c.0.len()).collect();
        assert!(sizes.contains(&1));
        assert!(sizes.contains(&2));
        // Verify n3 is in a component by itself
        let n3_component = comps.iter().find(|c| c.0.contains(&n3)).unwrap();
        assert_eq!(n3_component.0.len(), 1);
        assert_eq!(n3_component.1.len(), 0); // Isolated node has no edges
        // Verify the connected component has the edge
        let connected_component = comps.iter().find(|c| c.0.contains(&n1)).unwrap();
        assert_eq!(connected_component.1.len(), 1);
        assert!(connected_component.1.contains(&e1));
    }

    #[test]
    fn test_three_nodes_one_component() {
        let mut g: UnGraph<i32, i32> = UnGraph::new_undirected();
        let n1 = g.add_node(1);
        let n2 = g.add_node(2);
        let n3 = g.add_node(3);
        let e1 = g.add_edge(n1, n2, 0);
        let e2 = g.add_edge(n2, n3, 0);
        let comps = components(&g);
        assert_eq!(comps.len(), 1);
        assert_eq!(comps[0].0.len(), 3);
        assert_eq!(comps[0].1.len(), 2);
        assert!(comps[0].1.contains(&e1));
        assert!(comps[0].1.contains(&e2));
    }

    #[test]
    fn test_linear_path() {
        let mut g: UnGraph<i32, i32> = UnGraph::new_undirected();
        let n1 = g.add_node(1);
        let n2 = g.add_node(2);
        let n3 = g.add_node(3);
        let n4 = g.add_node(4);
        // Create a path: n1 - n2 - n3 - n4
        let e1 = g.add_edge(n1, n2, 0);
        let e2 = g.add_edge(n2, n3, 0);
        let e3 = g.add_edge(n3, n4, 0);
        let comps = components(&g);
        assert_eq!(comps.len(), 1);
        assert_eq!(comps[0].0.len(), 4);
        assert_eq!(comps[0].1.len(), 3);
        assert!(comps[0].1.contains(&e1));
        assert!(comps[0].1.contains(&e2));
        assert!(comps[0].1.contains(&e3));
    }

    #[test]
    fn test_cycle() {
        let mut g: UnGraph<i32, i32> = UnGraph::new_undirected();
        let n1 = g.add_node(1);
        let n2 = g.add_node(2);
        let n3 = g.add_node(3);
        // Create a triangle: n1 - n2 - n3 - n1
        let e1 = g.add_edge(n1, n2, 0);
        let e2 = g.add_edge(n2, n3, 0);
        let e3 = g.add_edge(n3, n1, 0);
        let comps = components(&g);
        assert_eq!(comps.len(), 1);
        assert_eq!(comps[0].0.len(), 3);
        assert_eq!(comps[0].1.len(), 3);
        assert!(comps[0].1.contains(&e1));
        assert!(comps[0].1.contains(&e2));
        assert!(comps[0].1.contains(&e3));
    }

    #[test]
    fn test_star_graph() {
        let mut g: UnGraph<i32, i32> = UnGraph::new_undirected();
        let center = g.add_node(0);
        let n1 = g.add_node(1);
        let n2 = g.add_node(2);
        let n3 = g.add_node(3);
        let n4 = g.add_node(4);
        // Star: center connected to all others
        let e1 = g.add_edge(center, n1, 0);
        let e2 = g.add_edge(center, n2, 0);
        let e3 = g.add_edge(center, n3, 0);
        let e4 = g.add_edge(center, n4, 0);
        let comps = components(&g);
        assert_eq!(comps.len(), 1);
        assert_eq!(comps[0].0.len(), 5);
        assert_eq!(comps[0].1.len(), 4);
        assert!(comps[0].1.contains(&e1));
        assert!(comps[0].1.contains(&e2));
        assert!(comps[0].1.contains(&e3));
        assert!(comps[0].1.contains(&e4));
    }

    #[test]
    fn test_multiple_components() {
        let mut g: UnGraph<i32, i32> = UnGraph::new_undirected();
        // Component 1: triangle
        let n1 = g.add_node(1);
        let n2 = g.add_node(2);
        let n3 = g.add_node(3);
        let e1 = g.add_edge(n1, n2, 0);
        let e2 = g.add_edge(n2, n3, 0);
        let e3 = g.add_edge(n3, n1, 0);

        // Component 2: two connected nodes
        let n4 = g.add_node(4);
        let n5 = g.add_node(5);
        let e4 = g.add_edge(n4, n5, 0);

        // Component 3: isolated node
        let n6 = g.add_node(6);

        let comps = components(&g);
        assert_eq!(comps.len(), 3);

        // Verify component node sizes
        let node_sizes: Vec<usize> = comps.iter().map(|c| c.0.len()).collect();
        assert!(node_sizes.contains(&3)); // triangle
        assert!(node_sizes.contains(&2)); // two connected
        assert!(node_sizes.contains(&1)); // isolated

        // Verify all nodes are present
        let all_nodes: Vec<_> = comps.iter().flat_map(|c| &c.0).collect();
        assert_eq!(all_nodes.len(), 6);

        // Verify n6 is in a component by itself
        let n6_component = comps.iter().find(|c| c.0.contains(&n6)).unwrap();
        assert_eq!(n6_component.0.len(), 1);
        assert_eq!(n6_component.1.len(), 0); // No edges

        // Verify triangle component has 3 edges
        let triangle_component = comps.iter().find(|c| c.0.contains(&n1)).unwrap();
        assert_eq!(triangle_component.1.len(), 3);
        assert!(triangle_component.1.contains(&e1));
        assert!(triangle_component.1.contains(&e2));
        assert!(triangle_component.1.contains(&e3));

        // Verify pair component has 1 edge
        let pair_component = comps.iter().find(|c| c.0.contains(&n4)).unwrap();
        assert_eq!(pair_component.1.len(), 1);
        assert!(pair_component.1.contains(&e4));
    }

    #[test]
    fn test_complete_graph() {
        let mut g: UnGraph<i32, i32> = UnGraph::new_undirected();
        let n1 = g.add_node(1);
        let n2 = g.add_node(2);
        let n3 = g.add_node(3);
        let n4 = g.add_node(4);
        // Complete graph K4
        let e1 = g.add_edge(n1, n2, 0);
        let e2 = g.add_edge(n1, n3, 0);
        let e3 = g.add_edge(n1, n4, 0);
        let e4 = g.add_edge(n2, n3, 0);
        let e5 = g.add_edge(n2, n4, 0);
        let e6 = g.add_edge(n3, n4, 0);
        let comps = components(&g);
        assert_eq!(comps.len(), 1);
        assert_eq!(comps[0].0.len(), 4);
        assert_eq!(comps[0].1.len(), 6); // K4 has 6 edges
        assert!(comps[0].1.contains(&e1));
        assert!(comps[0].1.contains(&e2));
        assert!(comps[0].1.contains(&e3));
        assert!(comps[0].1.contains(&e4));
        assert!(comps[0].1.contains(&e5));
        assert!(comps[0].1.contains(&e6));
    }
}
