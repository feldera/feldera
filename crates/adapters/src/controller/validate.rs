use std::collections::{BTreeMap, HashSet};

use feldera_adapterlib::errors::controller::ConfigError;
use feldera_types::config::PipelineConfig;

pub fn validate_config(config: &PipelineConfig) -> Result<(), ConfigError> {
    let mut dependencies = Vec::new();

    for (endpoint_name, input) in config.inputs.iter() {
        if let Some(start_after) = input.connector_config.start_after.as_ref() {
            if start_after.is_empty() {
                return Err(ConfigError::empty_start_after(endpoint_name));
            }
            for start_after in start_after.iter() {
                for label in input.connector_config.labels.iter() {
                    dependencies.push((
                        endpoint_name.to_string(),
                        label.clone(),
                        start_after.clone(),
                    ));
                }
            }
        }
    }

    // check for cycles
    if let Some(cycle) = find_cycle(&dependencies) {
        return Err(ConfigError::cyclic_dependency(cycle));
    }

    Ok(())
}

/// Detect the first cycle in a directed graph
fn find_cycle(edges: &[(String, String, String)]) -> Option<Vec<(String, String)>> {
    let mut graph: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();

    // Build adjacency list from edge list
    for (endpoint, u, v) in edges {
        graph
            .entry(u.clone())
            .or_default()
            .push((endpoint.clone(), v.clone()));
    }

    let mut visited = HashSet::new();
    let mut recursion_stack = Vec::new();
    let mut stack_set = HashSet::new(); // For quick lookup in the stack

    fn dfs(
        node: &str,
        graph: &BTreeMap<String, Vec<(String, String)>>,
        visited: &mut HashSet<String>,
        recursion_stack: &mut Vec<(String, String)>,
        stack_set: &mut HashSet<String>,
    ) -> Option<Vec<(String, String)>> {
        if stack_set.contains(node) {
            // Cycle detected → extract the cycle from the stack
            let pos = recursion_stack.iter().position(|x| x.1 == node).unwrap();
            return Some(recursion_stack[pos..].to_vec());
        }

        if visited.contains(node) {
            return None; // Already processed
        }

        visited.insert(node.to_string());
        stack_set.insert(node.to_string());

        if let Some(neighbors) = graph.get(node) {
            for (endpoint, neighbor) in neighbors {
                recursion_stack.push((endpoint.clone(), node.to_string()));

                if let Some(cycle) = dfs(neighbor, graph, visited, recursion_stack, stack_set) {
                    return Some(cycle);
                }

                // Backtrack
                recursion_stack.pop();
            }
        }

        stack_set.remove(node);

        None
    }

    // Perform DFS from each node
    for node in graph.keys() {
        if !visited.contains(node) {
            if let Some(cycle) = dfs(
                node,
                &graph,
                &mut visited,
                &mut recursion_stack,
                &mut stack_set,
            ) {
                return Some(cycle);
            }
        }
    }

    None
}

#[cfg(test)]
#[test]
fn test_find_cycle() {
    let edges1 = vec![
        ("ep1".to_string(), "A".to_string(), "B".to_string()),
        ("ep2".to_string(), "B".to_string(), "C".to_string()),
        ("ep3".to_string(), "C".to_string(), "A".to_string()), // Cycle: A → B → C → A
    ];

    let edges2 = vec![
        ("ep1".to_string(), "X".to_string(), "Y".to_string()),
        ("ep2".to_string(), "Y".to_string(), "Z".to_string()),
    ]; // No cycle

    let edges3 = vec![
        ("ep1".to_string(), "A".to_string(), "B".to_string()),
        ("ep2".to_string(), "B".to_string(), "C".to_string()),
        ("ep3".to_string(), "C".to_string(), "D".to_string()),
        ("ep4".to_string(), "D".to_string(), "E".to_string()),
        ("ep5".to_string(), "E".to_string(), "C".to_string()), // Cycle: C → D → E → C
    ];

    let edges4 = vec![
        ("ep1".to_string(), "A".to_string(), "B".to_string()),
        ("ep1".to_string(), "A".to_string(), "C".to_string()),
        ("ep2".to_string(), "C".to_string(), "D".to_string()),
        ("ep3".to_string(), "C".to_string(), "E".to_string()),
        ("ep4".to_string(), "D".to_string(), "A".to_string()),
        // Cycle: A → C → D → A
    ];

    assert_eq!(
        find_cycle(&edges1),
        Some(vec![
            ("ep1".to_string(), "A".to_string()),
            ("ep2".to_string(), "B".to_string()),
            ("ep3".to_string(), "C".to_string())
        ])
    );
    assert_eq!(find_cycle(&edges2), None);
    assert_eq!(
        find_cycle(&edges3),
        Some(vec![
            ("ep3".to_string(), "C".to_string()),
            ("ep4".to_string(), "D".to_string()),
            ("ep5".to_string(), "E".to_string())
        ])
    );
    assert_eq!(
        find_cycle(&edges4),
        Some(vec![
            ("ep1".to_string(), "A".to_string()),
            ("ep2".to_string(), "C".to_string()),
            ("ep4".to_string(), "D".to_string())
        ])
    );
}
