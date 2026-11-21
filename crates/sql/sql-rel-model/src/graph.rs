use crate::ValidationError;

fn require(condition: bool, path: &str, message: &'static str) -> Result<(), ValidationError> {
    if condition {
        Ok(())
    } else {
        Err(ValidationError {
            path: path.into(),
            message,
        })
    }
}

/// Iterative DFS checks orphans and memoizes subtree heights. Reaching a shared
/// subtree first by a short path must not hide a later excessive path depth.
pub(crate) fn check_graph(
    edges: &[Vec<usize>],
    max_depth: usize,
    arena: &str,
) -> Result<Vec<usize>, ValidationError> {
    let mut postorder = Vec::with_capacity(edges.len());
    let mut active = vec![false; edges.len()];
    let mut heights = vec![0usize; edges.len()];
    for root in 0..edges.len() {
        if heights[root] != 0 {
            continue;
        }
        let mut stack = vec![(root, 0)];
        active[root] = true;
        while let Some((node, next)) = stack.last_mut() {
            if let Some(child) = edges[*node].get(*next) {
                *next += 1;
                require(
                    !active[*child],
                    &format!("{arena}[{child}]"),
                    "cyclic arena reference",
                )?;
                if heights[*child] == 0 {
                    require(
                        stack.len() < max_depth,
                        arena,
                        "graph depth exceeds the validation limit",
                    )?;
                    active[*child] = true;
                    stack.push((*child, 0));
                }
            } else {
                let node = *node;
                let height = edges[node]
                    .iter()
                    .map(|child| heights[*child])
                    .max()
                    .unwrap_or(0)
                    + 1;
                require(
                    height <= max_depth,
                    &format!("{arena}[{node}]"),
                    "graph depth exceeds the validation limit",
                )?;
                heights[node] = height;
                postorder.push(node);
                active[node] = false;
                stack.pop();
            }
        }
    }
    Ok(postorder)
}
