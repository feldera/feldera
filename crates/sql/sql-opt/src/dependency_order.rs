//! Stable root operator order with the extra dependencies introduced by state retention.
use crate::consumer_graph::successors;
use feldera_sql_ir::{Circuit, Error, NodeId, OperatorKind as Op};
use std::collections::{HashMap, VecDeque};

/// Schedule the supplied edges after the ordinary dataflow edges and before the
/// input and sink ordering constraints. FIFO tie-breaking is visible in S4.
pub fn dependency_order(
    circuit: &Circuit,
    extra: impl IntoIterator<Item = (NodeId, NodeId)>,
) -> Result<Vec<NodeId>, Error> {
    let root = circuit.root_id();
    let ids = circuit.root().edges("allOperators")?;
    let positions: HashMap<_, _> = ids.iter().copied().zip(0..).collect();
    let mut edges = successors(circuit, root)?;
    let position = |id| {
        positions
            .get(&id)
            .copied()
            .ok_or_else(|| Error::new("ordering dependency crosses circuit bodies"))
    };
    for (from, to) in extra {
        edges[position(from)?].push((position(to)?, 0));
    }
    let mut previous_source = None;
    let mut previous_sink = None;
    for (position, (_, op)) in circuit.operators().enumerate() {
        let previous = match op.operator_kind() {
            Some(
                Op::SourceMap
                | Op::SourceLazyMap
                | Op::SourceMultiset
                | Op::InputMapWithWaterline
                | Op::ViewDeclaration,
            ) => &mut previous_source,
            Some(Op::Sink) => &mut previous_sink,
            _ => continue,
        };
        if let Some(index) = previous.replace(position) {
            edges[index].push((position, 0));
        }
    }
    let mut degrees = vec![0usize; ids.len()];
    for &(to, _) in edges.iter().flatten() {
        degrees[to] += 1;
    }
    let mut ready: VecDeque<_> = (0..ids.len()).filter(|&i| degrees[i] == 0).collect();
    let mut ordered = Vec::with_capacity(ids.len());
    while let Some(i) = ready.pop_front() {
        ordered.push(ids[i]);
        for &(to, _) in &edges[i] {
            degrees[to] -= 1;
            if degrees[to] == 0 {
                ready.push_back(to);
            }
        }
    }
    if ordered.len() != ids.len() {
        return Err(Error::new("state retention dependencies introduce a cycle"));
    }
    Ok(ordered)
}
