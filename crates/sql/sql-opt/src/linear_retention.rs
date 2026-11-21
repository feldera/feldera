//! Give a linear aggregate access to its trace retention waterline.
use crate::rebuild::rebuild_input_closures;
use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Error, NodeId, OperatorKind as Op};
use std::collections::{HashMap, HashSet, VecDeque};

/// Every retainer constrains the order. The first in the resulting order
/// supplies the predicate for the replacement aggregate.
fn retention_edges(circuit: &Circuit, body: NodeId) -> Result<Vec<(NodeId, NodeId)>, Error> {
    let mut edges = Vec::new();
    for id in circuit
        .node(body)
        .expect("validated body")
        .edges("allOperators")?
    {
        let op = circuit.node(id).expect("validated operator");
        if op.operator_kind() == Some(Op::IntegrateTraceRetainKeys) {
            for port in op.inputs() {
                if circuit.node(port.operator).and_then(|n| n.operator_kind())
                    == Some(Op::AggregateLinearPostprocess)
                {
                    edges.push((port.operator, id));
                }
            }
        }
    }
    Ok(edges)
}

fn waterline_order(
    circuit: &Circuit,
    mut retainers: Vec<(NodeId, NodeId)>,
) -> Result<Vec<NodeId>, Error> {
    let ids = circuit.root().edges("allOperators")?;
    let index: HashMap<_, _> = ids.iter().copied().zip(0..).collect();
    let mut successors = vec![Vec::new(); ids.len()];
    let mut degrees = vec![0usize; ids.len()];
    let mut add = |from: NodeId, to: NodeId| -> Result<(), Error> {
        let from = *index
            .get(&from)
            .ok_or_else(|| Error::new("dependency crosses circuit bodies"))?;
        let to = index[&to];
        successors[from].push(to);
        degrees[to] += 1;
        Ok(())
    };
    let mut views = HashMap::new();
    let mut declarations = Vec::new();
    let mut sources = Vec::new();
    let mut sinks = Vec::new();
    for (id, node) in circuit.operators() {
        match node.operator_kind() {
            Some(
                Op::SourceMap | Op::SourceLazyMap | Op::SourceMultiset | Op::InputMapWithWaterline,
            ) => sources.push(id),
            Some(Op::ViewDeclaration) => {
                declarations.push((id, node.relation_name("tableName")?.replace("-decl", "")));
                sources.push(id);
                continue;
            }
            Some(Op::View) => {
                views.insert(node.relation_name("viewName")?, id);
            }
            Some(Op::Sink) => sinks.push(id),
            Some(Op::Delta) => continue,
            _ => (),
        }
        for port in node.inputs() {
            add(port.operator, id)?;
        }
    }
    for (id, name) in declarations {
        if let Some(&view) = views.get(name.as_str()) {
            add(view, id)?;
        }
    }
    // Append new dependencies in producer order after all original consumer edges.
    // FIFO tie-breaking in CircuitGraph.sort is observable in the serialized circuit.
    retainers.sort_by_key(|(aggregate, _)| index[aggregate]);
    for (aggregate, retainer) in retainers {
        let waterline = circuit
            .node(retainer)
            .expect("validated retainer")
            .inputs()
            .nth(1)
            .ok_or_else(|| Error::new("key retainer requires a waterline"))?;
        add(waterline.operator, aggregate)?;
    }
    for chain in [sources, sinks] {
        for pair in chain.windows(2) {
            add(pair[0], pair[1])?;
        }
    }
    let mut ready: VecDeque<_> = (0..ids.len()).filter(|&i| degrees[i] == 0).collect();
    let mut sorted = Vec::with_capacity(ids.len());
    while let Some(i) = ready.pop_front() {
        sorted.push(ids[i]);
        for &next in &successors[i] {
            degrees[next] -= 1;
            if degrees[next] == 0 {
                ready.push_back(next);
            }
        }
    }
    if sorted.len() != ids.len() {
        return Err(Error::new("waterline dependencies introduce a cycle"));
    }
    Ok(sorted)
}

/// The optional in-place ordering step observed before Java builds the replacement graph.
pub fn linear_retention_order(circuit: &Circuit) -> Result<Option<Vec<NodeId>>, Error> {
    let retainers = retention_edges(circuit, circuit.root_id())?;
    if retainers.is_empty() {
        return Ok(None);
    }
    waterline_order(circuit, retainers).map(Some)
}

pub fn linear_postprocess_retain_keys(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let Some(order) = linear_retention_order(&circuit)? else {
        return Ok(circuit);
    };
    let root = circuit.root_id();
    let mut editor = CircuitEditor::new(circuit);
    editor.order_root_operators(&order)?;
    let nested = editor.circuit().operator_order(BodyOrder::Preorder)?;
    for body in std::iter::once(root).chain(nested) {
        if editor.node(body)?.field("allOperators").is_none() {
            continue;
        }
        let mut selected = HashSet::new();
        for (aggregate, retainer) in retention_edges(editor.circuit(), body)? {
            if selected.insert(aggregate) {
                editor.retain_aggregate_keys(aggregate, retainer)?;
            }
        }
    }
    rebuild_input_closures(editor.finish_with_rewrites()?, next)
}
