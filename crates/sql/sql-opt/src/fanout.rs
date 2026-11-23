//! Duplicate cheap projections and filters for consumers that can fuse with them.
use crate::function_calls::function_name;
use crate::rebuild::rebuild_input_closures;
use crate::stream_rewrites::{body_members, reader_counts};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionKind as Expr, InnerKind, Node, NodeId,
    OperatorKind as Op, TypeKind,
};
use std::collections::{BTreeSet, HashMap};

pub(super) fn expensive(circuit: &Circuit, root: NodeId) -> Result<bool, Error> {
    let mut pending = vec![root];
    let mut size = 0;
    // The heuristic counts occurrences, including shared subexpressions visited
    // through separate edges, and stops before descending into a type.
    while let Some(id) = pending.pop() {
        let node = circuit.node(id).expect("resolved expression node");
        match node.inner_kind() {
            Some(InnerKind::Type(_)) => continue,
            Some(InnerKind::Expression(Expr::Apply)) => {
                if function_name(circuit, node)?.is_some_and(|n| n.starts_with("tumble_")) {
                    continue;
                }
                return Ok(true);
            }
            Some(InnerKind::Expression(Expr::ApplyMethod)) => return Ok(true),
            Some(InnerKind::Expression(Expr::Binary))
                if node.data("opcode").and_then(|v| v.as_str()) == Some("VARIANT_INDEX") =>
            {
                return Ok(true)
            }
            Some(InnerKind::Expression(Expr::Cast)) => {
                let source = circuit.node(node.edge("source")?).expect("cast source");
                for ty in [source.edge("type")?, node.edge("type")?] {
                    if matches!(
                        circuit.node(ty).and_then(|n| n.inner_kind()),
                        Some(InnerKind::Type(TypeKind::Variant))
                    ) {
                        return Ok(true);
                    }
                }
            }
            _ => {}
        }
        size += 1;
        if size > 100 {
            return Ok(true);
        }
        crate::lexical_scope::push_expression_children(node, &mut pending);
    }
    Ok(false)
}

fn fusible(kind: Option<Op>) -> bool {
    matches!(kind, Some(Op::Map | Op::MapIndex | Op::Filter))
}

struct Site {
    body: NodeId,
    consumer: NodeId,
    producer: NodeId,
    input: feldera_sql_ir::OutputPort,
}

fn candidates(circuit: &Circuit) -> Result<Vec<Site>, Error> {
    let members = body_members(circuit)?;
    let readers = reader_counts(circuit, &members);
    let mut sites = Vec::new();
    for (body, consumer) in members {
        let node = circuit.node(consumer).expect("validated consumer");
        if !fusible(node.operator_kind()) {
            continue;
        }
        let first_input = |op: &Node| {
            op.inputs()
                .next()
                .ok_or_else(|| Error::new("projection requires an input"))
        };
        let producer = first_input(node)?.operator;
        let source = circuit.node(producer).expect("validated producer");
        if fusible(source.operator_kind()) && readers.get(&producer).is_some_and(|&n| n > 1) {
            sites.push(Site {
                body,
                consumer,
                producer,
                input: first_input(source)?,
            });
        }
    }
    Ok(sites)
}

pub(super) fn has_candidates(circuit: &Circuit) -> Result<bool, Error> {
    Ok(!candidates(circuit)?.is_empty())
}

/// Only these producers need their closure serialized for the live cost check.
pub fn fanout_producers(circuit: &Circuit) -> Result<BTreeSet<NodeId>, Error> {
    Ok(candidates(circuit)?
        .into_iter()
        .map(|site| site.producer)
        .collect())
}

/// Readers that get a private copy of their input in this iteration.
pub fn fanout_readers(circuit: &Circuit) -> Result<Vec<NodeId>, Error> {
    Ok(cheap_sites(circuit)?
        .into_iter()
        .map(|site| site.consumer)
        .collect())
}

/// One Java visitor invocation. The graph is measured before any clones are added.
pub fn clone_operators_with_fanout(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    Ok(clone_once(circuit, next)?.0)
}

fn cheap_sites(circuit: &Circuit) -> Result<Vec<Site>, Error> {
    let mut costs = HashMap::new();
    let mut sites = Vec::new();
    for site in candidates(circuit)? {
        let node = circuit.node(site.producer).expect("validated producer");
        let function = node.edge("function")?;
        let cost = match costs.entry(function) {
            std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                *entry.insert(expensive(circuit, function)?)
            }
        };
        if !cost {
            sites.push(site);
        }
    }
    Ok(sites)
}

fn clone_once(circuit: Circuit, next: &mut u64) -> Result<(Circuit, bool), Error> {
    let sites = cheap_sites(&circuit)?;
    if sites.is_empty() {
        return Ok((circuit, false));
    }
    let mut edit = CircuitEditor::new(circuit);
    for site in sites {
        edit.clone_projection_for_reader(site.body, site.consumer, site.producer, site.input)?;
    }
    Ok((
        rebuild_input_closures(edit.finish_with_rewrites()?, next)?,
        true,
    ))
}

/// Dead-code elimination after a clone can expose another eligible branch on the next iteration.
pub fn optimize_fanout(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let mut largest = circuit.operators().count();
    for (_, op) in circuit
        .operators()
        .filter(|(_, op)| op.operator_kind() == Some(Op::Nested))
    {
        largest = largest.max(op.edges("allOperators")?.len());
    }
    let outcome = crate::repeat(circuit, largest, None, |circuit, _| {
        let (cloned, changed) = clone_once(circuit, next)?;
        let before_pruning = cloned
            .operator_order(feldera_sql_ir::BodyOrder::Preorder)?
            .len();
        let circuit = crate::remove_dead_operators(cloned, next)?;
        let changed = changed
            || circuit
                .operator_order(feldera_sql_ir::BodyOrder::Preorder)?
                .len()
                != before_pruning;
        Ok::<_, Error>((circuit, changed))
    })?;
    match outcome {
        crate::RepeatResult::Finished(circuit) => Ok(circuit),
        crate::RepeatResult::DidNotConverge { .. } => Err(Error::new(
            "fanout cloning did not converge within the circuit size limit",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn size_threshold_counts_shared_occurrences_in_each_expression() {
        let expression = |n| {
            let mut fields = vec![json!({"class":"DBSPI32Literal","id":2,"value":0})];
            fields.extend((1..n).map(|_| json!({"node":2})));
            Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPMapOperator","id":1,"inputs":[],"function":{"class":"DBSPTupleExpression","id":1,"fields":fields}}]})).unwrap()
        };
        assert!(!expensive(&expression(99), NodeId::Inner(1)).unwrap());
        assert!(expensive(&expression(100), NodeId::Inner(1)).unwrap());
    }
}
