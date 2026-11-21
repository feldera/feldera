//! Merge the Noop operators that give several consumers their own copy of a trace.
mod policies;
pub use policies::merge_retention_policies;

use crate::{
    consumer_graph::successors,
    cse::same_operation,
    rebuild::rebuild_input_closures,
    recursive_views::metadata_field,
    terms::Terms,
    trace_integrators::integrates_input,
    trace_retention::{retention_slot, Retained},
};
use feldera_sql_ir::{BodyOrder, Circuit, Error, Node, NodeId, OperatorKind as Op, OutputPort};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Decisions made by FindEquivalentNoops at the preceding boundary. Each alias
/// points to the first Noop in its group. A removed policy has no surviving reader.
#[derive(Default, Deserialize, Serialize, PartialEq, Eq, Debug)]
pub struct SharedTraceInputs {
    pub aliases: BTreeMap<u64, u64>,
    pub removed: BTreeSet<u64>,
}

impl SharedTraceInputs {
    pub fn apply(&self, circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
        if self.aliases.is_empty() && self.removed.is_empty() {
            return Ok(circuit);
        }
        let mut aliases = BTreeMap::new();
        for (&from, &to) in &self.aliases {
            let (from, to) = (NodeId::Outer(from), NodeId::Outer(to));
            for id in [from, to] {
                if circuit.node(id).and_then(Node::operator_kind) != Some(Op::Noop) {
                    return Err(Error::new("a shared trace input must be a Noop"));
                }
            }
            aliases.insert(from, to);
        }
        let removed = self
            .removed
            .iter()
            .copied()
            .map(NodeId::Outer)
            .collect::<BTreeSet<_>>();
        for id in &removed {
            if retention_slot(circuit.node(*id).and_then(Node::operator_kind)).is_none() {
                return Err(Error::new("can only discard an unused retention policy"));
            }
        }
        rebuild_input_closures(circuit.coalesce_pruning(&aliases, &removed)?, next)
    }
}

fn outer(id: NodeId) -> u64 {
    let NodeId::Outer(number) = id else {
        unreachable!("operator has an outer identity")
    };
    number
}

/// The policy's control stream is part of its identity. Independent bound
/// calculations can carry the same values when their roots and operations agree.
fn same_stream(circuit: &Circuit, left: OutputPort, right: OutputPort, terms: &mut Terms) -> bool {
    let mut pending = vec![(left, right)];
    let mut seen = HashSet::new();
    while let Some((left, right)) = pending.pop() {
        if left == right || !seen.insert((left, right)) {
            continue;
        }
        let l = circuit.node(left.operator).expect("validated producer");
        let r = circuit.node(right.operator).expect("validated producer");
        if left.number != right.number
            || l.inputs().next().is_none()
            || l.inputs().count() != r.inputs().count()
            || !same_operation(circuit, l, r, terms)
        {
            return false;
        }
        pending.extend(l.inputs().zip(r.inputs()));
    }
    true
}

fn same_policy(
    circuit: &Circuit,
    left: &Node,
    right: &Node,
    terms: &mut Terms,
) -> Result<bool, Error> {
    let control = |n: &Node| {
        n.inputs()
            .nth(1)
            .ok_or_else(|| Error::new("missing retention control"))
    };
    Ok(same_stream(circuit, control(left)?, control(right)?, terms)
        && same_operation(circuit, left, right, terms))
}

fn can_merge(circuit: &Circuit, a: NodeId, b: NodeId, terms: &mut Terms) -> Result<bool, Error> {
    if a == b {
        return Ok(true);
    }
    let left = circuit.node(a).expect("validated retainer");
    let right = circuit.node(b).expect("validated retainer");
    if left.operator_kind() != right.operator_kind() {
        return Ok(false);
    }
    match left.operator_kind() {
        Some(Op::IntegrateTraceRetainKeys) => {
            let mode = |n: &Node| {
                n.data("accumulate")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| Error::new("key retention needs its accumulation mode"))
            };
            Ok(mode(left)? == mode(right)?)
        }
        Some(Op::IntegrateTraceRetainValues) => Ok(true),
        Some(Op::IntegrateTraceRetainNValues) => same_policy(circuit, left, right, terms),
        _ => Err(Error::new("expected a retention operator")),
    }
}

/// Only two distinct N-value policies can require expression equality. Policies
/// from separate groups never reach that comparison.
#[derive(Clone, Copy)]
pub enum RetentionGroup {
    DataInput,
    SiblingNoops,
}

impl RetentionGroup {
    pub fn needs_expressions(self, circuit: &Circuit) -> Result<bool, Error> {
        let mut inputs = HashSet::new();
        for id in circuit.operator_order(BodyOrder::Preorder)? {
            let node = circuit.node(id).expect("validated operator");
            if node.operator_kind() != Some(Op::IntegrateTraceRetainNValues) {
                continue;
            }
            let data = node
                .inputs()
                .next()
                .ok_or_else(|| Error::new("missing retention data input"))?;
            let key = match self {
                Self::DataInput => data,
                Self::SiblingNoops => {
                    let producer = circuit.node(data.operator).expect("validated producer");
                    if producer.operator_kind() != Some(Op::Noop) {
                        continue;
                    }
                    producer
                        .inputs()
                        .next()
                        .ok_or_else(|| Error::new("missing Noop input"))?
                }
            };
            if !inputs.insert(key) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Each group begins at the first eligible retainer in body order. Retainers
/// with different bounds may share keys or values, but retaining N values
/// requires the same bound, ordering, count and predicate.
pub fn retention_groups(
    circuit: &Circuit,
    already_grouped: &BTreeSet<u64>,
) -> Result<Vec<Vec<u64>>, Error> {
    let mut by_data = HashMap::<OutputPort, Vec<usize>>::new();
    let mut groups: Vec<Vec<u64>> = Vec::new();
    let mut terms = Terms::default();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("validated operator");
        if retention_slot(node.operator_kind()).is_none() || already_grouped.contains(&outer(id)) {
            continue;
        }
        let data = node
            .inputs()
            .next()
            .ok_or_else(|| Error::new("missing retention input"))?;
        let candidates = by_data.entry(data).or_default();
        let mut matched = false;
        for &index in candidates.iter() {
            if can_merge(circuit, NodeId::Outer(groups[index][0]), id, &mut terms)? {
                groups[index].push(outer(id));
                matched = true;
                break;
            }
        }
        if !matched {
            candidates.push(groups.len());
            groups.push(vec![outer(id)]);
        }
    }
    Ok(groups)
}

struct TraceInput {
    id: NodeId,
    keys: Option<NodeId>,
    values: Option<NodeId>,
}

fn trace_input(
    circuit: &Circuit,
    id: NodeId,
    members: &[NodeId],
    readers: &[(usize, usize)],
) -> Result<Option<TraceInput>, Error> {
    let mut input = TraceInput {
        id,
        keys: None,
        values: None,
    };
    for &(position, slot) in readers {
        let reader = members[position];
        let op = circuit.node(reader).expect("validated consumer");
        let kind = op.operator_kind().expect("operator");
        if let Some(dimension) = retention_slot(Some(kind)) {
            if slot == 0 {
                let retainer = match dimension {
                    Retained::Keys => &mut input.keys,
                    Retained::Values => &mut input.values,
                };
                retainer.get_or_insert(reader);
            }
        } else {
            let whole = if kind == Op::Sink {
                metadata_field(op, "viewKind")? == "MATERIALIZED"
            } else if kind == Op::Window {
                slot == 0
                    && op
                        .data("lowerUnbounded")
                        .and_then(|v| v.as_bool())
                        .ok_or_else(|| Error::new("window requires its lower bound mode"))?
            } else {
                integrates_input(kind, slot)
            };
            if !whole {
                return Ok(None);
            }
        }
    }
    Ok(Some(input))
}

fn merge_siblings(
    circuit: &Circuit,
    group: &[TraceInput],
    terms: &mut Terms,
    output: &mut SharedTraceInputs,
) -> Result<(), Error> {
    let mut removed = Vec::new();
    for dimension in [Retained::Keys, Retained::Values] {
        let policies: Vec<_> = group
            .iter()
            .filter_map(|input| match dimension {
                Retained::Keys => input.keys,
                Retained::Values => input.values,
            })
            .collect();
        if policies.len() != group.len() {
            removed.extend(policies.into_iter().map(outer));
        } else {
            for &other in &policies[1..] {
                if !can_merge(circuit, policies[0], other, terms)? {
                    return Ok(());
                }
            }
        }
    }
    let representative = outer(group[0].id);
    output.aliases.extend(
        group[1..]
            .iter()
            .map(|input| (outer(input.id), representative)),
    );
    output.removed.extend(removed);
    Ok(())
}

pub fn share_trace_inputs(circuit: &Circuit) -> Result<SharedTraceInputs, Error> {
    let mut output = SharedTraceInputs::default();
    let mut terms = Terms::default();
    for body in
        std::iter::once(circuit.root_id()).chain(circuit.operator_order(BodyOrder::Preorder)?)
    {
        let node = circuit.node(body).expect("validated operator or root");
        if node.field("allOperators").is_none() {
            continue;
        }
        let members = node.edges("allOperators")?;
        let readers = successors(circuit, body)?;
        let mut siblings = HashMap::<OutputPort, Vec<TraceInput>>::new();
        for (position, &id) in members.iter().enumerate() {
            let noop = circuit.node(id).expect("validated operator");
            if noop.operator_kind() != Some(Op::Noop) {
                continue;
            }
            let upstream = noop
                .inputs()
                .next()
                .ok_or_else(|| Error::new("missing Noop input"))?;
            // FindEquivalentNoops visits only simple producers.
            if matches!(
                circuit
                    .node(upstream.operator)
                    .and_then(Node::operator_kind),
                Some(Op::Nested | Op::InputMapWithWaterline | Op::ControlledKeyFilter)
            ) {
                continue;
            }
            if let Some(trace) = trace_input(circuit, id, &members, &readers[position])? {
                siblings.entry(upstream).or_default().push(trace);
            }
        }
        for group in siblings.values().filter(|g| g.len() > 1) {
            merge_siblings(circuit, group, &mut terms, &mut output)?;
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn operator(id: u64, kind: &str, inputs: &[u64]) -> Value {
        let inputs = inputs
            .iter()
            .map(|producer| json!({"operator":{"node":producer},"outputNumber":0}))
            .collect::<Vec<_>>();
        json!({"id":id,"class":format!("DBSP{kind}Operator"),"inputs":inputs})
    }

    fn circuit(operators: Vec<Value>) -> Circuit {
        Circuit::from_json(
            json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":operators}),
        )
        .unwrap()
    }

    #[test]
    fn unrelated_n_value_policies_only_need_topology() {
        let mut ops = vec![
            operator(1, "Constant", &[]),
            operator(2, "Constant", &[]),
            operator(3, "Noop", &[1]),
            operator(4, "Noop", &[2]),
            operator(5, "IntegrateTraceRetainNValues", &[3, 2]),
            operator(6, "IntegrateTraceRetainNValues", &[4, 2]),
        ];
        let graph = circuit(ops.clone());
        assert!(!RetentionGroup::DataInput.needs_expressions(&graph).unwrap());
        assert!(!RetentionGroup::SiblingNoops
            .needs_expressions(&graph)
            .unwrap());

        ops[3] = operator(4, "Noop", &[1]);
        let graph = circuit(ops.clone());
        assert!(!RetentionGroup::DataInput.needs_expressions(&graph).unwrap());
        assert!(RetentionGroup::SiblingNoops
            .needs_expressions(&graph)
            .unwrap());

        ops.push(operator(7, "IntegrateTraceRetainNValues", &[4, 1]));
        assert!(RetentionGroup::DataInput
            .needs_expressions(&circuit(ops))
            .unwrap());
    }

    #[test]
    fn sharing_a_window_trace_requires_an_unbounded_lower_edge() {
        for unbounded in [true, false] {
            let mut window = operator(6, "Window", &[3, 2]);
            window["lowerUnbounded"] = unbounded.into();
            let graph = circuit(vec![
                operator(1, "Constant", &[]),
                operator(2, "Constant", &[]),
                operator(3, "Noop", &[1]),
                operator(4, "Noop", &[1]),
                operator(5, "Join", &[4, 4]),
                window,
                operator(7, "IntegrateTraceRetainValues", &[3, 2]),
            ]);
            let plan = share_trace_inputs(&graph).unwrap();
            if unbounded {
                assert_eq!(plan.aliases, BTreeMap::from([(4, 3)]));
                assert_eq!(plan.removed, BTreeSet::from([7]));
            } else {
                assert_eq!(plan, SharedTraceInputs::default());
            }
        }
    }

    #[test]
    fn retain_n_policies_compare_the_control_input_and_the_count() {
        let mut ops = vec![
            operator(1, "Constant", &[]),
            operator(2, "Constant", &[]),
            operator(3, "Constant", &[]),
        ];
        for (id, control, n) in [(4, 2, 1), (5, 2, 1), (6, 3, 1), (7, 2, 2)] {
            let mut op = operator(id, "IntegrateTraceRetainNValues", &[1, control]);
            op["n"] = n.into();
            op["which"] = "Max".into();
            ops.push(op);
        }
        let graph = circuit(ops);
        assert_eq!(
            retention_groups(&graph, &BTreeSet::new()).unwrap(),
            vec![vec![4, 5], vec![6], vec![7]]
        );
    }
}
