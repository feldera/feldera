// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: MIT

//! Move snapshot distincts across filters and joins, and eliminate redundant
//! distincts using the multiplicities of the streams produced by the rewrite.

use crate::{
    multiset::{input_fact, record_outputs, record_topology_outputs, Multiplicities},
    rebuild::{input_type, rebuild_input_closures},
    stream_rewrites::body_members,
};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, Field, NodeId, OperatorKind as Op, OutputPort,
};
use std::collections::{BTreeMap, HashMap};

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

fn unary(inputs: &[OutputPort]) -> Result<OutputPort, Error> {
    match inputs {
        [input] => Ok(*input),
        _ => Err(Error::new(
            "expected a unary distinct, filter or positive operator",
        )),
    }
}

fn inputs(edit: &CircuitEditor, id: NodeId) -> Result<Vec<OutputPort>, Error> {
    Ok(edit.node(id)?.inputs().collect())
}

/// The inputs below a layer of snapshot distincts, if every branch has one.
fn below_distincts(
    edit: &CircuitEditor,
    ports: &[OutputPort],
) -> Result<Option<Vec<OutputPort>>, Error> {
    let mut sources = Vec::with_capacity(ports.len());
    for port in ports {
        if edit.node(port.operator)?.operator_kind() != Some(Op::StreamDistinct) {
            return Ok(None);
        }
        sources.push(unary(&inputs(edit, port.operator)?)?);
    }
    Ok(Some(sources))
}

struct DistinctPass {
    edit: CircuitEditor,
    facts: Multiplicities,
    replaced: BTreeMap<NodeId, OutputPort>,
}

enum Action {
    Keep,
    Bypass(OutputPort),
    SkipPositive(Vec<OutputPort>),
    StripProducerInputs {
        producer: NodeId,
        kind: Op,
        raw: Vec<OutputPort>,
    },
    MoveAfter {
        raw: Vec<OutputPort>,
        site: NodeId,
    },
}

/// Use the same rules on the original topology and on the inputs already
/// rewritten by the pass. A rewrite can expose a new distinct to a later reader.
fn action(
    edit: &CircuitEditor,
    facts: &Multiplicities,
    id: NodeId,
    kind: Op,
    ports: &[OutputPort],
) -> Result<Action, Error> {
    match kind {
        Op::StreamDistinct | Op::Distinct | Op::Positive => {
            let input = unary(ports)?;
            let source = edit
                .node(input.operator)?
                .operator_kind()
                .ok_or_else(|| Error::new("expected a distinct input operator"))?;
            let redundant = if kind == Op::Positive {
                matches!(source, Op::Positive | Op::Distinct)
            } else {
                matches!(source, Op::Distinct | Op::StreamDistinct) || !input_fact(facts, input)?
            };
            if redundant {
                return Ok(Action::Bypass(input));
            }
            if kind == Op::StreamDistinct && source == Op::Positive {
                return Ok(Action::SkipPositive(inputs(edit, input.operator)?));
            }
            if kind != Op::Positive
                && (matches!(source, Op::StreamJoin | Op::Map | Op::Sum)
                    || (kind == Op::StreamDistinct && source == Op::AtomicSum))
            {
                if let Some(raw) = below_distincts(edit, &inputs(edit, input.operator)?)? {
                    return Ok(Action::StripProducerInputs {
                        producer: input.operator,
                        kind: source,
                        raw,
                    });
                }
            }
        }
        Op::Filter | Op::StreamJoin => {
            if let Some(raw) = below_distincts(edit, ports)? {
                // A filter moves the original distinct. A join creates a new one.
                let site = if kind == Op::Filter {
                    unary(ports)?.operator
                } else {
                    id
                };
                return Ok(Action::MoveAfter { raw, site });
            }
        }
        _ => {}
    }
    Ok(Action::Keep)
}

/// Unknown expression-dependent multiplicities are provisionally treated as
/// sets. This can request an unnecessary snapshot but cannot hide a rewrite:
/// the only multiplicity-dependent rule removes a distinct over a set.
pub(super) fn needs_snapshot(circuit: &Circuit, order: &[NodeId]) -> Result<bool, Error> {
    let edit = CircuitEditor::new(circuit.clone());
    let mut facts = Multiplicities::new();
    for &id in order {
        let node = edit.node(id)?;
        let kind = node
            .operator_kind()
            .ok_or_else(|| Error::new("expected a circuit operator"))?;
        if !matches!(
            action(&edit, &facts, id, kind, &inputs(&edit, id)?)?,
            Action::Keep
        ) {
            return Ok(true);
        }
        record_topology_outputs(id, node, &mut facts)?;
    }
    Ok(false)
}

impl DistinctPass {
    fn kind(&self, id: NodeId) -> Result<Op, Error> {
        self.edit
            .node(id)?
            .operator_kind()
            .ok_or_else(|| Error::new("expected a circuit operator"))
    }

    /// Copy the Java constructor fields used by these rewrites. The original
    /// producer can still have readers on other branches.
    fn append(
        &mut self,
        body: NodeId,
        site: NodeId,
        kind: Op,
        ports: &[OutputPort],
        copy_annotations: bool,
    ) -> Result<NodeId, Error> {
        let source = self.edit.node(site)?;
        let output_type = match kind {
            Op::Map | Op::StreamJoin => source.edge("outputType")?,
            Op::Sum | Op::AtomicSum | Op::Filter | Op::StreamDistinct => input_type(
                &self.edit,
                *ports
                    .first()
                    .ok_or_else(|| Error::new("distinct rewrite needs an input"))?,
            )?
            .into(),
            _ => return Err(Error::new("unexpected distinct rewrite operator")),
        };
        let mut fields = vec![("outputType", Field::Reference(output_type))];
        if matches!(kind, Op::Filter | Op::Map | Op::StreamJoin) {
            fields.push(("function", Field::Reference(source.edge("function")?)));
        }
        if kind == Op::StreamJoin {
            if let Some(balanced) = source.field("balanced") {
                fields.push(("balanced", balanced.clone()));
            }
        }
        let annotations = source
            .data("annotations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let id = self.edit.append_operator(body, site, kind, ports, fields)?;
        if copy_annotations {
            for annotation in annotations {
                self.edit.annotate(id, annotation)?;
            }
        }
        record_outputs(&self.edit, id, &mut self.facts)?;
        Ok(id)
    }

    fn visit(&mut self, body: NodeId, id: NodeId) -> Result<(), Error> {
        let kind = self.kind(id)?;
        let ports: Vec<_> = self
            .edit
            .node(id)?
            .inputs()
            .map(|port| self.replaced.get(&port.operator).copied().unwrap_or(port))
            .collect();
        self.edit.rewire_inputs(id, &ports)?;
        match action(&self.edit, &self.facts, id, kind, &ports)? {
            Action::Keep => {}
            Action::Bypass(input) => {
                self.replaced.insert(id, input);
                return Ok(());
            }
            Action::SkipPositive(raw) => self.edit.rewire_inputs(id, &raw)?,
            Action::StripProducerInputs {
                producer,
                kind,
                raw,
            } => {
                let replacement = self.append(body, producer, kind, &raw, true)?;
                self.edit.place_before(body, replacement, id)?;
                self.edit.rewire_inputs(id, &[output(replacement)])?;
            }
            Action::MoveAfter { raw, site } => {
                let inner = self.append(body, id, kind, &raw, true)?;
                let distinct = self.append(
                    body,
                    site,
                    Op::StreamDistinct,
                    &[output(inner)],
                    kind == Op::Filter,
                )?;
                self.edit.place_before(body, inner, id)?;
                self.replaced.insert(id, output(distinct));
                return Ok(());
            }
        }
        record_outputs(&self.edit, id, &mut self.facts)
    }
}

pub fn optimize_distinct(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let order = circuit.operator_order(BodyOrder::Preorder)?;
    if !order.iter().any(|&id| {
        matches!(
            circuit.node(id).and_then(|node| node.operator_kind()),
            Some(Op::Positive | Op::Distinct | Op::StreamDistinct)
        )
    }) {
        return Ok(circuit);
    }
    let bodies: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    let mut pass = DistinctPass {
        edit: CircuitEditor::new(circuit),
        facts: Multiplicities::new(),
        replaced: BTreeMap::new(),
    };
    for id in order {
        pass.visit(bodies[&id], id)?;
    }
    rebuild_input_closures(pass.edit.finish_forwarding(&pass.replaced)?, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn needs_distinct_snapshot(kinds: &[Op]) -> Result<bool, Error> {
        let mut operators = Vec::new();
        for (index, kind) in kinds.iter().enumerate() {
            let ports = if index == 0 {
                vec![]
            } else {
                vec![json!({"operator":{"node":index}, "outputNumber":0})]
            };
            operators.push(json!({"id":index + 1, "class":kind.wire_name(), "inputs":ports}));
        }
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":operators
        }))?;
        crate::needs_full_snapshot("OptimizeDistinctVisitor", &graph)
    }

    #[test]
    fn snapshot_depends_on_the_distinct_input() -> Result<(), Error> {
        use Op::*;
        assert!(!needs_distinct_snapshot(&[
            SourceMultiset,
            Map,
            StreamDistinct
        ])?);
        assert!(needs_distinct_snapshot(&[SourceMap, Map, StreamDistinct])?);
        assert!(needs_distinct_snapshot(&[
            SourceMultiset,
            StreamDistinct,
            Filter
        ])?);
        assert!(needs_distinct_snapshot(&[
            SourceMultiset,
            Positive,
            StreamDistinct
        ])?);
        assert!(needs_distinct_snapshot(&[
            SourceMultiset,
            Positive,
            Positive
        ])?);
        Ok(())
    }
}
