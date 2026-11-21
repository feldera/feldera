// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: MIT

//! Move snapshot distincts across filters and joins, and eliminate redundant
//! distincts using the multiplicities of the streams produced by the rewrite.

use crate::{
    multiset::{input_fact, record_outputs, Multiplicities},
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
        match kind {
            Op::StreamDistinct | Op::Distinct | Op::Positive => {
                let input = unary(&ports)?;
                let source = self.kind(input.operator)?;
                let redundant = if kind == Op::Positive {
                    matches!(source, Op::Positive | Op::Distinct)
                } else {
                    matches!(source, Op::Distinct | Op::StreamDistinct)
                        || !input_fact(&self.facts, input)?
                };
                if redundant {
                    self.replaced.insert(id, input);
                    return Ok(());
                }
                if kind == Op::StreamDistinct && source == Op::Positive {
                    let raw = inputs(&self.edit, input.operator)?;
                    self.edit.rewire_inputs(id, &raw)?;
                } else if kind != Op::Positive
                    && (matches!(source, Op::StreamJoin | Op::Map | Op::Sum)
                        || (kind == Op::StreamDistinct && source == Op::AtomicSum))
                {
                    if let Some(raw) =
                        below_distincts(&self.edit, &inputs(&self.edit, input.operator)?)?
                    {
                        let producer = self.append(body, input.operator, source, &raw, true)?;
                        self.edit.place_before(body, producer, id)?;
                        self.edit.rewire_inputs(id, &[output(producer)])?;
                    }
                }
            }
            Op::Filter | Op::StreamJoin => {
                if let Some(raw) = below_distincts(&self.edit, &ports)? {
                    let inner = self.append(body, id, kind, &raw, true)?;
                    // The filter moves its input distinct. A join creates a
                    // fresh distinct with the join's own relational origin.
                    let site = if kind == Op::Filter {
                        unary(&ports)?.operator
                    } else {
                        id
                    };
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
            _ => {}
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
