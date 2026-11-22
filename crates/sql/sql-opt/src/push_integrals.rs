// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: MIT
// The original permission notice is retained in LICENSE-VMWARE.

//! Move integrations to the outputs of operators that can consume input deltas.
use crate::rebuild::{input_type, rebuild_input_closures};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, Field, Node, NodeId, OperatorKind as Op, OutputPort,
};
use std::collections::BTreeMap;

fn accepts_deltas(op: Op) -> bool {
    matches!(
        op,
        Op::Differentiate
            | Op::Deindex
            | Op::Map
            | Op::Filter
            | Op::Negate
            | Op::FlatMap
            | Op::Noop
            | Op::Hop
            | Op::View
            | Op::PartitionedRollingAggregate
            | Op::ChainAggregate
            | Op::MapIndex
            | Op::StreamJoin
            | Op::StreamJoinIndex
            | Op::StreamAntiJoin
            | Op::Sum
            | Op::AtomicSum
            | Op::Subtract
            | Op::StreamDistinct
            | Op::StreamAggregate
    )
}

fn marked_no_inc(node: &Node) -> bool {
    node.data("annotations")
        .and_then(|v| v.as_array())
        .is_some_and(|annotations| annotations.iter().any(|a| a["class"] == "NoInc"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntegralEdit {
    Bypass,
    Push,
    ReplaceAndPush(Op),
}

/// Both the live plan and standalone rewrite select an edit after checking the inputs.
fn integral_edit(kind: Op, node: &Node) -> Option<IntegralEdit> {
    if !accepts_deltas(kind) {
        None
    } else if kind == Op::Differentiate && !marked_no_inc(node) {
        Some(IntegralEdit::Bypass)
    } else {
        Some(
            kind.incremental_form()
                .map_or(IntegralEdit::Push, IntegralEdit::ReplaceAndPush),
        )
    }
}

/// On a flat circuit, only the number of consecutive integrals on a rewritten
/// output can affect the next decision. Cancelling a derivative may expose an
/// earlier integral. Recursive imports and exports use the full graph rewrite.
pub fn flat_integral_edits(
    circuit: &Circuit,
) -> Result<Option<BTreeMap<NodeId, IntegralEdit>>, Error> {
    let mut depths: BTreeMap<NodeId, usize> = BTreeMap::new();
    let mut edits = BTreeMap::new();
    for id in circuit.root().edges("allOperators")? {
        let node = circuit
            .node(id)
            .ok_or_else(|| Error::new("missing circuit member"))?;
        let kind = node
            .operator_kind()
            .ok_or_else(|| Error::new("expected an operator"))?;
        if kind == Op::Nested {
            return Ok(None);
        }
        let mut minimum = None;
        for port in node.inputs() {
            let Some(&depth) = depths.get(&port.operator) else {
                // An import or forward edge needs the graph visitor's port mapping.
                return Ok(None);
            };
            let input = if port.number == 0 { depth } else { 0 };
            minimum = Some(minimum.map_or(input, |old: usize| old.min(input)));
        }
        let mut depth = 0;
        if let Some(input) = minimum {
            if kind == Op::Integrate {
                depth = input + 1;
            } else if let Some(edit) = integral_edit(kind, node).filter(|_| input > 0) {
                depth = if edit == IntegralEdit::Bypass {
                    input - 1
                } else {
                    1
                };
                edits.insert(id, edit);
            }
        }
        depths.insert(id, depth);
    }
    Ok(Some(edits))
}

fn unary_input(node: &Node) -> Result<OutputPort, Error> {
    let mut ports = node.inputs();
    ports
        .next()
        .filter(|_| ports.next().is_none())
        .ok_or_else(|| Error::new("integrator requires exactly one input"))
}

struct PushIntegrals {
    edit: CircuitEditor,
    // Each original output maps directly to the port its next consumer must read.
    outputs: BTreeMap<OutputPort, OutputPort>,
    removed: BTreeMap<NodeId, OutputPort>,
}

impl PushIntegrals {
    fn mapped(&self, port: OutputPort) -> OutputPort {
        self.outputs.get(&port).copied().unwrap_or(port)
    }

    fn all_integrated(&self, inputs: &[OutputPort]) -> bool {
        !inputs.is_empty()
            && inputs.iter().all(|p| {
                self.edit
                    .circuit()
                    .node(p.operator)
                    .and_then(Node::operator_kind)
                    == Some(Op::Integrate)
            })
    }

    fn visit_body(&mut self, body: NodeId, push_imports: bool) -> Result<(), Error> {
        // The Java visitor only dispatches on operators in the original circuit.
        let members = self.edit.node(body)?.edges("allOperators")?;
        for (index, &id) in members.iter().enumerate() {
            let next_member = members.get(index + 1).copied();
            let node = self.edit.node(id)?;
            let kind = node.operator_kind().expect("validated body member");
            let inputs: Vec<_> = node.inputs().map(|p| self.mapped(p)).collect();
            if kind == Op::Nested {
                let push = self.all_integrated(&inputs);
                self.visit_body(id, push)?;
                let Some(Field::Array(slots)) = self.edit.node(id)?.field("internalOutputs") else {
                    return Err(Error::new("nested circuit requires output slots"));
                };
                let slots: Vec<_> = slots
                    .iter()
                    .map(|f| OutputPort::from_field(f).map(|p| self.mapped(p)))
                    .collect();
                self.edit.remap_nested_outputs(id, &slots)?;
                if push {
                    for (number, internal) in slots.into_iter().enumerate() {
                        if let Some(internal) = internal {
                            let output = OutputPort {
                                operator: id,
                                number,
                            };
                            let ty = input_type(&self.edit, internal)?;
                            let integral = self.edit.insert_integral(
                                body,
                                id,
                                output,
                                ty,
                                next_member,
                                false,
                            )?;
                            self.outputs.insert(output, integral);
                        }
                    }
                }
                continue;
            }
            if kind == Op::Delta && push_imports {
                let source = inputs
                    .first()
                    .ok_or_else(|| Error::new("delta requires an import"))?;
                let raw = unary_input(self.edit.node(source.operator)?)?;
                self.edit.rewire_inputs(id, &[raw])?;
                continue;
            }
            let Some(action) = integral_edit(kind, node).filter(|_| self.all_integrated(&inputs))
            else {
                self.edit.rewire_inputs(id, &inputs)?;
                continue;
            };
            let raw: Vec<_> = inputs
                .iter()
                .map(|p| unary_input(self.edit.node(p.operator)?))
                .collect::<Result<_, Error>>()?;
            let output = OutputPort {
                operator: id,
                number: 0,
            };
            if action == IntegralEdit::Bypass {
                self.outputs.insert(output, raw[0]);
                self.removed.insert(id, raw[0]);
            } else {
                let aggregate_origin = (kind == Op::StreamAggregate).then_some(inputs[0].operator);
                let ty = self.edit.type_id(node.edge("outputType")?)?;
                let integral = self.edit.insert_integral(
                    body,
                    id,
                    output,
                    ty,
                    next_member,
                    aggregate_origin.is_some(),
                )?;
                self.edit.rewire_inputs(id, &raw)?;
                self.edit.incremental_form(id, aggregate_origin)?;
                self.outputs.insert(output, integral);
            }
        }
        Ok(())
    }
}

pub fn push_integrals(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let root = circuit.root_id();
    let mut pass = PushIntegrals {
        edit: CircuitEditor::new(circuit),
        outputs: BTreeMap::new(),
        removed: BTreeMap::new(),
    };
    pass.visit_body(root, false)?;
    rebuild_input_closures(pass.edit.finish_forwarding(&pass.removed)?, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn port(id: u64, number: u64) -> Value {
        json!({"operator":{"node": id}, "outputNumber":number})
    }

    #[test]
    fn cancelling_a_derivative_exposes_the_previous_integral() {
        let unary = |class: &str, id, source| {
            json!({
                "class":class, "id":id, "inputs":[port(source,0)], "annotations":[]
            })
        };
        let mut keep_derivative = unary("DBSPDifferentiateOperator", 6, 5);
        keep_derivative["annotations"] = json!([{"class":"NoInc"}]);
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[]},
                unary("DBSPIntegrateOperator",2,1), unary("DBSPIntegrateOperator",3,2),
                unary("DBSPDifferentiateOperator",4,3), unary("DBSPStreamDistinctOperator",5,4),
                keep_derivative,
                {"class":"DBSPStreamJoinOperator","id":7,"inputs":[port(5,0),port(1,0)]}
            ]
        }))
        .unwrap();
        assert_eq!(
            flat_integral_edits(&graph).unwrap().unwrap(),
            BTreeMap::from([
                (NodeId::Outer(4), IntegralEdit::Bypass),
                (NodeId::Outer(5), IntegralEdit::ReplaceAndPush(Op::Distinct)),
                (NodeId::Outer(6), IntegralEdit::Push),
            ])
        );
    }

    #[test]
    fn recursive_exports_keep_their_numbers_and_reintegrate_only_occupied_slots() {
        let ty =
            json!({"class":"DBSPTypeZSet","id":1,"elementType":{"class":"DBSPTypeBool","id":2}});
        let unary = |kind: &str, id, input| {
            json!({"class":kind, "id":id, "inputs":[input],
                                                   "annotations":[], "outputType":{"node":1}})
        };
        let input = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],"outputType":ty},
            unary("DBSPIntegrateOperator",2,port(1,0)),
            {"class":"DBSPNestedOperator","id":3,"inputs":[port(2,0)], "annotations":[],
             "allOperators":[unary("DBSPDeltaOperator",4,port(2,0))],"internalOutputs":[null,port(4,0)]},
            unary("DBSPDifferentiateOperator",5,port(3,1)),
            unary("DBSPSinkOperator",6,port(5,0))
        ]})).unwrap();
        let result = push_integrals(input, &mut 0).unwrap();
        let slots: Vec<_> = result
            .operators()
            .map(|(id, op)| {
                (
                    id,
                    op.operator_kind().unwrap(),
                    op.inputs().collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(
            slots
                .iter()
                .filter(|(_, kind, _)| *kind == Op::Integrate)
                .count(),
            2
        );
        let sink = result.node(NodeId::Outer(6)).unwrap();
        assert_eq!(
            unary_input(sink).unwrap(),
            OutputPort {
                operator: NodeId::Outer(3),
                number: 1
            }
        );
        assert_eq!(
            unary_input(result.node(NodeId::Outer(4)).unwrap())
                .unwrap()
                .operator,
            NodeId::Outer(1)
        );
        assert!(result.node(NodeId::Outer(5)).is_none());
    }

    #[test]
    fn annotated_differentiation_moves_through_the_integral_without_cancellation() {
        let input = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],
             "outputType":{"class":"DBSPTypeZSet","id":1,"elementType":{"class":"DBSPTypeBool","id":2}}},
            {"class":"DBSPIntegrateOperator","id":2,"inputs":[port(1,0)],"outputType":{"node":1}},
            {"class":"DBSPDifferentiateOperator","id":3,"inputs":[port(2,0)],"outputType":{"node":1},
             "annotations":[{"class":"NoInc"}]},
            {"class":"DBSPSinkOperator","id":4,"inputs":[port(3,0)],"outputType":{"node":1}}
        ]})).unwrap();
        let result = push_integrals(input, &mut 0).unwrap();
        let difference = result.node(NodeId::Outer(3)).unwrap();
        assert_eq!(unary_input(difference).unwrap().operator, NodeId::Outer(1));
        assert!(marked_no_inc(difference));
        let integral = unary_input(result.node(NodeId::Outer(4)).unwrap())
            .unwrap()
            .operator;
        assert_eq!(
            result.node(integral).unwrap().operator_kind(),
            Some(Op::Integrate)
        );
        assert_eq!(
            unary_input(result.node(integral).unwrap())
                .unwrap()
                .operator,
            NodeId::Outer(3)
        );
    }
}
