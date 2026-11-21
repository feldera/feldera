//! Build one fixed-point circuit for each nontrivial recursive component.
use crate::rebuild::{input_type, rebuild_input_closures};
use crate::recursive_views::{components, metadata_field, Components};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, Field, NodeId, OperatorKind as Op, OutputPort,
};
use std::collections::{hash_map::Entry, HashMap};

#[derive(Default)]
pub struct NestedDiagnostics {
    pub unused_declarations: Vec<NodeId>,
    pub late_column: Option<(NodeId, usize)>,
}

pub struct NestedResult {
    pub circuit: Circuit,
    pub diagnostics: NestedDiagnostics,
}

fn stream(id: NodeId) -> OutputPort {
    OutputPort {
        operator: id,
        number: 0,
    }
}

struct Block {
    operator: NodeId,
    /// Each external port gets one delta per block. Each use gets its own integral.
    deltas: HashMap<OutputPort, OutputPort>,
    published: bool,
}

fn append_unary(
    editor: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    kind: Op,
    input: OutputPort,
) -> Result<OutputPort, Error> {
    let ty = input_type(editor, input)?;
    Ok(stream(editor.append_operator(
        body,
        site,
        kind,
        &[input],
        [("outputType", Field::Reference(ty.into()))],
    )?))
}

// Differentiation belongs to the recursive clock of its reader. Java moves
// each such producer during a source-order scan, even when it has multiple readers.
fn include_differentiators(circuit: &Circuit, graph: &mut Components<'_>) -> Vec<usize> {
    let ids = &graph.ids;
    let group_of = &mut graph.group_of;
    let mut sizes: Vec<_> = graph.groups.iter().map(Vec::len).collect();
    for (i, &id) in ids.iter().enumerate() {
        let group = group_of[i];
        if sizes[group] <= 1 {
            continue;
        }
        for port in circuit.node(id).expect("validated operator").inputs() {
            let source = graph.index[&port.operator];
            let old = group_of[source];
            if group != old
                && circuit.node(port.operator).and_then(|n| n.operator_kind())
                    == Some(Op::Differentiate)
            {
                sizes[old] -= 1;
                sizes[group] += 1;
                group_of[source] = group;
            }
        }
    }

    sizes
}

fn check_declarations(
    circuit: &Circuit,
    graph: &Components<'_>,
    sizes: &[usize],
) -> Result<NestedDiagnostics, Error> {
    let mut diagnostics = NestedDiagnostics::default();
    for (i, &id) in graph.ids.iter().enumerate() {
        let op = circuit.node(id).expect("validated operator");
        if sizes[graph.group_of[i]] == 1 {
            if let Some(name) = graph.declaration_names.get(&i) {
                let used = match graph.views.get(name.as_str()) {
                    Some(&view) => {
                        sizes[graph.group_of[view]] > 1
                            || metadata_field(
                                circuit.node(graph.ids[view]).expect("validated view"),
                                "viewKind",
                            )? == "MATERIALIZED"
                    }
                    None => false,
                };
                if !used {
                    diagnostics.unused_declarations.push(id);
                }
            }
        } else if op.operator_kind() == Some(Op::View) {
            let Some(Field::Object(metadata)) = op.field("metadata") else {
                return Err(Error::new("missing recursive view metadata"));
            };
            let Some(Field::Array(columns)) = metadata.get("columns") else {
                return Err(Error::new("missing recursive view columns"));
            };
            if let Some(column) = columns.iter().position(
                |col| matches!(col, Field::Object(fields) if fields.contains_key("lateness")),
            ) {
                diagnostics.late_column = Some((id, column));
                return Ok(diagnostics);
            }
        }
    }
    Ok(diagnostics)
}

pub fn build_nested_operators(circuit: Circuit, next: &mut u64) -> Result<NestedResult, Error> {
    let Some(mut graph) = components(&circuit)? else {
        return Ok(NestedResult {
            circuit,
            diagnostics: NestedDiagnostics::default(),
        });
    };
    let sizes = include_differentiators(&circuit, &mut graph);
    let diagnostics = check_declarations(&circuit, &graph, &sizes)?;
    if diagnostics.late_column.is_some() || sizes.iter().all(|&size| size <= 1) {
        return Ok(NestedResult {
            circuit,
            diagnostics,
        });
    }
    let Components {
        ids,
        group_of,
        index,
        ..
    } = graph;
    let mut editor = CircuitEditor::new(circuit);
    let mut blocks = HashMap::new();
    let mut exports = HashMap::new();
    let mut root_order = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let group = group_of[i];
        let op = editor.node(id)?;
        let is_view = op.operator_kind() == Some(Op::View);
        let inputs: Vec<_> = op.inputs().collect();
        if sizes[group] == 1 {
            let mapped: Vec<_> = inputs
                .iter()
                .map(|p| *exports.get(p).unwrap_or(p))
                .collect();
            editor.rewire_inputs(id, &mapped)?;
            root_order.push(id);
            continue;
        }
        let block = match blocks.entry(group) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(Block {
                operator: editor.begin_recursive_body(id)?,
                deltas: HashMap::new(),
                published: false,
            }),
        };
        if is_view && !block.published {
            root_order.push(block.operator);
            block.published = true;
        }
        let mut mapped = Vec::with_capacity(inputs.len());
        for port in inputs {
            let same_group = group == group_of[index[&port.operator]];
            let input = if !same_group {
                let external = *exports.get(&port).unwrap_or(&port);
                let delta = match block.deltas.entry(external) {
                    Entry::Occupied(entry) => *entry.get(),
                    Entry::Vacant(entry) => *entry.insert(append_unary(
                        &mut editor,
                        block.operator,
                        id,
                        Op::Delta,
                        external,
                    )?),
                };
                append_unary(&mut editor, block.operator, id, Op::Integrate, delta)?
            } else if editor.node(port.operator)?.operator_kind() == Some(Op::ViewDeclaration) {
                append_unary(&mut editor, block.operator, id, Op::Integrate, port)?
            } else {
                // Internal readers consume the view itself at the inner clock.
                port
            };
            mapped.push(input);
        }
        editor.rewire_inputs(id, &mapped)?;
        editor.move_into_recursive_body(block.operator, id)?;
        if is_view {
            let delta = append_unary(
                &mut editor,
                block.operator,
                id,
                Op::Differentiate,
                stream(id),
            )?;
            let exported = editor.export_recursive_view(block.operator, id, delta)?;
            exports.insert(stream(id), exported);
        }
    }
    editor.order_root_operators(&root_order)?;
    let circuit = rebuild_input_closures(editor.finish_with_rewrites()?, next)?;
    Ok(NestedResult {
        circuit,
        diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn recursive_sum(lateness: bool) -> Circuit {
        let port = |id| json!({"operator":{"node":id},"outputNumber":0});
        let ty = json!({"node":10});
        let mut column = json!({});
        if lateness {
            column["lateness"] = json!({"class":"DBSPI32Literal","id":11,"value":1});
        }
        Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            {"class":"DBSPViewDeclarationOperator","id":1,"tableName":{"name":"unused-decl"},"inputs":[]},
            {"class":"DBSPSourceMultisetOperator","id":2,"inputs":[],"outputType":{"class":"DBSPTypeZSet","id":10}},
            {"class":"DBSPViewDeclarationOperator","id":3,"tableName":{"name":"r-decl"},"inputs":[],"outputType":ty},
            {"class":"DBSPSumOperator","id":4,"inputs":[port(3),port(2),port(2)],"outputType":ty},
            {"class":"DBSPViewOperator","id":5,"viewName":{"name":"r"},"metadata":{"columns":[column]},"inputs":[port(4)],"outputType":ty},
            {"class":"DBSPSinkOperator","id":6,"inputs":[port(5)],"outputType":ty}
        ]})).unwrap()
    }

    #[test]
    fn external_uses_share_a_delta_but_have_separate_integrals() {
        let result = build_nested_operators(recursive_sum(false), &mut 0).unwrap();
        let circuit = result.circuit;
        let (block_id, block) = circuit
            .operators()
            .find(|(_, n)| n.operator_kind() == Some(Op::Nested))
            .unwrap();
        let input = stream(NodeId::Outer(2));
        assert_eq!(block.inputs().collect::<Vec<_>>(), [input]);
        let inputs: Vec<_> = circuit.node(NodeId::Outer(4)).unwrap().inputs().collect();
        assert_ne!(inputs[1], inputs[2]);
        let producer = |p: OutputPort| circuit.node(p.operator).unwrap().inputs().next().unwrap();
        assert_eq!(producer(inputs[1]), producer(inputs[2]));
        assert_eq!(producer(producer(inputs[1])), input);
        assert_eq!(producer(stream(NodeId::Outer(6))), stream(block_id));
        assert_eq!(result.diagnostics.unused_declarations, [NodeId::Outer(1)]);
    }

    #[test]
    fn first_late_column_keeps_the_preceding_declaration_warning() {
        let result = build_nested_operators(recursive_sum(true), &mut 0).unwrap();
        assert_eq!(result.diagnostics.unused_declarations, [NodeId::Outer(1)]);
        assert_eq!(result.diagnostics.late_column, Some((NodeId::Outer(5), 0)));
    }
}
