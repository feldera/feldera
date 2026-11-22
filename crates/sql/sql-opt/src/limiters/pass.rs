//! Apply the source, view and bound-combination portion of InsertLimiters to
//! the original circuit, using the analysis from its matching expansion.

use super::source::{Columns, LatenessSite, source_bounds};
use super::{Bound, Projection, Stream, StreamKind, connect_error_view, minimum_bound};
use feldera_sql_ir::{Circuit, CircuitEditor, Error, Field, Node, NodeId, OperatorKind as Op};
use feldera_sql_ir::{OutputClocks, OutputPort, StreamKind as Clock, provenance::SourceRange};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct OperatorFacts {
    position: SourceRange,
    reachable_from_error: bool,
    input_kinds: Option<Vec<Clock>>,
    projection: Option<Projection>,
}

#[derive(Default, Deserialize)]
struct TransferFunctions {
    checkpoint: BTreeMap<u64, serde_json::Value>,
    // A live compiler sends only the closures in its checkpoint's identity space.
    #[serde(default)]
    observation: BTreeMap<u64, serde_json::Value>,
}

impl TransferFunctions {
    fn attach(self, circuit: Circuit) -> Result<(Circuit, BTreeMap<u64, NodeId>), Error> {
        let checkpoint = circuit
            .operators()
            .any(|(_, node)| node.field("provenance").is_some());
        let functions = if checkpoint {
            self.checkpoint
        } else {
            self.observation
        };
        let (operators, definitions): (Vec<_>, Vec<_>) = functions.into_iter().unzip();
        let (circuit, roots) = circuit.with_inner_roots(definitions)?;
        Ok((circuit, operators.into_iter().zip(roots).collect()))
    }
}

#[derive(Deserialize)]
pub struct LimiterInputs {
    pub next_generated_name: u64,
    gen2: bool,
    singletons: Vec<u64>,
    #[serde(default)]
    transfer_functions: TransferFunctions,
    operators: BTreeMap<u64, OperatorFacts>,
}

/// A malformed checkpoint is an internal failure. SQL refusals identify the
/// live table or output column so the front end can report its source range.
#[derive(Debug, thiserror::Error)]
pub enum LimiterError {
    #[error(transparent)]
    InvalidCircuit(#[from] Error),
    #[error("LATENESS on the primary-key table '{table}' is not yet supported by the Gen-2 engine; remove the LATENESS annotation or select a different runtime version")]
    Gen2KeyLateness { operator: u64, table: String, next: u64 },
    #[error("Could not infer a waterline for column '{view}'.'{column}' which has an 'emit_final' annotation")]
    MissingWaterline { operator: u64, row: usize, view: String, column: String, next: u64 },
}

fn identifier<'a>(op: &'a Node, slot: &str) -> Option<&'a str> {
    let Field::Object(name) = op.field(slot)? else {
        return None;
    };
    let Field::Data(value) = name.get("name")? else {
        return None;
    };
    value.as_str()
}

fn projected_bound(
    edit: &mut CircuitEditor,
    body: NodeId,
    input: Bound,
    src: &Projection,
    dst: &Projection,
    next: &mut u64,
) -> Result<Bound, Error> {
    let function = src.project_waterline(edit, input.ty, dst, None, next)?;
    apply_bound(edit, body, input, function)
}

fn apply_bound(
    edit: &mut CircuitEditor,
    body: NodeId,
    input: Bound,
    function: feldera_sql_ir::ExpressionId,
) -> Result<Bound, Error> {
    let result = edit.node(function.into())?.edge("body")?;
    let ty = edit.expression_type(edit.expression(result)?)?;
    let id = edit.append_operator(
        body,
        input.port.operator,
        Op::Apply,
        &[input.port],
        [
            ("outputType", Field::Reference(ty.into())),
            ("function", Field::Reference(function.into())),
        ],
    )?;
    edit.annotate(id, serde_json::json!({"class":"Waterline"}))?;
    Ok(Bound {
        port: OutputPort {
            operator: id,
            number: 0,
        },
        ty,
    })
}

/// The pass needs data clocks at tables, views and their output sinks. Infer them on the
/// original graph before editing the sources. Earlier sidecars also recorded
/// every input clock, which lets replay detect mismatched circuit/state pairs.
fn relation_clocks(
    circuit: &Circuit,
    facts: &BTreeMap<u64, OperatorFacts>,
) -> Result<BTreeMap<NodeId, StreamKind>, Error> {
    let mut clocks = circuit
        .root()
        .field("incremental")
        .map(|_| OutputClocks::new(circuit))
        .transpose()?;
    let mut recorded = BTreeMap::new();
    for (id, node) in circuit.operators() {
        let NodeId::Outer(number) = id else {
            unreachable!("validated circuit member")
        };
        let facts = facts
            .get(&number)
            .ok_or_else(|| Error::new(format!("missing limiter facts for {id:?}")))?;
        let Some(kinds) = &facts.input_kinds else {
            if clocks.is_none() {
                return Err(Error::new(
                    "limiter replay needs the circuit incremental flag or recorded input clocks",
                ));
            }
            continue;
        };
        if kinds.len() != node.inputs().count() {
            return Err(Error::new("limiter input stream kinds disagree with graph"));
        }
        for (port, &kind) in node.inputs().zip(kinds) {
            if let Some(clocks) = clocks.as_mut() {
                if clocks.get(port)? != kind {
                    return Err(Error::new(format!(
                        "recorded limiter clock differs at {port:?}"
                    )));
                }
            } else if recorded.insert(port, kind).is_some_and(|old| old != kind) {
                return Err(Error::new("conflicting stream kinds for the same output"));
            }
        }
    }
    let mut relations = BTreeMap::new();
    for (id, node) in circuit.operators() {
        if !matches!(
            node.operator_kind(),
            Some(Op::SourceMultiset | Op::View | Op::Sink)
        ) {
            continue;
        }
        let port = OutputPort {
            operator: id,
            number: 0,
        };
        let kind = if let Some(clocks) = clocks.as_mut() {
            clocks.get(port)?
        } else {
            // Legacy snapshots without a circuit mode carried clocks at the readers.
            let source = node.inputs().next().unwrap_or(port);
            recorded
                .get(&source)
                .or_else(|| recorded.get(&port))
                .copied()
                .unwrap_or(Clock::Collection)
        };
        relations.insert(id, StreamKind::try_from(kind)?);
    }
    Ok(relations)
}

pub fn insert_limiters(circuit: Circuit, state: LimiterInputs) -> Result<(Circuit, u64), Error> {
    insert_limiters_with_warnings(circuit, state, &mut Vec::new())
        .map_err(|error| Error::new(error.to_string()))
}

pub fn insert_limiters_with_warnings(
    circuit: Circuit,
    state: LimiterInputs,
    warnings: &mut Vec<feldera_sql_expr::SimplifyWarning>,
) -> Result<(Circuit, u64), LimiterError> {
    let body = circuit.root_id();
    let original = circuit.root().edges("allOperators")?;
    if original.len() != state.operators.len() {
        return Err(Error::new(
            "limiter facts and circuit have different populations",
        ).into());
    }
    let clocks = relation_clocks(&circuit, &state.operators)?;
    let (circuit, functions) = state.transfer_functions.attach(circuit)?;
    let mut edit = CircuitEditor::new(circuit);
    for number in state.singletons {
        let id = NodeId::Inner(number);
        if edit.circuit().node(id).is_some() {
            edit.register_singleton(id)?;
        }
    }
    let mut next = state.next_generated_name;
    let mut errors = Vec::new();
    let mut data = BTreeMap::<OutputPort, Stream>::new();
    let mut bounds = BTreeMap::<OutputPort, (Bound, &Projection)>::new();
    for (index, &id) in original.iter().enumerate() {
        let NodeId::Outer(number) = id else {
            return Err(Error::new("expected outer operator").into());
        };
        let facts = state
            .operators
            .get(&number)
            .ok_or_else(|| Error::new(format!("missing limiter facts for {id:?}")))?;
        edit.remember_position(id, facts.position)?;
        let op = edit.node(id)?;
        let inputs = op.inputs().collect::<Vec<_>>();
        let output = OutputPort {
            operator: id,
            number: 0,
        };
        let projection = facts.projection.as_ref().filter(|p| p.monotone());
        let kind = op.operator_kind();
        match kind {
            Some(Op::SourceMultiset | Op::View)
                if identifier(op, "viewName") != Some("error_view") =>
            {
                let columns = Columns::read(op)?;
                if let Some(projection) = projection.filter(|_| !columns.declared.is_empty()) {
                    let name = identifier(op, "tableName")
                        .or_else(|| identifier(op, "viewName"))
                        .ok_or_else(|| Error::new("missing table name"))?
                        .to_owned();
                    if state.gen2 && !columns.key.is_empty() {
                        return Err(LimiterError::Gen2KeyLateness { operator: number, table: name, next });
                    }
                    let kind = clocks[&id];
                    let inherited = inputs.first().and_then(|p| bounds.get(p)).copied();
                    if let Some(input) = inputs.first().and_then(|p| data.get(p)) {
                        edit.replace_stream_inputs(id, &[input.port], input.ty)?;
                    }
                    let result = source_bounds(
                        &mut edit,
                        body,
                        LatenessSite {
                            operator: id,
                            name: &name,
                            columns: &columns,
                            kind,
                            inherited,
                        },
                        &mut next,
                    )?;
                    if !facts.reachable_from_error {
                        errors.push(result.error);
                    }
                    data.insert(output, result.data);
                    bounds.insert(output, (result.bound, projection));
                    if let Some(&before) = original.get(index + 1) {
                        edit.place_before(body, result.first_inserted, before)?;
                    }
                    continue;
                }
                if let Some(dst) = projection {
                    if let Some(&(input, _)) = inputs.first().and_then(|p| bounds.get(p)) {
                        let param = edit.ref_parameter(input.ty, &mut next)?;
                        let read = edit.deref_parameter(&param)?;
                        let function = edit.closure(read, &[param])?;
                        let bound = apply_bound(&mut edit, body, input, function)?;
                        bounds.insert(output, (bound, dst));
                        edit.place_before(body, bound.port.operator, id)?;
                    }
                }
            }
            Some(Op::View) if identifier(op, "viewName") == Some("error_view") => {
                if inputs.len() != 1 {
                    return Err(Error::new("ERROR_VIEW needs one input").into());
                }
                connect_error_view(&mut edit, body, id, &errors, clocks[&id])?;
                continue;
            }
            Some(Op::Sum | Op::AtomicSum | Op::Subtract) => {
                if let Some(dst) = projection {
                    let upstream = inputs
                        .iter()
                        .map(|p| bounds.get(p).copied())
                        .collect::<Option<Vec<_>>>();
                    if let Some(upstream) = upstream {
                        let projected = upstream
                            .into_iter()
                            .map(|(b, src)| {
                                projected_bound(&mut edit, body, b, src, dst, &mut next)
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        let bound = minimum_bound(&mut edit, body, &projected, &mut next)?;
                        bounds.insert(output, (bound, dst));
                        edit.place_before(body, projected[0].port.operator, id)?;
                    }
                }
            }
            Some(Op::Map | Op::MapIndex | Op::Filter | Op::Hop | Op::FlatMap) => {
                let [source] = inputs.as_slice() else {
                    return Err(Error::new(format!("{} requires one input stream", op.class())).into());
                };
                if let Some(projection) = projection {
                    let function = functions.get(&number).copied().ok_or_else(|| {
                        Error::new(format!("missing reduced waterline closure for {}", op.class()))
                    })?;
                    let (bound, _) = bounds.get(source).copied().ok_or_else(|| {
                        Error::new(format!("missing input waterline for {}", op.class()))
                    })?;
                    let function = super::apply_waterlines(
                        &mut edit,
                        function,
                        &[bound.ty],
                        &mut next,
                        warnings,
                    )?;
                    let bound = apply_bound(&mut edit, body, bound, function)?;
                    bounds.insert(output, (bound, projection));
                    edit.place_before(body, bound.port.operator, id)?;
                }
                if let Some(input) = data.get(source) {
                    if kind == Some(Op::Filter) {
                        edit.replace_stream_inputs(id, &[input.port], input.ty)?;
                    } else {
                        edit.rewire_inputs(id, &[input.port])?;
                    }
                }
                continue;
            }
            Some(Op::Sink) => {
                if let Some(field) = super::FinalityColumn::from_sink(op, projection, number, next)? {
                    let input = op
                        .inputs()
                        .next()
                        .ok_or_else(|| Error::new("EMIT FINAL sink needs a stream"))?;
                    let (bound, _) = bounds.get(&input).copied().ok_or_else(|| {
                        Error::new("missing upstream bound for monotone EMIT FINAL column")
                    })?;
                    let stream = match data.get(&input) {
                        Some(stream) => *stream,
                        None => Stream {
                            port: input,
                            ty: crate::rebuild::input_type(&edit, input)?,
                            kind: clocks[&id],
                        },
                    };
                    super::finality::emit_final(
                        &mut edit, body, id, stream, bound, field, &mut next,
                    )?;
                    continue;
                }
            }
            Some(Op::Constant | Op::IncrementalConstant) => {}
            _ => {
                return Err(Error::new(format!(
                    "{} requires another InsertLimiters visitor",
                    op.class()
                )).into());
            }
        }
        if inputs.iter().any(|port| data.contains_key(port)) {
            let first = inputs[0];
            let output_type = match data.get(&first) {
                Some(input) => input.ty,
                None => edit.type_id(edit.node(id)?.edge("outputType")?)?,
            };
            let ports = inputs
                .iter()
                .map(|p| data.get(p).map_or(*p, |s| s.port))
                .collect::<Vec<_>>();
            edit.replace_stream_inputs(id, &ports, output_type)?;
        }
    }
    Ok((edit.finish()?, next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unused_view_keeps_the_clock_of_its_input() {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "incremental": true, "declarations": [],
            "allOperators": [
                {"class": "DBSPIncrementalConstantOperator", "id": 1, "inputs": []},
                {"class": "DBSPIntegrateOperator", "id": 2,
                 "inputs": [{"operator": {"node": 1}, "outputNumber": 0}]},
                {"class": "DBSPViewOperator", "id": 3,
                 "inputs": [{"operator": {"node": 2}, "outputNumber": 0}]}
            ]
        }))
        .unwrap();
        let mut facts: BTreeMap<_, _> = (1..=3)
            .map(|id| {
                (
                    id,
                    OperatorFacts {
                        position: SourceRange::default(),
                        reachable_from_error: false,
                        input_kinds: None,
                        projection: None,
                    },
                )
            })
            .collect();
        let clocks = relation_clocks(&circuit, &facts).unwrap();
        assert_eq!(clocks[&NodeId::Outer(3)], StreamKind::Collection);

        facts.get_mut(&3).unwrap().input_kinds = Some(vec![Clock::Delta]);
        assert!(
            relation_clocks(&circuit, &facts)
                .unwrap_err()
                .to_string()
                .contains("recorded limiter clock differs")
        );
    }
}
