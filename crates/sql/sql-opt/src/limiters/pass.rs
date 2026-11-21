//! Apply the source, view and bound-combination portion of InsertLimiters to
//! the original circuit, using the analysis from its matching expansion.

use super::source::{source_bounds, Columns, LatenessSite};
use super::{connect_error_view, minimum_bound, Bound, Projection, Stream, StreamKind};
use feldera_sql_ir::{provenance::SourceRange, OutputPort};
use feldera_sql_ir::{Circuit, CircuitEditor, Error, Field, Node, NodeId, OperatorKind as Op};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct OperatorFacts {
    position: SourceRange,
    reachable_from_error: bool,
    input_kinds: Vec<StreamKind>,
    projection: Option<Projection>,
}

#[derive(Deserialize)]
pub struct LimiterInputs {
    pub next_generated_name: u64,
    gen2: bool,
    singletons: Vec<u64>,
    operators: BTreeMap<u64, OperatorFacts>,
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
    *next = next
        .checked_add(1)
        .ok_or_else(|| Error::new("generated names exhausted"))?;
    let param = edit.ref_parameter(input.ty, next)?;
    let read = edit.deref_parameter(&param)?;
    let projected = src.project(edit, read, dst)?;
    apply_bound(edit, body, input, &param, projected)
}

fn apply_bound(
    edit: &mut CircuitEditor,
    body: NodeId,
    input: Bound,
    param: &feldera_sql_ir::RefParameter,
    value: feldera_sql_ir::ExpressionId,
) -> Result<Bound, Error> {
    let ty = edit.expression_type(value)?;
    let function = edit.closure(value, std::slice::from_ref(param))?;
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

pub fn insert_limiters(circuit: Circuit, state: LimiterInputs) -> Result<(Circuit, u64), Error> {
    let body = circuit.root_id();
    let original = circuit.root().edges("allOperators")?;
    if original.len() != state.operators.len() {
        return Err(Error::new(
            "limiter facts and circuit have different populations",
        ));
    }
    let default_kind = if circuit.root().data("incremental").and_then(|v| v.as_bool()) == Some(true)
    {
        StreamKind::Delta
    } else {
        StreamKind::Collection
    };
    let mut edit = CircuitEditor::new(circuit);
    for number in state.singletons {
        let id = NodeId::Inner(number);
        if edit.circuit().node(id).is_some() {
            edit.register_singleton(id)?;
        }
    }
    let mut next = state.next_generated_name;
    let mut kinds = BTreeMap::new();
    for (&number, facts) in &state.operators {
        let id = NodeId::Outer(number);
        edit.remember_position(id, facts.position)?;
        let op = edit.node(id)?;
        if facts.input_kinds.len() != op.inputs().count() {
            return Err(Error::new("limiter input stream kinds disagree with graph"));
        }
        for (port, &kind) in op.inputs().zip(&facts.input_kinds) {
            if kinds
                .insert(port, kind)
                .is_some_and(|previous| previous != kind)
            {
                return Err(Error::new("conflicting stream kinds for the same output"));
            }
        }
    }
    let mut errors = Vec::new();
    let mut data = BTreeMap::<OutputPort, Stream>::new();
    let mut bounds = BTreeMap::<OutputPort, (Bound, &Projection)>::new();
    for (index, &id) in original.iter().enumerate() {
        let NodeId::Outer(number) = id else {
            return Err(Error::new("expected outer operator"));
        };
        let facts = state
            .operators
            .get(&number)
            .ok_or_else(|| Error::new(format!("missing limiter facts for {id:?}")))?;
        let op = edit.node(id)?;
        let inputs = op.inputs().collect::<Vec<_>>();
        let output = OutputPort {
            operator: id,
            number: 0,
        };
        let projection = facts.projection.as_ref().filter(|p| p.monotone());
        let kind = op.operator_kind();
        let old_count = edit.node(body)?.edges("allOperators")?.len();
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
                        return Err(Error::new(format!("LATENESS on the primary-key table '{name}' is not yet supported by the Gen-2 engine; remove the LATENESS annotation or select a different runtime version")));
                    }
                    let kind = kinds.get(&output).copied().unwrap_or(default_kind);
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
                        let first = edit.node(body)?.edges("allOperators")?[old_count];
                        edit.place_before(body, first, before)?;
                    }
                    continue;
                }
                if let Some(dst) = projection {
                    if let Some(&(input, _)) = inputs.first().and_then(|p| bounds.get(p)) {
                        let param = edit.ref_parameter(input.ty, &mut next)?;
                        let read = edit.deref_parameter(&param)?;
                        let bound = apply_bound(&mut edit, body, input, &param, read)?;
                        bounds.insert(output, (bound, dst));
                        edit.place_before(body, bound.port.operator, id)?;
                    }
                }
            }
            Some(Op::View) if identifier(op, "viewName") == Some("error_view") => {
                let [kind] = facts.input_kinds.as_slice() else {
                    return Err(Error::new("ERROR_VIEW needs one input"));
                };
                connect_error_view(&mut edit, body, id, &errors, *kind)?;
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
                        let first = edit.node(body)?.edges("allOperators")?[old_count];
                        edit.place_before(body, first, id)?;
                    }
                }
            }
            Some(Op::Sink) => {
                if let Some(Field::Object(metadata)) = op.field("metadata") {
                    if matches!(metadata.get("emitFinalColumn"),Some(Field::Data(index)) if index.as_i64().is_some_and(|i|i>=0))
                    {
                        return Err(Error::new(
                            "EMIT FINAL requires the sink visitor outside the P1 limiter slice",
                        ));
                    }
                }
            }
            Some(Op::Constant | Op::IncrementalConstant) => {}
            _ => {
                return Err(Error::new(format!(
                    "{} requires another InsertLimiters visitor",
                    op.class()
                )))
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
