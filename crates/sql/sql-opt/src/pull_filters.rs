//! Move a filter across a single-reader producer when its input can evaluate the predicate.
use crate::{
    fanout::expensive,
    left_joins::uses_missing_value,
    propagate_constants::evaluate_constant_input,
    rebuild::{input_type, noop_function, ProjectionNames},
    stream_rewrites::{body_members, reader_counts},
};
use feldera_sql_expr::{reduce_closure_body_with_warnings, SimplifyWarning};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field,
    InnerKind, NodeId, OperatorKind as Op, OutputPort, TypeId,
};
use std::collections::{BTreeMap, HashMap};

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

fn apply(
    edit: &mut CircuitEditor,
    closure: NodeId,
    arg: ExpressionId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let function = edit.node(closure)?;
    let result = reduce_closure_body_with_warnings(
        edit,
        function.edge("body")?,
        &function.edges("parameters")?,
        &[arg.into()],
        Some(warnings),
    )?;
    edit.expression(result)
}

fn through_map(
    edit: &mut CircuitEditor,
    kind: Op,
    map: NodeId,
    filter: NodeId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<ExpressionId>, Error> {
    if expensive(edit.circuit(), map)? {
        return Ok(None);
    }
    let function = edit.node(map)?;
    let params = function.edges("parameters")?;
    let row = edit.expression(function.edge("body")?)?;
    let arg = if kind == Op::MapIndex {
        let key = edit.tuple_field(row, 0)?;
        let value = edit.tuple_field(row, 1)?;
        let refs = [edit.borrow_value(key)?, edit.borrow_value(value)?];
        edit.tuple_expr(map, true, &refs)?
    } else {
        edit.borrow_value(row)?
    };
    let predicate = apply(edit, filter, arg, warnings)?;
    edit.closure_with_parameters(predicate, &params).map(Some)
}

/// Substitute a sentinel for the aggregate result. A surviving reference
/// means the predicate cannot run before aggregation.
fn through_aggregate(
    edit: &mut CircuitEditor,
    input: TypeId,
    result: TypeId,
    filter: NodeId,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<ExpressionId>, Error> {
    let ty = edit.node(input.into())?;
    let key = edit.type_id(ty.edge("keyType")?)?;
    let row = edit.type_id(ty.edge("elementType")?)?;
    let output_value = edit.type_id(edit.node(result.into())?.edge("elementType")?)?;
    let key = edit.reference_type(key)?;
    let row = edit.reference_type(row)?;
    let tuple = edit.tuple_type(filter, true, [key.into(), row.into()])?;
    let var = edit.variable(tuple, next)?;
    let result_ref = edit.reference_type(output_value)?;
    let missing = edit.empty_origin_node(
        filter,
        InnerKind::Expression(Expr::NoExpression),
        [("type", Field::Reference(result_ref.into()))],
    )?;
    let fields = [edit.tuple_field(var, 0)?, edit.expression(missing)?];
    let arg = edit.tuple_expr(filter, true, &fields)?;
    let predicate = apply(edit, filter, arg, warnings)?;
    if uses_missing_value(edit, predicate.into())? {
        return Ok(None);
    }
    edit.closure_from_variables(predicate, &[var]).map(Some)
}

fn conjunction(
    edit: &mut CircuitEditor,
    first: NodeId,
    second: NodeId,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let function = edit.node(first)?;
    let params = function.edges("parameters")?;
    let [param] = params.as_slice() else {
        return Err(Error::new("expected one filter parameter"));
    };
    let ty = edit.type_id(edit.node(*param)?.edge("type")?)?;
    let result = edit.expression_type(edit.expression(function.edge("body")?)?)?;
    let var = edit.variable(ty, next)?;
    let left = apply(edit, first, var, warnings)?;
    let right = apply(edit, second, var, warnings)?;
    let both = edit.empty_origin_node(
        first,
        InnerKind::Expression(Expr::Binary),
        [
            ("opcode", Field::Data("AND".into())),
            ("type", Field::Reference(result.into())),
            ("left", Field::Reference(left.into())),
            ("right", Field::Reference(right.into())),
        ],
    )?;
    edit.closure_from_variables(edit.expression(both)?, &[var])
}

fn insert_filter(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    input: OutputPort,
    predicate: ExpressionId,
) -> Result<NodeId, Error> {
    let ty = input_type(edit, input)?;
    let id = edit.append_operator(
        body,
        site,
        Op::Filter,
        &[input],
        [
            ("outputType", Field::Reference(ty.into())),
            ("function", Field::Reference(predicate.into())),
        ],
    )?;
    edit.copy_annotations(site, id)?;
    Ok(id)
}

/// Rebuild the original unary operator over the samples retained by the filter.
fn after_filter(
    edit: &mut CircuitEditor,
    body: NodeId,
    producer: NodeId,
    kind: Op,
    filter: NodeId,
    next: &mut u64,
) -> Result<NodeId, Error> {
    let node = edit.node(producer)?;
    let mut fields: Vec<_> = [
        "outputType",
        "function",
        "init",
        "aggregate",
        "partitioningFunction",
        "lower",
        "upper",
        "postProcess",
    ]
    .into_iter()
    .filter_map(|key| node.field(key).cloned().map(|field| (key, field)))
    .collect();
    let input = output(filter);
    if matches!(kind, Op::Noop | Op::Integrate | Op::Differentiate) {
        fields.clear();
        let ty = input_type(edit, input)?;
        fields.push(("outputType", Field::Reference(ty.into())));
        if kind == Op::Noop {
            let identity = noop_function(edit, ty, next)?;
            fields.push(("function", Field::Reference(identity.into())));
        }
    }
    let id = edit.append_operator(body, producer, kind, &[input], fields)?;
    edit.copy_annotations(producer, id)?;
    Ok(id)
}

pub fn pull_filters(
    circuit: Circuit,
    next: &mut u64,
) -> Result<(Circuit, Vec<SimplifyWarning>), Error> {
    let members = body_members(&circuit)?;
    let readers = reader_counts(&circuit, &members);
    let bodies: HashMap<_, _> = members.into_iter().map(|(body, id)| (id, body)).collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let original = circuit.clone();
    let mut provenance = None;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut projections = ProjectionNames::default();
    let mut names = *next;
    let mut warnings = Vec::new();
    for id in order {
        let node = edit.node(id)?;
        projections.visit(id, node, &mut names)?;
        if node.operator_kind() != Some(Op::Filter) {
            continue;
        }
        let source = node
            .inputs()
            .next()
            .ok_or_else(|| Error::new("filter requires an input"))?;
        if readers.get(&source.operator) != Some(&1) {
            continue;
        }
        let body = bodies[&id];
        let predicate = node.edge("function")?;
        let source = replacements
            .get(&source.operator)
            .copied()
            .unwrap_or(source);
        let producer = source.operator;
        let node = edit.node(producer)?;
        let Some(kind) = node.operator_kind() else {
            continue;
        };
        let (first, last) = if matches!(kind, Op::Constant | Op::IncrementalConstant) {
            let Some(value) =
                evaluate_constant_input(&mut edit, id, producer, Op::Filter, &mut warnings)?
            else {
                continue;
            };
            let ty = edit.expression_type(value)?;
            let new = edit.append_operator(
                body,
                producer,
                kind,
                &[],
                [
                    ("outputType", Field::Reference(ty.into())),
                    ("function", Field::Reference(value.into())),
                ],
            )?;
            (new, new)
        } else {
            let Some(upstream) = node.inputs().next() else {
                continue;
            };
            let upstream = replacements
                .get(&upstream.operator)
                .copied()
                .unwrap_or(upstream);
            let function = node.edge("function");
            let pushed = match kind {
                Op::Filter => Some(conjunction(
                    &mut edit,
                    function?,
                    predicate,
                    &mut names,
                    &mut warnings,
                )?),
                Op::Map | Op::MapIndex => {
                    through_map(&mut edit, kind, function?, predicate, &mut warnings)?
                }
                Op::Aggregate
                | Op::StreamAggregate
                | Op::PartitionedRollingAggregate
                | Op::ChainAggregate
                | Op::AggregateLinearPostprocess => {
                    let input = input_type(&edit, upstream)?;
                    let result = input_type(&edit, source)?;
                    through_aggregate(
                        &mut edit,
                        input,
                        result,
                        predicate,
                        &mut names,
                        &mut warnings,
                    )?
                }
                Op::Noop | Op::Differentiate | Op::Integrate => Some(edit.expression(predicate)?),
                _ => None,
            };
            let Some(pushed) = pushed else { continue };
            let first = insert_filter(&mut edit, body, id, upstream, pushed)?;
            let origins = match &mut provenance {
                Some(p) => p,
                None => provenance.insert(original.provenance()?),
            };
            let last = if kind == Op::Filter {
                let origin = origins.fuse_origins(first, producer, id)?;
                edit.set_intermediate_origin(first, origin)?;
                first
            } else {
                origins.inherit_origin(first, id);
                let copy = after_filter(&mut edit, body, producer, kind, first, &mut names)?;
                origins.inherit_origin(copy, producer);
                copy
            };
            (first, last)
        };
        edit.place_before(body, first, id)?;
        replacements.insert(id, output(last));
        projections.replaced(id);
    }
    let result = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok((result, warnings))
}
