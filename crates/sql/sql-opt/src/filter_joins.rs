//! Push a predicate onto each join input that can evaluate it independently.
use crate::{
    left_joins::uses_missing_value,
    rebuild::{input_type, ProjectionNames},
    stream_rewrites::{body_members, reader_counts},
};
use feldera_sql_expr::reduce_closure_body;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind, Field, InnerKind,
    NodeId, OperatorKind as Op, OutputPort,
};
use std::collections::{BTreeMap, HashMap};

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

/// Substitute a sentinel for the opposite row. If simplification removes the
/// sentinel, the predicate only needs the join key and this side's value.
fn input_predicate(
    edit: &mut CircuitEditor,
    join: NodeId,
    filter: NodeId,
    slot: usize,
    next: &mut u64,
) -> Result<Option<ExpressionId>, Error> {
    let closure = edit.node(join)?;
    let body = closure.edge("body")?;
    let params: [NodeId; 3] = closure
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("join predicate requires a key and two row parameters"))?;
    let key_type = edit.node(params[0])?.edge("type")?;
    let row_type = edit.node(params[1 + slot])?.edge("type")?;
    let missing_type = edit.node(params[2 - slot])?.edge("type")?;
    let pair = edit.tuple_type(join, true, [key_type, row_type])?;
    let var = edit.variable(pair, next)?;
    let key = edit.tuple_field(var, 0)?.into();
    let row = edit.tuple_field(var, 1)?.into();
    let missing = edit.empty_origin_node(
        join,
        InnerKind::Expression(ExpressionKind::NoExpression),
        [("type", Field::Reference(missing_type))],
    )?;
    let arguments = if slot == 0 {
        [key, row, missing]
    } else {
        [key, missing, row]
    };
    let joined = reduce_closure_body(edit, body, &params, &arguments)?;
    let arg = edit.borrow_value(edit.expression(joined)?)?;
    let predicate = edit.node(filter)?;
    let reduced = reduce_closure_body(
        edit,
        predicate.edge("body")?,
        &predicate.edges("parameters")?,
        &[arg.into()],
    )?;
    if uses_missing_value(edit, reduced)? {
        return Ok(None);
    }
    edit.closure_from_variables(edit.expression(reduced)?, &[var])
        .map(Some)
}

pub fn filter_joins(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let members = body_members(&circuit)?;
    let readers = reader_counts(&circuit, &members);
    let bodies: HashMap<_, _> = members.into_iter().map(|(body, id)| (id, body)).collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let original = circuit.clone();
    let mut origins = None;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut names = *next;
    let mut projections = ProjectionNames::default();
    for id in order {
        let body = bodies[&id];
        let filter = edit.node(id)?;
        projections.visit(id, filter, &mut names)?;
        if filter.operator_kind() != Some(Op::Filter) {
            continue;
        }
        let source = filter
            .inputs()
            .next()
            .ok_or_else(|| Error::new("filter needs an input"))?;
        if readers.get(&source.operator) != Some(&1) {
            continue;
        }
        let predicate = filter.edge("function")?;
        let source = replacements
            .get(&source.operator)
            .copied()
            .unwrap_or(source)
            .operator;
        let join = edit.node(source)?;
        let (kind, fused) = match join.operator_kind() {
            Some(Op::Join) => (Op::Join, Op::JoinFilterMap),
            Some(Op::LeftJoin) => (Op::LeftJoin, Op::LeftJoinFilterMap),
            Some(Op::StarJoin) => (Op::StarJoin, Op::StarJoinFilterMap),
            _ => continue,
        };
        let function = join.edge("function")?;
        let mut fields = vec![
            ("outputType", Field::Reference(join.edge("outputType")?)),
            ("function", Field::Reference(function)),
        ];
        if let Some(balanced) = join.field("balanced") {
            fields.push(("balanced", balanced.clone()));
        }
        let mut inputs: Vec<_> = join.inputs().collect();
        let mut first = None;
        if kind != Op::StarJoin {
            let sides = if kind == Op::Join { 2 } else { 1 };
            for (slot, port) in inputs.iter_mut().take(sides).enumerate() {
                if let Some(test) =
                    input_predicate(&mut edit, function, predicate, slot, &mut names)?
                {
                    let ty = input_type(&edit, *port)?;
                    let filter = edit.append_operator(
                        body,
                        id,
                        Op::Filter,
                        &[*port],
                        [
                            ("outputType", Field::Reference(ty.into())),
                            ("function", Field::Reference(test.into())),
                        ],
                    )?;
                    first.get_or_insert(filter);
                    *port = output(filter);
                }
            }
        }
        let (kind, annotation_source) = if first.is_some() {
            (kind, source)
        } else {
            fields.push(("filter", Field::Reference(predicate)));
            (fused, id)
        };
        let replacement = edit.append_operator(body, source, kind, &inputs, fields)?;
        let annotations = edit
            .node(annotation_source)?
            .data("annotations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        for annotation in annotations {
            edit.annotate(replacement, annotation)?;
        }
        let table = match &mut origins {
            Some(table) => table,
            None => origins.insert(original.provenance()?),
        };
        if first.is_some() {
            table.inherit_origin(replacement, source);
        } else {
            // FilterJoinVisitor composes join.after(filter).
            let origin = table.fuse_origins(replacement, id, source)?;
            edit.set_intermediate_origin(replacement, origin)?;
        }
        edit.place_before(body, first.unwrap_or(replacement), id)?;
        replacements.insert(id, output(replacement));
        projections.replaced(id);
    }
    let result = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok(result)
}
