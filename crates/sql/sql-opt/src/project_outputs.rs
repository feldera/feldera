//! Fuse output projections into constants, UNNEST and single-reader joins.
use crate::{
    projection::{projection, simple_tuple, Projection},
    rebuild::ProjectionNames,
    stream_rewrites::{body_members, reader_counts},
};
use feldera_sql_expr::constant_rows::{self as rows, multiset};
use feldera_sql_expr::{
    compose_closures, project_flatmap, reduce_closure_body_with_warnings, SimplifyWarning,
};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId,
    OperatorKind as Op, OutputPort, TypeKind,
};
use std::collections::{BTreeMap, HashMap};

/// DetectShuffle accepts only a tuple of fields of the closure parameter,
/// allowing dereference and clone on either side of a field access.
fn shuffle(
    edit: &CircuitEditor,
    body: NodeId,
    parameter: NodeId,
) -> Result<Option<Vec<usize>>, Error> {
    let transparent = |mut id| -> Result<NodeId, Error> {
        while matches!(
            edit.expr_kind(edit.expression(id)?)?,
            Expr::Deref | Expr::Clone
        ) {
            id = edit.node(id)?.edge("expression")?;
        }
        Ok(id)
    };
    let parameter_node = edit.node(parameter)?;
    let reference = edit.node(parameter_node.edge("type")?)?;
    let Some(tuple) = reference.field("type").and_then(Field::node_id) else {
        return Ok(None);
    };
    if !matches!(
        edit.type_kind(edit.type_id(tuple)?)?,
        TypeKind::RawTuple | TypeKind::Tuple
    ) {
        return Ok(None);
    }
    let body = transparent(body)?;
    if !matches!(
        edit.expr_kind(edit.expression(body)?)?,
        Expr::Tuple | Expr::RawTuple
    ) {
        return Ok(None);
    }
    let mut result = Vec::new();
    if edit.node(body)?.field("fields").is_some() {
        for field in edit.node(body)?.edges("fields")? {
            let field = transparent(field)?;
            if !matches!(
                edit.expr_kind(edit.expression(field)?)?,
                Expr::Field | Expr::CustomOrdField
            ) {
                return Ok(None);
            }
            let access = edit.node(field)?;
            let var = edit.node(transparent(access.edge("expression")?)?)?;
            if var.data("variable") != parameter_node.data("name") {
                return Ok(None);
            }
            result.push(
                access
                    .data("fieldNo")
                    .and_then(|n| n.as_u64())
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| Error::new("invalid projection field"))?,
            );
        }
    }
    Ok(Some(result))
}

fn project_constant(
    edit: &mut CircuitEditor,
    constant: ExpressionId,
    function: NodeId,
    indexed: bool,
    output_type: NodeId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<ExpressionId>, Error> {
    let kind = edit.expr_kind(constant)?;
    if indexed {
        if kind == Expr::IndexedZSet
            || (kind == Expr::ZSet && edit.node(constant.into())?.edges("data")?.is_empty())
        {
            return edit
                .make_expr(
                    constant.into(),
                    Expr::IndexedZSet,
                    edit.type_id(output_type)?,
                    [],
                )
                .map(Some);
        }
        return Ok(None);
    }
    if kind != Expr::ZSet {
        return Ok(None);
    }
    let closure = edit.node(function)?;
    let params = closure.edges("parameters")?;
    let body = closure.edge("body")?;
    let element = edit.expression_type(edit.expression(body)?)?;
    let mut projected = Vec::new();
    for (row, weight) in rows::entries(edit, constant)? {
        let arg = edit.borrow_value(row)?;
        let row =
            reduce_closure_body_with_warnings(edit, body, &params, &[arg.into()], Some(warnings))?;
        // The projection uses LinkedHashMap.put. A repeated literal replaces
        // the previous weight while keeping the original key and its position.
        let mut overwritten = false;
        for (key, previous) in &mut projected {
            if rows::object_equals(edit, NodeId::from(*key), row)? {
                *previous = weight;
                overwritten = true;
                break;
            }
        }
        if !overwritten {
            projected.push((edit.expression(row)?, weight));
        }
    }
    multiset(edit, element, projected).map(Some)
}

fn projected_join(source: Op, indexed: bool) -> Option<(Op, bool)> {
    let (plain, index, was_indexed) = match source {
        Op::Join | Op::JoinIndex => (Op::Join, Op::JoinIndex, source == Op::JoinIndex),
        Op::LeftJoin | Op::LeftJoinIndex => {
            (Op::LeftJoin, Op::LeftJoinIndex, source == Op::LeftJoinIndex)
        }
        Op::StreamJoin | Op::StreamJoinIndex => (
            Op::StreamJoin,
            Op::StreamJoinIndex,
            source == Op::StreamJoinIndex,
        ),
        Op::StarJoin | Op::StarJoinIndex => {
            (Op::StarJoin, Op::StarJoinIndex, source == Op::StarJoinIndex)
        }
        Op::AsofJoin if !indexed => return Some((source, false)),
        _ => return None,
    };
    Some((if indexed { index } else { plain }, was_indexed))
}

pub fn project_outputs(
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
        if !matches!(node.operator_kind(), Some(Op::Map | Op::MapIndex)) {
            continue;
        }
        let indexed = node.operator_kind() == Some(Op::MapIndex);
        let function = node.edge("function")?;
        let output_type = node.edge("outputType")?;
        let incoming = node
            .inputs()
            .next()
            .ok_or_else(|| Error::new("projection requires one input"))?;
        let single = readers.get(&incoming.operator) == Some(&1);
        let source = replacements
            .get(&incoming.operator)
            .copied()
            .unwrap_or(incoming)
            .operator;
        let producer = edit.node(source)?;
        let Some(kind) = producer.operator_kind() else {
            continue;
        };
        let join = projected_join(kind, indexed);
        if !matches!(kind, Op::FlatMap | Op::Constant) && join.is_none() {
            continue;
        }
        if !single && (indexed || join.is_some()) {
            continue;
        }
        let before = edit.expression(producer.edge("function")?)?;
        let closure = edit.node(function)?;
        if closure.class() != "DBSPClosureExpression" {
            continue;
        }
        let params = closure.edges("parameters")?;
        let [parameter] = params.as_slice() else {
            continue;
        };
        let body = closure.edge("body")?;
        if indexed && !simple_tuple(&edit, edit.node(body)?.edge("type")?)? {
            continue;
        }
        let shape = projection(&edit, body, *parameter)?;
        if matches!(shape, Projection::Other)
            || ((indexed || join.is_some()) && !matches!(shape, Projection::Fields(_)))
        {
            continue;
        }
        let (kind, new_function) = match (kind, join) {
            (Op::Constant, _) => {
                let Some(c) = project_constant(
                    &mut edit,
                    before,
                    function,
                    indexed,
                    output_type,
                    &mut warnings,
                )?
                else {
                    continue;
                };
                (Op::Constant, c)
            }
            (Op::FlatMap, _) if !indexed && edit.expr_kind(before)? == Expr::Flatmap => {
                let Some(fields) = shuffle(&edit, body, *parameter)? else {
                    continue;
                };
                let projected = project_flatmap(&mut edit, before.into(), &fields)?;
                (Op::FlatMap, edit.expression(projected)?)
            }
            (_, Some((kind, was_indexed))) => (
                kind,
                compose_closures(
                    &mut edit,
                    before,
                    was_indexed,
                    function,
                    Some(&mut warnings),
                )?,
            ),
            _ => continue,
        };
        let output_type = if kind == Op::Constant {
            edit.expression_type(new_function)?.into()
        } else {
            output_type
        };
        let producer = edit.node(source)?;
        let inputs = producer
            .inputs()
            .map(|p| replacements.get(&p.operator).copied().unwrap_or(p))
            .collect::<Vec<_>>();
        let mut fields = vec![
            ("outputType", Field::Reference(output_type)),
            ("function", Field::Reference(new_function.into())),
        ];
        for key in [
            "balanced",
            "isLeft",
            "leftTimestampIndex",
            "rightTimestampIndex",
            "comparator",
        ] {
            if let Some(field) = producer.field(key) {
                fields.push((key, field.clone()));
            }
        }
        let site = if indexed && kind == Op::Constant {
            id
        } else {
            source
        };
        let new = edit.append_operator(bodies[&id], site, kind, &inputs, fields)?;
        if site == source {
            edit.copy_annotations(source, new)?;
        }
        let origins = match &mut provenance {
            Some(origins) => origins,
            None => provenance.insert(original.provenance()?),
        };
        if indexed && join.is_some() {
            let origin = origins.fuse_origins(new, source, id)?;
            edit.set_intermediate_origin(new, origin)?;
        } else {
            origins.inherit_origin(new, site);
        }
        edit.place_before(bodies[&id], new, id)?;
        replacements.insert(
            id,
            OutputPort {
                operator: new,
                number: 0,
            },
        );
        projections.replaced(id);
    }
    let result = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok((result, warnings))
}
