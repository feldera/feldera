//! Expand a left join into matched and null-extended rows inside a recursive body.
use crate::rebuild::{input_type, rebuild_input_closures};
use crate::stream_rewrites::body_members;
use feldera_sql_expr::{SimplifyWarning, reduce_closure_body, reduce_closure_body_with_warnings};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId,
    OperatorKind as Op, OutputPort, TypeId,
};
use std::collections::{BTreeMap, BTreeSet, HashSet};

/// Bind a (key, left row) input and supply a typed SQL NULL for the missing right row.
fn unmatched_arguments(
    edit: &mut CircuitEditor,
    function: NodeId,
    indexed: TypeId,
    next: &mut u64,
) -> Result<(ExpressionId, [NodeId; 3]), Error> {
    let params = edit.node(function)?.edges("parameters")?;
    let [_, _, right] = params.as_slice() else {
        return Err(Error::new("left join requires three parameters"));
    };
    let right_ref = edit.node(*right)?.edge("type")?;
    let nullable_right = edit.type_id(edit.node(right_ref)?.edge("type")?)?;
    let key = edit.type_id(edit.node(indexed.into())?.edge("keyType")?)?;
    let row = edit.type_id(edit.node(indexed.into())?.edge("elementType")?)?;
    let key_ref = edit.reference_type(key)?;
    let row_ref = edit.reference_type(row)?;
    let pair = edit.tuple_type(function, true, [key_ref.into(), row_ref.into()])?;
    let var = edit.variable_at(function, pair, next)?;
    let mut column = |i| -> Result<NodeId, Error> {
        let field = edit.tuple_field(var, i)?;
        let field = edit.dereference(field)?;
        let field = edit.clone_if_needed(field)?;
        Ok(edit.borrow_value(field)?.into())
    };
    let key = column(0)?;
    let row = column(1)?;
    let absent = edit.typed_null(nullable_right)?;
    Ok((var, [key, row, edit.borrow_value(absent)?.into()]))
}

/// Specialize the original join function for a left row with no matching right row.
pub fn empty_right_join_closure(
    edit: &mut CircuitEditor,
    function: NodeId,
    indexed: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let (var, args) = unmatched_arguments(edit, function, indexed, next)?;
    let callable = edit.node(function)?.edge("type")?;
    let result = edit.type_id(edit.node(callable)?.edge("resultType")?)?;
    let call = edit.empty_origin_node(
        function,
        InnerKind::Expression(Expr::Apply),
        [
            ("type", Field::Reference(result.into())),
            ("function", Field::Reference(function)),
            (
                "arguments",
                Field::Array(args.into_iter().map(Field::Reference).collect()),
            ),
        ],
    )?;
    edit.closure_from_variables(edit.expression(call)?, &[var])
}

/// Inline the join body for the null-padded rows produced by the antijoin.
pub fn unmatched_row_projection(
    edit: &mut CircuitEditor,
    function: NodeId,
    indexed: TypeId,
    next: &mut u64,
    warnings: Option<&mut Vec<SimplifyWarning>>,
) -> Result<ExpressionId, Error> {
    let (row, args) = unmatched_arguments(edit, function, indexed, next)?;
    let closure = edit.node(function)?;
    let body = closure.edge("body")?;
    let parameters = closure.edges("parameters")?;
    let body = reduce_closure_body_with_warnings(edit, body, &parameters, &args, warnings)?;
    edit.closure_from_variables(edit.expression(body)?, &[row])
}

pub fn substitute_left_joins(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let members = body_members(&circuit)?;
    let root = circuit.root_id();
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut counter = *next;
    let port = |operator| OutputPort {
        operator,
        number: 0,
    };
    for (body, id) in members {
        let node = edit.node(id)?;
        if body == root || node.operator_kind() != Some(Op::LeftJoin) {
            continue;
        }
        let inputs: [OutputPort; 2] = node
            .inputs()
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| Error::new("a left join requires two inputs"))?;
        let output = node.edge("outputType")?;
        let function = node.edge("function")?;
        let balanced = node
            .field("balanced")
            .cloned()
            .ok_or_else(|| Error::new("missing join balance flag"))?;
        let join = edit.append_operator(
            body,
            id,
            Op::Join,
            &inputs,
            [
                ("outputType", Field::Reference(output)),
                ("balanced", balanced),
                ("function", Field::Reference(function)),
            ],
        )?;
        let left_type = input_type(&edit, inputs[0])?;
        let anti = edit.append_operator(
            body,
            id,
            Op::AntiJoin,
            &inputs,
            [("outputType", Field::Reference(left_type.into()))],
        )?;
        let projection =
            unmatched_row_projection(&mut edit, function, left_type, &mut counter, None)?;
        let (map, _) = crate::stream_rewrites::append_map(
            &mut edit, body, id, Op::Map, port(anti), projection,
        )?;
        let sum = edit.append_operator(
            body,
            id,
            Op::Sum,
            &[port(join), port(map)],
            [("outputType", Field::Reference(output))],
        )?;
        edit.place_before(body, join, id)?;
        replacements.insert(id, port(sum));
    }
    let result = rebuild_input_closures(edit.finish_forwarding(&replacements)?, &mut counter)?;
    *next = counter;
    Ok(result)
}

/// KeyAnalysis supplies the joins whose right index matches each left row at most once.
/// Replace a join only if reduction removes every use of its missing right value.
pub fn remove_useless_left_joins(
    circuit: Circuit,
    keyed_joins: &BTreeSet<NodeId>,
    next: &mut u64,
) -> Result<(Circuit, Vec<NodeId>), Error> {
    if keyed_joins.is_empty() {
        return Ok((circuit, Vec::new()));
    }
    let order = circuit.operator_order(feldera_sql_ir::BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut removed = Vec::new();
    let mut names = *next;
    let mut visited = 0;
    for join in order.into_iter().filter(|id| keyed_joins.contains(id)) {
        visited += 1;
        let op = edit.node(join)?;
        if !matches!(op.operator_kind(), Some(Op::LeftJoin | Op::LeftJoinIndex)) {
            return Err(Error::new("key analysis selected a non-left-join operator"));
        }
        let function = edit.node(op.edge("function")?)?;
        let params = function.edges("parameters")?;
        let [_, _, right] = params.as_slice() else {
            return Err(Error::new("left join requires three parameters"));
        };
        let body = function.edge("body")?;
        let right_type = edit.node(*right)?.edge("type")?;
        let left = op
            .inputs()
            .next()
            .ok_or_else(|| Error::new("missing left join input"))?;
        let stream = input_type(&edit, left)?;
        let key = edit.type_id(edit.node(stream.into())?.edge("keyType")?)?;
        let row = edit.type_id(edit.node(stream.into())?.edge("elementType")?)?;
        let key_ref = edit.reference_type(key)?;
        let row_ref = edit.reference_type(row)?;
        let pair = edit.tuple_type(stream.into(), true, [key_ref.into(), row_ref.into()])?;
        let var = edit.variable(pair, &mut names)?;
        let key_field = edit.tuple_field(var, 0)?;
        let row_field = edit.tuple_field(var, 1)?;
        let missing = edit.empty_origin_node(
            body,
            InnerKind::Expression(Expr::NoExpression),
            [("type", Field::Reference(right_type))],
        )?;
        let reduced = reduce_closure_body(
            &mut edit,
            body,
            &params,
            &[key_field.into(), row_field.into(), missing],
        )?;
        if !uses_missing_value(&edit, reduced)? {
            let closure = edit.closure_from_variables(edit.expression(reduced)?, &[var])?;
            edit.project_left_input(join, closure)?;
            removed.push(join);
        }
    }
    if visited != keyed_joins.len() {
        return Err(Error::new("key analysis returned an unknown join"));
    }
    let result = rebuild_input_closures(edit.finish_with_rewrites()?, &mut names)?;
    *next = names;
    Ok((result, removed))
}

pub(super) fn uses_missing_value(edit: &CircuitEditor, root: NodeId) -> Result<bool, Error> {
    let mut pending = vec![root];
    let mut visited = HashSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        let node = edit.node(id)?;
        match node.inner_kind() {
            Some(InnerKind::Expression(Expr::NoExpression)) => return Ok(true),
            Some(InnerKind::Type(_)) => continue,
            _ => (),
        }
        for (_, field) in node.fields() {
            field.collect_inner_edges(&mut pending);
        }
    }
    Ok(false)
}
