//! Lower a computation chain to a map or flat-map over its original input.
use super::{chain_output, chain_prefix, Computation};
use crate::{
    collapse_shared_chains::compose, fanout::expensive, projection_composition::should_inline,
    rebuild::ProjectionNames, stream_rewrites::body_members,
};
use feldera_sql_expr::{reduce_closure_body, simplify_closure_expression};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field,
    InnerKind, NodeId, OperatorKind as Op, OutputPort, TypeId, TypeKind as Ty,
};
use std::collections::{BTreeMap, HashMap};

type Stage = (Computation, NodeId);

fn shrink(edit: &mut CircuitEditor, stages: Vec<Stage>) -> Result<Vec<Stage>, Error> {
    let mut reduced = Vec::<Stage>::new();
    for (kind, mut closure) in stages {
        if let Some(&(previous, before)) = reduced.last() {
            if kind != Computation::Filter
                && previous != Computation::Filter
                && !expensive(edit.circuit(), before)?
            {
                closure = compose(
                    edit,
                    edit.expression(before)?,
                    previous == Computation::MapIndex,
                    closure,
                )?
                .into();
                reduced.pop();
            }
        }
        reduced.push((kind, closure));
    }
    // Moving a filter ahead of a map allows the next iteration to collapse a
    // newly adjacent triple. The two original consumers can choose independently.
    let mut i = 0;
    while i + 2 < reduced.len() {
        let (first, before) = reduced[i];
        let (middle, filter) = reduced[i + 1];
        let (last, after) = reduced[i + 2];
        if first == Computation::Map
            && middle == Computation::Filter
            && last != Computation::Filter
            && should_inline(edit, filter, before)?
            && should_inline(edit, after, before)?
        {
            let before = edit.expression(before)?;
            let predicate = compose(edit, before, false, filter)?;
            let map = compose(edit, before, false, after)?;
            reduced.splice(
                i..i + 3,
                [(Computation::Filter, predicate.into()), (last, map.into())],
            );
            i = i.saturating_sub(2);
        } else {
            i += 1;
        }
    }
    Ok(reduced)
}

fn row_parameter(
    edit: &mut CircuitEditor,
    site: NodeId,
    stream: TypeId,
    next: &mut u64,
) -> Result<(ExpressionId, ExpressionId), Error> {
    let element = edit.type_id(edit.node(stream.into())?.edge("elementType")?)?;
    let value = edit.reference_type(element)?;
    if edit.type_kind(stream)? == Ty::ZSet {
        let path = edit.variable(value, next)?;
        return Ok((path, edit.dereference(path)?));
    }
    let key = edit.type_id(edit.node(stream.into())?.edge("keyType")?)?;
    let key = edit.reference_type(key)?;
    let pair = edit.tuple_type(site, true, [key.into(), value.into()])?;
    let path = edit.variable(pair, next)?;
    let (key, value) = (edit.tuple_field(path, 0)?, edit.tuple_field(path, 1)?);
    let fields = [edit.dereference(key)?, edit.dereference(value)?];
    Ok((path, edit.tuple_expr(site, true, &fields)?))
}

fn call(
    edit: &mut CircuitEditor,
    closure: NodeId,
    row: ExpressionId,
) -> Result<ExpressionId, Error> {
    let arg = if edit.type_kind(edit.expression_type(row)?)? == Ty::RawTuple {
        let (k, v) = (edit.tuple_field(row, 0)?, edit.tuple_field(row, 1)?);
        let pair = [edit.borrow_value(k)?, edit.borrow_value(v)?];
        edit.tuple_expr(row.into(), true, &pair)?
    } else {
        edit.borrow_value(row)?
    };
    let function = edit.node(closure)?;
    let result = reduce_closure_body(
        edit,
        function.edge("body")?,
        &function.edges("parameters")?,
        &[arg.into()],
    )?;
    edit.expression(result)
}

fn filter_statement(
    edit: &mut CircuitEditor,
    site: NodeId,
    condition: ExpressionId,
    none: ExpressionId,
) -> Result<NodeId, Error> {
    let negated = edit.make_expr(
        condition.into(),
        Expr::Unary,
        edit.expression_type(condition)?,
        [
            ("opcode", Field::Data("NOT".into())),
            ("source", Field::Reference(condition.into())),
        ],
    )?;
    let condition = simplify_closure_expression(edit, negated.into())?;
    let void = edit.standard_type(site, Ty::Void)?;
    let ret = edit.make_expr(
        site,
        Expr::Return,
        void,
        [("argument", Field::Reference(none.into()))],
    )?;
    let predicate = edit.node(condition)?;
    let literal = match predicate.inner_kind() {
        Some(InnerKind::Expression(Expr::BoolLiteral)) => {
            Some(predicate.data("value").and_then(|v| v.as_bool()) == Some(true))
        }
        _ => None,
    };
    let guard = match literal {
        Some(true) => ret,
        Some(false) => {
            let unit = edit.empty_origin_node(
                site,
                InnerKind::Expression(Expr::VoidLiteral),
                [("type", Field::Reference(void.into()))],
            )?;
            edit.expression(unit)?
        }
        None => edit.make_expr(
            site,
            Expr::If,
            void,
            [
                ("condition", Field::Reference(condition)),
                ("positive", Field::Reference(ret.into())),
            ],
        )?,
    };
    edit.inner_at(
        guard.into(),
        InnerKind::Other("DBSPExpressionStatement".into()),
        [("expression", Field::Reference(guard.into()))],
    )
}

fn collapse(
    edit: &mut CircuitEditor,
    site: NodeId,
    input: TypeId,
    stages: &[Stage],
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    if let [(Computation::Map | Computation::MapIndex, function)] = stages {
        return edit.expression(*function);
    }
    let (parameter, mut row) = row_parameter(edit, site, input, next)?;
    let has_filter = stages.iter().any(|s| s.0 == Computation::Filter);
    let none = if has_filter {
        let stream = chain_output(edit, stages, input)?;
        let node = edit.node(stream.into())?;
        let element = node.edge("elementType")?;
        let value = if edit.type_kind(stream)? == Ty::IndexedZSet {
            edit.tuple_type(site, true, [node.edge("keyType")?, element])?
        } else {
            edit.type_id(element)?
        };
        let nullable = edit.with_nullability(value, true)?;
        Some(edit.typed_null(nullable)?)
    } else {
        None
    };
    let mut statements = Vec::with_capacity(stages.len());
    let mut mapped = false;
    for &(kind, closure) in stages {
        let result = call(edit, closure, row)?;
        let statement = if kind == Computation::Filter {
            filter_statement(edit, closure, result, none.expect("chain has a filter"))?
        } else {
            mapped = true;
            let function_type = edit.node(closure)?.edge("type")?;
            let ty = edit.type_id(edit.node(function_type)?.edge("resultType")?)?;
            row = edit.variable(ty, next)?;
            let name = edit
                .node(row.into())?
                .field("variable")
                .expect("constructed variable")
                .clone();
            edit.inner_at(
                result.into(),
                InnerKind::Other("DBSPLetStatement".into()),
                [
                    ("variable", name),
                    (
                        "type",
                        Field::Reference(edit.expression_type(result)?.into()),
                    ),
                    ("initializer", Field::Reference(result.into())),
                    ("mutable", Field::Data(false.into())),
                ],
            )?
        };
        statements.push(Field::Reference(statement));
    }
    if !mapped {
        row = edit.clone_if_needed(row)?;
    }
    if has_filter {
        row = edit.some_at(row.into(), row)?;
    }
    let block = edit.empty_origin_node(
        site,
        InnerKind::Expression(Expr::Block),
        [
            ("type", Field::Reference(edit.expression_type(row)?.into())),
            ("contents", Field::Array(statements)),
            ("lastExpression", Field::Reference(row.into())),
        ],
    )?;
    edit.closure_from_variables(edit.expression(block)?, &[parameter])
}

pub fn implement_chains(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let owners: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    let mut edit = CircuitEditor::new(circuit);
    let mut projections = ProjectionNames::default();
    let mut replacements = BTreeMap::new();
    for id in order {
        let op = edit.node(id)?;
        projections.visit(id, op, next)?;
        if op.operator_kind() != Some(Op::Chain) {
            continue;
        }
        let body = owners[&id];
        let input = super::unary_input(op)?;
        let stream = edit.type_id(op.edge("inputType")?)?;
        let output = edit.type_id(op.edge("outputType")?)?;
        let stages = chain_prefix(&edit, op)?;
        let reduced = shrink(&mut edit, stages)?;
        let filter = reduced.iter().any(|stage| stage.0 == Computation::Filter);
        let closure = collapse(&mut edit, id, stream, &reduced, next)?;
        let kind = match (edit.type_kind(output)?, filter) {
            (Ty::ZSet, false) => Op::Map,
            (Ty::ZSet, true) => Op::FlatMap,
            (Ty::IndexedZSet, false) => Op::MapIndex,
            (Ty::IndexedZSet, true) => Op::FlatMapIndex,
            _ => return Err(Error::new("chain must produce a collection")),
        };
        let replacement = edit.append_operator(
            body,
            id,
            kind,
            &[input],
            [
                ("outputType", Field::Reference(output.into())),
                ("function", Field::Reference(closure.into())),
            ],
        )?;
        edit.place_before(body, replacement, id)?;
        replacements.insert(
            id,
            OutputPort {
                operator: replacement,
                number: 0,
            },
        );
        projections.replaced(id);
    }
    projections.finish(edit.finish_forwarding(&replacements)?)
}
