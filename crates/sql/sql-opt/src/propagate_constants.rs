//! Evaluate filters, maps and weight negation on constant collections.
pub(super) mod rows;

use crate::{
    multiset::{input_fact, record_outputs},
    rebuild::rebuild_input_closures,
    stream_rewrites::body_members,
};
use feldera_sql_expr::{
    reduce_closure_body_with_warnings, simplify_closure_with_warnings, SimplifyWarning,
};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field,
    InnerKind, NodeId, OperatorKind as Op, OutputPort, TypeId,
};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Constructors are constant when all of their value operands are constant.
fn constant_value(edit: &CircuitEditor, root: ExpressionId) -> Result<bool, Error> {
    let mut pending = vec![root];
    let mut seen = HashSet::new();
    while let Some(value) = pending.pop() {
        if !seen.insert(NodeId::from(value)) {
            continue;
        }
        let fields: &[&str] = match edit.expr_kind(value)? {
            Expr::Tuple | Expr::RawTuple => &["fields"],
            Expr::Array | Expr::ZSet => &["data"],
            Expr::IndexedZSet => &[],
            Expr::Map => &["keys", "values"],
            Expr::GeoPointConstructor => &["left", "right"],
            Expr::Variant => &["value"],
            kind if kind.wire_name().ends_with("Literal") => continue,
            _ => return Ok(false),
        };
        for key in fields {
            if let Some(field) = edit.node(value.into())?.field(key) {
                let mut children = Vec::new();
                field.collect_inner_edges(&mut children);
                for child in children {
                    pending.push(edit.expression(child)?);
                }
            }
        }
    }
    Ok(true)
}

fn evaluate_row(
    edit: &mut CircuitEditor,
    closure: ExpressionId,
    row: ExpressionId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let function = edit.node(closure.into())?;
    let body = function.edge("body")?;
    let params = function.edges("parameters")?;
    let arg = edit.borrow_value(row)?;
    let body =
        reduce_closure_body_with_warnings(edit, body, &params, &[arg.into()], Some(warnings))?;
    let value = simplify_closure_with_warnings(edit, body, Some(warnings))?;
    edit.expression(value)
}

pub(super) fn multiset(
    edit: &mut CircuitEditor,
    element: TypeId,
    entries: impl IntoIterator<Item = (ExpressionId, i64)>,
) -> Result<ExpressionId, Error> {
    let ty = edit.zset_type(element)?;
    let (data, weights) = entries
        .into_iter()
        .map(|(v, w)| (Field::Reference(v.into()), Field::Data(w.into())))
        .unzip();
    let literal = edit.empty_origin_node(
        element.into(),
        InnerKind::Expression(Expr::ZSet),
        [
            ("elementType", Field::Reference(element.into())),
            ("type", Field::Reference(ty.into())),
            ("data", Field::Array(data)),
            ("weights", Field::Array(weights)),
        ],
    )?;
    edit.expression(literal)
}

pub(super) fn evaluate_constant_input(
    edit: &mut CircuitEditor,
    id: NodeId,
    constant: NodeId,
    op: Op,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<ExpressionId>, Error> {
    let value = edit.expression(edit.node(constant)?.edge("function")?)?;
    match edit.expr_kind(value)? {
        Expr::IndexedZSet if op == Op::Negate => return Ok(Some(value)),
        Expr::ZSet => (),
        _ => return Ok(None),
    }
    let literal = edit.node(value.into())?;
    let mut element = edit.type_id(literal.edge("elementType")?)?;
    let entries = rows::entries(edit, value)?;
    let mut result = Vec::with_capacity(entries.len());
    if op == Op::Negate {
        for (row, weight) in entries {
            rows::append(edit, &mut result, (row, weight.wrapping_neg()))?;
        }
        return multiset(edit, element, result).map(Some);
    }
    let function = edit.expression(edit.node(id)?.edge("function")?)?;
    if op == Op::Map {
        element =
            edit.expression_type(edit.expression(edit.node(function.into())?.edge("body")?)?)?;
    }
    for (row, weight) in entries {
        let value = evaluate_row(edit, function, row, warnings)?;
        if op == Op::Filter {
            if edit.expr_kind(value)? != Expr::BoolLiteral {
                return Ok(None);
            }
            match edit
                .node(value.into())?
                .data("value")
                .and_then(|v| v.as_bool())
            {
                Some(true) => {
                    result.push((row, weight));
                }
                Some(false) => (),
                None => return Ok(None),
            }
        } else {
            if !constant_value(edit, value)? {
                return Ok(None);
            }
            rows::append(edit, &mut result, (value, weight))?;
        }
    }
    multiset(edit, element, result).map(Some)
}

pub fn propagate_constants(
    circuit: Circuit,
    next: &mut u64,
) -> Result<(Circuit, Vec<SimplifyWarning>), Error> {
    let bodies: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut facts = crate::stream_multiplicities(&circuit)?;
    let mut replacements = BTreeMap::<NodeId, OutputPort>::new();
    let mut warnings = Vec::new();
    let mut edit = CircuitEditor::new(circuit);
    for id in order {
        let consumer = edit.node(id)?;
        let Some(op @ (Op::Map | Op::Filter | Op::Negate | Op::Distinct)) =
            consumer.operator_kind()
        else {
            continue;
        };
        let source = consumer
            .inputs()
            .next()
            .ok_or_else(|| Error::new("constant propagation requires one input"))?;
        let source = replacements
            .get(&source.operator)
            .copied()
            .unwrap_or(source);
        if op == Op::Distinct {
            if edit.node(source.operator)?.operator_kind() != Some(Op::Nested)
                && !input_fact(&facts, source)?
            {
                replacements.insert(id, source);
            }
            continue;
        }
        if edit.node(source.operator)?.operator_kind() != Some(Op::Constant) {
            continue;
        }
        let Some(value) =
            evaluate_constant_input(&mut edit, id, source.operator, op, &mut warnings)?
        else {
            continue;
        };
        let site = if op == Op::Negate {
            id
        } else {
            source.operator
        };
        let output = edit.expression_type(value)?;
        let replacement = edit.append_operator(
            bodies[&id],
            site,
            Op::Constant,
            &[],
            [
                ("outputType", Field::Reference(output.into())),
                ("function", Field::Reference(value.into())),
            ],
        )?;
        edit.place_before(bodies[&id], replacement, id)?;
        record_outputs(&edit, replacement, &mut facts)?;
        replacements.insert(
            id,
            OutputPort {
                operator: replacement,
                number: 0,
            },
        );
    }
    let circuit = rebuild_input_closures(edit.finish_forwarding(&replacements)?, next)?;
    Ok((circuit, warnings))
}
