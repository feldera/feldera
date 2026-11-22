//! Bounds for temporal predicates over an unordered input row and the system clock.
use crate::function_calls::function_name;
use crate::monotonicity::binary::{BinaryWaterline, Domain, Operand, binary_waterline};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, NodeId, TypeKind,
};
use std::collections::{HashMap, HashSet};

/// Return the monotone operands when the entire predicate uses scalar columns,
/// clock calls, constants and time arithmetic. Other functions and lexical
/// bindings require the general closure analysis.
pub(super) fn monotone_operands(
    edit: &CircuitEditor,
    parameter: NodeId,
    body: ExpressionId,
) -> Result<Option<HashSet<ExpressionId>>, Error> {
    let parameter = edit.node(parameter)?;
    let mut facts = HashMap::<ExpressionId, Operand>::new();
    let mut pending = vec![(body, false)];
    while let Some((id, visited_children)) = pending.pop() {
        if facts.contains_key(&id) {
            continue;
        }
        let node = edit.node(id.into())?;
        let kind = edit.expr_kind(id)?;
        let child = |name| edit.expression(node.edge(name)?);
        let opcode = node.data("opcode").and_then(|v| v.as_str());
        let mut value = Operand::default();
        match kind {
            Expr::Field => {
                let source = child("expression")?;
                if edit.expr_kind(source)? != Expr::Deref {
                    return Ok(None);
                }
                let var = edit.expression(edit.node(source.into())?.edge("expression")?)?;
                let path = edit.node(var.into())?;
                if edit.expr_kind(var)? != Expr::VariablePath
                    || path.data("variable") != parameter.data("name")
                    || !edit.same_type(
                        edit.expression_type(var)?,
                        edit.type_id(parameter.edge("type")?)?,
                    )?
                    || matches!(
                        edit.type_kind(edit.expression_type(id)?)?,
                        TypeKind::Tuple | TypeKind::RawTuple | TypeKind::Ref
                    )
                {
                    return Ok(None);
                }
            }
            Expr::Apply
                if node.edges("arguments")?.is_empty()
                    && function_name(edit.circuit(), node)?.as_deref() == Some("now") =>
            {
                value.monotone = true;
            }
            Expr::DateLiteral
            | Expr::TimestampLiteral
            | Expr::ShortIntervalLiteral
            | Expr::LongIntervalLiteral
            | Expr::I32Literal
            | Expr::I64Literal
            | Expr::BoolLiteral => {
                value.monotone = true;
                // A NULL literal has a bound but cannot serve as a constant offset.
                value.constant = node.data("value").is_some_and(|v| !v.is_null());
            }
            Expr::Binary | Expr::TimeAddSub => {
                let left = child("left")?;
                let right = child("right")?;
                if !visited_children {
                    pending.extend([(id, true), (right, false), (left, false)]);
                    continue;
                }
                let (left, right) = (facts[&left], facts[&right]);
                value.constant = left.constant && right.constant;
                if kind == Expr::TimeAddSub {
                    let domain = if edit.type_kind(edit.expression_type(id)?)? == TypeKind::Time {
                        Domain::TimeOfDay
                    } else {
                        Domain::Temporal
                    };
                    value.monotone = binary_waterline(domain, opcode.unwrap_or(""), left, right)
                        != BinaryWaterline::None;
                } else if !matches!(
                    opcode,
                    Some("LT" | "LTE" | "GT" | "GTE" | "EQ" | "NEQ" | "AND" | "OR")
                ) {
                    return Ok(None);
                }
            }
            Expr::Unary if opcode == Some("WRAP_BOOL") => {
                let source = child("source")?;
                if !visited_children {
                    pending.extend([(id, true), (source, false)]);
                    continue;
                }
                value.constant = facts[&source].constant;
            }
            _ => return Ok(None),
        }
        facts.insert(id, value);
    }
    Ok(Some(
        facts
            .into_iter()
            .filter_map(|(id, fact)| fact.monotone.then_some(id))
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TemporalPredicate, WindowComparison, infer_temporal_predicates};
    use feldera_sql_ir::{BinaryOp, Circuit, Field};
    use serde_json::json;

    #[test]
    fn clock_minus_interval_needs_a_constant_nonnull_offset() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([
            json!({"class":"DBSPApplyExpression","id":1,"arguments":[],
                "type":{"class":"DBSPTypeTimestamp","id":2,"code":"TIMESTAMP","mayBeNull":false},
                "function":{"class":"DBSPPathExpression","id":3,"path":{"class":"DBSPPath","id":4,
                    "components":[{"class":"DBSPSimplePathSegment","id":5,"identifier":"now","genericArgs":[]}]}}}),
            json!({"class":"DBSPShortIntervalLiteral","id":6,"value":"1000",
                "type":{"class":"DBSPTypeShortInterval","id":7,"code":"INTERVAL_SHORT","units":"SECONDS","mayBeNull":false}}),
            json!({"class":"DBSPShortIntervalLiteral","id":8,
                "type":{"class":"DBSPTypeShortInterval","id":9,"code":"INTERVAL_SHORT","units":"SECONDS","mayBeNull":true}}),
        ])?;
        let mut edit = CircuitEditor::new(circuit);
        let clock = edit.expression(roots[0])?;
        let timestamp = edit.expression_type(clock)?;
        let row = edit.tuple_type(roots[0], false, [timestamp.into()])?;
        let reference = edit.reference_type(row)?;
        let row = edit.named_variable(reference, "row")?;
        let parameter = edit.parameter_at(roots[0], row)?;
        let value = edit.dereference(row)?;
        let key = edit.tuple_field(value, 0)?;
        let boolean = edit.bool_type(roots[0], false)?;
        for (offset, is_bound) in [(roots[1], true), (roots[2], false)] {
            let bound = edit.make_expr(
                roots[0],
                Expr::TimeAddSub,
                timestamp,
                [
                    ("left", Field::Reference(clock.into())),
                    ("right", Field::Reference(offset)),
                    ("opcode", Field::Data("SUB".into())),
                ],
            )?;
            let body = edit.binary_expression(roots[0], boolean, BinaryOp::Gte, key, bound)?;
            let expected = if is_bound {
                TemporalPredicate::Bound {
                    key,
                    clock: bound,
                    comparison: WindowComparison::GreaterEqual,
                }
            } else {
                TemporalPredicate::Residual(body)
            };
            assert_eq!(
                infer_temporal_predicates(&mut edit, parameter, body)?,
                Some(vec![expected])
            );
        }
        // A reference outside the filter's binding needs lexical resolution.
        let other = edit.named_variable(reference, "other")?;
        let other_parameter = edit.parameter_at(roots[0], other)?;
        assert!(monotone_operands(&edit, other_parameter, key)?.is_none());
        Ok(())
    }
}
