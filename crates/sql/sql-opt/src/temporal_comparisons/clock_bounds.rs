//! Bounds for temporal predicates over an unordered input row and the system clock.
use crate::function_calls::function_name;
use crate::lexical_scope::predicate_references;
use crate::monotonicity::binary::{BinaryWaterline, Domain, Operand, binary_waterline};
use crate::monotonicity::casts::{CastWaterline, cast_waterline};
use crate::monotonicity::functions::{Argument, function_waterline};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, NodeId, TypeKind,
};
use std::collections::{HashMap, HashSet};

/// Java writes a DOUBLE literal's raw IEEE 754 bits as a signed long.
fn double_literal(edit: &CircuitEditor, id: ExpressionId) -> Result<Option<f64>, Error> {
    if edit.expr_kind(id)? != Expr::DoubleLiteral {
        return Ok(None);
    }
    edit.node(id.into())?
        .data("value")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_i64()
                .map(|bits| f64::from_bits(bits as u64))
                .ok_or_else(|| Error::new("invalid DOUBLE literal bits in clock predicate"))
        })
        .transpose()
}

/// Return the monotone operands when the entire predicate uses scalar columns,
/// clock calls, immutable scalar bindings and supported scalar operations.
pub(super) fn monotone_operands(
    edit: &CircuitEditor,
    parameter: NodeId,
    body: ExpressionId,
) -> Result<Option<HashSet<ExpressionId>>, Error> {
    let row = edit.node(parameter)?;
    let mut references = None;
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
        if matches!(kind, Expr::Let | Expr::Block) && references.is_none() {
            references = predicate_references(edit.circuit(), parameter, body.into())?;
            if references.is_none() {
                return Ok(None);
            }
        }
        match kind {
            Expr::Let | Expr::Block => {
                let result = if kind == Expr::Let {
                    child("consumer")?
                } else if node
                    .field("lastExpression")
                    .and_then(|field| field.node_id())
                    .is_some()
                {
                    child("lastExpression")?
                } else {
                    return Ok(None);
                };
                if !visited_children {
                    pending.extend([(id, true), (result, false)]);
                    if kind == Expr::Let {
                        pending.push((child("initializer")?, false));
                    } else {
                        for statement in node.edges("contents")?.into_iter().rev() {
                            let binding = edit.node(statement)?;
                            if binding.class() != "DBSPLetStatement"
                                || binding.data("mutable").and_then(|v| v.as_bool()) == Some(true)
                            {
                                return Ok(None);
                            }
                            let Some(init) = binding.field("initializer").and_then(|f| f.node_id())
                            else {
                                return Ok(None);
                            };
                            pending.push((edit.expression(init)?, false));
                        }
                    }
                    continue;
                }
                value.monotone = facts[&result].monotone;
                value.constant = facts[&result].constant;
            }
            Expr::VariablePath => {
                let Some(declaration) = references.as_ref().and_then(|refs| refs.get(&id.into()))
                else {
                    return Ok(None);
                };
                if *declaration == parameter {
                    return Ok(None);
                }
                let initializer = edit.expression(edit.node(*declaration)?.edge("initializer")?)?;
                let Some(initial) = facts.get(&initializer) else {
                    return Ok(None);
                };
                if !edit.same_type(
                    edit.expression_type(id)?,
                    edit.expression_type(initializer)?,
                )? {
                    return Ok(None);
                }
                // The closure analysis tracks constants by expression identity. Reading a
                // bound variable inherits its waterline but does not make the read a literal.
                value.monotone = initial.monotone;
            }
            Expr::Field => {
                let source = child("expression")?;
                if edit.expr_kind(source)? != Expr::Deref {
                    return Ok(None);
                }
                let var = edit.expression(edit.node(source.into())?.edge("expression")?)?;
                let path = edit.node(var.into())?;
                if edit.expr_kind(var)? != Expr::VariablePath
                    || path.data("variable") != row.data("name")
                    || references
                        .as_ref()
                        .is_some_and(|refs| refs.get(&var.into()) != Some(&parameter))
                    || !edit
                        .same_type(edit.expression_type(var)?, edit.type_id(row.edge("type")?)?)?
                    || matches!(
                        edit.type_kind(edit.expression_type(id)?)?,
                        TypeKind::Tuple | TypeKind::RawTuple | TypeKind::Ref
                    )
                {
                    return Ok(None);
                }
            }
            Expr::Apply => {
                let Some(name) = function_name(edit.circuit(), node)? else {
                    return Ok(None);
                };
                let args = node.edges("arguments")?;
                if name == "now" {
                    if !args.is_empty() {
                        return Ok(None);
                    }
                    value.monotone = true;
                } else {
                    if !visited_children && !args.is_empty() {
                        pending.push((id, true));
                        for arg in args.into_iter().rev() {
                            pending.push((edit.expression(arg)?, false));
                        }
                        continue;
                    }
                    let mut arguments = Vec::with_capacity(args.len());
                    for arg in args {
                        let arg = edit.expression(arg)?;
                        let operand = facts[&arg];
                        arguments.push(Argument {
                            monotone: operand.monotone,
                            constant: operand.constant,
                            double_literal_above_one: double_literal(edit, arg)?
                                .is_some_and(|base| base > 1.0),
                        });
                    }
                    value.constant = arguments.iter().all(|arg| arg.constant);
                    value.monotone = function_waterline(&name, &arguments).is_some();
                }
            }
            Expr::Cast => {
                let source = child("source")?;
                if !visited_children {
                    pending.extend([(id, true), (source, false)]);
                    continue;
                }
                let safe = match node.data("safe").and_then(|v| v.as_str()) {
                    Some("SqlSafe") => true,
                    Some("SqlUnsafe") => false,
                    _ => return Ok(None),
                };
                value.constant = facts[&source].constant;
                if facts[&source].monotone {
                    // Every scalar bound here has its operand's domain. Removing
                    // nullability can change an ordinary cast to identity, both monotone.
                    let from = edit.expression_type(source)?;
                    value.monotone =
                        cast_waterline(edit, from, edit.expression_type(id)?, from, safe)?
                            != CastWaterline::None;
                }
            }
            Expr::DateLiteral
            | Expr::TimeLiteral
            | Expr::TimestampLiteral
            | Expr::TimestampTzLiteral
            | Expr::ShortIntervalLiteral
            | Expr::LongIntervalLiteral
            | Expr::I32Literal
            | Expr::I64Literal
            | Expr::BoolLiteral => {
                value.monotone = true;
                // A NULL literal has a bound but cannot serve as a constant offset.
                value.constant = node.data("value").is_some_and(|v| !v.is_null());
                if value.constant
                    && matches!(
                        kind,
                        Expr::I32Literal
                            | Expr::I64Literal
                            | Expr::ShortIntervalLiteral
                            | Expr::LongIntervalLiteral
                    )
                {
                    let literal = node.data("value").expect("nonnull numeric literal");
                    let number = literal
                        .as_i64()
                        .or_else(|| literal.as_str()?.parse().ok())
                        .ok_or_else(|| {
                            Error::new("invalid integer or interval literal in clock predicate")
                        })?;
                    value.positive_literal = number > 0;
                }
            }
            Expr::DoubleLiteral => {
                let literal = double_literal(edit, id)?;
                value.monotone = true;
                value.constant = literal.is_some();
                value.positive_literal = literal.is_some_and(|n| n > 0.0);
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
                let domain = match (kind, opcode) {
                    (Expr::TimeAddSub, _)
                        if edit.type_kind(edit.expression_type(id)?)? == TypeKind::Time =>
                    {
                        Domain::TimeOfDay
                    }
                    (Expr::TimeAddSub, _) => Domain::Temporal,
                    (
                        Expr::Binary,
                        Some(
                            "ADD" | "SUB" | "MUL" | "DIV" | "MUL_INTERVAL" | "DIV_INTERVAL" | "LT"
                            | "LTE" | "GT" | "GTE" | "EQ" | "NEQ" | "AND" | "OR",
                        ),
                    ) => Domain::Scalar,
                    _ => return Ok(None),
                };
                value.monotone = binary_waterline(domain, opcode.unwrap_or(""), left, right)
                    != BinaryWaterline::None;
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
    fn scalar_clock_bounds_follow_cast_and_function_rules() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPTypeTimestamp","id":1,"code":"TIMESTAMP","mayBeNull":false
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let site = roots[0];
        let timestamp = edit.type_id(site)?;
        let row = edit.named_variable(timestamp, "row")?;
        let parameter = edit.parameter_at(site, row)?;
        let now = edit.function_path(site, "now")?;
        let clock = edit.make_expr(
            site,
            Expr::Apply,
            timestamp,
            [
                ("function", Field::Reference(now.into())),
                ("arguments", Field::Array(vec![])),
            ],
        )?;
        let bigint = edit.integer_type(timestamp, 64, true, false)?;
        let epoch = edit.cast(site, clock, bigint)?;
        let offset = edit.make_expr(
            site,
            Expr::I64Literal,
            bigint,
            [("value", Field::Data(86400.into()))],
        )?;
        let sum = edit.binary_expression(site, bigint, BinaryOp::Add, epoch, offset)?;
        assert!(
            monotone_operands(&edit, parameter, sum)?
                .unwrap()
                .contains(&sum)
        );
        for factor in [-1, 0, 3] {
            let scale = edit.make_expr(
                site,
                Expr::I64Literal,
                bigint,
                [("value", Field::Data(factor.to_string().into()))],
            )?;
            let product = edit.binary_expression(site, bigint, BinaryOp::Mul, epoch, scale)?;
            assert_eq!(
                monotone_operands(&edit, parameter, product)?
                    .unwrap()
                    .contains(&product),
                factor > 0
            );
        }
        let narrow = edit.integer_type(bigint, 16, true, true)?;
        for (safety, bounded) in [("SqlSafe", false), ("SqlUnsafe", true)] {
            let cast = edit.make_expr(
                site,
                Expr::Cast,
                narrow,
                [
                    ("source", Field::Reference(epoch.into())),
                    ("safe", Field::Data(safety.into())),
                ],
            )?;
            assert_eq!(
                monotone_operands(&edit, parameter, cast)?
                    .unwrap()
                    .contains(&cast),
                bounded
            );
        }
        for (callee, bounded) in [
            ("extract_year_Timestamp", true),
            ("extract_minute_Timestamp", false),
        ] {
            let path = edit.function_path(site, callee)?;
            let call = edit.make_expr(
                site,
                Expr::Apply,
                bigint,
                [
                    ("function", Field::Reference(path.into())),
                    (
                        "arguments",
                        Field::Array(vec![Field::Reference(clock.into())]),
                    ),
                ],
            )?;
            assert_eq!(
                monotone_operands(&edit, parameter, call)?
                    .unwrap()
                    .contains(&call),
                bounded
            );
        }
        Ok(())
    }

    #[test]
    fn logarithm_clock_bound_requires_a_constant_double_base_above_one() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([
            json!({"class":"DBSPTypeDouble","id":1,"code":"DOUBLE","mayBeNull":false}),
            json!({"class":"DBSPTypeTimestamp","id":2,"code":"TIMESTAMP","mayBeNull":false}),
        ])?;
        let mut edit = CircuitEditor::new(circuit);
        let site = roots[0];
        let double = edit.type_id(site)?;
        let timestamp = edit.type_id(roots[1])?;
        let row = edit.named_variable(timestamp, "row")?;
        let parameter = edit.parameter_at(site, row)?;
        let now = edit.function_path(site, "now")?;
        let clock = edit.make_expr(
            site,
            Expr::Apply,
            timestamp,
            [
                ("function", Field::Reference(now.into())),
                ("arguments", Field::Array(vec![])),
            ],
        )?;
        let seconds = edit.integer_type(timestamp, 64, true, false)?;
        let epoch = edit.cast(site, clock, seconds)?;
        let value = edit.cast(site, epoch, double)?;
        let log = edit.function_path(site, "log_d_d")?;
        for base in [2.0_f64, 1.0, 0.5, -2.0, f64::NAN] {
            let literal = edit.make_expr(
                site,
                Expr::DoubleLiteral,
                double,
                [("value", Field::Data((base.to_bits() as i64).into()))],
            )?;
            let call = edit.make_expr(
                site,
                Expr::Apply,
                double,
                [
                    ("function", Field::Reference(log.into())),
                    (
                        "arguments",
                        Field::Array(vec![
                            Field::Reference(value.into()),
                            Field::Reference(literal.into()),
                        ]),
                    ),
                ],
            )?;
            assert_eq!(
                monotone_operands(&edit, parameter, call)?
                    .unwrap()
                    .contains(&call),
                base > 1.0
            );
        }
        Ok(())
    }

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
        // Shadowing the same name resolves an initializer in the enclosing
        // scope and its consumer in the new scope.
        let first = edit.let_statement("t", clock)?;
        let read_clock = edit.named_variable(timestamp, "t")?;
        let inner = edit.named_variable(timestamp, "t")?;
        let read_inner = edit.named_variable(timestamp, "t")?;
        let shadow = edit.let_expression(inner, read_clock, read_inner)?;
        let block = edit.make_expr(
            roots[0],
            Expr::Block,
            timestamp,
            [
                ("contents", Field::Array(vec![Field::Reference(first)])),
                ("lastExpression", Field::Reference(shadow.into())),
            ],
        )?;
        let comparison = edit.binary_expression(roots[0], boolean, BinaryOp::Gt, key, block)?;
        assert_eq!(
            infer_temporal_predicates(&mut edit, parameter, comparison)?,
            Some(vec![TemporalPredicate::Bound {
                key,
                clock: block,
                comparison: WindowComparison::Greater
            }])
        );
        let interval = edit.expression(roots[1])?;
        let interval_type = edit.expression_type(interval)?;
        let local = edit.named_variable(interval_type, "offset")?;
        let offset = edit.named_variable(interval_type, "offset")?;
        let difference = edit.make_expr(
            roots[0],
            Expr::TimeAddSub,
            timestamp,
            [
                ("left", Field::Reference(clock.into())),
                ("right", Field::Reference(offset.into())),
                ("opcode", Field::Data("SUB".into())),
            ],
        )?;
        let expr = edit.let_expression(local, interval, difference)?;
        assert!(
            !monotone_operands(&edit, parameter, expr)?
                .unwrap()
                .contains(&expr)
        );
        // A reference outside the filter's binding needs lexical resolution.
        let other = edit.named_variable(reference, "other")?;
        let other_parameter = edit.parameter_at(roots[0], other)?;
        assert!(monotone_operands(&edit, other_parameter, key)?.is_none());
        Ok(())
    }
}
