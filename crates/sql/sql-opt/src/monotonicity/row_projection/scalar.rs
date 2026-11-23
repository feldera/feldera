//! Scalar literals and arithmetic inside a projected waterline closure.
use super::{type_number, ProjectedValue, RowType, Shape};
use crate::monotonicity::binary::{
    binary_waterline, BinaryOperation, BinaryWaterline, Domain, Operand,
};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, TypeKind as Ty,
};

pub(super) fn literal(kind: Expr) -> bool {
    matches!(
        kind,
        Expr::BoolLiteral
            | Expr::I8Literal
            | Expr::I16Literal
            | Expr::I32Literal
            | Expr::I64Literal
            | Expr::I128Literal
            | Expr::U8Literal
            | Expr::U16Literal
            | Expr::U32Literal
            | Expr::U64Literal
            | Expr::U128Literal
            | Expr::ISizeLiteral
            | Expr::USizeLiteral
            | Expr::DecimalLiteral
            | Expr::RealLiteral
            | Expr::DoubleLiteral
            | Expr::DateLiteral
            | Expr::TimeLiteral
            | Expr::TimestampLiteral
            | Expr::TimestampTzLiteral
            | Expr::ShortIntervalLiteral
            | Expr::LongIntervalLiteral
    )
}

impl ProjectedValue {
    pub(super) fn bounded(
        edit: &CircuitEditor,
        expression: ExpressionId,
        bound: ExpressionId,
        constant: bool,
    ) -> Result<Self, Error> {
        let type_id = type_number(edit.expression_type(expression)?);
        Ok(Self {
            shape: Shape::Scalar(true),
            row_type: RowType::BoundedScalar { type_id },
            reduced: Some(bound),
            constant,
        })
    }
}

/// Mirror getWithNullable(false), including the fresh, unlocated SQL types
/// built by integer and boolean literal constructors.
pub(super) fn literal_bound(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
) -> Result<Option<ProjectedValue>, Error> {
    let site = expression.into();
    let ty = edit.expression_type(expression)?;
    let kind = edit.expr_kind(expression)?;
    let Some(value) = edit
        .node(site)?
        .data("value")
        .filter(|v| !v.is_null())
        .cloned()
    else {
        // These maxima depend on the target. Java reports the unsupported
        // operation at the SQL site when a NULL literal needs that maximum.
        if matches!(kind, Expr::ISizeLiteral | Expr::USizeLiteral) {
            return Ok(None);
        }
        let required = edit.with_nullability(ty, false)?;
        let maximum = edit.scalar_maximum(required)?;
        return ProjectedValue::bounded(edit, expression, maximum, false).map(Some);
    };
    let sql_type = edit.type_kind(ty)?;
    let bound = match sql_type {
        Ty::ISize | Ty::USize => edit.required_size_literal(expression)?,
        Ty::Integer | Ty::Bool | Ty::Real | Ty::Double => {
            let required = match sql_type {
                Ty::Integer => {
                    let node = edit.node(ty.into())?;
                    let property = |key| {
                        node.data(key)
                            .cloned()
                            .map(Field::Data)
                            .ok_or_else(|| Error::new(format!("missing integer {key}")))
                    };
                    let fields = [
                        ("width", property("width")?),
                        ("signed", property("signed")?),
                        ("code", property("code")?),
                        ("mayBeNull", Field::Data(false.into())),
                    ];
                    edit.empty_origin_node(site, InnerKind::Type(Ty::Integer), fields)?
                }
                Ty::Bool => edit.empty_origin_node(
                    site,
                    InnerKind::Type(Ty::Bool),
                    [
                        ("code", Field::Data("BOOL".into())),
                        ("mayBeNull", Field::Data(false.into())),
                    ],
                )?,
                _ => edit.float_literal_type(ty, false)?.into(),
            };
            let bound = edit.empty_origin_node(
                site,
                InnerKind::Expression(kind),
                [
                    ("type", Field::Reference(required)),
                    ("value", Field::Data(value)),
                ],
            )?;
            edit.expression(bound)?
        }
        _ => {
            let required = edit.with_nullability(ty, false)?;
            edit.make_expr(site, kind, required, [("value", Field::Data(value))])?
        }
    };
    ProjectedValue::bounded(edit, expression, bound, true).map(Some)
}

fn never_null(edit: &CircuitEditor, mut expression: ExpressionId) -> Result<bool, Error> {
    while edit.nullable(edit.expression_type(expression)?)? {
        let node = edit.node(expression.into())?;
        if edit.expr_kind(expression)? != Expr::Cast
            || node.data("safe").and_then(|v| v.as_str()) != Some("SqlUnsafe")
        {
            return Ok(false);
        }
        expression = edit.expression(node.edge("source")?)?;
    }
    Ok(true)
}

fn operand(
    edit: &CircuitEditor,
    expression: ExpressionId,
    value: &ProjectedValue,
    opcode: &str,
) -> Result<Operand, Error> {
    Ok(Operand {
        monotone: value.reduced.is_some(),
        constant: value.constant,
        positive_literal: matches!(opcode, "MUL" | "DIV" | "MUL_INTERVAL" | "DIV_INTERVAL")
            && literal(edit.expr_kind(expression)?)
            && feldera_sql_expr::literal_is_positive(edit, expression)?,
        never_null: opcode == "MAX_IGNORE_NULLS" && never_null(edit, expression)?,
    })
}

pub(super) fn unary_bound(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
    source: ProjectedValue,
) -> Result<Option<ProjectedValue>, Error> {
    if !matches!(source.shape, Shape::Scalar(_)) {
        return Ok(None);
    }
    let reduced = match source.reduced {
        None => None,
        Some(bound) if edit.expr_kind(expression)? == Expr::Cast => {
            super::super::casts::cast_bound(edit, expression, bound)?
        }
        Some(bound) => super::super::unary_waterline(edit, expression, bound)?,
    };
    if let Some(bound) = reduced {
        return ProjectedValue::bounded(edit, expression, bound, source.constant).map(Some);
    }
    let mut projected = super::unbounded(edit, expression)?;
    if let Some(value) = &mut projected {
        value.constant = source.constant;
    }
    Ok(projected)
}

pub(super) fn binary_bound(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
    [left, right]: [ExpressionId; 2],
    [lhs, rhs]: [ProjectedValue; 2],
) -> Result<Option<ProjectedValue>, Error> {
    let opcode = edit
        .node(expression.into())?
        .data("opcode")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("missing waterline opcode"))?
        .to_owned();
    let ty = edit.expression_type(expression)?;
    let result_kind = edit.type_kind(ty)?;
    let domain = match (edit.expr_kind(expression)?, result_kind) {
        (Expr::TimeAddSub, Ty::Time) => Domain::TimeOfDay,
        (Expr::TimeAddSub, _) => Domain::Temporal,
        _ => Domain::Scalar,
    };
    let operation = BinaryOperation {
        domain,
        opcode: &opcode,
        left: operand(edit, left, &lhs, &opcode)?,
        right: operand(edit, right, &rhs, &opcode)?,
    };
    let scalar_operands = matches!(
        (&lhs.shape, &rhs.shape),
        (Shape::Scalar(_), Shape::Scalar(_))
    );
    let scalar_result = !matches!(
        result_kind,
        Ty::RawTuple | Ty::Tuple | Ty::Ref | Ty::Stream | Ty::Struct
    );
    if !(scalar_operands && scalar_result)
        && binary_waterline(domain, &opcode, operation.left, operation.right)
            != BinaryWaterline::None
    {
        return Ok(None);
    }
    let constant = lhs.constant && rhs.constant;
    let (_, bound) = operation.lower(edit, expression.into(), ty, [lhs.reduced, rhs.reduced])?;
    if let Some(bound) = bound {
        return ProjectedValue::bounded(edit, expression, bound, constant).map(Some);
    }
    // Retain the layout of an unbounded ROW for subsequent field reads.
    let mut value = super::unbounded(edit, expression)?;
    if let Some(value) = &mut value {
        value.constant = constant;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn deep_arithmetic_and_unary_clock_bounds_include_each_intermediate_expression(
    ) -> Result<(), Error> {
        let arena = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?;
        let site = arena.root_id();
        let mut edit = CircuitEditor::new(arena);
        let flag = edit.bool_type(site, false)?;
        let ty = edit.integer_type(flag, 32, true, false)?;
        let row = edit.reference_type(ty)?;
        let var = edit.named_variable(row, "row")?;
        let parameter = edit.parameter_at(site, var)?;
        let unknown = edit.dereference(var)?;
        let mut expr = unknown;
        let mut bounded = Vec::new();
        for n in 0..256 {
            let literal = edit.make_expr(
                site,
                Expr::I32Literal,
                ty,
                [("value", Field::Data(n.into()))],
            )?;
            expr =
                edit.binary_expression(site, ty, feldera_sql_ir::BinaryOp::Max, expr, literal)?;
            bounded.push(expr);
            expr = edit.make_expr(
                site,
                Expr::Unary,
                ty,
                [
                    ("source", Field::Reference(expr.into())),
                    ("opcode", Field::Data("UNARY_PLUS".into())),
                ],
            )?;
            bounded.push(expr);
        }
        let clocks =
            super::super::clock_operands(&mut edit, parameter, expr)?.expect("clock operands");
        assert!(!clocks.contains(&unknown));
        assert!(bounded.iter().all(|id| clocks.contains(id)));
        Ok(())
    }

    #[test]
    fn integer_bounds_keep_the_wire_type_for_subsequent_operations() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?;
        let mut edit = CircuitEditor::new(circuit);
        let site = edit.circuit().root_id();
        let origin = edit.bool_type(site, false)?;
        for (width, signed, kind) in [(16, true, Expr::I16Literal), (32, false, Expr::U32Literal)] {
            let ty = edit.integer_type(origin, width, signed, true)?;
            let literal = edit.make_expr(site, kind, ty, [("value", Field::Data("2".into()))])?;
            let bound = literal_bound(&mut edit, literal)?.unwrap().reduced.unwrap();
            let required = edit.expression_type(bound)?;
            let original = edit.node(ty.into())?;
            let projected = edit.node(required.into())?;
            for key in ["code", "width", "signed"] {
                assert_eq!(projected.data(key), original.data(key));
            }
            assert!(!edit.nullable(required)?);
            assert!(edit.scalar_maximum(required).is_ok());
        }
        Ok(())
    }
}
