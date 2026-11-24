//! Scalar literals and arithmetic inside a projected waterline closure.
use super::{
    ProjectedValue, RowType, Shape, WaterlineError, WaterlineTypeError, evaluate::Evaluation,
    type_number,
};
use crate::monotonicity::binary::{
    BinaryOperation, BinaryWaterline, Domain, Operand, binary_waterline,
};
use crate::monotonicity::literal::{LiteralWaterline, literal_waterline, ordered_literal};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, TypeId, TypeKind as Ty,
};

/// Direct construction of a scalar monotonicity type accepts references. A
/// non-monotone reference built this way differs from a reference whose base
/// is non-monotone.
pub(super) fn scalar_type(
    edit: &CircuitEditor,
    ty: TypeId,
    monotone: bool,
    constant: bool,
) -> Result<Evaluation, Error> {
    if matches!(edit.type_kind(ty)?, Ty::RawTuple | Ty::Tuple) {
        return Ok(Evaluation::Failed(WaterlineError::Type(
            WaterlineTypeError::ExpectedScalar,
            ty,
        )));
    }
    let type_id = type_number(ty);
    Ok(Evaluation::Value(ProjectedValue {
        shape: Shape::Scalar(monotone),
        row_type: if monotone {
            RowType::BoundedScalar { type_id }
        } else {
            RowType::UnboundedScalar { type_id }
        },
        reduced: None,
        constant,
    }))
}

/// These types reject withMayBeNull(false), even when already nonnullable.
pub(super) fn required_type_error(kind: Ty, ty: TypeId) -> Option<WaterlineError> {
    matches!(kind, Ty::Keyword | Ty::Function | Ty::Null | Ty::Stream)
        .then_some(WaterlineError::Type(WaterlineTypeError::RequiredType, ty))
}

fn copy_scalar_type(
    edit: &CircuitEditor,
    ty: TypeId,
    source: ProjectedValue,
    constant: bool,
) -> Result<Evaluation, Error> {
    match source.shape {
        Shape::Scalar(monotone) => scalar_type(edit, ty, monotone, constant),
        Shape::Opaque(_) => Ok(Evaluation::NeedsVisitor),
        _ => Ok(Evaluation::Failed(WaterlineError::ExpectedScalarOperand(
            source.row_type,
        ))),
    }
}

pub(super) fn literal_bound(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
) -> Result<Evaluation, Error> {
    match literal_waterline(edit, expression)? {
        LiteralWaterline::Bounded { bound, constant } => {
            Ok(
                scalar_type(edit, edit.expression_type(expression)?, true, constant)?
                    .with_bound(Some(bound)),
            )
        }
        LiteralWaterline::Unbounded => super::unbounded(edit, expression, false),
        LiteralWaterline::TargetMaximum => Ok(Evaluation::Failed(WaterlineError::TargetMaximum(
            expression,
        ))),
    }
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
        monotone: value.shape.monotone(),
        constant: value.constant,
        positive_literal: matches!(opcode, "MUL" | "DIV" | "MUL_INTERVAL" | "DIV_INTERVAL")
            && ordered_literal(edit.expr_kind(expression)?)
            && feldera_sql_expr::literal_is_positive(edit, expression)?,
        never_null: opcode == "MAX_IGNORE_NULLS" && never_null(edit, expression)?,
    })
}

/// Casts and unsigned wrappers read the input bound before testing their rule.
/// Other unary operators only read it when they preserve order.
fn reads_unary_bound(edit: &CircuitEditor, expression: ExpressionId) -> Result<bool, Error> {
    if edit.expr_kind(expression)? != Expr::Unary {
        return Ok(true);
    }
    let opcode = super::super::opcode(edit.node(expression.into())?)
        .ok_or_else(|| Error::new("missing unary opcode"))?;
    Ok(super::super::unary_preserves_order(opcode))
}

pub(super) fn unary_bound(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
    source: ProjectedValue,
) -> Result<Evaluation, Error> {
    let kind = edit.expr_kind(expression)?;
    let ty = edit.expression_type(expression)?;
    let constant = source.constant;
    // The ordinary unary visitor constructs the unbounded result even when
    // the operand later supplies a bound. A cast constructs it only on failure.
    let default = if kind == Expr::Unary {
        match super::unbounded_type(edit, ty, constant)? {
            Evaluation::Value(value) => Some(value),
            failure => return Ok(failure),
        }
    } else {
        None
    };
    if source.shape.monotone() && reads_unary_bound(edit, expression)? {
        if kind == Expr::Unary {
            if let Some(error) = required_type_error(edit.type_kind(ty)?, ty) {
                return Ok(Evaluation::Failed(error));
            }
        }
        if source.reduced.is_none() {
            let input = edit.expression(edit.node(expression.into())?.edge("source")?)?;
            return Ok(Evaluation::Failed(WaterlineError::MissingBound(input)));
        }
    }
    let reduced = match (kind, source.reduced) {
        (Expr::Cast, Some(bound)) => super::super::casts::cast_bound(edit, expression, bound)?,
        (_, Some(bound)) => super::super::unary_waterline(edit, expression, bound)?,
        (_, None) => None,
    };
    if reduced.is_some() || kind == Expr::UnsignedUnwrap {
        return Ok(copy_scalar_type(edit, ty, source, constant)?.with_bound(reduced));
    }
    match default {
        Some(value) => Ok(Evaluation::Value(value)),
        None if kind == Expr::UnsignedWrap => scalar_type(edit, ty, false, constant),
        None => super::unbounded_type(edit, ty, constant),
    }
}

/// A one-sided MAX and each side of COALESCE cast the selected bound
/// immediately. A failing left cast precedes a missing right bound.
fn binary_operand_error(
    edit: &CircuitEditor,
    expression: ExpressionId,
    rule: BinaryWaterline,
    operands: [ExpressionId; 2],
    bounds: [Option<ExpressionId>; 2],
    ty: TypeId,
) -> Result<Option<WaterlineError>, Error> {
    use BinaryWaterline::{Coalesce, Left, Right};
    let target = edit.type_kind(ty)?;
    let required = || required_type_error(target, ty);
    if rule == Coalesce {
        if let Some(error) = required() {
            return Ok(Some(error));
        }
    }
    for (i, (operand, bound)) in operands.into_iter().zip(bounds).enumerate() {
        if (i == 0 && rule == Right) || (i == 1 && rule == Left) {
            continue;
        }
        let Some(bound) = bound else {
            return Ok(Some(WaterlineError::MissingBound(operand)));
        };
        if matches!(rule, Coalesce | Left | Right) {
            if let Some(error) = required() {
                return Ok(Some(error));
            }
            let from = edit.expression_type(bound)?;
            let source = edit.type_kind(from)?;
            let tuple = |kind| matches!(kind, Ty::Tuple | Ty::RawTuple);
            let incompatible = tuple(source) != tuple(target)
                && !matches!(source, Ty::Null | Ty::Variant | Ty::SqlResult)
                && target != Ty::Variant;
            // An identity cast returns its operand without constructing a cast.
            if incompatible
                || target == Ty::SqlResult && !edit.same_type_ignoring_nullability(from, ty)?
            {
                return Ok(Some(WaterlineError::InvalidBoundCast {
                    operation: expression,
                    bound,
                }));
            }
        }
    }
    Ok(required())
}

pub(super) fn binary_bound(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
    [left, right]: [ExpressionId; 2],
    [lhs, rhs]: [ProjectedValue; 2],
) -> Result<Evaluation, Error> {
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
    let constant = lhs.constant && rhs.constant;
    let default = match domain {
        Domain::Scalar => super::unbounded_type(edit, ty, constant)?,
        Domain::TimeOfDay | Domain::Temporal => scalar_type(edit, ty, false, constant)?,
    };
    let Evaluation::Value(_) = default else {
        return Ok(default);
    };
    let operation = BinaryOperation {
        domain,
        opcode: &opcode,
        left: operand(edit, left, &lhs, &opcode)?,
        right: operand(edit, right, &rhs, &opcode)?,
    };
    let rule = binary_waterline(domain, &opcode, operation.left, operation.right);
    let bounds = [lhs.reduced, rhs.reduced];
    let result = match rule {
        BinaryWaterline::None => return Ok(default),
        BinaryWaterline::InheritLeft => copy_scalar_type(edit, ty, lhs, constant)?,
        BinaryWaterline::InheritRight => copy_scalar_type(edit, ty, rhs, constant)?,
        _ => scalar_type(edit, ty, true, constant)?,
    };
    let Evaluation::Value(_) = result else {
        return Ok(result);
    };
    if let Some(error) = binary_operand_error(edit, expression, rule, [left, right], bounds, ty)? {
        return Ok(Evaluation::Failed(error));
    }
    let (_, bound) = operation.lower(edit, expression.into(), ty, bounds)?;
    Ok(result.with_bound(bound))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{Circuit, Field};
    use serde_json::json;

    #[test]
    fn unbounded_row_cast_uses_the_nullable_target_type() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let field = edit.bool_type(site, false)?;
        let row = edit.tuple_type(site, false, [field.into()])?;
        let target = edit.with_nullability(row, true)?;
        let input = edit.named_variable(row, "payload")?;
        let cast = edit.cast(site, input, target)?;
        for constant in [false, true] {
            let source = super::super::unbounded(&mut edit, input, constant)?
                .projected()
                .unwrap();
            let value = unary_bound(&mut edit, cast, source)?
                .projected()
                .expect("unbounded row cast");
            assert!(value.reduced.is_none());
            assert_eq!(value.constant, constant);
            assert!(matches!(value.shape, Shape::Tuple { nullable: true, .. }));
            assert!(
                matches!(value.row_type, RowType::Unbounded { type_id } if type_id == type_number(target))
            );
        }
        Ok(())
    }

    #[test]
    fn deep_arithmetic_and_unary_clock_bounds_include_each_intermediate_expression()
    -> Result<(), Error> {
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
            let bound = literal_bound(&mut edit, literal)?
                .projected()
                .unwrap()
                .reduced
                .unwrap();
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
