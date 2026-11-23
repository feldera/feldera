//! Scalar literals and arithmetic inside a projected waterline closure.
use super::{ProjectedValue, RowType, Shape, WaterlineError, evaluate::Evaluation, type_number};
use crate::monotonicity::binary::{
    BinaryOperation, BinaryWaterline, Domain, Operand, binary_waterline,
};
use crate::monotonicity::literal::{LiteralWaterline, literal_waterline, ordered_literal};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, TypeKind as Ty};

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

pub(super) fn literal_bound(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
) -> Result<Evaluation, Error> {
    match literal_waterline(edit, expression)? {
        LiteralWaterline::Bounded { bound, constant } => {
            ProjectedValue::bounded(edit, expression, bound, constant).map(Evaluation::Value)
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
pub(super) fn reads_unary_bound(
    edit: &CircuitEditor,
    expression: ExpressionId,
) -> Result<bool, Error> {
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
    let reduced = match (kind, source.reduced) {
        (Expr::Cast, None) => None,
        (Expr::Cast, Some(bound)) => super::super::casts::cast_bound(edit, expression, bound)?,
        (Expr::UnsignedUnwrap, _) if !matches!(source.shape, Shape::Scalar(_)) => {
            return Ok(Evaluation::NeedsVisitor);
        }
        (_, Some(bound)) => super::super::unary_waterline(edit, expression, bound)?,
        (_, None) => None,
    };
    if let Some(bound) = reduced {
        // A successful transfer copies the input's scalar monotonicity. Keep the
        // visitor's diagnostic if that operation is requested on a tuple or reference.
        if !matches!(source.shape, Shape::Scalar(_)) {
            return Ok(Evaluation::NeedsVisitor);
        }
        return ProjectedValue::bounded(edit, expression, bound, source.constant)
            .map(Evaluation::Value);
    }
    super::unbounded(edit, expression, source.constant)
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
    let operation = BinaryOperation {
        domain,
        opcode: &opcode,
        left: operand(edit, left, &lhs, &opcode)?,
        right: operand(edit, right, &rhs, &opcode)?,
    };
    let rule = binary_waterline(domain, &opcode, operation.left, operation.right);
    let scalar_operands = matches!(
        (&lhs.shape, &rhs.shape),
        (Shape::Scalar(_), Shape::Scalar(_))
    );
    let scalar_result = !matches!(
        result_kind,
        Ty::RawTuple | Ty::Tuple | Ty::Ref | Ty::Stream | Ty::Struct
    );
    if rule != BinaryWaterline::None {
        // Java constructs the result type (or copies the inherited operand type)
        // before demanding either bound. Preserve failures at that earlier site.
        if !scalar_result
            || (rule == BinaryWaterline::InheritLeft && !matches!(lhs.shape, Shape::Scalar(_)))
            || (rule == BinaryWaterline::InheritRight && !matches!(rhs.shape, Shape::Scalar(_)))
        {
            return Ok(Evaluation::NeedsVisitor);
        }
        if rule != BinaryWaterline::Right && lhs.reduced.is_none() {
            return Ok(Evaluation::Failed(WaterlineError::MissingBound(left)));
        }
        if rule != BinaryWaterline::Left && rhs.reduced.is_none() {
            return Ok(Evaluation::Failed(WaterlineError::MissingBound(right)));
        }
        if !scalar_operands {
            return Ok(Evaluation::NeedsVisitor);
        }
    }
    let constant = lhs.constant && rhs.constant;
    let (_, bound) = operation.lower(edit, expression.into(), ty, [lhs.reduced, rhs.reduced])?;
    if let Some(bound) = bound {
        return ProjectedValue::bounded(edit, expression, bound, constant).map(Evaluation::Value);
    }
    // Retain the layout of an unbounded ROW for subsequent field reads.
    super::unbounded(edit, expression, constant)
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
