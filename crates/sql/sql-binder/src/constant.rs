//! Fold a bounded set of constant projections with the shared P0 evaluator.

use super::{BoundExpression, ExpressionKind, Failure, Kind, unsupported};
use feldera_fold_probe::{Expression, Refusal, SemanticsProfile, SqlType, evaluate_with_profile};
use feldera_sql_rel_model::literals::LiteralValue;

pub(super) fn runtime_error(bound: &BoundExpression) -> bool {
    expression(bound).is_ok_and(|expr| {
        matches!(
            evaluate_with_profile(&expr, SemanticsProfile::LegacyJanino),
            Err(Refusal::RuntimeError(_))
        )
    })
}

pub(super) fn fold(bound: BoundExpression) -> Result<BoundExpression, Failure> {
    let request = expression(&bound)?;
    let result = match evaluate_with_profile(&request, SemanticsProfile::LegacyJanino) {
        Ok(value) => value,
        // Calcite keeps these checked calls so that the generated circuit
        // reports an error if execution reaches the expression.
        Err(Refusal::RuntimeError(_)) => return Ok(bound),
        Err(_) => return Err(unsupported("constant evaluation declined the projection")),
    };
    let value = match (
        result["type"].as_str(),
        bound.ty.kind,
        result["value"].as_str(),
    ) {
        (Some("null"), _, _) if bound.ty.nullable => LiteralValue::Null {},
        (Some("integer"), Kind::Integer { .. }, Some(value)) => LiteralValue::Integer {
            value: value.into(),
        },
        (Some("float"), Kind::Double, Some(value)) => {
            let bits = u64::from_str_radix(value, 16)
                .map_err(|_| unsupported("invalid floating-point fold result"))?;
            if !f64::from_bits(bits).is_finite() {
                return Err(unsupported("nonfinite constant projection"));
            }
            LiteralValue::Float64 { bits }
        }
        _ => return Err(unsupported("constant fold returned an unsupported type")),
    };
    Ok(BoundExpression {
        kind: ExpressionKind::FoldedLiteral { value },
        ..bound
    })
}

fn expression(bound: &BoundExpression) -> Result<Expression, Failure> {
    let ty = match bound.ty.kind {
        Kind::Integer { bits: 8 } => "TINYINT",
        Kind::Integer { bits: 16 } => "SMALLINT",
        Kind::Integer { bits: 32 } => "INTEGER",
        Kind::Integer { bits: 64 } => "BIGINT",
        Kind::Double => "DOUBLE",
        _ => {
            return Err(unsupported(
                "constant projection type is outside the fold scope",
            ));
        }
    };
    let (kind, operator, operands, value) = match &bound.kind {
        ExpressionKind::TypedLiteral { literal, .. } => (
            "LITERAL",
            None,
            &[][..],
            (!literal.eq_ignore_ascii_case("NULL")).then(|| literal.clone()),
        ),
        ExpressionKind::Coerce { inner } => (
            "CAST",
            Some("CAST"),
            std::slice::from_ref(inner.as_ref()),
            None,
        ),
        ExpressionKind::Binary { operator, operands }
            if matches!(bound.ty.kind, Kind::Integer { .. }) =>
        {
            let kind = match *operator {
                "+" => "CHECKED_PLUS",
                "-" => "CHECKED_MINUS",
                "*" => "CHECKED_TIMES",
                "/" => "CHECKED_DIVIDE",
                _ => {
                    return Err(unsupported(
                        "constant binary operator is outside the fold scope",
                    ));
                }
            };
            (kind, Some(*operator), operands.as_slice(), None)
        }
        ExpressionKind::Function { name, operands } if name == "TAN" => {
            ("OTHER_FUNCTION", Some("TAN"), operands.as_slice(), None)
        }
        _ => return Err(unsupported("computed constant is outside the fold scope")),
    };
    Ok(Expression {
        kind: kind.into(),
        ty: SqlType {
            name: ty.into(),
            precision: -1,
            scale: -1,
            component: None,
            key: None,
            value: None,
        },
        operator: operator.map(str::to_owned),
        operator_class: None,
        builtin: None,
        deterministic: Some(true),
        dynamic: false,
        arguments: operands.iter().map(expression).collect::<Result<_, _>>()?,
        value,
    })
}
