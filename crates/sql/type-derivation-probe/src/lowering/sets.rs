//! Scalar maps and filters along the two arms of the set-operation probes.
use super::{
    Projection, case_arm,
    expression::{Expression, ExpressionKind, print_type},
    literals,
};
use crate::model::{DerivedType, Kind, Outcome, SqlType};
use serde::Serialize;

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct SetProjection {
    operator: &'static str,
    #[serde(flatten)]
    projection: Projection,
}

fn record(operator: &'static str, expression: &Expression) -> SetProjection {
    SetProjection {
        operator,
        projection: Projection {
            expression_text: expression.print(),
            type_text: print_type(expression.ty),
        },
    }
}

fn column(ty: SqlType, variable: usize, ordinal: usize) -> Expression {
    Expression {
        ty,
        kind: ExpressionKind::Column { variable, ordinal },
    }
}

fn arm(
    ty: SqlType,
    peer: SqlType,
    result: SqlType,
    ordinal: usize,
    literal: Option<&str>,
) -> Option<Expression> {
    let source = match literal {
        Some(text) => literals::parse(ty, text)?,
        None => column(ty, ordinal + 1, ordinal),
    };
    let target = ty.with_kind(result.kind);
    let mixed_datetime = (matches!(ty.kind, Kind::Date | Kind::Timestamp { .. }) && peer.numeric())
        || (matches!(peer.kind, Kind::Date | Kind::Timestamp { .. }) && ty.numeric());
    let expression = if mixed_datetime || (ty.character() && !peer.character()) {
        source
    } else {
        case_arm(source, target)?
    };
    if expression.ty.character() && matches!(expression.kind, ExpressionKind::Cast(_)) {
        Some(Expression {
            ty: expression.ty,
            kind: ExpressionKind::Clone(Box::new(expression)),
        })
    } else {
        Some(expression)
    }
}

pub fn lower_set_projections(
    operation: &str,
    declared: &[SqlType],
    literals: &[Option<&str>],
    outcome: &Outcome,
) -> Option<Vec<SetProjection>> {
    if !matches!(operation, "UNION ALL" | "INTERSECT ALL" | "EXCEPT ALL") {
        return None;
    }
    let [left, right] = declared else { return None };
    let [left_literal, right_literal] = literals else {
        return None;
    };
    let Outcome::Bound {
        result: DerivedType::Scalar(result),
        ..
    } = outcome
    else {
        return None;
    };
    let left = arm(*left, *right, *result, 0, *left_literal)?;
    let right = arm(*right, declared[0], *result, 1, *right_literal)?;
    let mut maps = vec![record("map", &left), record("map", &right)];
    let mut variable = 3;
    for input in [left.ty, right.ty] {
        if operation != "UNION ALL" && input.nullable && !result.nullable {
            let ty = SqlType {
                kind: Kind::Boolean,
                nullable: false,
            };
            let is_null = Expression {
                ty,
                kind: ExpressionKind::IsNull(Box::new(column(input, variable, 0))),
            };
            maps.push(record(
                "filter",
                &Expression {
                    ty,
                    kind: ExpressionKind::Not(Box::new(is_null)),
                },
            ));
            variable += 1;
        }
        if input != *result {
            let expression = column(input, variable, 0).cast(*result);
            maps.push(record("map", &expression));
            // Tuple casting creates a variable for the input and one for the
            // intermediate tuple before reduction produces the final closure.
            variable += 2;
        }
    }
    Some(maps)
}
