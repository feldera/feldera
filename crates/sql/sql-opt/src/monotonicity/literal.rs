//! Lower bounds of SQL literals used by monotonicity analysis.
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, TypeKind as Ty,
};

pub(super) fn ordered_literal(kind: Expr) -> bool {
    kind.is_numeric_literal()
        || matches!(
            kind,
            Expr::BoolLiteral
                | Expr::DateLiteral
                | Expr::TimeLiteral
                | Expr::TimestampLiteral
                | Expr::TimestampTzLiteral
        )
}

pub enum LiteralWaterline {
    Unbounded,
    /// A target-sized integer has no compiler-defined maximum for a NULL bound.
    TargetMaximum,
    Bounded {
        bound: ExpressionId,
        constant: bool,
    },
}

/// A non-NULL ordered literal bounds itself. A NULL bounds no data values,
/// so its maximum is a valid waterline, but it cannot serve as a constant factor.
pub fn literal_waterline(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
) -> Result<LiteralWaterline, Error> {
    let kind = edit.expr_kind(expression)?;
    if !ordered_literal(kind) {
        return Ok(LiteralWaterline::Unbounded);
    }
    let site = expression.into();
    let ty = edit.expression_type(expression)?;
    let Some(value) = edit
        .node(site)?
        .data("value")
        .filter(|v| !v.is_null())
        .cloned()
    else {
        if matches!(kind, Expr::ISizeLiteral | Expr::USizeLiteral) {
            return Ok(LiteralWaterline::TargetMaximum);
        }
        let required = edit.with_nullability(ty, false)?;
        let bound = edit.scalar_maximum(required)?;
        return Ok(LiteralWaterline::Bounded {
            bound,
            constant: false,
        });
    };
    // Integer and boolean literal constructors allocate fresh types with an empty origin.
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
    Ok(LiteralWaterline::Bounded {
        bound,
        constant: true,
    })
}
