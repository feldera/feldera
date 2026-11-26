//! Express equality with a clock-dependent value as two inclusive temporal bounds.
use crate::{filters::rewrite_filters, function_calls::contains_now};
use feldera_sql_expr::{copy_expression, ensure_expression_tree, Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, NodeId, OperatorKind,
    OutputPort, TypeKind as Ty,
};

struct ClockEquality;

impl Transformation for ClockEquality {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        Ok(
            if matches!(edit.node(id)?.inner_kind(), Some(InnerKind::Type(_))) {
                Visit::Keep
            } else {
                Visit::Children
            },
        )
    }

    fn postorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        let node = edit.node(id)?;
        if !matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::Binary)))
            || node.data("opcode").and_then(|v| v.as_str()) != Some("EQ")
        {
            return Ok(id);
        }
        let left = node.edge("left")?;
        let right = node.edge("right")?;
        let ty = edit.type_kind(edit.expression_type(edit.expression(left)?)?)?;
        if !matches!(
            ty,
            Ty::Binary
                | Ty::Bool
                | Ty::Date
                | Ty::Decimal
                | Ty::Double
                | Ty::GeoPoint
                | Ty::ISize
                | Ty::Integer
                | Ty::Keyword
                | Ty::LongInterval
                | Ty::Null
                | Ty::Real
                | Ty::ShortInterval
                | Ty::Str
                | Ty::String
                | Ty::Time
                | Ty::Timestamp
                | Ty::TimestampTz
                | Ty::USize
                | Ty::Uuid
                | Ty::Variant
                | Ty::Void
        ) || contains_now(edit.circuit(), &[left])? == contains_now(edit.circuit(), &[right])?
        {
            return Ok(id);
        }
        let binary = |edit: &mut CircuitEditor, opcode: &str, left, right| {
            edit.derive_inner(
                id,
                [
                    ("opcode", Field::Data(opcode.into())),
                    ("left", Field::Reference(left)),
                    ("right", Field::Reference(right)),
                ],
            )
        };
        let ge = binary(edit, "GTE", left, right)?;
        let left = copy_expression(edit, left)?;
        let right = copy_expression(edit, right)?;
        let le = binary(edit, "LTE", left, right)?;
        binary(edit, "AND", ge, le)
    }
}

pub fn desugar_now_equality(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    Ok(
        rewrite_filters::<Error>(circuit, next, |edit, body, id, _, _| {
            let node = edit.node(id)?;
            let closure = node.edge("function")?;
            if !contains_now(edit.circuit(), &[closure])? {
                return Ok(None);
            }
            let output = node.edge("outputType")?;
            let input = node
                .inputs()
                .next()
                .ok_or_else(|| Error::new("filter requires an input"))?;
            let tree = ensure_expression_tree(edit, closure)?;
            let rewritten = Rewrite::new(ClockEquality).apply(edit, tree)?;
            if rewritten == closure {
                return Ok(None);
            }
            let filter = edit.append_operator(
                body,
                id,
                OperatorKind::Filter,
                &[input],
                [
                    ("function", Field::Reference(rewritten)),
                    ("outputType", Field::Reference(output)),
                ],
            )?;
            Ok(Some((
                filter,
                OutputPort {
                    operator: filter,
                    number: 0,
                },
            )))
        })?
        .0,
    )
}
