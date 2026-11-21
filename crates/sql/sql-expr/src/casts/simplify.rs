//! Simplifications applied to a column default before expanding its SQL casts.

use super::{arithmetic, literals, unwrap_option, visit_expression};
use crate::{Transformation, Visit};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
    TypeKind as Ty,
};

pub struct SimplifyWarning {
    pub position: feldera_sql_ir::provenance::SourceRange,
    pub kind: &'static str,
    pub message: String,
}

pub(crate) struct SimplifyDefault<'a> {
    pub warnings: Option<&'a mut Vec<SimplifyWarning>>,
}

/// Called only after constant folding has declined the conversion. Fractional
/// seconds beyond microsecond precision are deliberately left to the runtime.
fn text_cast_warning(text: &str, target: Ty) -> Option<SimplifyWarning> {
    let trimmed = text.trim_matches(|c: char| c <= ' ');
    let (kind, description, prefix) = match target {
        Ty::Bool => ("Suspicious argument", "BOOLEAN", ""),
        Ty::Date => ("Suspicious argument", "DATE", ""),
        Ty::Time => ("Not a TIME", "time", " "),
        Ty::Timestamp => {
            if trimmed.is_ascii()
                && trimmed.rsplit_once('.').is_some_and(|(_, fraction)| {
                    fraction.len() > 6 && fraction.bytes().all(|b| b.is_ascii_digit())
                })
            {
                return None;
            }
            ("Not a TIMESTAMP", "timestamp", " ")
        }
        Ty::Uuid => ("Not a UUID", "UUID", " "),
        Ty::Integer => ("Not a number", "number", " "),
        _ => return None,
    };
    Some(SimplifyWarning {
        position: Default::default(),
        kind,
        message: format!("{prefix}String '{text}' cannot be interpreted as a {description}"),
    })
}

fn cast_position(
    editor: &CircuitEditor,
    cast: NodeId,
) -> Result<feldera_sql_ir::provenance::SourceRange, Error> {
    Ok(editor
        .node(cast)?
        .field("position")
        .map(Field::source_range)
        .transpose()?
        .unwrap_or_default())
}

impl SimplifyDefault<'_> {
    fn warn_unfolded_cast(
        &mut self,
        editor: &CircuitEditor,
        cast: NodeId,
        source: ExpressionId,
        target: TypeId,
    ) -> Result<(), Error> {
        let Some(warnings) = &mut self.warnings else {
            return Ok(());
        };
        let source_type = editor.type_kind(editor.expression_type(source)?)?;
        let target_kind = editor.type_kind(target)?;
        let Some(text) = editor
            .node(source.into())?
            .data("value")
            .and_then(|v| v.as_str())
        else {
            return Ok(());
        };
        let warning = if target_kind == Ty::Decimal {
            let input = match source_type {
                Ty::String if editor.expr_kind(source)? == Expr::StringLiteral => {
                    let trimmed = text.trim_matches(|c: char| c <= ' ');
                    if !trimmed.is_ascii()
                        || trimmed.contains('_')
                        || trimmed.parse::<bigdecimal::BigDecimal>().is_err()
                    {
                        let mut warning = SimplifyWarning {
                            position: Default::default(),
                            kind: "Not a number",
                            message: format!(" String '{text}' cannot be interpreted as a DECIMAL"),
                        };
                        warning.position = cast_position(editor, cast)?;
                        warnings.push(warning);
                        return Ok(());
                    }
                    format!("'{text}'")
                }
                Ty::Integer | Ty::Decimal
                    if editor.expr_kind(source)?.wire_name().ends_with("Literal") =>
                {
                    if editor.node(cast)?.data("safe").and_then(|v| v.as_str())
                        == Some("Saturating")
                    {
                        return Ok(());
                    }
                    if source_type == Ty::Decimal
                        && editor.nullable(editor.expression_type(source)?)?
                    {
                        format!("Some({text})")
                    } else {
                        text.to_owned()
                    }
                }
                _ => return Ok(()),
            };
            let target = editor.node(target.into())?;
            let precision = target
                .data("precision")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| Error::new("DECIMAL requires a precision"))?;
            let scale = target
                .data("scale")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| Error::new("DECIMAL requires a scale"))?;
            Some(SimplifyWarning {
                position: Default::default(),
                kind: "Invalid DECIMAL",
                message: format!("cannot represent {input} as DECIMAL({precision}, {scale}): precision of DECIMAL type too small to represent value"),
            })
        } else if editor.expr_kind(source)? == Expr::StringLiteral {
            text_cast_warning(text, target_kind)
        } else {
            None
        };
        if let Some(mut warning) = warning {
            warning.position = cast_position(editor, cast)?;
            warnings.push(warning);
        }
        Ok(())
    }
}

impl Transformation for SimplifyDefault<'_> {
    fn preorder(&mut self, editor: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        visit_expression(editor, id)
    }

    fn postorder_rebuilt(
        &mut self,
        editor: &mut CircuitEditor,
        original: NodeId,
        id: NodeId,
    ) -> Result<NodeId, Error> {
        let node = editor.node(id)?;
        match node.inner_kind() {
            Some(InnerKind::Expression(Expr::Unary)) => simplify_unary(editor, id),
            Some(InnerKind::Expression(Expr::Binary)) => {
                match node.data("opcode").and_then(|op| op.as_str()) {
                    Some("EQ" | "NEQ" | "IS_DISTINCT" | "LT" | "LTE" | "GT" | "GTE") => {
                        super::comparison::fold(editor, id)
                    }
                    Some("AND") => simplify_boolean(editor, id, true),
                    Some("OR") => simplify_boolean(editor, id, false),
                    Some("MAP_INDEX" | "SQL_INDEX" | "RUST_INDEX") => {
                        let left = editor.node(node.edge("left")?)?;
                        if matches!(left.inner_kind(), Some(InnerKind::Expression(Expr::Clone))) {
                            editor.derive_inner(
                                id,
                                [("left", Field::Reference(left.edge("expression")?))],
                            )
                        } else {
                            Ok(id)
                        }
                    }
                    _ => arithmetic::simplify(editor, id, self.warnings.as_deref_mut()),
                }
            }
            Some(InnerKind::Expression(Expr::Cast)) => {
                let source = editor.expression(node.edge("source")?)?;
                let target = editor.type_id(node.edge("type")?)?;
                if editor.is_null_literal(source)? && editor.nullable(target)? {
                    return editor.typed_null(target).map(Into::into);
                }
                if editor.expr_kind(source)? == Expr::StringLiteral
                    && !editor.is_null_literal(source)?
                    && editor.type_kind(target)? == Ty::String
                {
                    return string_literal(editor, id, source, target);
                }
                if !editor
                    .same_type_ignoring_nullability(editor.expression_type(source)?, target)?
                {
                    if let Some(folded) = literals::fold(editor, original, source, target)? {
                        return Ok(folded.into());
                    }
                    self.warn_unfolded_cast(editor, id, source, target)?;
                }
                Ok(id)
            }
            Some(InnerKind::Expression(Expr::Unwrap)) => {
                let source = editor.expression(node.edge("expression")?)?;
                let message = node
                    .data("message")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| Error::new("unwrap requires an error message"))?
                    .to_owned();
                unwrap_option(editor, id, source, &message)
            }
            _ => Ok(id),
        }
    }
}

fn simplify_unary(editor: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
    let unary = editor.node(id)?;
    let op = unary
        .data("opcode")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("unary expression requires an opcode"))?;
    let ty = editor.type_id(unary.edge("type")?)?;
    let source = editor.expression(unary.edge("source")?)?;
    let operand = editor.node(source.into())?;
    let value = match (editor.expr_kind(source)?, op) {
        (Expr::Fail, _) => {
            let message = operand
                .field("message")
                .ok_or_else(|| Error::new("failure requires a message"))?
                .clone();
            editor.make_expr(id, Expr::Fail, ty, [("message", message)])?
        }
        (_, "NEG") if editor.expr_kind(source)?.wire_name().ends_with("Literal") => {
            let Some(negated) = arithmetic::negate(editor, source)? else {
                return Ok(id);
            };
            negated
        }
        (Expr::BoolLiteral, _) => {
            let value = operand.data("value").and_then(|v| v.as_bool());
            let folded = match op {
                "NOT" => match value {
                    Some(b) => !b,
                    None => return Ok(source.into()),
                },
                "WRAP_BOOL" | "IS_TRUE" => value == Some(true),
                "IS_FALSE" => value == Some(false),
                "IS_NOT_TRUE" => value.unwrap_or(true),
                "IS_NOT_FALSE" => !value.unwrap_or(false),
                _ => return Ok(id),
            };
            editor.make_expr(
                id,
                Expr::BoolLiteral,
                ty,
                [("value", Field::Data(folded.into()))],
            )?
        }
        (Expr::Unary, "NOT") if operand.data("opcode").and_then(|v| v.as_str()) == Some("NOT") => {
            editor.expression(operand.edge("source")?)?
        }
        (Expr::Cast, "WRAP_BOOL") => {
            let inner = editor.expression(operand.edge("source")?)?;
            let inner_ty = editor.expression_type(inner)?;
            if editor.type_kind(inner_ty)? != Ty::Bool || editor.nullable(inner_ty)? {
                return Ok(id);
            }
            inner
        }
        (
            Expr::If,
            "NOT" | "NEG" | "WRAP_BOOL" | "IS_TRUE" | "IS_FALSE" | "IS_NOT_TRUE" | "IS_NOT_FALSE",
        ) => {
            let (yes, no) = (operand.edge("positive")?, operand.edge("negative")?);
            let yes = editor.derive_inner(id, [("source", Field::Reference(yes))])?;
            let no = editor.derive_inner(id, [("source", Field::Reference(no))])?;
            let branch = editor.derive_inner(
                source.into(),
                [
                    ("type", Field::Reference(ty.into())),
                    ("positive", Field::Reference(yes)),
                    ("negative", Field::Reference(no)),
                ],
            )?;
            editor.expression(branch)?
        }
        _ => return Ok(id),
    };
    editor.cast(id, value, ty).map(Into::into)
}

fn simplify_boolean(editor: &mut CircuitEditor, id: NodeId, and: bool) -> Result<NodeId, Error> {
    let binary = editor.node(id)?;
    let ty = editor.type_id(binary.edge("type")?)?;
    let left = editor.expression(binary.edge("left")?)?;
    let right = editor.expression(binary.edge("right")?)?;
    for (operand, other) in [(left, right), (right, left)] {
        if editor.expr_kind(operand)? == Expr::BoolLiteral {
            let Some(value) = editor
                .node(operand.into())?
                .data("value")
                .and_then(|v| v.as_bool())
            else {
                return Ok(id);
            };
            let result = if value == and { other } else { operand };
            return editor.cast(id, result, ty).map(Into::into);
        }
    }

    // A conjunction can test for SQL TRUE once, after combining both nullable operands.
    let unwrap = |operand: ExpressionId| -> Result<Option<NodeId>, Error> {
        let node = editor.node(operand.into())?;
        if matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::Unary)))
            && node.data("opcode").and_then(|v| v.as_str()) == Some("WRAP_BOOL")
        {
            return node.edge("source").map(Some);
        }
        Ok(None)
    };
    if and {
        if let (Some(a), Some(b)) = (unwrap(left)?, unwrap(right)?) {
            let inner_type = editor.expression_type(editor.expression(a)?)?;
            let combined = editor.make_expr(
                id,
                Expr::Binary,
                inner_type,
                [
                    ("left", Field::Reference(a)),
                    ("right", Field::Reference(b)),
                    ("opcode", Field::Data("AND".into())),
                ],
            )?;
            return editor.wrap_bool_if_needed(combined).map(Into::into);
        }
    }
    Ok(id)
}

fn string_literal(
    editor: &mut CircuitEditor,
    origin: NodeId,
    source: ExpressionId,
    target: TypeId,
) -> Result<NodeId, Error> {
    let ty = editor.node(target.into())?;
    let precision = ty
        .data("precision")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| Error::new("string type requires a precision"))?;
    let fixed = ty
        .data("fixed")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| Error::new("string type requires fixed"))?;
    let literal = editor
        .node(source.into())?
        .data("value")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("expected a non-null string literal"))?;
    let value = if precision == -1 {
        literal.to_owned()
    } else {
        let width =
            usize::try_from(precision).map_err(|_| Error::new("invalid string precision"))?;
        let mut prefix: String = literal.chars().take(width).collect();
        if fixed {
            let spaces = width.saturating_sub(prefix.chars().count());
            prefix
                .try_reserve(spaces)
                .map_err(|_| Error::new("string default exceeds available memory"))?;
            prefix.extend(std::iter::repeat_n(' ', spaces));
        }
        prefix
    };
    let fields = [
        ("type", Field::Reference(target.into())),
        ("value", Field::Data(value.into())),
        ("charset", Field::Data("UTF-8".into())),
    ];
    let kind = InnerKind::Expression(Expr::StringLiteral);
    if precision == -1 {
        editor.empty_origin_node(origin, kind, fields)
    } else {
        editor.inner_at(origin, kind, fields)
    }
}
