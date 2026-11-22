//! Compare the canonical spelling of a projection over one bound row. The Java
//! window pass uses printed closures, so cast safety and call result types do
//! not distinguish keys, while operand nullability in a binary expression does.
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Node, NodeId, TypeId,
    TypeKind as Ty,
};
use std::collections::HashSet;

#[derive(Eq, Hash, PartialEq)]
pub(super) struct TimestampKey(Vec<Step>);

#[derive(Eq, Hash, PartialEq)]
enum Step {
    Field(u64),
    Deref,
    Clone,
    Call(String),
    Cast(String),
    VariantField {
        name: String,
        row_nullable: bool,
        name_nullable: bool,
    },
}

impl TimestampKey {
    pub(super) fn read(
        edit: &CircuitEditor,
        mut expression: ExpressionId,
        parameter: &str,
    ) -> Result<Option<Self>, Error> {
        let mut steps = Vec::new();
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(expression) {
                return Err(Error::new("cycle in a window key expression"));
            }
            let node = edit.node(expression.into())?;
            let (step, child) = match edit.expr_kind(expression)? {
                Expr::VariablePath => {
                    return Ok((string(node, "variable") == Some(parameter)).then_some(Self(steps)))
                }
                Expr::Field => {
                    let column = node
                        .data("fieldNo")
                        .and_then(|v| v.as_u64())
                        .ok_or_else(|| Error::new("invalid window key column"))?;
                    (Step::Field(column), node.edge("expression")?)
                }
                Expr::Deref => (Step::Deref, node.edge("expression")?),
                Expr::Clone => (Step::Clone, node.edge("expression")?),
                Expr::Cast => {
                    let Some(target) = cast_spelling(edit, edit.expression_type(expression)?)?
                    else {
                        return Ok(None);
                    };
                    (Step::Cast(target), node.edge("source")?)
                }
                Expr::Apply => {
                    let [argument] = node.edges("arguments")?[..] else {
                        return Ok(None);
                    };
                    let function = edit.expression(node.edge("function")?)?;
                    if edit.expr_kind(function)? != Expr::Path {
                        return Ok(None);
                    }
                    let path = edit.node(edit.node(function.into())?.edge("path")?)?;
                    let [segment] = path.edges("components")?[..] else {
                        return Ok(None);
                    };
                    let segment = edit.node(segment)?;
                    if segment.class() != "DBSPSimplePathSegment"
                        || !segment.edges("genericArgs")?.is_empty()
                    {
                        return Ok(None);
                    }
                    let Some(name) = string(segment, "identifier") else {
                        return Ok(None);
                    };
                    // Punctuation in a raw identifier could print as a different expression.
                    if name.is_empty()
                        || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    {
                        return Ok(None);
                    }
                    (Step::Call(name.to_owned()), argument)
                }
                Expr::Binary if string(node, "opcode") == Some("VARIANT_INDEX") => {
                    let left = edit.expression(node.edge("left")?)?;
                    let right = edit.expression(node.edge("right")?)?;
                    if edit.expr_kind(right)? != Expr::StringLiteral {
                        return Ok(None);
                    }
                    let Some(name) = string(edit.node(right.into())?, "value") else {
                        return Ok(None);
                    };
                    let step = Step::VariantField {
                        name: name.to_owned(),
                        row_nullable: edit.nullable(edit.expression_type(left)?)?,
                        name_nullable: edit.nullable(edit.expression_type(right)?)?,
                    };
                    (step, left.into())
                }
                _ => return Ok(None),
            };
            steps.push(step);
            expression = edit.expression(child)?;
        }
    }
}

fn string<'a>(node: &'a Node, field: &str) -> Option<&'a str> {
    node.data(field)?.as_str()
}

/// Canonical closures rename the parameter and print its type, including
/// both references in the (key, value) parameter of an indexed map.
pub(super) fn row_parameter(
    edit: &CircuitEditor,
    parameter: NodeId,
) -> Result<(TypeId, &str), Error> {
    let parameter = edit.node(parameter)?;
    let ty = edit.type_id(parameter.edge("type")?)?;
    let name = parameter
        .data("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("missing timestamp parameter name"))?;
    Ok((ty, name))
}

/// Match the Java debug printer used for temporal keys.
fn cast_spelling(edit: &CircuitEditor, ty: TypeId) -> Result<Option<String>, Error> {
    let node = edit.node(ty.into())?;
    let mut text = match edit.type_kind(ty)? {
        Ty::Date => "Date".to_owned(),
        Ty::Timestamp => "Timestamp".to_owned(),
        Ty::TimestampTz => "TimestampTz".to_owned(),
        Ty::String => {
            let Some(precision) = node.data("precision").and_then(|v| v.as_i64()) else {
                return Ok(None);
            };
            if precision == -1 {
                "s".to_owned()
            } else {
                let Some(fixed) = node.data("fixed").and_then(|v| v.as_bool()) else {
                    return Ok(None);
                };
                format!("s({precision},{fixed})")
            }
        }
        _ => return Ok(None),
    };
    if edit.nullable(ty)? {
        text.push('?');
    }
    Ok(Some(text))
}

/// Prove that copies of a source row type print identically. The debug printer
/// ignores the original SQL struct attached to a tuple. For an unfamiliar
/// type, the caller can still use the compiler's printer.
pub(super) fn same_row_spelling(
    edit: &CircuitEditor,
    left: TypeId,
    right: TypeId,
) -> Result<bool, Error> {
    if left == right {
        return Ok(true);
    }
    let mut pending = vec![(left, right)];
    let mut compared = HashSet::new();
    while let Some((left, right)) = pending.pop() {
        if left == right || !compared.insert((NodeId::from(left), NodeId::from(right))) {
            continue;
        }
        let kind = edit.type_kind(left)?;
        if kind != edit.type_kind(right)? || edit.nullable(left)? != edit.nullable(right)? {
            return Ok(false);
        }
        match kind {
            Ty::Ref => {
                let a = edit.node(left.into())?;
                let b = edit.node(right.into())?;
                if a.data("mutable") != b.data("mutable") {
                    return Ok(false);
                }
                pending.push((
                    edit.type_id(a.edge("type")?)?,
                    edit.type_id(b.edge("type")?)?,
                ));
            }
            Ty::Tuple | Ty::RawTuple => {
                let a = edit.node(left.into())?.edges("tupFields")?;
                let b = edit.node(right.into())?.edges("tupFields")?;
                if a.len() != b.len() {
                    return Ok(false);
                }
                for (a, b) in a.into_iter().zip(b) {
                    pending.push((edit.type_id(a)?, edit.type_id(b)?));
                }
            }
            Ty::Binary
            | Ty::Bool
            | Ty::Date
            | Ty::Decimal
            | Ty::Double
            | Ty::GeoPoint
            | Ty::Integer
            | Ty::LongInterval
            | Ty::Real
            | Ty::ShortInterval
            | Ty::String
            | Ty::Time
            | Ty::Timestamp
            | Ty::TimestampTz
            | Ty::Uuid
            | Ty::Variant => {
                if !edit.same_type(left, right)? {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}
