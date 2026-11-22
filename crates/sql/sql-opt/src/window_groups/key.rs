//! Compare the canonical spelling of a projection over one bound row. The Java
//! window pass uses printed closures, so cast safety and call result types do
//! not distinguish keys, while operand nullability in a binary expression does.
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Node, NodeId, TypeId,
    TypeKind as Ty,
};
use indexmap::IndexSet;
use std::collections::{HashMap, HashSet};

/// Postorder value numbers preserve operand order and ignore incidental sharing
/// in the input graph. Repeated subexpressions occupy one slot.
#[derive(Eq, Hash, PartialEq)]
pub(super) struct TimestampKey(Vec<(Term, Vec<usize>)>);

#[derive(Eq, Hash, PartialEq)]
enum Term {
    Parameter,
    Field(u64),
    Deref,
    Clone,
    Call(String),
    Cast(String),
    Integer(String),
    TypedNull(String),
    Binary {
        opcode: BinaryOp,
        left_nullable: bool,
        right_nullable: bool,
    },
    VariantField {
        name: String,
        row_nullable: bool,
        name_nullable: bool,
    },
    Interval {
        months: bool,
        units: String,
        value: i64,
        nullable: bool,
    },
}

enum Visit {
    Read(ExpressionId),
    Intern(ExpressionId, Term, Vec<ExpressionId>),
}

impl TimestampKey {
    pub(super) fn read(
        edit: &CircuitEditor,
        root: ExpressionId,
        parameter: RowParameter<'_>,
    ) -> Result<Option<Self>, Error> {
        let mut terms = IndexSet::new();
        let mut numbers = HashMap::new();
        let mut active = HashSet::new();
        let mut work = vec![Visit::Read(root)];
        while let Some(visit) = work.pop() {
            match visit {
                Visit::Read(id) => {
                    if numbers.contains_key(&id) {
                        continue;
                    }
                    if !active.insert(id) {
                        return Err(Error::new("cycle in a window key expression"));
                    }
                    let Some((term, children)) = key_term(edit, id, parameter)? else {
                        return Ok(None);
                    };
                    work.push(Visit::Intern(id, term, children.clone()));
                    work.extend(children.into_iter().rev().map(Visit::Read));
                }
                Visit::Intern(id, term, children) => {
                    let operands = children.into_iter().map(|child| numbers[&child]).collect();
                    // A nullable integer and an explicit Some applied to the
                    // same nonnullable integer have identical Java spellings.
                    let wrap = matches!(term, Term::Integer(_))
                        && edit.nullable(edit.expression_type(id)?)?;
                    let (mut number, _) = terms.insert_full((term, operands));
                    if wrap {
                        number = terms
                            .insert_full((Term::Call("Some".into()), vec![number]))
                            .0;
                    }
                    numbers.insert(id, number);
                    active.remove(&id);
                }
            }
        }
        Ok(Some(Self(terms.into_iter().collect())))
    }
}

fn key_term(
    edit: &CircuitEditor,
    id: ExpressionId,
    parameter: RowParameter<'_>,
) -> Result<Option<(Term, Vec<ExpressionId>)>, Error> {
    let node = edit.node(id.into())?;
    let child = |field| edit.expression(node.edge(field)?);
    let kind = edit.expr_kind(id)?;
    Ok(Some(match kind {
        Expr::VariablePath
            if string(node, "variable") == Some(parameter.name)
                && edit.same_type(edit.expression_type(id)?, parameter.ty)? =>
        {
            (Term::Parameter, vec![])
        }
        Expr::Field => {
            let column = node
                .data("fieldNo")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| Error::new("invalid window key column"))?;
            (Term::Field(column), vec![child("expression")?])
        }
        Expr::Deref => (Term::Deref, vec![child("expression")?]),
        Expr::Clone => (Term::Clone, vec![child("expression")?]),
        Expr::Some => (Term::Call("Some".into()), vec![child("expression")?]),
        Expr::Cast => {
            let Some(target) = cast_spelling(edit, edit.expression_type(id)?)? else {
                return Ok(None);
            };
            (Term::Cast(target), vec![child("source")?])
        }
        Expr::Apply => {
            let function = child("function")?;
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
            if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                return Ok(None);
            }
            let arguments = node
                .edges("arguments")?
                .into_iter()
                .map(|arg| edit.expression(arg))
                .collect::<Result<_, _>>()?;
            (Term::Call(name.to_owned()), arguments)
        }
        Expr::Binary | Expr::TimeAddSub => {
            let left = child("left")?;
            let right = child("right")?;
            let left_nullable = edit.nullable(edit.expression_type(left)?)?;
            let right_nullable = edit.nullable(edit.expression_type(right)?)?;
            let Some(op) = string(node, "opcode").and_then(BinaryOp::from_wire) else {
                return Ok(None);
            };
            match op {
                BinaryOp::VariantIndex if edit.expr_kind(right)? == Expr::StringLiteral => {
                    let Some(name) = string(edit.node(right.into())?, "value") else {
                        return Ok(None);
                    };
                    (
                        Term::VariantField {
                            name: name.to_owned(),
                            row_nullable: left_nullable,
                            name_nullable: right_nullable,
                        },
                        vec![left],
                    )
                }
                // Collection conversions print multiline function calls.
                BinaryOp::ArrayConvert
                | BinaryOp::ArrayConvertSafe
                | BinaryOp::MapConvert
                | BinaryOp::MapConvertSafe => return Ok(None),
                _ => {
                    // Several distinct opcodes print the same infix operator.
                    let opcode = match op {
                        BinaryOp::MulInterval => BinaryOp::Mul,
                        BinaryOp::DivInterval => BinaryOp::Div,
                        BinaryOp::MulIntervalSaturating => BinaryOp::MulSaturating,
                        BinaryOp::DivIntervalSaturating => BinaryOp::DivSaturating,
                        BinaryOp::Concat => BinaryOp::Or,
                        other => other,
                    };
                    (
                        Term::Binary {
                            opcode,
                            left_nullable,
                            right_nullable,
                        },
                        vec![left, right],
                    )
                }
            }
        }
        Expr::I8Literal
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
        | Expr::USizeLiteral => {
            let term = match node.data("value").filter(|v| !v.is_null()) {
                Some(value) => {
                    // Current writers use decimal strings. Earlier snapshots
                    // also contain JSON integers.
                    let digits = match value.as_str() {
                        Some(s) => s
                            .parse::<i128>()
                            .map(|n| n.to_string())
                            .or_else(|_| s.parse::<u128>().map(|n| n.to_string()))
                            .ok(),
                        None if value.is_i64() || value.is_u64() => Some(value.to_string()),
                        None => None,
                    };
                    let Some(digits) = digits else {
                        return Ok(None);
                    };
                    Term::Integer(digits)
                }
                None => {
                    let Some(name) = cast_spelling(edit, edit.expression_type(id)?)? else {
                        return Ok(None);
                    };
                    Term::TypedNull(name)
                }
            };
            (term, vec![])
        }
        Expr::ShortIntervalLiteral | Expr::LongIntervalLiteral => {
            let ty = edit.expression_type(id)?;
            let Some(units) = string(edit.node(ty.into())?, "units") else {
                return Ok(None);
            };
            let Some(value) = string(node, "value").and_then(|n| n.parse().ok()) else {
                return Ok(None);
            };
            (
                Term::Interval {
                    months: kind == Expr::LongIntervalLiteral,
                    units: units.to_owned(),
                    value,
                    nullable: edit.nullable(ty)?,
                },
                vec![],
            )
        }
        _ => return Ok(None),
    }))
}

fn string<'a>(node: &'a Node, field: &str) -> Option<&'a str> {
    node.data(field)?.as_str()
}

/// Canonical closures rename the parameter and print its type, including
/// both references in the (key, value) parameter of an indexed map.
#[derive(Clone, Copy)]
pub(super) struct RowParameter<'a> {
    pub(super) ty: TypeId,
    name: &'a str,
}

pub(super) fn row_parameter(
    edit: &CircuitEditor,
    parameter: NodeId,
) -> Result<RowParameter<'_>, Error> {
    let parameter = edit.node(parameter)?;
    let ty = edit.type_id(parameter.edge("type")?)?;
    let name = parameter
        .data("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("missing timestamp parameter name"))?;
    Ok(RowParameter { ty, name })
}

/// Match the Java debug printer used for temporal keys.
fn cast_spelling(edit: &CircuitEditor, ty: TypeId) -> Result<Option<String>, Error> {
    let node = edit.node(ty.into())?;
    let mut text = match edit.type_kind(ty)? {
        Ty::Date => "Date".to_owned(),
        Ty::Timestamp => "Timestamp".to_owned(),
        Ty::TimestampTz => "TimestampTz".to_owned(),
        Ty::ISize => "i".to_owned(),
        Ty::USize => "u".to_owned(),
        Ty::Integer => {
            let Some(width @ (8 | 16 | 32 | 64 | 128)) =
                node.data("width").and_then(|v| v.as_u64())
            else {
                return Ok(None);
            };
            let Some(signed) = node.data("signed").and_then(|v| v.as_bool()) else {
                return Ok(None);
            };
            format!("{}{width}", if signed { "i" } else { "u" })
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{Circuit, Field};
    use serde_json::json;

    #[test]
    fn repeated_operands_share_one_value_number_per_level() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let (graph, types) = graph.with_inner_roots([json!({
            "class":"DBSPTypeTimestamp","id":0,"code":"TIMESTAMP","mayBeNull":false
        })])?;
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.type_id(types[0])?;
        let site = edit.circuit().root_id();
        let row = RowParameter { name: "t", ty };
        let left = edit.named_variable(ty, "t")?;
        let right = edit.named_variable(ty, "t")?;
        let mut shared = edit.binary_expression(site, ty, BinaryOp::Max, left, left)?;
        let mut copied = edit.binary_expression(site, ty, BinaryOp::Max, left, right)?;
        for _ in 0..128 {
            shared = edit.binary_expression(site, ty, BinaryOp::Max, shared, shared)?;
            copied = edit.binary_expression(site, ty, BinaryOp::Max, copied, copied)?;
        }
        let key = TimestampKey::read(&edit, shared, row)?.unwrap();
        assert_eq!(key.0.len(), 130);
        assert!(Some(key) == TimestampKey::read(&edit, copied, row)?);
        let wrong_type = edit.bool_type(site, false)?;
        let invalid = edit.named_variable(wrong_type, "t")?;
        assert!(TimestampKey::read(&edit, invalid, row)?.is_none());
        Ok(())
    }

    #[test]
    fn integer_keys_follow_printed_width_nullability_and_operators() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let origin = edit.bool_type(site, false)?;
        let i16 = edit.integer_type(origin, 16, true, false)?;
        let u64 = edit.integer_type(origin, 64, false, false)?;
        let opt_i16 = edit.with_nullability(i16, true)?;
        let opt_u64 = edit.with_nullability(u64, true)?;
        let one = edit.make_expr(
            site,
            Expr::I16Literal,
            i16,
            [("value", Field::Data(json!(1)))],
        )?;
        let wide = edit.make_expr(
            site,
            Expr::U64Literal,
            u64,
            [("value", Field::Data(json!("1")))],
        )?;
        let nullable = edit.make_expr(
            site,
            Expr::U64Literal,
            opt_u64,
            [("value", Field::Data(json!("1")))],
        )?;
        let some = edit.make_expr(
            site,
            Expr::Some,
            opt_i16,
            [("expression", Field::Reference(one.into()))],
        )?;
        let function = edit.function_path(site, "Some")?;
        let call = edit.make_expr(
            site,
            Expr::Apply,
            opt_u64,
            [
                ("function", Field::Reference(function.into())),
                (
                    "arguments",
                    Field::Array(vec![Field::Reference(wide.into())]),
                ),
            ],
        )?;
        let null16 = edit.make_expr(site, Expr::I16Literal, opt_i16, [])?;
        let null64 = edit.make_expr(site, Expr::U64Literal, opt_u64, [])?;
        let binary = |edit: &mut CircuitEditor, op: BinaryOp, right: ExpressionId| {
            edit.make_expr(
                site,
                Expr::Binary,
                u64,
                [
                    ("left", Field::Reference(wide.into())),
                    ("opcode", Field::Data(json!(op.wire_name()))),
                    ("right", Field::Reference(right.into())),
                ],
            )
        };
        let product = binary(&mut edit, BinaryOp::Mul, one)?;
        let scaled_interval = binary(&mut edit, BinaryOp::MulInterval, wide)?;
        let nullable_product = binary(&mut edit, BinaryOp::Mul, some)?;
        let quotient = binary(&mut edit, BinaryOp::Div, one)?;
        let key = |id| {
            TimestampKey::read(&edit, id, RowParameter { name: "r", ty: i16 })
                .map(|k| k.expect("numeric window key"))
        };
        assert!(key(one)? == key(wide)?);
        assert!(key(nullable)? == key(some)? && key(some)? == key(call)?);
        assert!(key(one)? != key(nullable)? && key(null16)? != key(null64)?);
        assert!(key(product)? == key(scaled_interval)?);
        assert!(key(product)? != key(nullable_product)? && key(product)? != key(quotient)?);
        Ok(())
    }
}
