//! Compare the canonical spelling of a projection over one bound row. The Java
//! window pass uses printed closures, so cast safety and call result types do
//! not distinguish keys, while operand nullability in a binary expression does.
use super::numbers::{decimal_text, floating_text};
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, InnerKind, Node, NodeId,
    TypeId, TypeKind as Ty,
};
use indexmap::IndexSet;
use std::collections::{HashMap, HashSet};

/// Postorder value numbers preserve operand order and ignore incidental sharing
/// in the input graph. Repeated subexpressions occupy one slot.
#[derive(Eq, Hash, PartialEq)]
pub(super) struct TimestampKey(Vec<(Term, Vec<usize>)>);

#[derive(Eq, Hash, PartialEq)]
enum Term {
    Variable(usize),
    Closure,
    Let,
    ReferenceType {
        mutable: bool,
        nullable: bool,
    },
    Array,
    Map,
    Tuple,
    AbsentTuple,
    VariantNullLiteral,
    NullPoint {
        nullable: bool,
    },
    Field(u64),
    Deref,
    Clone,
    IsNull,
    If,
    Call(String),
    Cast,
    ScalarType(String),
    CompositeType {
        kind: Ty,
        nullable: bool,
    },
    Number(String),
    Bool(bool),
    String(String),
    Bytes(String),
    Uuid(String),
    TemporalText(String),
    Date(i64),
    Timestamp(i64),
    TypedNull(String),
    Binary {
        opcode: BinaryOp,
        left_nullable: bool,
        right_nullable: bool,
    },
    Unary {
        opcode: String,
        nullable: bool,
    },
    Interval {
        months: bool,
        units: String,
        value: i64,
        nullable: bool,
    },
}

/// A binding's position within the active parameter list identifies its uses
/// across renaming. Its index in the arena distinguishes sibling scopes when
/// memoizing shared expressions.
struct Binding<'a> {
    variable: RowParameter<'a>,
    parent: Option<usize>,
    position: usize,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct ScopedNode {
    id: NodeId,
    scope: usize,
}

enum Visit {
    Read(ScopedNode),
    Intern(ScopedNode, Term, Vec<ScopedNode>),
}

impl TimestampKey {
    pub(super) fn read(
        edit: &CircuitEditor,
        root: ExpressionId,
        parameter: RowParameter<'_>,
    ) -> Result<Option<Self>, Error> {
        let mut scopes = vec![Binding {
            variable: parameter,
            parent: None,
            position: 0,
        }];
        let mut terms = IndexSet::new();
        let mut numbers = HashMap::new();
        let mut active = HashSet::new();
        let mut work = vec![Visit::Read(ScopedNode {
            id: root.into(),
            scope: 0,
        })];
        while let Some(visit) = work.pop() {
            match visit {
                Visit::Read(at) => {
                    if numbers.contains_key(&at) {
                        continue;
                    }
                    if !active.insert(at.id) {
                        return Err(Error::new("cycle in a window key expression"));
                    }
                    let Some((term, children)) = scoped_term(edit, at, &mut scopes)? else {
                        return Ok(None);
                    };
                    work.push(Visit::Intern(at, term, children.clone()));
                    work.extend(children.into_iter().rev().map(Visit::Read));
                }
                Visit::Intern(at, term, children) => {
                    let id = at.id;
                    let operands = children.into_iter().map(|child| numbers[&child]).collect();
                    // The literal printer and an explicit Some use the same spelling.
                    let wrap = matches!(
                        term,
                        Term::Number(_)
                            | Term::Tuple
                            | Term::Bool(_)
                            | Term::String(_)
                            | Term::Bytes(_)
                            | Term::Uuid(_)
                            | Term::TemporalText(_)
                            | Term::Date(_)
                            | Term::Timestamp(_)
                    ) && edit.nullable(edit.expression_type(edit.expression(id)?)?)?;
                    let (mut number, _) = terms.insert_full((term, operands));
                    if wrap {
                        number = terms
                            .insert_full((Term::Call("Some".into()), vec![number]))
                            .0;
                    }
                    numbers.insert(at, number);
                    active.remove(&id);
                }
            }
        }
        Ok(Some(Self(terms.into_iter().collect())))
    }
}

fn scoped_term<'a>(
    edit: &'a CircuitEditor,
    at: ScopedNode,
    scopes: &mut Vec<Binding<'a>>,
) -> Result<Option<(Term, Vec<ScopedNode>)>, Error> {
    let node = edit.node(at.id)?;
    let same_scope = |id| ScopedNode {
        id,
        scope: at.scope,
    };
    Ok(Some(match node.inner_kind() {
        Some(InnerKind::Expression(Expr::Closure)) => {
            let mut scope = at.scope;
            let mut children = Vec::new();
            for param in node.edges("parameters")? {
                let variable = row_parameter(edit, param)?;
                children.push(same_scope(variable.ty.into()));
                scope = bind(scopes, scope, variable);
            }
            children.push(ScopedNode {
                id: node.edge("body")?,
                scope,
            });
            (Term::Closure, children)
        }
        Some(InnerKind::Expression(Expr::Let)) => {
            let path = edit.node(node.edge("variable")?)?;
            let variable = RowParameter {
                name: string(path, "variable")
                    .ok_or_else(|| Error::new("missing LET binding name"))?,
                ty: edit.type_id(path.edge("type")?)?,
            };
            let scope = bind(scopes, at.scope, variable);
            // Only the consumer sees the new binding.
            (
                Term::Let,
                vec![
                    same_scope(node.edge("initializer")?),
                    ScopedNode {
                        id: node.edge("consumer")?,
                        scope,
                    },
                ],
            )
        }
        Some(InnerKind::Expression(Expr::VariablePath)) => {
            let name = string(node, "variable")
                .ok_or_else(|| Error::new("missing window key variable name"))?;
            let binding = std::iter::successors(Some(at.scope), |&s| scopes[s].parent)
                .map(|s| &scopes[s])
                .find(|b| b.variable.name == name);
            let Some(binding) = binding else {
                return Ok(None);
            };
            if !edit.same_type(binding.variable.ty, edit.type_id(node.edge("type")?)?)? {
                return Ok(None);
            }
            (Term::Variable(binding.position), Vec::new())
        }
        kind => {
            let term = match kind {
                Some(InnerKind::Type(_)) => type_term(edit, edit.type_id(at.id)?)?,
                _ => key_term(edit, edit.expression(at.id)?)?,
            };
            let Some((term, children)) = term else {
                return Ok(None);
            };
            (term, children.into_iter().map(same_scope).collect())
        }
    }))
}

fn bind<'a>(scopes: &mut Vec<Binding<'a>>, parent: usize, variable: RowParameter<'a>) -> usize {
    let scope = scopes.len();
    scopes.push(Binding {
        variable,
        parent: Some(parent),
        position: scopes[parent].position + 1,
    });
    scope
}

fn key_term(edit: &CircuitEditor, id: ExpressionId) -> Result<Option<(Term, Vec<NodeId>)>, Error> {
    let node = edit.node(id.into())?;
    let child = |field| edit.expression(node.edge(field)?).map(NodeId::from);
    let children = |field| {
        node.edges(field)?
            .into_iter()
            .map(|id| edit.expression(id).map(NodeId::from))
            .collect::<Result<Vec<_>, Error>>()
    };
    let kind = edit.expr_kind(id)?;
    Ok(Some(match kind {
        Expr::Field => {
            let column = node
                .data("fieldNo")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| Error::new("invalid window key column"))?;
            (Term::Field(column), vec![child("expression")?])
        }
        // Non-null constructors print their contents in order and omit their type.
        Expr::Array if node.field("data").is_some() => (Term::Array, children("data")?),
        // Tuple constructors include their arity and a trailing separator after each field.
        Expr::Tuple if node.field("fields").is_some() => (Term::Tuple, children("fields")?),
        Expr::Tuple => (Term::AbsentTuple, vec![]),
        Expr::VariantNullLiteral => (Term::VariantNullLiteral, vec![]),
        Expr::GeoPointConstructor => {
            let ty = edit.expression_type(id)?;
            match (node.field("left"), node.field("right")) {
                (Some(_), Some(_)) => {
                    let Some(name) = scalar_spelling(edit, ty)? else {
                        return Ok(None);
                    };
                    (Term::Call(name), vec![child("left")?, child("right")?])
                }
                (None, None) => (
                    Term::NullPoint {
                        nullable: edit.nullable(ty)?,
                    },
                    vec![],
                ),
                _ => return Ok(None),
            }
        }
        Expr::Variant => match node.field("value").and_then(|field| field.node_id()) {
            Some(value) => (
                Term::Call("VARIANT".into()),
                vec![edit.expression(value)?.into()],
            ),
            // Both an absent VARIANT and a VARIANT containing SQL NULL print "(VARIANT)null".
            None => (Term::TypedNull("VARIANT".into()), vec![]),
        },
        Expr::Map if node.field("keys").is_some() => {
            let keys = children("keys")?;
            let values = children("values")?;
            if keys.len() != values.len() {
                return Err(Error::new(
                    "window key map has different key and value counts",
                ));
            }
            let entries = keys
                .into_iter()
                .zip(values)
                .flat_map(|(key, value)| [key, value])
                .collect();
            (Term::Map, entries)
        }
        Expr::Deref => (Term::Deref, vec![child("expression")?]),
        Expr::Clone => (Term::Clone, vec![child("expression")?]),
        Expr::IsNull => (Term::IsNull, vec![child("expression")?]),
        Expr::If => {
            let mut branches = vec![child("condition")?, child("positive")?];
            if node.field("negative").is_some() {
                branches.push(child("negative")?);
            }
            (Term::If, branches)
        }
        Expr::Unary => {
            let Some(
                opcode @ ("WRAP_BOOL" | "IS_TRUE" | "IS_FALSE" | "IS_NOT_TRUE" | "IS_NOT_FALSE"
                | "NOT" | "NEG" | "UNARY_PLUS" | "INDICATOR" | "TYPEDBOX"),
            ) = string(node, "opcode")
            else {
                return Ok(None);
            };
            let source = child("source")?;
            (
                Term::Unary {
                    opcode: opcode.to_owned(),
                    nullable: edit.nullable(edit.expression_type(edit.expression(source)?)?)?,
                },
                vec![source],
            )
        }
        Expr::Some => (Term::Call("Some".into()), vec![child("expression")?]),
        Expr::Cast => (
            Term::Cast,
            vec![edit.expression_type(id)?.into(), child("source")?],
        ),
        Expr::Apply => {
            let function = child("function")?;
            if edit.expr_kind(edit.expression(function)?)? != Expr::Path {
                return Ok(None);
            }
            let path = edit.node(edit.node(function)?.edge("path")?)?;
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
            (Term::Call(name.to_owned()), children("arguments")?)
        }
        Expr::Binary | Expr::TimeAddSub => {
            let left = child("left")?;
            let right = child("right")?;
            let left_nullable = edit.nullable(edit.expression_type(edit.expression(left)?)?)?;
            let right_nullable = edit.nullable(edit.expression_type(edit.expression(right)?)?)?;
            let Some(op) = string(node, "opcode").and_then(BinaryOp::from_wire) else {
                return Ok(None);
            };
            match op {
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
        Expr::BinaryLiteral
        | Expr::BoolLiteral
        | Expr::DateLiteral
        | Expr::DecimalLiteral
        | Expr::RealLiteral
        | Expr::DoubleLiteral
        | Expr::StringLiteral
        | Expr::TimeLiteral
        | Expr::TimestampLiteral
        | Expr::TimestampTzLiteral
        | Expr::UuidLiteral
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
        | Expr::USizeLiteral => {
            let term = match node.data("value").filter(|v| !v.is_null()) {
                Some(value) if kind == Expr::BoolLiteral => {
                    let Some(value) = value.as_bool() else {
                        return Ok(None);
                    };
                    Term::Bool(value)
                }
                Some(_) if kind == Expr::DecimalLiteral => {
                    let (coefficient, scale) = feldera_sql_expr::decimal_literal_parts(node)?;
                    Term::Number(decimal_text(&coefficient, scale))
                }
                Some(value) if matches!(kind, Expr::RealLiteral | Expr::DoubleLiteral) => {
                    let Some(bits) = value.as_i64() else {
                        return Ok(None);
                    };
                    let text = if kind == Expr::RealLiteral {
                        let Ok(bits) = i32::try_from(bits) else {
                            return Ok(None);
                        };
                        floating_text(f32::from_bits(bits as u32))
                    } else {
                        floating_text(f64::from_bits(bits as u64))
                    };
                    Term::Number(text)
                }
                Some(value) if kind == Expr::StringLiteral => {
                    // Quoting and escaping are injective in Java's debug printer.
                    let Some(value) = value.as_str() else {
                        return Ok(None);
                    };
                    Term::String(value.to_owned())
                }
                Some(value) if matches!(kind, Expr::BinaryLiteral | Expr::UuidLiteral) => {
                    // The checkpoint writer uses padded base64 for bytes and the
                    // canonical UUID string. Each uniquely identifies the printed literal.
                    let Some(value) = value.as_str() else {
                        return Ok(None);
                    };
                    if kind == Expr::BinaryLiteral {
                        Term::Bytes(value.to_owned())
                    } else {
                        Term::Uuid(value.to_owned())
                    }
                }
                Some(value) if matches!(kind, Expr::TimeLiteral | Expr::TimestampTzLiteral) => {
                    // Java prints the wire text unquoted, including the original zone.
                    let Some(text) = value.as_str() else {
                        return Ok(None);
                    };
                    Term::TemporalText(text.to_owned())
                }
                Some(value) if matches!(kind, Expr::DateLiteral | Expr::TimestampLiteral) => {
                    let Some(ticks) = value.as_i64().or_else(|| value.as_str()?.parse().ok())
                    else {
                        return Ok(None);
                    };
                    let days = if kind == Expr::DateLiteral {
                        ticks
                    } else {
                        ticks.div_euclid(86_400_000_000)
                    };
                    // Each epoch value has a distinct spelling within Calcite's
                    // four-digit CE years (0001-01-01 through 9999-12-31).
                    if !(-719_162..=2_932_896).contains(&days) {
                        return Ok(None);
                    }
                    if kind == Expr::DateLiteral {
                        Term::Date(ticks)
                    } else {
                        Term::Timestamp(ticks)
                    }
                }
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
                    Term::Number(digits)
                }
                None => {
                    let Some(name) = scalar_spelling(edit, edit.expression_type(id)?)? else {
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

/// Number each type argument just like an expression operand. A row can share
/// field types at every level, so constructing its full spelling can be exponential.
fn type_term(edit: &CircuitEditor, ty: TypeId) -> Result<Option<(Term, Vec<NodeId>)>, Error> {
    let kind = edit.type_kind(ty)?;
    let field = match kind {
        Ty::Ref => {
            let node = edit.node(ty.into())?;
            let Some(mutable) = node.data("mutable").and_then(|v| v.as_bool()) else {
                return Ok(None);
            };
            return Ok(Some((
                Term::ReferenceType {
                    mutable,
                    nullable: edit.nullable(ty)?,
                },
                vec![node.edge("type")?],
            )));
        }
        Ty::Tuple | Ty::RawTuple => "tupFields",
        Ty::Array | Ty::Map => "typeArgs",
        _ => return Ok(scalar_spelling(edit, ty)?.map(|name| (Term::ScalarType(name), vec![]))),
    };
    let args = edit
        .node(ty.into())?
        .edges(field)?
        .into_iter()
        .map(|id| edit.type_id(id).map(NodeId::from))
        .collect::<Result<Vec<_>, _>>()?;
    let nullable = kind != Ty::RawTuple && edit.nullable(ty)?;
    Ok(Some((Term::CompositeType { kind, nullable }, args)))
}

/// Match the Java debug printer used for temporal keys.
fn scalar_spelling(edit: &CircuitEditor, ty: TypeId) -> Result<Option<String>, Error> {
    let node = edit.node(ty.into())?;
    let mut text = match edit.type_kind(ty)? {
        Ty::GeoPoint => "geopoint".to_owned(),
        kind @ (Ty::LongInterval | Ty::ShortInterval) => {
            let Some(units) = string(node, "units") else {
                return Ok(None);
            };
            let name = if kind == Ty::LongInterval {
                "LongInterval"
            } else {
                "ShortInterval"
            };
            // The Java printer puts the nullability marker before the unit qualifier.
            let suffix = if edit.nullable(ty)? { "?" } else { "" };
            return Ok(Some(format!("{name}{suffix}({units})")));
        }
        Ty::Variant => "VARIANT".to_owned(),
        Ty::Uuid => "Uuid".to_owned(),
        Ty::Binary => {
            let Some(precision) = node.data("precision").and_then(|v| v.as_i64()) else {
                return Ok(None);
            };
            // The binary type printer omits the fixed-width flag.
            if precision == -1 {
                "bytes".to_owned()
            } else {
                format!("bytes({precision})")
            }
        }
        Ty::Date => "Date".to_owned(),
        Ty::Time => "Time".to_owned(),
        Ty::Real => "f".to_owned(),
        Ty::Double => "d".to_owned(),
        Ty::Decimal => {
            let precision = node.data("precision").and_then(|v| v.as_u64());
            let scale = node.data("scale").and_then(|v| v.as_u64());
            let (Some(precision), Some(scale)) = (precision, scale) else {
                return Ok(None);
            };
            format!("DECIMAL({precision}, {scale})")
        }
        Ty::Bool => "b".to_owned(),
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
            Ty::Tuple | Ty::RawTuple | Ty::Array | Ty::Map => {
                let field = match kind {
                    Ty::Array | Ty::Map => "typeArgs",
                    _ => "tupFields",
                };
                let a = edit.node(left.into())?.edges(field)?;
                let b = edit.node(right.into())?.edges(field)?;
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
    fn variant_nulls_and_nullable_tuples_follow_the_printed_constructor() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let (graph, types) = graph.with_inner_roots([json!({
            "class":"DBSPTypeVariant","id":50,"code":"VARIANT","mayBeNull":true
        })])?;
        let mut edit = CircuitEditor::new(graph);
        let bool_ty = edit.bool_type(site, false)?;
        let variant = edit.type_id(types[0])?;
        let mut null = |sql_null: bool| {
            edit.make_expr(
                site,
                Expr::Variant,
                variant,
                [("isSqlNull", Field::Data(sql_null.into()))],
            )
        };
        let absent = null(false)?;
        let sql_null = null(true)?;
        let variant_null = edit.make_expr(site, Expr::VariantNullLiteral, variant, [])?;
        let param = edit.named_variable(bool_ty, "v")?;
        let tuple = edit.tuple_expr(site, false, &[param])?;
        let nullable = edit.with_nullability(edit.expression_type(tuple)?, true)?;
        let fields = [("fields", Field::Array(vec![Field::Reference(param.into())]))];
        let wrapped = edit.make_expr(site, Expr::Tuple, nullable, fields)?;
        let some = edit.make_expr(
            site,
            Expr::Some,
            nullable,
            [("expression", Field::Reference(tuple.into()))],
        )?;
        let absent_tuple = edit.make_expr(site, Expr::Tuple, nullable, [])?;
        let key = |expr| {
            TimestampKey::read(
                &edit,
                expr,
                RowParameter {
                    name: "v",
                    ty: bool_ty,
                },
            )
            .map(Option::unwrap)
        };
        assert!(key(absent)? == key(sql_null)? && key(absent)? != key(variant_null)?);
        assert!(key(wrapped)? == key(some)? && key(wrapped)? != key(tuple)?);
        assert!(key(absent_tuple)? != key(absent)? && key(absent_tuple)? != key(wrapped)?);
        Ok(())
    }

    #[test]
    fn closure_and_let_keys_preserve_the_declaration_each_use_reads() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.bool_type(site, false)?;
        let outer = edit.named_variable(ty, "row")?;
        let row = RowParameter { name: "row", ty };
        let mut key = |parameter_name, local_name, capture| -> Result<TimestampKey, Error> {
            let parameter = edit.named_variable(ty, parameter_name)?;
            let local = edit.named_variable(ty, local_name)?;
            // The initializer reads the lambda parameter even when the LET shadows its name.
            let initializer = edit.named_variable(ty, parameter_name)?;
            let body =
                edit.let_expression(local, initializer, if capture { outer } else { local })?;
            let lambda = edit.closure_from_variables(body, &[parameter])?;
            Ok(TimestampKey::read(&edit, lambda, row)?.expect("bound lambda key"))
        };
        let shadow = key("x", "x", false)?;
        assert!(shadow == key("y", "z", false)?);
        assert!(shadow != key("y", "z", true)?);
        let free = edit.named_variable(ty, "p1")?;
        let lambda = edit.closure_from_variables(free, &[outer])?;
        assert!(TimestampKey::read(&edit, lambda, row)?.is_none());
        Ok(())
    }

    #[test]
    fn a_shared_lambda_body_is_numbered_once_per_scope() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.bool_type(site, false)?;
        let row = RowParameter { name: "x", ty };
        let x = edit.named_variable(ty, "x")?;
        let y = edit.named_variable(ty, "y")?;
        let mut body = x;
        for _ in 0..128 {
            body = edit.binary_expression(site, ty, BinaryOp::And, body, body)?;
        }
        let shadow = edit.closure_from_variables(body, &[x])?;
        let capture = edit.closure_from_variables(body, &[y])?;
        let both = edit.tuple_expr(site, false, &[shadow, capture])?;
        let same = edit.tuple_expr(site, false, &[shadow, shadow])?;
        let key = TimestampKey::read(&edit, both, row)?.expect("two bound lambdas");
        assert!(key.0.len() < 270);
        assert!(Some(key) != TimestampKey::read(&edit, same, row)?);
        Ok(())
    }

    #[test]
    fn a_cast_numbers_each_shared_field_type_once() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.bool_type(site, false)?;
        let row = RowParameter { name: "v", ty };
        let mut target = ty;
        for _ in 0..160 {
            target = edit.tuple_type(site, false, [target.into(), target.into()])?;
        }
        let variable = edit.named_variable(ty, "v")?;
        let operand = [("source", Field::Reference(variable.into()))];
        let cast = edit.make_expr(site, Expr::Cast, target, operand.clone())?;
        let nullable = edit.with_nullability(target, true)?;
        let other = edit.make_expr(site, Expr::Cast, nullable, operand)?;
        let key = TimestampKey::read(&edit, cast, row)?.unwrap();
        assert_eq!(key.0.len(), 163);
        assert!(Some(key) != TimestampKey::read(&edit, other, row)?);
        Ok(())
    }

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
    fn decimal_keys_preserve_the_scale_printed_by_java() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"allOperators":[],"declarations":[]
        }))?;
        let (graph, roots) = graph.with_inner_roots([json!({
            "class":"DBSPTypeDecimal","id":0,"code":"DECIMAL",
            "precision":18,"scale":4,"mayBeNull":false
        })])?;
        let site = graph.root_id();
        let mut edit = CircuitEditor::new(graph);
        let ty = edit.type_id(roots[0])?;
        let mut decimal = |value: &str| {
            edit.make_expr(
                site,
                Expr::DecimalLiteral,
                ty,
                [("value", Field::Data(value.into()))],
            )
        };
        let whole = decimal("1")?;
        let tenth = decimal("1.0")?;
        let hundredth = decimal("1.00")?;
        let exponent = decimal("100E-2")?;
        let i32_type = edit.integer_type(ty, 32, true, false)?;
        let integer = edit.make_expr(
            site,
            Expr::I32Literal,
            i32_type,
            [("value", Field::Data(1.into()))],
        )?;
        let key =
            |id| TimestampKey::read(&edit, id, RowParameter { name: "r", ty }).map(Option::unwrap);
        assert!(key(whole)? == key(integer)?);
        assert!(key(tenth)? != key(integer)? && key(tenth)? != key(hundredth)?);
        assert!(key(hundredth)? == key(exponent)?);
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
