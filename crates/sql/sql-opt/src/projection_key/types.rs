//! Compare the spelling of types in printed closures and generic call paths.
use super::{Term, string};
use feldera_sql_ir::{CircuitEditor, Error, Node, NodeId, TypeId, TypeKind as Ty};

type TypeTerm = (Term, Vec<NodeId>);

fn reference(node: &Node, nullable: bool) -> Result<Option<TypeTerm>, Error> {
    let Some(mutable) = node.data("mutable").and_then(|v| v.as_bool()) else {
        return Ok(None);
    };
    Ok(Some((
        Term::ReferenceType { mutable, nullable },
        vec![node.edge("type")?],
    )))
}

/// A user type can have the same spelling as a primitive, tuple or nullable reference.
fn named(
    edit: &CircuitEditor,
    name: &str,
    nullable: bool,
    args: Vec<NodeId>,
) -> Result<Option<TypeTerm>, Error> {
    if !name.split("::").all(|part| {
        !part.is_empty() && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    }) {
        // Punctuation in a user-provided name can itself spell type syntax.
        return Ok(None);
    }
    if args.is_empty() {
        let suffix = if nullable { "?" } else { "" };
        return Ok(Some((Term::ScalarType(format!("{name}{suffix}")), args)));
    }
    if let ("Option", false, [arg]) = (name, nullable, args.as_slice()) {
        let ty = edit.type_id(*arg)?;
        if edit.type_kind(ty)? == Ty::Ref && !edit.nullable(ty)? {
            return reference(edit.node(*arg)?, true);
        }
    }
    let term = if !nullable && name.starts_with("Tup") && name == format!("Tup{}", args.len()) {
        Term::TupleType { nullable: false }
    } else {
        Term::NamedType {
            name: name.to_owned(),
            nullable,
        }
    };
    Ok(Some((term, args)))
}

/// The operands retain sharing, including when a nested signature reuses a row type.
pub(super) fn type_term(edit: &CircuitEditor, ty: TypeId) -> Result<Option<TypeTerm>, Error> {
    let kind = edit.type_kind(ty)?;
    let node = edit.node(ty.into())?;
    let nullable = edit.nullable(ty)?;
    if let Some(args) = edit.user_type_arguments(ty)? {
        // Older snapshots of these built-in containers can omit their fixed name.
        let name = match kind {
            Ty::Array => "Array",
            Ty::Map => "Map",
            Ty::SqlResult => "SqlResult",
            _ => match string(node, "name") {
                Some(name) => name,
                None => return Ok(None),
            },
        };
        return named(edit, name, nullable, args);
    }
    Ok(Some(match kind {
        Ty::Ref => return reference(node, nullable),
        Ty::RawTuple => (Term::RawTupleType, node.edges("tupFields")?),
        Ty::Tuple => (Term::TupleType { nullable }, node.edges("tupFields")?),
        Ty::Function => {
            let mut signature = node.edges("parameterTypes")?;
            signature.push(node.edge("resultType")?);
            (Term::FunctionType, signature)
        }
        // Stream prints only its element. Kind, nesting and the outer nullability
        // do not add any characters to the closure.
        Ty::Stream => return named(edit, "Stream", false, vec![node.edge("elementType")?]),
        _ => return Ok(scalar_spelling(edit, ty)?.map(|name| (Term::ScalarType(name), vec![]))),
    }))
}

/// Match the Java debug printer used for temporal keys.
pub(super) fn scalar_spelling(edit: &CircuitEditor, ty: TypeId) -> Result<Option<String>, Error> {
    let node = edit.node(ty.into())?;
    let mut text = match edit.type_kind(ty)? {
        Ty::Any => return Ok(Some("_".to_owned())),
        Ty::Null => "null".to_owned(),
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
