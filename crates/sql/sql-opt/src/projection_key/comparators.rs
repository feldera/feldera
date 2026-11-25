//! Comparator expressions as printed in canonical closures.
use super::Term;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, Node, NodeId, TypeKind,
};

/// The ordered integer encoding selects its conversion from the SQL value's
/// type, which is the input when wrapping and the result when unwrapping.
pub(super) fn unsigned_method(
    edit: &CircuitEditor,
    id: ExpressionId,
    source: ExpressionId,
    wrap: bool,
) -> Result<Option<&'static str>, Error> {
    let ty = edit.expression_type(if wrap { source } else { id })?;
    let signed = match edit.type_kind(ty)? {
        TypeKind::Date | TypeKind::Time | TypeKind::Timestamp => true,
        TypeKind::Integer => edit
            .node(ty.into())?
            .data("signed")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| Error::new("missing signedness for ordered integer"))?,
        _ => return Ok(None),
    };
    Ok(Some(match (wrap, signed, edit.nullable(ty)?) {
        (true, true, false) => "from_signed",
        (true, true, true) => "from_option",
        (true, false, false) => "from_unsigned",
        (true, false, true) => "from_unsigned_option",
        (false, true, false) => "to_signed",
        (false, true, true) => "to_signed_option",
        (false, false, false) => "to_unsigned",
        (false, false, true) => "to_unsigned_option",
    }))
}

#[derive(Eq, Hash, PartialEq)]
pub(super) struct Collation {
    left: u64,
    right: u64,
    ascending: bool,
    nulls_first: bool,
}

impl Collation {
    fn read(field: &Field) -> Result<Self, Error> {
        let Field::Object(parts) = field else {
            return Err(Error::new("expected an asymmetric comparator collation"));
        };
        let property = |name: &str| match parts.get(name) {
            Some(Field::Data(value)) => Ok(value),
            _ => Err(Error::new(format!("missing collation property {name}"))),
        };
        let column = |name| {
            property(name)?
                .as_u64()
                .ok_or_else(|| Error::new(format!("expected a column index for {name}")))
        };
        let flag = |name| {
            property(name)?
                .as_bool()
                .ok_or_else(|| Error::new(format!("expected a boolean for {name}")))
        };
        Ok(Self {
            left: column("leftField")?,
            right: column("rightField")?,
            ascending: flag("ascending")?,
            nulls_first: flag("nullsFirst")?,
        })
    }
}

pub(super) fn printed(node: &Node, kind: Expr) -> Result<(Term, Vec<NodeId>), Error> {
    match kind {
        Expr::NoComparator => Ok((Term::Call("compare".into()), vec![node.edge("tupleType")?])),
        Expr::DirectComparator | Expr::FieldComparator => {
            let ascending = node
                .data("ascending")
                .and_then(|v| v.as_bool())
                .ok_or_else(|| Error::new("missing comparator direction"))?;
            // A field comparator prints its index and direction. Its null order
            // appears only in the generated type, which the printer omits.
            let field = if kind == Expr::FieldComparator {
                Some(
                    node.data("fieldNo")
                        .and_then(|v| v.as_u64())
                        .ok_or_else(|| Error::new("missing comparator field number"))?,
                )
            } else {
                None
            };
            Ok((
                Term::Compare { field, ascending },
                vec![node.edge("source")?],
            ))
        }
        Expr::AsymmetricFieldComparator => {
            let Some(Field::Array(fields)) = node.field("comparisons") else {
                return Err(Error::new("missing asymmetric comparator collations"));
            };
            let comparisons = fields
                .iter()
                .map(Collation::read)
                .collect::<Result<_, _>>()?;
            Ok((Term::AsymmetricComparator(comparisons), vec![]))
        }
        _ => unreachable!("expected a comparator expression"),
    }
}
