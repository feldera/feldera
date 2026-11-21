//! Weighted constants use ISameValue when appending rows. Literal elements of
//! Java collections use equals, which also compares types. Other expressions
//! in those collections compare by object identity.
use crate::same_literal_value;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
};

type Row = (ExpressionId, i64);

pub fn multiset(
    edit: &mut CircuitEditor,
    element: TypeId,
    entries: impl IntoIterator<Item = (ExpressionId, i64)>,
) -> Result<ExpressionId, Error> {
    let ty = edit.zset_type(element)?;
    let (data, weights) = entries
        .into_iter()
        .map(|(v, w)| (Field::Reference(v.into()), Field::Data(w.into())))
        .unzip();
    let literal = edit.empty_origin_node(
        element.into(),
        InnerKind::Expression(Expr::ZSet),
        [
            ("elementType", Field::Reference(element.into())),
            ("type", Field::Reference(ty.into())),
            ("data", Field::Array(data)),
            ("weights", Field::Array(weights)),
        ],
    )?;
    edit.expression(literal)
}

pub fn entries(edit: &CircuitEditor, set: ExpressionId) -> Result<Vec<Row>, Error> {
    let node = edit.node(set.into())?;
    let rows = node.edges("data")?;
    let Some(Field::Array(weights)) = node.field("weights") else {
        return Err(Error::new("constant collection needs weights"));
    };
    if rows.len() != weights.len() {
        return Err(Error::new(
            "constant collection has different row and weight counts",
        ));
    }
    rows.into_iter()
        .zip(weights)
        .map(|(row, weight)| {
            let weight = match weight {
                Field::Data(value) => value.as_i64(),
                _ => None,
            }
            .ok_or_else(|| Error::new("expected integer row weight"))?;
            Ok((edit.expression(row)?, weight))
        })
        .collect()
}

pub fn append(edit: &CircuitEditor, rows: &mut Vec<Row>, row: Row) -> Result<(), Error> {
    append_at(edit, rows, row, 0)
}

fn append_at(
    edit: &CircuitEditor,
    rows: &mut Vec<Row>,
    (value, weight): Row,
    depth: usize,
) -> Result<(), Error> {
    for (index, (existing, total)) in rows.iter_mut().enumerate() {
        if same_value(edit, (*existing).into(), value.into(), depth)? {
            *total = total.wrapping_add(weight);
            if *total == 0 {
                rows.remove(index);
            }
            return Ok(());
        }
    }
    rows.push((value, weight));
    Ok(())
}

pub fn object_equals(edit: &CircuitEditor, a: NodeId, b: NodeId) -> Result<bool, Error> {
    if a == b {
        return Ok(true);
    }
    let (a, b) = (edit.expression(a)?, edit.expression(b)?);
    Ok(edit.expr_kind(a)?.wire_name().ends_with("Literal")
        && edit.same_type(edit.expression_type(a)?, edit.expression_type(b)?)?
        && scalar_equals(edit, a, b)?)
}

fn scalar_equals(edit: &CircuitEditor, a: ExpressionId, b: ExpressionId) -> Result<bool, Error> {
    same_literal_value(edit, a, b)?
        .ok_or_else(|| Error::new("invalid zoned timestamp in a constant collection"))
}

fn same_value(edit: &CircuitEditor, a: NodeId, b: NodeId, depth: usize) -> Result<bool, Error> {
    if a == b {
        return Ok(true);
    }
    if depth > 128 {
        return Err(Error::new("constant exceeds the circuit nesting limit"));
    }
    let (a, b) = (edit.expression(a)?, edit.expression(b)?);
    let kind = edit.expr_kind(a)?;
    if kind != edit.expr_kind(b)? {
        return Ok(false);
    }
    let (left, right) = (edit.node(a.into())?, edit.node(b.into())?);
    let same_type = || edit.same_type(edit.expression_type(a)?, edit.expression_type(b)?);
    let same_list = |field: &str, compare: &dyn Fn(NodeId, NodeId) -> Result<bool, Error>| match (
        left.field(field),
        right.field(field),
    ) {
        (None, None) => Ok(true),
        (Some(_), Some(_)) => {
            let (xs, ys) = (left.edges(field)?, right.edges(field)?);
            if xs.len() != ys.len() {
                return Ok(false);
            }
            for (x, y) in xs.into_iter().zip(ys) {
                if !compare(x, y)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    };
    let objects = |x, y| object_equals(edit, x, y);
    let edge =
        |node: &feldera_sql_ir::Node, field: &str| node.field(field).and_then(Field::node_id);
    Ok(match kind {
        Expr::Tuple | Expr::RawTuple => {
            same_list("fields", &|x, y| same_value(edit, x, y, depth + 1))?
        }
        Expr::Array => same_type()? && same_list("data", &objects)?,
        Expr::Map => same_type()? && same_list("keys", &objects)? && same_list("values", &objects)?,
        Expr::GeoPointConstructor => {
            for key in ["left", "right"] {
                match (edge(left, key), edge(right, key)) {
                    (None, None) => {}
                    (Some(x), Some(y)) if objects(x, y)? => {}
                    _ => return Ok(false),
                }
            }
            true
        }
        Expr::Variant => match (edge(left, "value"), edge(right, "value")) {
            (None, None) => {
                left.data("isSqlNull").and_then(|v| v.as_bool()) != Some(true)
                    || right.data("isSqlNull").and_then(|v| v.as_bool()) == Some(true)
            }
            (Some(x), Some(y)) => same_value(edit, x, y, depth + 1)?,
            _ => false,
        },
        Expr::IndexedZSet => same_type()?,
        Expr::ZSet => {
            if !same_type()? {
                return Ok(false);
            }
            let mut negated = Vec::new();
            for (row, weight) in entries(edit, b)? {
                append_at(edit, &mut negated, (row, weight.wrapping_neg()), depth + 1)?;
            }
            let mut difference = entries(edit, a)?;
            for row in negated {
                append_at(edit, &mut difference, row, depth + 1)?;
            }
            difference.is_empty()
        }
        kind if kind.wire_name().ends_with("Literal") => scalar_equals(edit, a, b)?,
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{Circuit, InnerKind, TypeKind};
    use serde_json::{json, Value};

    fn editor() -> CircuitEditor {
        CircuitEditor::new(
            Circuit::from_json(json!({
                "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
            }))
            .unwrap(),
        )
    }

    fn literal(edit: &mut CircuitEditor, kind: Expr, ty: TypeKind, value: Value) -> ExpressionId {
        let ty = edit
            .inner(
                InnerKind::Type(ty),
                [("mayBeNull", Field::Data(false.into()))],
            )
            .unwrap();
        edit.make_expr(
            ty,
            kind,
            edit.type_id(ty).unwrap(),
            [("value", Field::Data(value))],
        )
        .unwrap()
    }

    #[test]
    fn equivalent_instants_cancel_and_a_reinserted_row_keeps_its_new_position() {
        let mut edit = editor();
        let west = literal(
            &mut edit,
            Expr::TimestampTzLiteral,
            TypeKind::TimestampTz,
            json!("2024-11-03 01:30:00 America/New_York"),
        );
        let utc = literal(
            &mut edit,
            Expr::TimestampTzLiteral,
            TypeKind::TimestampTz,
            json!("2024-11-03 05:30:00 UTC"),
        );
        let later = literal(
            &mut edit,
            Expr::TimestampTzLiteral,
            TypeKind::TimestampTz,
            json!("2024-11-03 06:30:00 UTC"),
        );
        let mut rows = vec![(west, 2), (later, 1)];
        append(&edit, &mut rows, (utc, -2)).unwrap();
        append(&edit, &mut rows, (utc, 1)).unwrap();
        assert_eq!(rows, [(later, 1), (utc, 1)]);
    }

    #[test]
    fn nan_payloads_consolidate_but_signed_zeros_are_distinct() {
        let mut edit = editor();
        let mut rows = Vec::new();
        for bits in [
            0x7ff8_0000_0000_0001_i64,
            0x7ff8_0000_0000_0002,
            0,
            i64::MIN,
        ] {
            let value = literal(
                &mut edit,
                Expr::DoubleLiteral,
                TypeKind::Double,
                json!(bits),
            );
            append(&edit, &mut rows, (value, 1)).unwrap();
        }
        assert_eq!(
            rows.iter().map(|(_, weight)| *weight).collect::<Vec<_>>(),
            [2, 1, 1]
        );
    }
}
