//! Weighted constants consolidate rows by value. Arrays, maps and geographic
//! points compare their elements using Java expression-object equality.
use crate::{compare_literal_values, ValueComparisonError};
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
    for (index, &(existing, _)) in rows.iter().enumerate() {
        if compare_values(edit, existing, row.0).map_err(collection_error)? {
            merge_weight(rows, index, row.1);
            return Ok(());
        }
    }
    store_unmatched(edit, rows, row).map_err(collection_error)
}

/// Replace the weight at its original position, as LinkedHashMap.put does.
/// Constant projection uses this operation directly. Z-set append reaches it
/// only after exhausting the search for a row that can combine weights.
pub fn put(edit: &CircuitEditor, rows: &mut Vec<Row>, row: Row) -> Result<(), Error> {
    put_row(edit, rows, row).map_err(collection_error)
}

fn put_row(
    edit: &CircuitEditor,
    rows: &mut Vec<Row>,
    row: Row,
) -> Result<(), ValueComparisonError> {
    for (key, weight) in rows.iter_mut() {
        if equal_map_keys(edit, row.0, *key)? {
            *weight = row.1;
            return Ok(());
        }
    }
    rows.push(row);
    Ok(())
}

fn store_unmatched(
    edit: &CircuitEditor,
    rows: &mut Vec<Row>,
    row: Row,
) -> Result<(), ValueComparisonError> {
    let kind = edit.expr_kind(row.0)?;
    // For these values, a map-key match would already have matched the value
    // scan. Tuples can contain computed fields that lack value equality.
    if has_value_equality(kind) && !matches!(kind, Expr::Tuple | Expr::RawTuple) {
        rows.push(row);
        Ok(())
    } else {
        put_row(edit, rows, row)
    }
}

fn merge_weight(rows: &mut Vec<Row>, index: usize, weight: i64) {
    let total = &mut rows[index].1;
    *total = total.wrapping_add(weight);
    if *total == 0 {
        rows.remove(index);
    }
}

mod object_equality;
pub use object_equality::object_equals;
use object_equality::{compare_elements, equal_map_keys};

fn has_value_equality(kind: Expr) -> bool {
    kind.wire_name().ends_with("Literal")
        || matches!(
            kind,
            Expr::Tuple
                | Expr::RawTuple
                | Expr::Variant
                | Expr::Array
                | Expr::Map
                | Expr::GeoPointConstructor
                | Expr::ZSet
                | Expr::IndexedZSet
        )
}

fn collection_error(error: ValueComparisonError) -> Error {
    match error {
        ValueComparisonError::InvalidIr(error) => error,
        ValueComparisonError::InvalidTimestamp(_) | ValueComparisonError::UnresolvedTimestamp => {
            Error::new("invalid zoned timestamp in a constant collection")
        }
    }
}

/// Compare weighted collection literals under the compiler's row-value equality.
/// Equal rows cancel even when the two collections enumerate them in different orders.
pub fn equal_collections(
    edit: &CircuitEditor,
    left: ExpressionId,
    right: ExpressionId,
) -> Result<bool, Error> {
    compare_values(edit, left, right).map_err(collection_error)
}

type Pair = (ExpressionId, ExpressionId);

/// Subtract the right Z-set exactly as Java does: first consolidate its negation,
/// then append those rows to a copy of the left. Row equality is directional.
struct Difference {
    rows: Vec<Row>,
    incoming: std::vec::IntoIter<Row>,
    left: Option<Vec<Row>>,
    position: usize,
}

impl Difference {
    fn new(left: Vec<Row>, mut right: Vec<Row>) -> Self {
        for (_, weight) in &mut right {
            *weight = weight.wrapping_neg();
        }
        Self {
            rows: Vec::new(),
            incoming: right.into_iter(),
            left: Some(left),
            position: 0,
        }
    }

    /// Resume the linear search for the incoming row after comparing two values.
    fn advance(
        &mut self,
        edit: &CircuitEditor,
        matched: Option<bool>,
    ) -> Result<Step, ValueComparisonError> {
        match matched {
            Some(true) => {
                let (_, weight) = self.incoming.next().expect("row under comparison");
                merge_weight(&mut self.rows, self.position, weight);
                self.position = 0;
            }
            Some(false) => self.position += 1,
            None => {}
        }
        loop {
            if let Some(&(value, _)) = self.incoming.as_slice().first() {
                if let Some(&(existing, _)) = self.rows.get(self.position) {
                    return Ok(Step::Compare((existing, value)));
                }
                store_unmatched(
                    edit,
                    &mut self.rows,
                    self.incoming.next().expect("incoming row"),
                )?;
                self.position = 0;
            } else if let Some(left) = self.left.take() {
                self.incoming = std::mem::replace(&mut self.rows, left).into_iter();
            } else {
                return Ok(Step::Done(self.rows.is_empty()));
            }
        }
    }
}

enum Step {
    Compare(Pair),
    Done(bool),
}

enum Comparison {
    Immediate(bool),
    Fields(std::iter::Zip<std::vec::IntoIter<NodeId>, std::vec::IntoIter<NodeId>>),
    Difference(Difference),
}

impl Comparison {
    fn advance(
        &mut self,
        edit: &CircuitEditor,
        matched: Option<bool>,
    ) -> Result<Step, ValueComparisonError> {
        Ok(match self {
            Self::Immediate(equal) => Step::Done(*equal),
            Self::Fields(_) if matched == Some(false) => Step::Done(false),
            Self::Fields(fields) => match fields.next() {
                Some((a, b)) => Step::Compare((edit.expression(a)?, edit.expression(b)?)),
                None => Step::Done(true),
            },
            Self::Difference(diff) => return diff.advance(edit, matched),
        })
    }
}

/// Evaluate tuple fields, VARIANT payloads and weighted rows on a work stack.
/// Remember successful compound comparisons so shared subtrees are visited once.
pub fn compare_values(
    edit: &CircuitEditor,
    a: ExpressionId,
    b: ExpressionId,
) -> Result<bool, ValueComparisonError> {
    use std::collections::HashSet;
    let mut equal = HashSet::new();
    let mut active = HashSet::new();
    let mut parents = Vec::<(Pair, Comparison)>::new();
    let mut step = Step::Compare((a, b));
    loop {
        step = match step {
            Step::Compare((a, b)) if a == b => Step::Done(has_value_equality(edit.expr_kind(a)?)),
            Step::Compare(pair) if equal.contains(&pair) => Step::Done(true),
            Step::Compare(pair) => {
                let mut comparison = compare(edit, pair.0, pair.1)?;
                let first = comparison.advance(edit, None)?;
                if matches!(first, Step::Compare(_)) {
                    if !active.insert(pair) {
                        return Err(Error::new("cycle in a constant row").into());
                    }
                    parents.push((pair, comparison));
                }
                first
            }
            Step::Done(matched) => {
                let Some((pair, parent)) = parents.last_mut() else {
                    return Ok(matched);
                };
                let next = parent.advance(edit, Some(matched))?;
                if let Step::Done(result) = next {
                    active.remove(pair);
                    if result {
                        equal.insert(*pair);
                    }
                    parents.pop();
                }
                next
            }
        };
    }
}

fn compare(
    edit: &CircuitEditor,
    a: ExpressionId,
    b: ExpressionId,
) -> Result<Comparison, ValueComparisonError> {
    use Comparison::Immediate;
    let kind = edit.expr_kind(a)?;
    if kind != edit.expr_kind(b)? {
        return Ok(Immediate(false));
    }
    let (left, right) = (edit.node(a.into())?, edit.node(b.into())?);
    let same_type = || edit.same_type(edit.expression_type(a)?, edit.expression_type(b)?);
    let same_objects = |field: &str| -> Result<bool, ValueComparisonError> {
        match (left.field(field), right.field(field)) {
            (None, None) => Ok(true),
            (Some(_), Some(_)) => {
                let (xs, ys) = (left.edges(field)?, right.edges(field)?);
                if xs.len() != ys.len() {
                    return Ok(false);
                }
                for (x, y) in xs.into_iter().zip(ys) {
                    if !compare_elements(edit, x, y)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    };
    let edge =
        |node: &feldera_sql_ir::Node, field: &str| node.field(field).and_then(Field::node_id);
    let equal = match kind {
        Expr::Tuple | Expr::RawTuple => match (left.field("fields"), right.field("fields")) {
            (None, None) => true,
            (Some(_), Some(_)) => {
                let xs = left.edges("fields")?;
                let ys = right.edges("fields")?;
                if xs.len() != ys.len() {
                    return Ok(Immediate(false));
                }
                return Ok(Comparison::Fields(xs.into_iter().zip(ys)));
            }
            _ => false,
        },
        Expr::Array => same_type()? && same_objects("data")?,
        Expr::Map => same_type()? && same_objects("keys")? && same_objects("values")?,
        Expr::GeoPointConstructor => {
            for key in ["left", "right"] {
                match (edge(left, key), edge(right, key)) {
                    (None, None) => {}
                    (Some(x), Some(y)) if compare_elements(edit, x, y)? => {}
                    _ => return Ok(Immediate(false)),
                }
            }
            true
        }
        Expr::Variant => match (edge(left, "value"), edge(right, "value")) {
            (None, None) => {
                left.data("isSqlNull").and_then(|v| v.as_bool()) != Some(true)
                    || right.data("isSqlNull").and_then(|v| v.as_bool()) == Some(true)
            }
            (Some(x), Some(y)) => return Ok(Comparison::Fields(vec![x].into_iter().zip(vec![y]))),
            _ => false,
        },
        Expr::IndexedZSet => same_type()?,
        Expr::ZSet => {
            if !same_type()? {
                return Ok(Immediate(false));
            }
            return Ok(Comparison::Difference(Difference::new(
                entries(edit, a)?,
                entries(edit, b)?,
            )));
        }
        kind if kind.wire_name().ends_with("Literal") => compare_literal_values(edit, a, b)?,
        _ => false,
    };
    Ok(Immediate(equal))
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
    fn legacy_collection_with_an_unknown_instant_declines_only_when_compared() -> Result<(), Error>
    {
        let mut edit = editor();
        let mut set = |first: bool, spelling: &str| -> Result<_, Error> {
            let flag = literal(&mut edit, Expr::BoolLiteral, TypeKind::Bool, json!(first));
            let stamp = literal(
                &mut edit,
                Expr::TimestampTzLiteral,
                TypeKind::TimestampTz,
                json!(spelling),
            );
            let row = edit.tuple_expr(flag.into(), false, &[flag, stamp])?;
            let ty = edit.expression_type(row)?;
            multiset(&mut edit, ty, [(row, 1)])
        };
        let unknown = set(true, "2020-01-01 00:00:00 GMT+19:00")?;
        let same_prefix = set(true, "2020-01-01 00:00:00 UTC")?;
        let other_prefix = set(false, "2020-01-01 00:00:00 UTC")?;
        assert!(matches!(
            compare_values(&edit, unknown, same_prefix),
            Err(ValueComparisonError::UnresolvedTimestamp)
        ));
        assert!(matches!(
            compare_values(&edit, unknown, other_prefix),
            Ok(false)
        ));
        Ok(())
    }

    #[test]
    fn deep_shared_rows_cancel_inside_nested_weighted_collections() -> Result<(), Error> {
        let mut edit = editor();
        let site = edit.circuit().root_id();
        let ty = edit.bool_type(site, false)?;
        let mut row = |value| -> Result<ExpressionId, Error> {
            let mut row =
                edit.make_expr(site, Expr::BoolLiteral, ty, [("value", Field::Data(value))])?;
            for _ in 0..160 {
                row = edit.tuple_expr(site, false, &[row, row])?;
            }
            for _ in 0..8 {
                let element = edit.expression_type(row)?;
                row = multiset(&mut edit, element, [(row, -2)])?;
            }
            Ok(row)
        };
        let left = row(json!(true))?;
        let right = row(json!(true))?;
        let different = row(json!(false))?;
        assert!(equal_collections(&edit, left, right)?);
        assert!(!equal_collections(&edit, left, different)?);
        let mut rows = vec![(left, 3), (different, 1)];
        append(&edit, &mut rows, (right, -3))?;
        assert_eq!(rows, [(different, 1)]);
        Ok(())
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
    fn computed_rows_replace_weights_at_the_original_position() -> Result<(), Error> {
        let mut edit = editor();
        let site = edit.circuit().root_id();
        let ty = edit.bool_type(site, false)?;
        let x = edit.named_variable(ty, "x")?;
        let y = edit.named_variable(ty, "y")?;
        let mut rows = vec![(x, 4), (y, 1)];
        append(&edit, &mut rows, (x, -4))?;
        assert_eq!(rows, [(x, -4), (y, 1)]);
        for raw in [true, false] {
            let first = edit.tuple_expr(site, raw, &[x])?;
            let second = edit.tuple_expr(site, raw, &[x])?;
            let other = edit.tuple_expr(site, raw, &[y])?;
            let mut rows = vec![(first, 3), (other, 2)];
            append(&edit, &mut rows, (second, -3))?;
            assert_eq!(rows, [(first, -3), (other, 2)]);
            append(&edit, &mut rows, (first, 3))?;
            assert_eq!(rows, [(other, 2)]);
        }
        Ok(())
    }

    #[test]
    fn projected_timestamp_keys_use_the_hash_of_the_stored_spelling() -> Result<(), Error> {
        let mut edit = editor();
        let site = edit.circuit().root_id();
        let make_timestamp = |edit: &mut CircuitEditor, value| {
            literal(
                edit,
                Expr::TimestampTzLiteral,
                TypeKind::TimestampTz,
                json!(value),
            )
        };
        let west = make_timestamp(&mut edit, "2024-11-03 01:30:00 America/New_York");
        let utc = make_timestamp(&mut edit, "2024-11-03 05:30:00 UTC");
        let mut projected = vec![(west, 1)];
        put(&edit, &mut projected, (utc, 2))?;
        assert_eq!(projected, [(west, 1), (utc, 2)]);

        // The middle two leaves have the same coefficient in the outer tuple's
        // hash. Exchanging their spellings preserves both hash and equality.
        let w_u = edit.tuple_expr(site, true, &[west, utc])?;
        let w_w = edit.tuple_expr(site, true, &[west, west])?;
        let a = edit.tuple_expr(site, true, &[w_u, w_w])?;
        let u_w = edit.tuple_expr(site, true, &[utc, west])?;
        let b = edit.tuple_expr(site, true, &[w_w, u_w])?;
        let mut projected = vec![(a, 1)];
        put(&edit, &mut projected, (b, 2))?;
        assert_eq!(projected, [(a, 2)]);
        Ok(())
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
