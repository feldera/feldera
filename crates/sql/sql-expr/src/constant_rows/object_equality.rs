//! Expression equality used by collection elements and keys in a LinkedHashMap.
use crate::{compare_literal_values, ValueComparisonError};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Node, NodeId};
use std::collections::{HashMap, HashSet};

type Pair = (NodeId, NodeId);

#[derive(Clone, Copy, PartialEq)]
enum Use {
    Element,
    MapKey,
}

/// Arrays, maps and point coordinates invoke the element's Java equals method.
pub fn object_equals(edit: &CircuitEditor, a: NodeId, b: NodeId) -> Result<bool, Error> {
    compare_elements(edit, a, b).map_err(super::collection_error)
}

pub(super) fn compare_elements(
    edit: &CircuitEditor,
    a: NodeId,
    b: NodeId,
) -> Result<bool, ValueComparisonError> {
    compare(edit, (a, b), Use::Element)
}

pub(super) fn equal_map_keys(
    edit: &CircuitEditor,
    a: ExpressionId,
    b: ExpressionId,
) -> Result<bool, ValueComparisonError> {
    compare(edit, (a.into(), b.into()), Use::MapKey)
}

enum Comparison {
    Unequal,
    /// For equal objects, the difference between their Java hash codes, modulo 2³².
    Equal(u32),
    Children(Vec<Pair>),
}

fn string_hash(node: &Node) -> u32 {
    node.data("value")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .encode_utf16()
        .fold(0u32, |hash, c| {
            hash.wrapping_mul(31).wrapping_add(u32::from(c))
        })
}

fn inspect(
    edit: &CircuitEditor,
    (a, b): Pair,
    usage: Use,
) -> Result<Comparison, ValueComparisonError> {
    use Comparison::*;
    if a == b {
        return Ok(Equal(0));
    }
    let (left, right) = (edit.node(a)?, edit.node(b)?);
    if left.class() != right.class() {
        return Ok(Unequal);
    }
    let a = edit.expression(a)?;
    let kind = edit.expr_kind(a)?;
    Ok(match kind {
        Expr::Tuple | Expr::RawTuple => match (left.field("fields"), right.field("fields")) {
            (None, None) => Equal(0),
            (Some(_), Some(_)) => {
                let (xs, ys) = (left.edges("fields")?, right.edges("fields")?);
                if xs == ys {
                    Equal(0)
                } else if kind == Expr::Tuple || xs.len() != ys.len() {
                    Unequal
                } else {
                    Children(xs.into_iter().zip(ys).collect())
                }
            }
            _ => Unequal,
        },
        // Point.hashCode includes Object.hashCode. Only a shared point has
        // a portable identity hash across detached compiler snapshots.
        Expr::GeoPointConstructor if usage == Use::Element => {
            let mut coordinates = Vec::new();
            for name in ["left", "right"] {
                match (left.field(name), right.field(name)) {
                    (None, None) => {}
                    (Some(_), Some(_)) => coordinates.push((left.edge(name)?, right.edge(name)?)),
                    _ => return Ok(Unequal),
                }
            }
            Children(coordinates)
        }
        k if k.wire_name().ends_with("Literal") => {
            let b = edit.expression(b)?;
            if !compare_literal_values(edit, a, b)?
                || !edit.same_type(edit.expression_type(a)?, edit.expression_type(b)?)?
            {
                Unequal
            } else if usage == Use::MapKey && k == Expr::TimestampTzLiteral {
                // Calcite hashes the stored spelling; literal equality compares runtime instants.
                Equal(string_hash(left).wrapping_sub(string_hash(right)))
            } else {
                Equal(0)
            }
        }
        _ => Unequal,
    })
}

fn compare(edit: &CircuitEditor, root: Pair, usage: Use) -> Result<bool, ValueComparisonError> {
    let matches = |delta| usage == Use::Element || delta == 0;
    let children = match inspect(edit, root, usage)? {
        Comparison::Unequal => return Ok(false),
        Comparison::Equal(delta) => return Ok(matches(delta)),
        Comparison::Children(children) => children,
    };
    // Raw tuples hash their fields with Arrays.hashCode. Evaluate shared
    // subtrees once, folding the child differences in field order.
    let mut stack = vec![(root, children.into_iter(), 0u32)];
    let mut active = HashSet::from([root]);
    let mut completed = HashMap::new();
    while let Some((parent, children, delta)) = stack.last_mut() {
        if let Some(pair) = children.next() {
            *delta = delta.wrapping_mul(31);
            if let Some(&known) = completed.get(&pair) {
                *delta = delta.wrapping_add(known);
                continue;
            }
            match inspect(edit, pair, usage)? {
                Comparison::Unequal => return Ok(false),
                Comparison::Equal(child) => *delta = delta.wrapping_add(child),
                Comparison::Children(fields) => {
                    if !active.insert(pair) {
                        return Err(Error::new("cycle in collection element equality").into());
                    }
                    stack.push((pair, fields.into_iter(), 0));
                }
            }
        } else {
            let (pair, child) = (*parent, *delta);
            active.remove(&pair);
            completed.insert(pair, child);
            stack.pop();
            let Some((_, _, delta)) = stack.last_mut() else {
                return Ok(matches(child));
            };
            *delta = delta.wrapping_add(child);
        }
    }
    unreachable!("the root returns after its last field")
}
