//! Combine field waterlines for a join key or for rows from several input streams.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy)]
pub enum BoundCombination {
    Union,
    Intersection,
}

/// Slots refer to the caller's type objects, including their precise SQL types.
#[derive(Deserialize)]
#[serde(try_from = "WireBound")]
pub struct BoundType(Vec<BoundEntry>);

/// New callers send a preorder sequence so JSON nesting is independent of SQL row depth.
/// Earlier compiler packages embedded each field's bound in its parent.
#[derive(Deserialize)]
#[serde(untagged)]
enum WireBound {
    Preorder(Vec<BoundEntry>),
    Nested {
        slot: usize,
        #[serde(flatten)]
        form: NestedKind,
    },
}

#[derive(Clone, Copy, Deserialize)]
struct BoundEntry {
    slot: usize,
    #[serde(flatten)]
    form: BoundKind,
    /// Number of consecutive preorder entries occupied by this subtree.
    #[serde(skip)]
    span: usize,
    #[serde(skip)]
    rebuilt: bool,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BoundKind {
    Scalar { monotone: bool },
    Tuple { arity: usize },
    Reference,
    Opaque,
}

impl BoundKind {
    fn arity(self) -> usize {
        match self {
            Self::Tuple { arity } => arity,
            Self::Reference => 1,
            Self::Scalar { .. } | Self::Opaque => 0,
        }
    }
}

impl TryFrom<WireBound> for BoundType {
    type Error = &'static str;

    fn try_from(input: WireBound) -> Result<Self, Self::Error> {
        match input {
            WireBound::Preorder(entries) => Self::from_entries(entries),
            WireBound::Nested { slot, form } => {
                let (form, children) = match form {
                    NestedKind::Scalar { monotone } => (BoundKind::Scalar { monotone }, vec![]),
                    NestedKind::Opaque => (BoundKind::Opaque, vec![]),
                    NestedKind::Reference { base } => (BoundKind::Reference, vec![*base]),
                    NestedKind::Tuple { fields } => (
                        BoundKind::Tuple {
                            arity: fields.len(),
                        },
                        fields,
                    ),
                };
                let span = 1 + children.iter().map(|child| child.0.len()).sum::<usize>();
                let mut entries = Vec::with_capacity(span);
                entries.push(BoundEntry {
                    slot,
                    form,
                    span,
                    rebuilt: false,
                });
                entries.extend(children.into_iter().flat_map(|child| child.0));
                Ok(Self(entries))
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum NestedKind {
    Scalar { monotone: bool },
    Tuple { fields: Vec<BoundType> },
    Reference { base: Box<BoundType> },
    Opaque,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum BoundSelection {
    Original(usize),
    /// Retain the tuple type, clearing the bound of every value field.
    UnboundedFields {
        unbounded_fields: usize,
    },
    /// Reuse the original tuple's raw and nullable flags, or the original reference shape.
    Rebuild {
        template: usize,
        fields: Vec<Self>,
    },
}

impl BoundSelection {
    /// Java rebuilds children before their parent, preserving the original leaf objects.
    pub fn into_postorder(self) -> Vec<serde_json::Value> {
        use serde_json::json;
        let mut steps = Vec::new();
        let mut pending = vec![self];
        while let Some(selection) = pending.pop() {
            steps.push(match selection {
                Self::Original(slot) => json!(slot),
                Self::UnboundedFields { unbounded_fields } => {
                    json!({ "unbounded_fields": unbounded_fields })
                }
                Self::Rebuild { template, fields } => {
                    let arity = fields.len();
                    pending.extend(fields);
                    json!({ "template": template, "arity": arity })
                }
            });
        }
        steps.reverse();
        steps
    }
}

impl BoundType {
    fn from_entries(mut entries: Vec<BoundEntry>) -> Result<Self, &'static str> {
        let mut sizes = Vec::new();
        for entry in entries.iter_mut().rev() {
            let first = sizes
                .len()
                .checked_sub(entry.form.arity())
                .ok_or("missing waterline children")?;
            entry.span = 1 + sizes.drain(first..).sum::<usize>();
            sizes.push(entry.span);
        }
        if sizes.len() != 1 {
            return Err("expected one waterline root");
        }
        Ok(Self(entries))
    }

    /// Each child's span gives the offset of its next sibling.
    fn children(mut siblings: &[BoundEntry]) -> impl Iterator<Item = &[BoundEntry]> {
        std::iter::from_fn(move || {
            let (child, rest) = siblings.split_at(siblings.first()?.span);
            siblings = rest;
            Some(child)
        })
    }

    fn combine(self, other: Self, op: BoundCombination) -> Option<Self> {
        let mut result = Vec::new();
        let mut work = vec![(self.0.as_slice(), other.0.as_slice())];
        while let Some((left, right)) = work.pop() {
            let form = match (left[0].form, right[0].form) {
                (BoundKind::Scalar { monotone }, _) => {
                    let keep_left = match op {
                        BoundCombination::Union => monotone,
                        BoundCombination::Intersection => !monotone,
                    };
                    result.extend_from_slice(if keep_left { left } else { right });
                    continue;
                }
                (BoundKind::Reference, BoundKind::Reference) => BoundKind::Reference,
                (BoundKind::Tuple { arity: a }, BoundKind::Tuple { arity: b }) => {
                    BoundKind::Tuple { arity: a.min(b) }
                }
                _ => return None,
            };
            result.push(BoundEntry {
                form,
                rebuilt: true,
                ..left[0]
            });
            let mark = work.len();
            work.extend(Self::children(&left[1..]).zip(Self::children(&right[1..])));
            work[mark..].reverse();
        }
        Some(Self::from_entries(result).expect("combined children match each parent's arity"))
    }

    fn selection(self) -> BoundSelection {
        let mut pending = Vec::new();
        for entry in self.0.into_iter().rev() {
            let first = pending.len() - entry.form.arity();
            let value = if entry.rebuilt {
                BoundSelection::Rebuild {
                    template: entry.slot,
                    fields: pending.drain(first..).rev().collect(),
                }
            } else {
                pending.truncate(first);
                BoundSelection::Original(entry.slot)
            };
            pending.push(value);
        }
        pending.pop().expect("validated waterline root")
    }
}

/// A join accepts a bound from either input. A sum needs a bound from every input.
/// Preserve the winning operand's type identity even when both flags agree.
/// Shapes without a defined combination are returned to the caller for diagnostics.
pub fn combine_bounds(inputs: Vec<BoundType>, op: BoundCombination) -> Option<BoundSelection> {
    let mut inputs = inputs.into_iter();
    let first = inputs.next()?;
    inputs
        .try_fold(first, |a, b| a.combine(b, op))
        .map(BoundType::selection)
}

#[derive(Deserialize)]
pub struct IndexedInput {
    key: BoundType,
    /// The join carries the value projection unchanged, even for a wide nested row.
    value: usize,
}

/// The join function takes a common key followed by the two input values.
/// Either key can establish a lower bound. Value projections remain independent.
pub fn join_arguments([left, right]: [IndexedInput; 2]) -> Option<[BoundSelection; 3]> {
    let key = left.key.combine(right.key, BoundCombination::Union)?;
    Some([
        key.selection(),
        BoundSelection::Original(left.value),
        BoundSelection::Original(right.value),
    ])
}

/// A change on either side can retract a row. Only key fields bounded on both
/// inputs survive, and the result keeps the left input's value type.
pub fn antijoin_waterline([left, right]: [IndexedInput; 2]) -> Option<[BoundSelection; 2]> {
    let key = left
        .key
        .combine(right.key, BoundCombination::Intersection)?;
    Some([
        key.selection(),
        BoundSelection::UnboundedFields {
            unbounded_fields: left.value,
        },
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preorder_bounds_require_one_complete_tree() {
        for input in [
            r#"[]"#,
            r#"[{"slot":0,"kind":"reference"}]"#,
            r#"[{"slot":0,"kind":"tuple","arity":2},{"slot":1,"kind":"opaque"}]"#,
            r#"[{"slot":0,"kind":"opaque"},{"slot":1,"kind":"opaque"}]"#,
        ] {
            assert!(serde_json::from_str::<BoundType>(input).is_err(), "{input}");
        }
    }

    #[test]
    fn join_parameters_take_bounds_from_both_keys_and_keep_each_value_intact() {
        let inputs = r#"[
            {"key":{"slot":1,"kind":"tuple","fields":[
                {"slot":2,"kind":"scalar","monotone":true},{"slot":3,"kind":"scalar","monotone":false}]},"value":4},
            {"key":{"slot":6,"kind":"tuple","fields":[
                {"slot":7,"kind":"scalar","monotone":false},{"slot":8,"kind":"scalar","monotone":true}]},"value":9}
        ]"#;
        use BoundSelection::{Original, Rebuild};
        assert_eq!(
            join_arguments(serde_json::from_str(inputs).unwrap()),
            Some([
                Rebuild {
                    template: 1,
                    fields: vec![Original(2), Original(8)]
                },
                Original(4),
                Original(9),
            ])
        );
        assert_eq!(
            antijoin_waterline(serde_json::from_str(inputs).unwrap()),
            Some([
                Rebuild {
                    template: 1,
                    fields: vec![Original(7), Original(3)]
                },
                BoundSelection::UnboundedFields {
                    unbounded_fields: 4
                },
            ])
        );
    }

    #[test]
    fn compound_bounds_keep_leaf_identity_and_the_first_tuple_shape() {
        let inputs = r#"[
            {"slot":0,"kind":"tuple","fields":[{"slot":1,"kind":"scalar","monotone":true},
                {"slot":2,"kind":"reference","base":{"slot":3,"kind":"scalar","monotone":false}}]},
            {"slot":4,"kind":"tuple","fields":[{"slot":5,"kind":"scalar","monotone":true},
                {"slot":6,"kind":"reference","base":{"slot":7,"kind":"scalar","monotone":true}}]},
            {"slot":8,"kind":"tuple","fields":[{"slot":9,"kind":"scalar","monotone":false},
                {"slot":10,"kind":"reference","base":{"slot":11,"kind":"scalar","monotone":true}}]}
        ]"#;
        use BoundSelection::{Original, Rebuild};
        for (mode, a, b) in [
            (BoundCombination::Union, 1, 7),
            (BoundCombination::Intersection, 9, 3),
        ] {
            let result = combine_bounds(serde_json::from_str(inputs).unwrap(), mode);
            assert_eq!(
                result,
                Some(Rebuild {
                    template: 0,
                    fields: vec![
                        Original(a),
                        Rebuild {
                            template: 2,
                            fields: vec![Original(b)]
                        }
                    ],
                })
            );
        }
    }

    #[test]
    fn selecting_a_whole_operand_reuses_it_without_rebuilding_its_fields() {
        let inputs = r#"[
            {"slot":0,"kind":"scalar","monotone":false},
            {"slot":1,"kind":"tuple","fields":[]}
        ]"#;
        for (mode, slot) in [
            (BoundCombination::Union, 1),
            (BoundCombination::Intersection, 0),
        ] {
            assert_eq!(
                combine_bounds(serde_json::from_str(inputs).unwrap(), mode),
                Some(BoundSelection::Original(slot))
            );
        }
    }
}
