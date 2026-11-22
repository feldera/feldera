//! Combine field waterlines for a join key or for rows from several input streams.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy)]
pub enum BoundCombination {
    Union,
    Intersection,
}

/// Slots refer to the caller's type objects, including their precise SQL types.
#[derive(Deserialize)]
pub struct BoundType {
    slot: usize,
    #[serde(flatten)]
    form: Form,
    #[serde(skip)]
    rebuilt: bool,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Form {
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

impl BoundType {
    fn combine(self, other: Self, op: BoundCombination) -> Option<Self> {
        let form = match self.form {
            Form::Scalar { monotone } => {
                let keep_left = match op {
                    BoundCombination::Union => monotone,
                    BoundCombination::Intersection => !monotone,
                };
                return Some(if keep_left { self } else { other });
            }
            Form::Tuple { fields } => {
                let Form::Tuple { fields: right } = other.form else {
                    return None;
                };
                let fields = fields
                    .into_iter()
                    .zip(right)
                    .map(|(a, b)| a.combine(b, op))
                    .collect::<Option<_>>()?;
                Form::Tuple { fields }
            }
            Form::Reference { base } => {
                let Form::Reference { base: right } = other.form else {
                    return None;
                };
                Form::Reference {
                    base: Box::new(base.combine(*right, op)?),
                }
            }
            Form::Opaque => return None,
        };
        Some(Self {
            form,
            rebuilt: true,
            slot: self.slot,
        })
    }

    fn selection(self) -> BoundSelection {
        if !self.rebuilt {
            return BoundSelection::Original(self.slot);
        }
        let fields = match self.form {
            Form::Tuple { fields } => fields.into_iter().map(Self::selection).collect(),
            Form::Reference { base } => vec![base.selection()],
            _ => unreachable!("only tuple and reference projections are rebuilt"),
        };
        BoundSelection::Rebuild {
            template: self.slot,
            fields,
        }
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
