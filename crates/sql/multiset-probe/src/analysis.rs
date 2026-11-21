//! Port of the supported rules in Java's MultisetAnalysis and LosslessCastLineage.
//! Facts describe the integral of a delta stream, not the delta's signed weights.

use crate::model::{Circuit, Closure, ExprKind, OperatorKind, Scalar, Type, TypeKind};
use std::collections::BTreeSet;

/// A field is indivisible here, even when its SQL type is itself a row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Source {
    parameter: usize,
    part: Part,
    field: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Part {
    Row,
    Key,
    Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Lineage {
    Unknown,
    Field(Source),
    Ref(Box<Lineage>),
    Tuple(Vec<Lineage>),
}

impl Lineage {
    fn initial(ty: &Type, parameter: usize, part: Part) -> Self {
        match &ty.kind {
            TypeKind::Ref(inner) => Self::Ref(Box::new(Self::initial(inner, parameter, part))),
            TypeKind::RawTuple(fields) if part == Part::Row && fields.len() == 2 => {
                Self::Tuple(vec![
                    Self::initial(&fields[0], parameter, Part::Key),
                    Self::initial(&fields[1], parameter, Part::Value),
                ])
            }
            TypeKind::Tuple(fields) => Self::Tuple(
                (0..fields.len())
                    .map(|field| {
                        Self::Field(Source {
                            parameter,
                            part,
                            field,
                        })
                    })
                    .collect(),
            ),
            _ => Self::Unknown,
        }
    }

    /// Collect copied fields, returning whether every component is a known copy.
    fn copied_fields(&self, copied: &mut BTreeSet<Source>) -> bool {
        match self {
            Self::Unknown => false,
            Self::Field(source) => {
                copied.insert(*source);
                true
            }
            Self::Ref(inner) => inner.copied_fields(copied),
            Self::Tuple(fields) => fields
                .iter()
                .fold(true, |all, field| field.copied_fields(copied) & all),
        }
    }

    fn intersection(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Field(a), Self::Field(b)) if a == b => self.clone(),
            (Self::Ref(a), Self::Ref(b)) => Self::Ref(Box::new(a.intersection(b))),
            (Self::Tuple(a), Self::Tuple(b)) if a.len() == b.len() => {
                Self::Tuple(a.iter().zip(b).map(|(a, b)| a.intersection(b)).collect())
            }
            _ => Self::Unknown,
        }
    }
}

fn lossless_cast(from: &Type, to: &Type, safe: bool) -> bool {
    match (&from.kind, &to.kind) {
        (
            TypeKind::Integer {
                width: a,
                signed: sa,
            },
            TypeKind::Integer {
                width: b,
                signed: sb,
            },
        ) => {
            // An unsafe cast fails instead of merging two successful values.
            if !safe || (sa == sb && b >= a) || (*sb && !*sa && b > a) {
                return true;
            }
        }
        (
            TypeKind::String { fixed: false, .. },
            TypeKind::String {
                fixed: false,
                precision: -1,
            },
        ) => return true,
        _ => {}
    }
    from.kind == to.kind
        && !(to.kind == TypeKind::Scalar(Scalar::Variant) && from.nullable && !to.nullable)
}

pub(crate) fn injective(closure: &Closure) -> bool {
    if closure.has_unhandled_control_flow {
        return false;
    }
    let parameters: Vec<_> = closure
        .parameters
        .iter()
        .enumerate()
        .map(|(index, ty)| Lineage::initial(ty, index, Part::Row))
        .collect();
    let mut needed = BTreeSet::new();
    for (ty, value) in closure.parameters.iter().zip(&parameters) {
        if !ty.has_single_value() && !value.copied_fields(&mut needed) {
            return false;
        }
    }
    let mut values: Vec<Lineage> = Vec::with_capacity(closure.expressions.len());
    for expression in &closure.expressions {
        let value = match &expression.kind {
            ExprKind::Unknown => Lineage::Unknown,
            ExprKind::Parameter(index) => parameters[*index].clone(),
            ExprKind::Copy(source) => values[source.0].clone(),
            ExprKind::Borrow(source) => Lineage::Ref(Box::new(values[source.0].clone())),
            ExprKind::Deref(source) => match &values[source.0] {
                Lineage::Ref(inner) => *inner.clone(),
                _ => Lineage::Unknown,
            },
            ExprKind::Field(source, field) => match &values[source.0] {
                Lineage::Tuple(fields) => fields.get(*field).cloned().unwrap_or(Lineage::Unknown),
                _ => Lineage::Unknown,
            },
            ExprKind::Tuple(fields) => {
                Lineage::Tuple(fields.iter().map(|f| values[f.0].clone()).collect())
            }
            ExprKind::Intersection(a, b) => values[a.0].intersection(&values[b.0]),
            ExprKind::Cast {
                source,
                from,
                to,
                safe,
            } => {
                if lossless_cast(from, to, *safe) {
                    values[source.0].clone()
                } else {
                    Lineage::Unknown
                }
            }
        };
        values.push(value);
    }
    let mut copied = BTreeSet::new();
    values[closure.body.0].copied_fields(&mut copied);
    needed.is_subset(&copied)
}

pub(crate) fn analyze(circuit: &Circuit) -> Vec<Vec<bool>> {
    let mut facts: Vec<Vec<bool>> = Vec::with_capacity(circuit.operators.len());
    for operator in &circuit.operators {
        let input = |index: usize| {
            let port = operator.inputs[index];
            facts[port.operator.0][port.output]
        };
        facts.push(operator_facts(&operator.kind, input));
    }
    facts
}

pub(crate) fn constant_multiset(
    weights: impl ExactSizeIterator<Item = i64>,
    indexed: bool,
) -> bool {
    // Java only proves the empty collection or a single unindexed row with
    // weight zero or one to be a set.
    let mut weights = weights;
    match weights.len() {
        0 => false,
        1 if !indexed => !(0..=1).contains(&weights.next().expect("one constant weight")),
        _ => true,
    }
}

pub(crate) fn operator_facts(kind: &OperatorKind, input: impl Fn(usize) -> bool) -> Vec<bool> {
    let multiset = match kind {
        OperatorKind::Source { primary_key } => !primary_key,
        OperatorKind::Now
        | OperatorKind::Distinct(_)
        | OperatorKind::ChainAggregate
        | OperatorKind::SingleValue => false,
        OperatorKind::Constant {
            weights, indexed, ..
        } => constant_multiset(weights.iter().copied(), *indexed),
        OperatorKind::Sum
        | OperatorKind::Negate
        | OperatorKind::FlatMap
        | OperatorKind::FlatMapIndex => true,
        OperatorKind::Filter(_)
        | OperatorKind::Positive
        | OperatorKind::Noop
        | OperatorKind::WeightValidator
        | OperatorKind::View { .. }
        | OperatorKind::Sink
        | OperatorKind::Delay
        | OperatorKind::Differentiate
        | OperatorKind::Integrate
        | OperatorKind::Window
        | OperatorKind::RetainTrace => input(0),
        OperatorKind::Map(closure) | OperatorKind::Deindex(closure) => {
            input(0) || !injective(closure)
        }
        OperatorKind::InnerJoin(closure, _) => input(0) || input(1) || !injective(closure),
        OperatorKind::ControlledFilter => return vec![input(0), true],
        OperatorKind::StreamOperator { outputs, .. } => return vec![true; *outputs],
    };
    vec![multiset]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Expr, ExprId, Operator, OperatorId, Port};

    fn integer(width: u64, signed: bool) -> Type {
        Type {
            nullable: true,
            kind: TypeKind::Integer { width, signed },
        }
    }

    fn row(fields: usize) -> Type {
        Type {
            nullable: false,
            kind: TypeKind::Tuple(vec![integer(32, true); fields]),
        }
    }

    fn projection(fields: &[usize]) -> Closure {
        let mut expressions = vec![Expr {
            kind: ExprKind::Parameter(0),
        }];
        for field in fields {
            expressions.push(Expr {
                kind: ExprKind::Field(ExprId(0), *field),
            });
        }
        let body = ExprId(expressions.len());
        expressions.push(Expr {
            kind: ExprKind::Tuple((1..=fields.len()).map(ExprId).collect()),
        });
        Closure {
            parameters: vec![row(2)],
            expressions,
            body,
            has_unhandled_control_flow: false,
        }
    }

    #[test]
    fn every_field_must_survive_but_order_and_duplicates_do_not_matter() {
        assert!(injective(&projection(&[1, 0])));
        assert!(injective(&projection(&[0, 1, 0])));
        assert!(!injective(&projection(&[0, 0])));
        assert!(!injective(&projection(&[])));
        let mut returning = projection(&[0, 1]);
        returning.has_unhandled_control_flow = true;
        assert!(!injective(&returning));
    }

    #[test]
    fn indexed_key_and_value_are_distinct_sources() {
        let mut closure = projection(&[0, 1]);
        closure.parameters = vec![Type {
            nullable: false,
            kind: TypeKind::RawTuple(vec![row(1), row(1)]),
        }];
        // Keeping both halves retains every field; keeping only the key does not.
        assert!(injective(&closure));
        closure.body = ExprId(1);
        assert!(!injective(&closure));
    }

    #[test]
    fn safe_integer_casts_require_range_inclusion() {
        assert!(!lossless_cast(&integer(32, true), &integer(8, true), true));
        assert!(lossless_cast(&integer(32, true), &integer(8, true), false));
        assert!(lossless_cast(&integer(32, false), &integer(64, true), true));
        assert!(!lossless_cast(
            &integer(32, false),
            &integer(32, true),
            true
        ));
        assert!(!lossless_cast(&integer(8, true), &integer(64, false), true));
    }

    #[test]
    fn casts_that_merge_values_do_not_preserve_lineage() {
        let string = |precision, fixed| Type {
            nullable: true,
            kind: TypeKind::String { precision, fixed },
        };
        assert!(!lossless_cast(&string(3, false), &string(4, false), false));
        assert!(lossless_cast(&string(3, false), &string(-1, false), false));
        assert!(!lossless_cast(&string(3, true), &string(-1, false), false));
        let variant = Type {
            nullable: true,
            kind: TypeKind::Scalar(Scalar::Variant),
        };
        assert!(!lossless_cast(
            &variant,
            &Type {
                nullable: false,
                ..variant.clone()
            },
            false
        ));
    }

    #[test]
    fn constant_distinctness_matches_the_conservative_java_rule() {
        let values = [vec![], vec![0], vec![1], vec![-1], vec![2], vec![1, 1]];
        let circuit = Circuit {
            operators: values
                .into_iter()
                .map(|weights| Operator {
                    inputs: vec![],
                    kind: OperatorKind::Constant {
                        weights,
                        indexed: false,
                        stream: crate::model::CollectionKind::Snapshot,
                    },
                })
                .collect(),
        };
        assert_eq!(
            analyze(&circuit),
            vec![
                vec![false],
                vec![false],
                vec![false],
                vec![true],
                vec![true],
                vec![true]
            ]
        );
    }

    #[test]
    fn controlled_filter_errors_are_a_multiset_even_when_data_is_a_set() {
        let source = |id| Port {
            operator: OperatorId(id),
            output: 0,
        };
        let circuit = Circuit {
            operators: vec![
                Operator {
                    inputs: vec![],
                    kind: OperatorKind::Source { primary_key: true },
                },
                Operator {
                    inputs: vec![source(0)],
                    kind: OperatorKind::SingleValue,
                },
                Operator {
                    inputs: vec![source(0), source(1)],
                    kind: OperatorKind::ControlledFilter,
                },
                Operator {
                    inputs: vec![Port {
                        operator: OperatorId(2),
                        output: 1,
                    }],
                    kind: OperatorKind::Sink,
                },
            ],
        };
        assert_eq!(
            analyze(&circuit),
            vec![vec![false], vec![false], vec![false, true], vec![true]]
        );
    }
}
