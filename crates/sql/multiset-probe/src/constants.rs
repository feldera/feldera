//! Constant propagation over the supported flat graph and closure subset.

use crate::analysis::{constant_multiset, operator_facts};
use crate::model::{
    Arithmetic, Circuit, CollectionKind, Comparison, Constant, ConstantInput, ConstantLiterals,
    ConstantMapBody, ConstantValue, Edit, FilterPredicate, GeneratedInteger, OperatorId,
    OperatorKind, Port, ScalarLiteral,
};
use crate::{scalar, Error, Result};
use std::cmp::Ordering;
use std::collections::HashMap;

pub(crate) fn propagate(
    circuit: &Circuit,
    inputs: Vec<ConstantInput>,
    literals: &mut ConstantLiterals,
) -> Result<Vec<Edit>> {
    let mut aliases = Vec::<Option<Port>>::with_capacity(inputs.len());
    let mut constants = Vec::<Option<Constant>>::with_capacity(inputs.len());
    let mut facts = Vec::<Vec<bool>>::with_capacity(inputs.len());
    let mut edits = Vec::with_capacity(inputs.len());
    let mut row_budget = 1_000_000usize;

    for (index, (operator, candidate)) in circuit.operators.iter().zip(inputs).enumerate() {
        let ports: Vec<_> = operator
            .inputs
            .iter()
            .map(|port| aliases[port.operator.0].unwrap_or(*port))
            .collect();
        let input_fact = |index: usize| {
            let port = ports[index];
            facts[port.operator.0][port.output]
        };
        let alias = if matches!(operator.kind, OperatorKind::Distinct(CollectionKind::Delta))
            && !matches!(
                circuit.operators[ports[0].operator.0].kind,
                OperatorKind::ControlledFilter
            )
            && !input_fact(0)
        {
            Some(ports[0])
        } else {
            None
        };
        let source = ports
            .first()
            .filter(|port| port.output == 0)
            .and_then(|port| constants[port.operator.0].as_ref());
        let folded = match source {
            Some(source) => fold(
                &operator.kind,
                &candidate,
                source,
                OperatorId(index),
                literals,
                &mut row_budget,
            )?,
            None => None,
        };
        let output_facts = match folded.as_ref().map(|constant| &constant.value) {
            Some(ConstantValue::Indexed { .. }) => vec![false],
            Some(ConstantValue::Rows { rows, .. }) => {
                vec![constant_multiset(
                    rows.iter().map(|(_, weight)| *weight),
                    false,
                )]
            }
            None => operator_facts(&operator.kind, input_fact),
        };
        let edit = if alias.is_some() {
            Edit::Bypass
        } else if let Some(constant) = &folded {
            Edit::FoldedConstant(constant.clone())
        } else {
            Edit::Retain(ports)
        };
        facts.push(output_facts);
        constants.push(folded.or(candidate.value));
        aliases.push(alias);
        edits.push(edit);
    }
    Ok(edits)
}

fn fold(
    kind: &OperatorKind,
    candidate: &ConstantInput,
    source: &Constant,
    operator: OperatorId,
    literals: &mut ConstantLiterals,
    row_budget: &mut usize,
) -> Result<Option<Constant>> {
    if !matches!(kind, OperatorKind::Negate | OperatorKind::Filter(_)) && candidate.map.is_none() {
        return Ok(None);
    }
    let ConstantValue::Rows { element_type, rows } = &source.value else {
        return Ok(matches!(kind, OperatorKind::Negate).then(|| Constant {
            origin: operator,
            value: source.value.clone(),
        }));
    };
    *row_budget = row_budget
        .checked_sub(rows.len())
        .ok_or_else(|| Error("constant propagation row budget exceeded".into()))?;

    let mut result_type = *element_type;
    let (origin, result) = match kind {
        OperatorKind::Negate => (
            operator,
            rows.iter()
                .map(|&(row, weight)| (row, weight.wrapping_neg()))
                .collect(),
        ),
        OperatorKind::Filter(predicate) => {
            let result = match predicate {
                _ if rows.is_empty() => Vec::new(),
                FilterPredicate::Literal(true) => rows.clone(),
                FilterPredicate::Literal(false) => Vec::new(),
                FilterPredicate::Null => return Ok(None),
                FilterPredicate::RowField(_)
                | FilterPredicate::Comparison { .. }
                | FilterPredicate::IsNull { .. }
                | FilterPredicate::Not(_)
                | FilterPredicate::WrapBool(_) => {
                    let mut retained = Vec::new();
                    for &(row, weight) in rows {
                        match evaluate_predicate(predicate, row, literals)? {
                            FoldedPredicate::Known(Some(true)) => retained.push((row, weight)),
                            FoldedPredicate::Known(Some(false)) => {}
                            FoldedPredicate::Known(None) | FoldedPredicate::Unfolded => {
                                return Ok(None)
                            }
                        }
                    }
                    retained
                }
                FilterPredicate::Dynamic => {
                    return Err(Error(
                        "constant filter needs scalar expression evaluation".into(),
                    ));
                }
            };
            (source.origin, result)
        }
        _ => {
            let map = candidate.map.as_ref().expect("eligible constant map");
            result_type = map.element_type;
            let result = match &map.body {
                _ if rows.is_empty() => Vec::new(),
                ConstantMapBody::Identity => rows.clone(),
                ConstantMapBody::Field(column) => project_column(literals, rows, *column)?,
                ConstantMapBody::Arithmetic(expression) => {
                    let Some(result) = arithmetic_rows(literals, rows, expression)? else {
                        return Ok(None);
                    };
                    result
                }
                ConstantMapBody::Literal(row) => {
                    let mut total: Option<i64> = None;
                    for &(_, weight) in rows {
                        total = match total {
                            Some(sum) => {
                                let combined = sum.wrapping_add(weight);
                                (combined != 0).then_some(combined)
                            }
                            None => Some(weight),
                        };
                    }
                    total.into_iter().map(|weight| (*row, weight)).collect()
                }
                ConstantMapBody::Other => {
                    return Err(Error(
                        "constant map needs scalar expression evaluation".into(),
                    ));
                }
            };
            (source.origin, result)
        }
    };
    Ok(Some(Constant {
        origin,
        value: ConstantValue::Rows {
            element_type: result_type,
            rows: result,
        },
    }))
}

enum FoldedPredicate {
    Known(Option<bool>),
    Unfolded,
}

fn evaluate_predicate(
    predicate: &FilterPredicate,
    row: u64,
    literals: &ConstantLiterals,
) -> Result<FoldedPredicate> {
    use FoldedPredicate::{Known, Unfolded};
    match predicate {
        FilterPredicate::Literal(value) => Ok(Known(Some(*value))),
        FilterPredicate::Null => Ok(Known(None)),
        FilterPredicate::RowField(column) => match scalar::column(literals, row, *column)?.1 {
            ScalarLiteral::Bool(value) => Ok(Known(value)),
            _ => Err(Error("constant filter needs a boolean column".into())),
        },
        FilterPredicate::IsNull { operand, nullable } => {
            if !nullable {
                return Ok(Known(Some(false)));
            }
            Ok(match scalar::evaluate(literals, row, operand)? {
                Some(scalar) => Known(Some(scalar.value.is_null())),
                None => Unfolded,
            })
        }
        FilterPredicate::Not(source) => Ok(match evaluate_predicate(source, row, literals)? {
            Known(value) => Known(value.map(|boolean| !boolean)),
            Unfolded => Unfolded,
        }),
        FilterPredicate::WrapBool(source) => Ok(match evaluate_predicate(source, row, literals)? {
            Known(value) => Known(Some(value.unwrap_or(false))),
            Unfolded => Unfolded,
        }),
        FilterPredicate::Comparison { kind, left, right } => {
            let (Some(left), Some(right)) = (
                scalar::evaluate(literals, row, left)?,
                scalar::evaluate(literals, row, right)?,
            ) else {
                return Ok(Unfolded);
            };
            compare_scalars(kind, left.value, right.value)
        }
        FilterPredicate::Dynamic => Ok(Unfolded),
    }
}

fn compare_scalars(
    kind: &Comparison,
    left: ScalarLiteral,
    right: ScalarLiteral,
) -> Result<FoldedPredicate> {
    let order = match (left, right) {
        (ScalarLiteral::Int8(left), ScalarLiteral::Int8(right)) => {
            left.zip(right).map(|(left, right)| left.cmp(&right))
        }
        (ScalarLiteral::Int32(left), ScalarLiteral::Int32(right)) => {
            left.zip(right).map(|(left, right)| left.cmp(&right))
        }
        (ScalarLiteral::Int64(left), ScalarLiteral::Int64(right)) => {
            left.zip(right).map(|(left, right)| left.cmp(&right))
        }
        (ScalarLiteral::Bool(left), ScalarLiteral::Bool(right)) => {
            if !matches!(kind, Comparison::Equal | Comparison::NotEqual) {
                return Ok(FoldedPredicate::Unfolded);
            }
            left.zip(right).map(|(left, right)| left.cmp(&right))
        }
        _ => return Err(Error("mismatched scalar comparison types".into())),
    };
    Ok(FoldedPredicate::Known(order.map(|order| match kind {
        Comparison::Equal => order == Ordering::Equal,
        Comparison::NotEqual => order != Ordering::Equal,
        Comparison::Less => order.is_lt(),
        Comparison::LessOrEqual => order.is_le(),
        Comparison::Greater => order.is_gt(),
        Comparison::GreaterOrEqual => order.is_ge(),
    })))
}

fn project_column(
    literals: &ConstantLiterals,
    rows: &[(u64, i64)],
    column: usize,
) -> Result<Vec<(u64, i64)>> {
    let mut mapped = ScalarRows::default();
    for &(row, weight) in rows {
        let (id, value) = scalar::column(literals, row, column)?;
        mapped.add(id, value, weight);
    }
    Ok(mapped.finish())
}

fn arithmetic_rows(
    literals: &mut ConstantLiterals,
    rows: &[(u64, i64)],
    expression: &Arithmetic,
) -> Result<Option<Vec<(u64, i64)>>> {
    let mut evaluated = Vec::with_capacity(rows.len());
    for &(row, weight) in rows {
        let Some(value) = scalar::arithmetic(literals, row, expression)? else {
            // Java retains the map if any result still contains arithmetic.
            return Ok(None);
        };
        evaluated.push((value, weight));
    }
    let mut mapped = ScalarRows::default();
    for (scalar, weight) in evaluated {
        let id = if scalar.computed {
            let value = match scalar.value {
                ScalarLiteral::Int8(Some(value)) => i32::from(value),
                ScalarLiteral::Int32(Some(value)) => value,
                _ => unreachable!("computed arithmetic yields a non-null integer"),
            };
            literals.last_id = literals
                .last_id
                .checked_add(1)
                .ok_or_else(|| Error("no fresh identity for integer literal".into()))?;
            let id = literals.last_id;
            let template = literals
                .generated
                .get(&scalar.source)
                .map_or(scalar.source, |generated| generated.template);
            literals
                .generated
                .insert(id, GeneratedInteger { template, value });
            literals.scalars.insert(id, scalar.value);
            id
        } else {
            scalar.source
        };
        mapped.add(id, scalar.value, weight);
    }
    Ok(Some(mapped.finish()))
}

#[derive(Default)]
struct ScalarRows {
    positions: HashMap<ScalarLiteral, usize>,
    rows: Vec<Option<(u64, i64)>>,
}

impl ScalarRows {
    fn add(&mut self, id: u64, value: ScalarLiteral, weight: i64) {
        if let Some(&position) = self.positions.get(&value) {
            let (original_id, old_weight) = self.rows[position].expect("live projected row");
            let total = old_weight.wrapping_add(weight);
            self.rows[position] = (total != 0).then_some((original_id, total));
            if total == 0 {
                self.positions.remove(&value);
            }
        } else {
            self.positions.insert(value, self.rows.len());
            self.rows.push(Some((id, weight)));
        }
    }

    fn finish(self) -> Vec<(u64, i64)> {
        self.rows.into_iter().flatten().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_projected_row_reinserted_after_cancellation_moves_to_the_end() {
        let literals = ConstantLiterals {
            scalars: [
                (10, ScalarLiteral::Int32(Some(1))),
                (11, ScalarLiteral::Int32(Some(2))),
                (12, ScalarLiteral::Int32(Some(1))),
            ]
            .into(),
            tuples: [(0, vec![10]), (1, vec![11]), (2, vec![12])].into(),
            ..ConstantLiterals::default()
        };
        let rows = [(0, 3), (1, 1), (0, -3), (2, 2)];
        assert_eq!(
            project_column(&literals, &rows, 0).unwrap(),
            [(11, 1), (12, 2)]
        );
    }
}
