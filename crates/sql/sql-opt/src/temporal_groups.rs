//! Group consecutive SQL predicates into windows without moving residual filters.
use crate::index_key_equivalence::equivalent_bodies;
use crate::temporal_comparisons::conjoin;
use crate::{TemporalPredicate, WindowComparison};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, NodeId};

#[derive(Clone, Copy)]
pub struct TemporalBoundKey {
    pub parameter: NodeId,
    pub key: ExpressionId,
    pub comparison: WindowComparison,
}

impl TemporalBoundKey {
    fn compatible(self, edit: &CircuitEditor, other: Self) -> Result<Option<bool>, Error> {
        use WindowComparison::*;
        let lower = |op| matches!(op, Greater | GreaterEqual);
        if lower(self.comparison) == lower(other.comparison) && self.comparison != other.comparison
        {
            return Ok(Some(false));
        }
        equivalent_bodies(
            edit,
            self.key.into(),
            other.key.into(),
            Some((edit.node(self.parameter)?, edit.node(other.parameter)?)),
        )
    }
}

#[derive(Clone, Copy)]
pub enum WindowTerm {
    Bound(TemporalBoundKey),
    NoNow(ExpressionId),
    Residual(ExpressionId),
}

impl WindowTerm {
    pub(super) fn from_predicate(parameter: NodeId, predicate: &TemporalPredicate) -> Self {
        match *predicate {
            TemporalPredicate::NoNow(expression) => Self::NoNow(expression),
            TemporalPredicate::Residual(expression) => Self::Residual(expression),
            TemporalPredicate::Bound {
                key, comparison, ..
            } => Self::Bound(TemporalBoundKey {
                parameter,
                key,
                comparison,
            }),
        }
    }
}

/// MergeFilters checks each upstream bound against the downstream bound.
/// The upstream bounds can have different inclusivity on the opposite side.
pub fn accepts_temporal_bound(
    edit: &CircuitEditor,
    upstream: &[WindowTerm],
    downstream: WindowTerm,
) -> Result<Option<bool>, Error> {
    let WindowTerm::Bound(next) = downstream else {
        return Ok(Some(false));
    };
    for term in upstream {
        let WindowTerm::Bound(bound) = *term else {
            return Ok(Some(false));
        };
        match bound.compatible(edit, next)? {
            Some(true) => {}
            other => return Ok(other),
        }
    }
    Ok(Some(!upstream.is_empty()))
}

pub struct TemporalGroup {
    pub first: usize,
    pub count: usize,
    /// A window uses the bounds at the indicated positions. Other groups
    /// carry their combined predicate, preserving SQL nullability.
    pub predicate: Option<ExpressionId>,
}

pub fn group_temporal_filters(
    edit: &mut CircuitEditor,
    terms: &[WindowTerm],
) -> Result<Option<Vec<TemporalGroup>>, Error> {
    use WindowTerm::*;
    let mut result = Vec::new();
    let mut first = 0;
    while first < terms.len() {
        let head = terms[first];
        let mut predicate = match head {
            Bound(_) => None,
            NoNow(v) | Residual(v) => Some(v),
        };
        let mut end = first + 1;
        while let Some(&next) = terms.get(end) {
            match (head, next) {
                (Bound(_), Bound(_)) => {
                    match accepts_temporal_bound(edit, &terms[first..end], next)? {
                        Some(true) => {}
                        Some(false) => break,
                        None => return Ok(None),
                    }
                }
                (NoNow(_), NoNow(value)) | (Residual(_), NoNow(value) | Residual(value)) => {
                    predicate = Some(conjoin(edit, predicate.expect("residual group"), value)?);
                }
                (Residual(_), Bound(_)) => {
                    return Err(Error::new(
                        "window bound follows a residual clock predicate",
                    ));
                }
                _ => break,
            }
            end += 1;
        }
        result.push(TemporalGroup {
            first,
            count: end - first,
            predicate,
        });
        first = end;
    }
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{Circuit, ExpressionKind, Field};
    use serde_json::json;

    #[test]
    fn bounds_compare_the_row_under_its_binding_and_preserve_open_endpoints() -> Result<(), Error> {
        let circuit = Circuit::from_json(
            json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]}),
        )?;
        let (circuit, roots)=circuit.with_inner_roots([json!({"class":"DBSPTypeInteger","id":1,"code":"INT32","width":32,"signed":true,"mayBeNull":false})])?;
        let mut edit = CircuitEditor::new(circuit);
        let mut term = |name, op| -> Result<WindowTerm, Error> {
            let ty = edit.type_id(roots[0])?;
            let key = edit.named_variable(ty, name)?;
            let parameter = edit.inner_at(
                roots[0],
                feldera_sql_ir::InnerKind::Parameter,
                [
                    ("name", Field::Data(name.into())),
                    ("type", Field::Reference(roots[0])),
                ],
            )?;
            Ok(WindowTerm::Bound(TemporalBoundKey {
                parameter,
                key,
                comparison: op,
            }))
        };
        use WindowComparison::*;
        let a = term("a", Greater)?;
        let b = term("b", GreaterEqual)?;
        let c = term("c", Less)?;
        assert_eq!(accepts_temporal_bound(&edit, &[a, b], c)?, Some(true));
        let groups = group_temporal_filters(&mut edit, &[a, c, b])?.unwrap();
        assert_eq!(groups.iter().map(|g| g.count).collect::<Vec<_>>(), [2, 1]);
        let boolean = edit.bool_type(roots[0], false)?;
        let v = edit.make_expr(
            roots[0],
            ExpressionKind::BoolLiteral,
            boolean,
            [("value", Field::Data(true.into()))],
        )?;
        let groups = group_temporal_filters(
            &mut edit,
            &[
                WindowTerm::NoNow(v),
                WindowTerm::Residual(v),
                WindowTerm::NoNow(v),
            ],
        )?
        .unwrap();
        assert_eq!(groups.iter().map(|g| g.count).collect::<Vec<_>>(), [1, 2]);
        Ok(())
    }
}
