//! Group compatible aggregate implementations while preserving SQL output order.
use feldera_sql_ir::{Error, ExpressionKind, InnerKind, Node};
use serde::Deserialize;

#[derive(Clone, Copy, Deserialize)]
pub enum AggregateKind {
    #[serde(rename = "LinearAggregate")]
    Linear,
    #[serde(rename = "MinMaxAggregate")]
    MinMax,
    #[serde(rename = "NonLinearAggregate")]
    NonLinear,
}

/// Closures that read the input row, in the order of getRowVariableReferences.
pub(super) fn row_parameters(aggregate: &Node) -> Result<&'static [(&'static str, usize)], Error> {
    match aggregate.inner_kind() {
        Some(InnerKind::Expression(ExpressionKind::LinearAggregate)) => Ok(&[("map", 0)]),
        Some(InnerKind::Expression(ExpressionKind::NonLinearAggregate)) => Ok(&[("increment", 1)]),
        Some(InnerKind::Expression(ExpressionKind::MinMaxAggregate)) => {
            Ok(&[("increment", 1), ("comparedValue", 0)])
        }
        _ => Err(Error::new(
            "expected a linear, nonlinear or min/max aggregate",
        )),
    }
}

/// Indexes refer to the original aggregate list. Concatenating the groups
/// gives the permutation used to restore the requested column order.
pub fn aggregate_groups(kinds: &[AggregateKind], append_only: bool) -> Vec<Vec<usize>> {
    let mut groups = Vec::<Vec<usize>>::new();
    let mut linear = None;
    let mut non_linear = None;
    for (index, kind) in kinds.iter().enumerate() {
        let slot = match kind {
            AggregateKind::NonLinear => &mut non_linear,
            AggregateKind::Linear => &mut linear,
            AggregateKind::MinMax if append_only => &mut linear,
            AggregateKind::MinMax => {
                // A retractable MIN/MAX needs its own accumulator.
                groups.push(vec![index]);
                continue;
            }
        };
        let group = slot.get_or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[*group].push(index);
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::{aggregate_groups, AggregateKind::*};

    #[test]
    fn min_max_only_shares_an_accumulator_on_append_only_inputs() {
        let kinds = [MinMax, NonLinear, Linear, MinMax, NonLinear, Linear];
        assert_eq!(
            aggregate_groups(&kinds, true),
            [vec![0, 2, 3, 5], vec![1, 4]]
        );
        assert_eq!(
            aggregate_groups(&kinds, false),
            [vec![0], vec![1, 4], vec![2, 5], vec![3]]
        );
    }
}
