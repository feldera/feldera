//! Candidate rewrites for a Map or MapIndex following a live producer.
use feldera_sql_ir::OperatorKind;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ProjectionRule {
    #[default]
    Keep,
    ComposeMap,
    ComposeIndex,
    PushThrough,
    ProjectSum,
    ProjectJoin,
    ProjectAsof,
    ProjectIndexedJoin,
    ProjectFilteredJoin,
    ProjectFilteredStar,
    ProjectFlatMap,
    ProjectDeindex,
    ProjectAggregateZero,
    TrimAggregate,
    TrimTopK,
    ProjectAntiJoin,
}

/// Fanout belongs to the original graph. The producer kind belongs to the
/// rewritten graph, where an earlier visit may already have replaced a node.
#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionRules {
    row: ProjectionRule,
    shared_row: ProjectionRule,
    index: ProjectionRule,
}

impl ProjectionRules {
    pub fn for_producer(producer: OperatorKind) -> Self {
        use OperatorKind as Op;
        use ProjectionRule::*;
        let (row, shared_row, index) = match producer {
            Op::Map => (ComposeMap, Keep, ComposeMap),
            Op::MapIndex => (Keep, Keep, ComposeIndex),
            Op::Integrate => (PushThrough, Keep, PushThrough),
            Op::Differentiate | Op::Delay | Op::Negate | Op::Noop => {
                (PushThrough, PushThrough, PushThrough)
            }
            // A computed expression can fail on a row canceled by the sum.
            Op::Sum | Op::AtomicSum | Op::Subtract => (ProjectSum, ProjectSum, Keep),
            Op::Join | Op::LeftJoin | Op::StreamJoin | Op::StarJoin => {
                (ProjectJoin, Keep, ProjectJoin)
            }
            Op::JoinIndex | Op::LeftJoinIndex | Op::StreamJoinIndex | Op::StarJoinIndex => {
                (ProjectIndexedJoin, Keep, ProjectIndexedJoin)
            }
            Op::AntiJoin | Op::StreamAntiJoin => (ProjectAntiJoin, Keep, ProjectAntiJoin),
            Op::AsofJoin => (ProjectAsof, Keep, Keep),
            Op::JoinFilterMap => (ProjectFilteredJoin, Keep, Keep),
            Op::StarJoinFilterMap => (ProjectFilteredStar, Keep, Keep),
            Op::FlatMap => (ProjectFlatMap, Keep, Keep),
            Op::Deindex => (ProjectDeindex, Keep, Keep),
            Op::AggregateZero => (ProjectAggregateZero, Keep, Keep),
            Op::StreamAggregate => (Keep, Keep, TrimAggregate),
            Op::IndexedTopK => (Keep, Keep, TrimTopK),
            _ => (Keep, Keep, Keep),
        };
        Self {
            row,
            shared_row,
            index,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ProjectionRule::*, ProjectionRules};
    use feldera_sql_ir::OperatorKind as Op;

    #[test]
    fn shared_producers_distinguish_stateful_and_linear_rewrites() {
        for (producer, single, shared, indexed) in [
            (Op::Integrate, PushThrough, Keep, PushThrough),
            (Op::Differentiate, PushThrough, PushThrough, PushThrough),
            (Op::Subtract, ProjectSum, ProjectSum, Keep),
            (Op::MapIndex, Keep, Keep, ComposeIndex),
            (
                Op::LeftJoinIndex,
                ProjectIndexedJoin,
                Keep,
                ProjectIndexedJoin,
            ),
            (Op::AsofJoin, ProjectAsof, Keep, Keep),
        ] {
            let rules = ProjectionRules::for_producer(producer);
            assert_eq!(
                (rules.row, rules.shared_row, rules.index),
                (single, shared, indexed)
            );
        }
    }
}
