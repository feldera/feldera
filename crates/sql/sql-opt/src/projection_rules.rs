//! Candidate rewrites for a Map or MapIndex following a live producer.
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind, OutputPort, TypeId, TypeKind,
};
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

/// Move a reader before a linear operator. All copies share the original
/// closure and declared output type, including when two ports name the same input.
pub struct ProjectionPushdown {
    pub producer: OperatorKind,
    pub initial: Option<ExpressionId>,
    pub reader: ExpressionId,
    pub output: TypeId,
}

impl ProjectionPushdown {
    pub fn append(
        self,
        edit: &mut CircuitEditor,
        body: NodeId,
        inputs: &[OutputPort],
        next: &mut u64,
    ) -> Result<(), Error> {
        use OperatorKind as Op;
        let map = match edit.type_kind(self.output)? {
            TypeKind::IndexedZSet => Op::MapIndex,
            TypeKind::ZSet => Op::Map,
            _ => return Err(Error::new("pushed projection requires a collection output")),
        };
        let rules = ProjectionRules::for_producer(self.producer);
        let rule = if map == Op::MapIndex {
            rules.index
        } else {
            rules.row
        };
        if !matches!(
            rule,
            ProjectionRule::PushThrough | ProjectionRule::ProjectSum
        ) {
            return Err(Error::new("operator does not admit a pushed projection"));
        }
        let output = Field::Reference(self.output.into());
        let mut projected = Vec::with_capacity(inputs.len());
        for &input in inputs {
            let operator = edit.append_operator(
                body,
                body,
                map,
                &[input],
                [
                    ("function", Field::Reference(self.reader.into())),
                    ("outputType", output.clone()),
                ],
            )?;
            projected.push(OutputPort {
                operator,
                number: 0,
            });
        }
        let function = if self.producer == Op::Noop {
            Some(crate::rebuild::noop_function(edit, self.output, next)?)
        } else {
            self.initial
        };
        let fields = std::iter::once(("outputType", output))
            .chain(function.map(|f| ("function", Field::Reference(f.into()))));
        edit.append_operator(body, body, self.producer, &projected, fields)?;
        Ok(())
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
