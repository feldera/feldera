//! Binary join expansions used to analyze monotonicity on the delta circuit.

use feldera_sql_expr::{ensure_expression_tree, SimplifyWarning};
use feldera_sql_ir::{
    CircuitEditor, Error, Field, NodeId, OperatorKind as Op, OutputPort, TypeId, TypeKind,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Term {
    Left,
    Right,
    LeftIntegral,
    RightIntegral,
    LeftDelta,
    RightDelta,
    Both,
    AntiJoin,
    Unmatched,
    LeftFilter,
    RightFilter,
    BothFilter,
    UnmatchedFilter,
    Sum,
}

struct Step {
    output: Term,
    inputs: &'static [Term],
}

impl Term {
    fn from(self, inputs: &'static [Self]) -> Step {
        Step {
            output: self,
            inputs,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JoinForm {
    Inner,
    FilteredInner,
    LeftOuter,
    FilteredLeftOuter,
}

impl JoinForm {
    /// The derivative has three products. A left join also contributes a
    /// null-padded row from the incremental antijoin. Preserve the source
    /// visitor's construction and summation order for each form.
    fn steps(self) -> Vec<Step> {
        use Term::*;
        if matches!(self, Self::FilteredInner) {
            return vec![
                RightIntegral.from(&[Right]),
                LeftDelta.from(&[Left, RightIntegral]),
                LeftFilter.from(&[LeftDelta]),
                Both.from(&[Left, Right]),
                BothFilter.from(&[Both]),
                LeftIntegral.from(&[Left]),
                RightDelta.from(&[LeftIntegral, Right]),
                RightFilter.from(&[RightDelta]),
                Sum.from(&[LeftFilter, BothFilter, RightFilter]),
            ];
        }
        let mut steps = vec![
            LeftIntegral.from(&[Left]),
            RightDelta.from(&[LeftIntegral, Right]),
            RightIntegral.from(&[Right]),
            LeftDelta.from(&[Left, RightIntegral]),
            Both.from(&[Left, Right]),
        ];
        if matches!(self, Self::Inner) {
            steps.push(Sum.from(&[RightDelta, LeftDelta, Both]));
            return steps;
        }
        steps.extend([AntiJoin.from(&[Left, Right]), Unmatched.from(&[AntiJoin])]);
        if matches!(self, Self::FilteredLeftOuter) {
            steps.extend([
                LeftFilter.from(&[LeftDelta]),
                RightFilter.from(&[RightDelta]),
                BothFilter.from(&[Both]),
                UnmatchedFilter.from(&[Unmatched]),
                Sum.from(&[RightFilter, LeftFilter, BothFilter, UnmatchedFilter]),
            ]);
        } else {
            steps.push(Sum.from(&[RightDelta, LeftDelta, Both, Unmatched]));
        }
        steps
    }
}

/// A binary join on the expanded graph retains one operator per derivative
/// term. Limiter insertion looks them up again on the original join.
pub struct DeltaJoin {
    pub form: JoinForm,
    pub function: NodeId,
    pub filter: Option<NodeId>,
    pub output: TypeId,
    pub balanced: bool,
}

impl DeltaJoin {
    pub fn expand(
        &self,
        edit: &mut CircuitEditor,
        body: NodeId,
        inputs: [(OutputPort, TypeId); 2],
        original_left: TypeId,
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<BTreeMap<Term, NodeId>, Error> {
        use Term::*;
        let [left, right] = inputs;
        let indexed = edit.type_kind(self.output)? == TypeKind::IndexedZSet;
        let mut streams = BTreeMap::from([(Left, left), (Right, right)]);
        let mut operators = BTreeMap::new();
        for step in self.form.steps() {
            let ports: Vec<_> = step.inputs.iter().map(|term| streams[term].0).collect();
            let source_type = streams[&step.inputs[0]].1;
            let (kind, ty, function) = match step.output {
                LeftIntegral | RightIntegral => (Op::DelayedIntegral, source_type, None),
                LeftDelta | RightDelta | Both => {
                    let kind = if indexed {
                        Op::StreamJoinIndex
                    } else {
                        Op::StreamJoin
                    };
                    (kind, self.output, Some(self.function))
                }
                AntiJoin => (Op::AntiJoin, left.1, None),
                Unmatched => {
                    let pad = crate::unmatched_row_projection(
                        edit,
                        self.function,
                        original_left,
                        next,
                        Some(warnings),
                    )?;
                    let pad = ensure_expression_tree(edit, pad.into())?;
                    if indexed {
                        (Op::MapIndex, self.output, Some(pad))
                    } else {
                        let row = edit.expression(edit.node(pad)?.edge("body")?)?;
                        let ty = edit.zset_type(edit.expression_type(row)?)?;
                        (Op::Map, ty, Some(pad))
                    }
                }
                LeftFilter | RightFilter | BothFilter | UnmatchedFilter => {
                    let predicate = self
                        .filter
                        .ok_or_else(|| Error::new("filtered join requires a predicate"))?;
                    (Op::Filter, source_type, Some(predicate))
                }
                Sum => (Op::Sum, source_type, None),
                Left | Right => unreachable!("input terms precede the derivative"),
            };
            let mut fields = vec![("outputType", Field::Reference(ty.into()))];
            if let Some(function) = function {
                fields.push(("function", Field::Reference(function)));
            }
            if matches!(kind, Op::StreamJoin | Op::StreamJoinIndex) {
                fields.push(("balanced", Field::Data(self.balanced.into())));
            }
            let id = edit.append_operator(body, body, kind, &ports, fields)?;
            streams.insert(
                step.output,
                (
                    OutputPort {
                        operator: id,
                        number: 0,
                    },
                    ty,
                ),
            );
            operators.insert(step.output, id);
        }
        Ok(operators)
    }
}
