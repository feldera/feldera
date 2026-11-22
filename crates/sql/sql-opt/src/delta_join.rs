//! Binary join expansions used to analyze monotonicity on the delta circuit.

use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
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

#[derive(Serialize)]
pub struct Step {
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
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
    pub fn steps(self) -> Vec<Step> {
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
