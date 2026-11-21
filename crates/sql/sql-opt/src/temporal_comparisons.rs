//! Classify the conjuncts that can become window bounds. Once a residual
//! predicate interrupts the prefix, keep each remaining right subtree intact.
use feldera_sql_ir::Error;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct ClockOperand {
    clock: bool,
    monotone: bool,
}

/// Conjunction children index earlier entries in left-to-right postorder.
#[derive(Deserialize)]
#[serde(untagged)]
pub enum TemporalPredicate {
    Conjunction {
        children: [usize; 2],
    },
    Binary {
        opcode: String,
        left: ClockOperand,
        right: ClockOperand,
    },
    Other {
        clock: bool,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
pub enum TemporalPredicateKind {
    NoNow,
    Residual,
    #[serde(rename = "LT")]
    Less,
    #[serde(rename = "LTE")]
    LessEqual,
    #[serde(rename = "GT")]
    Greater,
    #[serde(rename = "GTE")]
    GreaterEqual,
}

impl TemporalPredicateKind {
    fn opaque(clock: bool) -> Self {
        if clock {
            Self::Residual
        } else {
            Self::NoNow
        }
    }

    fn binary(opcode: &str, left: &ClockOperand, right: &ClockOperand) -> Self {
        use TemporalPredicateKind::*;
        if !left.clock && !right.clock {
            return NoNow;
        }
        if left.clock == right.clock || !(if left.clock { left } else { right }).monotone {
            return Residual;
        }
        match (opcode, left.clock) {
            ("LT", false) | ("GT", true) => Less,
            ("LTE", false) | ("GTE", true) => LessEqual,
            ("GT", false) | ("LT", true) => Greater,
            ("GTE", false) | ("LTE", true) => GreaterEqual,
            _ => Residual,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct TemporalPredicatePart {
    index: usize,
    kind: TemporalPredicateKind,
}

/// A group with several parts is a left-associated residual conjunction.
/// Its individual parts retain their SQL nullability in the calling compiler.
pub fn classify_temporal_predicates(
    nodes: &[TemporalPredicate],
) -> Result<Vec<Vec<TemporalPredicatePart>>, Error> {
    let mut roots = Vec::new();
    let mut clocks = Vec::with_capacity(nodes.len());
    for (index, node) in nodes.iter().enumerate() {
        let clock = match node {
            TemporalPredicate::Conjunction {
                children: [left, right],
            } => {
                if roots.pop() != Some(*right) || roots.pop() != Some(*left) {
                    return Err(Error::new("invalid temporal conjunction children"));
                }
                clocks[*left] || clocks[*right]
            }
            TemporalPredicate::Binary { left, right, .. } => left.clock || right.clock,
            TemporalPredicate::Other { clock } => *clock,
        };
        clocks.push(clock);
        roots.push(index);
    }
    if roots.len() != 1 {
        return Err(Error::new("expected one temporal predicate tree"));
    }

    let mut groups: Vec<Vec<TemporalPredicatePart>> = Vec::new();
    let mut scanning_bounds = true;
    while let Some(index) = roots.pop() {
        if !scanning_bounds {
            let kind = TemporalPredicateKind::opaque(clocks[index]);
            let group = groups.last_mut().expect("a residual ended the prefix");
            let combine = group[0].kind == TemporalPredicateKind::Residual
                || kind == TemporalPredicateKind::NoNow;
            let part = TemporalPredicatePart { index, kind };
            if combine {
                group.push(part);
            } else {
                groups.push(vec![part]);
            }
            continue;
        }
        let kind = match &nodes[index] {
            TemporalPredicate::Conjunction {
                children: [left, right],
            } => {
                roots.extend([*right, *left]);
                continue;
            }
            TemporalPredicate::Binary {
                opcode,
                left,
                right,
            } => {
                let kind = TemporalPredicateKind::binary(opcode, left, right);
                scanning_bounds = kind != TemporalPredicateKind::Residual;
                kind
            }
            TemporalPredicate::Other { clock } => {
                // A non-binary expression ends the prefix even when it is clock-free.
                scanning_bounds = false;
                TemporalPredicateKind::opaque(*clock)
            }
        };
        groups.push(vec![TemporalPredicatePart { index, kind }]);
    }
    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keeps_the_unvisited_right_subtree_after_a_residual() {
        let predicates = serde_json::from_value::<Vec<TemporalPredicate>>(json!([
            {"opcode":"LT","left":{"clock":true,"monotone":true},"right":{"clock":false,"monotone":false}},
            {"clock":false}, {"children":[0,1]},
            {"clock":false}, {"clock":true}, {"children":[3,4]}, {"children":[2,5]},
            {"clock":false}, {"children":[6,7]}
        ])).unwrap();
        use TemporalPredicateKind::*;
        let part = |index, kind| TemporalPredicatePart { index, kind };
        assert_eq!(
            classify_temporal_predicates(&predicates).unwrap(),
            [
                vec![part(0, Greater)],
                vec![part(1, NoNow)],
                vec![part(5, Residual), part(7, NoNow)]
            ]
        );
    }

    #[test]
    fn bounds_require_exactly_one_monotone_clock_operand() {
        use TemporalPredicateKind::*;
        let ordinary = ClockOperand {
            clock: false,
            monotone: true,
        };
        let clock = ClockOperand {
            clock: true,
            monotone: true,
        };
        let varying = ClockOperand {
            clock: true,
            monotone: false,
        };
        assert_eq!(
            TemporalPredicateKind::binary("GTE", &clock, &ordinary),
            LessEqual
        );
        assert_eq!(
            TemporalPredicateKind::binary("GTE", &ordinary, &clock),
            GreaterEqual
        );
        assert_eq!(
            TemporalPredicateKind::binary("GT", &ordinary, &varying),
            Residual
        );
        assert_eq!(
            TemporalPredicateKind::binary("GT", &clock, &clock),
            Residual
        );
        assert_eq!(
            TemporalPredicateKind::binary("EQ", &clock, &ordinary),
            Residual
        );
        assert_eq!(
            TemporalPredicateKind::binary("EQ", &ordinary, &ordinary),
            NoNow
        );
    }
}
