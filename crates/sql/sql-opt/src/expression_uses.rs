//! Find expensive expressions used more than once in the same closure.
//! Value numbering supplies the canonical users. Borrows, clones, dereferences
//! and tuples forward their users because the rewriter never shares a wrapper.
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Deserialize)]
pub struct NumberedExpression {
    pub(crate) value: u64,
    pub(crate) candidate: bool,
    pub(crate) users: Vec<OperandUse>,
}

#[derive(Deserialize)]
pub(crate) struct OperandUse {
    pub(crate) closure: u64,
    pub(crate) parent: u64,
    pub(crate) operand: usize,
    pub(crate) forwards: bool,
}

type ScopedValue = (u64, u64); // (closure, canonical expression)

/// Two different canonical (parent, operand) pairs imply two uses. For one
/// pair, there are two uses only if its unshared parent itself has two uses.
/// Propagate this fact backwards without enumerating paths through nested tuples.
pub fn shared_expression_uses(expressions: &[NumberedExpression]) -> BTreeSet<(u64, u64)> {
    let mut first_operand = HashMap::new();
    let mut dependencies = HashMap::<ScopedValue, Vec<ScopedValue>>::new();
    let mut multiple = HashSet::new();
    let mut candidates = HashSet::new();

    for expression in expressions {
        for site in &expression.users {
            let child = (site.closure, expression.value);
            let operand = (site.parent, site.operand);
            if expression.candidate {
                candidates.insert(child);
            }
            if *first_operand.entry(child).or_insert(operand) != operand {
                multiple.insert(child);
            }
            if site.forwards {
                dependencies
                    .entry((site.closure, site.parent))
                    .or_default()
                    .push(child);
            }
        }
    }

    let mut pending: Vec<_> = multiple.iter().copied().collect();
    while let Some(parent) = pending.pop() {
        if let Some(children) = dependencies.remove(&parent) {
            for child in children {
                if multiple.insert(child) {
                    pending.push(child);
                }
            }
        }
    }
    candidates.intersection(&multiple).copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn use_at(closure: u64, parent: u64, operand: usize, forwards: bool) -> OperandUse {
        OperandUse {
            closure,
            parent,
            operand,
            forwards,
        }
    }

    #[test]
    fn collapsed_users_count_separately_only_through_unshared_wrappers() {
        let exprs = [
            NumberedExpression {
                value: 1,
                candidate: true,
                users: vec![
                    use_at(10, 2, 0, true),
                    use_at(10, 2, 0, true),
                    use_at(11, 2, 0, true),
                ],
            },
            NumberedExpression {
                value: 2,
                candidate: false,
                users: vec![use_at(10, 3, 0, true), use_at(11, 3, 0, true)],
            },
            NumberedExpression {
                value: 3,
                candidate: false,
                users: vec![
                    use_at(10, 4, 0, false),
                    use_at(10, 4, 1, false),
                    use_at(11, 4, 0, false),
                ],
            },
            NumberedExpression {
                value: 5,
                candidate: true,
                users: vec![use_at(10, 3, 0, false), use_at(10, 3, 0, false)],
            },
        ];
        assert_eq!(shared_expression_uses(&exprs), BTreeSet::from([(10, 1)]));
    }
}
