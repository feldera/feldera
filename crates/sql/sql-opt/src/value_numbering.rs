//! Intern expression values in evaluation order. A key contains the original
//! printer's text between operands and the value number of each operand.
use crate::expression_uses::{NumberedExpression, OperandUse, shared_expression_uses};
use feldera_sql_ir::Error;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write;

#[derive(Deserialize)]
pub struct NumberingInput {
    events: Vec<Event>,
    // The rewriter consumes only groups shared in at least one closure.
    rewrite: bool,
    sites: Vec<UseSite>,
    mutable: HashSet<u64>,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
struct UseSite {
    parent: u64,
    operand: usize,
    closure: Option<u64>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Event {
    Use {
        input: u64,
        site: usize,
    },
    Define {
        expression: u64,
        fragments: Vec<String>,
        operands: Vec<u64>,
        declarations: Vec<u64>,
        wrapper: bool,
        expensive: bool,
    },
}

#[derive(Serialize)]
pub struct ValueGroup {
    representative: u64,
    expensive: bool,
    declarations: BTreeSet<u64>,
    occurrences: Vec<u64>,
    #[serde(skip)]
    wrapper: bool,
    shared: BTreeSet<u64>,
    users: Vec<usize>,
}

impl NumberingInput {
    pub fn number(self) -> Result<Vec<ValueGroup>, Error> {
        let mut by_expression = HashMap::<u64, usize>::new();
        let mut by_key = HashMap::new();
        let mut groups = Vec::<ValueGroup>::new();
        'events: for event in self.events {
            match event {
                Event::Use { input, site } => {
                    if let Some(&index) = by_expression.get(&input) {
                        groups[index].users.push(site);
                    }
                }
                Event::Define {
                    expression,
                    fragments,
                    operands,
                    declarations,
                    mut expensive,
                    wrapper,
                } => {
                    let (first, rest) = fragments
                        .split_first()
                        .ok_or_else(|| Error::new("missing expression key"))?;
                    if rest.len() != operands.len() {
                        return Err(Error::new(
                            "expression key does not match its operand count",
                        ));
                    }
                    let mut key = first.clone();
                    let mut dependencies: BTreeSet<_> = declarations.into_iter().collect();
                    for (operand, suffix) in operands.iter().zip(rest) {
                        let Some(&index) = by_expression.get(operand) else {
                            // The parent cannot be shared when any operand has no number.
                            continue 'events;
                        };
                        let child = &groups[index];
                        write!(key, "#{}#{}", child.representative, suffix)
                            .expect("writing to a string");
                        dependencies.extend(&child.declarations);
                        expensive |= child.expensive;
                    }
                    let index = *by_key.entry(key).or_insert_with(|| {
                        let index = groups.len();
                        groups.push(ValueGroup {
                            representative: expression,
                            wrapper,
                            shared: BTreeSet::new(),
                            expensive,
                            declarations: dependencies,
                            occurrences: Vec::new(),
                            users: Vec::new(),
                        });
                        index
                    });
                    by_expression.insert(expression, index);
                }
            }
        }
        // A later mutable borrow invalidates earlier reads of the same declaration.
        for (expression, index) in by_expression {
            groups[index].occurrences.push(expression);
        }
        groups.retain(|g| {
            !g.occurrences.is_empty() && g.declarations.iter().all(|d| !self.mutable.contains(d))
        });
        for group in &mut groups {
            group.occurrences.sort_unstable();
        }
        if self.rewrite {
            shared_groups(&mut groups, &self.sites)?;
        }
        Ok(groups)
    }
}

/// Canonicalize parent identities only after removing mutable reads. A use
/// whose parent lost its number retains the original parent and operand slot.
fn shared_groups(groups: &mut Vec<ValueGroup>, sites: &[UseSite]) -> Result<(), Error> {
    let canonical: HashMap<_, _> = groups
        .iter()
        .flat_map(|g| {
            g.occurrences
                .iter()
                .map(move |&id| (id, (g.representative, g.wrapper)))
        })
        .collect();
    let mut graph = Vec::new();
    for group in groups.iter() {
        if !group.wrapper && (!group.expensive || group.users.len() < 2) {
            continue;
        }
        let mut distinct = HashSet::new();
        for &index in &group.users {
            distinct.insert(
                *sites
                    .get(index)
                    .ok_or_else(|| Error::new("missing expression use site"))?,
            );
        }
        let candidate = !group.wrapper && distinct.len() >= 2;
        let users = distinct
            .into_iter()
            .filter_map(|site| {
                let (parent, forwards) = canonical
                    .get(&site.parent)
                    .copied()
                    .unwrap_or((site.parent, false));
                Some(OperandUse {
                    closure: site.closure?,
                    operand: site.operand,
                    parent,
                    forwards,
                })
            })
            .collect();
        graph.push(NumberedExpression {
            value: group.representative,
            candidate,
            users,
        });
    }
    let mut shared = HashMap::<u64, BTreeSet<u64>>::new();
    for (closure, value) in shared_expression_uses(&graph) {
        shared.entry(value).or_default().insert(closure);
    }
    groups.retain_mut(|group| {
        let Some(closures) = shared.remove(&group.representative) else {
            return false;
        };
        group.shared = closures;
        group.users.clear();
        true
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn define(
        id: u64,
        fragments: &[&str],
        operands: &[u64],
        dependencies: &[u64],
        expensive: bool,
    ) -> Event {
        Event::Define {
            expression: id,
            fragments: fragments.iter().map(|s| (*s).to_owned()).collect(),
            operands: operands.to_vec(),
            declarations: dependencies.to_vec(),
            expensive,
            wrapper: false,
        }
    }

    #[test]
    fn first_representatives_carry_dependencies_cost_and_available_uses() -> Result<(), Error> {
        let input = NumberingInput {
            rewrite: false,
            sites: Vec::new(),
            mutable: HashSet::from([90]),
            events: vec![
                Event::Use { input: 1, site: 0 },
                define(1, &["constant 🦀#5#"], &[], &[], true),
                define(2, &["constant 🦀#5#"], &[], &[], true),
                Event::Use { input: 2, site: 1 },
                define(3, &["(", "+", ")"], &[1, 2], &[], false),
                define(4, &["(", "+", ")"], &[2, 1], &[], false),
                define(5, &["unknown(", ")"], &[99], &[], true),
                define(6, &["var"], &[], &[90], false),
                define(7, &["(", "+", ")"], &[1, 6], &[], false),
            ],
        };
        let groups = input.number()?;
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].representative, 1);
        assert_eq!(groups[0].occurrences, [1, 2]);
        assert_eq!(groups[0].users, [1]);
        assert_eq!(groups[1].occurrences, [3, 4]);
        assert!(groups[1].expensive);
        Ok(())
    }
}
