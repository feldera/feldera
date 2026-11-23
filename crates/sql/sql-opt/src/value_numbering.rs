//! Intern expression values in evaluation order. A key contains the original
//! printer's text between operands and the value number of each operand.
mod key;

use self::key::{Form, Key, Number};
use crate::expression_uses::{shared_expression_uses, NumberedExpression, OperandUse};
use feldera_sql_ir::{Circuit, Error, ExpressionKind, InnerKind, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write;

/// Find the variable whose stored value can change through a mutable borrow.
/// A borrow of a computed value has no known declaration and disables CSE for
/// the entire expression. A field access or dereference preserves the base.
pub fn borrowed_variables(circuit: &Circuit, places: &[NodeId]) -> Result<Vec<Option<u64>>, Error> {
    places
        .iter()
        .map(|&place| {
            let mut current = place;
            let mut seen = HashSet::new();
            loop {
                if !seen.insert(current) {
                    return Err(Error::new("cycle in a mutably borrowed expression"));
                }
                let node = circuit
                    .node(current)
                    .ok_or_else(|| Error::new("missing mutably borrowed expression"))?;
                match node.inner_kind() {
                    Some(InnerKind::Expression(ExpressionKind::Field | ExpressionKind::Deref)) => {
                        current = node.edge("expression")?;
                    }
                    Some(InnerKind::Expression(ExpressionKind::VariablePath)) => {
                        let NodeId::Inner(id) = current else {
                            unreachable!("variable has an inner identity")
                        };
                        return Ok(Some(id));
                    }
                    Some(InnerKind::Expression(_)) => return Ok(None),
                    _ => return Err(Error::new("mutable borrow requires an expression")),
                }
            }
        })
        .collect()
}

#[derive(Deserialize)]
pub struct NumberingInput {
    events: Vec<Event>,
    // The rewriter consumes only groups shared in at least one closure.
    rewrite: bool,
    sites: Vec<UseSite>,
    #[serde(default)]
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
        operands: Vec<u64>,
        #[serde(flatten)]
        key: Definition,
    },
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Definition {
    Typed {
        form: Form,
    },
    // Recorded requests made before the compiler supplied typed expressions.
    Fragments {
        fragments: Vec<String>,
        declarations: Vec<u64>,
        wrapper: bool,
        expensive: bool,
    },
}

impl Definition {
    fn key(self, numbers: &[Number]) -> Result<Key, Error> {
        match self {
            Self::Typed { form } => form.key(numbers),
            Self::Fragments {
                fragments,
                declarations,
                wrapper,
                expensive,
            } => {
                let mut parts = fragments.into_iter();
                let mut text = parts
                    .next()
                    .ok_or_else(|| Error::new("missing expression key"))?;
                for (number, suffix) in numbers.iter().zip(parts) {
                    write!(text, "{number}{suffix}").expect("writing to a string");
                }
                Ok(Key {
                    text,
                    declarations: declarations.into_iter().collect(),
                    expensive,
                    wrapper,
                })
            }
        }
    }
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

/// The live collector identifies variable reads and mutable places by occurrence.
/// Resolve them in the original closure before discarding groups that depend on a write.
#[derive(Deserialize)]
pub struct LiveNumbering {
    #[serde(flatten)]
    numbers: NumberingInput,
    mutable_reads: Vec<u64>,
    mutable_places: Vec<u64>,
}

#[derive(Serialize)]
pub struct ResolvedNumbers {
    groups: Vec<ValueGroup>,
    mutable: BTreeSet<u64>,
    unknown_write: bool,
}

impl LiveNumbering {
    pub fn number(
        mut self,
        circuit: &Circuit,
        references: &HashMap<NodeId, NodeId>,
    ) -> Result<ResolvedNumbers, Error> {
        let declaration = |read| match references.get(&NodeId::Inner(read)) {
            Some(&NodeId::Inner(id)) => Ok(id),
            _ => Err(Error::new(format!("missing declaration for CSE variable {read}"))),
        };
        for event in &mut self.numbers.events {
            if let Event::Define { expression, key: Definition::Typed { form: form @ Form::Read }, .. } = event {
                *form = Form::Variable { declaration: declaration(*expression)? };
            }
        }
        let places: Vec<_> = self.mutable_places.into_iter().map(NodeId::Inner).collect();
        let borrows = borrowed_variables(circuit, &places)?;
        let unknown_write = borrows.iter().any(Option::is_none);
        let mutable = self.mutable_reads.into_iter().chain(borrows.into_iter().flatten())
            .map(declaration).collect::<Result<BTreeSet<_>, _>>()?;
        self.numbers.mutable.extend(&mutable);
        Ok(ResolvedNumbers { groups: self.numbers.number()?, mutable, unknown_write })
    }
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
                    key,
                    operands,
                } => {
                    if matches!(&key, Definition::Fragments { fragments, .. }
                        if fragments.len().checked_sub(1) != Some(operands.len()))
                    {
                        return Err(Error::new(
                            "expression key does not match its operand count",
                        ));
                    }
                    let mut numbers = Vec::with_capacity(operands.len());
                    let mut dependencies = BTreeSet::new();
                    let mut expensive = false;
                    for operand in operands {
                        let Some(&index) = by_expression.get(&operand) else {
                            // A parent cannot be shared when any operand has no number.
                            continue 'events;
                        };
                        let child = &groups[index];
                        numbers.push(Number(child.representative));
                        dependencies.extend(&child.declarations);
                        expensive |= child.expensive;
                    }
                    let key = key.key(&numbers)?;
                    let index = *by_key.entry(key.text).or_insert_with(|| {
                        dependencies.extend(key.declarations);
                        let index = groups.len();
                        groups.push(ValueGroup {
                            representative: expression,
                            wrapper: key.wrapper,
                            shared: BTreeSet::new(),
                            expensive: expensive || key.expensive,
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
            operands: operands.to_vec(),
            key: Definition::Fragments {
                fragments: fragments.iter().map(|s| (*s).to_owned()).collect(),
                declarations: dependencies.to_vec(),
                expensive,
                wrapper: false,
            },
        }
    }

    #[test]
    fn typed_and_recorded_keys_share_the_first_representative() -> Result<(), Error> {
        use serde_json::json;

        let input: NumberingInput = serde_json::from_value(json!({
            "rewrite": false, "mutable": [], "sites": [],
            "events": [
                {"expression": 1, "operands": [], "form": {"kind": "Variable", "declaration": 7}},
                {"expression": 2, "operands": [], "form": {"kind": "Literal", "type": "s", "value": "🦀#1#"}},
                {"expression": 3, "operands": [1, 2], "form": {"kind": "Binary", "type": "b", "opcode": "OR", "symbol": "||"}},
                {"expression": 4, "operands": [1, 2], "form": {"kind": "Binary", "type": "b", "opcode": "CONCAT", "symbol": "||"}},
                {"expression": 5, "operands": [1, 2], "fragments": ["(b)", "||", ""], "declarations": [], "expensive": true, "wrapper": false},
                {"expression": 6, "operands": [4, 2, 1], "form": {"kind": "Call", "type": "s", "method": true}},
                {"expression": 7, "operands": [5, 2, 1], "fragments": ["", ".", "(", ",)s"], "declarations": [], "expensive": true, "wrapper": false}
            ]
        })).unwrap();
        let groups = input.number()?;
        assert_eq!(groups.len(), 4);
        assert_eq!(groups[2].occurrences, [3, 4, 5]);
        assert!(!groups[2].expensive);
        assert_eq!(groups[3].occurrences, [6, 7]);
        assert!(groups[3].expensive);
        assert_eq!(groups[3].declarations, BTreeSet::from([7]));
        Ok(())
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
