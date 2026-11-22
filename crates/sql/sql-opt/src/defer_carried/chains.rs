//! Walk left inputs of left joins. A shared port belongs to the first chain
//! that claims it. A new chain begins at the collection feeding a differentiator.
use super::{Column, Part};
use feldera_sql_ir::Error;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub enum LinkKind {
    Unary,
    Differentiate,
    Integrate,
    LeftJoin,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainOperator {
    operator: u64,
    // Only output zero of a simple operator can itself be a chain link.
    input: Option<u64>,
    kind: LinkKind,
    collection_input: bool,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChainStep {
    end: u64,
    /// An existing tail, or a new chain at the input of the first differentiator in `via`.
    previous: Option<u64>,
    /// Single-input links crossed before appending the left join, in evaluation order.
    via: Vec<u64>,
    candidate: bool,
    terminal: bool,
}

/// The result also selects which closures need read and copy facts. Evaluating
/// an unrelated closure could produce a diagnostic that the optimizer never requested.
pub fn chain_steps(nodes: &[ChainOperator]) -> Result<Option<Vec<ChainStep>>, Error> {
    let mut sources = HashMap::<u64, &ChainOperator>::new();
    let mut tails = HashMap::<u64, usize>::new();
    let mut steps = Vec::<ChainStep>::new();
    for node in nodes {
        let mut previous = node.input;
        let mut via = Vec::new();
        if node.kind == LinkKind::LeftJoin {
            let mut seed = None;
            while let Some(port) = previous {
                if let Some(&index) = tails.get(&port) {
                    if !steps[index].terminal {
                        previous = None;
                    }
                    break;
                }
                let Some(source) = sources.get(&port).copied() else {
                    previous = None;
                    break;
                };
                via.push(port);
                if via.len() > sources.len() {
                    return Err(Error::new("cycle in left-join chain inputs"));
                }
                if source.collection_input && seed.is_none() {
                    seed = Some((via.len(), source.kind));
                }
                previous = source.input;
            }
            if previous.is_none() {
                match seed {
                    None => continue,
                    Some((1, LinkKind::Differentiate)) => via.truncate(1),
                    // The live visitor reports this invalid chain at the join's SQL site.
                    Some(_) => return Ok(None),
                }
            }
            via.reverse();
        } else {
            sources.insert(node.operator, node);
            if !previous
                .and_then(|p| tails.get(&p))
                .is_some_and(|&i| steps[i].terminal)
            {
                continue;
            }
        }
        if let Some(prev) = previous {
            steps[tails[&prev]].terminal = false;
        }
        if tails.insert(node.operator, steps.len()).is_some() {
            return Err(Error::new("duplicate left-join chain endpoint"));
        }
        steps.push(ChainStep {
            end: node.operator,
            previous,
            via,
            candidate: node.kind == LinkKind::Integrate,
            terminal: true,
        });
    }
    Ok(Some(steps))
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct CarriedCopy {
    source: Column,
    result: Column,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkColumns {
    operator: u64,
    join: bool,
    /// Ordered columns of the input when this differentiator seeds a chain.
    seed: Option<Vec<Column>>,
    /// None means an opaque computed expression may read any input column.
    reads: Option<BTreeSet<Column>>,
    copies: Vec<CarriedCopy>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CarriedColumns {
    /// The differentiator immediately after the collection where the chain starts.
    seed: u64,
    end: u64,
    length: usize,
    read: BTreeSet<Column>,
    /// For each start column its first surviving copy is the representative used by the lookup.
    copies: Vec<CarriedCopy>,
}

impl CarriedColumns {
    fn start(link: &LinkColumns) -> Result<Self, Error> {
        let columns = link
            .seed
            .as_ref()
            .ok_or_else(|| Error::new("missing chain start columns"))?;
        Ok(Self {
            seed: link.operator,
            end: link.operator,
            length: 0,
            read: BTreeSet::new(),
            copies: columns
                .iter()
                .map(|&column| CarriedCopy {
                    source: column,
                    result: column,
                })
                .collect(),
        })
    }

    fn extend(&mut self, link: &LinkColumns) {
        let mut outputs = HashMap::<Column, Vec<Column>>::new();
        for copy in &link.copies {
            outputs.entry(copy.source).or_default().push(copy.result);
        }
        let mut copies = Vec::new();
        for carried in &self.copies {
            let column = carried.result;
            // A join reads all its index columns for matching. Its left-value
            // parameter sees the value tuple as a plain row.
            let read = if link.join {
                column.part == Part::Index
                    || link.reads.as_ref().is_none_or(|reads| {
                        reads.contains(&Column {
                            part: Part::None,
                            field: column.field,
                        })
                    })
            } else {
                link.reads
                    .as_ref()
                    .is_none_or(|reads| reads.contains(&column))
            };
            if read {
                self.read.insert(carried.source);
            }
            if let Some(targets) = outputs.get(&column) {
                copies.extend(targets.iter().map(|&result| CarriedCopy {
                    source: carried.source,
                    result,
                }));
            }
        }
        self.end = link.operator;
        self.length += usize::from(link.join);
        self.copies = copies;
    }
}

#[derive(Deserialize)]
pub struct ChainInputs {
    steps: Vec<ChainStep>,
    links: Vec<LinkColumns>,
    logging: bool,
}

pub fn carried_columns(input: ChainInputs) -> Result<Vec<CarriedColumns>, Error> {
    let links: HashMap<_, _> = input
        .links
        .iter()
        .map(|link| (link.operator, link))
        .collect();
    let link = |id: &u64| {
        links
            .get(id)
            .copied()
            .ok_or_else(|| Error::new("missing chain link columns"))
    };
    let mut tails = HashMap::<u64, CarriedColumns>::new();
    let mut result = Vec::new();
    for step in input.steps {
        let mut carried = match step.previous {
            Some(id) => tails
                .remove(&id)
                .ok_or_else(|| Error::new("chain tail already consumed"))?,
            None => {
                let seed = step
                    .via
                    .first()
                    .ok_or_else(|| Error::new("missing chain differentiator"))?;
                CarriedColumns::start(link(seed)?)?
            }
        };
        for id in step.via.iter().chain([&step.end]) {
            carried.extend(link(id)?);
        }
        let report = carried.length >= 2 && (step.candidate || (input.logging && step.terminal));
        if step.terminal {
            if report {
                result.push(carried);
            }
        } else {
            if report {
                result.push(carried.clone());
            }
            tails.insert(step.end, carried);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(operator: u64, input: u64, kind: LinkKind, collection_input: bool) -> ChainOperator {
        ChainOperator {
            operator,
            input: Some(input),
            kind,
            collection_input,
        }
    }

    #[test]
    fn a_second_consumer_starts_at_its_own_differentiator() -> Result<(), Error> {
        use LinkKind::*;
        let graph = [
            node(1, 0, Differentiate, true),
            node(2, 1, LeftJoin, false),
            node(3, 2, Integrate, false),
            node(4, 3, Differentiate, true),
            node(5, 4, LeftJoin, false),
            // Link 3 already belongs to the first chain.
            node(6, 3, Differentiate, true),
            node(7, 6, LeftJoin, false),
        ];
        let steps = chain_steps(&graph)?.unwrap();
        assert_eq!(steps.len(), 5);
        assert_eq!((steps[0].end, &steps[0].via), (2, &vec![1]));
        assert_eq!((steps[2].end, steps[2].previous), (4, Some(3)));
        assert_eq!((steps[3].end, steps[3].previous), (5, Some(4)));
        assert_eq!(
            (steps[4].end, steps[4].previous, &steps[4].via),
            (7, None, &vec![6])
        );
        assert!(steps[3].terminal && steps[4].terminal);
        assert!(chain_steps(&[node(1, 0, Unary, true), node(2, 1, LeftJoin, false)])?.is_none());
        Ok(())
    }

    #[test]
    fn join_reads_follow_the_original_column_across_multiple_copies() {
        let col = |part, field| Column { part, field };
        let copy = |source, result| CarriedCopy { source, result };
        let id = col(Part::None, 0);
        let payload = col(Part::None, 1);
        let index = col(Part::Index, 0);
        let value = col(Part::Value, 0);
        let alias = col(Part::Value, 1);
        let mut chain = CarriedColumns {
            seed: 1,
            end: 2,
            length: 1,
            read: BTreeSet::new(),
            copies: vec![copy(id, index), copy(payload, value), copy(payload, alias)],
        };
        chain.extend(&LinkColumns {
            operator: 3,
            join: true,
            seed: None,
            reads: Some([col(Part::None, 1)].into()),
            copies: vec![copy(alias, id), copy(value, payload)],
        });
        assert_eq!(chain.read, [id, payload].into());
        assert_eq!(chain.copies, [copy(payload, payload), copy(payload, id)]);
        assert_eq!(chain.length, 2);
    }
}
