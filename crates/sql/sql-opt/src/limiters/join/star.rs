//! Each delta of a star join traverses a tree of binary joins and a row projection.
//! The minimum over the trees bounds the sum before the optional filter and map.
use super::{Bound, InsertionSite, JoinTerm, Projection, UnaryTerm, projected_minimum};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{CircuitEditor, Error, NodeId};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct Integral {
    source: u64,
    id: u64,
    monotone: bool,
}

#[derive(Deserialize)]
struct Binary {
    inputs: [u64; 2],
    #[serde(flatten)]
    term: JoinTerm,
}

#[derive(Deserialize)]
struct Tree {
    joins: Vec<Binary>,
    result: UnaryTerm,
}

#[derive(Deserialize)]
struct Sum {
    id: u64,
    inputs: Vec<u64>,
    output: Option<Projection>,
}

#[derive(Deserialize)]
pub(in crate::limiters) struct StarJoinExpansion {
    integrals: Vec<Integral>,
    trees: Vec<Tree>,
    sum: Sum,
    filter: Option<UnaryTerm>,
    map: Option<UnaryTerm>,
}

impl StarJoinExpansion {
    pub(in crate::limiters) fn has_original_bound(&self) -> bool {
        // A filtered star without a map marks only the filter in the expansion.
        self.filter.is_none() || self.map.is_some()
    }

    pub(in crate::limiters) fn insert<'a>(
        &'a self,
        edit: &mut CircuitEditor,
        site: InsertionSite,
        functions: &BTreeMap<u64, NodeId>,
        inputs: impl ExactSizeIterator<Item = Option<(Bound, &'a Projection)>>,
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<Option<(Bound, &'a Projection)>, Error> {
        if inputs.len() != self.integrals.len() || inputs.len() != self.trees.len() {
            return Err(Error::new(
                "star join requires one integral and one delta tree per input",
            ));
        }
        let mut bounds = BTreeMap::new();
        for (input, integral) in inputs.zip(&self.integrals) {
            if let Some(input) = input {
                bounds.insert(integral.source, input);
                if integral.monotone {
                    bounds.insert(integral.id, input);
                }
            }
        }
        for tree in &self.trees {
            for binary in &tree.joins {
                let sides = binary
                    .inputs
                    .map(|id| bounds.get(&id).map(|(bound, _)| *bound));
                if let Some(result) = binary
                    .term
                    .insert(edit, site, functions, sides, next, warnings)?
                {
                    bounds.insert(binary.term.id, result);
                }
            }
            tree.result
                .apply(edit, site, functions, &mut bounds, next, warnings)?;
        }
        if let Some(output) = &self.sum.output {
            let terms = self
                .sum
                .inputs
                .iter()
                .map(|id| bounds.get(id).copied())
                .collect::<Option<Vec<_>>>();
            if let Some(terms) = terms {
                let result = projected_minimum(edit, site, terms, output, next)?;
                bounds.insert(self.sum.id, (result, output));
            }
        }
        let mut last = self.sum.id;
        for unary in self.filter.iter().chain(&self.map) {
            unary.apply(edit, site, functions, &mut bounds, next, warnings)?;
            last = unary.id;
        }
        Ok(bounds.get(&last).copied())
    }
}
