//! Select indexes whose sole reader can share the integral of a common input.
use crate::consumer_graph::consumer_graphs;
use crate::index_key_equivalence::{KeyComparisonError, check_closures, equivalent};
use crate::lexical_scope::{Resolution, lexical_references};
use crate::projection::{ProjectionMode, is_map_projection, simple_tuple};
use crate::shared_indexes::shares_input_integral;
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind as Expr, NodeId, OperatorKind as Op,
};
use feldera_sql_ir::{ExpressionId, OutputPort};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Copy, Debug, serde::Deserialize, Serialize)]
pub struct IndexReader {
    pub index: u64,
    pub consumer: u64,
    pub slot: usize,
}

fn number(id: NodeId) -> u64 {
    let NodeId::Outer(id) = id else {
        unreachable!("circuit operators have outer ids")
    };
    id
}

/// Keep eligible singletons, because the visitor records them as visited even
/// when another reader of their input cannot use a shared index.
fn readers(circuit: &Circuit) -> Result<HashMap<NodeId, IndexReader>, Error> {
    let mut readers = HashMap::new();
    for (body, graph) in consumer_graphs(circuit)? {
        let members = circuit
            .node(NodeId::Outer(body))
            .expect("validated body")
            .edges("allOperators")?;
        for siblings in graph.iter().filter(|siblings| siblings.len() > 1) {
            for &(index, _) in siblings {
                let id = members[index];
                if circuit.node(id).and_then(|n| n.operator_kind()) != Some(Op::MapIndex) {
                    continue;
                }
                let &[(reader, slot)] = graph[index].as_slice() else {
                    continue;
                };
                let consumer = members[reader];
                if shares_input_integral(circuit.node(consumer).expect("validated reader"), slot)? {
                    readers.insert(
                        id,
                        IndexReader {
                            index: number(id),
                            consumer: number(consumer),
                            slot,
                        },
                    );
                }
            }
        }
    }
    Ok(readers)
}

pub fn shared_index_payloads(circuit: &Circuit) -> Result<BTreeSet<u64>, Error> {
    Ok(readers(circuit)?
        .values()
        .map(|reader| reader.index)
        .collect())
}

#[derive(Debug, thiserror::Error)]
pub enum IndexDiscoveryError {
    #[error("index closure has an unresolved variable {0:?}")]
    FreeVariable(NodeId),
    #[error("index closure variable {variable:?} resolves to both {previous:?} and {current:?}")]
    ConflictingDeclarations {
        variable: NodeId,
        previous: NodeId,
        current: NodeId,
    },
    #[error(transparent)]
    Comparison(#[from] KeyComparisonError),
    #[error(transparent)]
    InvalidIr(#[from] Error),
}

fn decomposable(edit: &CircuitEditor, function: NodeId) -> Result<bool, IndexDiscoveryError> {
    // Projection resolves the whole closure before it inspects the value tuple.
    match lexical_references(edit.circuit(), function)? {
        Resolution::Bound(_) => {}
        Resolution::Free { variable, .. } => {
            return Err(IndexDiscoveryError::FreeVariable(variable));
        }
        Resolution::Conflict {
            variable,
            previous,
            current,
        } => {
            return Err(IndexDiscoveryError::ConflictingDeclarations {
                variable,
                previous,
                current,
            });
        }
    }
    let body = edit.expression(edit.node(function)?.edge("body")?)?;
    if edit.expr_kind(body)? == Expr::RawTuple {
        let tuple = edit.node(body.into())?;
        if tuple.field("fields").is_some() {
            if let [_, value] = tuple.edges("fields")?.as_slice() {
                if edit.expr_kind(edit.expression(*value)?)? == Expr::Tuple {
                    return Ok(true);
                }
            }
        }
    }
    Ok(simple_tuple(edit, edit.expression_type(body)?.into())?
        && is_map_projection(edit, function, ProjectionMode::NestedWithCasts)?)
}

/// Keep siblings in visitor order. A bad closure must first be inspected at
/// its seed or sibling visit, after recording any earlier groups.
#[derive(Clone, Copy)]
struct IndexCandidate {
    reader: IndexReader,
    input: OutputPort,
    function: NodeId,
    nullable: bool,
}

impl IndexCandidate {
    fn eligible(
        &self,
        edit: &CircuitEditor,
        cache: &mut HashMap<NodeId, bool>,
    ) -> Result<bool, IndexDiscoveryError> {
        match cache.entry(self.function) {
            std::collections::hash_map::Entry::Occupied(entry) => Ok(*entry.get()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                Ok(*entry.insert(decomposable(edit, self.function)?))
            }
        }
    }
}

fn shared_index_candidates(edit: &CircuitEditor) -> Result<Vec<IndexCandidate>, Error> {
    let readers = readers(edit.circuit())?;
    let mut result = Vec::with_capacity(readers.len());
    for id in edit.circuit().operator_order(BodyOrder::Postorder)? {
        let Some(&reader) = readers.get(&id) else {
            continue;
        };
        let index = edit.node(id)?;
        let input = index
            .inputs()
            .next()
            .ok_or_else(|| Error::new("shared index requires an input"))?;
        let value = edit.node(index.edge("outputType")?)?.edge("elementType")?;
        result.push(IndexCandidate {
            reader,
            input,
            function: index.edge("function")?,
            nullable: edit.nullable(edit.type_id(value)?)?,
        });
    }
    Ok(result)
}

/// Project the key of an index closure, preserving its parameters and the
/// simplification diagnostics produced when the projection is applied.
fn index_key(
    edit: &mut CircuitEditor,
    function: NodeId,
    next: &mut u64,
    warnings: &mut Vec<feldera_sql_expr::SimplifyWarning>,
) -> Result<feldera_sql_ir::ExpressionId, Error> {
    let function = edit.expression(function)?;
    let row = edit.expression(edit.node(function.into())?.edge("body")?)?;
    let parameter = edit.ref_parameter(edit.expression_type(row)?, next)?;
    let pair = edit.deref_parameter(&parameter)?;
    let key = edit.tuple_field(pair, 0)?;
    let project = edit.closure(key, &[parameter])?;
    feldera_sql_expr::compose_closures(edit, function, false, project.into(), Some(warnings))
}

pub struct SharedIndexes {
    pub groups: Vec<Vec<IndexReader>>,
    pub visited: BTreeSet<u64>,
    pub warnings: Vec<SimplifyWarning>,
}

#[derive(Default)]
struct Keys {
    projections: HashMap<u64, (ExpressionId, Vec<SimplifyWarning>)>,
}

impl Keys {
    fn project(
        &mut self,
        edit: &mut CircuitEditor,
        index: u64,
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<ExpressionId, Error> {
        let (function, emitted) = match self.projections.entry(index) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                // Each application of the reference key projection takes a new
                // parameter name, including repeated comparisons of a seed.
                *next = next
                    .checked_add(1)
                    .ok_or_else(|| Error::new("index projection name overflow"))?;
                entry.into_mut()
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let closure = edit.node(NodeId::Outer(index))?.edge("function")?;
                let mut emitted = Vec::new();
                let key = index_key(edit, closure, next, &mut emitted)?;
                entry.insert((key, emitted))
            }
        };
        warnings.extend(emitted.iter().cloned());
        Ok(*function)
    }
}

/// Visit seeds in postorder and compare only unmatched siblings in their input
/// partition. A complex key may require the caller's expression equivalence.
pub fn find_shared_indexes(
    edit: &mut CircuitEditor,
    previously_visited: &BTreeSet<u64>,
    next: &mut u64,
    mut compare_complex: impl FnMut(&CircuitEditor, [ExpressionId; 2]) -> Result<bool, Error>,
) -> Result<SharedIndexes, Error> {
    let mut state = SharedIndexes::new(previously_visited.clone());
    state
        .scan(edit, next, |edit, [left, right]| {
            match equivalent(edit, left, right)? {
                Some(equal) => Ok(equal),
                None => Ok(compare_complex(edit, [left, right])?),
            }
        })
        .map_err(|error| Error::new(error.to_string()))?;
    Ok(state)
}

impl SharedIndexes {
    pub fn new(visited: BTreeSet<u64>) -> Self {
        Self {
            visited,
            groups: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// A failed comparison leaves the completed groups, visits, warnings, and
    /// allocator at the same point as the Java visitor.
    pub fn discover(
        &mut self,
        edit: &mut CircuitEditor,
        next: &mut u64,
        mut unresolved: impl FnMut(&CircuitEditor, [ExpressionId; 2]) -> Result<bool, KeyComparisonError>,
    ) -> Result<(), IndexDiscoveryError> {
        self.scan(edit, next, |edit, keys| match check_closures(
            edit, keys[0], keys[1],
        )? {
            Some(equal) => Ok(equal),
            None => Ok(unresolved(edit, keys)?),
        })
    }

    fn scan(
        &mut self,
        edit: &mut CircuitEditor,
        next: &mut u64,
        mut compare: impl FnMut(&CircuitEditor, [ExpressionId; 2]) -> Result<bool, IndexDiscoveryError>,
    ) -> Result<(), IndexDiscoveryError> {
        let candidates = shared_index_candidates(edit)?;
        let mut partitions = HashMap::<OutputPort, Vec<IndexCandidate>>::new();
        for candidate in &candidates {
            partitions
                .entry(candidate.input)
                .or_default()
                .push(*candidate);
        }
        let mut keys = Keys::default();
        let mut eligible = HashMap::new();
        for seed in candidates {
            if self.visited.contains(&seed.reader.index) || !seed.eligible(edit, &mut eligible)? {
                continue;
            }
            self.visited.insert(seed.reader.index);
            let mut group = vec![seed.reader];
            for sibling in &partitions[&seed.input] {
                if self.visited.contains(&sibling.reader.index)
                    || !sibling.eligible(edit, &mut eligible)?
                    || sibling.nullable != seed.nullable
                {
                    continue;
                }
                let left = keys.project(edit, seed.reader.index, next, &mut self.warnings)?;
                let right = keys.project(edit, sibling.reader.index, next, &mut self.warnings)?;
                if compare(edit, [left, right])? {
                    self.visited.insert(sibling.reader.index);
                    group.push(sibling.reader);
                }
            }
            if group.len() > 1 {
                self.groups.push(group);
            }
        }
        Ok(())
    }
}
