//! Select indexes whose sole reader can share the integral of a common input.
use crate::consumer_graph::consumer_graphs;
use crate::index_key_equivalence::equivalent;
use crate::projection::{is_map_projection, simple_tuple};
use crate::shared_indexes::shares_input_integral;
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::ExpressionId;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind as Expr, NodeId, OperatorKind as Op,
};
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

fn decomposable(edit: &CircuitEditor, function: NodeId) -> Result<bool, Error> {
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
        && is_map_projection(edit, function, true)?)
}

/// Partition by the complete producer port and the nullability of the indexed
/// value. Record the traversal order independently of the partition so later
/// consumers see groups in the same order as their first matching index.
struct IndexCandidate {
    pub reader: IndexReader,
    pub partition: usize,
}

fn shared_index_candidates(edit: &CircuitEditor) -> Result<Vec<IndexCandidate>, Error> {
    let readers = readers(edit.circuit())?;
    let mut partitions = HashMap::new();
    let mut result = Vec::new();
    for id in edit.circuit().operator_order(BodyOrder::Postorder)? {
        let Some(&reader) = readers.get(&id) else {
            continue;
        };
        let index = edit.node(id)?;
        if !decomposable(edit, index.edge("function")?)? {
            continue;
        }
        let port = index
            .inputs()
            .next()
            .ok_or_else(|| Error::new("shared index requires an input"))?;
        let value = edit.node(index.edge("outputType")?)?.edge("elementType")?;
        let key = (port, edit.nullable(edit.type_id(value)?)?);
        let next = partitions.len();
        let partition = *partitions.entry(key).or_insert(next);
        result.push(IndexCandidate { reader, partition });
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
    let candidates = shared_index_candidates(edit)?;
    let mut partitions = HashMap::<usize, Vec<IndexReader>>::new();
    for candidate in &candidates {
        partitions
            .entry(candidate.partition)
            .or_default()
            .push(candidate.reader);
    }
    let mut result = SharedIndexes {
        groups: Vec::new(),
        visited: previously_visited.clone(),
        warnings: Vec::new(),
    };
    let mut keys = Keys::default();
    for seed in candidates {
        if !result.visited.insert(seed.reader.index) {
            continue;
        }
        let mut group = vec![seed.reader];
        for &sibling in &partitions[&seed.partition] {
            if result.visited.contains(&sibling.index) {
                continue;
            }
            let left = keys.project(edit, seed.reader.index, next, &mut result.warnings)?;
            let right = keys.project(edit, sibling.index, next, &mut result.warnings)?;
            let same = match equivalent(edit, left, right)? {
                Some(decided) => decided,
                None => compare_complex(edit, [left, right])?,
            };
            if same {
                result.visited.insert(sibling.index);
                group.push(sibling);
            }
        }
        if group.len() > 1 {
            result.groups.push(group);
        }
    }
    Ok(result)
}
