//! Select indexes whose sole reader can share the integral of a common input.
use crate::consumer_graph::consumer_graphs;
use crate::projection::{is_map_projection, simple_tuple};
use crate::shared_indexes::shares_input_integral;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind as Expr, NodeId, OperatorKind as Op,
};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Copy, Debug, Serialize)]
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
#[derive(Serialize)]
pub struct IndexCandidate {
    #[serde(flatten)]
    pub reader: IndexReader,
    pub partition: usize,
}

pub fn shared_index_candidates(edit: &CircuitEditor) -> Result<Vec<IndexCandidate>, Error> {
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
pub fn index_key(
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
