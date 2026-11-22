//! Find fanouts whose readers can share a narrower input row.
use crate::{
    consumer_graph::consumer_graphs, row_fields::RowFields, row_projection::RowProjection,
};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, InnerKind, NodeId, OperatorKind as Op,
    TypeKind as Ty,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

type Paths = BTreeSet<Vec<usize>>;

fn outer(id: NodeId) -> u64 {
    let NodeId::Outer(id) = id else {
        unreachable!("circuit body member")
    };
    id
}

fn candidates(circuit: &Circuit) -> Result<BTreeMap<NodeId, Vec<NodeId>>, Error> {
    let mut groups = BTreeMap::new();
    for (body, graph) in consumer_graphs(circuit)? {
        let members = circuit
            .node(NodeId::Outer(body))
            .expect("validated body")
            .edges("allOperators")?;
        for (&producer, edges) in members.iter().zip(graph) {
            if edges.len() < 2
                || matches!(
                    circuit.node(producer).and_then(|n| n.operator_kind()),
                    Some(Op::Nested | Op::Constant)
                )
            {
                continue;
            }
            let readers: Vec<_> = edges.into_iter().map(|(i, _)| members[i]).collect();
            if readers.iter().all(|&r| {
                matches!(
                    circuit.node(r).and_then(|n| n.operator_kind()),
                    Some(Op::Map | Op::MapIndex | Op::Deindex)
                )
            }) {
                groups.insert(producer, readers);
            }
        }
    }
    Ok(groups)
}

/// Only candidate reader closures and their producers' output types are needed.
pub fn common_projection_payloads(circuit: &Circuit) -> Result<BTreeSet<u64>, Error> {
    Ok(candidates(circuit)?
        .into_iter()
        .flat_map(|(p, readers)| std::iter::once(p).chain(readers))
        .map(outer)
        .collect())
}

#[derive(Serialize)]
pub struct CommonProjection {
    pub producer: u64,
    pub readers: Vec<u64>,
    pub depth: usize,
    pub paths: Paths,
    #[serde(skip)]
    first_reader_uses_input: bool,
}

/// A producer's projection or one of the readers rebound to its narrower input.
pub struct SharedProjectionClosure {
    pub operator: u64,
    pub function: ExpressionId,
    pub original_output_size: Option<usize>,
}

pub fn shared_projection_closures(
    edit: &mut CircuitEditor,
    next: &mut u64,
) -> Result<Vec<SharedProjectionClosure>, Error> {
    let mut result = Vec::new();
    for group in common_projections(edit.circuit())? {
        let readers = group
            .readers
            .into_iter()
            .map(|id| Ok((id, edit.node(NodeId::Outer(id))?.edge("function")?)))
            .collect::<Result<Vec<_>, Error>>()?;
        let first = readers
            .first()
            .ok_or_else(|| Error::new("shared projection has no readers"))?
            .1;
        // The shared layout uses the first reader's types. Readers can have
        // equivalent, separately allocated tuple types at the same fanout.
        let layout = RowProjection::new(edit, first, &group.paths)?;
        for (id, closure) in readers {
            result.push(SharedProjectionClosure {
                operator: id,
                function: layout.rebind(edit, closure)?,
                original_output_size: None,
            });
        }
        result.push(SharedProjectionClosure {
            operator: group.producer,
            function: layout.project(edit, next, group.first_reader_uses_input)?,
            original_output_size: Some(layout.input_width()),
        });
    }
    Ok(result)
}

pub fn common_projections(circuit: &Circuit) -> Result<Vec<CommonProjection>, Error> {
    let mut candidates = candidates(circuit)?;
    let mut result = Vec::new();
    let mut shape = RowFields::new(circuit);
    for producer in circuit.operator_order(BodyOrder::Postorder)? {
        let Some(readers) = candidates.remove(&producer) else {
            continue;
        };
        let output = circuit
            .node(producer)
            .expect("validated producer")
            .edge("outputType")?;
        let depth = if matches!(
            circuit.node(output).and_then(|n| n.inner_kind()),
            Some(InnerKind::Type(Ty::ZSet))
        ) {
            1
        } else {
            2
        };
        let mut paths = Paths::new();
        let mut parameter_type = None;
        let mut first_reader_uses_input = false;
        for &reader in &readers {
            let function = circuit
                .node(reader)
                .expect("validated reader")
                .edge("function")?;
            let closure = circuit.node(function).expect("validated reader function");
            if !matches!(
                closure.inner_kind(),
                Some(InnerKind::Expression(
                    feldera_sql_ir::ExpressionKind::Closure
                ))
            ) {
                parameter_type = None;
                break;
            }
            let [param] = closure
                .edges("parameters")?
                .try_into()
                .map_err(|_| Error::new("shared projection must have one parameter"))?;
            let ty = circuit
                .node(param)
                .expect("validated parameter")
                .edge("type")?;
            let used = feldera_sql_expr::input_field_paths(circuit, function)?;
            if parameter_type.is_none() {
                first_reader_uses_input = !used.is_empty();
            }
            if !shape.unused(ty, &[], &used, depth)? {
                parameter_type = None;
                break;
            }
            parameter_type = Some(ty);
            paths.extend(used);
        }
        if let Some(ty) = parameter_type {
            if shape.unused(ty, &[], &paths, depth)? {
                result.push(CommonProjection {
                    producer: outer(producer),
                    readers: readers.into_iter().map(outer).collect(),
                    depth,
                    paths,
                    first_reader_uses_input,
                });
            }
        }
    }
    Ok(result)
}
