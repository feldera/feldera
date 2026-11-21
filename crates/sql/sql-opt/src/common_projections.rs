//! Find fanouts whose readers can share a narrower input row.
use crate::consumer_graph::consumer_graphs;
use feldera_sql_ir::{
    BodyOrder, Circuit, Error, InnerKind, NodeId, OperatorKind as Op, TypeKind as Ty,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};

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

struct RowFields<'a> {
    circuit: &'a Circuit,
    empty_tuples: HashMap<NodeId, bool>,
}

impl RowFields<'_> {
    // A zero-column tuple counts as used, even when its enclosing row is discarded.
    fn contains_empty_tuple(&mut self, ty: NodeId, level: usize) -> Result<bool, Error> {
        if level > 128 {
            return Err(Error::new("input field type exceeds the nesting limit"));
        }
        if let Some(&empty) = self.empty_tuples.get(&ty) {
            return Ok(empty);
        }
        let node = self.circuit.node(ty).expect("validated row type");
        let result = match node.inner_kind() {
            Some(InnerKind::Type(Ty::Ref)) => {
                self.contains_empty_tuple(node.edge("type")?, level + 1)?
            }
            Some(InnerKind::Type(Ty::Tuple | Ty::RawTuple)) => {
                let fields = node.edges("tupFields")?;
                let mut empty = fields.is_empty();
                for f in fields {
                    if self.contains_empty_tuple(f, level + 1)? {
                        empty = true;
                        break;
                    }
                }
                empty
            }
            _ => false,
        };
        self.empty_tuples.insert(ty, result);
        Ok(result)
    }

    fn unused(
        &mut self,
        ty: NodeId,
        prefix: &mut Vec<usize>,
        paths: &Paths,
        depth: usize,
        level: usize,
    ) -> Result<bool, Error> {
        if level > 128 {
            return Err(Error::new("input field type exceeds the nesting limit"));
        }
        if paths.contains(prefix) {
            return Ok(false);
        }
        let node = self.circuit.node(ty).expect("validated field type");
        match node.inner_kind() {
            Some(InnerKind::Type(Ty::Ref)) => {
                self.unused(node.edge("type")?, prefix, paths, depth, level + 1)
            }
            Some(InnerKind::Type(Ty::Tuple | Ty::RawTuple)) if depth > 0 => {
                for (i, field) in node.edges("tupFields")?.into_iter().enumerate() {
                    prefix.push(i);
                    let unused = self.unused(field, prefix, paths, depth - 1, level + 1)?;
                    prefix.pop();
                    if unused {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            _ => Ok(!paths.iter().any(|p| p.starts_with(prefix))
                && !self.contains_empty_tuple(ty, level)?),
        }
    }
}

#[derive(Serialize)]
pub struct CommonProjection {
    pub producer: u64,
    pub readers: Vec<u64>,
    pub depth: usize,
    pub paths: Paths,
}

pub fn common_projections(circuit: &Circuit) -> Result<Vec<CommonProjection>, Error> {
    let mut candidates = candidates(circuit)?;
    let mut result = Vec::new();
    let mut shape = RowFields {
        circuit,
        empty_tuples: HashMap::new(),
    };
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
            if !shape.unused(ty, &mut Vec::new(), &used, depth, 0)? {
                parameter_type = None;
                break;
            }
            parameter_type = Some(ty);
            paths.extend(used);
        }
        if let Some(ty) = parameter_type {
            if shape.unused(ty, &mut Vec::new(), &paths, depth, 0)? {
                result.push(CommonProjection {
                    producer: outer(producer),
                    readers: readers.into_iter().map(outer).collect(),
                    depth,
                    paths,
                });
            }
        }
    }
    Ok(result)
}
