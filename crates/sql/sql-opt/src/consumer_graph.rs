//! Reverse the input edges of each circuit body for optimizer fanout and reachability queries.
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind as Op};
use std::collections::{BTreeMap, HashMap};

/// Successors in consumer order, indexed by the producer's position in its body.
/// Each pair identifies the consumer's position and its input slot.
pub type Successors = Vec<Vec<(usize, usize)>>;

pub(super) fn successors(circuit: &Circuit, body: NodeId) -> Result<Successors, Error> {
    let members = circuit
        .node(body)
        .expect("validated body")
        .edges("allOperators")?;
    let mut positions = HashMap::with_capacity(members.len());
    let mut result = vec![Vec::new(); members.len()];
    let mut views = HashMap::new();
    let mut declarations = Vec::new();
    for (position, &id) in members.iter().enumerate() {
        let node = circuit.node(id).expect("validated body member");
        positions.insert(id, position);
        match node.operator_kind() {
            // The enclosing nested operator accounts for cross-body inputs.
            Some(Op::Delta) => continue,
            Some(Op::ViewDeclaration) => {
                declarations.push((
                    position,
                    node.relation_name("tableName")?.replace("-decl", ""),
                ));
                continue;
            }
            Some(Op::View) => {
                views.insert(node.relation_name("viewName")?, position);
            }
            _ => {}
        }
        for (slot, input) in node.inputs().enumerate() {
            let &source = positions.get(&input.operator).ok_or_else(|| {
                Error::new("consumer graph input must precede its reader in the same body")
            })?;
            result[source].push((position, slot));
        }
    }
    // A declaration reads the previous value of its corresponding view. These
    // feedback edges come after ordinary readers, including forward references.
    for (decl, name) in declarations {
        if let Some(&view) = views.get(name.as_str()) {
            result[view].push((decl, 0));
        }
    }
    Ok(result)
}

pub fn consumer_graphs(circuit: &Circuit) -> Result<BTreeMap<u64, Successors>, Error> {
    let bodies = std::iter::once(circuit.root_id()).chain(
        circuit
            .operator_order(BodyOrder::Preorder)?
            .into_iter()
            .filter(|&id| {
                circuit
                    .node(id)
                    .expect("validated operator")
                    .operator_kind()
                    == Some(Op::Nested)
            }),
    );
    bodies
        .map(|body| {
            let NodeId::Outer(id) = body else {
                unreachable!("circuit body has an outer identity")
            };
            Ok((id, successors(circuit, body)?))
        })
        .collect()
}
