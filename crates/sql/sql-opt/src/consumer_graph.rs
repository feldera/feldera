//! Reverse the input edges of each circuit body for optimizer fanout and reachability queries.
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind as Op};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};

/// Successors in consumer order, indexed by the producer's position in its body.
/// Each pair identifies the consumer's position and its input slot.
pub type Successors = Vec<Vec<(usize, usize)>>;

/// The JNI projection carries only the ordered inputs and the operators that
/// affect recursive feedback. Normal operators omit their role on the wire.
#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Role {
    #[default]
    Ordinary,
    Delta,
    View(String),
    Declaration(String),
}

#[derive(Deserialize)]
struct ConsumerOperator {
    id: u64,
    inputs: Vec<u64>,
    #[serde(default)]
    role: Role,
}

#[derive(Deserialize)]
pub struct ConsumerBody {
    id: u64,
    operators: Vec<ConsumerOperator>,
}

/// Build successor lists while reading operators in body order. A recursive
/// declaration can precede its view, so attach its feedback edge after the scan.
struct BodyGraph {
    positions: HashMap<NodeId, usize>,
    successors: Successors,
    views: HashMap<String, usize>,
    declarations: Vec<(usize, String)>,
}

impl BodyGraph {
    fn new(capacity: usize) -> Self {
        Self {
            positions: HashMap::with_capacity(capacity),
            successors: Vec::with_capacity(capacity),
            views: HashMap::new(),
            declarations: Vec::new(),
        }
    }

    fn add(
        &mut self,
        id: NodeId,
        role: Role,
        inputs: impl Iterator<Item = NodeId>,
    ) -> Result<(), Error> {
        let position = self.successors.len();
        if self.positions.insert(id, position).is_some() {
            return Err(Error::new("duplicate operator in consumer graph"));
        }
        self.successors.push(Vec::new());
        match role {
            Role::Delta => return Ok(()),
            Role::Declaration(name) => {
                self.declarations
                    .push((position, name.replace("-decl", "")));
                return Ok(());
            }
            Role::View(name) => {
                self.views.insert(name, position);
            }
            Role::Ordinary => {}
        }
        for (slot, producer) in inputs.enumerate() {
            let &source = self.positions.get(&producer).ok_or_else(|| {
                Error::new("consumer graph input must precede its reader in the same body")
            })?;
            self.successors[source].push((position, slot));
        }
        Ok(())
    }

    fn finish(mut self) -> Successors {
        for (decl, name) in self.declarations {
            if let Some(&view) = self.views.get(&name) {
                self.successors[view].push((decl, 0));
            }
        }
        self.successors
    }
}

pub(super) fn successors(circuit: &Circuit, body: NodeId) -> Result<Successors, Error> {
    let members = circuit
        .node(body)
        .expect("validated body")
        .edges("allOperators")?;
    let mut graph = BodyGraph::new(members.len());
    for id in members {
        let node = circuit.node(id).expect("validated body member");
        let role = match node.operator_kind() {
            Some(Op::Delta) => Role::Delta,
            Some(Op::ViewDeclaration) => Role::Declaration(node.relation_name("tableName")?.into()),
            Some(Op::View) => Role::View(node.relation_name("viewName")?.into()),
            _ => Role::Ordinary,
        };
        graph.add(id, role, node.inputs().map(|input| input.operator))?;
    }
    Ok(graph.finish())
}

pub fn projected_consumer_graphs(
    bodies: Vec<ConsumerBody>,
) -> Result<BTreeMap<u64, Successors>, Error> {
    let mut graphs = BTreeMap::new();
    for body in bodies {
        let mut graph = BodyGraph::new(body.operators.len());
        for op in body.operators {
            graph.add(
                NodeId::Outer(op.id),
                op.role,
                op.inputs.into_iter().map(NodeId::Outer),
            )?;
        }
        if graphs.insert(body.id, graph.finish()).is_some() {
            return Err(Error::new("duplicate body in consumer graph"));
        }
    }
    Ok(graphs)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_slots_precede_recursive_feedback() {
        let bodies = serde_json::from_str(
            r#"[[1,[[2,[]],[3,[2]]]],[3,[[4,[],{"declaration":"v-decl"}],
                [5,[2],"delta"],[6,[5,5],{"view":"v"}],[7,[6]]]]]"#,
        )
        .unwrap();
        let graphs = projected_consumer_graphs(bodies).unwrap();
        assert_eq!(graphs[&1], [vec![(1, 0)], vec![]]);
        assert_eq!(
            graphs[&3],
            [vec![], vec![(2, 0), (2, 1)], vec![(3, 0), (0, 0)], vec![]]
        );
    }
}
