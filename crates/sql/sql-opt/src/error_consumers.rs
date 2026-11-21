//! Schedule the system error stream after every producer that can report a late row.
use feldera_sql_ir::{Circuit, CircuitEditor, Error, NodeId, OperatorKind as Op};
use std::collections::{HashMap, HashSet};

/// The suffix starting at `first_error` feeds the synthetic error stream.
pub struct ErrorConsumerOrder {
    pub operators: Vec<NodeId>,
    pub first_error: usize,
}

impl ErrorConsumerOrder {
    pub fn apply(&self, circuit: Circuit) -> Result<Circuit, Error> {
        if circuit
            .operators()
            .map(|(id, _)| id)
            .eq(self.operators.iter().copied())
        {
            return Ok(circuit);
        }
        let mut edit = CircuitEditor::new(circuit);
        edit.order_root_operators(&self.operators)?;
        edit.finish()
    }
}

/// Root operators reachable from the error table in their original body order.
/// A nested circuit is a single vertex here. Its delta imports belong to its
/// private graph. A view declaration has an implicit edge from its definition.
pub fn error_consumers(circuit: &Circuit, table: &str) -> Result<ErrorConsumerOrder, Error> {
    let mut source = None;
    let mut views = HashMap::new();
    let mut declarations = Vec::new();
    let mut readers = HashMap::<NodeId, Vec<NodeId>>::new();
    for (id, node) in circuit.operators() {
        match node.operator_kind() {
            Some(
                Op::SourceMap | Op::SourceLazyMap | Op::SourceMultiset | Op::InputMapWithWaterline,
            ) => {
                if node.relation_name("tableName")? == table && source.replace(id).is_some() {
                    return Err(Error::new("duplicate system error input"));
                }
            }
            Some(Op::View) => {
                views.insert(node.relation_name("viewName")?, id);
            }
            Some(Op::ViewDeclaration) => {
                declarations.push((id, node.relation_name("tableName")?.replace("-decl", "")));
                continue;
            }
            Some(Op::Delta) => continue,
            _ => {}
        }
        for port in node.inputs() {
            readers.entry(port.operator).or_default().push(id);
        }
    }

    for (id, name) in declarations {
        if let Some(&definition) = views.get(name.as_str()) {
            readers.entry(definition).or_default().push(id);
        }
    }
    let mut reached = HashSet::new();
    let mut pending: Vec<_> = source.into_iter().collect();
    while let Some(id) = pending.pop() {
        if reached.insert(id) {
            if let Some(next) = readers.get(&id) {
                pending.extend_from_slice(next);
            }
        }
    }
    let (mut operators, error): (Vec<_>, Vec<_>) = circuit
        .operators()
        .map(|(id, _)| id)
        .partition(|id| !reached.contains(id));
    let first_error = operators.len();
    operators.extend(error);
    Ok(ErrorConsumerOrder {
        operators,
        first_error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn follows_recursive_back_edges_and_keeps_definition_order() {
        let port = |id| json!({"outputNumber":0,"operator":{"node":id}});
        let graph = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            {"class":"DBSPViewDeclarationOperator","id":10,"tableName":{"name":"v-decl"},"inputs":[]},
            {"class":"DBSPSourceMultisetOperator","id":2,"tableName":{"name":"error"},"inputs":[]},
            {"class":"DBSPViewOperator","id":7,"viewName":{"name":"v"},"inputs":[port(2)]},
            {"class":"DBSPMapOperator","id":6,"inputs":[port(10)]},
            {"class":"DBSPConstantOperator","id":1,"inputs":[]}
        ]})).unwrap();
        let order = error_consumers(&graph, "error").unwrap();
        assert_eq!(
            order.operators[order.first_error..],
            [10, 2, 7, 6].map(NodeId::Outer)
        );
        assert_eq!(
            order
                .apply(graph.clone())
                .unwrap()
                .operators()
                .next()
                .unwrap()
                .0,
            NodeId::Outer(1)
        );
        let absent = error_consumers(&graph, "ERROR").unwrap();
        assert_eq!(absent.first_error, absent.operators.len());
    }
}
