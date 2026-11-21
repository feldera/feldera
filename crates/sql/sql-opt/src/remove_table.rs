//! Remove the compiler's synthetic input after the last pass that consumes it.
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind};
use std::collections::BTreeSet;

/// The compiler also drops the table's buffered DML when a matching
/// unkeyed source exists. A schema entry alone does not trigger removal.
pub struct TableRemoval {
    pub circuit: Circuit,
    pub removed: bool,
}

pub fn remove_table(
    mut circuit: Circuit,
    name: &str,
    next: &mut u64,
) -> Result<TableRemoval, Error> {
    let remove = unreferenced_table_sources(&circuit, name)?;
    let removed = !remove.is_empty();
    if removed {
        circuit.remove_input_schema(name)?;
        let keep = circuit
            .operator_order(BodyOrder::Preorder)?
            .into_iter()
            .filter(|id| !remove.contains(id))
            .collect();
        circuit = crate::rebuild::rebuild_input_closures(circuit.retain_operators(&keep)?, next)?;
    }
    Ok(TableRemoval { circuit, removed })
}

/// Find the sources to erase from the live JVM circuit. Its surviving
/// operators retain their relation, expression and variable identities.
pub fn unreferenced_table_sources(
    circuit: &Circuit,
    name: &str,
) -> Result<BTreeSet<NodeId>, Error> {
    let mut remove = BTreeSet::new();
    let order = circuit.operator_order(BodyOrder::Preorder)?;
    for &id in &order {
        let op = circuit.node(id).expect("validated circuit member");
        if op.operator_kind() == Some(OperatorKind::SourceMultiset)
            && op.relation_name("tableName")? == name
        {
            remove.insert(id);
        }
    }
    if !remove.is_empty() {
        // The original clone visitor cannot remap a reader of the deleted source.
        if order.iter().any(|&id| {
            circuit
                .node(id)
                .expect("validated circuit member")
                .inputs()
                .any(|p| remove.contains(&p.operator))
        }) {
            return Err(Error::new("cannot remove a table that still has readers"));
        }
    }
    Ok(remove)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn removes_only_the_matching_unkeyed_source_and_its_schema_entry() {
        let wire = json!({"class":"DBSPCircuit","id":0,"declarations":[],
        "metadata":{"inputs":[{"name":"CLOCK"},{"name":"clock"},{"name":"keyed"}],"outputs":[]},
        "allOperators":[
            {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],"tableName":{"name":"CLOCK","isQuoted":true}},
            {"class":"DBSPSourceMapOperator","id":2,"inputs":[],"tableName":{"name":"keyed","isQuoted":false}}
        ]});
        let mut next = 9;
        let result = remove_table(Circuit::from_json(wire).unwrap(), "CLOCK", &mut next).unwrap();
        assert!(result.removed);
        let keyed = remove_table(result.circuit, "keyed", &mut next).unwrap();
        assert!(!keyed.removed);
        let wire = keyed.circuit.to_json();
        assert_eq!(
            wire["metadata"]["inputs"],
            json!([{"name":"clock"},{"name":"keyed"}])
        );
        assert_eq!(wire["allOperators"].as_array().unwrap().len(), 1);
        assert_eq!(next, 9);
    }
}
