//! Validate global-aggregate regions before the circuit is handed to DBSP.
//! Match the two traversals of TagRegions. Conflicts remove a region everywhere.

use feldera_sql_ir::{BodyOrder, Circuit, Field, NodeId};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
};

/// GlobalAggregate is the region annotation supported by the circuit format.
/// Other annotation classes coexist with it and survive region cleanup.
fn region(annotation: &Value) -> Result<Option<i32>, Box<dyn Error>> {
    if annotation["class"] != "GlobalAggregate" {
        return Ok(None);
    }
    let id = annotation["id"]
        .as_i64()
        .ok_or("GlobalAggregate requires an integer id")?;
    Ok(Some(i32::try_from(id)?))
}

fn regions(circuit: &Circuit, id: NodeId) -> Result<Vec<i32>, Box<dyn Error>> {
    let Some(Field::Data(Value::Array(annotations))) =
        circuit.node(id).and_then(|n| n.field("annotations"))
    else {
        return Err(format!("operator {id:?} requires annotations").into());
    };
    annotations
        .iter()
        .filter_map(|a| region(a).transpose())
        .collect()
}

/// Detect conflicts in circuit postorder, then remove all occurrences of the
/// rejected regions. The predecessor set uses the original input annotations.
pub fn tag_regions(mut circuit: Circuit) -> Result<Circuit, Box<dyn Error>> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut removed = BTreeSet::new();
    let mut predecessors = BTreeMap::<NodeId, BTreeSet<i32>>::new();
    for &id in &order {
        let mut local = regions(&circuit, id)?;
        local.retain(|r| !removed.contains(r));
        // Java orders the tag's variable names lexically, including the numeric suffix.
        local.sort_by_cached_key(i32::to_string);
        removed.extend(local.iter().skip(1).copied());
        let mut ancestors = BTreeSet::new();
        for port in circuit.node(id).expect("ordered operator").inputs() {
            let direct = regions(&circuit, port.operator)?;
            if let Some(current) = local.first() {
                if direct.contains(current) {
                    continue;
                }
                if predecessors
                    .get(&port.operator)
                    .ok_or("unvisited region predecessor")?
                    .contains(current)
                {
                    removed.insert(*current);
                }
            }
            ancestors.extend(direct);
        }
        ancestors.extend(local);
        predecessors.insert(id, ancestors);
    }
    for id in order {
        circuit.annotations_mut(id)?.retain(|a| {
            !region(a)
                .expect("annotations validated before mutation")
                .is_some_and(|r| removed.contains(&r))
        });
    }
    Ok(circuit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn gap_and_overlapping_regions_remove_conflicts_across_the_graph() {
        let region = |id| json!({"class":"GlobalAggregate","id":id});
        let ops = [
            vec![region(3), region(2), region(10)],
            vec![],
            vec![region(10)],
            vec![region(2)],
        ];
        let ops = ops.into_iter().enumerate().map(|(i, annotations)| {
            let inputs = i.checked_sub(1).map(|p| json!({"outputNumber":0,"operator":{"node":p}}));
            json!({"class":"DBSPMapOperator","id":i,"annotations":annotations,"inputs":inputs.into_iter().collect::<Vec<_>>()})
        }).collect::<Vec<_>>();
        let circuit = Circuit::from_json(
            json!({"class":"DBSPCircuit","id":9,"declarations":[],"allOperators":ops}),
        )
        .unwrap();
        let out = tag_regions(circuit).unwrap().to_json();
        assert!(out["allOperators"]
            .as_array()
            .unwrap()
            .iter()
            .all(|op| op["annotations"] == json!([])));
    }
}
