//! Select global aggregate regions that are incompatible with circuit topology.

use super::{array, field, uint};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};

#[derive(Default)]
struct RegionState {
    operators: HashMap<u64, OperatorRegions>,
    removed: BTreeSet<i32>,
}

struct OperatorRegions {
    attached: Vec<i32>,
    predecessors: BTreeSet<i32>,
}

pub(crate) fn conflicting_regions(circuit: &Value) -> Result<Value> {
    feldera_sql_difftest::compare_circuits(circuit, circuit)
        .map_err(|error| Error(error.to_string()))?;
    let mut state = RegionState::default();
    state.visit(array(circuit, "allOperators")?)?;
    Ok(json!(state.removed))
}

impl RegionState {
    fn visit(&mut self, operators: &[Value]) -> Result<()> {
        for operator in operators {
            if operator["class"] == "DBSPNestedOperator" {
                self.visit(array(operator, "allOperators")?)?;
            }
            self.visit_operator(operator)?;
        }
        Ok(())
    }

    fn visit_operator(&mut self, operator: &Value) -> Result<()> {
        let attached = array(operator, "annotations")?
            .iter()
            .filter(|annotation| annotation["class"] == "GlobalAggregate")
            .map(region_id)
            .collect::<Result<Vec<_>>>()?;
        let mut remaining: Vec<_> = attached
            .iter()
            .copied()
            .filter(|region| !self.removed.contains(region))
            .collect();
        // Java orders by `agg_zero_<id>`, so 10 sorts before 2.
        remaining.sort_by_cached_key(i32::to_string);
        self.removed.extend(remaining.iter().skip(1).copied());

        let mut predecessors = BTreeSet::new();
        for input in array(operator, "inputs")? {
            let id = uint(field(input, "operator")?, "node")?;
            let source = self
                .operators
                .get(&id)
                .ok_or_else(|| Error(format!("missing region predecessor {id}")))?;
            if let Some(region) = remaining.first() {
                if source.attached.contains(region) {
                    continue;
                }
                if source.predecessors.contains(region) {
                    self.removed.insert(*region);
                }
            }
            predecessors.extend(source.attached.iter().copied());
        }
        predecessors.extend(remaining);
        self.operators.insert(
            uint(operator, "id")?,
            OperatorRegions {
                attached,
                predecessors,
            },
        );
        Ok(())
    }
}

fn region_id(annotation: &Value) -> Result<i32> {
    annotation["id"]
        .as_i64()
        .and_then(|id| i32::try_from(id).ok())
        .ok_or_else(|| Error("global aggregate region id must be a 32-bit integer".into()))
}
