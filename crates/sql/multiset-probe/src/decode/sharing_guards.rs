//! Check the preconditions of index and output-integrator sharing.

use super::{array, nesting, reachability, text, uint, wire_port, WirePort};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy)]
enum Check {
    Index,
    OutputIntegrator,
    Projection,
}

pub(super) fn share_indexes(circuit: &Value) -> Result<Value> {
    check(array(circuit, "allOperators")?, Check::Index, 0)?;
    reachability::prune(circuit, true)
}

pub(super) fn share_output_integrators(circuit: &Value) -> Result<Value> {
    check(array(circuit, "allOperators")?, Check::OutputIntegrator, 0)?;
    Ok(circuit.clone())
}

pub(super) fn optimize_projection_visitor(circuit: &Value) -> Result<Value> {
    check(array(circuit, "allOperators")?, Check::Projection, 0)?;
    reachability::prune(circuit, true)
}

fn check(operators: &[Value], stage: Check, depth: usize) -> Result<()> {
    nesting(depth)?;
    let mut classes = HashMap::new();
    let mut unary_inputs = HashMap::new();
    let mut fanout: HashMap<u64, usize> = HashMap::new();
    for node in operators {
        let id = uint(node, "id")?;
        let class = text(node, "class")?;
        let inputs = array(node, "inputs")?;
        classes.insert(id, class);
        for input in inputs {
            *fanout.entry(wire_port(input)?.operator).or_default() += 1;
        }
        if matches!(class, "DBSPMapOperator" | "DBSPMapIndexOperator") {
            let [input] = inputs else {
                return Err(Error(format!("{class} requires a single input")));
            };
            unary_inputs.insert(id, wire_port(input)?);
        }
        if class == "DBSPNestedOperator" {
            check(array(node, "allOperators")?, stage, depth + 1)?;
        }
    }
    let mut source_ports = HashSet::<WirePort>::new();
    for (id, input) in unary_inputs.iter() {
        let indexed = classes.get(id).copied() == Some("DBSPMapIndexOperator");
        let source = classes.get(&input.operator).copied().unwrap_or_default();
        if matches!(stage, Check::Projection) {
            let one_reader = fanout.get(&input.operator) == Some(&1);
            if (one_reader && projection_join(source))
                || (!indexed && source == "DBSPFlatMapOperator")
                || (source == "DBSPConstantOperator" && (one_reader || !indexed))
            {
                return Err(Error(format!(
                    "OptimizeProjectionVisitor requires analyzing {source}"
                )));
            }
            continue;
        }
        if !indexed {
            continue;
        }
        if matches!(stage, Check::Index) {
            if fanout.get(id).copied().unwrap_or_default() > 1 {
                return Err(Error(
                    "ShareIndexes requires checking a shared index".into(),
                ));
            }
            let mut source = *input;
            for _ in 0..operators.len() {
                let Some(parent) = unary_inputs.get(&source.operator) else {
                    break;
                };
                source = *parent;
            }
            if !source_ports.insert(source) {
                return Err(Error("ShareIndexes requires comparing index chains".into()));
            }
        } else if matches!(
            source,
            "DBSPPartitionedRollingAggregateOperator"
                | "DBSPPartitionedRollingAggregateWithWaterlineOperator"
                | "DBSPIntegrateOperator"
                | "DBSPIndexedTopKOperator"
                | "DBSPRankOperator"
                | "DBSPAggregateOperator"
                | "DBSPRowNumberOperator"
                | "DBSPLagOperator"
        ) {
            return Err(Error(
                "ShareOutputIntegrators requires comparing an index key".into(),
            ));
        }
    }
    Ok(())
}

fn projection_join(class: &str) -> bool {
    matches!(
        class,
        "DBSPJoinOperator"
            | "DBSPLeftJoinOperator"
            | "DBSPStreamJoinOperator"
            | "DBSPAsofJoinOperator"
            | "DBSPStarJoinOperator"
            | "DBSPJoinIndexOperator"
            | "DBSPLeftJoinIndexOperator"
            | "DBSPStreamJoinIndexOperator"
            | "DBSPStarJoinIndexOperator"
    )
}
