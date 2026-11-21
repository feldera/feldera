//! Match the operator preconditions of inactive stages in the bounded prefix.

use super::{
    array, cse, expression_guards, field, nesting, reachability, sharing_guards, text, uint,
    wire_port, Definitions,
};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub(crate) fn check(circuit: &Value, pass: &str) -> Result<Value> {
    match pass {
        "LineageDisabled" => return Ok(circuit.clone()),
        "CSE" => return cse::require_no_duplicates(circuit),
        "ShareIndexes" => return sharing_guards::share_indexes(circuit),
        "ShareOutputIntegrators" => return sharing_guards::share_output_integrators(circuit),
        "OptimizeProjectionVisitor" => return sharing_guards::optimize_projection_visitor(circuit),
        "DecomposeExpensiveFilters"
        | "FuseExpensiveMaps"
        | "SimplifyConditionals"
        | "SimplifyWaterline" => return expression_guards::check(circuit, pass),
        "ComparatorDeclarations" => {
            let definitions = Definitions::read(circuit)?;
            if definitions
                .inner
                .values()
                .any(|value| value["class"] == "DBSPTypeWithCustomOrd")
            {
                return Err(Error(
                    "ComparatorDeclarations requires lowering a custom order".into(),
                ));
            }
            return Ok(circuit.clone());
        }
        "UDFInliner" => {
            let definitions = Definitions::read(circuit)?;
            let mut executable = HashSet::new();
            for value in definitions.inner.values() {
                if value["class"] == "DBSPFunctionItem" {
                    let function = definitions.resolve(field(value, "function")?)?;
                    if !function["body"].is_null() {
                        executable.insert(text(function, "name")?);
                    }
                }
            }
            if !executable.is_empty() {
                require_no_udf_call(circuit, &definitions, &executable, &mut HashSet::new(), 0)?;
            }
            return Ok(circuit.clone());
        }
        "FilterJoinVisitor" => {
            require_no_filter_join(array(circuit, "allOperators")?, 0)?;
            return reachability::prune(circuit, true);
        }
        "LowerCircuitClassic" | "LowerCircuitGen2" => {
            let definitions = Definitions::read(circuit)?;
            require_no_lowering(
                array(circuit, "allOperators")?,
                &definitions,
                pass == "LowerCircuitGen2",
                0,
            )?;
            return Ok(circuit.clone());
        }
        _ => {}
    }
    let candidate: fn(&str) -> bool = match pass {
        "RecursiveComponents" => |class| {
            matches!(class, "DBSPViewDeclarationOperator" | "DBSPNestedOperator")
                || left_join(class)
        },
        "LeftJoinChains" | "RemoveUselessLeftJoins" => left_join,
        "RemoveStarJoins" => |class| class.starts_with("DBSPStarJoin"),
        "ExpandHop" => |class| class == "DBSPHopOperator",
        "LowerAsof" => |class| {
            matches!(
                class,
                "DBSPAsofJoinOperator"
                    | "DBSPConcreteAsofJoinOperator"
                    | "DBSPIntegrateTraceRetainValuesOperator"
                    | "DBSPIntegrateTraceRetainNValuesOperator"
            )
        },
        "ExpandAggregates" => |class| class == "DBSPStreamAggregateOperator",
        "FuseIntoArrayAgg" => |class| {
            matches!(
                class,
                "DBSPStreamAggregateOperator"
                    | "DBSPAggregateOperator"
                    | "DBSPPartitionedRollingAggregateOperator"
            )
        },
        _ => return Err(Error(format!("unknown optimizer stage {pass}"))),
    };
    require_no_candidate(array(circuit, "allOperators")?, candidate, pass, 0)?;
    if pass == "RemoveUselessLeftJoins" {
        return reachability::prune(circuit, true);
    }
    Ok(circuit.clone())
}

fn require_no_candidate(
    operators: &[Value],
    candidate: fn(&str) -> bool,
    pass: &str,
    depth: usize,
) -> Result<()> {
    nesting(depth)?;
    for node in operators {
        let class = text(node, "class")?;
        if candidate(class) {
            return Err(Error(format!(
                "{pass} requires an unimplemented rewrite for {class}"
            )));
        }
        if class == "DBSPNestedOperator" {
            require_no_candidate(array(node, "allOperators")?, candidate, pass, depth + 1)?;
        }
    }
    Ok(())
}

fn left_join(class: &str) -> bool {
    class.starts_with("DBSPLeftJoin")
}

fn require_no_udf_call<'a>(
    node: &'a Value,
    definitions: &Definitions<'a>,
    executable: &HashSet<&str>,
    visited: &mut HashSet<u64>,
    depth: usize,
) -> Result<()> {
    nesting(depth)?;
    match node {
        Value::Array(items) => {
            for item in items {
                require_no_udf_call(item, definitions, executable, visited, depth + 1)?;
            }
        }
        Value::Object(fields) => {
            if let Some(id) = node["node"].as_u64() {
                if let Some(value) = definitions.inner.get(&id).filter(|_| visited.insert(id)) {
                    require_no_udf_call(value, definitions, executable, visited, depth + 1)?;
                }
                return Ok(());
            }
            if definitions
                .call_name(node)?
                .is_some_and(|name| executable.contains(name))
            {
                return Err(Error("UDFInliner requires inlining a SQL UDF call".into()));
            }
            // The circuit rewriter traverses executable expressions, while
            // table column defaults are copied with the original metadata.
            for (key, value) in fields {
                if key != "metadata" && key != "inputs" {
                    require_no_udf_call(value, definitions, executable, visited, depth + 1)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn require_no_filter_join(operators: &[Value], depth: usize) -> Result<()> {
    nesting(depth)?;
    let mut fanout = HashMap::<u64, usize>::new();
    let mut joins = HashSet::new();
    for node in operators {
        for input in array(node, "inputs")? {
            *fanout.entry(wire_port(input)?.operator).or_default() += 1;
        }
        match text(node, "class")? {
            "DBSPJoinOperator" | "DBSPLeftJoinOperator" | "DBSPStarJoinOperator" => {
                joins.insert(uint(node, "id")?);
            }
            "DBSPNestedOperator" => {
                require_no_filter_join(array(node, "allOperators")?, depth + 1)?;
            }
            _ => {}
        }
    }
    for node in operators {
        if text(node, "class")? != "DBSPFilterOperator" {
            continue;
        }
        let [input] = array(node, "inputs")? else {
            return Err(Error("filter must have one input".into()));
        };
        let source = wire_port(input)?.operator;
        if fanout.get(&source) == Some(&1) && joins.contains(&source) {
            return Err(Error(
                "FilterJoinVisitor requires a filter-join rewrite".into(),
            ));
        }
    }
    Ok(())
}

fn require_no_lowering(
    operators: &[Value],
    definitions: &Definitions<'_>,
    keep_aggregate_lists: bool,
    depth: usize,
) -> Result<()> {
    nesting(depth)?;
    for node in operators {
        let class = text(node, "class")?;
        let changes = match class {
            "DBSPFlatMapOperator" => {
                definitions.resolve(field(node, "function")?)?["class"] == "DBSPFlatmap"
            }
            "DBSPStreamAggregateOperator" | "DBSPAggregateOperator" => {
                !keep_aggregate_lists && node["function"].is_null()
            }
            "DBSPPartitionedRollingAggregateOperator"
            | "DBSPPartitionedRollingAggregateWithWaterlineOperator" => {
                !keep_aggregate_lists && !node["aggregateList"].is_null()
            }
            "DBSPJoinFilterMapOperator"
            | "DBSPLeftJoinFilterMapOperator"
            | "DBSPStarJoinFilterMapOperator" => !node["filter"].is_null(),
            "DBSPNoopOperator" => true,
            "DBSPNestedOperator" => {
                require_no_lowering(
                    array(node, "allOperators")?,
                    definitions,
                    keep_aggregate_lists,
                    depth + 1,
                )?;
                false
            }
            _ => false,
        };
        if changes {
            return Err(Error(format!(
                "LowerCircuitVisitor requires lowering {class}"
            )));
        }
    }
    Ok(())
}
