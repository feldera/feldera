//! Analyze simple source projections and fully used stream-join values.

use super::{
    advance_name, array, field, nesting, text, uint, wire_port, Definitions, GENERATED_NAME,
};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub(crate) fn analyze(circuit: &Value) -> Result<Value> {
    let defs = Definitions::read(circuit)?;
    let operators = array(circuit, "allOperators")?;
    let mut by_id = HashMap::new();
    let mut fanout = HashMap::new();
    for node in operators {
        by_id.insert(uint(node, "id")?, node);
        for input in array(node, "inputs")? {
            *fanout.entry(wire_port(input)?.operator).or_insert(0_usize) += 1;
        }
    }
    let mut next = uint(circuit, GENERATED_NAME)?;
    for operator in operators {
        let kind = text(operator, "class")?;
        match kind {
            "DBSPMapOperator" | "DBSPMapIndexOperator" => {}
            "DBSPStreamJoinOperator" => {
                require_full_join(operator, &defs)?;
                continue;
            }
            "DBSPNestedOperator"
            | "DBSPFlatMapOperator"
            | "DBSPAggregateLinearPostprocessOperator"
            | "DBSPIndexedTopKOperator"
            | "DBSPStreamAggregateOperator"
            | "DBSPAsofJoinOperator"
            | "DBSPStreamJoinIndexOperator"
            | "DBSPJoinOperator"
            | "DBSPLeftJoinOperator"
            | "DBSPStarJoinOperator"
            | "DBSPJoinIndexOperator"
            | "DBSPStarJoinIndexOperator"
            | "DBSPLeftJoinIndexOperator"
            | "DBSPJoinFilterMapOperator"
            | "DBSPLeftJoinFilterMapOperator" => return Err(outside_scope()),
            _ => continue,
        }
        let [input] = array(operator, "inputs")? else {
            return Err(outside_scope());
        };
        let id = wire_port(input)?.operator;
        let source = *by_id.get(&id).ok_or_else(outside_scope)?;
        if source["class"] != "DBSPSourceMultisetOperator" || fanout[&id] != 1 {
            return Err(outside_scope());
        }
        let closure = defs.resolve(field(operator, "function")?)?;
        let [parameter] = array(closure, "parameters")? else {
            return Err(outside_scope());
        };
        let parameter = defs.resolve(parameter)?;
        let body = defs.resolve(field(closure, "body")?)?;
        let mut reads = Vec::new();
        let projection = collect_reads(body, &defs, &mut reads, 0)?;
        let name = text(parameter, "name")?;
        if reads.iter().any(|&(variable, _)| variable != name) {
            return Err(outside_scope());
        }
        let width = tuple_width(parameter, &defs)?;
        let fields = used_fields(&reads, name, width)?;
        if fields.len() == width {
            continue;
        }
        if kind != "DBSPMapOperator"
            || !projection
            || body["class"] != "DBSPTupleExpression"
            || reads.len() != fields.len()
            || !reads.windows(2).all(|pair| pair[0].1 < pair[1].1)
        {
            return Err(outside_scope());
        }
        for expression in array(body, "fields")? {
            if defs.resolve(expression)?["class"] != "DBSPFieldExpression" {
                return Err(outside_scope());
            }
        }
        let columns = array(field(source, "metadata")?, "columnMetadata")?;
        if columns.len() != width
            || columns
                .iter()
                .enumerate()
                .any(|(index, column)| !fields.contains(&index) && column["isPrimaryKey"] != true)
        {
            return Err(outside_scope());
        }
        // A trial projection consumes one name. Its fields already match
        // this map's projection, so the rewrite keeps the original closure.
        next = advance_name(next, 1)?;
    }
    let mut result = circuit.clone();
    result[GENERATED_NAME] = next.into();
    Ok(result)
}

fn require_full_join<'a>(operator: &'a Value, defs: &Definitions<'a>) -> Result<()> {
    let closure = defs.resolve(field(operator, "function")?)?;
    let [key, left, right] = array(closure, "parameters")? else {
        return Err(outside_scope());
    };
    let key = defs.resolve(key)?;
    let left = defs.resolve(left)?;
    let right = defs.resolve(right)?;
    let mut reads = Vec::new();
    collect_reads(defs.resolve(field(closure, "body")?)?, defs, &mut reads, 0)?;
    let names = [
        text(key, "name")?,
        text(left, "name")?,
        text(right, "name")?,
    ];
    if reads.iter().any(|(name, _)| !names.contains(name)) {
        return Err(outside_scope());
    }
    for parameter in [left, right] {
        let width = tuple_width(parameter, defs)?;
        if used_fields(&reads, text(parameter, "name")?, width)?.len() != width {
            return Err(outside_scope());
        }
    }
    Ok(())
}

fn collect_reads<'a>(
    node: &'a Value,
    defs: &Definitions<'a>,
    reads: &mut Vec<(&'a str, usize)>,
    depth: usize,
) -> Result<bool> {
    nesting(depth)?;
    match text(node, "class")? {
        "DBSPTupleExpression" | "DBSPRawTupleExpression" => {
            let mut projection = true;
            for field in array(node, "fields")? {
                projection &= collect_reads(defs.resolve(field)?, defs, reads, depth + 1)?;
            }
            Ok(projection)
        }
        "DBSPFieldExpression" => {
            let deref = defs.resolve(field(node, "expression")?)?;
            let variable = defs.resolve(field(deref, "expression")?)?;
            if deref["class"] != "DBSPDerefExpression" || variable["class"] != "DBSPVariablePath" {
                return Err(outside_scope());
            }
            let index = usize::try_from(uint(node, "fieldNo")?).map_err(|_| outside_scope())?;
            reads.push((text(variable, "variable")?, index));
            Ok(true)
        }
        "DBSPCastExpression" => {
            collect_reads(
                defs.resolve(field(node, "source")?)?,
                defs,
                reads,
                depth + 1,
            )?;
            Ok(false)
        }
        _ => Err(outside_scope()),
    }
}

fn tuple_width<'a>(parameter: &'a Value, defs: &Definitions<'a>) -> Result<usize> {
    let reference = defs.resolve(field(parameter, "type")?)?;
    let tuple = defs.resolve(field(reference, "type")?)?;
    if reference["class"] != "DBSPTypeRef" || tuple["class"] != "DBSPTypeTuple" {
        return Err(outside_scope());
    }
    Ok(array(tuple, "tupFields")?.len())
}

fn used_fields(reads: &[(&str, usize)], name: &str, width: usize) -> Result<HashSet<usize>> {
    let used: HashSet<_> = reads
        .iter()
        .filter_map(|&(variable, index)| (variable == name).then_some(index))
        .collect();
    if used.iter().any(|&index| index >= width) {
        return Err(outside_scope());
    }
    Ok(used)
}

fn outside_scope() -> Error {
    Error("unused-field optimization outside the measured projection slice".into())
}
