// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: MIT
// The original permission notice is retained in LICENSE-VMWARE.

//! Find reachable nodes across nested output ports and preserve region slots.

use super::{array, boolean, encode_rewrite, field, nesting, text, uint, wire_port, WirePort};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

const NESTED: &str = "DBSPNestedOperator";

pub(crate) fn prune(wire: &Value, keep_sources: bool) -> Result<Value> {
    prune_with_graph(wire, keep_sources).map(|(circuit, _)| circuit)
}

pub(crate) fn eliminate(request: &Value) -> Result<Value> {
    let wire = field(request, "circuit")?;
    let keep_sources = boolean(request, "keep_sources")?;
    let (circuit, reachable) = prune_with_graph(wire, keep_sources)?;
    let mut unused = Vec::new();
    let mut seen = HashSet::new();
    for source in array(request, "input_order")? {
        let id = source
            .as_u64()
            .ok_or_else(|| Error("invalid input identity".into()))?;
        let node = reachable
            .nodes
            .get(&id)
            .ok_or_else(|| Error("unknown input identity".into()))?;
        if !seen.insert(id) {
            return Err(Error("duplicate input identity".into()));
        }
        match text(node, "class")? {
            "DBSPViewDeclarationOperator" => {}
            "DBSPSourceMapOperator"
            | "DBSPSourceLazyMapOperator"
            | "DBSPSourceMultisetOperator" => {
                if !keep_sources && !reachable.kept.contains(&id) {
                    unused.push(id);
                }
            }
            class => return Err(Error(format!("unsupported dead-code input class {class}"))),
        }
    }
    Ok(json!({"circuit": circuit, "unused_inputs": unused}))
}

fn prune_with_graph(wire: &Value, keep_sources: bool) -> Result<(Value, Reachability<'_>)> {
    let mut graph = Reachability::default();
    let operators = array(wire, "allOperators")?;
    graph.index(operators, keep_sources, 0)?;
    graph.trace()?;
    let mut result = wire.clone();
    result["allOperators"] = graph.filter(operators, 0)?.into();
    Ok((encode_rewrite(wire, &result)?, graph))
}

#[derive(Default)]
struct Reachability<'a> {
    nodes: HashMap<u64, &'a Value>,
    pending: Vec<WirePort>,
    kept: HashSet<u64>,
}

impl<'a> Reachability<'a> {
    fn index(&mut self, operators: &'a [Value], keep_sources: bool, depth: usize) -> Result<()> {
        nesting(depth)?;
        for node in operators {
            let id = uint(node, "id")?;
            if self.nodes.insert(id, node).is_some() {
                return Err(Error(format!("duplicate reachability node {id}")));
            }
            match text(node, "class")? {
                NESTED => self.index(array(node, "allOperators")?, keep_sources, depth + 1)?,
                "DBSPSinkOperator"
                | "DBSPIntegrateTraceRetainKeysOperator"
                | "DBSPIntegrateTraceRetainValuesOperator"
                | "DBSPIntegrateTraceRetainNValuesOperator" => {
                    self.pending.push(WirePort {
                        operator: id,
                        output: 0,
                    });
                }
                "DBSPSourceMapOperator"
                | "DBSPSourceLazyMapOperator"
                | "DBSPSourceMultisetOperator"
                    if keep_sources =>
                {
                    self.kept.insert(id);
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn trace(&mut self) -> Result<()> {
        let mut visited = HashSet::new();
        while let Some(port) = self.pending.pop() {
            if !visited.insert(port) {
                continue;
            }
            let node = self
                .nodes
                .get(&port.operator)
                .ok_or_else(|| Error(format!("unknown reachability node {}", port.operator)))?;
            if node["class"] == NESTED {
                let output = array(node, "internalOutputs")?
                    .get(usize::try_from(port.output).unwrap_or(usize::MAX))
                    .ok_or_else(|| Error("unknown nested output".into()))?;
                self.pending.push(wire_port(output)?);
            }
            if self.kept.insert(port.operator) {
                for input in array(node, "inputs")? {
                    self.pending.push(wire_port(input)?);
                }
            }
        }
        Ok(())
    }

    fn filter(&self, operators: &[Value], depth: usize) -> Result<Vec<Value>> {
        nesting(depth)?;
        let mut kept = Vec::with_capacity(operators.len());
        for node in operators {
            if node["class"] == NESTED {
                kept.push(self.filter_region(node, depth + 1)?);
            } else if self.kept.contains(&uint(node, "id")?) {
                kept.push(node.clone());
            }
        }
        Ok(kept)
    }

    fn filter_region(&self, node: &Value, depth: usize) -> Result<Value> {
        let children = self.filter(array(node, "allOperators")?, depth)?;
        let imports = children
            .iter()
            .filter(|node| node["class"] == "DBSPDeltaOperator")
            .map(|node| {
                let [input] = array(node, "inputs")? else {
                    return Err(Error("delta operator must import one port".into()));
                };
                Ok(input.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let outputs = array(node, "internalOutputs")?
            .iter()
            .map(|port| {
                if port.is_null() || self.kept.contains(&wire_port(port)?.operator) {
                    Ok(port.clone())
                } else {
                    Ok(Value::Null)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let mut region = node.clone();
        region["inputs"] = imports.into();
        region["internalOutputs"] = outputs.into();
        region["allOperators"] = children.into();
        Ok(region)
    }
}
