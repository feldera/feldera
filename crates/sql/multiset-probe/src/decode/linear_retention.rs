//! Select retainers and schedule the new waterline dependencies.

use super::{array, field, uint, wire_port};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};

struct Dependencies {
    successors: Vec<Vec<usize>>,
    incoming: Vec<usize>,
}

impl Dependencies {
    fn new(count: usize) -> Self {
        Self {
            successors: vec![Vec::new(); count],
            incoming: vec![0; count],
        }
    }

    fn add(&mut self, source: usize, destination: usize) {
        self.successors[source].push(destination);
        self.incoming[destination] += 1;
    }

    fn sort(mut self) -> Result<Vec<usize>> {
        let mut ready: VecDeque<_> = self
            .incoming
            .iter()
            .enumerate()
            .filter_map(|(index, degree)| (*degree == 0).then_some(index))
            .collect();
        let mut order = Vec::with_capacity(self.incoming.len());
        while let Some(source) = ready.pop_front() {
            order.push(source);
            for &destination in &self.successors[source] {
                self.incoming[destination] -= 1;
                if self.incoming[destination] == 0 {
                    ready.push_back(destination);
                }
            }
        }
        if order.len() != self.incoming.len() {
            return Err(Error(
                "waterline dependencies create a circuit cycle".into(),
            ));
        }
        Ok(order)
    }
}

pub(crate) fn plan(input: &Value) -> Result<Value> {
    let circuit = field(input, "circuit")?;
    feldera_sql_difftest::compare_circuits(circuit, circuit)
        .map_err(|error| Error(error.to_string()))?;
    let operators = array(circuit, "allOperators")?;
    let ids = operators
        .iter()
        .map(|op| uint(op, "id"))
        .collect::<Result<Vec<_>>>()?;
    let indices: HashMap<_, _> = ids.iter().copied().zip(0..ids.len()).collect();
    let index = |id| {
        indices.get(&id).copied().ok_or_else(|| {
            Error(format!(
                "unknown root operator {id} in waterline dependencies"
            ))
        })
    };
    let mut graph = Dependencies::new(ids.len());
    for (destination, op) in operators.iter().enumerate() {
        if op["class"] != "DBSPDeltaOperator" {
            for input in array(op, "inputs")? {
                graph.add(index(wire_port(input)?.operator)?, destination);
            }
        }
    }
    let mut candidates = Vec::new();
    let mut waterline_edges = Vec::new();
    for (aggregate, op) in operators.iter().enumerate() {
        if op["class"] != "DBSPAggregateLinearPostprocessOperator" {
            continue;
        }
        let mut retainers = Vec::new();
        for &retainer in &graph.successors[aggregate] {
            if operators[retainer]["class"] != "DBSPIntegrateTraceRetainKeysOperator" {
                continue;
            }
            let [_data, waterline] = array(&operators[retainer], "inputs")? else {
                return Err(Error(
                    "key retention needs data and waterline inputs".into(),
                ));
            };
            waterline_edges.push((index(wire_port(waterline)?.operator)?, aggregate));
            retainers.push(retainer);
        }
        if !retainers.is_empty() {
            candidates.push((aggregate, retainers));
        }
    }
    if candidates.is_empty() {
        return Ok(Value::Null);
    }
    for (source, destination) in waterline_edges {
        graph.add(source, destination);
    }
    for field in ["input_order", "output_order"] {
        let ordered = array(input, field)?;
        for pair in ordered.windows(2) {
            let endpoint = |value: &Value| {
                index(
                    value
                        .as_u64()
                        .ok_or_else(|| Error("invalid IO operator identity".into()))?,
                )
            };
            graph.add(endpoint(&pair[0])?, endpoint(&pair[1])?);
        }
    }
    let order = graph.sort()?;
    let mut rank = vec![0; ids.len()];
    for (position, &node) in order.iter().enumerate() {
        rank[node] = position;
    }
    let replacements = candidates
        .into_iter()
        .map(|(aggregate, retainers)| {
            // Java rebuilds the successor list in the new topological order.
            let first = retainers
                .into_iter()
                .min_by_key(|&node| rank[node])
                .expect("candidate has a retainer");
            json!({"aggregate": ids[aggregate], "retainer": ids[first]})
        })
        .collect::<Vec<_>>();
    Ok(
        json!({"order": order.into_iter().map(|index| ids[index]).collect::<Vec<_>>(), "replacements": replacements}),
    )
}
