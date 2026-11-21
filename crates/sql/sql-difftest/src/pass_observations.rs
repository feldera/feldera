//! Compare two PassCorpusOracle streams against the population that produced them.
use crate::files::Error;
use feldera_sql_difftest::{compare_circuits, parse_json};
use feldera_sql_ir::{BodyOrder, Circuit, NodeId};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

struct Records<'a> {
    path: &'a Path,
    input: BufReader<File>,
    line: Vec<u8>,
    number: usize,
}

impl<'a> Records<'a> {
    fn open(path: &'a Path) -> Result<Self, Error> {
        let file =
            File::open(path).map_err(|e| Error::Input(format!("{}: {e}", path.display())))?;
        Ok(Self {
            path,
            input: BufReader::new(file),
            line: Vec::new(),
            number: 0,
        })
    }

    fn next(&mut self) -> Result<Option<Value>, Error> {
        self.line.clear();
        self.number += 1;
        let invalid = |e: &dyn std::fmt::Display| {
            Error::Input(format!("{} line {}: {e}", self.path.display(), self.number))
        };
        if self
            .input
            .read_until(b'\n', &mut self.line)
            .map_err(|e| invalid(&e))?
            == 0
        {
            return Ok(None);
        }
        parse_json(&self.line).map(Some).map_err(|e| invalid(&e))
    }
}

#[derive(Deserialize, PartialEq)]
struct Placement {
    container: String,
    index: u64,
    iteration: u64,
    instance: u64,
    names_before: u64,
    names_after: u64,
    diagnostics: Vec<Value>,
    #[serde(flatten)]
    state: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
struct Invocation {
    before: Value,
    after: Value,
    #[serde(flatten)]
    placement: Placement,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Observation {
    id: String,
    input_sha256: String,
    s2_sha256: String,
    gen2: bool,
    pass: String,
    invocations: Vec<Invocation>,
    streamed_invocations: Option<usize>,
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The streaming oracle emits one graph pair per line and confirms the count
/// and the S2 identity in the final record for that program.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Chunk {
    id: String,
    input_sha256: String,
    pass: String,
    index: usize,
    invocation: Invocation,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionState {
    inputs: BTreeMap<u64, Value>,
    outputs: BTreeMap<u64, Value>,
    origin: Option<Value>,
}

impl Invocation {
    /// Projection closures can reference nodes defined in the entry graph and
    /// each other. Compare them in that graph's namespace, in operator order.
    fn attach_projection_state(&mut self) -> Result<(), String> {
        let Some(value) = self.placement.state.get_mut("projection_closures") else {
            return Ok(());
        };
        let mut state: ProjectionState =
            serde_json::from_value(value.take()).map_err(|e| e.to_string())?;
        *value = state.origin.unwrap_or(Value::Null);
        if state.inputs.is_empty() && state.outputs.is_empty() {
            return Ok(());
        }
        let graph = Circuit::from_json(self.before.clone()).map_err(|e| e.to_string())?;
        let order = graph
            .operator_order(BodyOrder::Preorder)
            .map_err(|e| e.to_string())?;
        let mut attached = Vec::new();
        for operator in order {
            let NodeId::Outer(id) = operator else {
                return Err("expected an outer operator".into());
            };
            let input = state.inputs.remove(&id);
            let output = state.outputs.remove(&id);
            if input.is_some() || output.is_some() {
                attached.push(serde_json::json!({
                    "owner": {"operator": {"node": id}, "outputNumber": 0},
                    "input": input, "output": output
                }));
            }
        }
        if !state.inputs.is_empty() || !state.outputs.is_empty() {
            return Err("projection state references an operator outside its entry graph".into());
        }
        if self.before.get("projection_closures").is_some() {
            return Err("entry graph already has a projection_closures field".into());
        }
        self.before["projection_closures"] = Value::Array(attached);
        Ok(())
    }
}

fn compare_invocation(i: usize, mut left: Invocation, mut right: Invocation) -> Result<(), String> {
    left.attach_projection_state()?;
    right.attach_projection_state()?;
    if left.placement != right.placement {
        return Err(format!(
            "invocation {i}: placement, generated names, diagnostics or pass state differs"
        ));
    }
    for (boundary, a, b) in [
        ("entry", &left.before, &right.before),
        ("exit", &left.after, &right.after),
    ] {
        compare_circuits(a, b).map_err(|e| format!("invocation {i} {boundary}: {e}"))?;
    }
    Ok(())
}

fn compare_chunk(pass: &str, source: &Value, rows: [Value; 2], index: usize) -> Result<(), String> {
    let [left, right] = rows.map(serde_json::from_value::<Chunk>);
    let left = left.map_err(|e| format!("invalid control chunk: {e}"))?;
    let right = right.map_err(|e| format!("invalid candidate chunk: {e}"))?;
    for chunk in [&left, &right] {
        if source["id"].as_str() != Some(&chunk.id)
            || source["input_sha256"].as_str() != Some(&chunk.input_sha256)
            || chunk.pass != pass
            || chunk.index != index
        {
            return Err(format!(
                "invalid identity or sequence at invocation {index}"
            ));
        }
    }
    compare_invocation(index, left.invocation, right.invocation)
}

fn compare_case(
    pass: &str,
    source: &Value,
    rows: [Value; 2],
    streamed: usize,
) -> Result<usize, String> {
    let [control, candidate] = rows.map(serde_json::from_value::<Observation>);
    let control = control.map_err(|e| format!("invalid control observation: {e}"))?;
    let candidate = candidate.map_err(|e| format!("invalid candidate observation: {e}"))?;
    for row in [&control, &candidate] {
        if source["id"].as_str() != Some(&row.id)
            || source["input_sha256"].as_str() != Some(&row.input_sha256)
            || source
                .pointer("/options/ioOptions/gen2")
                .and_then(Value::as_bool)
                != Some(row.gen2)
            || row.pass != pass
            || !is_sha256(&row.input_sha256)
            || !is_sha256(&row.s2_sha256)
        {
            return Err("population identity, engine mode, pass name or digest differs".into());
        }
    }
    if control.s2_sha256 != candidate.s2_sha256 {
        return Err("different S2 checkpoints".into());
    }
    if control.streamed_invocations.is_some()
        || candidate.streamed_invocations.is_some()
        || streamed != 0
    {
        for row in [&control, &candidate] {
            if row.streamed_invocations != Some(streamed) || !row.invocations.is_empty() {
                return Err(format!(
                    "expected a summary of {streamed} streamed invocations"
                ));
            }
        }
        return Ok(streamed);
    }
    let count = control.invocations.len();
    if count != candidate.invocations.len() {
        return Err(format!(
            "expected {count} invocations, found {}",
            candidate.invocations.len()
        ));
    }
    for (i, (left, right)) in control
        .invocations
        .into_iter()
        .zip(candidate.invocations)
        .enumerate()
    {
        compare_invocation(i, left, right)?;
    }
    Ok(count)
}

pub fn compare(
    pass: &str,
    population: &Path,
    control: &Path,
    candidate: &Path,
) -> Result<String, Error> {
    let mut population = Records::open(population)?;
    let mut control = Records::open(control)?;
    let mut candidate = Records::open(candidate)?;
    let mut seen = HashSet::new();
    let mut invocations = 0;
    while let Some(row) = population.next()? {
        let id = row["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                Error::Input(format!(
                    "missing case ID at population line {}",
                    population.number
                ))
            })?;
        if !seen.insert(id.to_owned()) {
            return Err(Error::Input(format!("duplicate case {id}")));
        }
        let required = |stream: &mut Records<'_>| {
            stream.next()?.ok_or_else(|| {
                Error::Difference(format!("{} ended before {id}", stream.path.display()))
            })
        };
        let difference = |e| Error::Difference(format!("{id}: {e}"));
        let mut streamed = 0;
        loop {
            let pair = [required(&mut control)?, required(&mut candidate)?];
            if pair[0].get("invocation").is_none() {
                invocations += compare_case(pass, &row, pair, streamed).map_err(difference)?;
                break;
            }
            compare_chunk(pass, &row, pair, streamed).map_err(difference)?;
            streamed += 1;
        }
    }
    if control.next()?.is_some() || candidate.next()?.is_some() {
        return Err(Error::Difference(
            "extra observations after the population ended".into(),
        ));
    }
    if invocations == 0 {
        return Err(Error::Difference(format!(
            "no invocations of {pass} in the population"
        )));
    }
    Ok(format!(
        "{pass} matched at {invocations} invocations across {} cases",
        seen.len()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn shared_projection(owner: u64, closure: u64, ty: u64) -> Invocation {
        let graph = json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [],
            "allOperators": [{"class":"DBSPConstantOperator", "id":owner, "inputs":[],
                              "outputType":{"class":"DBSPTypeBool","id":ty}}]
        });
        serde_json::from_value(json!({
            "before":graph, "after":graph, "container":"Optimizer", "index":0,
            "iteration":0, "instance":0, "names_before":0,"names_after":1,"diagnostics":[],
            "projection_closures":{
                "outputs":{owner.to_string():{"class":"DBSPClosureExpression","id":closure,"type":{"node":ty}}},
                "inputs":{}
            }
        })).unwrap()
    }

    #[test]
    fn projection_state_preserves_sharing_with_its_circuit_after_renumbering() {
        assert!(
            compare_invocation(0, shared_projection(9, 2, 1), shared_projection(19, 12, 11))
                .is_ok()
        );
        let mut copied = shared_projection(19, 12, 11);
        copied
            .placement
            .state
            .get_mut("projection_closures")
            .unwrap()["outputs"]["19"]["type"] = json!({"class":"DBSPTypeBool","id":13});
        assert!(compare_invocation(0, shared_projection(9, 2, 1), copied).is_err());
    }
}
