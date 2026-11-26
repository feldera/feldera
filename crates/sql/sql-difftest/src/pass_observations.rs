//! Compare two PassCorpusOracle streams against the population that produced them.
use crate::files::Error;
use feldera_sql_difftest::compare_circuits;
use feldera_sql_ir::{parse_circuit_json, BodyOrder, Circuit, NodeId};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

mod window_groups;

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
        parse_circuit_json(&self.line)
            .map(Some)
            .map_err(|e| invalid(&e))
    }
}

#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct SharedTraces {
    aliases: BTreeMap<String, u64>,
    #[serde(deserialize_with = "removed_policies")]
    removed: BTreeSet<u64>,
}

// The finder publishes a Java Set. Iteration order can vary across JVMs.
fn removed_policies<'de, D: serde::Deserializer<'de>>(input: D) -> Result<BTreeSet<u64>, D::Error> {
    let items = Vec::<u64>::deserialize(input)?;
    let count = items.len();
    let ids: BTreeSet<_> = items.into_iter().collect();
    if ids.len() != count {
        return Err(serde::de::Error::custom(
            "duplicate discarded retention policy",
        ));
    }
    Ok(ids)
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
    shared_traces: Option<SharedTraces>,
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
    /// Implementation coverage. Both paths must still match at every pass boundary.
    native_clock_filters: Option<usize>,
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
    /// The scan records one conjunct per selected filter in sharing-group order.
    /// Attach it to the exit graph so the comparison follows each JVM's node IDs.
    fn attach_chosen_conjuncts(&mut self) -> Result<(), String> {
        let choices: Vec<(u64, usize)> = serde_json::from_value(
            self.placement
                .state
                .remove("chosen_conjuncts")
                .ok_or("missing chosen_conjuncts")?,
        )
        .map_err(|e| e.to_string())?;
        if self.after.get("chosen_conjuncts").is_some() {
            return Err("exit graph already has a chosen_conjuncts field".into());
        }
        let mut selected = Vec::with_capacity(choices.len());
        if !choices.is_empty() {
            let graph =
                Circuit::from_checkpoint_json(self.after.clone()).map_err(|e| e.to_string())?;
            let mut seen = HashSet::new();
            for (filter, conjunct) in choices {
                if !seen.insert(filter) {
                    return Err(format!("multiple chosen conjuncts for filter {filter}"));
                }
                if graph
                    .node(NodeId::Outer(filter))
                    .and_then(|node| node.operator_kind())
                    != Some(feldera_sql_ir::OperatorKind::Filter)
                {
                    return Err(format!(
                        "window selection refers to unknown filter {filter}"
                    ));
                }
                selected.push(serde_json::json!({
                    "filter": {"operator": {"node": filter}, "outputNumber": 0},
                    "conjunct": conjunct,
                }));
            }
        }
        self.after["chosen_conjuncts"] = Value::Array(selected);
        Ok(())
    }

    /// A compiler can own the weight variable before the first operator uses it.
    /// Compare its binding independently in each graph, including first use at exit.
    fn attach_aggregate_state(&mut self) -> Result<(), String> {
        let state = &mut self.placement.state;
        let Some(weight) = state.remove("weight_id") else {
            return Ok(());
        };
        let weight = weight.as_u64().ok_or("invalid aggregate weight ID")?;
        let input_ids: Vec<u64> = serde_json::from_value(
            state
                .remove("append_only_inputs")
                .ok_or("missing append-only inputs")?,
        )
        .map_err(|e| e.to_string())?;
        let mut inputs: HashSet<_> = input_ids.iter().copied().collect();
        if inputs.len() != input_ids.len() {
            return Err("duplicate append-only input".into());
        }
        let name = state
            .get("weight_name")
            .and_then(Value::as_str)
            .ok_or("missing aggregate weight name")?;
        for (entry, wire) in [(true, &mut self.before), (false, &mut self.after)] {
            let graph = Circuit::from_checkpoint_json(wire.clone()).map_err(|e| e.to_string())?;
            if wire.get("aggregate_weight").is_some() || wire.get("append_only_inputs").is_some() {
                return Err("aggregate state collides with circuit fields".into());
            }
            if let Some(node) = graph.node(NodeId::Inner(weight)) {
                if node.class() != "DBSPVariablePath"
                    || node.data("variable").and_then(Value::as_str) != Some(name)
                {
                    return Err("aggregate weight does not reference its named variable".into());
                }
                wire["aggregate_weight"] = serde_json::json!({"node":weight});
            }
            if entry {
                let mut ports = Vec::with_capacity(inputs.len());
                for operator in graph
                    .operator_order(BodyOrder::Preorder)
                    .map_err(|e| e.to_string())?
                {
                    let NodeId::Outer(id) = operator else {
                        return Err("expected outer operator in append-only analysis".into());
                    };
                    if inputs.remove(&id) {
                        ports.push(serde_json::json!({"operator":{"node":id},"outputNumber":0}));
                    }
                }
                if !inputs.is_empty() {
                    return Err("append-only input outside the aggregate entry graph".into());
                }
                wire["append_only_inputs"] = ports.into();
            }
        }
        Ok(())
    }

    /// Reader IDs belong to this entry graph. Resolve them with its operator
    /// references so allocator differences cannot hide a changed selection.
    fn attach_filter_readers(&mut self) -> Result<(), String> {
        let Some(readers) = self.placement.state.remove("filter_readers") else {
            return Ok(());
        };
        let readers: Vec<u64> = serde_json::from_value(readers).map_err(|e| e.to_string())?;
        let graph = self
            .before
            .as_object_mut()
            .ok_or("expected an entry graph")?;
        if graph.contains_key("filter_readers") {
            return Err("entry graph already has a filter_readers field".into());
        }
        let ports = readers
            .into_iter()
            .map(|id| serde_json::json!({"outputNumber":0,"operator":{"node":id}}))
            .collect();
        graph.insert("filter_readers".into(), Value::Array(ports));
        Ok(())
    }

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
        let graph =
            Circuit::from_checkpoint_json(self.before.clone()).map_err(|e| e.to_string())?;
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

fn compare_invocation(
    pass: &str,
    i: usize,
    mut left: Invocation,
    mut right: Invocation,
) -> Result<(), String> {
    if pass == "Scan" {
        left.attach_chosen_conjuncts()?;
        right.attach_chosen_conjuncts()?;
    }
    if pass == "FindWindowGroups" {
        window_groups::attach(&mut left)?;
        window_groups::attach(&mut right)?;
    }
    left.attach_aggregate_state()?;
    right.attach_aggregate_state()?;
    left.attach_projection_state()?;
    right.attach_projection_state()?;
    left.attach_filter_readers()?;
    right.attach_filter_readers()?;
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
    compare_invocation(pass, index, left.invocation, right.invocation)
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
    if control.native_clock_filters.unwrap_or(0) != 0
        || (candidate.native_clock_filters.is_some() && pass != "RewriteNow")
    {
        return Err("unexpected native window filter coverage".into());
    }
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
        compare_invocation(pass, i, left, right)?;
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

    #[test]
    fn discarded_policies_compare_by_membership() {
        let traces = |removed| {
            serde_json::from_value::<SharedTraces>(json!({
                "aliases": {"8": 3}, "removed": removed,
            }))
        };
        let left = traces(vec![4, 7]).unwrap();
        assert!(left == traces(vec![7, 4]).unwrap());
        assert!(left != traces(vec![7, 5]).unwrap());
        assert!(traces(vec![4, 7, 7]).is_err());
    }

    #[test]
    fn aggregate_state_tracks_the_selected_input_and_the_first_weight_use() {
        fn observed(input: u64, weight: u64) -> Invocation {
            let mut graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPMapOperator","id":input,"inputs":[]},
                {"class":"DBSPMapOperator","id":input+1,"inputs":[]}
            ]});
            let before = graph.clone();
            graph["allOperators"][1]["function"] = json!({
                "class":"DBSPVariablePath","id":weight,"variable":"w",
                "type":{"class":"DBSPTypeWeight","id":weight+1}
            });
            serde_json::from_value(json!({
                "before":before,"after":graph,"container":"ExpandAggregates","index":1,
                "iteration":0,"instance":0,"names_before":0,"names_after":0,"diagnostics":[],
                "weight_id":weight,"weight_name":"w","append_only_inputs":[input]
            }))
            .unwrap()
        }
        assert!(
            compare_invocation("ExpandAggregates", 0, observed(7, 11), observed(27, 31)).is_ok()
        );
        let mut wrong_weight = observed(27, 31);
        wrong_weight
            .placement
            .state
            .insert("weight_id".into(), 99.into());
        assert!(compare_invocation("ExpandAggregates", 0, observed(7, 11), wrong_weight).is_err());
        let mut wrong_input = observed(27, 31);
        wrong_input
            .placement
            .state
            .insert("append_only_inputs".into(), json!([28]));
        assert!(compare_invocation("ExpandAggregates", 0, observed(7, 11), wrong_input).is_err());
    }

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
        assert!(compare_invocation(
            "ReplaceCommonProjections",
            0,
            shared_projection(9, 2, 1),
            shared_projection(19, 12, 11)
        )
        .is_ok());
        let mut copied = shared_projection(19, 12, 11);
        copied
            .placement
            .state
            .get_mut("projection_closures")
            .unwrap()["outputs"]["19"]["type"] = json!({"class":"DBSPTypeBool","id":13});
        assert!(compare_invocation(
            "ReplaceCommonProjections",
            0,
            shared_projection(9, 2, 1),
            copied
        )
        .is_err());
    }

    #[test]
    fn window_choices_compare_filter_identity_and_conjunct_position() {
        let scan = |id: u64, choices: Value| -> Invocation {
            let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPFilterOperator","id":id,"inputs":[]},
                {"class":"DBSPFilterOperator","id":id+1,"inputs":[]}
            ]});
            serde_json::from_value(json!({
                "before":graph,"after":graph,"container":"Conditional","index":0,"iteration":0,
                "instance":0,"names_before":0,"names_after":0,"diagnostics":[],"chosen_conjuncts":choices
            })).unwrap()
        };
        let expected = || scan(7, json!([[7, 1], [8, 0]]));
        assert!(
            compare_invocation("Scan", 0, expected(), scan(19, json!([[19, 1], [20, 0]]))).is_ok()
        );
        for choices in [
            json!([[19, 0], [20, 1]]),
            json!([[20, 0], [19, 1]]),
            json!([[19, 1], [19, 0]]),
            json!([[21, 1]]),
        ] {
            assert!(compare_invocation("Scan", 0, expected(), scan(19, choices)).is_err());
        }
    }

    #[test]
    fn filter_readers_compare_in_entry_graph_order() {
        fn observed(first: u64, second: u64, readers: &[u64]) -> Invocation {
            let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPMapOperator","id":first,"inputs":[]},
                {"class":"DBSPMapOperator","id":second,"inputs":[]}
            ]});
            serde_json::from_value(json!({
                "before":graph,"after":graph,"container":"TrimFilters","index":1,"iteration":0,
                "instance":0,"names_before":0,"names_after":0,"diagnostics":[],"filter_readers":readers
            })).unwrap()
        }
        let original = || observed(5, 8, &[5, 8]);
        assert!(
            compare_invocation("TrimFilters", 0, original(), observed(51, 12, &[51, 12])).is_ok()
        );
        assert!(
            compare_invocation("TrimFilters", 0, original(), observed(51, 12, &[12, 51])).is_err()
        );
        assert!(
            compare_invocation("TrimFilters", 0, original(), observed(51, 12, &[51, 99])).is_err()
        );
    }
}
