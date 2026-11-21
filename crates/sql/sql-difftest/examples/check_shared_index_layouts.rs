//! Compare Rust's fixed-consumer groups and column positions with Java's
//! ReplaceSharedIndexes state on streamed PassCorpusOracle invocations.
mod support;
use feldera_sql_ir::{Circuit, CircuitEditor, NodeId};
use feldera_sql_opt::{split_shared_indexes, wide_index_fields};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::error::Error;

#[derive(Deserialize)]
struct Candidate {
    index: u64,
    fixed: bool,
}

#[derive(Serialize)]
struct Layout {
    indexes: Vec<u64>,
    columns: Vec<Vec<usize>>,
}

fn check(invocation: &Value) -> Result<Value, Box<dyn Error>> {
    let candidates: Vec<Vec<Candidate>> =
        serde_json::from_value(invocation["candidate_indexes"].clone())?;
    let circuit = Circuit::from_json(invocation["before"].clone())?;
    let mut next = invocation["names_before"]
        .as_u64()
        .ok_or("missing input name counter")?;
    let mut layouts = Vec::new();
    let mut warnings = Vec::new();
    for group in &candidates {
        let functions: Vec<_> = group
            .iter()
            .map(|member| {
                circuit
                    .node(NodeId::Outer(member.index))
                    .ok_or("unknown candidate index")?
                    .edge("function")
                    .map_err(Box::<dyn Error>::from)
            })
            .collect::<Result<_, _>>()?;
        let fixed: Vec<_> = group.iter().map(|m| m.fixed).collect();
        let groups = split_shared_indexes(&circuit, &functions, &fixed, &mut next, &mut warnings)?
            .ok_or("native splitter requires a Java expression comparison")?;
        for positions in groups {
            let closures: Vec<_> = positions.iter().map(|&i| functions[i]).collect();
            let mut edit = CircuitEditor::new(circuit.clone());
            let fields = wide_index_fields(&mut edit, &closures, &mut next, &mut warnings)?;
            let layout = fields
                .layout(&edit)?
                .ok_or("native column layout requires a Java comparison")?;
            layouts.push(Layout {
                indexes: positions.into_iter().map(|i| group[i].index).collect(),
                columns: layout.positions,
            });
        }
    }
    layouts.sort_by_key(|layout| layout.indexes[0]);
    if json!(layouts) != invocation["wide_indexes"] {
        return Err(format!(
            "different wide indexes: Rust {}, Java {}",
            json!(layouts),
            invocation["wide_indexes"]
        )
        .into());
    }
    Ok(json!({"candidates":candidates.len(),"groups":layouts.len()}))
}

fn main() -> Result<(), Box<dyn Error>> {
    support::check(|record| {
        if record["pass"] != "ReplaceSharedIndexes" {
            return Err("expected a ReplaceSharedIndexes observation".into());
        }
        if let Some(invocation) = record.get("invocation") {
            return check(invocation).map(Some);
        }
        if record["streamed_invocations"] != 1 || record["invocations"] != json!([]) {
            return Err("expected one streamed index replacement".into());
        }
        Ok(None)
    })
}
