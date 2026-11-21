//! Compare the Rust optimizer tail to both sides of Java's persistent-ID hash pass.
use feldera_sql_difftest::compare_circuits;
use feldera_sql_ir::Circuit;
use feldera_sql_opt::{
    check_hints, circuit_statistics, compact_names, declare_comparators, gen2_passes, tag_regions,
};
use serde_json::Value;
use std::error::Error;
mod support;

fn compare(record: &Value) -> Result<(), Box<dyn Error>> {
    let input = Circuit::from_json(record["input"].clone())?;
    if !check_hints(&input)?.is_empty() {
        return Err("Rust reported a hint conflict on an accepted Java case".into());
    }
    let mut output = declare_comparators(input)?;
    if record["gen2"].as_bool().ok_or("missing engine")? {
        output = gen2_passes(output)?;
    }
    let before_hash = compact_names(output)?.to_json();
    compare_circuits(&record["before_hash"], &before_hash)
        .map_err(|e| format!("before hash: {e}"))?;
    let after_hash = Circuit::from_json(record["after_hash"].clone())?;
    let final_circuit = tag_regions(after_hash)?;
    compare_circuits(&record["output"], &final_circuit.to_json())
        .map_err(|e| format!("after hash: {e}"))?;
    let actual = serde_json::to_value(circuit_statistics(&final_circuit)?)?;
    if actual != record["statistics"] {
        return Err(format!("statistics: Java {}, Rust {actual}", record["statistics"]).into());
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    support::check(|record| {
        compare(record)?;
        Ok(Some(Value::Null))
    })
}
