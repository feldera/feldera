//! Standalone driver for one generated catalog circuit and frozen Python fixture.
//!
//! Copies into a disposable crate beside the generated library. It deliberately
//! does not read the expected error: the supervisor compares observed errors.
//! This exercises the real JSON record decoder, not HTTP ingress or its envelope
//! parser. Each table is flushed and completed before the next is submitted.

use anyhow::{Context, Result, ensure};
use feldera_types::format::json::JsonFlavor;
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use static_connectors::{CircuitCatalog, RecordFormat};
use std::{env, fs};

#[derive(Deserialize)]
struct Case {
    id: String,
    fixtures: Vec<Fixture>,
}

#[derive(Deserialize)]
struct Fixture {
    id: String,
    table_name: String,
    records: Vec<Box<RawValue>>,
}

fn output_rows(catalog: &static_connectors::Catalog) -> Result<Value> {
    let mut outputs = Vec::new();
    for (name, handles) in catalog.output_iter() {
        let readers = handles.delta_handle.take_readers();
        let mut cursor = readers
            .whole
            .cursor(RecordFormat::Json(JsonFlavor::Default))?;
        let mut rows = Vec::new();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let mut bytes = Vec::new();
                cursor.serialize_val(&mut bytes)?;
                rows.push(json!({"weight": cursor.weight(), "record": serde_json::from_slice::<Value>(&bytes)?}));
                cursor.step_val();
            }
            cursor.step_key()?;
        }
        outputs.push(json!({"name": name.sql_name(), "rows": rows}));
    }
    Ok(Value::Array(outputs))
}

fn run(case: &Case, workers: usize) -> Result<Value> {
    let (mut circuit, catalog) = match p0_python_runtime_probe::circuit(
        &case.id,
        dbsp::circuit::CircuitConfig::with_workers(workers),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(
                json!({"id": case.id, "observation": {"stage": "construction", "error": error.to_string()},
                "completed_transactions": [], "cleanup_failed": false, "runtime_workers": workers,
                "platform_transport_executed": false}),
            );
        }
    };
    for fixture in &case.fixtures {
        ensure!(
            catalog
                .input_collection_handle(&fixture.table_name.as_str().into())
                .is_some(),
            "registered fixture {} has no generated input",
            fixture.id
        );
    }
    for (_, handles) in catalog.output_iter() {
        handles.enable_count.enable();
    }
    let mut completed = Vec::new();
    let outcome = (|| -> Result<Value> {
        if let Err(error) = circuit.transaction() {
            return Ok(json!({"stage": "initial_transaction", "error": error.to_string()}));
        }
        completed.push(json!({"stage": "initial_transaction", "outputs": output_rows(&catalog)?}));
        for (table_index, fixture) in case.fixtures.iter().enumerate() {
            let handle = catalog
                .input_collection_handle(&fixture.table_name.as_str().into())
                .context("input disappeared after validation")?;
            let mut stream = handle
                .handle
                .configure_deserializer(RecordFormat::Json(JsonFlavor::Default))?;
            for (row_index, row) in fixture.records.iter().enumerate() {
                if let Err(error) = stream.insert(row.get().as_bytes(), &None) {
                    return Ok(json!({"stage": "record_decode", "table": fixture.id,
                        "table_index": table_index, "row_index": row_index, "error": error.to_string()}));
                }
            }
            if let Err(error) = stream.flush() {
                return Ok(json!({"stage": "input_flush", "table": fixture.id,
                    "table_index": table_index, "error": error.to_string()}));
            }
            if let Err(error) = circuit.transaction() {
                return Ok(json!({"stage": "table_transaction", "table": fixture.id,
                    "table_index": table_index, "rows_submitted": fixture.records.len(), "error": error.to_string()}));
            }
            completed.push(json!({"stage": "table_transaction", "table": fixture.id,
                "table_index": table_index, "rows_submitted": fixture.records.len(), "outputs": output_rows(&catalog)?}));
        }
        Ok(json!({"stage": "completed_without_exception"}))
    })();
    // Runtime owns worker and merger threads. Tear it down even when decoding or
    // collection failed; report cleanup errors separately from the observation.
    let cleanup_failed = circuit.kill().is_err();
    Ok(
        json!({"id": case.id, "observation": outcome?, "completed_transactions": completed,
        "cleanup_failed": cleanup_failed, "runtime_workers": workers, "platform_transport_executed": false}),
    )
}

fn main() -> Result<()> {
    let path = env::args()
        .nth(1)
        .context("expected frozen case JSON path")?;
    let case: Case = serde_json::from_slice(&fs::read(path)?)?;
    let workers = env::args()
        .nth(2)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(8);
    ensure!(
        (1..=64).contains(&workers),
        "worker count must be between 1 and 64"
    );
    println!("{}", run(&case, workers)?);
    Ok(())
}
