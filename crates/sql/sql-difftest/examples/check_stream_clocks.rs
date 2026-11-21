//! Compare decoded operator clocks with the live Java StreamKindsProbe.
mod support;

use feldera_sql_ir::{BodyOrder, Circuit, OutputClocks, OutputPort, StreamKind};
use serde_json::json;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    support::check(|row| {
        let circuit = Circuit::from_json(row["circuit"].clone())?;
        let operators = circuit.operator_order(BodyOrder::Postorder)?;
        let expected = row["expected"].as_array().ok_or("missing Java clocks")?;
        if operators.len() != expected.len() {
            return Err("different operator counts".into());
        }

        let mut clocks = OutputClocks::new(&circuit)?;
        let mut counts = [0; 3];
        for (operator, ports) in operators.into_iter().zip(expected) {
            for (number, name) in ports
                .as_array()
                .ok_or("missing output array")?
                .iter()
                .enumerate()
            {
                let port = OutputPort { operator, number };
                if name.is_null() {
                    if clocks.get(port).is_ok() {
                        return Err(format!("unexpected clock at {port:?}").into());
                    }
                    continue;
                }
                let (expected, count) = match name.as_str() {
                    Some("COLLECTION") => (StreamKind::Collection, 0),
                    Some("DELTA") => (StreamKind::Delta, 1),
                    Some("WATERLINE") => (StreamKind::Waterline, 2),
                    _ => return Err(format!("unknown Java clock {name}").into()),
                };
                let actual = clocks.get(port)?;
                if actual != expected {
                    return Err(format!("{port:?}: expected {expected:?}, found {actual:?}").into());
                }
                counts[count] += 1u64;
            }
        }
        let [collections, deltas, waterlines] = counts;
        Ok(Some(
            json!({ "collections": collections, "deltas": deltas, "waterlines": waterlines }),
        ))
    })
}
