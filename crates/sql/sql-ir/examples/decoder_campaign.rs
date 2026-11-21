//! Batch entry point for the frozen G10 decoder campaign in the devcontainer.
use feldera_sql_ir::{parse_json, Circuit};
use std::io::{self, BufRead, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut output = io::stdout().lock();
    for line in io::stdin().lock().lines() {
        let cases: Vec<String> = serde_json::from_str(&line?)?;
        if !(1..=32).contains(&cases.len()) {
            return Err("expected between one and 32 decoder cases".into());
        }
        let mut accepted = 0;
        for case in &cases {
            let Ok(value) = parse_json(case.as_bytes()) else {
                continue;
            };
            let Ok(circuit) = Circuit::from_json(value) else {
                continue;
            };
            let encoded = circuit.to_json();
            if Circuit::from_json(encoded.clone())?.to_json() != encoded {
                return Err("circuit changed on its second round trip".into());
            }
            accepted += 1;
        }
        writeln!(output, "{} {accepted}", cases.len())?;
        output.flush()?;
    }
    Ok(())
}
