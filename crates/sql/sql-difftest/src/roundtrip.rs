//! Exercise the production circuit codec with the independent P0 oracle.

use crate::files::{read_circuit, Error};
use feldera_sql_difftest::compare_circuits;
use feldera_sql_ir::Circuit;
use std::{
    io::{self, Write},
    path::Path,
};

pub(crate) fn verify(document: &mut serde_json::Value) -> Result<(), Error> {
    let circuit = match document.get_mut("circuit") {
        Some(payload) => payload,
        None => document,
    };
    let decoded =
        Circuit::from_checkpoint_json(circuit.clone()).map_err(|e| Error::Difference(e.to_string()))?;
    decoded
        .provenance()
        .map_err(|e| Error::Difference(e.to_string()))?;
    let encoded = decoded.to_json();
    compare_circuits(circuit, &encoded).map_err(|e| Error::Difference(e.to_string()))?;
    if *circuit != encoded {
        return Err(Error::Difference(
            "production codec changed a wire field or definition site".into(),
        ));
    }
    *circuit = encoded;
    Ok(())
}

pub(crate) fn run(path: &Path) -> Result<(), Error> {
    let mut document = read_circuit(path)?;
    verify(&mut document)?;
    let mut output = io::BufWriter::new(io::stdout().lock());
    serde_json::to_writer(&mut output, &document).map_err(|e| Error::Input(e.to_string()))?;
    output.flush().map_err(|e| Error::Input(e.to_string()))
}
