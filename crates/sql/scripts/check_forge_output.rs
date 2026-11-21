//! Build as a circuit-ir example in the pinned Forge checkout.

use circuit_ir::Program;
use std::{env, fs, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    let [directory] = args.as_slice() else {
        return Err("expected a compile_bound_gen2.py output directory".into());
    };
    let directory = Path::new(directory);
    let circuit = fs::read_to_string(directory.join("circuit.json"))?;
    let dataflow = fs::read_to_string(directory.join("lowered/dataflow.json"))?;
    let program = Program::from_ir_json(&circuit, &dataflow)?;
    println!("{} operators", program.len());
    Ok(())
}
