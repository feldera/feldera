use feldera_connector_probe::{Request, Validator};
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: feldera-connector-probe INPUT.jsonl OUTPUT.jsonl".into());
    }
    let validator = Validator::new()?;
    let mut input = BufReader::new(File::open(&args[0])?);
    let mut output = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&args[1])?,
    );
    let mut ids = HashSet::new();
    loop {
        let mut line = Vec::new();
        // Bound allocation before reading an untrusted line.
        input
            .by_ref()
            .take(2 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut line)?;
        if line.is_empty() {
            break;
        }
        if line.len() > 2 * 1024 * 1024 {
            return Err("probe record exceeds 2 MiB".into());
        }
        let request: Request = serde_json::from_slice(&line)?;
        if !ids.insert(request.id.clone()) {
            return Err("duplicate probe identity".into());
        }
        serde_json::to_writer(&mut output, &validator.validate(request)?)?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    Ok(())
}
