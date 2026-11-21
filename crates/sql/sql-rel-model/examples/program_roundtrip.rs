//! Bounded corpus transport of shared expression objects and contextual uses.
use feldera_sql_rel_model::{LiteralLimits, ObjectLimits, ValidationLimits, program::Program};
use std::{
    collections::HashSet,
    env,
    error::Error,
    fs,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: program_roundtrip INPUT.jsonl OUTPUT_DIR".into());
    }
    let mut reader = BufReader::new(fs::File::open(&args[0])?);
    let output = Path::new(&args[1]);
    fs::create_dir(output)?;
    let mut writer = BufWriter::new(fs::File::create_new(output.join("cases.jsonl"))?);
    let mut identities = HashSet::new();
    let mut bytes = Vec::new();
    let mut total_bytes = 0_u64;

    let (mut objects, mut environments, mut uses) = (0_u64, 0_u64, 0_u64);
    loop {
        bytes.clear();
        let read = (&mut reader)
            .take(16 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut bytes)?;
        if read == 0 {
            break;
        }
        total_bytes += read as u64;
        if read > 16 * 1024 * 1024 || total_bytes > 1024 * 1024 * 1024 {
            return Err("expression corpus byte limit exceeded".into());
        }
        if identities.len() >= 20_000 {
            return Err("expression corpus case limit exceeded".into());
        }
        let case: Program = serde_json::from_slice(&bytes)?;
        if case.id.is_empty() || case.id.len() > 4096 || !identities.insert(case.id.clone()) {
            return Err("missing, overlarge or duplicate case identity".into());
        }
        case.validate(
            ValidationLimits::default(),
            LiteralLimits::default(),
            ObjectLimits::default(),
        )
        .map_err(|error| format!("{}: {error}", case.id))?;
        objects += case.table.objects.len() as u64;
        environments += case.table.environments.len() as u64;
        uses += case.table.uses.len() as u64;
        serde_json::to_writer(&mut writer, &case)?;
        writeln!(writer)?;
    }
    if identities.is_empty() {
        return Err("empty expression corpus".into());
    }
    writer.flush()?;
    serde_json::to_writer_pretty(
        fs::File::create_new(output.join("complete.json"))?,
        &serde_json::json!({
            "schema_version":1,"complete":true,"cases":identities.len(),
            "objects":objects,"environments":environments,"uses":uses,
            "scope":"ordered typed statements and catalog-backed relations","runtime_executed":false,"S1_parity_proven":false
        }),
    )?;
    Ok(())
}
