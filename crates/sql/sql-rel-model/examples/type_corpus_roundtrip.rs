//! Bounded streaming transport of measured corpus type arenas. Every supplied
//! case must have complete type support; a partial audit cannot certify success.
use feldera_sql_rel_model::{TypeTable, ValidationLimits};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    env,
    error::Error,
    fs,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

const MAX_RECORD_BYTES: u64 = 16 * 1024 * 1024;
const MAX_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_CASES: usize = 20_000;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Unsupported {
    class: String,
    #[serde(rename = "type")]
    type_description: String,
    reason: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    type_roots: usize,
    supported_roots: usize,
    unsupported: Vec<Unsupported>,
    type_table: TypeTable,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: type_corpus_roundtrip INPUT.jsonl OUTPUT_DIR".into());
    }
    let input = Path::new(&args[0]);
    if fs::metadata(input)?.len() > MAX_TOTAL_BYTES {
        return Err("type corpus exceeds 1 GiB".into());
    }
    let mut reader = BufReader::new(fs::File::open(input)?);
    let output = Path::new(&args[1]);
    fs::create_dir(output)?;
    let mut writer = BufWriter::new(fs::File::create_new(output.join("cases.jsonl"))?);
    let mut identities = HashSet::new();
    let mut bytes = Vec::new();
    let mut total_bytes = 0;
    let mut total_roots = 0;
    let mut arena_entries = 0;
    loop {
        bytes.clear();
        let read = (&mut reader)
            .take(MAX_RECORD_BYTES + 1)
            .read_until(b'\n', &mut bytes)?;
        if read == 0 {
            break;
        }
        total_bytes += read as u64;
        if read as u64 > MAX_RECORD_BYTES || total_bytes > MAX_TOTAL_BYTES {
            return Err("type corpus byte limit exceeded".into());
        }
        if identities.len() >= MAX_CASES {
            return Err("type corpus case limit exceeded".into());
        }
        let case: Case = serde_json::from_slice(&bytes)?;
        if case.id.is_empty() || case.id.len() > 4096 || !identities.insert(case.id.clone()) {
            return Err("missing, overlarge or duplicate case identity".into());
        }
        if !case.unsupported.is_empty()
            || case.type_roots != case.supported_roots
            || case.supported_roots != case.type_table.roots.len()
        {
            return Err(format!("incomplete type support for {}", case.id).into());
        }
        case.type_table.validate(ValidationLimits::default())?;
        total_roots += case.type_roots;
        arena_entries += case.type_table.types.len();
        serde_json::to_writer(&mut writer, &case)?;
        writeln!(writer)?;
    }
    if identities.is_empty() {
        return Err("empty type corpus".into());
    }
    writer.flush()?;
    let complete = fs::File::create_new(output.join("complete.json"))?;
    serde_json::to_writer_pretty(
        complete,
        &serde_json::json!({
            "schema_version": 1, "complete": true, "cases": identities.len(),
            "type_roots": total_roots, "arena_entries": arena_entries,
            "runtime_executed": false, "S1_parity_proven": false,
        }),
    )?;
    Ok(())
}
