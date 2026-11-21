//! Bounded streaming transport for complete, pre-S1 literal observations.
use feldera_sql_rel_model::{LiteralLimits, TypeTable, ValidationLimits, literals::Literal};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    env,
    error::Error,
    fs,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    Supported,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Record {
    index: usize,
    rex_type: String,
    sql_type: String,
    value_class: String,
    status: Status,
    literal: Literal,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    expanded_search_calls: usize,
    type_table: TypeTable,
    literals: Vec<Record>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: literal_corpus_roundtrip INPUT.jsonl OUTPUT_DIR".into());
    }
    let input = Path::new(&args[0]);
    if fs::metadata(input)?.len() > 1024 * 1024 * 1024 {
        return Err("literal corpus exceeds 1 GiB".into());
    }
    let mut reader = BufReader::new(fs::File::open(input)?);
    let output = Path::new(&args[1]);
    fs::create_dir(output)?;
    let mut writer = BufWriter::new(fs::File::create_new(output.join("cases.jsonl"))?);
    let mut identities = HashSet::new();
    let mut bytes = Vec::new();
    let mut total_bytes = 0_u64;
    let mut literals = 0;
    let mut expanded = 0;
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
            return Err("literal corpus byte limit exceeded".into());
        }
        if identities.len() >= 20_000 {
            return Err("literal corpus case limit exceeded".into());
        }
        let case: Case = serde_json::from_slice(&bytes)?;
        if case.id.is_empty() || case.id.len() > 4096 || !identities.insert(case.id.clone()) {
            return Err("missing, overlarge or duplicate case identity".into());
        }
        if case.type_table.roots.len() != case.literals.len() {
            return Err("literal/type root count mismatch".into());
        }
        let types = case.type_table.validated(ValidationLimits::default())?;
        for (index, record) in case.literals.iter().enumerate() {
            if record.index != index || record.literal.ty != case.type_table.roots[index] {
                return Err("literal order or type identity changed".into());
            }
            if record.rex_type == "SARG" {
                return Err("SARG must be expanded before S1 transport".into());
            }
            types.validate_literal(&record.literal, LiteralLimits::default())?;
        }
        literals += case.literals.len();
        expanded += case.expanded_search_calls;
        serde_json::to_writer(&mut writer, &case)?;
        writeln!(writer)?;
    }
    if identities.is_empty() {
        return Err("empty literal corpus".into());
    }
    writer.flush()?;
    serde_json::to_writer_pretty(
        fs::File::create_new(output.join("complete.json"))?,
        &serde_json::json!({
            "schema_version":1,"complete":true,"cases":identities.len(),"literals":literals,
            "expanded_search_calls":expanded,"runtime_executed":false,"S1_parity_proven":false,
        }),
    )?;
    Ok(())
}
