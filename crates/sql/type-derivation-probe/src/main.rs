//! Strict bounded JSONL adapter. Oracle columns are never inputs to inference.
use feldera_type_derivation_probe::{
    binding, extras, lowering, model::SqlType, rewritten, translated,
};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::HashSet,
    env,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    process::ExitCode,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Argument {
    #[serde(rename = "type")]
    sql_type: String,
    nullable: bool,
    origin: String,
    boundary_value: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Probe {
    schema_version: u32,
    id: u64,
    operation: String,
    arguments: Vec<Argument>,
    program: String,
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args_os().collect();
    if args.len() != 3 {
        return Err("usage: feldera-type-derivation-probe INPUT.jsonl OUTPUT.jsonl".into());
    }
    let mut input = BufReader::new(File::open(&args[1])?);
    let mut output = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&args[2])?,
    );
    let mut seen = HashSet::new();
    loop {
        let mut line = Vec::new();
        let bytes = input.by_ref().take(65_537).read_until(b'\n', &mut line)?;
        if bytes == 0 {
            break;
        }
        if bytes > 65_536 {
            return Err("probe exceeds 64 KiB".into());
        }
        let probe: Probe = serde_json::from_slice(&line)?;
        if probe.schema_version != 1
            || probe.arguments.is_empty()
            || probe.arguments.len() > 2
            || probe.operation.len() > 64
            || probe.program.len() > 60_000
            || seen.len() >= 100_000
            || !seen.insert(probe.id)
        {
            return Err(format!("invalid, duplicate, or excess probe {}", probe.id).into());
        }
        let types = probe
            .arguments
            .iter()
            .map(|a| {
                if !matches!(a.origin.as_str(), "literal" | "column")
                    || a.boundary_value.len() > 1024
                {
                    return Err("invalid probe argument".to_owned());
                }
                SqlType::parse(&a.sql_type, a.nullable)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let translated = translated::infer(&probe.operation, &types);
        let rewritten = rewritten::infer(&probe.operation, &types);
        let additional = extras::infer(&probe.operation, &types);
        let literal_values = probe
            .arguments
            .iter()
            .map(|argument| {
                (argument.origin == "literal").then_some(argument.boundary_value.as_str())
            })
            .collect::<Vec<_>>();
        let lower = |outcome| {
            lowering::lower_projection(&probe.operation, &types, &literal_values, outcome)
        };
        serde_json::to_writer(
            &mut output,
            &json!({
                "schema_version": 1, "id": probe.id,
                "translated_binding": binding::bind(&probe.operation,&types,&translated),
                "rewritten_binding": binding::bind(&probe.operation,&types,&rewritten),
                "translated_s2_projection": lower(&translated),
                "rewritten_s2_projection": lower(&rewritten),
                "additional_s2_projection": lower(&additional),
                "aggregate_s2_components": lowering::lower_aggregate(
                    &probe.operation, &types, &literal_values, &additional
                ),
                "set_s2_projections": lowering::lower_set_projections(
                    &probe.operation, &types, &literal_values, &additional
                ),
                "translated": translated,
                "rewritten": rewritten,
                "additional": additional,
                "runtime_verified": false
            }),
        )?;
        writeln!(output)?;
    }
    output.flush()?;
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("type probe: {error}");
            ExitCode::FAILURE
        }
    }
}
