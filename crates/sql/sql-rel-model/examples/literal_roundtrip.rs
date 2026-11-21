//! Fixed-fixture Java/Rust/Java transport with optional scalar type validation.

use feldera_sql_rel_model::{LiteralLimits, TypeTable, ValidationLimits, literals::Literal};
use serde::{Deserialize, Serialize};
use std::{
    env,
    error::Error,
    fs::{self, OpenOptions},
    io::{BufRead, BufWriter, Cursor, Write},
    path::Path,
};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    id: u32,
    type_description: String,
    literal: Literal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    type_table: Option<TypeTable>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: literal_roundtrip INPUT.jsonl OUTPUT_DIR".into());
    }
    let input_path = Path::new(&args[0]);
    if fs::metadata(input_path)?.len() > 16 * 1024 * 1024 {
        return Err("literal fixture input exceeds 16 MiB".into());
    }
    let input = fs::read(input_path)?;
    if input.len() > 16 * 1024 * 1024 {
        return Err("literal fixture input grew beyond 16 MiB".into());
    }
    let output = Path::new(&args[1]);
    fs::create_dir(output)?;
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output.join("cases.jsonl"))?,
    );
    let mut count = 0_u32;
    let mut typed = None;
    for line in Cursor::new(input).lines() {
        let fixture: Fixture = serde_json::from_str(&line?)?;
        if fixture.id != count {
            return Err("literal fixture identities are missing, duplicated or reordered".into());
        }
        let has_type = fixture.type_table.is_some();
        if typed.is_some_and(|previous| previous != has_type) {
            return Err("cannot mix typed and untyped literal fixtures".into());
        }
        typed = Some(has_type);
        if let Some(types) = &fixture.type_table {
            types.validate_literal(
                &fixture.literal,
                ValidationLimits::default(),
                LiteralLimits::default(),
            )?;
        } else {
            fixture.literal.validate_payload(LiteralLimits::default())?;
        }
        serde_json::to_writer(&mut writer, &fixture)?;
        writeln!(writer)?;
        count += 1;
    }
    if count == 0 {
        return Err("empty literal fixture input".into());
    }
    writer.flush()?;
    let completion = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.join("complete.json"))?;
    serde_json::to_writer_pretty(
        completion,
        &serde_json::json!({
            "schema_version": 1, "complete": true, "cases": count,
            "SQL_types_imported": false, "SQL_types_validated": typed.unwrap_or(false),
            "runtime_executed": false,
        }),
    )?;
    Ok(())
}
