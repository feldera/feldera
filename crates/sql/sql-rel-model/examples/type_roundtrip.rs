//! Java/Rust/Java ordered type-arena transport. Byte bounds precede decoding.
use feldera_sql_rel_model::{TypeTable, ValidationLimits};
use std::{env, error::Error, fs, io::Write, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: type_roundtrip INPUT.json OUTPUT_DIR".into());
    }
    let input = Path::new(&args[0]);
    if fs::metadata(input)?.len() > 16 * 1024 * 1024 {
        return Err("type arena exceeds 16 MiB".into());
    }
    let bytes = fs::read(input)?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err("type arena grew beyond 16 MiB".into());
    }
    let types: TypeTable = serde_json::from_slice(&bytes)?;
    types.validate(ValidationLimits::default())?;
    let output = Path::new(&args[1]);
    fs::create_dir(output)?;
    let mut file = fs::File::create_new(output.join("types.json"))?;
    serde_json::to_writer_pretty(&mut file, &types)?;
    file.flush()?;
    let complete = fs::File::create_new(output.join("complete.json"))?;
    serde_json::to_writer_pretty(
        complete,
        &serde_json::json!({
            "schema_version": 1, "complete": true, "types": types.types.len(),
            "roots": types.roots.len(), "runtime_executed": false,
        }),
    )?;
    Ok(())
}
