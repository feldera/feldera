//! Bounded transport for the experimental resolved-expression fragment.
use feldera_sql_rel_model::{
    ExpressionLimits, LiteralLimits, ValidationLimits, expressions::ExpressionTable,
};
use std::{
    env,
    error::Error,
    fs,
    io::{Read, Write},
    path::Path,
};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: expression_roundtrip INPUT.json OUTPUT_DIR".into());
    }
    const MAX_BYTES: u64 = 16 * 1024 * 1024;
    let mut bytes = Vec::new();
    fs::File::open(&args[0])?
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err("expression arena exceeds 16 MiB".into());
    }
    let table: ExpressionTable = serde_json::from_slice(&bytes)?;
    table.validate(
        ValidationLimits::default(),
        LiteralLimits::default(),
        ExpressionLimits::default(),
    )?;
    let output = Path::new(&args[1]);
    fs::create_dir(output)?;
    let mut file = fs::File::create_new(output.join("expressions.json"))?;
    serde_json::to_writer_pretty(&mut file, &table)?;
    file.flush()?;
    let complete = fs::File::create_new(output.join("complete.json"))?;
    serde_json::to_writer_pretty(
        complete,
        &serde_json::json!({
            "schema_version":1,"complete":true,"expressions":table.expressions.len(),
            "roots":table.roots.len(),"runtime_executed":false,"S1_parity_proven":false
        }),
    )?;
    Ok(())
}
