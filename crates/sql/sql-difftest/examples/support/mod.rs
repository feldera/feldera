use feldera_sql_ir::parse_json;
use serde_json::{json, Value};
use std::{
    error::Error,
    fs,
    io::{self, BufRead},
    path::PathBuf,
};

/// A streaming oracle can send multiple observations before it finishes one case.
pub fn check(
    mut compare: impl FnMut(&Value) -> Result<Option<Value>, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let failures = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("expected a failure directory")?,
    );
    fs::create_dir(&failures)?;
    let mut mismatches = 0;
    let mut compared = 0;
    for line in io::stdin().lock().lines() {
        let line = line?;
        let record = parse_json(line.as_bytes())?;
        let (details, failure) = match compare(&record) {
            Ok(Some(details)) => (details, None),
            Ok(None) => continue,
            Err(error) => (Value::Null, Some(error.to_string())),
        };
        if failure.is_some() {
            if mismatches < 5 {
                fs::write(failures.join(format!("case-{compared}.json")), line)?;
            }
            mismatches += 1;
        }
        let mut result =
            json!({"id":record["id"], "input_sha256":record["input_sha256"], "failure":failure});
        if !details.is_null() {
            result["details"] = details;
        }
        println!("{result}");
        compared += 1;
    }
    if compared == 0 || mismatches != 0 {
        return Err(format!("{mismatches} mismatches in {compared} cases").into());
    }
    Ok(())
}
