//! Actual sqllib oracle for the P0 native BIGINT experiment. Each TSV line is
//! op, left, right; nullable operands are decimal i64 or NULL. Overflow is caught
//! outside sqllib and reported with its exact diagnostic text.
use std::{fs::File, io::Read};

use feldera_sqllib::{minus_i64N_i64N, plus_i64N_i64N, times_i64N_i64N};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    let path = std::env::args_os()
        .nth(1)
        .ok_or("usage: native_integer_reference INPUT.tsv")?;
    File::open(path)?
        .take(1_048_577)
        .read_to_string(&mut input)?;
    if input.len() > 1_048_576 {
        return Err("input exceeds 1 MiB".into());
    }
    // Expected overflow panics are records in this single-threaded test oracle.
    std::panic::set_hook(Box::new(|_| {}));
    for (index, line) in input.lines().enumerate() {
        let mut fields = line.split('\t');
        let operation = fields.next().ok_or("missing operation")?;
        let parse = |field: Option<&str>| -> Result<Option<i64>, Box<dyn std::error::Error>> {
            match field.ok_or("missing operand")? {
                "NULL" => Ok(None),
                value => Ok(Some(value.parse()?)),
            }
        };
        let left = parse(fields.next())?;
        let right = parse(fields.next())?;
        if fields.next().is_some() {
            return Err("extra TSV field".into());
        }
        let function = match operation {
            "add" => plus_i64N_i64N,
            "subtract" => minus_i64N_i64N,
            "multiply" => times_i64N_i64N,
            _ => return Err("unsupported operation".into()),
        };
        let outcome = std::panic::catch_unwind(|| function(left, right));
        let value = match outcome {
            Ok(Some(value)) => value.to_string(),
            Ok(None) => "NULL".into(),
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied());
                if !message
                    .is_some_and(|message| message.contains("causes overflow for type BIGINT"))
                {
                    std::panic::resume_unwind(payload);
                }
                format!("OVERFLOW:{}", message.expect("checked overflow message"))
            }
        };
        println!("{index}\t{value}");
    }
    Ok(())
}
