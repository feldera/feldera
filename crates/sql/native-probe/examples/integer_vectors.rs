//! TSV adapter for the independent sqllib comparison. Input: op, left, right;
//! operands are decimal i64 or NULL. Output: line index and value/NULL/OVERFLOW.
use std::{fs::File, io::Read};

use feldera_native_probe::{Arithmetic, Batch, CompiledProgram, Error, Expr, Program};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let programs = [Arithmetic::Add, Arithmetic::Subtract, Arithmetic::Multiply].map(|op| {
        let program = Program::new(
            2,
            vec![
                Expr::Column(0),
                Expr::Column(1),
                Expr::Arithmetic {
                    op,
                    left: 0,
                    right: 1,
                },
            ],
        )?;
        CompiledProgram::compile(&program)
    });
    let programs: Vec<_> = programs.into_iter().collect::<Result<_, _>>()?;
    let mut input = String::new();
    let path = std::env::args_os()
        .nth(1)
        .ok_or("usage: integer_vectors INPUT.tsv")?;
    File::open(path)?
        .take(1_048_577)
        .read_to_string(&mut input)?;
    if input.len() > 1_048_576 {
        return Err("input exceeds 1 MiB".into());
    }
    for (index, line) in input.lines().enumerate() {
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 3 {
            return Err(format!("line {index}: expected exactly three TSV fields").into());
        }
        let operation = match fields[0] {
            "add" => 0,
            "subtract" => 1,
            "multiply" => 2,
            _ => return Err(format!("line {index}: unsupported operation").into()),
        };
        let operands = fields[1..]
            .iter()
            .map(|value| {
                if *value == "NULL" {
                    Ok(None)
                } else {
                    value.parse::<i64>().map(Some)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch = Batch::from_rows(2, &[operands])?;
        let value = match programs[operation].evaluate(&batch) {
            Ok(values) => values[0].map_or_else(|| "NULL".into(), |v| v.to_string()),
            Err(error @ Error::Overflow { row: 0, .. }) => format!("OVERFLOW:{error}"),
            Err(error) => return Err(error.into()),
        };
        println!("{index}\t{value}");
    }
    Ok(())
}
