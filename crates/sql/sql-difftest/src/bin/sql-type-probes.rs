//! Deterministic scalar type/coercion probes for the Phase 0 Java golden table.
//! Every argument point is (SQL type, nullability, literal/column origin).

use serde_json::json;
use std::{
    env,
    fs::OpenOptions,
    io::{self, BufWriter, Write},
    process::ExitCode,
};

const TYPES: &[(&str, &str)] = &[
    ("BOOLEAN", "TRUE"),
    ("TINYINT", "127"),
    ("SMALLINT", "32767"),
    ("INTEGER", "2147483647"),
    ("BIGINT", "9223372036854775807"),
    ("REAL", "1.25"),
    ("DOUBLE", "1.25"),
    ("DECIMAL(1,0)", "9"),
    ("DECIMAL(10,2)", "12345678.90"),
    ("DECIMAL(38,10)", "1234567890123456789012345678.1234567890"),
    ("CHAR(1)", "'1'"),
    ("CHAR(5)", "'12.5'"),
    ("VARCHAR(1)", "'1'"),
    ("VARCHAR(5)", "'12.5'"),
    ("DATE", "DATE '2024-02-29'"),
    ("TIMESTAMP", "TIMESTAMP '2024-02-29 12:34:56'"),
];

const UNARY_FUNCTIONS: &[&str] = &[
    "ABS",
    "CEIL",
    "FLOOR",
    "SIGN",
    "SQRT",
    "CBRT",
    "EXP",
    "LN",
    "LOG10",
    "SIN",
    "COS",
    "TAN",
    "ASIN",
    "ACOS",
    "ATAN",
    "COT",
    "DEGREES",
    "RADIANS",
    "ROUND",
    "TRUNCATE",
    "LENGTH",
    "CHAR_LENGTH",
    "OCTET_LENGTH",
    "UPPER",
    "LOWER",
    "TRIM",
    "INITCAP",
    "MD5",
    "REVERSE",
    "ASCII",
];
const BINARY_FUNCTIONS: &[&str] = &[
    "POWER", "MOD", "LOG", "ATAN2", "CONCAT", "LEFT", "RIGHT", "REPEAT", "NULLIF", "COALESCE",
];
const AGGREGATES: &[&str] = &["SUM", "AVG", "MIN", "MAX", "COUNT", "ARRAY_AGG"];
const BINARY_OPERATORS: &[&str] = &["+", "-", "*", "/", "=", "<"];
const SET_OPERATIONS: &[&str] = &["UNION ALL", "INTERSECT ALL", "EXCEPT ALL"];

#[derive(Clone, Copy)]
struct Argument {
    sql_type: &'static str,
    value: &'static str,
    nullable: bool,
    literal: bool,
}

impl Argument {
    fn expression(self, name: &str) -> String {
        if self.literal {
            format!(
                "CAST({} AS {})",
                if self.nullable { "NULL" } else { self.value },
                self.sql_type
            )
        } else {
            name.to_owned()
        }
    }

    fn json(self) -> serde_json::Value {
        json!({"type": self.sql_type, "nullable": self.nullable,
            "origin": if self.literal {"literal"} else {"column"}, "boundary_value": self.value})
    }
}

fn arguments() -> Vec<Argument> {
    let mut result = Vec::new();
    for &(sql_type, value) in TYPES {
        for nullable in [false, true] {
            for literal in [false, true] {
                result.push(Argument {
                    sql_type,
                    value,
                    nullable,
                    literal,
                });
            }
        }
    }
    result
}

struct Generator<W> {
    output: W,
    count: usize,
}

impl<W: Write> Generator<W> {
    fn emit(&mut self, operation: &str, args: &[Argument], query: &str) -> io::Result<()> {
        let columns: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(index, arg)| {
                format!(
                    "{} {}{}",
                    if index == 0 { "a" } else { "b" },
                    arg.sql_type,
                    if arg.nullable { "" } else { " NOT NULL" }
                )
            })
            .collect();
        let program = format!(
            "CREATE TABLE t({});\nCREATE VIEW v AS {query};",
            columns.join(", ")
        );
        let record = json!({"schema_version": 1, "id": self.count, "operation": operation,
            "arguments": args.iter().map(|a| a.json()).collect::<Vec<_>>(), "program": program});
        serde_json::to_writer(&mut self.output, &record)?;
        writeln!(self.output)?;
        self.count += 1;
        Ok(())
    }

    fn generate(&mut self) -> io::Result<()> {
        let points = arguments();
        for function in UNARY_FUNCTIONS.iter().chain(AGGREGATES) {
            for &a in &points {
                self.emit(
                    function,
                    &[a],
                    &format!("SELECT {function}({}) AS result FROM t", a.expression("a")),
                )?;
            }
        }
        for &a in &points {
            for &b in &points {
                let left = a.expression("a");
                let right = b.expression("b");
                for function in BINARY_FUNCTIONS {
                    self.emit(
                        function,
                        &[a, b],
                        &format!("SELECT {function}({left}, {right}) AS result FROM t"),
                    )?;
                }
                for operator in BINARY_OPERATORS {
                    self.emit(
                        operator,
                        &[a, b],
                        &format!("SELECT ({left}) {operator} ({right}) AS result FROM t"),
                    )?;
                }
                // A column predicate prevents folding CASE to one selected arm.
                self.emit(
                    "CASE",
                    &[a, b],
                    &format!(
                        "SELECT CASE WHEN a IS NULL THEN {left} ELSE {right} END AS result FROM t"
                    ),
                )?;
                for operation in SET_OPERATIONS {
                    self.emit(operation, &[a, b], &format!("SELECT {left} AS result FROM t {operation} SELECT {right} AS result FROM t"))?;
                }
            }
        }
        Ok(())
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 1 {
        return Err("Usage: sql-type-probes OUTPUT.jsonl (must not exist)".into());
    }
    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[0])?;
    let mut generator = Generator {
        output: BufWriter::new(output),
        count: 0,
    };
    generator.generate()?;
    generator.output.flush()?;
    println!(
        "{}",
        json!({"schema_version": 1, "probes": generator.count,
        "type_points": TYPES.len(), "argument_points": arguments().len(),
        "scalar_functions": UNARY_FUNCTIONS.len() + BINARY_FUNCTIONS.len(),
        "aggregates": AGGREGATES.len(), "sampling": "full cartesian product of declared argument points"})
    );
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_count_follows_the_declared_cross_product() {
        let mut generator = Generator {
            output: io::sink(),
            count: 0,
        };
        generator.generate().unwrap();
        assert_eq!(generator.count, 84_224);
        assert_eq!(arguments().len(), 64);
    }

    #[test]
    fn literal_nulls_are_typed_and_column_nullability_is_explicit() {
        let a = Argument {
            sql_type: "DECIMAL(38,10)",
            value: "1",
            nullable: true,
            literal: true,
        };
        let b = Argument {
            literal: false,
            nullable: false,
            ..a
        };
        let mut generator = Generator {
            output: Vec::new(),
            count: 0,
        };
        generator
            .emit(
                "+",
                &[a, b],
                &format!("SELECT {} + b FROM t", a.expression("a")),
            )
            .unwrap();
        let row: serde_json::Value = serde_json::from_slice(&generator.output).unwrap();
        let sql = row["program"].as_str().unwrap();
        assert!(sql.contains("a DECIMAL(38,10), b DECIMAL(38,10) NOT NULL"));
        assert!(sql.contains("CAST(NULL AS DECIMAL(38,10)) + b"));
    }
}
