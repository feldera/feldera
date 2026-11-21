use feldera_parser_probe::binder_probe::{
    Submission, bind_constant_program, bind_generated_projection, bind_scalar_projection,
    bind_scalar_submissions, bind_table_program,
};
use feldera_sql_rel_model::program::Program;
use serde::Deserialize;
use serde_json::json;
use std::{
    borrow::Cow,
    collections::HashSet,
    env,
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
};

#[derive(Deserialize)]
struct Record {
    schema_version: u32,
    id: u64,
    program: String,
    #[serde(default)]
    submissions: Vec<Submission>,
}

enum Output {
    Binding,
    Schema,
    FRelExpression { include_schema: bool },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    let (input, output, emit) = match args.as_slice() {
        [mode, prelude, input, output] if mode == "--constant-program" => {
            return write_programs(prelude, input, output, ProgramScope::ConstantView);
        }
        [mode, prelude, input, output] if mode == "--table-program" => {
            return write_programs(prelude, input, output, ProgramScope::TableProjection);
        }
        [mode, prelude, input, output] if mode == "--table-program-census" => {
            return write_programs(prelude, input, output, ProgramScope::TableCensus);
        }
        [input, output] => (input, output, Output::Binding),
        [mode, input, output] if mode == "--include-schema" => (input, output, Output::Schema),
        [mode, input, output] if mode == "--include-frel-expression" => {
            (input, output, Output::FRelExpression { include_schema: false })
        }
        [mode, input, output] if mode == "--include-scoped-schema" => {
            (input, output, Output::FRelExpression { include_schema: true })
        }
        _ => {
            return Err(concat!(
                "usage: a-binder-probe [--include-schema|--include-frel-expression|--include-scoped-schema] INPUT.jsonl OUTPUT.jsonl\n",
                "       a-binder-probe [--constant-program|--table-program|--table-program-census] PRELUDE.json INPUT.jsonl OUTPUT.jsonl",
            ).into())
        }
    };
    let mut input = BufReader::new(File::open(input)?);
    let mut output = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)?,
    );
    let mut seen = HashSet::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        let length = input.by_ref().take(65_537).read_until(b'\n', &mut line)?;
        if length == 0 {
            break;
        }
        if length > 65_536 || seen.len() >= 100_000 {
            return Err("manifest exceeds probe limits".into());
        }
        let record: Record = serde_json::from_slice(&line)?;
        if record.schema_version != 1 || !seen.insert(record.id) {
            return Err("invalid probe version or duplicate probe ID".into());
        }
        let projection = match emit {
            Output::FRelExpression { .. } if !record.submissions.is_empty() => {
                bind_scalar_submissions(&record.submissions)
                    .map(|(bound, source)| (bound, Cow::Owned(source)))
            }
            Output::FRelExpression { .. } => bind_scalar_projection(&record.program)
                .map(|bound| (bound, Cow::Borrowed(record.program.as_str()))),
            _ => bind_generated_projection(&record.program)
                .map(|bound| (bound, Cow::Borrowed(record.program.as_str()))),
        };
        let result = match projection {
            Ok((projection, source)) => {
                let mut result =
                    json!({"id": record.id, "accepted": true, "projection": projection});
                match emit {
                    Output::Binding => {}
                    Output::Schema => result["schema"] = projection.generated_schema(),
                    Output::FRelExpression { include_schema } => {
                        match projection.frel_expression(&source) {
                            Ok(fragment) => {
                                result["frel_expression"] = serde_json::to_value(fragment)?;
                                if include_schema {
                                    result["schema"] = projection.scoped_schema();
                                }
                            }
                            Err(refusal) => result["frel_refusal"] = serde_json::to_value(refusal)?,
                        }
                    }
                }
                result
            }
            Err(error) => json!({"id": record.id, "accepted": false, "diagnostic": error}),
        };
        serde_json::to_writer(&mut output, &result)?;
        writeln!(output)?;
    }
    output.flush()?;
    Ok(())
}

#[derive(Deserialize)]
struct ProgramInput {
    id: String,
    submissions: Vec<Submission>,
}

#[derive(Clone, Copy)]
enum ProgramScope {
    ConstantView,
    TableProjection,
    TableCensus,
}

fn write_programs(
    prelude: &OsStr,
    input: &OsStr,
    output: &OsStr,
    scope: ProgramScope,
) -> Result<(), Box<dyn std::error::Error>> {
    let program: Program = serde_json::from_reader(BufReader::new(File::open(prelude)?))?;
    let mut input = BufReader::new(File::open(input)?);
    let mut output = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)?,
    );
    let mut seen = HashSet::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        let length = input.by_ref().take(65_537).read_until(b'\n', &mut line)?;
        if length == 0 {
            break;
        }
        if length > 65_536 || seen.len() >= 100_000 {
            return Err("manifest exceeds probe limits".into());
        }
        let record: ProgramInput = serde_json::from_slice(&line)?;
        if !seen.insert(record.id.clone()) {
            return Err("duplicate program ID".into());
        }
        let candidate = match scope {
            ProgramScope::TableProjection | ProgramScope::TableCensus => {
                bind_table_program(record.id.clone(), program.clone(), &record.submissions)
            }
            ProgramScope::ConstantView => {
                let [submission] = record.submissions.as_slice() else {
                    return Err("constant programs require exactly one submission".into());
                };
                bind_constant_program(record.id.clone(), program.clone(), submission)
            }
        };
        if matches!(scope, ProgramScope::TableCensus) {
            let row = match candidate {
                Ok(_) => json!({"id": record.id, "accepted": true}),
                Err(failure) => {
                    json!({"id": record.id, "accepted": false, "diagnostic": failure})
                }
            };
            serde_json::to_writer(&mut output, &row)?;
        } else {
            let program = candidate.map_err(|failure| format!("{failure:?}"))?;
            serde_json::to_writer(&mut output, &program)?;
        }
        writeln!(output)?;
    }
    output.flush()?;
    Ok(())
}
