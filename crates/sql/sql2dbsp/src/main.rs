//! Local compiler entry points for SQL source and versioned circuit checkpoints.

mod checkpoint;
mod diagnostics;
#[cfg(unix)]
mod hybrid;
mod worker;

use std::{
    env,
    ffi::{OsStr, OsString},
    fs,
    io::{self, Write},
    path::Path,
    process::ExitCode,
};

const USAGE: &str =
    "Usage: sql2dbsp (--check-syntax | --syntax-diagnostics | --format | --schema | --emit-s1 | --emit-plan) FILE
       sql2dbsp --bind-submissions FILE.json
       sql2dbsp (--emit-s2 | --lower-s1) PRODUCER RUNTIME_ABI FILE
       sql2dbsp --hybrid REQUEST.json NEW_ATTEMPT_DIR
       sql2dbsp --s3-worker PRODUCER RUNTIME_ABI CHECKPOINT NEW_ATTEMPT_DIR
       sql2dbsp (--incrementalize | --propagate-empty-sources | --remove-constant-filters | --merge-sums | --remove-i-after-d | --remove-deindex | --remove-noops | --optimize-noops | --fuse-array-agg | --cse) NEXT_GENERATED_NAME CIRCUIT.json
       sql2dbsp --insert-limiters STATE.json CIRCUIT.json
       sql2dbsp (--statistics | --check-no-integral | --check-recursion | --check-hints | --declare-comparators | --gen2-passes | --compact-names | --tag-regions | --cse) CIRCUIT.json
       sql2dbsp (--s3-to-gen2 | --s3-to-dataflow | --s3-to-diagnostics | --s3-to-artifacts) FILE
Use - to read from standard input.";

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmissionRequest {
    id: String,
    submissions: Vec<feldera_sql_binder::Submission>,
}

enum OptimizerPass {
    Cse,
    Incrementalize,
    PropagateEmptySources,
    RemoveConstantFilters,
    MergeSums,
    RemoveIAfterD,
    RemoveDeindex,
    RemoveNoops,
    OptimizeNoops,
    FuseIntoArrayAgg,
}

enum Command<'a> {
    Syntax,
    SyntaxDiagnostics,
    Statistics,
    CheckHints,
    CheckNoIntegral,
    CheckRecursion,
    Comparators,
    Gen2Passes,
    CompactNames,
    TagRegions,
    Cse,
    Optimizer(OptimizerPass, u64),
    Limiters(&'a OsStr),
    Format,
    Schema,
    Bind,
    Submissions,
    Plan,
    Lower {
        detached: bool,
        producer: &'a str,
        runtime: &'a str,
    },
    Checkpoint(checkpoint::Output),
}

impl<'a> Command<'a> {
    fn parse(args: &'a [OsString]) -> Option<(Self, &'a OsStr)> {
        match args {
            [mode, state, file] if mode == "--insert-limiters" => {
                Some((Self::Limiters(state), file))
            }
            [mode, next, file] => {
                let pass = match mode.to_str()? {
                    "--cse" => OptimizerPass::Cse,
                    "--incrementalize" => OptimizerPass::Incrementalize,
                    "--propagate-empty-sources" => OptimizerPass::PropagateEmptySources,
                    "--remove-constant-filters" => OptimizerPass::RemoveConstantFilters,
                    "--merge-sums" => OptimizerPass::MergeSums,
                    "--remove-i-after-d" => OptimizerPass::RemoveIAfterD,
                    "--remove-deindex" => OptimizerPass::RemoveDeindex,
                    "--remove-noops" => OptimizerPass::RemoveNoops,
                    "--optimize-noops" => OptimizerPass::OptimizeNoops,
                    "--fuse-array-agg" => OptimizerPass::FuseIntoArrayAgg,
                    _ => return None,
                };
                Some((Self::Optimizer(pass, next.to_str()?.parse().ok()?), file))
            }
            [mode, producer, runtime, file] if mode == "--emit-s2" || mode == "--lower-s1" => {
                Some((
                    Self::Lower {
                        detached: mode == "--lower-s1",
                        producer: producer.to_str()?,
                        runtime: runtime.to_str()?,
                    },
                    file,
                ))
            }
            [mode, file] => {
                let command = match mode.to_str()? {
                    "--cse" => Self::Cse,
                    "--tag-regions" => Self::TagRegions,
                    "--statistics" => Self::Statistics,
                    "--check-no-integral" => Self::CheckNoIntegral,
                    "--check-recursion" => Self::CheckRecursion,
                    "--check-hints" => Self::CheckHints,
                    "--declare-comparators" => Self::Comparators,
                    "--gen2-passes" => Self::Gen2Passes,
                    "--compact-names" => Self::CompactNames,
                    "--syntax-diagnostics" => Self::SyntaxDiagnostics,
                    "--check-syntax" => Self::Syntax,
                    "--format" => Self::Format,
                    "--schema" => Self::Schema,
                    "--bind-submissions" => Self::Submissions,
                    "--emit-s1" => Self::Bind,
                    "--emit-plan" => Self::Plan,
                    _ => Self::Checkpoint(checkpoint::Output::from_flag(mode)?),
                };
                Some((command, file))
            }
            _ => None,
        }
    }

    fn run(
        self,
        filename: Option<&Path>,
        source: &str,
    ) -> Result<ExitCode, Box<dyn std::error::Error>> {
        let mut output = io::BufWriter::new(io::stdout().lock());
        let name = || filename.map_or_else(|| "<stdin>".into(), |f| f.display().to_string());
        let mut status = ExitCode::SUCCESS;
        match self {
            Self::SyntaxDiagnostics => {
                let diagnostic = diagnostics::syntax(source)?;
                if diagnostic.is_some() {
                    status = ExitCode::FAILURE;
                }
                let messages: Vec<_> = diagnostic.into_iter().collect();
                write_json(&mut output, &messages.into())?;
            }
            Self::Limiters(state) => {
                let graph = feldera_sql_ir::Circuit::from_json(feldera_sql_ir::parse_json(
                    source.as_bytes(),
                )?)?;
                let inputs = serde_json::from_slice(&fs::read(state)?)?;
                let mut warnings = Vec::new();
                let (result, next) = feldera_sql_opt::limiters::insert_limiters_with_warnings(
                    graph,
                    inputs,
                    &mut warnings,
                )?;
                let mut record = serde_json::json!({
                    "circuit": result.to_json(),
                    "next_generated_name": next
                });
                if !warnings.is_empty() {
                    record["warnings"] = warnings
                        .into_iter()
                        .map(|warning| {
                            serde_json::json!({
                                "range": warning.position,
                                "error_type": warning.kind,
                                "message": warning.message
                            })
                        })
                        .collect();
                }
                write_json(&mut output, &record)?;
            }
            Self::Statistics
            | Self::CheckNoIntegral
            | Self::CheckRecursion
            | Self::CheckHints
            | Self::Comparators
            | Self::Gen2Passes
            | Self::CompactNames
            | Self::TagRegions
            | Self::Cse
            | Self::Optimizer(_, _) => {
                let raw = feldera_sql_ir::parse_json(source.as_bytes())?;
                let circuit = feldera_sql_ir::Circuit::from_json(raw)?;
                let transformed = match self {
                    Self::Optimizer(pass, mut next) => {
                        let result = match pass {
                            OptimizerPass::Cse => {
                                feldera_sql_opt::common_subexpressions_with_names(
                                    circuit, &mut next,
                                )?
                            }
                            OptimizerPass::Incrementalize => {
                                feldera_sql_opt::incrementalize(circuit, &mut next)?
                            }
                            OptimizerPass::PropagateEmptySources => {
                                feldera_sql_opt::propagate_empty_sources(circuit, &mut next)?
                            }
                            OptimizerPass::RemoveConstantFilters => {
                                feldera_sql_opt::remove_constant_filters(circuit, &mut next)?
                            }
                            OptimizerPass::MergeSums => {
                                feldera_sql_opt::merge_sums(circuit, &mut next)?
                            }
                            OptimizerPass::RemoveIAfterD => {
                                feldera_sql_opt::remove_redundant_integrates(circuit, &mut next)?
                            }
                            OptimizerPass::RemoveDeindex => {
                                feldera_sql_opt::remove_deindex_operators(circuit, &mut next)?
                            }
                            OptimizerPass::RemoveNoops => {
                                feldera_sql_opt::remove_noops(circuit, &mut next)?
                            }
                            OptimizerPass::OptimizeNoops => {
                                feldera_sql_opt::optimize_noops(circuit, &mut next)?
                            }
                            OptimizerPass::FuseIntoArrayAgg => {
                                feldera_sql_opt::fuse_into_array_agg(circuit, &mut next)?
                            }
                        };
                        serde_json::json!({"circuit": result.to_json(), "next_generated_name": next})
                    }
                    Self::Cse => feldera_sql_opt::common_subexpressions(circuit)?.to_json(),
                    Self::Statistics => {
                        serde_json::to_value(feldera_sql_opt::circuit_statistics(&circuit)?)?
                    }
                    Self::CheckNoIntegral => match feldera_sql_opt::first_integral(&circuit)? {
                        Some(feldera_sql_ir::NodeId::Outer(id)) => id.into(),
                        Some(_) => unreachable!("integrator is an outer node"),
                        None => serde_json::Value::Null,
                    },
                    Self::CheckRecursion => {
                        serde_json::to_value(feldera_sql_opt::recursive_violation(&circuit)?)?
                    }
                    Self::CheckHints => {
                        serde_json::to_value(feldera_sql_opt::check_hints(&circuit)?)?
                    }
                    Self::Comparators => feldera_sql_opt::declare_comparators(circuit)?.to_json(),
                    Self::Gen2Passes => feldera_sql_opt::gen2_passes(circuit)?.to_json(),
                    Self::TagRegions => feldera_sql_opt::tag_regions(circuit)?.to_json(),
                    _ => feldera_sql_opt::compact_names(circuit)?.to_json(),
                };
                write_json(&mut output, &transformed)?;
            }
            Self::Syntax => {
                feldera_sql_parser::parse(source)?;
            }
            Self::Format => {
                let statements = feldera_sql_parser::parse(source)?;
                for statement in feldera_sql_parser::emit_statements(&statements)? {
                    writeln!(output, "{statement};")?;
                }
            }
            Self::Schema => write_json(&mut output, &feldera_sql_binder::preview_schema(source)?)?,
            Self::Plan => {
                let program = feldera_sql_binder::bind_sql(name(), source)?;
                write_json(&mut output, &feldera_sql_lower::query_plan(&program)?)?;
            }
            Self::Bind | Self::Submissions => {
                let program = if matches!(self, Self::Submissions) {
                    let request: SubmissionRequest = serde_json::from_str(source)?;
                    feldera_sql_binder::bind_submissions(request.id, &request.submissions)?
                } else {
                    feldera_sql_binder::bind_sql(name(), source)?
                };
                serde_json::to_writer(&mut output, &program)?;
                writeln!(output)?;
            }
            Self::Lower {
                detached,
                producer,
                runtime,
            } => {
                let filename = filename.map(|f| f.to_string_lossy());
                let (program, input) = if detached {
                    let wire = feldera_sql_ir::parse_json(source.as_bytes())?;
                    (
                        serde_json::from_value(wire)?,
                        feldera_sql_lower::Input::Checkpoint,
                    )
                } else {
                    (
                        feldera_sql_binder::bind_sql(name(), source)?,
                        feldera_sql_lower::Input::Sql(filename.as_deref()),
                    )
                };
                let circuit = feldera_sql_lower::lower_program(&program, input, producer, runtime)?;
                write_json(&mut output, &circuit)?;
            }
            Self::Checkpoint(kind) => write_json(&mut output, &kind.emit(source)?)?,
        }
        output.flush()?;
        Ok(status)
    }
}

fn write_json(mut output: impl Write, value: &serde_json::Value) -> io::Result<()> {
    serde_json::to_writer(&mut output, value)?;
    writeln!(output)
}

fn main() -> ExitCode {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if matches!(args.as_slice(), [help] if help == "--help" || help == "-h") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    #[cfg(unix)]
    if let [mode, manifest, attempt] = args.as_slice() {
        if mode == "--hybrid" {
            return hybrid::run(Path::new(manifest), Path::new(attempt)).unwrap_or_else(|error| {
                eprintln!("could not complete hybrid compilation: {error}");
                ExitCode::from(2)
            });
        }
    }
    if let [mode, producer, runtime, checkpoint, attempt] = args.as_slice() {
        if mode == "--s3-worker" {
            let (Some(producer), Some(runtime)) = (producer.to_str(), runtime.to_str()) else {
                eprintln!("compiler identities must be UTF-8");
                return ExitCode::from(2);
            };
            return worker::run(Path::new(checkpoint), Path::new(attempt), producer, runtime)
                .unwrap_or_else(|error| {
                    eprintln!("could not publish worker result: {error}");
                    ExitCode::from(2)
                });
        }
    }
    let Some((command, file)) = Command::parse(&args) else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let filename = (file != "-").then(|| Path::new(file));
    let label = filename.unwrap_or_else(|| Path::new("<stdin>")).display();
    let input = match filename {
        Some(file) => fs::read_to_string(file),
        None => io::read_to_string(io::stdin().lock()),
    };
    let source = match input {
        Ok(source) => source,
        Err(error) => {
            eprintln!("{label}: {error}");
            return ExitCode::from(2);
        }
    };
    match command.run(filename, &source) {
        Ok(status) => status,
        Err(error) => {
            eprintln!("{label}: {error}");
            ExitCode::FAILURE
        }
    }
}
