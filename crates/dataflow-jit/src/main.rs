use clap::Parser;
use dataflow_jit::{
    codegen::{json::JsonMapping, CodegenConfig},
    dataflow::CompiledDataflow,
    facade::Demands,
    ir::{GraphExt, Validator},
    sql_graph::SqlGraph,
    DbspCircuit,
};
use dbsp::Runtime;
use jsonschema::paths::PathChunk;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
    process::ExitCode,
};

fn main() -> ExitCode {
    {
        use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

        tracing_subscriber::registry()
            .with(EnvFilter::try_from_env("DATAFLOW_JIT_LOG").unwrap_or_default())
            .with(fmt::layer())
            .init();
    }

    match Args::parse() {
        Args::Run { program, config } => run(&program, &config),

        Args::Validate {
            file,
            print_layouts,
        } => validate(&file, print_layouts),

        Args::PrintSchema => print_schema(),
    }
}

#[derive(Debug, Deserialize)]
struct Config {
    workers: usize,
    optimize: bool,
    release: bool,
    inputs: HashMap<String, Input>,
}

#[derive(Debug, Deserialize)]
struct Input {
    file: PathBuf,
    kind: InputKind,
}

#[derive(Debug, Deserialize)]
enum InputKind {
    Json(JsonMapping),
    Csv(Vec<(usize, usize, Option<String>)>),
}

enum InputFormat {
    Json,
    Csv,
}

fn run(program: &Path, config: &Path) -> ExitCode {
    let config = File::open(config).unwrap();
    let config: Config = serde_json::from_reader(BufReader::new(config)).unwrap();

    let graph = File::open(program).unwrap();
    let graph = serde_json::from_reader::<_, SqlGraph>(BufReader::new(graph))
        .unwrap()
        .rematerialize();

    let sources = graph.source_nodes();
    let source_names: HashMap<_, _> = sources
        .iter()
        .filter_map(|&(node, layout)| {
            graph.nodes()[&node]
                .as_source()
                .and_then(|source| source.name())
                .map(|source| (source.to_owned(), (node, layout.unwrap_set())))
        })
        .collect();

    let sinks = graph.sink_nodes();

    let (mut demands, mut inputs) = (Demands::new(), Vec::with_capacity(config.inputs.len()));
    for (name, input) in config.inputs {
        let (node, layout) = source_names[&name];
        let format = match input.kind {
            InputKind::Json(mappings) => {
                demands.add_json_deserialize(layout, mappings);
                InputFormat::Json
            }
            InputKind::Csv(mappings) => {
                demands.add_csv_deserialize(layout, mappings);
                InputFormat::Csv
            }
        };

        inputs.push((node, input.file, format));
    }

    let mut circuit = DbspCircuit::new(
        graph,
        config.optimize,
        config.workers,
        if config.release {
            CodegenConfig::release()
        } else {
            CodegenConfig::debug()
        },
        demands,
    );

    for (target, file, format) in inputs {
        match format {
            InputFormat::Json => {
                let file = BufReader::new(File::open(file).unwrap());
                circuit.append_json_input(target, file);
            }

            InputFormat::Csv => circuit.append_csv_input(target, &file),
        }
    }

    circuit.step().unwrap();

    for (sink, _) in sinks {
        let output = circuit.consolidate_output(sink);
        println!("{output:?}");
    }

    circuit.kill().unwrap();

    ExitCode::SUCCESS
}

fn validate(file: &Path, print_layouts: bool) -> ExitCode {
    let schema_json = {
        let schema = schemars::schema_for!(SqlGraph);
        let schema = serde_json::to_string_pretty(&schema).unwrap();

        serde_json::from_str::<Value>(&schema).unwrap()
    };

    let mut source: Box<dyn Read> = if file == Path::new("-") {
        Box::new(io::stdin())
    } else {
        if file.extension().is_none() {
            eprintln!(
                "warning: {} has no extension and is not a json file",
                file.display(),
            );
        } else if let Some(extension) = file.extension() {
            if extension != Path::new("json") {
                eprintln!("warning: {} is not a json file", file.display());
            }
        }

        match File::open(file) {
            Ok(file) => Box::new(file),
            Err(error) => {
                eprintln!("failed to read {}: {error}", file.display());
                return ExitCode::FAILURE;
            }
        }
    };

    let mut raw_source = String::new();
    if let Err(error) = source.read_to_string(&mut raw_source) {
        eprintln!("failed to read input graph: {error}");
        return ExitCode::FAILURE;
    }

    let source: Value = match serde_json::from_str(&raw_source) {
        Ok(source) => source,
        Err(error) => {
            eprintln!("failed to parse json: {error}");
            return ExitCode::FAILURE;
        }
    };

    match jsonschema::JSONSchema::options()
        .with_draft(jsonschema::Draft::Draft7)
        .compile(&schema_json)
    {
        Ok(schema) => {
            if let Err(errors) = schema.validate(&source) {
                let mut total_errors = 0;
                for error in errors {
                    println!("{error:?}");
                    eprintln!(
                        "json validation error at `{}`: {error}",
                        error.instance_path,
                    );

                    // FIXME: Schema paths aren't correct, see
                    // https://github.com/Stranger6667/jsonschema-rs/issues/426
                    let mut expected_schema = &schema_json;
                    for key in error.schema_path.iter() {
                        expected_schema = match key {
                            PathChunk::Property(property) => &expected_schema[&**property],
                            PathChunk::Index(index) => &expected_schema[index],
                            PathChunk::Keyword(keyword) => &expected_schema[keyword],
                        };
                    }

                    if !expected_schema.is_null() {
                        eprintln!("expected item schema: {expected_schema}");
                    }

                    total_errors += 1;
                }

                eprintln!(
                    "encountered {total_errors} error{} while validating json, exiting",
                    if total_errors == 1 { "" } else { "s" },
                );
                return ExitCode::FAILURE;
            }
        }

        Err(error) => eprintln!("failed to compile json schema: {error}"),
    }

    let mut graph = match serde_json::from_value::<SqlGraph>(source) {
        Ok(graph) => graph.rematerialize(),
        Err(error) => {
            eprintln!("failed to parse json from {}: {error}", file.display());
            return ExitCode::FAILURE;
        }
    };

    println!("Unoptimized: {graph:#?}");
    if let Err(error) = Validator::new(graph.layout_cache().clone()).validate_graph(&graph) {
        eprintln!("validation error: {error}");
        return ExitCode::FAILURE;
    }
    graph.optimize();

    let (dataflow, jit_handle, layout_cache) =
        CompiledDataflow::new(&graph, CodegenConfig::release(), |_| ());

    if print_layouts {
        layout_cache.print_layouts();
    }

    let (runtime, _) =
        Runtime::init_circuit(1, move |circuit| dataflow.construct(circuit)).unwrap();
    if let Err(_error) = runtime.kill() {
        eprintln!("failed to kill runtime");
        return ExitCode::FAILURE;
    }
    unsafe { jit_handle.free_memory() }

    ExitCode::SUCCESS
}

fn print_schema() -> ExitCode {
    let schema = schemars::schema_for!(SqlGraph);
    let schema = serde_json::to_string_pretty(&schema).unwrap();
    println!("{schema}");

    ExitCode::SUCCESS
}

#[derive(Parser)]
enum Args {
    /// Run the given dataflow graph
    Run {
        /// The file to parse the program json from
        program: PathBuf,
        /// The configuration file specifying inputs
        config: PathBuf,
    },

    /// Validate the given dataflow graph
    Validate {
        /// The file to parse json from, if `-` is passed then stdin will be
        /// read from
        file: PathBuf,

        /// Print out all layouts involved in the program
        #[arg(long)]
        print_layouts: bool,
    },

    /// Print the json schema of the dataflow graph
    PrintSchema,
}
