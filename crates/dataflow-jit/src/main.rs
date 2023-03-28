use clap::Parser;
use dataflow_jit::{
    codegen::CodegenConfig,
    dataflow::CompiledDataflow,
    ir::{GraphExt, Validator},
    sqljson::SqlGraph,
};
use dbsp::Runtime;
use std::{
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

    let args = Args::parse();

    let source: Box<dyn Read> = if args.file == Path::new("-") {
        Box::new(io::stdin())
    } else {
        if args.file.extension().is_none() {
            eprintln!(
                "warning: {} has no extension and is not a json file",
                args.file.display(),
            );
        } else if let Some(extension) = args.file.extension() {
            if extension != Path::new("json") {
                eprintln!("warning: {} is not a json file", args.file.display());
            }
        }

        match File::open(&args.file) {
            Ok(file) => Box::new(file),
            Err(error) => {
                eprintln!("failed to read {}: {error}", args.file.display());
                return ExitCode::FAILURE;
            }
        }
    };

    let mut graph = match serde_json::from_reader::<_, SqlGraph>(BufReader::new(source)) {
        Ok(graph) => graph.rematerialize(),
        Err(error) => {
            eprintln!("failed to parse json from {}: {error}", args.file.display());
            return ExitCode::FAILURE;
        }
    };

    // TODO: Validate the given graph once validation works

    println!("Unoptimized: {graph:#?}");
    Validator::new(graph.layout_cache().clone())
        .validate_graph(&graph)
        .unwrap();
    graph.optimize();

    let (dataflow, jit_handle, _layout_cache) =
        CompiledDataflow::new(&graph, CodegenConfig::release());

    let (runtime, _) =
        Runtime::init_circuit(1, move |circuit| dataflow.construct(circuit)).unwrap();
    if let Err(_error) = runtime.kill() {
        eprintln!("failed to kill runtime");
        return ExitCode::FAILURE;
    }
    unsafe { jit_handle.free_memory() }

    ExitCode::SUCCESS
}

#[derive(Parser)]
struct Args {
    /// The file to parse json from, if `-` is passed then stdin will be read
    /// from
    pub file: PathBuf,
}
