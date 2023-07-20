use clap::Parser;
use dataflow_jit::{
    codegen::CodegenConfig,
    facade::Demands,
    ir::{
        literal::{NullableConstant, RowLiteral, StreamCollection},
        Constant, NodeId,
    },
    sql_graph::SqlGraph,
    DbspCircuit,
};
use std::{collections::BTreeSet, fs, num::NonZeroUsize, thread, time::Instant};

const VERTICES_ID: NodeId = NodeId::new(2456);
const EDGES_ID: NodeId = NodeId::new(2479);

const NOT_IN_ID: NodeId = NodeId::new(4428);
const NOT_EXISTS_ID: NodeId = NodeId::new(4816);
const LEFT_JOIN_ID: NodeId = NodeId::new(5004);

fn main() {
    set_logger();

    let args = Args::parse();

    let workers = if args.workers == 0 {
        thread::available_parallelism().map_or(8, NonZeroUsize::get)
    } else {
        args.workers
    };
    let config = if args.debug {
        CodegenConfig::debug()
    } else {
        CodegenConfig::release()
    };

    let circuit = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/benches/path.json"))
        .expect("failed to read circuit from file");
    let graph = serde_json::from_str::<SqlGraph>(&circuit)
        .unwrap()
        .rematerialize();

    let mut circuit = DbspCircuit::new(graph, !args.debug, workers, config, Demands::new());

    {
        let mut vertices = BTreeSet::new();

        let mut edges = Vec::with_capacity(5 * args.layers * args.layers);
        for layer in 0..5i64 {
            for from in 0..args.layers as i64 {
                for to in 0..args.layers as i64 {
                    let (src, dest) = (
                        from + (args.layers as i64 * layer),
                        to + args.layers as i64 * (layer + 1),
                    );

                    let row = RowLiteral::new(vec![
                        NullableConstant::NonNull(Constant::I64(src)),
                        NullableConstant::NonNull(Constant::I64(dest)),
                    ]);
                    edges.push((row, 1));

                    vertices.extend([
                        src,
                        dest,
                        src.wrapping_mul(0xDEADBEEF),
                        dest.wrapping_mul(0xD0D0CACA),
                    ]);
                }
            }
        }

        let (total_vertices, total_edges) = (vertices.len(), edges.len());
        circuit.append_input(EDGES_ID, &StreamCollection::Set(edges));

        let vertices: Vec<_> = vertices
            .into_iter()
            .map(|vertex| {
                let vertex =
                    RowLiteral::new(vec![NullableConstant::NonNull(Constant::I64(vertex))]);
                (vertex, 1)
            })
            .collect();
        circuit.append_input(VERTICES_ID, &StreamCollection::Set(vertices));

        println!("added {total_vertices} vertices and {total_edges} edges");
    }

    let start = Instant::now();
    circuit.step().unwrap();
    let elapsed = start.elapsed();
    println!("stepped in {elapsed:#?}");

    circuit.consolidate_output(NOT_IN_ID);
    circuit.consolidate_output(NOT_EXISTS_ID);
    circuit.consolidate_output(LEFT_JOIN_ID);

    circuit.kill().unwrap();
}

#[derive(Parser)]
struct Args {
    /// Set the number of path layers
    #[arg(long, default_value = "500")]
    layers: usize,

    /// Set the number of workers
    #[arg(long, default_value = "0")]
    workers: usize,

    /// Enable debug mode
    #[arg(long)]
    debug: bool,

    #[doc(hidden)]
    #[clap(long = "bench", hide = true)]
    __bench: bool,
}

fn set_logger() {
    use is_terminal::IsTerminal;
    use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

    let filter = EnvFilter::try_from_env("DATAFLOW_JIT_LOG")
        .or_else(|_| EnvFilter::try_new("info,cranelift_codegen=off,cranelift_jit=off"))
        .unwrap();
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_test_writer()
                .with_ansi(std::io::stdout().is_terminal()),
        )
        .try_init();
}
