//! Simple DBSP example for counting the distribution of degrees in a graph.
//!
//! This program creates a series of graph edges, then counts the number of
//! times each source node appears, then counts the number of times each count
//! appears.  This is similar to the example for Differential Dataflow at
//! <https://docs.rs/differential-dataflow/latest/differential_dataflow/>.

use anyhow::Result;
use clap::Parser;
use dbsp::{
    operator::FilterMap, time::NestedTimestamp32, IndexedZSet, OrdIndexedZSet, OutputHandle,
    Runtime,
};

type Node = usize;
type Weight = isize;

#[derive(Debug, Clone, Parser)]
struct Args {
    /// Number of initial edges in the graph.
    #[clap(long, default_value = "100")]
    edges: usize,

    /// Number of source nodes in the graph.
    #[clap(long, default_value = "13")]
    sources: usize,

    /// Number of extra edges added later to the graph.
    #[clap(long, default_value = "5")]
    extra: usize,

    /// Number of threads.
    #[clap(long, default_value = "2")]
    threads: usize,
}

fn print_changes(
    degrees: &OutputHandle<OrdIndexedZSet<Node, isize, Weight>>,
    distribution: &OutputHandle<OrdIndexedZSet<isize, isize, Weight>>,
) {
    for (src, outdegree, weight) in degrees.consolidate().iter() {
        println!("    {weight:+}: Node {src} has out-degree {outdegree}");
    }
    println!();

    for (outdegree, count, weight) in distribution.consolidate().iter() {
        println!("    {weight:+}: {count} nodes have out-degree {outdegree}");
    }
    println!();
}

fn main() -> Result<()> {
    let Args {
        threads,
        edges,
        sources,
        extra,
    } = Args::parse();

    let (mut dbsp, (hedges, degrees, distribution)) = Runtime::init_circuit(threads, |circuit| {
        let (edges, hedges) = circuit.add_input_zset::<(Node, Node), Weight>();

        // Count the number of edges with each node as its source (each node's
        // out-degree).
        let degrees = edges.map(|&(src, _dst)| (src, 1)).index();
        let degrees = degrees.aggregate_linear::<NestedTimestamp32, _, _>(|_k, v| *v);

        // Count the number of nodes with each out-degree.
        let distribution = degrees.map(|(_src, count)| (*count, 1)).index();
        let distribution = distribution.aggregate_linear::<NestedTimestamp32, _, _>(|_k, v| *v);

        (hedges, degrees.output(), distribution.output())
    })
    .unwrap();

    // Add some initial edges and print the results.
    for i in 0..edges {
        hedges.push((i % sources, i % 7), 1);
    }
    dbsp.step().unwrap();
    println!("Initialization:");
    print_changes(&degrees, &distribution);

    // Add a few more nodes and print the changes.
    for i in 0..extra {
        hedges.push((i % sources, i % 9), 1);
    }
    dbsp.step().unwrap();
    println!("Changes:");
    print_changes(&degrees, &distribution);

    dbsp.kill().unwrap();

    Ok(())
}
