//! Simple DBSP example for counting the distribution of degrees in a graph.
//!
//! This program creates a series of graph edges, then counts the number of
//! times each source node appears, then counts the number of times each count
//! appears.  This is similar to the example for Differential Dataflow at
//! <https://docs.rs/differential-dataflow/latest/differential_dataflow/>.

use anyhow::Result;
use clap::Parser;
use dbsp::{utils::Tup2, OrdIndexedZSet, OutputHandle, Runtime};

type Node = u64;

#[derive(Debug, Clone, Parser)]
struct Args {
    /// Number of initial edges in the graph.
    #[clap(long, default_value = "100")]
    edges: u64,

    /// Number of source nodes in the graph.
    #[clap(long, default_value = "13")]
    sources: u64,

    /// Number of extra edges added later to the graph.
    #[clap(long, default_value = "5")]
    extra: u64,

    /// Number of threads.
    #[clap(long, default_value = "2")]
    threads: u64,
}

fn print_changes(
    degrees: &OutputHandle<OrdIndexedZSet<Node, i64>>,
    distribution: &OutputHandle<OrdIndexedZSet<i64, i64>>,
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

    let (mut dbsp, (hedges, degrees, distribution)) =
        Runtime::init_circuit(threads as usize, |circuit| {
            let (edges, hedges) = circuit.add_input_zset::<Tup2<Node, Node>>();

            // Count the number of edges with each node as its source (each node's
            // out-degree).
            let degrees = edges.map(|Tup2(src, _dst)| *src).weighted_count();

            // Count the number of nodes with each out-degree.
            let distribution = degrees.map(|(_src, count)| *count).weighted_count();

            Ok((hedges, degrees.output(), distribution.output()))
        })
        .unwrap();

    // Add some initial edges and print the results.
    for i in 0..edges {
        hedges.push(Tup2(i % sources, i % 7), 1);
    }
    dbsp.step().unwrap();
    println!("Initialization:");
    print_changes(&degrees, &distribution);

    // Add a few more nodes and print the changes.
    for i in 0..extra {
        hedges.push(Tup2(i % sources, i % 9), 1);
    }
    dbsp.step().unwrap();
    println!("Changes:");
    print_changes(&degrees, &distribution);

    dbsp.kill().unwrap();

    Ok(())
}
