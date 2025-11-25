# DBSP

Database Stream Processor (DBSP) is a computational engine for
continuous analysis of changing data. With DBSP, a programmer writes
code in terms of computations on a complete data set, but DBSP
implements it incrementally, meaning that changes to the data set run
in time proportional to the size of the change rather than the size of
the data set. This is a major advantage for applications that work
with large data sets that change frequently in small ways.

The DBSP computational engine is a component of the [Feldera
Continuous Analytics Platform](https://www.feldera.com).

# Resources

## Learning Materials

- The [tutorial] walks through a series of simple examples.

- The [`circuit` module documentation] provides a catalog of
  computational elements.

- More sophisticated uses can be found as [examples] and [benchmarks]

[tutorial]: https://docs.rs/dbsp/latest/dbsp/tutorial

[`circuit` module documentation]: https://docs.rs/dbsp/latest/dbsp/circuit

[examples]: https://github.com/feldera/feldera/tree/main/crates/dbsp/examples

[benchmarks]: https://github.com/feldera/feldera/tree/main/crates/dbsp/benches

## Documentation

- [Rustdocs].

- [Feldera Continuous Analytics Platform documentation][1].

- The formal theory behind DBSP, as a [paper] or as [video] of a talk.

[Rustdocs]: https://docs.rs/dbsp

[1]: https://docs.feldera.com/

[paper]: https://docs.feldera.com/vldb23.pdf

[video]: https://www.youtube.com/watch?v=iT4k5DCnvPU

## Feedback

Some ways that you can give us your feedback:

- Join our [community Slack].

- [File an issue].

- [Submit a pull request]. DBSP uses the [Developer Certificate of
  Origin] (DCO) (see the [contribution guidelines]).

[community Slack]: https://www.feldera.com/slack/

[file an issue]: https://github.com/feldera/feldera/issues

[submit a pull request]: https://github.com/feldera/feldera/pulls

[Developer Certificate of Origin]: https://developercertificate.org/

[contribution guidelines]: https://github.com/feldera/feldera/blob/main/CONTRIBUTING.md

# Example

```rust
use dbsp::{operator::FilterMap, IndexedZSet, OrdIndexedZSet, OutputHandle, Runtime};

type Node = usize;
type Weight = isize;

// Creates a series of graph edges, then counts the number of times each source
// node appears, then counts the number of times each count appears.
fn main() {
    let edges = 100; // Number of initial edges in the graph.
    let sources = 13; // Number of source nodes in the graph.
    let extra = 5; // Number of extra edges added later to the graph.
    let threads = 2; // Number of threads.

    let (mut dbsp, (hedges, degrees, distribution)) = Runtime::init_circuit(threads, |circuit| {
        let (edges, hedges) = circuit.add_input_zset::<(Node, Node), Weight>();

        // Count the number of edges with each node as its source (each node's
        // out-degree).
        let degrees = edges.map(|(src, _dst)| *src).weighted_count();

        // Count the number of nodes with each out-degree.
        let distribution = degrees.map(|(_src, count)| *count).weighted_count();

        Ok((hedges, degrees.output(), distribution.output()))
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
}

fn print_changes(
    degrees: &OutputHandle<OrdIndexedZSet<Node, isize, Weight>>,
    distribution: &OutputHandle<OrdIndexedZSet<isize, isize, Weight>>,
) {
    for (src, outdegree, weight) in degrees.consolidate().iter() {
        println!("    {weight:+}: Node {src} has out-degree {outdegree}");
    }
    for (outdegree, count, weight) in distribution.consolidate().iter() {
        println!("    {weight:+}: {count} nodes have out-degree {outdegree}");
    }
}
```

## Parquet-backed batches

DBSP now offers a helper type, [`ParquetIndexedZSet`](src/storage/parquet.rs), that
wraps any [`OrdIndexedZSet`](crate::OrdIndexedZSet) and persists it as a Parquet
file. The helper also supports loading the Parquet file back into an
`OrdIndexedZSet`, so you can move batches between circuits or store checkpoints
using Arrow's columnar format in addition to the native rkyv-based layer files.
