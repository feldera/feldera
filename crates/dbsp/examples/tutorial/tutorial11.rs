use anyhow::Result;
use dbsp::{
    indexed_zset,
    operator::{Generator, Min},
    utils::{Tup2, Tup3, Tup4},
    zset_set, Circuit, NestedCircuit, OrdIndexedZSet, RootCircuit, Stream,
};

type Accumulator =
    Stream<NestedCircuit, OrdIndexedZSet<Tup2<usize, usize>, Tup4<usize, usize, usize, usize>>>;

fn main() -> Result<()> {
    const STEPS: usize = 2;

    let (circuit_handle, output_handle) = RootCircuit::build(move |root_circuit| {
        let mut edges_data = ([
            // The first step adds a graph of four nodes, just like before:
            // |0| -1-> |1| -1-> |2| -2-> |3| -2-> |4|
            zset_set! { Tup3(0_usize, 1_usize, 1_usize), Tup3(1, 2, 1), Tup3(2, 3, 2), Tup3(3, 4, 2) },
            // The second step introduces a cycle. Due to the code changes below,
            // the query does terminate though. In total, the graph now looks
            // like this:
            // |0| -1-> |1| -1-> |2| -2-> |3| -2-> |4|
            //  ^                                   |
            //  |                                   |
            //  ------------------3------------------
            zset_set! { Tup3(4, 0, 3)}
        ] as [_; STEPS])
        .into_iter();

        let edges = root_circuit.add_source(Generator::new(move || edges_data.next().unwrap()));

        // Create a base stream with all paths of length 1.
        let len_1 = edges
            .map_index(|Tup3(from, to, weight)| (Tup2(*from, *to), Tup4(*from, *to, *weight, 1)));

        let closure = root_circuit.recursive(|child_circuit, len_n_minus_1: Accumulator| {
            // Import the `edges` and `len_1` stream from the parent circuit.
            let edges = edges.delta0(child_circuit);
            let len_1 = len_1.delta0(child_circuit);

            // Perform an iterative step (n-1 to n) through joining the
            // paths of length n-1 with the edges.
            let len_n = len_n_minus_1
                .map_index(
                    |(Tup2(_start, _end), Tup4(start, end, cum_weight, hopcnt))| {
                        (*end, Tup4(*start, *end, *cum_weight, *hopcnt))
                    },
                )
                .join_index(
                    &edges.map_index(|Tup3(from, to, weight)| (*from, Tup3(*from, *to, *weight))),
                    |_end_from, Tup4(start, _end, cum_weight, hopcnt), Tup3(_from, to, weight)| {
                        Some((
                            Tup2(*start, *to),
                            Tup4(*start, *to, cum_weight + weight, hopcnt + 1),
                        ))
                    },
                )
                .plus(&len_1)
                .aggregate(Min);

            Ok(len_n)
        })?;

        let mut expected_outputs = ([
            // The transitive closure in the first step remains the same as in
            // `tutorial10.rs`.
            indexed_zset! { Tup2<usize, usize> => Tup4<usize, usize, usize, usize>:
                Tup2(0, 1) => { Tup4(0, 1, 1, 1) => 1 },
                Tup2(0, 2) => { Tup4(0, 2, 2, 2) => 1 },
                Tup2(0, 3) => { Tup4(0, 3, 4, 3) => 1 },
                Tup2(0, 4) => { Tup4(0, 4, 6, 4) => 1 },
                Tup2(1, 2) => { Tup4(1, 2, 1, 1) => 1 },
                Tup2(1, 3) => { Tup4(1, 3, 3, 2) => 1 },
                Tup2(1, 4) => { Tup4(1, 4, 5, 3) => 1 },
                Tup2(2, 3) => { Tup4(2, 3, 2, 1) => 1 },
                Tup2(2, 4) => { Tup4(2, 4, 4, 2) => 1 },
                Tup2(3, 4) => { Tup4(3, 4, 2, 1) => 1 },
            },
            // The second step's introduction of a cycle yields these new paths.
            indexed_zset! { Tup2<usize, usize> => Tup4<usize, usize, usize, usize>:
                Tup2(0, 0) => { Tup4(0, 0, 9, 5) => 1 },
                Tup2(1, 0) => { Tup4(1, 0, 8, 4) => 1 },
                Tup2(1, 1) => { Tup4(1, 1, 9, 5) => 1 },
                Tup2(2, 0) => { Tup4(2, 0, 7, 3) => 1 },
                Tup2(2, 1) => { Tup4(2, 1, 8, 4) => 1 },
                Tup2(2, 2) => { Tup4(2, 2, 9, 5) => 1 },
                Tup2(3, 0) => { Tup4(3, 0, 5, 2) => 1 },
                Tup2(3, 1) => { Tup4(3, 1, 6, 3) => 1 },
                Tup2(3, 2) => { Tup4(3, 2, 7, 4) => 1 },
                Tup2(3, 3) => { Tup4(3, 3, 9, 5) => 1 },
                Tup2(4, 0) => { Tup4(4, 0, 3, 1) => 1 },
                Tup2(4, 1) => { Tup4(4, 1, 4, 2) => 1 },
                Tup2(4, 2) => { Tup4(4, 2, 5, 3) => 1 },
                Tup2(4, 3) => { Tup4(4, 3, 7, 4) => 1 },
                Tup2(4, 4) => { Tup4(4, 4, 9, 5) => 1 },
            },
        ] as [_; STEPS])
            .into_iter();

        closure.inspect(move |output| {
            assert_eq!(*output, expected_outputs.next().unwrap());
        });

        Ok(closure.output())
    })?;

    for i in 0..STEPS {
        let iteration = i + 1;
        println!("Iteration {} starts...", iteration);
        circuit_handle.step()?;
        output_handle.consolidate().iter().for_each(
            |(Tup2(_start, _end), Tup4(start, end, cum_weight, hopcnt), z_weight)| {
                println!(
                    "{start} -> {end} (cum weight: {cum_weight}, hops: {hopcnt}) => {z_weight}"
                );
            },
        );
        println!("Iteration {} finished.", iteration);
    }

    Ok(())
}
