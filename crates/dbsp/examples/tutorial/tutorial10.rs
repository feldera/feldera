use anyhow::Result;
use dbsp::{
    operator::Generator,
    utils::{Tup3, Tup4},
    zset, zset_set, Circuit, OrdZSet, Runtime, Stream,
};

fn main() -> Result<()> {
    // Set this value to 3 and uncomment the data in the arrays the Rust compiler
    // points out, to see a fixed-point computation which does _not_ terminate.
    const STEPS: usize = 2;

    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let (mut circuit_handle, output_handle) = Runtime::init_circuit(
        threads,
        move |root_circuit| {
            let mut edges_data = ([
                // The first step adds a graph of four nodes:
                // |0| -1-> |1| -1-> |2| -2-> |3| -2-> |4|
                zset_set! { Tup3(0_usize, 1_usize, 1_usize), Tup3(1, 2, 1), Tup3(2, 3, 2), Tup3(3, 4, 2) },
                // The second step removes the edge |1| -1-> |2|.
                zset! { Tup3(1, 2, 1) => -1 },
                // The third step would introduce a cycle but that would
                // cause the fixed-point computation to never terminate because we keep
                // on finding new paths with a higher cumulative weight and hopcount.
                // In total, we have the following graph:
                // |0| -1-> |1| -1-> |2| -2-> |3| -2-> |4|
                //  ^                                   |
                //  |                                   |
                //  ------------------3------------------
                // zset_set! { Tup3(1,2,1), Tup3(4, 0, 3)}
            ] as [_; STEPS])
                .into_iter();

            let edges = root_circuit.add_source(Generator::new(move || edges_data.next().unwrap()));

            // Create a base stream with all paths of length 1.
            let len_1 = edges.map(|Tup3(from, to, weight)| Tup4(*from, *to, *weight, 1));

            let closure = root_circuit.recursive(
                |child_circuit, len_n_minus_1: Stream<_, OrdZSet<Tup4<usize, usize, usize, usize>>>| {
                    // Import the `edges` and `len_1` stream from the parent circuit.
                    let edges = edges.delta0(child_circuit);
                    let len_1 = len_1.delta0(child_circuit);

                    // Perform an iterative step (n-1 to n) through joining the
                    // paths of length n-1 with the edges.
                    let len_n = len_n_minus_1
                        .map_index(|Tup4(start, end, cum_weight, hopcnt)| {
                            (*end, Tup4(*start, *end, *cum_weight, *hopcnt))
                        })
                        .join(
                            &edges
                                .map_index(|Tup3(from, to, weight)| (*from, Tup3(*from, *to, *weight))),
                            |_end_from,
                            Tup4(start, _end, cum_weight, hopcnt),
                            Tup3(_from, to, weight)| {
                                Tup4(*start, *to, cum_weight + weight, hopcnt + 1)
                            },
                        )
                        .plus(&len_1);

                    Ok(len_n)
                },
            )?;

            Ok(closure.output())
        },
    )?;

    let mut expected_outputs = ([
        // We expect the full transitive closure in the first step.
        zset! {
            Tup4(0, 1, 1, 1) => 1,
            Tup4(0, 2, 2, 2) => 1,
            Tup4(0, 3, 4, 3) => 1,
            Tup4(0, 4, 6, 4) => 1,
            Tup4(1, 2, 1, 1) => 1,
            Tup4(1, 3, 3, 2) => 1,
            Tup4(1, 4, 5, 3) => 1,
            Tup4(2, 3, 2, 1) => 1,
            Tup4(2, 4, 4, 2) => 1,
            Tup4(3, 4, 2, 1) => 1,
        },
        // These paths are removed in the second step.
        zset! {
            Tup4(0, 2, 2, 2) => -1,
            Tup4(0, 3, 4, 3) => -1,
            Tup4(0, 4, 6, 4) => -1,
            Tup4(1, 2, 1, 1) => -1,
            Tup4(1, 3, 3, 2) => -1,
            Tup4(1, 4, 5, 3) => -1,
        },
        // This does not matter, as the computation does not terminate
        // anymore due to the cycle.
        // zset! {},
    ] as [_; STEPS])
        .into_iter();

    for i in 0..STEPS {
        let iteration = i + 1;
        println!("Iteration {} starts...", iteration);
        circuit_handle.step()?;
        let output = output_handle.consolidate();
        assert_eq!(output, expected_outputs.next().unwrap());
        output
            .iter()
            .for_each(|(Tup4(start, end, cum_weight, hopcnt), _, z_weight)| {
                println!(
                    "{start} -> {end} (cum weight: {cum_weight}, hops: {hopcnt}) => {z_weight}"
                );
            });
        println!("Iteration {} finished.", iteration);
    }

    Ok(())
}
