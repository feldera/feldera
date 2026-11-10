use dbsp::{
    Circuit, OrdZSet, RootCircuit, Runtime, Stream, operator::Generator,
    typed_batch::DynBatchReader, utils::Tup2,
};

fn main() {
    let hruntime = Runtime::run(16, |_parker| {
        let circuit = RootCircuit::build(|circuit| {
            const LAYER: u32 = 200;

            let mut tuples = Vec::new();
            if Runtime::worker_index() == 0 {
                for layer in 0..5 {
                    for from in 0..LAYER {
                        for to in 0..LAYER {
                            tuples.push(Tup2(
                                Tup2(Tup2(from + (LAYER * layer), to + LAYER * (layer + 1)), ()),
                                1,
                            ));
                        }
                    }
                }
            }

            let edges = <OrdZSet<Tup2<u32, u32>>>::from_tuples((), tuples);

            let edges: Stream<_, OrdZSet<Tup2<u32, u32>>> =
                circuit.add_source(Generator::new(move || edges.clone()));

            let paths = circuit
                .recursive(|child, paths: Stream<_, OrdZSet<Tup2<u32, u32>>>| {
                    // ```text
                    //                            distinct
                    //               ┌───┐          ┌───┐
                    // edges         │   │          │   │  paths
                    // ────┬────────►│ + ├──────────┤   ├────────┬───►
                    //     │         │   │          │   │        │
                    //     │         └───┘          └───┘        │
                    //     │           ▲                         │
                    //     │           │                         │
                    //     │         ┌─┴─┐                       │
                    //     │         │   │                       │
                    //     └────────►│ X │ ◄─────────────────────┘
                    //               │   │
                    //               └───┘
                    //               join
                    // ```
                    let edges = edges.delta0(child);

                    let paths_inverted = paths.map(|&Tup2(x, y)| Tup2(y, x));

                    let paths_inverted_indexed = paths_inverted.map_index(|Tup2(k, v)| (*k, *v));
                    let edges_indexed = edges.map_index(|Tup2(k, v)| (*k, *v));

                    Ok(edges.plus(
                        &paths_inverted_indexed
                            .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                    ))
                })
                .unwrap();

            paths.gather(0).inspect(|zs: &OrdZSet<_>| {
                if Runtime::worker_index() == 0 {
                    println!("paths: {}", zs.len_upper_bound())
                }
            });
            Ok(())
        })
        .unwrap()
        .0;

        circuit.transaction().unwrap();
    })
    .expect("runtime initialization should succeed");

    hruntime.join().unwrap();
}
