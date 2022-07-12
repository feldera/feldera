mod bfs;
mod data;

use dbsp::{
    circuit::{Root, Runtime},
    operator::Generator,
    trace::{BatchReader, Cursor},
    OrdZSet,
};
use std::time::Instant;

type Weight = isize;

const DATA_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/benches/ldbc-graphalytics-data",
);

// example-directed
const XDIR_URL: &str = "https://surfdrive.surf.nl/files/index.php/s/7hGIIZ6nzxgi0dU/download";
// datagen-7_7-zf
const D77_URL: &str = "https://surfdrive.surf.nl/files/index.php/s/sstTvqgcyhWVVPn/download";

fn main() {
    let data = bfs::load_datasets(
        "example-undirected",
        "https://surfdrive.surf.nl/files/index.php/s/enKFbXmUBP2rxgB/download",
    );
    // let data = bfs::load_datasets("datagen-7_7-zf", D77_URL);
    let mut root_data = Some(data.roots);
    let mut vertex_data = Some(data.vertices);
    let mut edge_data = Some(data.edges);
    // let mut expected_data = Some(data.expected);

    Runtime::run(1, |_runtime, _index| {
        println!("running dfs benchmark");
        let start = Instant::now();

        let root = Root::build(move |circuit| {
            let roots = circuit.add_source(Generator::new(move || root_data.take().unwrap()));
            let vertices = circuit.add_source(Generator::new(move || vertex_data.take().unwrap()));
            let edges = circuit.add_source(Generator::new(move || edge_data.take().unwrap()));
            // let expected =
            //     circuit.add_source(Generator::new(move || expected_data.take().unwrap()));

            let results = bfs::bfs(roots, vertices, edges);

            results.inspect(|distances| {
                let mut cursor = distances.cursor();
                while cursor.key_valid() {
                    let key = *cursor.key();
                    println!("results: {key:?}: {:+}", cursor.weight());
                    cursor.step_key();
                }
            });

            // results
            //     .index()
            //     .join::<(), _, _, OrdZSet<_, _>>(&expected.index(), |&node, &result, &expected| {
            //         (node, (expected, result))
            //     })
            //     .filter(|(_, (expected, result))| expected != result)
            //     .inspect(move |results| {
            //         let mut cursor = results.cursor();
            //         while cursor.key_valid() {
            //             let (node, (expected, result)) = *cursor.key();
            //             println!(
            //                 "results: {node} {:+}, {expected} != {result}",
            //                 cursor.weight(),
            //             );
            //             cursor.step_key();
            //         }
            //     });

            results.inspect(move |results| {
                assert_eq!(results, &data.expected);
            });
        })
        .unwrap();

        root.step().unwrap();

        let elapsed = start.elapsed();
        println!("finished dfs in {elapsed:#?}");
    })
    .join()
    .unwrap();
}
