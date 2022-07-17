mod bfs;
mod data;

use crate::data::DataSet;
use clap::Parser;
use dbsp::{
    circuit::{Root, Runtime},
    operator::Generator,
    zset_set,
};
use std::{cell::RefCell, io::Write, rc::Rc, time::Instant};

fn main() {
    // TODO: Parse args from cli
    let dataset = DataSet::D76;

    println!("loading dataset {}...", dataset.name);
    let start = Instant::now();

    let (properties, edges, vertices, expected_output) = dataset.load().unwrap();
    let mut root_data = Some(zset_set! { properties.source_vertex });
    let mut vertex_data = Some(vertices);
    let mut edge_data = Some(edges);

    let elapsed = start.elapsed();
    println!("finished in {elapsed:#?}");

    println!(
        "using dataset {} with {} vertices and {} edges",
        dataset.name, properties.vertices, properties.edges,
    );
    Runtime::run(1, move |_runtime, _index| {
        print!("building dataflow... ");
        std::io::stdout().flush().unwrap();
        let start = Instant::now();

        let output = Rc::new(RefCell::new(None));

        let output2 = output.clone();
        let root = Root::build(move |circuit| {
            let roots = circuit.add_source(Generator::new(move || root_data.take().unwrap()));
            let vertices = circuit.add_source(Generator::new(move || vertex_data.take().unwrap()));
            let edges = circuit.add_source(Generator::new(move || edge_data.take().unwrap()));

            bfs::bfs(roots, vertices, edges)
                .inspect(move |results| *output2.borrow_mut() = Some(results.clone()));
        })
        .unwrap();

        let elapsed = start.elapsed();
        println!("finished in {elapsed:#?}");

        print!("running dfs benchmark... ");
        std::io::stdout().flush().unwrap();
        let start = Instant::now();

        root.step().unwrap();

        let elapsed = start.elapsed();
        println!("finished in {elapsed:#?}");

        assert_eq!(output.borrow_mut().take().unwrap(), expected_output);
    })
    .join()
    .unwrap();
}

#[derive(Debug, Clone, Parser)]
struct Config {
    #[clap(
        name = "DATASET",
        value_enum,
        default_value = "DataSet::default().name"
    )]
    dataset: DataSet,
}
