//! An implementation of LDBC Graphalytics Breadth-First Search
//!
//! Benchmark specified in LDBC reference sections [2.3.1] and [A.1].
//!
//! [2.3.1]: https://arxiv.org/pdf/2011.15028v4.pdf#subsection.2.3.1
//! [A.1]: https://arxiv.org/pdf/2011.15028v4.pdf#section.A.1

use crate::{Weight, DATA_PATH};
use dbsp::{
    algebra::HasOne,
    operator::recursive::RecursiveStreams,
    time::NestedTimestamp32,
    trace::{
        ord::{indexed_zset_batch::OrdIndexedZSetBuilder, zset_batch::OrdZSetBuilder},
        BatchReader, Builder, Cursor,
    },
    zset_set, Circuit, OrdIndexedZSet, OrdZSet, Stream,
};
use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
    process::Command,
};

type Node = u64;
type Distance = u64;

type EdgeMap = OrdIndexedZSet<Node, Node, Weight>;
type VertexSet = OrdZSet<Node, Weight>;
type DistanceSet = OrdZSet<(Node, Distance), Weight>;

pub fn bfs<P>(
    roots: Stream<Circuit<P>, VertexSet>,
    vertices: Stream<Circuit<P>, VertexSet>,
    edges: Stream<Circuit<P>, EdgeMap>,
) -> Stream<Circuit<P>, DistanceSet>
where
    P: Clone + 'static,
    Stream<Circuit<Circuit<P>>, DistanceSet>:
        RecursiveStreams<Circuit<Circuit<P>>, Output = Stream<Circuit<P>, DistanceSet>>,
{
    // Initialize the roots to have a distance of zero
    let root_nodes = roots.index_with_generic::<DistanceSet, _>(|&root| ((root, 0), ()));

    // edges.inspect(|edges| {
    //     let mut cursor = edges.cursor();
    //     while cursor.key_valid() {
    //         let key = *cursor.key();
    //         while cursor.val_valid() {
    //             let value = *cursor.val();
    //             println!("edges: {key}, {value} {:+}", cursor.weight());
    //             cursor.step_val();
    //         }
    //         cursor.step_key();
    //     }
    // });

    let distances = root_nodes
        .circuit()
        .recursive(|scope, nodes: Stream<_, DistanceSet>| {
            // Import the nodes and edges into the recursive scope
            let root_nodes = root_nodes.delta0(scope);
            let edges = edges.delta0(scope);

            let distances = nodes
                .index::<Node, Distance>()
                // Iterate over each edge within the graph, increasing the distance on each step
                .join::<NestedTimestamp32, _, _, DistanceSet>(&edges, |_, &dist, &dest| {
                    (dest, dist + 1)
                })
                // Add in the root nodes
                .plus(&root_nodes)
                .index::<Node, Distance>()
                // Select only the shortest distance to continue iterating with, `distances`
                // is sorted so we can simply select the first element
                .aggregate_incremental_nested(|&node, distances| (node, *distances[0].0));

            // distances.inspect(|distances: &DistanceSet| {
            //     let mut cursor = distances.cursor();
            //     while cursor.key_valid() {
            //         let key = *cursor.key();
            //         println!("distances (recursive): {key:?}: {:+}", cursor.weight());
            //         cursor.step_key();
            //     }
            // });

            Ok(distances)
        })
        .expect("failed to build dfs recursive scope");

    // Collect all reachable nodes
    let reachable_nodes = distances.map_keys(|&(node, _)| node);
    // Find all unreachable nodes (vertices not included in `distances`) and give them a weight of -1
    let unreachable_nodes =
        antijoin(&vertices, &reachable_nodes).map_keys(|&node| (node, i64::MAX as Distance));

    // reachable_nodes.inspect(|distances: &VertexSet| {
    //     let mut cursor = distances.cursor();
    //     while cursor.key_valid() {
    //         let key = *cursor.key();
    //         println!("reachable_nodes: {key:?}: {:+}", cursor.weight());
    //         cursor.step_key();
    //     }
    // });

    distances.plus(&unreachable_nodes)
}

// FIXME: Replace with a dedicated antijoin
fn antijoin<P>(
    vertices: &Stream<Circuit<P>, VertexSet>,
    reachable_nodes: &Stream<Circuit<P>, VertexSet>,
) -> Stream<Circuit<P>, VertexSet>
where
    P: Clone + 'static,
{
    let reachable_vertices = vertices.join::<(), _, _, _>(reachable_nodes, |&node, _, _| node);
    vertices.minus(&reachable_vertices)
}

// https://arxiv.org/pdf/2011.15028v4.pdf#subsection.3.2.1
// TODO: example-undirected https://surfdrive.surf.nl/files/index.php/s/enKFbXmUBP2rxgB/download
pub fn load_datasets(dataset_name: &str, dataset_url: &str) -> DfsData {
    let data = Path::new(DATA_PATH).canonicalize().unwrap();
    fs::create_dir_all(&data).expect("failed to create data directory");

    let required_files = [
        format!("{dataset_name}.v"),
        format!("{dataset_name}.e"),
        format!("{dataset_name}-BFS"),
    ];
    if !required_files.iter().all(|file| data.join(file).exists()) {
        let archive = data.join(format!("{dataset_name}.tar.zst"));
        if !archive.exists() {
            print!("downloading dataset... ");
            io::stdout().flush().unwrap();

            let mut contents = ureq::get(dataset_url)
                .call()
                .expect("failed to example-directed download data file")
                .into_reader();

            let mut file = BufWriter::new(
                File::create(&archive).expect("failed to create example-directed download file"),
            );

            io::copy(&mut contents, &mut file).expect("failed to write archive to disk");

            println!("done");
            // Make sure we drop & flush the file before we run tar
        }

        print!("decompressing dataset... ");
        io::stdout().flush().unwrap();

        let status = Command::new("zstd")
            .arg("-d")
            .arg(archive)
            .current_dir(&data)
            .spawn()
            .unwrap()
            .wait()
            .unwrap();

        if !status.success() {
            println!("failed");
            panic!("failed to run zstd on {dataset_name} archive");
        }

        let status = Command::new("tar")
            .arg("-xf")
            .arg(data.join(format!("{dataset_name}.tar")))
            .current_dir(&data)
            .spawn()
            .unwrap()
            .wait()
            .unwrap();

        if !status.success() {
            println!("failed");
            panic!("failed to run zstd on {dataset_name} archive");
        } else {
            println!("done");
        }
    }

    print!("reading dataset... ");
    let vertices = load(&data, &required_files[0]);
    let edges = load(&data, &required_files[1]);
    let results = load(&data, &required_files[2]);

    // TODO: Parse out directed-ness
    let data = DfsData::new(vertices, edges, results, false);
    println!("done");

    data
}

fn load(path: &Path, file: &str) -> BufReader<File> {
    // TODO: Download if file doesn't exist
    BufReader::new(
        File::open(path.join(file))
            .unwrap_or_else(|error| panic!("failed to load '{file}': {error}")),
    )
}

pub struct DfsData {
    pub roots: VertexSet,
    pub vertices: VertexSet,
    pub edges: EdgeMap,
    pub expected: DistanceSet,
}

impl DfsData {
    fn new(
        vertices_file: BufReader<File>,
        edges_file: BufReader<File>,
        results_file: BufReader<File>,
        directed: bool,
    ) -> Self {
        // Our root node is 1
        // TODO: Parse root node out of .properties file
        let roots = zset_set! { 2 };

        let mut vertices = OrdZSetBuilder::new(());
        for line in vertices_file.lines() {
            let vertex = line
                .expect("failed to read line from vertex file")
                .parse::<Node>()
                .expect("failed to parse vertex");
            vertices.push((vertex, (), Weight::one()));
        }

        let mut edges = OrdIndexedZSetBuilder::new(());
        for line in edges_file.lines() {
            let line = line.expect("failed to read line from edges file");
            let mut line = line.splitn(3, ' ');

            let src = line.next().unwrap().parse::<Node>().unwrap();
            let dest = line.next().unwrap().parse::<Node>().unwrap();
            // Weight is contained in third split as a float

            edges.push((src, dest, Weight::one()));

            // Add in the reversed edge if the graph isn't directed
            if !directed {
                edges.push((dest, src, Weight::one()));
            }
        }

        let mut results = OrdZSetBuilder::new(());
        for line in results_file.lines() {
            let line = line.expect("failed to read line from results file");
            let mut line = line.splitn(2, ' ');

            let node = line.next().unwrap().parse::<Node>().unwrap();
            let distance = line.next().unwrap().parse::<Distance>().unwrap();

            results.push(((node, distance), (), Weight::one()));
        }

        Self {
            roots,
            vertices: vertices.done(),
            edges: edges.done(),
            expected: results.done(),
        }
    }
}
