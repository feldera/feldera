use clap::{PossibleValue, ValueEnum};
use dbsp::{
    algebra::{HasOne, HasZero},
    trace::{Batch, Batcher},
    Circuit, OrdIndexedZSet, OrdZSet, Stream,
};
use deepsize::DeepSizeOf;
use indicatif::{ProgressBar, ProgressStyle};
use ordered_float::OrderedFloat;
use std::{
    fmt::{self, Debug, Display},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    iter::{Product, Sum},
    ops::{Add, AddAssign, Div, Mul, Neg, Sub},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};
use tar::Archive;
use zstd::Decoder;

pub type Node = u64;
/// Pagerank must use 64bit float values
pub type Rank = F64;
pub type Vertex = u64;
pub type Weight = isize;
pub type Distance = u64;

pub type EdgeMap = OrdIndexedZSet<Node, Node, Weight>;
pub type VertexSet = OrdZSet<Node, Weight>;
pub type DistanceSet = OrdZSet<(Node, Distance), Weight>;
pub type RankSet = OrdZSet<(Node, Rank), Weight>;
pub type RankMap = OrdIndexedZSet<Node, Rank, Weight>;

pub type Streamed<P, T> = Stream<Circuit<P>, T>;

pub type Edges<P> = Streamed<P, EdgeMap>;
pub type Ranks<P> = Streamed<P, RankSet>;
pub type RankPairs<P> = Streamed<P, RankMap>;
pub type Vertices<P> = Streamed<P, VertexSet>;

const DATA_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/benches/ldbc-graphalytics-data",
);

#[derive(Debug, Clone, Copy)]
pub struct DataSet {
    pub name: &'static str,
    pub url: &'static str,
}

impl DataSet {
    pub const fn new(name: &'static str, url: &'static str) -> Self {
        Self { name, url }
    }

    pub fn path(&self) -> PathBuf {
        Path::new(DATA_PATH).join(self.name)
    }

    pub fn load<R: ResultParser>(&self) -> io::Result<(Properties, EdgeMap, VertexSet, R::Parsed)> {
        let dataset_dir = self.dataset_dir()?;

        // Open & parse the properties file
        let properties_path = dataset_dir.join(format!("{}.properties", self.name));
        let properties_file = File::open(&properties_path).unwrap_or_else(|error| {
            panic!("failed to open {}: {error}", properties_path.display())
        });
        let properties = Properties::from_file(self.name, properties_file);

        // Open the edges file
        let edges_path = dataset_dir.join(&properties.edge_file);
        let edges = File::open(&edges_path)
            .unwrap_or_else(|error| panic!("failed to open {}: {error}", edges_path.display()));

        // Open the vertices file
        let vertices_path = dataset_dir.join(&properties.vertex_file);
        let vertices = File::open(&vertices_path)
            .unwrap_or_else(|error| panic!("failed to open {}: {error}", vertices_path.display()));

        // Load the edges and vertices in parallel
        let edges_handle =
            thread::spawn(move || EdgeParser::new(edges, properties.directed).load());

        let (vertices, results) = if let Some(suffix) = R::file_suffix() {
            let vertices_handle = thread::spawn(move || VertexParser::new(vertices).load());

            // Open the results file
            let result_path = dataset_dir.join(format!("{}{suffix}", self.name));
            let result_file = File::open(&result_path).unwrap_or_else(|error| {
                panic!("failed to open {}: {error}", result_path.display())
            });

            // Parse the results file in parallel to the vertices file
            let results = R::load(result_file);
            let vertices = vertices_handle.join().unwrap();

            (vertices, results)

        // Otherwise parse the vertices file on this thread
        } else {
            let vertices = VertexParser::new(vertices).load();
            (vertices, R::Parsed::default())
        };

        // Wait for the vertex and edge threads to finish parsing
        let edges = edges_handle.join().unwrap();

        Ok((properties, edges, vertices, results))
    }

    pub fn load_results<R: ResultParser>(&self) -> io::Result<R::Parsed> {
        if let Some(suffix) = R::file_suffix() {
            let dataset_dir = self.dataset_dir()?;

            let result_path = dataset_dir.join(format!("{}{suffix}", self.name));
            let result_file = File::open(&result_path).unwrap_or_else(|error| {
                panic!("failed to open {}: {error}", result_path.display())
            });

            Ok(R::load(result_file))
        } else {
            Ok(R::Parsed::default())
        }
    }

    /// Gets the dataset's directory if it exists or downloads and extracts it
    ///
    /// The full data repository is stored [here], the downloads can be *very* slow
    ///
    /// [here]: https://repository.surfsara.nl/datasets/cwi/graphalytics
    fn dataset_dir(&self) -> io::Result<PathBuf> {
        let data_path = self.path();
        let archive_path = Path::new(DATA_PATH).join(format!("{}.tar.zst", self.name));
        let tarball_path = Path::new(DATA_PATH).join(format!("{}.tar", self.name));

        fs::create_dir_all(&data_path)?;

        // If it doesn't exist, download the dataset
        if !archive_path.exists() && !tarball_path.exists() {
            let mut archive_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&archive_path)
                .unwrap_or_else(|error| {
                    panic!("failed to create {}: {error}", archive_path.display())
                });
            let mut writer = BufWriter::new(&mut archive_file);

            // Download and write the archive to disk
            println!(
                "\ndownloading {} from {}, this may take a while",
                self.name, self.url
            );
            let response = reqwest::blocking::get(self.url)
                .unwrap_or_else(|error| panic!("failed to download {}: {error}", self.url));

            let progress = if let Some(content_length) = response.content_length() {
                let progress = ProgressBar::new(content_length);
                progress.enable_steady_tick(Duration::from_millis(300));
                progress.set_style(
                    ProgressStyle::with_template(
                        "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})",
                    )
                    .unwrap()
                    .with_key("eta", |state| format!("{:.1}s", state.eta().as_secs_f64()))
                    .progress_chars("#>-"),
                );

                progress
            } else {
                todo!()
            };

            let mut response = BufReader::new(response);
            loop {
                let chunk = response.fill_buf()?;
                // `.fill_buf()` returns an empty slice when the underlying reader is done
                if chunk.is_empty() {
                    break;
                }

                writer.write_all(chunk)?;

                // Consume the chunk's bytes
                let chunk_len = chunk.len();
                progress.inc(chunk_len as u64);
                response.consume(chunk_len);
            }

            // Flush the writer
            writer
                .flush()
                .unwrap_or_else(|error| panic!("failed to flush {} to disk: {error}", self.url));
            progress.finish_with_message("done");
        }

        if !tarball_path.exists() && fs::read_dir(&data_path).unwrap().count() == 0 {
            // Note that we're *opening* the file and not *creating* it
            let archive_file = BufReader::new(File::open(&archive_path).unwrap_or_else(|error| {
                panic!("failed to create {}: {error}", archive_path.display())
            }));

            // Decompress the zstd-compressed tarball
            let mut decoder = Decoder::new(archive_file)?;
            let mut tarball = BufWriter::new(File::create(&tarball_path)?);
            io::copy(&mut decoder, &mut tarball)?;

            // TODO: Maybe want to delete the original zsd file?
        }

        // TODO: Finer-grained check for the files we care about
        if fs::read_dir(&data_path).unwrap().count() == 0 {
            // Note that we're *opening* the file and not *creating* it
            let archive_file = BufReader::new(File::open(&tarball_path).unwrap_or_else(|error| {
                panic!("failed to create {}: {error}", archive_path.display())
            }));

            // Open the archive
            let mut tar_archive = Archive::new(archive_file);

            // Extract the archive
            println!(
                "extracting {} to {}",
                archive_path.display(),
                data_path.display(),
            );
            tar_archive.unpack(&data_path).unwrap_or_else(|error| {
                panic!(
                    "failed to extract '{}' to '{}': {error}",
                    archive_path.display(),
                    data_path.display(),
                )
            });

            // TODO: Maybe want to delete the original tarball?
        }

        Ok(data_path)
    }

    pub const DATASETS: [Self; 11] = [
        Self::EXAMPLE_DIR,
        Self::EXAMPLE_UNDIR,
        Self::DATAGEN_7_5,
        Self::DATAGEN_7_6,
        Self::DATAGEN_7_7,
        Self::DATAGEN_8_2,
        Self::DATAGEN_8_3,
        Self::DATAGEN_8_4,
        Self::DATAGEN_8_5,
        Self::GRAPH_500_23,
        Self::GRAPH_500_24,
    ];

    pub const EXAMPLE_DIR: DataSet = DataSet::new(
        "example-directed",
        "https://surfdrive.surf.nl/files/index.php/s/7hGIIZ6nzxgi0dU/download",
    );

    pub const EXAMPLE_UNDIR: DataSet = DataSet::new(
        "example-undirected",
        "https://surfdrive.surf.nl/files/index.php/s/enKFbXmUBP2rxgB/download",
    );

    pub const DATAGEN_7_5: DataSet = DataSet::new(
        "datagen-7_5-fb",
        "https://surfdrive.surf.nl/files/index.php/s/ypGcsxzrBeh2YGb/download",
    );

    pub const DATAGEN_7_6: DataSet = DataSet::new(
        "datagen-7_6-fb",
        "https://surfdrive.surf.nl/files/index.php/s/pxl7rDvzDQJFhfc/download",
    );

    pub const DATAGEN_7_7: DataSet = DataSet::new(
        "datagen-7_7-zf",
        "https://surfdrive.surf.nl/files/index.php/s/sstTvqgcyhWVVPn/download",
    );

    pub const DATAGEN_8_2: DataSet = DataSet::new(
        "datagen-8_2-zf",
        "https://repository.surfsara.nl/datasets/cwi/graphalytics/files/graphalytics-graph-data-sets/datagen-8_2-zf.tar.zst",
    );

    pub const DATAGEN_8_3: DataSet = DataSet::new(
        "datagen-8_3-zf",
        "https://repository.surfsara.nl/datasets/cwi/graphalytics/files/graphalytics-graph-data-sets/datagen-8_3-zf.tar.zst",
    );

    pub const DATAGEN_8_4: DataSet = DataSet::new(
        "datagen-8_4-fb",
        "https://repository.surfsara.nl/datasets/cwi/graphalytics/files/graphalytics-graph-data-sets/datagen-8_4-fb.tar.zst",
    );

    pub const DATAGEN_8_5: DataSet = DataSet::new(
        "datagen-8_5-fb",
        "https://surfdrive.surf.nl/files/index.php/s/2d8wUj9HGIzime3/download",
    );

    pub const GRAPH_500_23: Self = Self::new(
        "graph500-23",
        "https://repository.surfsara.nl/datasets/cwi/graphalytics/files/graphalytics-graph-data-sets/graph500-23.tar.zst",
    );

    pub const GRAPH_500_24: Self = Self::new(
        "graph500-24",
        "https://repository.surfsara.nl/datasets/cwi/graphalytics/files/graphalytics-graph-data-sets/graph500-24.tar.zst",
    );
}

impl ValueEnum for DataSet {
    fn value_variants<'a>() -> &'a [Self] {
        &Self::DATASETS
    }

    fn to_possible_value<'a>(&self) -> Option<PossibleValue<'a>> {
        Some(PossibleValue::new(self.name))
    }
}

impl Default for DataSet {
    fn default() -> Self {
        Self::EXAMPLE_DIR
    }
}

#[derive(Debug, Default)]
pub struct Properties {
    pub vertex_file: String,
    pub edge_file: String,
    pub vertices: u64,
    pub edges: u64,
    pub directed: bool,
    pub source_vertex: Vertex,
    pub algorithms: Vec<Algorithm>,
    pub pagerank_damping_factor: Option<f64>,
    pub pagerank_iters: Option<usize>,
}

impl Properties {
    pub fn from_file(dataset: &str, file: File) -> Self {
        let mut vertex_file = None;
        let mut edge_file = None;
        let mut vertices = None;
        let mut edges = None;
        let mut directed = None;
        let mut source_vertex = None;
        let mut algorithms = Vec::new();
        let mut pagerank_iters = None;
        let mut pagerank_damping_factor = None;

        let mut file = BufReader::new(file);
        let mut buffer = String::with_capacity(256);

        while let Ok(n) = file.read_line(&mut buffer) {
            if n == 0 {
                break;
            }
            let line = buffer.trim();

            if !(line.starts_with('#') || line.is_empty()) {
                // Remove `graph.{dataset}.` from every property
                let line = line
                    .trim_start_matches("graph.")
                    .trim_start_matches(dataset)
                    .trim_start_matches('.');

                let (_, value) = line.split_once('=').unwrap();
                let value = value.trim();

                if line.starts_with("bfs.source-vertex") {
                    source_vertex = Some(value.parse().unwrap());
                } else if line.starts_with("directed") {
                    directed = Some(value.parse().unwrap());
                } else if line.starts_with("vertex-file") {
                    vertex_file = Some(value.to_owned());
                } else if line.starts_with("edge-file") {
                    edge_file = Some(value.to_owned());
                } else if line.starts_with("meta.vertices") {
                    vertices = Some(value.parse().unwrap());
                } else if line.starts_with("meta.edges") {
                    edges = Some(value.parse().unwrap());
                } else if line.starts_with("algorithms") {
                    algorithms.extend(
                        value
                            .split(',')
                            .map(|algo| Algorithm::try_from(algo.trim()).unwrap()),
                    );
                } else if line.starts_with("pr.damping-factor") {
                    pagerank_damping_factor = Some(value.parse().unwrap());
                } else if line.starts_with("pr.num-iterations") {
                    pagerank_iters = Some(value.parse().unwrap());
                }
            }

            buffer.clear();
        }

        Self {
            vertex_file: vertex_file.unwrap(),
            edge_file: edge_file.unwrap(),
            vertices: vertices.unwrap(),
            edges: edges.unwrap(),
            directed: directed.unwrap(),
            source_vertex: source_vertex.unwrap(),
            algorithms,
            pagerank_damping_factor,
            pagerank_iters,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Algorithm {
    Pr,
    Bfs,
    Lcc,
    Wcc,
    Cdlp,
    Sssp,
}

impl TryFrom<&str> for Algorithm {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match &*value.to_ascii_lowercase() {
            "pr" => Self::Pr,
            "bfs" => Self::Bfs,
            "lcc" => Self::Lcc,
            "wcc" => Self::Wcc,
            "cdlp" => Self::Cdlp,
            "sssp" => Self::Sssp,
            unknown => return Err(format!("unknown algorithm: {unknown:?}")),
        })
    }
}

struct EdgeParser {
    file: BufReader<File>,
    directed: bool,
}

impl EdgeParser {
    pub fn new(file: File, directed: bool) -> Self {
        Self {
            file: BufReader::new(file),
            directed,
        }
    }

    pub fn load(self) -> EdgeMap {
        let mut edges = <EdgeMap as Batch>::Batcher::new(());
        let mut batch = Vec::with_capacity(1024);

        let directed = self.directed;
        self.parse(|src, dest| {
            batch.push(((src, dest), Weight::one()));

            // Add in the reversed edge if the graph isn't directed
            if !directed {
                batch.push(((dest, src), Weight::one()));
            }

            if batch.len() + 1 + directed as usize >= 1024 {
                edges.push_batch(&mut batch);
            }
        });
        edges.push_batch(&mut batch);

        edges.seal()
    }

    fn parse<F>(mut self, mut append: F)
    where
        F: FnMut(Vertex, Vertex),
    {
        let mut buffer = String::with_capacity(256);
        while let Ok(n) = self.file.read_line(&mut buffer) {
            if n == 0 {
                break;
            }

            let line = buffer.trim_end();
            let mut line = line.splitn(3, ' ');

            let src = line.next().unwrap().parse().unwrap();
            let dest = line.next().unwrap().parse().unwrap();
            // let weight = line.next().and_then(|weight| weight.parse().ok());
            append(src, dest);

            buffer.clear();
        }
    }
}

struct VertexParser {
    file: BufReader<File>,
}

impl VertexParser {
    pub fn new(file: File) -> Self {
        Self {
            file: BufReader::new(file),
        }
    }

    pub fn load(self) -> VertexSet {
        let mut edges = <VertexSet as Batch>::Batcher::new(());
        let mut batch = Vec::with_capacity(1024);

        self.parse(|vertex| {
            batch.push(((vertex, ()), Weight::one()));

            if batch.len() + 1 >= 1024 {
                edges.push_batch(&mut batch);
            }
        });
        edges.push_batch(&mut batch);

        edges.seal()
    }

    fn parse<F>(mut self, mut append: F)
    where
        F: FnMut(Vertex),
    {
        let mut buffer = String::with_capacity(256);
        while let Ok(n) = self.file.read_line(&mut buffer) {
            if n == 0 {
                break;
            }

            let line = buffer.trim_end();
            let vertex: Vertex = line.parse().unwrap();
            append(vertex);

            buffer.clear();
        }
    }
}

pub trait ResultParser {
    type Parsed: Default;

    fn file_suffix() -> Option<&'static str>;

    fn load(file: File) -> Self::Parsed;
}

pub struct NoopResults;

impl ResultParser for NoopResults {
    type Parsed = ();

    fn file_suffix() -> Option<&'static str> {
        None
    }

    fn load(_file: File) -> Self::Parsed {}
}

pub struct BfsResults;

impl ResultParser for BfsResults {
    type Parsed = DistanceSet;

    fn file_suffix() -> Option<&'static str> {
        Some("-BFS")
    }

    fn load(file: File) -> Self::Parsed {
        let mut file = BufReader::new(file);

        // TODO: Can we estimate the capacity any better?
        let mut results = <DistanceSet as Batch>::Batcher::new(());
        let mut batch = Vec::with_capacity(1024);

        let mut buffer = String::with_capacity(256);
        while let Ok(n) = file.read_line(&mut buffer) {
            if n == 0 {
                break;
            }

            let line = buffer.trim_end();
            let (vertex, distance) = line.split_once(' ').unwrap();

            let vertex = vertex.parse().unwrap();
            let distance = distance.parse().unwrap();

            batch.push((((vertex, distance), ()), Weight::one()));
            if batch.len() + 1 >= 1024 {
                results.push_batch(&mut batch);
            }

            buffer.clear();
        }

        results.push_batch(&mut batch);
        results.seal()
    }
}

pub struct PageRankResults;

impl ResultParser for PageRankResults {
    type Parsed = RankMap;

    fn file_suffix() -> Option<&'static str> {
        Some("-PR")
    }

    fn load(file: File) -> Self::Parsed {
        let mut file = BufReader::new(file);

        // TODO: Can we estimate the capacity any better?
        let mut results = <RankMap as Batch>::Batcher::new(());
        let mut batch = Vec::with_capacity(1024);

        let mut buffer = String::with_capacity(256);
        while let Ok(n) = file.read_line(&mut buffer) {
            if n == 0 {
                break;
            }

            let line = buffer.trim_end();
            let (vertex, distance) = line.split_once(' ').unwrap();

            let vertex = vertex.parse().unwrap();
            let rank = F64::new(distance.parse::<f64>().unwrap());

            batch.push(((vertex, rank), Weight::one()));
            if batch.len() + 1 >= 1024 {
                results.push_batch(&mut batch);
            }

            buffer.clear();
        }

        results.push_batch(&mut batch);
        results.seal()
    }
}

// Custom f64 wrapper for ordering and DeepSizeOf
#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct F64(OrderedFloat<f64>);

impl F64 {
    #[inline]
    pub const fn new(float: f64) -> Self {
        Self(OrderedFloat(float))
    }

            #[inline]
    #[rustfmt::skip]
    pub const fn inner(self) -> f64 {
        self.0.0
    }
}

impl PartialEq<f64> for F64 {
    #[inline]
    fn eq(&self, other: &f64) -> bool {
        *self == F64::new(*other)
    }
}

impl PartialEq<F64> for f64 {
    #[inline]
    fn eq(&self, other: &F64) -> bool {
        F64::new(*self) == *other
    }
}

impl Add for F64 {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl Add<&F64> for F64 {
    type Output = Self;

    #[inline]
    fn add(self, rhs: &Self) -> Self::Output {
        self + *rhs
    }
}

impl<'a> Add<&'a F64> for &'a F64 {
    type Output = F64;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        *self + *rhs
    }
}

impl AddAssign for F64 {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

impl AddAssign<&'_ F64> for F64 {
    fn add_assign(&mut self, rhs: &Self) {
        *self = *self + *rhs;
    }
}

impl Sub for F64 {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl Sub<f64> for F64 {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: f64) -> Self::Output {
        self - Self::new(rhs)
    }
}

impl Sub<&F64> for F64 {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: &Self) -> Self::Output {
        self - *rhs
    }
}

impl<'a> Sub<&'a F64> for &'a F64 {
    type Output = F64;

    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        *self - *rhs
    }
}

impl Mul for F64 {
    type Output = Self;

    #[inline]
    fn mul(self, rhs: Self) -> Self::Output {
        Self(self.0 * rhs.0)
    }
}

impl Mul<&F64> for F64 {
    type Output = Self;

    #[inline]
    fn mul(self, rhs: &Self) -> Self::Output {
        self * *rhs
    }
}

impl<'a> Mul<&'a F64> for &'a F64 {
    type Output = F64;

    #[inline]
    fn mul(self, rhs: Self) -> Self::Output {
        *self * *rhs
    }
}

impl Mul<f64> for F64 {
    type Output = Self;

    #[inline]
    fn mul(self, rhs: f64) -> Self::Output {
        self * Self::new(rhs)
    }
}

impl Mul<F64> for f64 {
    type Output = F64;

    #[inline]
    fn mul(self, rhs: F64) -> Self::Output {
        F64::new(self) * rhs
    }
}

impl Mul<&F64> for f64 {
    type Output = F64;

    #[inline]
    fn mul(self, rhs: &F64) -> Self::Output {
        F64::new(self) * *rhs
    }
}

impl Div for F64 {
    type Output = Self;

    #[inline]
    fn div(self, rhs: Self) -> Self::Output {
        Self(self.0 / rhs.0)
    }
}

impl Div<&F64> for F64 {
    type Output = Self;

    #[inline]
    fn div(self, rhs: &Self) -> Self::Output {
        self / *rhs
    }
}

impl Div<f64> for F64 {
    type Output = Self;

    #[inline]
    fn div(self, rhs: f64) -> Self::Output {
        self / Self::new(rhs)
    }
}

impl Div<F64> for f64 {
    type Output = F64;

    #[inline]
    fn div(self, rhs: F64) -> Self::Output {
        F64::new(self) / rhs
    }
}

impl Div<&F64> for f64 {
    type Output = F64;

    #[inline]
    fn div(self, rhs: &F64) -> Self::Output {
        F64::new(self) / *rhs
    }
}

impl Neg for F64 {
    type Output = Self;

    #[inline]
    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}

impl Neg for &F64 {
    type Output = F64;

    #[inline]
    fn neg(self) -> Self::Output {
        F64(-self.0)
    }
}

impl HasZero for F64 {
    #[inline]
    fn zero() -> Self {
        Self::new(0.0)
    }

    #[inline]
    fn is_zero(&self) -> bool {
        // TODO: Should this use an epsilon comparison?
        *self == 0.0
    }
}

impl HasOne for F64 {
    #[inline]
    fn one() -> Self {
        Self::new(1.0)
    }
}

impl DeepSizeOf for F64 {
    #[inline]
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        0
    }
}

impl Sum for F64 {
    #[inline]
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::zero(), |a, b| a + b)
    }
}

impl<'a> Sum<&'a F64> for F64 {
    #[inline]
    fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        iter.fold(Self::zero(), |a, b| a + b)
    }
}

impl Product for F64 {
    #[inline]
    fn product<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::one(), |a, b| a * b)
    }
}

impl<'a> Product<&'a F64> for F64 {
    #[inline]
    fn product<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        iter.fold(Self::one(), |a, b| a * b)
    }
}

impl Debug for F64 {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0.0, f)
    }
}

impl Display for F64 {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0.0, f)
    }
}
