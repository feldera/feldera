use std::{
    fs::File,
    io::{BufRead, BufReader},
};

pub type Vertex = u64;

#[derive(Debug, Default)]
pub struct Properties {
    vertex_file: String,
    edge_file: String,
    vertices: u64,
    edges: u64,
    directed: bool,
    source_vertex: Vertex,
}

impl Properties {
    pub fn from_file(dataset: &str, file: File) -> Self {
        let mut vertex_file = None;
        let mut edge_file = None;
        let mut vertices = None;
        let mut edges = None;
        let mut directed = None;
        let mut source_vertex = None;

        for line in BufReader::new(file).lines() {
            let line = line.unwrap();
            let line = line.trim();

            if line.starts_with('#') || line.is_empty() {
                continue;
            }

            // Remove `graph.{dataset}.` from every property
            let line = line
                .trim_start_matches("graph.")
                .trim_start_matches(dataset)
                .trim_start_matches('.');

            let (_, value) = line.split_once('=').unwrap();
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
            }
        }

        Self {
            vertex_file: vertex_file.unwrap(),
            edge_file: edge_file.unwrap(),
            vertices: vertices.unwrap(),
            edges: edges.unwrap(),
            directed: directed.unwrap(),
            source_vertex: source_vertex.unwrap(),
        }
    }
}

pub struct EdgeParser {
    file: BufReader<File>,
}

impl EdgeParser {
    pub fn new(file: File) -> Self {
        Self {
            file: BufReader::new(file),
        }
    }

    pub fn parse<F>(self, mut append: F)
    where
        F: FnMut(Vertex, Vertex, f64),
    {
        for line in self.file.lines() {
            let line = line.unwrap();
            let mut line = line.splitn(3, ' ');
            let src = line.next().unwrap().parse().unwrap();
            let dest = line.next().unwrap().parse().unwrap();
            let weight = line.next().unwrap().parse().unwrap();
            append(src, dest, weight);
        }
    }
}

pub struct VertexParser {
    file: BufReader<File>,
}

impl VertexParser {
    pub fn new(file: File) -> Self {
        Self {
            file: BufReader::new(file),
        }
    }

    pub fn parse<F>(self, mut append: F)
    where
        F: FnMut(Vertex),
    {
        for line in self.file.lines() {
            let line = line.unwrap();
            let vertex: Vertex = line.parse().unwrap();
            append(vertex);
        }
    }
}

pub mod datasets {
    pub const D75: DataSet = DataSet::new(
        "datagen-7_5-fb",
        "https://surfdrive.surf.nl/files/index.php/s/ypGcsxzrBeh2YGb/download",
    );
    pub const D76: DataSet = DataSet::new(
        "datagen-7_6-fb",
        "https://surfdrive.surf.nl/files/index.php/s/pxl7rDvzDQJFhfc/download",
    );
    pub const D77: DataSet = DataSet::new(
        "datagen-7_7-zf",
        "https://surfdrive.surf.nl/files/index.php/s/sstTvqgcyhWVVPn/download",
    );

    pub struct DataSet {
        pub name: &'static str,
        pub url: &'static str,
    }

    impl DataSet {
        pub const fn new(name: &'static str, url: &'static str) -> Self {
            Self { name, url }
        }
    }
}
