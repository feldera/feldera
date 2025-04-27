use std::io::Write;

use serde::{Deserialize, Serialize};
use zip::{write::FileOptions, ZipWriter};

use crate::MirNodeId;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[repr(transparent)]
pub struct LirNodeId(String);

impl LirNodeId {
    pub fn new(id: &str) -> Self {
        Self(id.to_string())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[repr(transparent)]
pub struct LirStreamId(usize);

impl LirStreamId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LirNode {
    pub id: LirNodeId,

    /// Node type, e.g., map, join, etc.
    pub operation: String,

    /// The list of Mir node ids that this LIR node implementa completely or partially.
    #[serde(default)]
    pub implements: Vec<MirNodeId>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LirEdge {
    /// Stream id if this edge is a stream edge. None if this is a dependency edge.
    /// Dependency edges connect operators that implement a single logical function,
    /// e.g., exchange sender and receiver or the input and output halves of Z1.
    pub stream_id: Option<LirStreamId>,
    pub from: LirNodeId,
    pub to: LirNodeId,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LirCircuit {
    pub nodes: Vec<LirNode>,
    pub edges: Vec<LirEdge>,
}

impl LirCircuit {
    pub fn as_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    pub fn as_zip(&self) -> Vec<u8> {
        let json = self.as_json();
        let json = json.as_bytes();

        let mut zip = ZipWriter::new(std::io::Cursor::new(Vec::with_capacity(65536)));
        zip.start_file("ir.json", FileOptions::default()).unwrap();
        zip.write_all(json).unwrap();
        zip.finish().unwrap().into_inner()
    }
}
