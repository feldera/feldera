use std::collections::BTreeMap;

use petgraph::prelude::DiGraphMap;

use crate::{
    codegen::VTable,
    ir::{NodeId, StreamKind},
};

// FIXME: Ideally each node would carry a handle to the jit runtime so that we
// could ensure we don't deallocate anything important until the dataflow is
// totally shut down

#[derive(Debug, Clone)]
pub enum DataflowNode {
    Map(Map),
    MapIndex(MapIndex),
    Filter(Filter),
    FilterMap(FilterMap),
    FilterMapIndex(FilterMapIndex),
    Sum(Sum),
    Neg(Neg),
    Sink(Sink),
    Source(Source),
    SourceMap(SourceMap),
    IndexWith(IndexWith),
    Delta0(Delta0),
    DelayedFeedback(DelayedFeedback),
    Min(Min),
    Distinct(Distinct),
    JoinCore(JoinCore),
    Subgraph(DataflowSubgraph),
    Export(Export),
    Noop(Noop),
    Minus(Minus),
    MonotonicJoin(MonotonicJoin),
    Differentiate(Differentiate),
    Integrate(Integrate),
}

#[derive(Debug, Clone)]
pub struct Differentiate {
    pub input: NodeId,
}

#[derive(Debug, Clone)]
pub struct Integrate {
    pub input: NodeId,
}

#[derive(Debug, Clone)]
pub struct Minus {
    pub lhs: NodeId,
    pub rhs: NodeId,
}

#[derive(Debug, Clone)]
pub struct Noop {
    pub input: NodeId,
}

#[derive(Debug, Clone)]
pub struct Export {
    pub input: NodeId,
}

#[derive(Debug, Clone)]
pub struct DataflowSubgraph {
    pub edges: DiGraphMap<NodeId, ()>,
    pub inputs: BTreeMap<NodeId, NodeId>,
    pub nodes: BTreeMap<NodeId, DataflowNode>,
    pub feedback_connections: BTreeMap<NodeId, NodeId>,
}

#[derive(Debug, Clone)]
pub struct JoinCore {
    pub lhs: NodeId,
    pub rhs: NodeId,
    pub join_fn: unsafe extern "C" fn(*const u8, *const u8, *const u8, *mut u8, *mut u8),
    pub key_vtable: &'static VTable,
    pub value_vtable: &'static VTable,
    pub output_kind: StreamKind,
}

#[derive(Debug, Clone)]
pub struct MonotonicJoin {
    pub lhs: NodeId,
    pub rhs: NodeId,
    pub join_fn: unsafe extern "C" fn(*const u8, *const u8, *const u8, *mut u8),
    pub key_vtable: &'static VTable,
}

#[derive(Debug, Clone)]
pub struct Min {
    pub input: NodeId,
}

#[derive(Debug, Clone)]
pub struct Distinct {
    pub input: NodeId,
}

#[derive(Debug, Clone)]
pub struct DelayedFeedback {}

#[derive(Debug, Clone)]
pub struct Source {
    pub output_vtable: &'static VTable,
}

#[derive(Debug, Clone)]
pub struct SourceMap {
    pub key_vtable: &'static VTable,
    pub value_vtable: &'static VTable,
}

#[derive(Debug, Clone)]
pub struct Sink {
    pub input: NodeId,
}

#[derive(Debug, Clone)]
pub struct IndexWith {
    pub input: NodeId,
    pub index_fn: unsafe extern "C" fn(*const u8, *mut u8, *mut u8),
    pub key_vtable: &'static VTable,
    pub value_vtable: &'static VTable,
}

#[derive(Debug, Clone)]
pub struct Map {
    pub input: NodeId,
    pub map_fn: unsafe extern "C" fn(*const u8, *mut u8),
    pub output_vtable: &'static VTable,
}

#[derive(Debug, Clone)]
pub struct MapIndex {
    pub input: NodeId,
    pub map_fn: unsafe extern "C" fn(*const u8, *const u8, *mut u8),
    pub output_vtable: &'static VTable,
}

// TODO: Maybe just enum the filter function?
#[derive(Debug, Clone)]
pub enum Filter {
    Set {
        input: NodeId,
        filter_fn: unsafe extern "C" fn(*const u8) -> bool,
    },
    Map {
        input: NodeId,
        filter_fn: unsafe extern "C" fn(*const u8, *const u8) -> bool,
    },
}

impl Filter {
    pub const fn input(&self) -> NodeId {
        match *self {
            Self::Set { input, .. } | Self::Map { input, .. } => input,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterMap {
    pub input: NodeId,
    pub filter_map: unsafe extern "C" fn(*const u8, *mut u8) -> bool,
    pub output_vtable: &'static VTable,
}

#[derive(Debug, Clone)]
pub struct FilterMapIndex {
    pub input: NodeId,
    pub filter_map: unsafe extern "C" fn(*const u8, *const u8, *mut u8) -> bool,
    pub output_vtable: &'static VTable,
}

#[derive(Debug, Clone)]
pub struct Neg {
    pub input: NodeId,
}

#[derive(Debug, Clone)]
pub struct Sum {
    pub inputs: Vec<NodeId>,
}

#[derive(Debug, Clone)]
pub struct Delta0 {
    pub input: NodeId,
}
