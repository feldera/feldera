use crate::{codegen::VTable, ir::NodeId};

// FIXME: Ideally each node would carry a handle to the jit runtime so that we
// could ensure we don't deallocate anything important until the dataflow is
// totally shut down

#[derive(Clone)]
pub enum DataflowNode {
    Map(Map),
    MapIndex(MapIndex),
    Sum(Sum),
    Neg(Neg),
    Sink(Sink),
    Source(Source),
    Filter(Filter),
    FilterIndex(FilterIndex),
    IndexWith(IndexWith),
}

#[derive(Clone)]
pub struct Source {
    pub output_vtable: &'static VTable,
}

#[derive(Clone)]
pub struct Sink {
    pub input: NodeId,
}

#[derive(Clone)]
pub struct IndexWith {
    pub input: NodeId,
    pub index_fn: unsafe extern "C" fn(*const u8, *mut u8, *mut u8),
    pub key_vtable: &'static VTable,
    pub value_vtable: &'static VTable,
}

#[derive(Clone)]
pub struct Map {
    pub input: NodeId,
    pub map_fn: unsafe extern "C" fn(*const u8, *mut u8),
    pub output_vtable: &'static VTable,
}

#[derive(Clone)]
pub struct MapIndex {
    pub input: NodeId,
    pub map_fn: unsafe extern "C" fn(*const u8, *const u8, *mut u8),
    pub output_vtable: &'static VTable,
}

#[derive(Clone)]
pub struct Filter {
    pub input: NodeId,
    pub filter_fn: unsafe extern "C" fn(*const u8) -> bool,
}

#[derive(Clone)]
pub struct FilterIndex {
    pub input: NodeId,
    pub filter_fn: unsafe extern "C" fn(*const u8, *const u8) -> bool,
}

#[derive(Clone)]
pub struct Neg {
    pub input: NodeId,
}

#[derive(Clone)]
pub struct Sum {
    pub inputs: Vec<NodeId>,
}
