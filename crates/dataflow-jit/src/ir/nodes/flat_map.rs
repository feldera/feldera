// Basic sketch:
// - Flat map fn gets its normal params plus four additional pointers,
//   `keys: &mut Vec<Row>`, `values: &mut Vec<Row>` and the vtable pointers
//   for the key and value rows
// - User calls `@dbsp.row.vec.push(vec: &mut Vec<Row>, row_ptr: *mut RawRow)`
//   to push an element to a vec
// - User produces outputs with successive `@dbsp.row.vec.push()` calls
//   to key and value vecs
// - `@dbsp.row.vec.push(vec: &mut Vec<Row>, row_ptr: *mut RawRow)`, takes
//   the raw data of a row, allocates a `Row`, copies the raw data into it
//   and sets the proper vtable pointer before pushing the row value to the vec

use crate::ir::{
    DataflowNode, Function, LayoutId, NodeId, RowLayoutCache, Signature, StreamKind, StreamLayout,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FlatMap {
    input: NodeId,
    flat_map: Function,
    output: StreamLayout,
    // TODO: Allow the compiler to infer an output size hint
    //       to enable buffer pre-allocation
}

impl FlatMap {
    pub fn new(input: NodeId, flat_map: Function, output: StreamLayout) -> Self {
        Self {
            input,
            flat_map,
            output,
        }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn flat_map(&self) -> &Function {
        &self.flat_map
    }

    pub const fn output(&self) -> StreamLayout {
        self.output
    }
}

impl DataflowNode for FlatMap {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(self.output.kind())
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(self.output)
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        self.flat_map.optimize(layout_cache);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        match self.output {
            StreamLayout::Set(key) => layouts.push(key),
            StreamLayout::Map(key, value) => layouts.extend([key, value]),
        }
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        match &mut self.output {
            StreamLayout::Set(key) => *key = mappings[key],
            StreamLayout::Map(key, value) => {
                *key = mappings[key];
                *value = mappings[value];
            }
        }
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(&self.flat_map);
    }

    fn functions_mut<'a>(&'a mut self, functions: &mut Vec<&'a mut Function>) {
        functions.push(&mut self.flat_map);
    }
}
