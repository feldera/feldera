use crate::ir::{
    function::Function, layout_cache::RowLayoutCache, types::Signature, DataflowNode, LayoutId,
    NodeId, StreamKind, StreamLayout,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JoinCore {
    lhs: NodeId,
    rhs: NodeId,
    // fn(key, lhs_val, rhs_val, key_out, val_out)
    join_fn: Function,
    key_layout: LayoutId,
    value_layout: LayoutId,
    output_kind: StreamKind,
}

impl JoinCore {
    pub const fn new(
        lhs: NodeId,
        rhs: NodeId,
        join_fn: Function,
        key_layout: LayoutId,
        value_layout: LayoutId,
        output_kind: StreamKind,
    ) -> Self {
        Self {
            lhs,
            rhs,
            join_fn,
            key_layout,
            value_layout,
            output_kind,
        }
    }

    pub const fn lhs(&self) -> NodeId {
        self.lhs
    }

    pub const fn rhs(&self) -> NodeId {
        self.rhs
    }

    pub const fn join_fn(&self) -> &Function {
        &self.join_fn
    }

    pub const fn key_layout(&self) -> LayoutId {
        self.key_layout
    }

    pub const fn value_layout(&self) -> LayoutId {
        self.value_layout
    }

    pub(crate) fn result_kind(&self) -> StreamKind {
        self.output_kind
    }
}

impl DataflowNode for JoinCore {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.extend([self.lhs, self.rhs]);
    }

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(self.output_kind)
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(match self.output_kind {
            StreamKind::Set => StreamLayout::Set(self.key_layout),
            StreamKind::Map => StreamLayout::Map(self.key_layout, self.value_layout),
        })
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        if self.output_kind.is_set() {
            assert_eq!(self.value_layout, layout_cache.unit());
        }
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        self.join_fn.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(&self.join_fn);
    }

    fn functions_mut<'a>(&'a mut self, functions: &mut Vec<&'a mut Function>) {
        functions.push(&mut self.join_fn);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.extend([self.key_layout, self.value_layout]);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.key_layout = mappings[&self.key_layout];
        self.value_layout = mappings[&self.value_layout];
        self.join_fn.remap_layouts(mappings);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonotonicJoin {
    lhs: NodeId,
    rhs: NodeId,
    // fn(key, lhs_val, rhs_val, key_out)
    join_fn: Function,
    key_layout: LayoutId,
}

impl MonotonicJoin {
    pub const fn new(lhs: NodeId, rhs: NodeId, join_fn: Function, key_layout: LayoutId) -> Self {
        Self {
            lhs,
            rhs,
            join_fn,
            key_layout,
        }
    }

    pub const fn lhs(&self) -> NodeId {
        self.lhs
    }

    pub const fn rhs(&self) -> NodeId {
        self.rhs
    }

    pub const fn join_fn(&self) -> &Function {
        &self.join_fn
    }

    pub const fn key_layout(&self) -> LayoutId {
        self.key_layout
    }
}

impl DataflowNode for MonotonicJoin {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.extend([self.lhs, self.rhs]);
    }

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(StreamKind::Set)
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(StreamLayout::Set(self.key_layout))
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        self.join_fn.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(&self.join_fn);
    }

    fn functions_mut<'a>(&'a mut self, functions: &mut Vec<&'a mut Function>) {
        functions.push(&mut self.join_fn);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.key_layout);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.key_layout = mappings[&self.key_layout];
        self.join_fn.remap_layouts(mappings);
    }
}
