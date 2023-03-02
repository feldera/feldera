use crate::ir::{
    function::Function, layout_cache::RowLayoutCache, types::Signature, DataflowNode, LayoutId,
    NodeId, StreamKind, StreamLayout,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Map {
    input: NodeId,
    map_fn: Function,
    layout: LayoutId,
}

impl Map {
    pub const fn new(input: NodeId, map_fn: Function, layout: LayoutId) -> Self {
        Self {
            input,
            map_fn,
            layout,
        }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn map_fn(&self) -> &Function {
        &self.map_fn
    }

    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

impl DataflowNode for Map {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(match inputs[0] {
            StreamLayout::Set(_) => StreamLayout::Set(self.layout),
            StreamLayout::Map(key_layout, _) => StreamLayout::Map(key_layout, self.layout),
        })
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        self.map_fn.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(self.map_fn());
    }

    fn functions_mut<'a>(&'a mut self, functions: &mut Vec<&'a mut Function>) {
        functions.push(&mut self.map_fn);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.layout);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout = mappings[&self.layout];
        self.map_fn.remap_layouts(mappings);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Filter {
    input: NodeId,
    filter_fn: Function,
}

impl Filter {
    pub fn new(input: NodeId, filter_fn: Function) -> Self {
        Self { input, filter_fn }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn filter_fn(&self) -> &Function {
        &self.filter_fn
    }
}

impl DataflowNode for Filter {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(match inputs[0] {
            StreamLayout::Set(value) => StreamLayout::Set(value),
            StreamLayout::Map(key, value) => StreamLayout::Map(key, value),
        })
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        self.filter_fn.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(&self.filter_fn);
    }

    fn functions_mut<'a>(&'a mut self, functions: &mut Vec<&'a mut Function>) {
        functions.push(&mut self.filter_fn);
    }

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.filter_fn.remap_layouts(mappings);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FilterMap {
    input: NodeId,
    filter_map: Function,
    layout: LayoutId,
}

impl FilterMap {
    pub fn new(input: NodeId, filter_map: Function, layout: LayoutId) -> Self {
        Self {
            input,
            filter_map,
            layout,
        }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn filter_map(&self) -> &Function {
        &self.filter_map
    }

    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

impl DataflowNode for FilterMap {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(match inputs[0] {
            StreamLayout::Set(_) => StreamLayout::Set(self.layout),
            StreamLayout::Map(key_layout, _) => StreamLayout::Map(key_layout, self.layout),
        })
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        self.filter_map.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(self.filter_map());
    }

    fn functions_mut<'a>(&'a mut self, functions: &mut Vec<&'a mut Function>) {
        functions.push(&mut self.filter_map);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.layout);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout = mappings[&self.layout];
        self.filter_map.remap_layouts(mappings);
    }
}
