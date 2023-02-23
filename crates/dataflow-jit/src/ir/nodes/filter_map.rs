use crate::ir::{
    function::Function, layout_cache::RowLayoutCache, types::Signature, DataflowNode, LayoutId,
    NodeId, Stream, StreamKind,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Map {
    input: NodeId,
    map: Function,
    layout: LayoutId,
}

impl Map {
    pub const fn new(input: NodeId, map: Function, layout: LayoutId) -> Self {
        Self { input, map, layout }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn map_fn(&self) -> &Function {
        &self.map
    }

    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

impl DataflowNode for Map {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(match inputs[0] {
            Stream::Set(_) => Stream::Set(self.layout),
            Stream::Map(key_layout, _) => Stream::Map(key_layout, self.layout),
        })
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], layout_cache: &RowLayoutCache) {
        self.map.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(self.map_fn());
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.layout);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Filter {
    input: NodeId,
    filter: Function,
}

impl Filter {
    pub fn new(input: NodeId, filter: Function) -> Self {
        Self { input, filter }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn filter_fn(&self) -> &Function {
        &self.filter
    }
}

impl DataflowNode for Filter {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(match inputs[0] {
            Stream::Set(value) => Stream::Set(value),
            Stream::Map(key, value) => Stream::Map(key, value),
        })
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], layout_cache: &RowLayoutCache) {
        self.filter.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(self.filter_fn());
    }

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
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

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(match inputs[0] {
            Stream::Set(_) => Stream::Set(self.layout),
            Stream::Map(key_layout, _) => Stream::Map(key_layout, self.layout),
        })
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], layout_cache: &RowLayoutCache) {
        self.filter_map.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(self.filter_map());
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.layout);
    }
}
