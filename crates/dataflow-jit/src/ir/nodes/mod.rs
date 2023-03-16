mod aggregate;
mod constant;
mod differentiate;
mod filter_map;
mod flat_map;
mod index;
mod io;
mod join;
mod subgraph;
mod sum;

pub use aggregate::{Fold, Min, PartitionedRollingFold};
pub use constant::ConstantStream;
pub use differentiate::{Differentiate, Integrate};
pub use filter_map::{Filter, FilterMap, Map};
pub use flat_map::FlatMap;
pub use index::IndexWith;
pub use io::{Export, ExportedNode, Sink, Source, SourceMap};
pub use join::{JoinCore, MonotonicJoin};
pub use subgraph::Subgraph;
pub use sum::{Minus, Sum};

use crate::ir::{
    function::Function, layout_cache::RowLayoutCache, types::Signature, LayoutId, NodeId,
};
use derive_more::{IsVariant, Unwrap};
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[enum_dispatch(DataflowNode)]
#[derive(Debug, Clone, Deserialize, Serialize, IsVariant, Unwrap)]
pub enum Node {
    Map(Map),
    Min(Min),
    Neg(Neg),
    Sum(Sum),
    Fold(Fold),
    Sink(Sink),
    Minus(Minus),
    Filter(Filter),
    FilterMap(FilterMap),
    Source(Source),
    SourceMap(SourceMap),
    IndexWith(IndexWith),
    Differentiate(Differentiate),
    Integrate(Integrate),
    Delta0(Delta0),
    DelayedFeedback(DelayedFeedback),
    Distinct(Distinct),
    JoinCore(JoinCore),
    Subgraph(Subgraph),
    Export(Export),
    ExportedNode(ExportedNode),
    MonotonicJoin(MonotonicJoin),
    Constant(ConstantStream),
    PartitionedRollingFold(PartitionedRollingFold),
    FlatMap(FlatMap),
    // TODO: OrderBy, Windows
}

// TODO: Fully flesh this out, make it useful
#[enum_dispatch]
pub trait DataflowNode {
    fn inputs(&self, inputs: &mut Vec<NodeId>);

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind>;

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout>;

    fn signature(&self, inputs: &[StreamLayout], layout_cache: &RowLayoutCache) -> Signature;

    fn validate(&self, inputs: &[StreamLayout], layout_cache: &RowLayoutCache);

    fn optimize(&mut self, inputs: &[StreamLayout], layout_cache: &RowLayoutCache);

    fn functions<'a>(&'a self, _functions: &mut Vec<&'a Function>) {}

    fn functions_mut<'a>(&'a mut self, _functions: &mut Vec<&'a mut Function>) {}

    fn layouts(&self, layouts: &mut Vec<LayoutId>);

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>);
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    Serialize,
    IsVariant,
    Unwrap,
)]
pub enum StreamLayout {
    Set(LayoutId),
    Map(LayoutId, LayoutId),
}

impl StreamLayout {
    pub const fn key_layout(self) -> LayoutId {
        match self {
            Self::Set(key) | Self::Map(key, _) => key,
        }
    }

    pub const fn value_layout(self) -> Option<LayoutId> {
        match self {
            Self::Set(_) => None,
            Self::Map(_, value) => Some(value),
        }
    }

    pub const fn kind(self) -> StreamKind {
        match self {
            Self::Set(_) => StreamKind::Set,
            Self::Map(_, _) => StreamKind::Map,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, IsVariant,
)]
pub enum StreamKind {
    Set,
    Map,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Distinct {
    input: NodeId,
}

impl Distinct {
    pub const fn new(input: NodeId) -> Self {
        Self { input }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }
}

impl DataflowNode for Distinct {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(inputs[0])
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}

    fn remap_layouts(&mut self, _mappings: &BTreeMap<LayoutId, LayoutId>) {}
}

// FIXME: DelayedFeedback with maps
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DelayedFeedback {
    layout: LayoutId,
}

impl DelayedFeedback {
    pub const fn new(layout: LayoutId) -> Self {
        Self { layout }
    }

    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

impl DataflowNode for DelayedFeedback {
    fn inputs(&self, _inputs: &mut Vec<NodeId>) {}

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(StreamKind::Set)
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(StreamLayout::Set(self.layout))
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.layout);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout = mappings[&self.layout];
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Delta0 {
    input: NodeId,
}

impl Delta0 {
    pub const fn new(input: NodeId) -> Self {
        Self { input }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }
}

impl DataflowNode for Delta0 {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(inputs[0])
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}

    fn remap_layouts(&mut self, _mappings: &BTreeMap<LayoutId, LayoutId>) {}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Neg {
    input: NodeId,
    // FIXME: Neg should be able to operate over maps as well
    output_layout: LayoutId,
}

impl Neg {
    pub fn new(input: NodeId, output_layout: LayoutId) -> Self {
        Self {
            input,
            output_layout,
        }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn output_layout(&self) -> LayoutId {
        self.output_layout
    }
}

impl DataflowNode for Neg {
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

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.output_layout);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.output_layout = mappings[&self.output_layout];
    }
}
