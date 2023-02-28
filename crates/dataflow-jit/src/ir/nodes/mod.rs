mod aggregate;
mod differentiate;
mod filter_map;
mod io;
mod join;
mod subgraph;
mod sum;

pub use aggregate::{Fold, Min};
pub use differentiate::{Differentiate, Integrate};
pub use filter_map::{Filter, FilterMap, Map};
pub use io::{Export, ExportedNode, Sink, Source, SourceMap};
pub use join::{JoinCore, MonotonicJoin};
pub use subgraph::Subgraph;
pub use sum::{Minus, Sum};

use crate::ir::{
    function::{Function, InputFlags},
    layout_cache::RowLayoutCache,
    types::Signature,
    ColumnType, LayoutId, NodeId,
};
use derive_more::{IsVariant, Unwrap};
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndexWith {
    input: NodeId,
    /// Expects a function with a signature of `fn(input_layout, mut key_layout,
    /// mut value_layout)`
    index_fn: Function,
    key_layout: LayoutId,
    value_layout: LayoutId,
}

impl IndexWith {
    pub fn new(
        input: NodeId,
        index_fn: Function,
        key_layout: LayoutId,
        value_layout: LayoutId,
    ) -> Self {
        Self {
            input,
            index_fn,
            key_layout,
            value_layout,
        }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn index_fn(&self) -> &Function {
        &self.index_fn
    }

    pub const fn key_layout(&self) -> LayoutId {
        self.key_layout
    }

    pub const fn value_layout(&self) -> LayoutId {
        self.value_layout
    }
}

impl DataflowNode for IndexWith {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(StreamKind::Map)
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(StreamLayout::Map(self.key_layout, self.value_layout))
    }

    fn signature(&self, inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        debug_assert_eq!(inputs.len(), 1);

        let (args, flags) = match inputs[0] {
            StreamLayout::Set(value) => (
                vec![value, self.key_layout, self.value_layout],
                vec![InputFlags::INPUT, InputFlags::OUTPUT, InputFlags::OUTPUT],
            ),
            StreamLayout::Map(key, value) => (
                vec![key, value, self.key_layout, self.value_layout],
                vec![
                    InputFlags::INPUT,
                    InputFlags::INPUT,
                    InputFlags::OUTPUT,
                    InputFlags::OUTPUT,
                ],
            ),
        };

        Signature::new(args, flags, ColumnType::Unit)
    }

    fn validate(&self, inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        assert_eq!(
            self.signature(inputs, layout_cache),
            self.index_fn.signature()
        );
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        self.index_fn.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(&self.index_fn);
    }

    fn functions_mut<'a>(&'a mut self, functions: &mut Vec<&'a mut Function>) {
        functions.push(&mut self.index_fn);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.extend([self.key_layout, self.value_layout]);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Neg {
    input: NodeId,
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
}
