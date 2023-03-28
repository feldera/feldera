use crate::ir::{
    nodes::{
        Antijoin, ConstantStream, DelayedFeedback, Delta0, Differentiate, Distinct, Export,
        ExportedNode, Filter, FilterMap, FlatMap, Fold, IndexWith, Integrate, JoinCore, Map, Min,
        Minus, MonotonicJoin, Neg, Node, PartitionedRollingFold, Sink, Source, SourceMap, Subgraph,
        Sum,
    },
    GraphExt,
};

pub trait NodeVisitor {
    fn visit_map(&mut self, _map: &Map) {}
    fn visit_min(&mut self, _min: &Min) {}
    fn visit_neg(&mut self, _neg: &Neg) {}
    fn visit_sum(&mut self, _sum: &Sum) {}
    fn visit_fold(&mut self, _fold: &Fold) {}
    fn visit_sink(&mut self, _sink: &Sink) {}
    fn visit_minus(&mut self, _minus: &Minus) {}
    fn visit_filter(&mut self, _filter: &Filter) {}
    fn visit_filter_map(&mut self, _filter_map: &FilterMap) {}
    fn visit_source(&mut self, _source: &Source) {}
    fn visit_source_map(&mut self, _source_map: &SourceMap) {}
    fn visit_index_with(&mut self, _index_with: &IndexWith) {}
    fn visit_differentiate(&mut self, _differentiate: &Differentiate) {}
    fn visit_integrate(&mut self, _integrate: &Integrate) {}
    fn visit_delta0(&mut self, _delta0: &Delta0) {}
    fn visit_delayed_feedback(&mut self, _delayed_feedback: &DelayedFeedback) {}
    fn visit_distinct(&mut self, _distinct: &Distinct) {}
    fn visit_join_core(&mut self, _join_core: &JoinCore) {}
    fn visit_export(&mut self, _export: &Export) {}
    fn visit_exported_node(&mut self, _exported_node: &ExportedNode) {}
    fn visit_monotonic_join(&mut self, _monotonic_join: &MonotonicJoin) {}
    fn visit_constant(&mut self, _constant: &ConstantStream) {}
    fn visit_partitioned_rolling_fold(
        &mut self,
        _partitioned_rolling_fold: &PartitionedRollingFold,
    ) {
    }
    fn visit_flat_map(&mut self, _flat_map: &FlatMap) {}
    fn visit_antijoin(&mut self, _antijoin: &Antijoin) {}

    fn visit_subgraph(&mut self, subgraph: &Subgraph) {
        self.enter_subgraph(subgraph);

        for node in subgraph.nodes().values() {
            node.accept(self);
        }

        self.leave_subgraph(subgraph);
    }
    fn enter_subgraph(&mut self, _subgraph: &Subgraph) {}
    fn leave_subgraph(&mut self, _subgraph: &Subgraph) {}
}

pub trait MutNodeVisitor {
    fn visit_map(&mut self, _map: &mut Map) {}
    fn visit_min(&mut self, _min: &mut Min) {}
    fn visit_neg(&mut self, _neg: &mut Neg) {}
    fn visit_sum(&mut self, _sum: &mut Sum) {}
    fn visit_fold(&mut self, _fold: &mut Fold) {}
    fn visit_sink(&mut self, _sink: &mut Sink) {}
    fn visit_minus(&mut self, _minus: &mut Minus) {}
    fn visit_filter(&mut self, _filter: &mut Filter) {}
    fn visit_filter_map(&mut self, _filter_map: &mut FilterMap) {}
    fn visit_source(&mut self, _source: &mut Source) {}
    fn visit_source_map(&mut self, _source_map: &mut SourceMap) {}
    fn visit_index_with(&mut self, _index_with: &mut IndexWith) {}
    fn visit_differentiate(&mut self, _differentiate: &mut Differentiate) {}
    fn visit_integrate(&mut self, _integrate: &mut Integrate) {}
    fn visit_delta0(&mut self, _delta0: &mut Delta0) {}
    fn visit_delayed_feedback(&mut self, _delayed_feedback: &mut DelayedFeedback) {}
    fn visit_distinct(&mut self, _distinct: &mut Distinct) {}
    fn visit_join_core(&mut self, _join_core: &mut JoinCore) {}
    fn visit_export(&mut self, _export: &mut Export) {}
    fn visit_exported_node(&mut self, _exported_node: &mut ExportedNode) {}
    fn visit_monotonic_join(&mut self, _monotonic_join: &mut MonotonicJoin) {}
    fn visit_constant(&mut self, _constant: &mut ConstantStream) {}
    fn visit_partitioned_rolling_fold(
        &mut self,
        _partitioned_rolling_fold: &mut PartitionedRollingFold,
    ) {
    }
    fn visit_flat_map(&mut self, _flat_map: &mut FlatMap) {}
    fn visit_antijoin(&mut self, _antijoin: &mut Antijoin) {}

    fn visit_subgraph(&mut self, subgraph: &mut Subgraph) {
        self.enter_subgraph(subgraph);

        for node in subgraph.nodes_mut().values_mut() {
            node.accept_mut(self);
        }

        self.leave_subgraph(subgraph);
    }
    fn enter_subgraph(&mut self, _subgraph: &mut Subgraph) {}
    fn leave_subgraph(&mut self, _subgraph: &mut Subgraph) {}
}

impl Node {
    pub fn accept<V>(&self, visitor: &mut V)
    where
        V: NodeVisitor + ?Sized,
    {
        match self {
            Self::Map(map) => visitor.visit_map(map),
            Self::Min(min) => visitor.visit_min(min),
            Self::Neg(neg) => visitor.visit_neg(neg),
            Self::Sum(sum) => visitor.visit_sum(sum),
            Self::Fold(fold) => visitor.visit_fold(fold),
            Self::Sink(sink) => visitor.visit_sink(sink),
            Self::Minus(minus) => visitor.visit_minus(minus),
            Self::Filter(filter) => visitor.visit_filter(filter),
            Self::FilterMap(filter_map) => visitor.visit_filter_map(filter_map),
            Self::Source(source) => visitor.visit_source(source),
            Self::SourceMap(source_map) => visitor.visit_source_map(source_map),
            Self::IndexWith(index_with) => visitor.visit_index_with(index_with),
            Self::Differentiate(differentiate) => visitor.visit_differentiate(differentiate),
            Self::Integrate(integrate) => visitor.visit_integrate(integrate),
            Self::Delta0(delta0) => visitor.visit_delta0(delta0),
            Self::DelayedFeedback(delayed_feedback) => {
                visitor.visit_delayed_feedback(delayed_feedback);
            }
            Self::Distinct(distinct) => visitor.visit_distinct(distinct),
            Self::JoinCore(join_core) => visitor.visit_join_core(join_core),
            Self::Subgraph(subgraph) => visitor.visit_subgraph(subgraph),
            Self::Export(export) => visitor.visit_export(export),
            Self::ExportedNode(exported_node) => visitor.visit_exported_node(exported_node),
            Self::MonotonicJoin(monotonic_join) => visitor.visit_monotonic_join(monotonic_join),
            Self::Constant(constant) => visitor.visit_constant(constant),
            Self::PartitionedRollingFold(partitioned_rolling_fold) => {
                visitor.visit_partitioned_rolling_fold(partitioned_rolling_fold);
            }
            Self::FlatMap(flat_map) => visitor.visit_flat_map(flat_map),
            Self::Antijoin(antijoin) => visitor.visit_antijoin(antijoin),
        }
    }

    pub fn accept_mut<V>(&mut self, visitor: &mut V)
    where
        V: MutNodeVisitor + ?Sized,
    {
        match self {
            Self::Map(map) => visitor.visit_map(map),
            Self::Min(min) => visitor.visit_min(min),
            Self::Neg(neg) => visitor.visit_neg(neg),
            Self::Sum(sum) => visitor.visit_sum(sum),
            Self::Fold(fold) => visitor.visit_fold(fold),
            Self::Sink(sink) => visitor.visit_sink(sink),
            Self::Minus(minus) => visitor.visit_minus(minus),
            Self::Filter(filter) => visitor.visit_filter(filter),
            Self::FilterMap(filter_map) => visitor.visit_filter_map(filter_map),
            Self::Source(source) => visitor.visit_source(source),
            Self::SourceMap(source_map) => visitor.visit_source_map(source_map),
            Self::IndexWith(index_with) => visitor.visit_index_with(index_with),
            Self::Differentiate(differentiate) => visitor.visit_differentiate(differentiate),
            Self::Integrate(integrate) => visitor.visit_integrate(integrate),
            Self::Delta0(delta0) => visitor.visit_delta0(delta0),
            Self::DelayedFeedback(delayed_feedback) => {
                visitor.visit_delayed_feedback(delayed_feedback);
            }
            Self::Distinct(distinct) => visitor.visit_distinct(distinct),
            Self::JoinCore(join_core) => visitor.visit_join_core(join_core),
            Self::Subgraph(subgraph) => visitor.visit_subgraph(subgraph),
            Self::Export(export) => visitor.visit_export(export),
            Self::ExportedNode(exported_node) => visitor.visit_exported_node(exported_node),
            Self::MonotonicJoin(monotonic_join) => visitor.visit_monotonic_join(monotonic_join),
            Self::Constant(constant) => visitor.visit_constant(constant),
            Self::PartitionedRollingFold(partitioned_rolling_fold) => {
                visitor.visit_partitioned_rolling_fold(partitioned_rolling_fold);
            }
            Self::FlatMap(flat_map) => visitor.visit_flat_map(flat_map),
            Self::Antijoin(antijoin) => visitor.visit_antijoin(antijoin),
        }
    }
}
