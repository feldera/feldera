//! Transform the circuit in explicit pass order, preserving operator identities.

mod aggregate_groups;
mod aggregate_input;
pub use aggregate_input::{AggregateInput, ProjectedAggregateInput, trim_aggregate_input};
mod aggregate_projection;
pub use aggregate_projection::{project_indexed_value, trim_aggregate_projection, trim_indexed_reader, AggregateProjection, IndexedValueUse, IndexedReaderProjection};
mod aggregate_fold;
pub use aggregate_fold::lower_aggregate_fold;
mod aggregate_zero;
mod antijoin_projection;
pub use antijoin_projection::{antijoin_projection, split_antijoin_projection};
mod append_only;
mod pipeline;
pub use pipeline::{optimizer_order, OptimizerStep, PipelineOptions};
mod repeat;
pub use repeat::{repeat, RepeatResult};
use feldera_sql_expr::lexical_scope;
pub use lexical_scope::{BindingDiagnostic, Resolution as LexicalResolution, inspect_references, lexical_references};
mod map_fusion;
mod projection_key;
mod monotonicity;
pub use monotonicity::aggregate::{aggregate_transfer, chain_minimum_transfer, minimum_transfer, minimum_transfer_for_output, AggregateTransfer};
pub use monotonicity::asof::asof_waterline;
pub use monotonicity::bindings::{waterline_bindings, WaterlineBindings};
pub use monotonicity::chain_minimum::{check_minimum_chain, is_minimum_chain, MinimumChainError};
pub use monotonicity::combine::{
    antijoin_waterline, combine_bounds, join_arguments, BoundCombination, BoundError, BoundSelection, BoundType,
};
pub use monotonicity::flatmap::flatmap_waterline;
pub use monotonicity::lag::lag_transfer;
pub use monotonicity::literal::{literal_waterline, LiteralWaterline};
pub use monotonicity::schedule::{monotonicity_transfers, propagate_waterlines, MonotonicityTransfer, OutputWaterline, TransferOutcome};
pub use monotonicity::casts::{cast_bound, cast_waterline, CastWaterline};
pub use monotonicity::functions::{
    build_function_waterline, clock_waterline, function_rule, function_waterline, Argument as WaterlineArgument, FunctionRule,
};
pub use monotonicity::binary::{
    binary_waterline, BinaryOperation, BinaryWaterline, Domain as BinaryDomain, Operand as WaterlineOperand,
};
pub use monotonicity::identity::identity_bounds;
pub use monotonicity::row_projection::{expression_waterlines, project_closure, project_row, project_rows, tuple_waterline, ClosureAnalysis, ExpressionWaterline, FactType, ProjectedClosure, ProjectedRead, RowRead, WaterlineError, WaterlineInspection};
pub use monotonicity::{filter_lower_bounds, filter_transfer, unary_preserves_order, unary_waterline, FilterBound};
mod static_candidates;
mod static_items;
pub use static_items::{find_static_literal, renumber_static_errors, static_initializer, static_item};
pub use static_candidates::{static_candidates, StaticCandidates};
pub use map_fusion::{
    fuse_expensive_maps, fuse_expensive_maps_with, FusionComparison, has_duplicate_expensive_field, map_fusion_candidate, map_fusion_fragments,
    map_fusion_groups, scan_map_fusion, sibling_map_groups, FusionCandidate, FusionMember, FusionScan,
};
mod star_joins;
pub use star_joins::{expand_star_delta, DeltaStarJoin, expand_star_term, BinaryJoin, StarJoinExpansion, expand_star_join_layer, find_join_trees, names_after_isolated_joins, remove_star_joins, replace_star_joins, StarJoinTree};
mod asof;
pub use asof::{lower_asof, AsofRetention, LoweredAsof};
mod asof_retention;
pub use asof_retention::{asof_retainers, move_asof_retention};
mod array_agg;
mod balanced_joins;
mod chains;
mod collapse_shared_chains;
mod comparators;
mod field_comparator;
pub use field_comparator::{project_topk_comparators, topk_compared_fields};
mod replace_projections;
pub use replace_projections::{projection_edits, replace_common_projections, ProjectionClosures};
mod row_fields;
mod row_projection;
pub use row_projection::project_used_fields;
mod projected_reader;
pub use projected_reader::{FieldRewriteError, TupleUsage, rebind_projected_aggregate, rebind_projected_reader, rebind_reader_with_layout};
mod compressed_fields;
pub use compressed_fields::compressed_field_type;
mod topk_input;
pub use topk_input::{ProjectedTopK, TopK};
mod map_input;
pub use map_input::{MapOutput, append_input_projection, append_split_map, checked_split_map_input, split_map_input};
mod join_input;
pub use join_input::{narrow_join_inputs, project_join_input, JoinInputRewrite};
mod common_projections;
pub use common_projections::{common_projection_payloads, common_projections, shared_projection_closures, CommonProjection, SharedProjectionClosure};
mod conditionals;
mod constant_filters;
mod consumer_graph;
mod cse;
mod operator_inputs;
pub use operator_inputs::OperatorInputs;
mod dead_code;
mod delta_join;
pub use delta_join::{DeltaJoin, JoinForm};
mod delta_aggregate;
pub use delta_aggregate::UnaryDelta;
mod dependency_order;
pub use dependency_order::dependency_order;
mod distinct_outputs;
mod empty_sources;
mod error_consumers;
mod error_wrappers;
mod expand_aggregates;
mod binding_placement;
pub use binding_placement::{BindingScope, PendingBinding, bind_lazy_expression, lazy_statements, read_lazy_value};
mod value_numbering;
pub use value_numbering::{LiveNumbering, NumberingInput, NumberingSpellings, borrowed_variables};
mod expression_uses;
pub use expression_uses::{shared_expression_uses, NumberedExpression};
mod expression_visitor;
pub use expression_visitor::canonicalize_names;
mod fanout;
mod filter_joins;
mod filters;
mod flatmap;
pub use flatmap::{lower_flatmap, trim_flatmap_input};
mod function_calls;
mod gen2;
mod hints;
mod hoist_filters;
mod hop;
mod identity;
mod incrementalize;
mod indexed_inputs;
pub use feldera_sql_expr::live_field_dependencies;
mod input_fields;
pub use input_fields::{
    input_field_uses, input_projections, map_field_dependencies, source_column_decisions,
    source_columns, InputFieldUse, SourceColumns,
};
mod integrate;
mod lineage;
pub use lineage::{AnalysisError as LineageError, LineageDiagnostic, checked_column_copies, checked_fixed_columns, checked_equal_computations, checked_lineage_report, correlated_columns, lineage_report, column_copies, equal_computations, fixed_columns, Column, OutputShape};
mod collection_keys;
pub use collection_keys::{JoinInputs, JoinKeys, KeyColumns, KeyProjection, circuit_keys, filter_keys};
mod join_filter_map;
pub use join_filter_map::lower_join_filter_map;
mod left_joins;
mod lower_circuit;
pub use lower_circuit::{LoweringTypes, lower_circuit};
pub mod limiters;
mod linear_retention;
mod merge_gc;
mod metadata_casts;
mod multiset;
mod nested;
mod operator_hashes;
mod optimize_distinct;
mod output_integrators;
mod project_outputs;
mod projection;
pub use projection::{MapProjection, ProjectionMode, is_map_projection, map_projection};
mod projection_rules;
pub use projection_rules::{ProjectionPushdown, ProjectionRules, project_empty_result};
mod projection_composition;
pub use projection_composition::{compose_indexed_projection, compose_projection};
mod propagate_constants;
pub use project_outputs::{project_outputs, project_unnest, OutputProjection};
mod pull_filters;
mod push_integrals;
mod rebuild;
mod recursive_views;
mod regions;
mod remove_noops;
mod remove_table;
mod index_key_equivalence;
mod index_reader;
mod shared_index_candidates;
mod shared_indexes;
mod wide_index;
mod replace_shared_indexes;
mod state_bounds;
mod statistics;
mod stream_rewrites;
mod sums;
mod temporal_comparisons;
mod temporal_groups;
mod temporal_equality;
mod clock_source;
pub use clock_source::{ClockStreams, builtin_clock, scalar_clock, scalar_clock_operators, table_clock};
mod clock_join;
pub use clock_join::{ClockConsumer, ClockJoinInputs, ClockProjection, ClockRewrite, append_clock_join, apply_joined_clock};
mod temporal_window;
pub use temporal_window::{InitialBounds, TemporalStreams, WindowComparison, WindowFunctions, WindowKey, append_temporal_window, temporal_window};
mod window_bounds;
pub use window_bounds::{temporal_window_bounds, WindowEndpoint};
mod window_projection;
pub use window_projection::{window_projection, TemporalWindowProjection};
mod temporal_filters;
mod temporal_filter_lowering;
pub use temporal_filter_lowering::{ClockFilter, LoweredClockFilter, TemporalFilterLowering};
mod terms;
mod topk_projection;
pub use topk_projection::trim_topk_projection;
mod topology;
mod trace_integrators;
mod trace_retention;
mod defer_carried;
pub use defer_carried::{carried_columns, chain_steps, choose_chain_key, ChainKeyChoice, ChainKeys, defer_carried_columns, ChainLayout};
mod trim_filters;
pub use trim_filters::{trim_filter_closures, trim_filter_readers, trim_filters};
mod trim_windows;
pub use trim_windows::trim_windows;
mod trim_inputs;
pub use trim_inputs::{checked_trim_inputs, trim_inputs, InputLayout};
mod udf;
mod validation;
mod views;
mod waterline;
mod window_groups;

pub use window_groups::{
    compose_timestamp, implement_window_groups, indexed_window_groups, qualifying_window_groups,
    reorder_shared_windows, reorder_temporal_filters, reorder_temporal_predicate,
    scan_temporal_windows, shared_temporal_inputs, shared_window_fields, temporal_filter_sources,
    window_candidates, IndexedWindows, TemporalBound, TemporalCandidate, TemporalFilterSource,
    TemporalOrder, TemporalScan, WindowFields, WindowProjection,
};

pub use aggregate_groups::{aggregate_groups, AggregateKind};
pub use aggregate_zero::expand_aggregate_zero;
pub use append_only::append_only_streams;
pub use array_agg::{fuse_into_array_agg, ArrayFusions};
pub use balanced_joins::{balance_joins, optimize_balanced_joins};
pub use chains::{chain_readers, fuse_chains, implement_chains, optimize_chains};
pub use collapse_shared_chains::{collapse_shared_chains, collapsed_indexes};
pub use comparators::{declare_comparators, ordered_indexes, rewrite_index_functions};
pub use conditionals::{simplify_conditional_root, simplify_conditionals};
pub use constant_filters::remove_constant_filters;
pub use consumer_graph::{consumer_graphs, projected_consumer_graphs, ConsumerBody, Successors};
pub use cse::{common_subexpressions, common_subexpressions_with_names, cse_payloads, projected_cse_payloads, CseState};
pub use dead_code::{eliminate_dead_code, projected_pruning, pruning, remove_dead_operators, DeadCodeResult, Pruning};
pub use distinct_outputs::ensure_distinct_outputs;
pub use empty_sources::{empty_stream_edits, propagate_empty_sources, EmptyStreamEdit};
pub use error_consumers::{error_consumers, ErrorConsumerOrder};
pub use error_wrappers::create_runtime_error_wrappers;
pub use expand_aggregates::{expand_stream_aggregates, AggregateContext, ExpandedAggregates, StreamAggregate};
pub use expression_visitor::{ensure_expression_trees, simplify_expressions};
pub use fanout::{clone_operators_with_fanout, fanout_producers, fanout_readers, optimize_fanout};
pub use feldera_sql_expr::adjust_sql_index;
pub use feldera_sql_expr::CastSafety;
pub use filter_joins::filter_joins;
pub use function_calls::{
    circuit_contains_now, connector_metadata_call, is_clock_call, is_connector_metadata,
    nondeterministic_initializers, temporal_clock_sites, ConnectorMetadataCall,
    NondeterministicInitializer,
};
pub use gen2::{gen2_passes, remove_clones, remove_typed_boxes};
pub use hints::{check_hints, HintDiagnostic};
pub use hoist_filters::{decompose_expensive_filters, filters_need_decomposition};
pub use hop::{expand_hop, hop_waterline, Hop};
pub use identity::{identity_closure, identity_decisions, remove_identity_operators};
pub use incrementalize::{incremental_boundaries, incrementalize};
pub use indexed_inputs::{expand_indexed_inputs, IndexOptions};
pub use integrate::remove_redundant_integrates;
pub use left_joins::{
    empty_right_join_closure, remove_useless_left_joins, substitute_left_joins, unmatched_row_projection,
};
pub use linear_retention::{
    linear_postprocess_retain_keys, linear_retention_order, linear_retention_plan, LinearRetentionPlan,
};
pub use merge_gc::{
    merge_retention_policies, retention_groups, share_trace_inputs, RetentionGroup, SharedTraceInputs,
};
pub use metadata_casts::{expand_circuit_casts, expand_metadata_casts};
pub use multiset::{plan_multiplicities, stream_multiplicities, Multiplicities, MultiplicityPlan};
pub use nested::{build_nested_operators, NestedDiagnostics, NestedResult};
pub use operator_hashes::{
    check_persistent_ids, ComputedHashes, PersistentIdFinding, PrefixInputs,
};
pub use optimize_distinct::optimize_distinct;
pub use output_integrators::share_output_integrators;
pub use propagate_constants::propagate_constants;
pub use pull_filters::{pull_filter_sites, pull_filter_through_aggregate, pull_filter_through_map, pull_filters};
pub use push_integrals::{flat_integral_edits, push_integrals, IntegralEdit};
pub use recursive_views::{resolve_view_references, ViewReferences, ViewResolution};
pub use regions::{conflicting_regions, tag_regions};
pub use remove_noops::{bypassed_noops, optimize_noops, remove_noops};
pub use remove_table::{remove_table, unreferenced_table_sources, TableRemoval};
pub use shared_index_candidates::{find_shared_indexes, shared_index_payloads, IndexDiscoveryError, IndexReader, SharedIndexes};
pub use index_reader::{rewrite_index_reader, IndexParameter};
pub use replace_shared_indexes::{replace_shared_indexes, replace_shared_indexes_with};
pub use wide_index::{project_index as project_shared_index, split_with as split_shared_indexes_with,
    split_shared_indexes, wide_index_fields, CompareColumn, IndexComparison, ProjectedIndex, WideIndexFields, WideIndexLayout};
pub use shared_indexes::{duplicate_shared_indexes, find_map_chains};
pub use state_bounds::{
    detects_streaming, propagate_bounds, pruned_streams, unbounded_operators, UnboundedOperator,
};
pub use statistics::{circuit_statistics, CircuitStatistics};
pub use stream_rewrites::{
    implement_joins, insert_weight_validation, optimize_differentials, push_differentials_up,
};
pub use sums::merge_sums;
pub use index_key_equivalence::{compare_live_operands, ColumnRewriteError, ComparisonDiagnostic, KeyComparisonError};
pub use temporal_groups::{accepts_temporal_bound, check_temporal_bound, check_temporal_groups, group_temporal_filters, TemporalBoundKey, TemporalGroup, WindowTerm};
pub use temporal_comparisons::{TemporalDiagnostic, MissingClockKey, classify_temporal_predicates, conjunct_windows, infer_temporal_predicates, ConjunctWindow, TemporalPredicate};
pub use temporal_equality::desugar_now_equality;
pub use temporal_filters::{
    TemporalFilterPair, TemporalMerge, break_temporal_filters, merge_temporal_filters, merge_temporal_filters_with,
    merge_temporal_predicates,
};
pub use topology::needs_full_snapshot;
pub use trace_integrators::{separate_integrators, separate_trace_inputs, snapshot_aggregate};
pub use trace_retention::{
    check_and_prune_retention, duplicate_retention, retention_violation, DuplicateRetention,
    Retained, RetentionViolation,
};
pub use udf::{inline_udfs, may_inline_udfs};
pub use validation::{first_integral, recursive_violation, RecursiveViolation};
pub use views::{constant_views, remove_views, ViewRemoval};
pub use waterline::{simplify_waterline_root, simplify_waterlines};

use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Error, NodeId, OperatorKind};
use serde_json::json;

/// The live and detached rewrites select the same operators, including those
/// inside recursive circuits. Selection needs no expression payloads.
pub fn deindex_operators(circuit: &Circuit) -> Result<Vec<NodeId>, Error> {
    let mut operators = circuit.operator_order(BodyOrder::Postorder)?;
    operators.retain(|&id| {
        circuit.node(id).expect("ordered operator").operator_kind() == Some(OperatorKind::Deindex)
    });
    Ok(operators)
}

/// A Deindex already carries its projection closure. The physical map keeps
/// its function, declared output type, input port and relational provenance.
pub fn remove_deindex_operators(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let operators = deindex_operators(&circuit)?;
    let mut edit = CircuitEditor::new(circuit);
    for id in operators {
        edit.deindex_as_map(id)?;
    }
    rebuild::rebuild_input_closures(edit.finish_with_rewrites()?, next)
}

/// Append the stream names used by the profiler and generated code. Each pass
/// starts its own counter and visits a nested operator before its body.
pub fn compact_names(mut circuit: Circuit) -> Result<Circuit, Error> {
    for (i, id) in circuit
        .operator_order(BodyOrder::Preorder)?
        .into_iter()
        .enumerate()
    {
        circuit
            .annotations_mut(id)?
            .push(json!({"class":"CompactName","name":format!("s{i}")}));
    }
    Ok(circuit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_stream_names_follow_body_order_and_preserve_annotations() {
        let input = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            {"class":"DBSPNestedOperator","id":7,"inputs":[],"internalOutputs":[],
             "allOperators":[{"class":"DBSPConstantOperator","id":3,"inputs":[],"annotations":[]}],
             "annotations":[{"class":"Recursive"}]},
            {"class":"DBSPConstantOperator","id":2,"inputs":[],"annotations":[]}
        ]});
        let got = compact_names(Circuit::from_json(input).unwrap())
            .unwrap()
            .to_json();
        let ops = &got["allOperators"];
        assert_eq!(
            ops[0]["annotations"],
            json!([{"class":"Recursive"},{"class":"CompactName","name":"s0"}])
        );
        assert_eq!(ops[0]["allOperators"][0]["annotations"][0]["name"], "s1");
        assert_eq!(ops[1]["annotations"][0]["name"], "s2");
    }
}
