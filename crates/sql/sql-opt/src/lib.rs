//! Transform the circuit in explicit pass order, preserving operator identities.

mod aggregate_groups;
mod aggregate_projection;
pub use aggregate_projection::{trim_aggregate_projection, AggregateProjection};
mod aggregate_fold;
pub use aggregate_fold::lower_aggregate_fold;
mod aggregate_zero;
mod antijoin_projection;
pub use antijoin_projection::{antijoin_projection, split_antijoin_projection};
mod append_only;
mod lexical_scope;
mod map_fusion;
mod monotonicity;
pub use monotonicity::aggregate::{aggregate_transfer, chain_minimum_transfer, minimum_transfer, AggregateTransfer};
pub use monotonicity::lag::lag_transfer;
pub use monotonicity::casts::{cast_waterline, CastWaterline};
pub use monotonicity::functions::{function_waterline, Argument as WaterlineArgument};
pub use monotonicity::binary::{
    binary_waterline, BinaryWaterline, Domain as BinaryDomain, Operand as WaterlineOperand,
};
pub use monotonicity::{filter_lower_bounds, filter_transfer, monotone_identity, unary_waterline, FilterBound};
mod static_candidates;
mod static_items;
pub use static_items::{renumber_static_errors, static_initializer, static_item};
pub use static_candidates::{static_candidates, StaticCandidates};
pub use map_fusion::{fuse_expensive_maps, map_fusion_fragments, map_fusion_groups, FusionMember};
mod star_joins;
pub use star_joins::{expand_star_term, BinaryJoin, StarJoinExpansion, expand_star_join_layer, find_join_trees, names_after_isolated_joins, remove_star_joins, replace_star_joins, StarJoinTree};
mod asof;
pub use asof::{lower_asof, AsofRetention, LoweredAsof};
mod asof_retention;
pub use asof_retention::{asof_retainers, move_asof_retention};
mod array_agg;
mod balanced_joins;
mod chains;
mod collapse_shared_chains;
mod comparators;
mod replace_projections;
pub use replace_projections::{replace_common_projections, ProjectionClosures};
mod row_fields;
mod row_projection;
mod map_input;
pub use map_input::{split_map_input, MapOutput};
mod common_projections;
pub use common_projections::{common_projection_payloads, common_projections, shared_projection_closures, CommonProjection, SharedProjectionClosure};
mod conditionals;
mod constant_filters;
mod consumer_graph;
mod cse;
mod dead_code;
mod dependency_order;
pub use dependency_order::dependency_order;
mod distinct_outputs;
mod empty_sources;
mod error_consumers;
mod error_wrappers;
mod expand_aggregates;
mod binding_placement;
pub use binding_placement::{BindingScope, PendingBinding, bind_lazy_expression, lazy_statements};
mod expression_uses;
pub use expression_uses::{shared_expression_uses, NumberedExpression};
mod expression_visitor;
pub use expression_visitor::canonicalize_names;
mod fanout;
mod filter_joins;
mod filters;
mod flatmap;
pub use flatmap::lower_flatmap;
mod function_calls;
mod gen2;
mod hints;
mod hoist_filters;
mod hop;
mod identity;
mod incrementalize;
mod indexed_inputs;
mod input_fields;
pub use input_fields::{input_field_uses, input_projections, map_field_dependencies, InputFieldUse};
mod integrate;
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
pub use projection::is_map_projection;
mod projection_composition;
pub use projection_composition::{compose_indexed_projection, compose_projection};
mod propagate_constants;
pub use project_outputs::{project_outputs, project_unnest};
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
pub use clock_source::{clock_aggregate, clock_index, scalar_clock};
mod clock_join;
pub use clock_join::{ClockClosure, clock_join_functions, drop_clock, rewrite_clock_closure};
mod temporal_window;
pub use temporal_window::{InitialBounds, WindowComparison, WindowFunctions, WindowKey, temporal_window};
mod window_bounds;
pub use window_bounds::{temporal_window_bounds, WindowEndpoint};
mod window_projection;
pub use window_projection::{window_projection, TemporalWindowProjection};
mod temporal_filters;
mod terms;
mod topk_projection;
pub use topk_projection::trim_topk_projection;
mod topology;
mod trace_integrators;
mod trace_retention;
mod defer_carried;
pub use defer_carried::{choose_chain_key, ChainKeyChoice, ChainKeys, defer_carried_columns, ChainLayout};
mod trim_filters;
pub use trim_filters::trim_filters;
mod trim_windows;
pub use trim_windows::trim_windows;
mod trim_inputs;
pub use trim_inputs::{trim_inputs, InputLayout};
mod udf;
mod validation;
mod views;
mod waterline;
mod window_groups;

pub use window_groups::{
    implement_window_groups, qualifying_window_groups, shared_temporal_inputs, shared_window_fields,
    reorder_temporal_filters, reorder_temporal_predicate, TemporalOrder, compose_timestamp, temporal_filter_sources, TemporalFilterSource, window_candidates, TemporalBound, TemporalCandidate,
    WindowFields, WindowProjection,
};

pub use aggregate_groups::{aggregate_groups, AggregateKind};
pub use aggregate_zero::expand_aggregate_zero;
pub use append_only::append_only_streams;
pub use array_agg::fuse_into_array_agg;
pub use balanced_joins::{balance_joins, optimize_balanced_joins};
pub use chains::{fuse_chains, implement_chains, optimize_chains};
pub use collapse_shared_chains::collapse_shared_chains;
pub use comparators::declare_comparators;
pub use conditionals::simplify_conditionals;
pub use constant_filters::remove_constant_filters;
pub use consumer_graph::{consumer_graphs, projected_consumer_graphs, ConsumerBody, Successors};
pub use cse::{common_subexpressions, common_subexpressions_with_names, cse_payloads, CseState};
pub use dead_code::{eliminate_dead_code, pruning, remove_dead_operators, DeadCodeResult, Pruning};
pub use distinct_outputs::ensure_distinct_outputs;
pub use empty_sources::{empty_stream_edits, propagate_empty_sources, EmptyStreamEdit};
pub use error_consumers::{error_consumers, ErrorConsumerOrder};
pub use error_wrappers::create_runtime_error_wrappers;
pub use expand_aggregates::{expand_stream_aggregates, AggregateContext, ExpandedAggregates};
pub use expression_visitor::{ensure_expression_trees, simplify_expressions};
pub use fanout::{clone_operators_with_fanout, fanout_producers, fanout_readers, optimize_fanout};
pub use feldera_sql_expr::adjust_sql_index;
pub use feldera_sql_expr::CastSafety;
pub use filter_joins::filter_joins;
pub use function_calls::{
    circuit_contains_now, connector_metadata_call, is_clock_call, is_connector_metadata,
    nondeterministic_initializers, ConnectorMetadataCall, NondeterministicInitializer,
};
pub use gen2::gen2_passes;
pub use hints::{check_hints, HintDiagnostic};
pub use hoist_filters::{decompose_expensive_filters, filters_need_decomposition};
pub use hop::expand_hop;
pub use identity::{identity_closure, remove_identity_operators};
pub use incrementalize::incrementalize;
pub use indexed_inputs::{expand_indexed_inputs, IndexOptions};
pub use integrate::remove_redundant_integrates;
pub use left_joins::{
    empty_right_join_closure, remove_useless_left_joins, substitute_left_joins, unmatched_row_projection,
};
pub use linear_retention::{linear_postprocess_retain_keys, linear_retention_order};
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
pub use pull_filters::pull_filters;
pub use push_integrals::push_integrals;
pub use recursive_views::{resolve_view_references, ViewReferences, ViewResolution};
pub use regions::tag_regions;
pub use remove_noops::{optimize_noops, remove_noops};
pub use remove_table::{remove_table, unreferenced_table_sources, TableRemoval};
pub use shared_index_candidates::{find_shared_indexes, shared_index_payloads, IndexReader, SharedIndexes};
pub use index_reader::{rewrite_index_reader, IndexParameter};
pub use replace_shared_indexes::replace_shared_indexes;
pub use wide_index::{split_shared_indexes, wide_index_fields, WideIndexFields, WideIndexLayout};
pub use shared_indexes::{duplicate_shared_indexes, find_map_chains};
pub use state_bounds::{
    detects_streaming, propagate_bounds, pruned_streams, unbounded_operators, UnboundedOperator,
};
pub use statistics::{circuit_statistics, CircuitStatistics};
pub use stream_rewrites::{
    implement_joins, insert_weight_validation, optimize_differentials, push_differentials_up,
};
pub use sums::merge_sums;
pub use temporal_groups::{accepts_temporal_bound, group_temporal_filters, TemporalBoundKey, TemporalGroup, WindowTerm};
pub use temporal_comparisons::{classify_temporal_predicates, TemporalPredicate};
pub use temporal_equality::desugar_now_equality;
pub use temporal_filters::{break_temporal_filters, merge_temporal_predicates};
pub use topology::needs_full_snapshot;
pub use trace_integrators::{separate_integrators, snapshot_aggregate};
pub use trace_retention::{
    check_and_prune_retention, duplicate_retention, retention_violation, DuplicateRetention,
    Retained, RetentionViolation,
};
pub use udf::{inline_udfs, may_inline_udfs};
pub use validation::{first_integral, recursive_violation, RecursiveViolation};
pub use views::{constant_views, remove_views, ViewRemoval};
pub use waterline::simplify_waterlines;

use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Error, OperatorKind};
use serde_json::json;

/// A Deindex already carries its projection closure. The physical map keeps
/// its function, declared output type, input port and relational provenance.
pub fn remove_deindex_operators(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    for id in order {
        if edit.node(id)?.operator_kind() == Some(OperatorKind::Deindex) {
            edit.deindex_as_map(id)?;
        }
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
