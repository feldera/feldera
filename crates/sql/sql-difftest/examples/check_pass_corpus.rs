//! Check each Rust pass invocation against its place in the uninterrupted Java run.
#[path = "support/input_metadata.rs"]
mod input_metadata;
mod support;
use feldera_sql_difftest::compare_circuits;
use feldera_sql_ir::{Circuit, NodeId};
use serde_json::{json, Value};
use std::{collections::BTreeMap, error::Error};

type Instances = BTreeMap<u64, feldera_sql_opt::CseState>;

fn check_window_groups(circuit: &Circuit, invocation: &Value) -> Result<(), Box<dyn Error>> {
    #[derive(serde::Deserialize)]
    struct Candidate {
        window: u64,
        key: String,
        fields: Vec<String>,
        expressions: Vec<[u64; 2]>,
    }
    let candidates: Vec<Candidate> =
        serde_json::from_value(invocation["window_candidates"].clone())?;
    if !feldera_sql_opt::window_candidates(circuit)?
        .into_iter()
        .eq(candidates.iter().map(|c| NodeId::Outer(c.window)))
    {
        return Err("window candidates differ from Java".into());
    }
    let threshold = serde_json::from_value(invocation["threshold"].clone())?;
    let keys = candidates.iter().map(|c| c.key.clone()).collect::<Vec<_>>();
    let mut groups = Vec::new();
    for group in feldera_sql_opt::qualifying_window_groups(&keys, threshold) {
        let fields = group
            .iter()
            .map(|&i| candidates[i].fields.clone())
            .collect::<Vec<_>>();
        let plan = feldera_sql_opt::shared_window_fields(&fields);
        let expressions = plan
            .sources
            .iter()
            .map(|&(m, f)| candidates[group[m]].expressions[f])
            .collect::<Vec<_>>();
        let members = group
            .iter()
            .zip(plan.projections)
            .map(|(&i, fields)| json!({"window":candidates[i].window,"fields":fields}))
            .collect::<Vec<_>>();
        groups.push(json!({"expressions":expressions,"members":members}));
    }
    if json!(groups) != invocation["window_groups"] {
        return Err("shared window fields or member order differ from Java".into());
    }
    Ok(())
}

fn ports(
    value: &Value,
) -> Result<std::collections::BTreeSet<feldera_sql_ir::OutputPort>, serde_json::Error> {
    let pairs: Vec<(u64, usize)> = serde_json::from_value(value.clone())?;
    Ok(pairs
        .into_iter()
        .map(|(id, number)| feldera_sql_ir::OutputPort {
            operator: NodeId::Outer(id),
            number,
        })
        .collect())
}

fn compare_invocation(
    pass: &str,
    index: usize,
    invocation: &Value,
    cse_instances: &mut Instances,
) -> Result<bool, Box<dyn Error>> {
    let input = Circuit::from_json(invocation["before"].clone())?;
    let mut next = invocation["names_before"]
        .as_u64()
        .ok_or("missing input name counter")?;
    let actual = match pass {
        "EnsureDistinctOutputs" => {
            let sinks = serde_json::from_value::<Vec<_>>(invocation["multiset_sinks"].clone())?;
            feldera_sql_opt::ensure_distinct_outputs(input, &sinks, &mut next)?
        }
        "CircuitContainsNow" => {
            if invocation["uses_clock"].as_bool()
                != Some(feldera_sql_opt::circuit_contains_now(&input)?)
            {
                return Err("clock-use analysis differs from Java".into());
            }
            input
        }
        "DetectStreamingOperations" => {
            if invocation["streaming"].as_bool()
                != Some(feldera_sql_opt::detects_streaming(&input)?)
            {
                return Err("streaming classification differs from Java".into());
            }
            input
        }
        "FindEquivalentNoops" => {
            let expected = serde_json::from_value(invocation["shared_traces"].clone())?;
            let actual = feldera_sql_opt::share_trace_inputs(&input)?;
            if actual != expected {
                return Err(format!(
                    "shared trace inputs differ: Rust {actual:?}, Java {expected:?}"
                )
                .into());
            }
            input
        }
        "FindMultipleRetains" => {
            let previous = serde_json::from_value(invocation["previously_grouped"].clone())?;
            let groups = feldera_sql_opt::retention_groups(&input, &previous)?;
            let mut grouped = previous;
            grouped.extend(groups.iter().flatten().copied());
            let merged: Vec<_> = groups.into_iter().filter(|g| g.len() > 1).collect();
            if json!(grouped) != invocation["grouped"]
                || json!(merged) != invocation["retention_groups"]
            {
                return Err("retention groups differ from Java".into());
            }
            input
        }
        "MoveGC" => feldera_sql_opt::move_asof_retention(
            input,
            serde_json::from_value(invocation["asof_joins"].clone())?,
            &mut next,
        )?,
        "RemoveStarJoinsPass" => feldera_sql_opt::expand_star_join_layer(input, &mut next, &mut Vec::new())?,
        "CanonicalForm" => feldera_sql_opt::canonicalize_names(input, &mut next)?,
        "FindJoinTrees" => {
            let isolated = feldera_sql_opt::names_after_isolated_joins(&input, next)?;
            let trees = feldera_sql_opt::find_join_trees(&input, &mut next)?;
            if isolated.is_some_and(|after| after != next || !trees.is_empty()) {
                return Err("isolated join analysis disagrees with the full join tree analysis".into());
            }
            if !trees.is_empty() || invocation["join_trees"] != json!([]) {
                // The closures share definitions with their circuit and with one another.
                let mut expected = invocation["before"].clone();
                let mut actual = expected.clone();
                expected["join_trees"] = invocation["join_trees"].clone();
                actual["join_trees"] = serde_json::to_value(trees)?;
                compare_circuits(&expected, &actual)?;
            }
            input
        }
        "ReplaceStarJoins" => feldera_sql_opt::replace_star_joins(
            input, serde_json::from_value(invocation["join_trees"].clone())?, &mut next,
        )?,
        "ReplaceCommonProjections" => feldera_sql_opt::replace_common_projections(
            input, serde_json::from_value(invocation["projection_closures"].clone())?, &mut next)?,
        "DeferCarriedColumns" => feldera_sql_opt::defer_carried_columns(input, serde_json::from_value(invocation["carried_columns"].clone())?, &mut next)?,
        "TrimFilters" => feldera_sql_opt::trim_filters(input, serde_json::from_value(invocation["filter_readers"].clone())?, &mut next)?,
        "TrimWindows" => feldera_sql_opt::trim_windows(input, &mut next)?,
        "TrimInputs" => feldera_sql_opt::trim_inputs(
            input,
            &serde_json::from_value(invocation["input_layouts"].clone())?,
            &mut next,
        )?,
        "FindCommonProjections" => {
            let groups = feldera_sql_opt::common_projections(&input)?;
            if json!(groups) != invocation["shared_projections"] {
                return Err(format!(
                    "shared projections differ: Rust {}, Java {}",
                    json!(groups),
                    invocation["shared_projections"]
                )
                .into());
            }
            next += u64::try_from(groups.len())?;
            input
        }
        "FindUnusedInputFields" => {
            let uses = feldera_sql_opt::input_field_uses(&input)?;
            if json!(uses) != invocation["input_fields"] {
                return Err(format!(
                    "input projections or field paths differ: Rust {}, Java {}",
                    json!(uses),
                    invocation["input_fields"]
                )
                .into());
            }
            input_metadata::mark_unused(&input, &uses)?
        }
        "IndexedInputs" => feldera_sql_opt::expand_indexed_inputs(
            input,
            feldera_sql_opt::IndexOptions {
                dedup_keys: invocation["dedup_keys"]
                    .as_bool()
                    .ok_or("missing index layout")?,
                lazy_upsert: invocation["lazy_upsert"]
                    .as_bool()
                    .ok_or("missing source mode")?,
            },
            &mut next,
        )?,
        "SimplifyConditionals" => feldera_sql_opt::simplify_conditionals(input, &mut next)?,
        "ImplementChains" => feldera_sql_opt::implement_chains(input, &mut next)?,
        "LowerCircuitVisitor"
        | "LowerAsofInner"
        | "ExpandStreamAggregates"
        | "Fuse"
        | "Simplify"
        | "PropagateConstants"
        | "PullFilterVisitor"
        | "OptimizeProjectionVisitor" => {
            let (result, warnings) = match pass {
                "LowerAsofInner" => {
                    let rows = serde_json::from_value(invocation["asof_row_types"].clone())?;
                    let lowered = feldera_sql_opt::lower_asof(input, rows, &mut next)?;
                    (lowered.circuit, lowered.warnings)
                }
                "LowerCircuitVisitor" => {
                    let types: feldera_sql_opt::LoweringTypes =
                        serde_json::from_value(invocation["lowering_types"].clone())?;
                    let (input, context) = types.bind(input)?;
                    feldera_sql_opt::lower_circuit(input, &context, &mut next)?
                }
                "ExpandStreamAggregates" => {
                    let context = serde::Deserialize::deserialize(invocation)?;
                    let expanded =
                        feldera_sql_opt::expand_stream_aggregates(input, &context, &mut next)?;
                    (expanded.circuit, expanded.warnings)
                }
                "Fuse" => feldera_sql_opt::fuse_expensive_maps(
                    input, serde_json::from_value(invocation["map_clusters"].clone())?, &mut next,
                )?,
                "Simplify" => feldera_sql_opt::simplify_expressions(input, &mut next)?,
                "OptimizeProjectionVisitor" => feldera_sql_opt::project_outputs(input, &mut next)?,
                "PullFilterVisitor" => feldera_sql_opt::pull_filters(input, &mut next)?,
                _ => feldera_sql_opt::propagate_constants(input, &mut next)?,
            };
            let warnings: Vec<_> = warnings.into_iter().map(|warning| json!({
                "range":warning.position, "error_type":warning.kind, "message":warning.message,
                "warning":true, "continuation":false
            })).collect();
            if json!(warnings) != invocation["diagnostics"] {
                return Err(format!(
                    "{pass} diagnostics differ: Rust {warnings:?}, Java {}",
                    invocation["diagnostics"]
                )
                .into());
            }
            result
        }
        "CheckPersistentIds" => {
            let mut warnings = Vec::new();
            for finding in feldera_sql_opt::check_persistent_ids(&input)? {
                let feldera_sql_opt::PersistentIdFinding::IdenticalViews { operator, previous } =
                    finding
                else {
                    return Err(format!("unexpected invalid persistent ID: {finding:?}").into());
                };
                let name = |id| {
                    input
                        .node(NodeId::Outer(id))
                        .ok_or("missing view")?
                        .relation_name("viewName")
                        .map_err(|_| "missing view name")
                };
                warnings.push(format!(
                    "View '{}' always has the same contents as view '{}'",
                    name(operator)?,
                    name(previous)?
                ));
            }
            if json!(warnings) != invocation["identical_view_warnings"] {
                return Err("identical-view warnings differ from Java".into());
            }
            input
        }
        "SetOperatorHashes" => {
            let hashes: feldera_sql_opt::ComputedHashes =
                serde_json::from_value(invocation["computed_hashes"].clone())?;
            hashes.annotate(input)?
        }
        "DeltaHashPrefixes" | "GcHashPrefixes" => {
            let hashes: feldera_sql_opt::PrefixInputs =
                serde_json::from_value(invocation["prefix_inputs"].clone())?;
            let prefixes = if pass == "DeltaHashPrefixes" {
                hashes.delta_prefixes(&input)?
            } else {
                hashes.gc_prefixes(&input)?
            };
            if json!(prefixes) != invocation["prefixes"] {
                return Err("Merkle hash prefixes differ from Java".into());
            }
            input
        }
        "MergeRetain" => {
            let groups: Vec<Vec<u64>> =
                serde_json::from_value(invocation["retention_groups"].clone())?;
            feldera_sql_opt::merge_retention_policies(input, &groups, &mut next)?
        }
        "MergeNoops" => {
            let inputs: feldera_sql_opt::SharedTraceInputs =
                serde_json::from_value(invocation["shared_traces"].clone())?;
            inputs.apply(input, &mut next)?
        }
        "FindBounded" => {
            let context = &invocation["bound_inputs"];
            let actual = feldera_sql_opt::propagate_bounds(
                &input,
                ports(&context["known"])?,
                &ports(&context["keys"])?,
                context["now_table"]
                    .as_str()
                    .ok_or("missing NOW table name")?,
            )?;
            let expected = ports(&invocation["bounded"])?;
            if actual != expected {
                return Err(format!(
                    "bounded streams differ: extra {:?}, missing {:?}",
                    actual.difference(&expected),
                    expected.difference(&actual)
                )
                .into());
            }
            input
        }
        "CollectUnbounded" => {
            let bounded = ports(&invocation["bounded"])?;
            let actual = json!(feldera_sql_opt::unbounded_operators(&input, &bounded)?);
            if invocation["unbounded"] != actual {
                return Err(format!(
                    "unbounded state differs: Rust {actual}, Java {}",
                    invocation["unbounded"]
                )
                .into());
            }
            input
        }
        "FindGCedStreams" => {
            if feldera_sql_opt::pruned_streams(&input)? != ports(&invocation["pruned"])? {
                return Err("garbage-collected streams differ from Java".into());
            }
            input
        }
        "AppendOnly" => {
            let expected: Vec<u64> = serde_json::from_value(invocation["append_only"].clone())?;
            if !feldera_sql_opt::append_only_streams(&input)?
                .into_iter()
                .eq(expected.into_iter().map(NodeId::Outer))
            {
                return Err("append-only streams differ from Java".into());
            }
            input
        }
        "ValidateRecursiveOperators" => {
            if let Some(error) = feldera_sql_opt::recursive_violation(&input)? {
                return Err(format!("unexpected recursive violation {error:?}").into());
            }
            input
        }
        "CircuitStatistics" => {
            let actual = serde_json::to_value(feldera_sql_opt::circuit_statistics(&input)?)?;
            if actual != invocation["statistics"] {
                return Err(format!(
                    "circuit statistics differ: Rust {actual}, Java {}",
                    invocation["statistics"]
                )
                .into());
            }
            input
        }
        "ConstantViews" => {
            let reserved = invocation["error_view"]
                .as_str()
                .ok_or("missing error view")?;
            let sinks = feldera_sql_opt::constant_views(&input, reserved)?;
            let expected: Vec<u64> = serde_json::from_value(invocation["constant_views"].clone())?;
            if !sinks
                .into_iter()
                .eq(expected.into_iter().map(NodeId::Outer))
            {
                return Err("constant-view warning sinks differ".into());
            }
            input
        }
        "DeterministicFunctions" => {
            let findings = feldera_sql_opt::nondeterministic_initializers(&input)?;
            if !findings.is_empty() {
                return Err(
                    format!("unexpected nondeterministic initializers {findings:?}").into(),
                );
            }
            input
        }
        "NoConnectorMetadata" => {
            if let Some(call) = feldera_sql_opt::connector_metadata_call(&input)? {
                return Err(
                    format!("unexpected call to {} at {:?}", call.name, call.position).into(),
                );
            }
            input
        }
        "NoIntegralVisitor" => {
            if let Some(operator) = feldera_sql_opt::first_integral(&input)? {
                return Err(format!("unexpected integral {operator:?}").into());
            }
            input
        }
        "DeadCode" => {
            let keep = invocation["keep_sources"]
                .as_bool()
                .ok_or("missing dead-code retention mode")?;
            feldera_sql_opt::eliminate_dead_code(input, &mut next, keep)?.circuit
        }
        "EnsureExpressionTrees" => feldera_sql_opt::ensure_expression_trees(input, &mut next)?,
        "OptimizeDistinctVisitor" => feldera_sql_opt::optimize_distinct(input, &mut next)?,
        "ComparatorDeclarations" => feldera_sql_opt::declare_comparators(input)?,
        "SimplifyWaterline" => feldera_sql_opt::simplify_waterlines(input, &mut next)?,
        "DesugarNowEquality" => feldera_sql_opt::desugar_now_equality(input, &mut next)?,
        "BreakFilters" => feldera_sql_opt::break_temporal_filters(input, &mut next)?.0,
        "ShareOutputIntegrators" => feldera_sql_opt::share_output_integrators(input, &mut next)?,
        "FilterJoinVisitor" => feldera_sql_opt::filter_joins(input, &mut next)?,
        "DecomposeExpensiveFilters" => {
            feldera_sql_opt::decompose_expensive_filters(input, &mut next)?.0
        }
        "FindWindowGroups" => {
            check_window_groups(&input, invocation)?;
            input
        }
        "ImplementWindowGroups" => {
            let groups = serde_json::from_value::<Vec<_>>(invocation["groups"].clone())?;
            feldera_sql_opt::implement_window_groups(input, &groups, &mut next)?
        }
        "FindMapChains" => {
            let expected: Vec<Vec<u64>> = serde_json::from_value(invocation["map_chains"].clone())?;
            let expected: Vec<Vec<_>> = expected
                .into_iter()
                .map(|chain| chain.into_iter().map(NodeId::Outer).collect())
                .collect();
            if feldera_sql_opt::find_map_chains(&input)? != expected {
                return Err("map chains differ from Java".into());
            }
            input
        }
        "OrderGcDependencies" => {
            let edges: Vec<(u64, u64)> = serde_json::from_value(invocation["extra_edges"].clone())?;
            let edges = edges.into_iter().map(|(a, b)| (NodeId::Outer(a), NodeId::Outer(b)));
            let order = feldera_sql_opt::dependency_order(&input, edges)?;
            let expected = Circuit::from_json(invocation["after"].clone())?;
            if !expected.operators().map(|(id, _)| id).eq(order.iter().copied()) {
                return Err("ASOF dependency order differs from Java".into());
            }
            let mut edit = feldera_sql_ir::CircuitEditor::new(input);
            edit.order_root_operators(&order)?;
            edit.finish()?
        }
        "OrderErrorConsumers" => {
            let table = invocation["error_table"]
                .as_str()
                .ok_or("missing system error name")?;
            let expected: Vec<u64> = serde_json::from_value(invocation["error_consumers"].clone())?;
            let order = feldera_sql_opt::error_consumers(&input, table)?;
            if !order.operators[order.first_error..]
                .iter()
                .copied()
                .eq(expected.into_iter().map(NodeId::Outer))
            {
                return Err("error-reachable operators differ".into());
            }
            order.apply(input)?
        }
        "CheckRetain" => {
            if let Some(error) = feldera_sql_opt::duplicate_retention(&input)? {
                return Err(format!("unexpected duplicate retention {error:?}").into());
            }
            input
        }
        "SeparateIntegrators" => feldera_sql_opt::separate_integrators(input, &mut next)?,
        "CollapseSharedChains" => {
            let chains: Vec<Vec<u64>> = serde_json::from_value(invocation["map_chains"].clone())?;
            let chains: Vec<_> = chains
                .into_iter()
                .map(|c| c.into_iter().map(NodeId::Outer).collect())
                .collect();
            feldera_sql_opt::collapse_shared_chains(input, &chains, &mut next)?
        }
        "DuplicateSharedIndexes" => feldera_sql_opt::duplicate_shared_indexes(input, &mut next)?,
        "UDFInliner" => feldera_sql_opt::inline_udfs(input, &mut next)?,
        "RemoveUselessLeftJoinsVisitor" => {
            let keyed: Vec<u64> = serde_json::from_value(invocation["keyed_left_joins"].clone())?;
            let keyed = keyed.into_iter().map(NodeId::Outer).collect();
            feldera_sql_opt::remove_useless_left_joins(input, &keyed, &mut next)?.0
        }
        "SubstituteLeftJoins" => feldera_sql_opt::substitute_left_joins(input, &mut next)?,
        "StrayGC" => {
            if let Some(error) = feldera_sql_opt::retention_violation(&input)? {
                return Err(format!("unexpected trace-retention violation {error:?}").into());
            }
            input
        }
        "OptimizeWithGraph(StrayGC)" => {
            let (result, violation) = feldera_sql_opt::check_and_prune_retention(input, &mut next)?;
            if let Some(error) = violation {
                return Err(format!("unexpected trace-retention violation {error:?}").into());
            }
            result
        }
        "ChainVisitor" => feldera_sql_opt::fuse_chains(input, &mut next)?,
        "OptimizeWithGraph(ChainVisitor)" => feldera_sql_opt::optimize_chains(input, &mut next)?,
        "BuildNestedOperators" => {
            let result = feldera_sql_opt::build_nested_operators(input, &mut next)?;
            if result.diagnostics.late_column.is_some() {
                return Err("unexpected LATENESS in a recursive component".into());
            }
            result.circuit
        }
        "FixupViewReferences" => match feldera_sql_opt::resolve_view_references(&input)? {
            feldera_sql_opt::ViewResolution::Valid(edits) => edits.apply(input, &mut next)?,
            feldera_sql_opt::ViewResolution::UndeclaredRecursion { message, .. } => {
                return Err(message.into())
            }
        },
        "RemoveIdentityOperators" => feldera_sql_opt::remove_identity_operators(input, &mut next)?,
        "ExpandHop" => feldera_sql_opt::expand_hop(input, &mut next)?,
        "LinearPostprocessRetainKeys" => {
            let expected: Option<Vec<u64>> =
                serde_json::from_value(invocation["waterline_order"].clone())?;
            let expected = expected.map(|ids| ids.into_iter().map(NodeId::Outer).collect());
            if feldera_sql_opt::linear_retention_order(&input)? != expected {
                return Err("waterline dependency order differs from Java".into());
            }
            feldera_sql_opt::linear_postprocess_retain_keys(input, &mut next)?
        }
        "CreateRuntimeErrorWrappers" => {
            feldera_sql_opt::create_runtime_error_wrappers(input, &mut next)?
        }
        "ExpandSafeCasts" | "ExpandUnsafeCasts" => {
            let safety = if pass == "ExpandSafeCasts" {
                feldera_sql_opt::CastSafety::Safe
            } else {
                feldera_sql_opt::CastSafety::Unsafe
            };
            feldera_sql_opt::expand_circuit_casts(input, &mut next, safety)?
        }
        "ExpandMetadataCasts" => feldera_sql_opt::expand_metadata_casts(input, &mut next)?,
        "AdjustSqlIndex" => feldera_sql_opt::adjust_sql_index(input)?,
        "Gen2Passes" => feldera_sql_opt::gen2_passes(input)?,
        "CompactNames" => feldera_sql_opt::compact_names(input)?,
        "TagRegions" => feldera_sql_opt::tag_regions(input)?,
        "RemoveViewOperators" => {
            let mode = if invocation["remove_all"]
                .as_bool()
                .ok_or("missing view removal mode")?
            {
                feldera_sql_opt::ViewRemoval::All
            } else {
                feldera_sql_opt::ViewRemoval::Ordinary {
                    error_view: invocation["error_view"]
                        .as_str()
                        .ok_or("missing canonical error view name")?,
                }
            };
            feldera_sql_opt::remove_views(input, mode, &mut next)?
        }
        "IncrementalizeVisitor" => feldera_sql_opt::incrementalize(input, &mut next)?,
        "InsertWeightValidation" => feldera_sql_opt::insert_weight_validation(input, &mut next)?,
        "ExpandAggregateZero" => {
            let order: Vec<u64> = serde_json::from_value(invocation["nested_hoist_order"].clone())?;
            let order: Vec<_> = order.into_iter().map(NodeId::Outer).collect();
            feldera_sql_opt::expand_aggregate_zero(input, &mut next, &order)?
        }
        "OptimizeIncrementalVisitor" => feldera_sql_opt::push_integrals(input, &mut next)?,
        "ImplementJoins" => feldera_sql_opt::implement_joins(input, &mut next)?,
        "RemoveTable" => {
            let name = invocation["table_name"]
                .as_str()
                .ok_or("missing table name")?;
            feldera_sql_opt::remove_table(input, name, &mut next)?.circuit
        }
        "PushDifferentialsUp" => feldera_sql_opt::push_differentials_up(input, &mut next)?,
        "OptimizeWithGraph(PushDifferentialsUp)" => {
            feldera_sql_opt::optimize_differentials(input, &mut next)?
        }
        "PropagateEmptySources" => feldera_sql_opt::propagate_empty_sources(input, &mut next)?,
        "RemoveConstantFilters" => feldera_sql_opt::remove_constant_filters(input, &mut next)?,
        "CloneOperatorsWithFanout" => {
            feldera_sql_opt::clone_operators_with_fanout(input, &mut next)?
        }
        "OptimizeWithGraph(CloneOperatorsWithFanout)" => {
            feldera_sql_opt::optimize_fanout(input, &mut next)?
        }
        "BalancedJoins" => feldera_sql_opt::balance_joins(input, &mut next)?,
        "OptimizeWithGraph(BalancedJoins)" => {
            feldera_sql_opt::optimize_balanced_joins(input, &mut next)?
        }
        "MergeSums" => feldera_sql_opt::merge_sums(input, &mut next)?,
        "RemoveIAfterD" => feldera_sql_opt::remove_redundant_integrates(input, &mut next)?,
        "RemoveDeindexOperators" => feldera_sql_opt::remove_deindex_operators(input, &mut next)?,
        "RemoveNoops" => feldera_sql_opt::remove_noops(input, &mut next)?,
        "OptimizeWithGraph(RemoveNoops)" => feldera_sql_opt::optimize_noops(input, &mut next)?,
        "CSE" => {
            let instance = invocation["instance"]
                .as_u64()
                .ok_or("missing CSE instance")?;
            let shuffles: BTreeMap<u64, u64> =
                serde_json::from_value(invocation["flatmap_shuffles"].clone())?;
            let shuffles = shuffles
                .into_iter()
                .map(|(id, value)| (NodeId::Inner(id), value))
                .collect();
            cse_instances
                .entry(instance)
                .or_default()
                .run(input, &mut next, &shuffles)?
        }
        "FuseIntoArrayAgg" => feldera_sql_opt::fuse_into_array_agg(input, &mut next)?,
        _ => return Err(format!("no Rust implementation selected for {pass}").into()),
    };
    if Some(next) != invocation["names_after"].as_u64() {
        return Err(format!("{pass} invocation {index} generated-name counter is {next}, expected {}", invocation["names_after"]).into());
    }
    compare_circuits(&invocation["after"], &actual.to_json())
        .map_err(|e| format!("{pass} invocation {index}: {e}"))?;
    Ok(compare_circuits(&invocation["before"], &invocation["after"]).is_err())
}

#[derive(Default)]
struct Series {
    instances: Instances,
    count: usize,
    changed: usize,
    failed: bool,
}

impl Series {
    fn observe(&mut self, pass: &str, invocation: &Value) -> Result<(), Box<dyn Error>> {
        let index = self.count;
        self.count += 1;
        if self.failed {
            // The first mismatch already emitted the case and saved its input.
            // Still check every subsequent index and the terminating count.
            return Ok(());
        }
        let outcome = compare_invocation(pass, index, invocation, &mut self.instances);
        self.failed = outcome.is_err();
        self.changed += usize::from(outcome?);
        Ok(())
    }

    fn report(self, pass: &str) -> Value {
        json!({"pass":pass,"invocations":self.count,"changed":self.changed})
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut active = None::<(Value, Series)>;
    support::check(|row| {
        let pass = row["pass"].as_str().ok_or("missing pass class")?;
        let key = json!([row["id"], row["input_sha256"], pass]);
        if let Some(invocation) = row.get("invocation") {
            let (expected, series) = active.get_or_insert_with(|| (key.clone(), Series::default()));
            if *expected != key || row["index"].as_u64() != Some(series.count as u64) {
                return Err("missing or out-of-order streamed invocation".into());
            }
            series.observe(pass, invocation)?;
            return Ok(None);
        }
        let (expected, mut series) = active
            .take()
            .unwrap_or_else(|| (key.clone(), Series::default()));
        if expected != key {
            return Err("missing end of streamed case".into());
        }
        let invocations = row["invocations"].as_array().ok_or("missing invocations")?;
        if let Some(count) = row.get("streamed_invocations") {
            if count.as_u64() != Some(series.count as u64) || !invocations.is_empty() {
                return Err("streamed pass count differs from completed Java case".into());
            }
        } else {
            if series.count != 0 {
                return Err("missing stream count".into());
            }
            for invocation in invocations {
                series.observe(pass, invocation)?;
            }
        }
        Ok((!series.failed).then(|| series.report(pass)))
    })?;
    if active.is_some() {
        return Err("truncated final pass stream".into());
    }
    Ok(())
}
