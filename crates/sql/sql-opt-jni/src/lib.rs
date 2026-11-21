//! Run typed circuit passes inside the compiler JVM. Java retains the original
//! objects when an invocation changes only the generated-name counter.
use feldera_sql_ir::{parse_json, BodyOrder, Circuit, Field, NodeId, OutputPort};
use jni::{
    errors::ThrowRuntimeExAndDefault,
    objects::{JByteArray, JClass, JString},
    EnvUnowned,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, thiserror::Error)]
enum BridgeError {
    #[error(transparent)]
    Jni(#[from] jni::errors::Error),
    #[error("{0}")]
    Optimizer(String),
}

const MAX_BYTES: usize = 64 * 1024 * 1024;

fn annotations(circuit: &Circuit) -> Result<BTreeMap<u64, Value>, feldera_sql_ir::Error> {
    let mut values = BTreeMap::new();
    for id in circuit.operator_order(BodyOrder::Preorder)? {
        let NodeId::Outer(number) = id else {
            unreachable!("operator has an outer identity")
        };
        let node = circuit.node(id).expect("circuit body member");
        let Some(Field::Data(value @ Value::Array(_))) = node.field("annotations") else {
            return Err(feldera_sql_ir::Error::new("operator requires annotations"));
        };
        values.insert(number, value.clone());
    }
    Ok(values)
}

fn operator_number(id: NodeId) -> u64 {
    match id {
        NodeId::Outer(n) => n,
        NodeId::Inner(_) => unreachable!("operator has outer identity"),
    }
}

fn operator_numbers(ids: impl IntoIterator<Item = NodeId>) -> Vec<u64> {
    ids.into_iter().map(operator_number).collect()
}

fn ports(pairs: Vec<(u64, usize)>) -> BTreeSet<OutputPort> {
    pairs
        .into_iter()
        .map(|(id, number)| OutputPort {
            operator: NodeId::Outer(id),
            number,
        })
        .collect()
}

fn wire_ports(ports: BTreeSet<OutputPort>) -> Value {
    let pairs: Vec<_> = ports
        .into_iter()
        .map(|p| (operator_number(p.operator), p.number))
        .collect();
    json!(pairs)
}

fn multiplicities(facts: feldera_sql_opt::Multiplicities) -> Value {
    let mut operators = BTreeMap::<u64, Vec<Option<bool>>>::new();
    for (port, multiset) in facts {
        let outputs = operators.entry(operator_number(port.operator)).or_default();
        outputs.resize(port.number + 1, None);
        outputs[port.number] = Some(multiset);
    }
    json!(operators)
}

fn transform(
    pass: &str,
    error_view: &str,
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn std::error::Error>> {
    let live = pass.strip_prefix("Live/");
    let pass = live.unwrap_or(pass);
    if pass == "ContainsNow" {
        let calls: Vec<(String, usize)> = serde_json::from_slice(input)?;
        return Ok(json!(calls.iter().any(|(name, arity)| {
            feldera_sql_opt::is_clock_call(name, *arity)
        })));
    }
    if pass == "NoConnectorMetadata" {
        let calls: Vec<(String, feldera_sql_ir::provenance::SourceRange)> =
            serde_json::from_slice(input)?;
        let call = calls
            .into_iter()
            .find(|(name, _)| feldera_sql_opt::is_connector_metadata(name))
            .map(|(name, position)| json!({"name":name, "position":position}));
        return Ok(json!({"connector_call":call, "next_generated_name":next}));
    }
    let publish = |result: &Circuit, input: &Circuit| {
        if live.is_some() {
            result.changed_live_graph(input)
        } else {
            result.changed_graph(input)
        }
    };
    if let Some(pass) = pass.strip_prefix("Expressions/") {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Inventory<'a> {
            #[serde(borrow)]
            casts: Vec<&'a str>,
            #[serde(borrow)]
            binary: Vec<&'a str>,
            unwrap: bool,
            #[serde(default)]
            conditional: bool,
        }
        let sites: Inventory = serde_json::from_slice(input)?;
        let required = match pass {
            "SimplifyConditionals" => sites.conditional,
            "ExpandSafeCasts" => sites.casts.contains(&"SqlSafe"),
            "ExpandUnsafeCasts" => sites.casts.contains(&"SqlUnsafe"),
            "AdjustSqlIndex" => sites
                .binary
                .iter()
                .any(|op| matches!(*op, "SQL_INDEX" | "SAFE_RUST_INDEX")),
            "CreateRuntimeErrorWrappers" => {
                sites.unwrap
                    || sites
                        .casts
                        .iter()
                        .any(|mode| matches!(*mode, "UnwrapResultSafe" | "UnwrapResultUnsafe"))
            }
            other => return Err(format!("unknown expression pass {other}").into()),
        };
        return Ok(json!({"needs_full_snapshot":required}));
    }
    match pass {
        "AggregateGroups" => {
            let kinds: Vec<feldera_sql_opt::AggregateKind> = serde_json::from_slice(input)?;
            let append_only: bool = serde_json::from_slice(state)?;
            return Ok(json!(feldera_sql_opt::aggregate_groups(
                &kinds,
                append_only
            )));
        }
        "WindowGroups/Keys" => {
            let inputs: Vec<String> = serde_json::from_slice(input)?;
            let threshold: usize = serde_json::from_slice(state)?;
            return Ok(json!(feldera_sql_opt::qualifying_window_groups(
                &inputs, threshold
            )));
        }
        "WindowGroups/Fields" => {
            let fields: Vec<Vec<String>> = serde_json::from_slice(input)?;
            return Ok(serde_json::to_value(
                feldera_sql_opt::shared_window_fields(&fields),
            )?);
        }
        _ => {}
    }
    let wire = parse_json(input)?;
    if pass == "MultisetAnalysis" {
        let facts = feldera_sql_opt::stream_multiplicities(&Circuit::from_json(wire)?)?;
        return Ok(multiplicities(facts));
    }
    if pass == "Topology/FindCommonProjections" {
        return Ok(json!(feldera_sql_opt::common_projection_payloads(
            &Circuit::from_json(wire)?
        )?));
    }
    if pass == "FindCommonProjections" {
        return Ok(json!(feldera_sql_opt::common_projections(
            &Circuit::from_json(wire)?
        )?));
    }
    if pass == "Topology/FindUnusedInputFields" {
        return Ok(json!(operator_numbers(feldera_sql_opt::input_projections(
            &Circuit::from_json(wire)?
        )?)));
    }
    if pass == "FindUnusedInputFields" {
        return Ok(json!(feldera_sql_opt::input_field_uses(
            &Circuit::from_json(wire)?
        )?));
    }
    if pass == "Topology/MultisetAnalysis" {
        use feldera_sql_opt::MultiplicityPlan;
        let plan = feldera_sql_opt::plan_multiplicities(&Circuit::from_json(wire)?)?;
        return Ok(match plan {
            MultiplicityPlan::Complete(facts) => json!({"facts": multiplicities(facts)}),
            MultiplicityPlan::NeedsPayloads(ids) => json!({"payloads": operator_numbers(ids)}),
        });
    }
    let analysis = pass.strip_prefix("Topology/").unwrap_or(pass);
    if matches!(analysis, "FindEquivalentNoops" | "FindMultipleRetains") {
        let circuit = Circuit::from_json(wire)?;
        // All other retention kinds can be compared from the flags and edges.
        if analysis != pass
            && circuit
                .operator_order(BodyOrder::Preorder)?
                .iter()
                .any(|&id| {
                    circuit.node(id).and_then(|n| n.operator_kind())
                        == Some(feldera_sql_ir::OperatorKind::IntegrateTraceRetainNValues)
                })
        {
            return Ok(json!({"needs_full_snapshot":true}));
        }
        let facts = if analysis == "FindEquivalentNoops" {
            json!(feldera_sql_opt::share_trace_inputs(&circuit)?)
        } else {
            json!(feldera_sql_opt::retention_groups(
                &circuit,
                &serde_json::from_slice(state)?
            )?)
        };
        return Ok(json!({"facts": facts}));
    }
    if pass == "Graph" {
        return Ok(json!(feldera_sql_opt::consumer_graphs(
            &Circuit::from_json(wire)?
        )?));
    }
    if pass == "DetectStreamingOperations" {
        return Ok(json!(feldera_sql_opt::detects_streaming(
            &Circuit::from_json(wire)?
        )?));
    }
    if pass == "FindBounded" {
        #[derive(serde::Deserialize)]
        struct BoundsContext {
            known: Vec<(u64, usize)>,
            keys: Vec<(u64, usize)>,
            now_table: String,
        }
        let context: BoundsContext = serde_json::from_slice(state)?;
        let bounded = feldera_sql_opt::propagate_bounds(
            &Circuit::from_json(wire)?,
            ports(context.known),
            &ports(context.keys),
            &context.now_table,
        )?;
        return Ok(wire_ports(bounded));
    }
    if pass == "CollectUnbounded" {
        let bounded = ports(serde_json::from_slice(state)?);
        return Ok(json!(feldera_sql_opt::unbounded_operators(
            &Circuit::from_json(wire)?,
            &bounded
        )?));
    }
    if pass == "FindGCedStreams" {
        return Ok(wire_ports(feldera_sql_opt::pruned_streams(
            &Circuit::from_json(wire)?,
        )?));
    }
    if pass == "AppendOnly" {
        let streams = feldera_sql_opt::append_only_streams(&Circuit::from_json(wire)?)?;
        return Ok(json!(operator_numbers(streams)));
    }
    if pass == "FindWindowGroups" {
        let candidates = feldera_sql_opt::window_candidates(&Circuit::from_json(wire)?)?;
        return Ok(json!(operator_numbers(candidates)));
    }
    if let Some(name) = pass.strip_prefix("Topology/") {
        let circuit = Circuit::from_json(wire)?;
        if name == "CSE" {
            return Ok(
                json!({"payloads": operator_numbers(feldera_sql_opt::cse_payloads(&circuit)?)}),
            );
        }
        if name == "SeparateIntegrators" {
            if let Some(id) = feldera_sql_opt::snapshot_aggregate(&circuit)? {
                return Ok(json!({"stream_aggregate":operator_number(id)}));
            }
        }
        if name == "StrayGC" {
            if let Some(error) = feldera_sql_opt::retention_violation(&circuit)? {
                return Ok(json!({"retention_violation": error}));
            }
        }
        if matches!(name, "DeadCode" | "DeadCodeKeepSources") {
            let pruning = feldera_sql_opt::pruning(&circuit, name == "DeadCodeKeepSources")?;
            return Ok(json!({"pruned_operators":operator_numbers(pruning.removed),
                "unused_sources":operator_numbers(pruning.unused_sources)}));
        }
        let needs_full_snapshot = feldera_sql_opt::needs_full_snapshot(name, &circuit)?;
        return Ok(json!({ "needs_full_snapshot": needs_full_snapshot }));
    }
    if matches!(pass, "NoIntegralVisitor" | "ValidateRecursiveOperators") {
        let circuit = Circuit::from_json(wire)?;
        let violation = if pass == "NoIntegralVisitor" {
            feldera_sql_opt::first_integral(&circuit)?.map(|id| {
                let NodeId::Outer(operator) = id else { unreachable!("integrator is an operator") };
                json!({"operator":operator, "message":"Circuit contains an integration operator", "internal":true})
            })
        } else {
            feldera_sql_opt::recursive_violation(&circuit)?.map(|error| json!({
                "operator":error.operator, "internal":error.internal,
                "message":format!("Unsupported operation '{}' in recursive code", error.operation),
            }))
        };
        return Ok(json!({"violation":violation, "next_generated_name":next}));
    }
    if pass == "FindMapChains" {
        let chains: Vec<_> = feldera_sql_opt::find_map_chains(&Circuit::from_json(wire)?)?
            .into_iter()
            .map(operator_numbers)
            .collect();
        return Ok(json!({"map_chains": chains}));
    }
    if pass == "OrderErrorConsumers" {
        let table: &str = serde_json::from_slice(state)?;
        let order = feldera_sql_opt::error_consumers(&Circuit::from_json(wire)?, table)?;
        return Ok(
            json!({"order": operator_numbers(order.operators), "first_error": order.first_error}),
        );
    }
    if pass == "CheckRetain" {
        let duplicate = feldera_sql_opt::duplicate_retention(&Circuit::from_json(wire)?)?;
        return Ok(json!({"duplicate_retention":duplicate,"next_generated_name":next}));
    }
    if pass == "FixupViewReferences" {
        return Ok(
            match feldera_sql_opt::resolve_view_references(&Circuit::from_json(wire)?)? {
                feldera_sql_opt::ViewResolution::UndeclaredRecursion { operator, message } => {
                    json!({"violation":{"operator":operator_number(operator), "internal":false, "message":message}})
                }
                feldera_sql_opt::ViewResolution::Valid(plan) => {
                    let redirects: BTreeMap<_, _> = plan
                        .redirects
                        .into_iter()
                        .map(|(id, slots)| {
                            let inputs: BTreeMap<_, _> = slots
                                .into_iter()
                                .map(|(slot, source)| (slot, operator_number(source)))
                                .collect();
                            (operator_number(id), inputs)
                        })
                        .collect();
                    json!({"order":operator_numbers(plan.order), "redirects":redirects})
                }
            },
        );
    }
    if pass == "RemoveTable" {
        let name: String = serde_json::from_slice(state)?;
        let ids = feldera_sql_opt::unreferenced_table_sources(&Circuit::from_json(wire)?, &name)?;
        return Ok(json!({"remove_sources":operator_numbers(ids)}));
    }
    if pass == "CircuitStatistics" {
        let stats = feldera_sql_opt::circuit_statistics(&Circuit::from_json(wire)?)?;
        return Ok(json!({"statistics":stats,"next_generated_name":next}));
    }
    if pass == "ConstantViews" {
        let sinks = feldera_sql_opt::constant_views(&Circuit::from_json(wire)?, error_view)?;
        return Ok(json!({"constant_views":operator_numbers(sinks), "next_generated_name":next}));
    }
    if pass == "DeterministicFunctions" {
        let findings = feldera_sql_opt::nondeterministic_initializers(&Circuit::from_json(wire)?)?;
        return Ok(json!({"nondeterministic_initializers":findings,"next_generated_name":next}));
    }
    if pass == "CheckHints" {
        let diagnostics = feldera_sql_opt::check_hints(&Circuit::from_json(wire)?)?;
        return Ok(json!({"diagnostics":diagnostics, "next_generated_name":next}));
    }
    if pass == "CheckPersistentIds" {
        let findings = feldera_sql_opt::check_persistent_ids(&Circuit::from_json(wire)?)?;
        return Ok(json!({"persistent_id_findings": findings}));
    }
    if matches!(pass, "DeltaHashPrefixes" | "GcHashPrefixes") {
        let inputs: feldera_sql_opt::PrefixInputs = serde_json::from_slice(state)?;
        let circuit = Circuit::from_json(wire)?;
        let prefixes = if pass == "DeltaHashPrefixes" {
            inputs.delta_prefixes(&circuit)?
        } else {
            inputs.gc_prefixes(&circuit)?
        };
        return Ok(json!({"prefixes":prefixes}));
    }
    if matches!(pass, "CompactNames" | "TagRegions" | "SetOperatorHashes") {
        // Java implements these passes by mutating annotations on the live
        // operators. Preserve those identities for the subsequent hash pass.
        let circuit = Circuit::from_json(wire)?;
        let before = annotations(&circuit)?;
        let changed = match pass {
            "CompactNames" => feldera_sql_opt::compact_names(circuit)?,
            "SetOperatorHashes" => {
                serde_json::from_slice::<feldera_sql_opt::ComputedHashes>(state)?
                    .annotate(circuit)?
            }
            _ => feldera_sql_opt::tag_regions(circuit)?,
        };
        let mut edits = annotations(&changed)?;
        edits.retain(|id, value| before.get(id) != Some(value));
        return Ok(json!({"annotations": edits, "next_generated_name": next}));
    }
    let original = Circuit::from_json(wire)?;
    let circuit = original.clone();
    if pass == "RemoveUselessLeftJoinsVisitor" {
        let keyed: Vec<u64> = serde_json::from_slice(state)?;
        let keyed = keyed.into_iter().map(NodeId::Outer).collect();
        let (result, removed) =
            feldera_sql_opt::remove_useless_left_joins(circuit, &keyed, &mut next)?;
        return Ok(
            json!({"circuit": publish(&result, &original), "removed_left_joins": operator_numbers(removed),
            "unused_sources": [], "next_generated_name": next}),
        );
    }
    if pass == "BuildNestedOperators" {
        let result = feldera_sql_opt::build_nested_operators(circuit, &mut next)?;
        let late = result
            .diagnostics
            .late_column
            .map(|(id, column)| json!({"operator":operator_number(id), "column":column}));
        return Ok(
            json!({"circuit":publish(&result.circuit, &original), "next_generated_name":next,
            "unused_sources":[], "unused_declarations":operator_numbers(result.diagnostics.unused_declarations),
            "late_column":late}),
        );
    }
    let mut unused_sources = Vec::new();
    let result = match pass {
        "EnsureDistinctOutputs" => {
            let sinks = serde_json::from_slice::<Vec<_>>(state)?;
            feldera_sql_opt::ensure_distinct_outputs(circuit, &sinks, &mut next)?
        }
        "ImplementWindowGroups" => {
            let groups = serde_json::from_slice::<Vec<_>>(state)?;
            feldera_sql_opt::implement_window_groups(circuit, &groups, &mut next)?
        }
        "CollapseSharedChains" => {
            let chains: Vec<Vec<u64>> = serde_json::from_slice(state)?;
            let chains: Vec<_> = chains
                .into_iter()
                .map(|c| c.into_iter().map(NodeId::Outer).collect())
                .collect();
            feldera_sql_opt::collapse_shared_chains(circuit, &chains, &mut next)?
        }
        "DuplicateSharedIndexes" => feldera_sql_opt::duplicate_shared_indexes(circuit, &mut next)?,
        "ImplementChains" => feldera_sql_opt::implement_chains(circuit, &mut next)?,
        "EnsureExpressionTrees" => feldera_sql_opt::ensure_expression_trees(circuit, &mut next)?,
        "MergeRetain" => {
            let groups: Vec<Vec<u64>> = serde_json::from_slice(state)?;
            feldera_sql_opt::merge_retention_policies(circuit, &groups, &mut next)?
        }
        "MergeNoops" => {
            let inputs: feldera_sql_opt::SharedTraceInputs = serde_json::from_slice(state)?;
            inputs.apply(circuit, &mut next)?
        }
        "OptimizeDistinctVisitor" => feldera_sql_opt::optimize_distinct(circuit, &mut next)?,
        "UDFInliner" => feldera_sql_opt::inline_udfs(circuit, &mut next)?,
        "SubstituteLeftJoins" => feldera_sql_opt::substitute_left_joins(circuit, &mut next)?,
        "DeadCode" | "DeadCodeKeepSources" => {
            let result = feldera_sql_opt::eliminate_dead_code(
                circuit,
                &mut next,
                pass == "DeadCodeKeepSources",
            )?;
            for id in result.unused_sources {
                let NodeId::Outer(number) = id else {
                    unreachable!("table has an outer identity")
                };
                unused_sources.push(number);
            }
            result.circuit
        }
        "CSE" => {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Sidecar {
                seen_constants: Vec<u64>,
                shuffles: BTreeMap<u64, u64>,
            }
            let sidecar: Sidecar = serde_json::from_slice(state)?;
            let cse = feldera_sql_opt::CseState::with_seen_constants(
                sidecar.seen_constants.into_iter().map(NodeId::Outer),
            );
            let shuffles = sidecar
                .shuffles
                .into_iter()
                .map(|(id, group)| (NodeId::Inner(id), group))
                .collect();
            let aliases: Vec<_> = cse
                .aliases(&circuit, &shuffles)?
                .into_iter()
                .map(|(id, base)| [operator_number(id), operator_number(base)])
                .collect();
            return Ok(json!({"aliases":aliases}));
        }
        "IncrementalizeVisitor" => feldera_sql_opt::incrementalize(circuit, &mut next)?,
        "InsertWeightValidation" => feldera_sql_opt::insert_weight_validation(circuit, &mut next)?,
        "ExpandAggregateZero" => {
            let order: Vec<u64> = serde_json::from_slice(state)?;
            let order: Vec<_> = order.into_iter().map(NodeId::Outer).collect();
            feldera_sql_opt::expand_aggregate_zero(circuit, &mut next, &order)?
        }
        "ImplementJoins" => feldera_sql_opt::implement_joins(circuit, &mut next)?,
        "ShareOutputIntegrators" => feldera_sql_opt::share_output_integrators(circuit, &mut next)?,
        "FilterJoinVisitor" => feldera_sql_opt::filter_joins(circuit, &mut next)?,
        "SimplifyConditionals" => feldera_sql_opt::simplify_conditionals(circuit, &mut next)?,
        "SimplifyWaterline" => feldera_sql_opt::simplify_waterlines(circuit, &mut next)?,
        "DesugarNowEquality" => feldera_sql_opt::desugar_now_equality(circuit, &mut next)?,
        "ExpandStreamAggregates"
        | "OptimizeProjectionVisitor"
        | "PullFilterVisitor"
        | "PropagateConstants"
        | "Simplify"
        | "DecomposeExpensiveFilters"
        | "BreakFilters" => {
            let mut weight = None;
            let (output, warnings) = match pass {
                "ExpandStreamAggregates" => {
                    let context = serde_json::from_slice(state)?;
                    let result =
                        feldera_sql_opt::expand_stream_aggregates(circuit, &context, &mut next)?;
                    weight = result.new_weight;
                    (result.circuit, result.warnings)
                }
                "OptimizeProjectionVisitor" => {
                    feldera_sql_opt::project_outputs(circuit, &mut next)?
                }
                "PullFilterVisitor" => feldera_sql_opt::pull_filters(circuit, &mut next)?,
                "PropagateConstants" => feldera_sql_opt::propagate_constants(circuit, &mut next)?,
                "Simplify" => feldera_sql_opt::simplify_expressions(circuit, &mut next)?,
                "BreakFilters" => feldera_sql_opt::break_temporal_filters(circuit, &mut next)?,
                _ => feldera_sql_opt::decompose_expensive_filters(circuit, &mut next)?,
            };
            let warnings: Vec<_> = warnings
                .into_iter()
                .map(|w| {
                    json!({
                        "range":w.position, "error_type":w.kind, "message":w.message,
                        "warning":true, "continuation":false
                    })
                })
                .collect();
            return Ok(
                json!({"circuit":publish(&output, &original), "diagnostics":warnings, "weight_node":weight,
                "next_generated_name":next, "unused_sources":[]}),
            );
        }
        "RemoveIdentityOperators" => {
            feldera_sql_opt::remove_identity_operators(circuit, &mut next)?
        }
        "ReplaceCommonProjections" => feldera_sql_opt::replace_common_projections(
            circuit, serde_json::from_slice(state)?, &mut next)?,
        "TrimInputs" => {
            feldera_sql_opt::trim_inputs(circuit, &serde_json::from_slice(state)?, &mut next)?
        }
        "IndexedInputs" => feldera_sql_opt::expand_indexed_inputs(
            circuit,
            serde_json::from_slice(state)?,
            &mut next,
        )?,
        "ExpandHop" => feldera_sql_opt::expand_hop(circuit, &mut next)?,
        "LinearPostprocessRetainKeys" => {
            feldera_sql_opt::linear_postprocess_retain_keys(circuit, &mut next)?
        }
        "PushDifferentialsUp" => feldera_sql_opt::optimize_differentials(circuit, &mut next)?,
        "PropagateEmptySources" => feldera_sql_opt::propagate_empty_sources(circuit, &mut next)?,
        "RemoveConstantFilters" => feldera_sql_opt::remove_constant_filters(circuit, &mut next)?,
        "OptimizeIncrementalVisitor" => feldera_sql_opt::push_integrals(circuit, &mut next)?,
        "BalancedJoins" => feldera_sql_opt::optimize_balanced_joins(circuit, &mut next)?,
        "SeparateIntegrators" => feldera_sql_opt::separate_integrators(circuit, &mut next)?,
        "StrayGC" => {
            let (result, violation) =
                feldera_sql_opt::check_and_prune_retention(circuit, &mut next)?;
            if let Some(error) = violation {
                return Ok(json!({"retention_violation":error,"next_generated_name":next}));
            }
            result
        }
        "ChainVisitor" => feldera_sql_opt::optimize_chains(circuit, &mut next)?,
        "CloneOperatorsWithFanout" => feldera_sql_opt::optimize_fanout(circuit, &mut next)?,
        "MergeSums" => feldera_sql_opt::merge_sums(circuit, &mut next)?,
        "RemoveIAfterD" => feldera_sql_opt::remove_redundant_integrates(circuit, &mut next)?,
        "RemoveAllViews" | "RemoveViewOperators" => {
            let mode = if pass == "RemoveAllViews" {
                feldera_sql_opt::ViewRemoval::All
            } else {
                feldera_sql_opt::ViewRemoval::Ordinary { error_view }
            };
            feldera_sql_opt::remove_views(circuit, mode, &mut next)?
        }
        "RemoveNoops" => feldera_sql_opt::optimize_noops(circuit, &mut next)?,
        "FuseIntoArrayAgg" => feldera_sql_opt::fuse_into_array_agg(circuit, &mut next)?,
        "ComparatorDeclarations" => feldera_sql_opt::declare_comparators(circuit)?,
        "ExpandSafeCasts" | "ExpandUnsafeCasts" => {
            let safety = if pass == "ExpandSafeCasts" {
                feldera_sql_opt::CastSafety::Safe
            } else {
                feldera_sql_opt::CastSafety::Unsafe
            };
            feldera_sql_opt::expand_circuit_casts(circuit, &mut next, safety)?
        }
        "ExpandMetadataCasts" => feldera_sql_opt::expand_metadata_casts(circuit, &mut next)?,
        "CreateRuntimeErrorWrappers" => {
            feldera_sql_opt::create_runtime_error_wrappers(circuit, &mut next)?
        }
        "AdjustSqlIndex" => feldera_sql_opt::adjust_sql_index(circuit)?,
        "Gen2Passes" => feldera_sql_opt::gen2_passes(circuit)?,
        "RemoveDeindexOperators" => feldera_sql_opt::remove_deindex_operators(circuit, &mut next)?,
        other => return Err(format!("unknown optimizer pass {other}").into()),
    };
    let changed = publish(&result, &original);
    Ok(json!({"circuit": changed, "next_generated_name": next, "unused_sources": unused_sources}))
}

// SAFETY: the export name is unique to the matching static native method in
// RustCircuitPass. EnvUnowned contains panics before returning to Java.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_dbsp_sqlCompiler_compiler_backend_RustCircuitPass_transformWithContext<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    pass: JString<'local>,
    circuit: JByteArray<'local>,
    next: i64,
    error_view: JString<'local>,
    state: JByteArray<'local>,
) -> JByteArray<'local> {
    env.with_env(|env| -> Result<_, BridgeError> {
        if circuit.len(env)?.saturating_add(state.len(env)?) > MAX_BYTES {
            return Err(BridgeError::Optimizer(
                "optimizer input exceeds 64 MiB".into(),
            ));
        }
        let bytes = env.convert_byte_array(&circuit)?;
        let next = u64::try_from(next).map_err(|e| BridgeError::Optimizer(e.to_string()))?;
        let state = env.convert_byte_array(&state)?;
        let pass = pass.to_string();
        let result = transform(&pass, &error_view.to_string(), &bytes, &state, next)
            .map_err(|err| BridgeError::Optimizer(format!("{pass}: {err}")))?;
        let bytes =
            serde_json::to_vec(&result).map_err(|e| BridgeError::Optimizer(e.to_string()))?;
        if bytes.len() > MAX_BYTES {
            return Err(BridgeError::Optimizer(
                "optimizer output exceeds 64 MiB".into(),
            ));
        }
        Ok(env.byte_array_from_slice(&bytes)?)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
