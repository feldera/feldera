//! Disposable analysis and graph-rewrite slice for P0 spike 2. This is not a
//! complete typed IR decoder or optimizer. Unsupported forms fail closed.

mod analysis;
mod bypass;
mod constants;
mod decode;
mod empty;
mod filters;
mod integrate;
mod model;
mod provenance;
mod recursion;
mod scalar;
mod sums;
mod window_groups;

use serde_json::Value;
use std::fmt;

#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

/// Infer whether each operator output may contain a multiset. The outer vector
/// follows `allOperators`; each inner vector follows output-port order.
///
/// Requires a flat circuit in topological order. Rejects unsupported forms,
/// malformed graph references, invalid port numbers, and excessive nesting.
/// Saved annotations and expected analysis results are never used as inputs.
pub fn analyze(circuit: &Value) -> Result<Vec<Vec<bool>>> {
    let decoded = decode::circuit(circuit)?;
    Ok(analysis::analyze(&decoded))
}

/// Remove operators unreachable from sinks or trace-retention roots, retaining
/// every table source.
/// Nested circuits keep their numbered exports. Dead exports become empty
/// slots, and shared inner definitions move to their first surviving use.
pub fn remove_dead_operators(circuit: &Value) -> Result<Value> {
    decode::reachability::prune(circuit, true)
}

/// Prune the circuit and identify unused inputs in compiler source order.
pub fn eliminate_dead_code(request: &Value) -> Result<Value> {
    decode::reachability::eliminate(request)
}

/// Prune dependent operators and unused table sources. The Java caller runs
/// its diagnostic analysis before this transformation.
pub fn remove_unused_operators(circuit: &Value) -> Result<Value> {
    decode::reachability::prune(circuit, false)
}

/// Bypass ordinary views in the flat subset under default identifier casing.
/// Recursive views, views with lateness and the system error view are retained.
pub fn remove_view_operators(circuit: &Value) -> Result<Value> {
    strip_views(circuit, false)
}

/// Bypass all view operators after the optimizer has consumed their metadata.
pub fn remove_all_view_operators(circuit: &Value) -> Result<Value> {
    strip_views(circuit, true)
}

fn strip_views(circuit: &Value, all: bool) -> Result<Value> {
    let decoded = decode::circuit(circuit)?;
    let edits = bypass::bypass_views(&decoded, all);
    decode::rewrite_operators(circuit, &decoded, edits)
}

/// Bypass noops that do not directly feed trace retention.
pub fn remove_noops(circuit: &Value) -> Result<Value> {
    let decoded = decode::circuit(circuit)?;
    let edits = bypass::bypass_noops(&decoded);
    decode::rewrite_operators(circuit, &decoded, edits)
}

/// Remove identity maps, retaining a noop where retention cannot move.
pub fn remove_identity_maps(circuit: &Value) -> Result<Value> {
    let decoded = decode::circuit(circuit)?;
    let (edits, names) = decode::identity_maps(circuit, &decoded)?;
    let mut circuit = circuit.clone();
    if let Some(next) = circuit.get(decode::GENERATED_NAME) {
        let next = next
            .as_u64()
            .ok_or_else(|| Error("invalid generated-name counter".into()))?;
        circuit[decode::GENERATED_NAME] = decode::advance_name(next, names)?.into();
    }
    decode::rewrite_operators(&circuit, &decoded, edits)
}

/// Run Java's `MergeSums` rewrite on the supported flat operator subset.
/// Keeps input order, duplicates and output-port numbers after cancellation.
pub fn merge_sums(circuit: &Value) -> Result<Value> {
    let decoded = decode::circuit(circuit)?;
    let edits = sums::merge(&decoded)?;
    decode::rewrite_operators(circuit, &decoded, edits)
}

/// Propagate statically empty collections through the supported flat operators.
/// Empty replacements retain the original output's collection kind and type.
pub fn propagate_empty_sources(circuit: &Value) -> Result<Value> {
    let decoded = decode::stream_circuit(circuit)?;
    let edits = empty::propagate(&decoded)?;
    decode::rewrite_operators(circuit, &decoded, edits)
}

/// Propagate snapshot constants through the supported negations and closures.
/// Refuse non-empty constant folds that need unsupported expression evaluation.
pub fn propagate_constants(circuit: &Value) -> Result<Value> {
    let decoded = decode::circuit(circuit)?;
    let (inputs, mut literals) = decode::constant_inputs(circuit, &decoded)?;
    let edits = constants::propagate(&decoded, inputs, &mut literals)?;
    decode::rewrite_with_generated_literals(circuit, &decoded, edits, &literals.generated)
}

/// Remove an integration when it cancels a differentiation or converts a
/// delta constant into the corresponding collection constant.
pub fn remove_redundant_integrates(circuit: &Value) -> Result<Value> {
    let decoded = decode::circuit(circuit)?;
    let edits = integrate::remove_redundant(&decoded);
    decode::rewrite_operators(circuit, &decoded, edits)
}

/// Turn deindex operators into maps using the same function and output type.
pub fn remove_deindex_operators(circuit: &Value) -> Result<Value> {
    let decoded = decode::circuit(circuit)?;
    let edits = decoded
        .operators
        .iter()
        .map(|operator| {
            if matches!(operator.kind, model::OperatorKind::Deindex(_)) {
                model::Edit::ReplaceDeindex(operator.inputs.clone())
            } else {
                model::Edit::Retain(operator.inputs.clone())
            }
        })
        .collect();
    decode::rewrite_operators(circuit, &decoded, edits)
}

/// Remove the unused system error input and its schema entry after the
/// optimizer has finished monotonicity analysis.
pub fn remove_error_table(circuit: &Value) -> Result<Value> {
    decode::remove_system_table(circuit, "feldera_error_table")
}

/// Remove the clock input after the temporal pass has handled its uses.
pub fn remove_now_table(circuit: &Value) -> Result<Value> {
    decode::remove_system_table(circuit, "now")
}

/// Identify user sinks fed by constants, possibly through differentiation.
pub fn constant_view_sinks(circuit: &Value) -> Result<Vec<u64>> {
    let decoded = decode::circuit(circuit)?;
    let operators = circuit["allOperators"].as_array().expect("validated array");
    let mut constant = Vec::with_capacity(operators.len());
    let mut sinks = Vec::new();
    for (operator, wire) in decoded.operators.iter().zip(operators) {
        let is_constant = |port: model::Port| port.output == 0 && constant[port.operator.0];
        let produces_constant = match operator.kind {
            model::OperatorKind::Constant { .. } => true,
            model::OperatorKind::Differentiate => is_constant(operator.inputs[0]),
            model::OperatorKind::Sink if is_constant(operator.inputs[0]) => {
                let name = wire["viewName"]["name"]
                    .as_str()
                    .ok_or_else(|| Error("constant sink lacks a view name".into()))?;
                if name != "error_view" {
                    sinks.push(wire["id"].as_u64().expect("validated operator id"));
                }
                false
            }
            _ => false,
        };
        constant.push(produces_constant);
    }
    Ok(sinks)
}

/// Assign compact output stream names in the Java flat-circuit visit order.
pub fn compact_names(circuit: &Value) -> Result<Value> {
    decode::circuit(circuit)?;
    let mut named = circuit.clone();
    for (index, operator) in named["allOperators"]
        .as_array_mut()
        .expect("validated array")
        .iter_mut()
        .enumerate()
    {
        let annotations = operator["annotations"]
            .as_array_mut()
            .ok_or_else(|| Error("missing operator annotations for compact naming".into()))?;
        annotations.push(serde_json::json!({"class": "CompactName", "name": format!("s{index}")}));
    }
    Ok(named)
}

/// Check the post-incrementalization invariant.
pub fn require_no_integrals(circuit: &Value) -> Result<Value> {
    if first_integral(circuit)?.is_some() {
        return Err(Error("Circuit contains an integration operator".into()));
    }
    Ok(circuit.clone())
}

/// Return the first integration operator in Java's circuit visit order.
pub fn first_integral(circuit: &Value) -> Result<Option<u64>> {
    recursion::first_integral(circuit)
}

/// Move differentiation ahead of a map or map-index whose only consumer is
/// that differentiation operator.
pub fn push_differentials(circuit: &Value) -> Result<Value> {
    let decoded = decode::circuit(circuit)?;
    let edits = integrate::push_differentials(&decoded);
    decode::rewrite_operators(circuit, &decoded, edits)
}

/// Expand snapshot antijoins and indexed joins through supported delta operators.
pub fn implement_joins(circuit: &Value) -> Result<Value> {
    decode::implement_joins::rewrite(circuit)
}

/// Add the initial integral and differential boundaries to a flat circuit.
pub fn incrementalize(circuit: &Value) -> Result<Value> {
    decode::incrementalize::rewrite(circuit)
}

/// Move integrals through the supported flat operators in Java pass order.
pub fn optimize_incremental(circuit: &Value) -> Result<Value> {
    decode::optimize_incremental::rewrite(circuit)
}

/// Return the error table and its downstream operators in circuit order.
pub fn select_error_consumers(circuit: &Value) -> Result<Vec<u64>> {
    decode::order_error_consumers::select(circuit)
}

/// Insert noops to isolate traces before monotonicity analysis.
pub fn separate_integrators(circuit: &Value) -> Result<Value> {
    decode::separate_integrators::rewrite(circuit)
}

/// Eliminate redundant distinct operators and move them through supported streams.
pub fn optimize_distinct(circuit: &Value) -> Result<Value> {
    decode::optimize_distinct::rewrite(circuit)
}

/// Split shared variable paths in supported indexed-row reconstructions.
pub fn inner_cse(circuit: &Value) -> Result<Value> {
    decode::inner_cse::rewrite(circuit)
}

/// Expand supported shared variable paths before monotonicity analysis.
pub fn ensure_expression_trees(circuit: &Value) -> Result<Value> {
    decode::inner_cse::ensure_expression_trees(circuit)
}

/// Fuse supported tuple projections into single-consumer stream joins.
pub fn optimize_projections(circuit: &Value) -> Result<Value> {
    decode::optimize_projections::rewrite(circuit)
}

/// Simplify supported function declarations and retention predicates.
pub fn simplify(circuit: &Value) -> Result<Value> {
    decode::simplify::rewrite(circuit)
}

/// Remove the clone wrappers in the measured Gen-2 circuit forms.
pub fn gen2_clones(circuit: &Value) -> Result<Value> {
    decode::gen2_clones::rewrite(circuit)
}

/// Analyze isolated stream joins and preserve closure name allocation.
pub fn analyze_star_joins(circuit: &Value) -> Result<Value> {
    decode::star_joins::analyze(circuit)
}

/// Hoist the measured classic key, waterline and diagnostic constants.
pub fn implement_statics(circuit: &Value) -> Result<Value> {
    decode::implement_statics::rewrite(circuit)
}

/// Analyze direct row projections and their untouched primary-key columns.
pub fn analyze_unused_fields(circuit: &Value) -> Result<Value> {
    decode::unused_fields::analyze(circuit)
}

/// Report unbounded distinct traces in the supported streaming circuit forms.
pub fn find_unbounded_state(circuit: &Value) -> Result<Value> {
    decode::unbounded_state::analyze(circuit)
}

/// Find two policies writing the same retention slot of one trace.
pub fn duplicate_retention(circuit: &Value) -> Result<Value> {
    decode::trace_retention::duplicate(circuit)
}

/// Preserve retention when no trace or noop has another merge candidate.
pub fn merge_gc(circuit: &Value) -> Result<Value> {
    decode::trace_retention::merge_gc(circuit)
}

/// Expand supported circuit operators into the analysis-only delta graph.
pub fn expand_deltas(circuit: &Value) -> Result<Value> {
    decode::delta_expansion::expand(circuit)
}

/// Replay a pass whose supported input has no candidate for its rewrite.
pub fn require_inactive_pass(circuit: &Value, pass: &str) -> Result<Value> {
    decode::inactive_passes::check(circuit, pass)
}

/// Build supported waterlines and error streams. Null leaves the graph to Java.
pub fn insert_limiters(circuit: &Value) -> Result<Value> {
    decode::limiters::insert_limiters(circuit)
}

/// Determine the facts needed by the supported limiter plans.
pub fn analyze_limiter_inputs(circuit: &Value) -> Result<Value> {
    decode::limiters::analyze(circuit)
}

/// Encode the Rust circuit into a version-four S3 replay artifact for Java.
pub fn reencode_s3(envelope: &Value) -> Result<Value> {
    if envelope["schema_version"] != 4 || envelope["stage"] != "S3-subset" {
        return Err(Error("expected a version-four S3 artifact".into()));
    }
    let circuit = envelope
        .get("circuit")
        .ok_or_else(|| Error("S3 artifact has no circuit".into()))?;
    let mut output = envelope.clone();
    output["circuit"] = decode::encode_rewrite(circuit, circuit)?;
    Ok(output)
}

/// Materialize a supported clock projection and report its source warnings.
pub fn implement_now(request: &Value) -> Result<Value> {
    decode::implement_now::rewrite(request)
}

/// Expand supported primary-key sources in the selected engine mode.
pub fn expand_indexed_inputs(circuit: &Value, dedup_keys: bool) -> Result<Value> {
    decode::indexed_inputs::expand(circuit, dedup_keys)
}

/// Pull supported predicates through single-consumer tuple projections.
pub fn pull_filter(circuit: &Value) -> Result<Value> {
    decode::pull_filter::rewrite(circuit)
}

/// Add handlers to supported Rust result unwrapping operations.
pub fn wrap_runtime_cast_errors(circuit: &Value) -> Result<Value> {
    decode::runtime_error_wrappers::wrap(circuit)
}

/// Expand supported scalar and metadata casts.
pub fn expand_casts(circuit: &Value) -> Result<Value> {
    decode::expand_casts::expand(circuit)
}

/// Rename the bound variables of a supported circuit closure.
pub fn canonical_form(circuit: &Value) -> Result<Value> {
    decode::canonical_form::canonicalize(circuit)
}

/// Collapse the bounded indexed-row projection chain.
pub fn collapse_chains(circuit: &Value) -> Result<Value> {
    decode::chain_collapse::collapse(circuit)
}

/// Fuse a single iteration of flat, single-consumer map and filter chains.
pub fn fuse_chains(circuit: &Value) -> Result<Value> {
    decode::chains::fuse(circuit)
}

/// Convert bounded signed SQL array indexes to the runtime's isize offset.
pub fn adjust_array_indexes(circuit: &Value) -> Result<Value> {
    decode::sql_index::adjust(circuit)
}

/// Run the consecutive early set-output passes across one Java/Rust boundary.
pub fn optimize_early_sets(circuit: &Value) -> Result<Value> {
    let distinct = ensure_distinct_outputs(circuit)?;
    decode::stream_kinds::infer(&distinct)?;
    let constants = propagate_constants(&distinct)?;
    decode::stream_kinds::infer(&constants)?;
    let nonempty = propagate_empty_sources(&constants)?;
    decode::stream_kinds::infer(&nonempty)?;
    let sums = merge_sums(&nonempty)?;
    decode::stream_kinds::infer(&sums)?;
    Ok(sums)
}

/// Apply the scheduled stream rewrites and prune dead operators between them.
pub fn optimize_streams(circuit: &Value, incremental: bool) -> Result<Value> {
    fn remove_round_trips(circuit: &Value) -> Result<Value> {
        let graph = decode::stream_circuit(circuit)?;
        decode::rewrite_operators(circuit, &graph, integrate::remove_redundant(&graph))
    }

    type Pass = fn(&Value) -> Result<Value>;
    let mut passes: Vec<Pass> = vec![
        remove_dead_operators,
        propagate_empty_sources,
        optimize_distinct,
        optimize_incremental,
        remove_dead_operators,
    ];
    if incremental {
        passes.push(incrementalize);
    }
    passes.extend([
        optimize_incremental as Pass,
        remove_round_trips,
        remove_dead_operators,
    ]);
    let mut output = passes[0](circuit)?;
    decode::stream_kinds::infer(&output)?;
    for pass in &passes[1..] {
        output = pass(&output)?;
        decode::stream_kinds::infer(&output)?;
    }
    Ok(output)
}

/// Expand the empty-input result of a global aggregate in a flat circuit.
pub fn expand_aggregate_zero(circuit: &Value) -> Result<Value> {
    decode::expand_aggregate_zero::rewrite(circuit)
}

/// Return the validated kinds in circuit order, visiting nested operators
/// before the region that contains them.
pub fn stream_kinds(circuit: &Value) -> Result<Value> {
    let result = decode::stream_kinds::infer(circuit)?
        .into_iter()
        .map(|ports| {
            ports
                .into_iter()
                .map(|kind| kind.map(decode::stream_kinds::Kind::name))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Ok(serde_json::json!(result))
}

/// Group bounded windows and assign shared field slots in first-use order.
pub fn group_windows(candidates: &Value) -> Result<Value> {
    window_groups::group(candidates)
}

/// Return the root join IDs that may maintain independent input traces.
pub fn select_balanced_joins(circuit: &Value) -> Result<Vec<u64>> {
    decode::balanced_joins::select(circuit)
}

/// Schedule aggregate waterline dependencies and select the fused retainers.
pub fn linear_retention_plan(input: &Value) -> Result<Value> {
    decode::linear_retention::plan(input)
}

/// Return consumers that should receive their own copy of a shared input.
pub fn select_fanout_clones(circuit: &Value) -> Result<Vec<u64>> {
    decode::clone_fanout::select(circuit)
}

/// Return the first unsupported recursive operator, if any.
pub fn recursive_violation(circuit: &Value) -> Result<Value> {
    recursion::violation(circuit)
}

/// Check for executable connector-metadata calls outside column defaults.
pub fn connector_metadata_violation(circuit: &Value) -> Result<Value> {
    decode::connector_metadata_violation(circuit)
}

/// Return the live declaration and source-column identities that must report
/// nondeterminism diagnostics.
pub fn nondeterministic_initializers(circuit: &Value) -> Result<Value> {
    decode::nondeterministic_initializers(circuit)
}

/// Return the original annotation addresses for each conflicting pair.
pub fn conflicting_join_hints(circuit: &Value) -> Result<Value> {
    decode::conflicting_join_hints(circuit)
}

/// Return the global aggregate region IDs that must be removed throughout the circuit.
pub fn conflicting_regions(circuit: &Value) -> Result<Value> {
    decode::conflicting_regions(circuit)
}

/// Return the first trace-retention error and the live operator identities it uses.
pub fn trace_retention_violation(input: &Value) -> Result<Value> {
    decode::trace_retention::violation(input)
}

/// Insert a validator after each unkeyed input when the scheduled SQL option is on.
pub fn insert_weight_validation(circuit: &Value) -> Result<Value> {
    decode::insert_weight_validation(circuit, &decode::circuit(circuit)?)
}

/// Replace filters with literal false by an empty stream and bypass literal
/// true filters. Other closure bodies stay intact.
pub fn remove_constant_filters(circuit: &Value) -> Result<Value> {
    let incremental = circuit["incremental"]
        .as_bool()
        .ok_or_else(|| Error("constant filter rewrite needs a circuit stream mode".into()))?;
    let decoded = decode::circuit(circuit)?;
    let edits = filters::remove_constants(&decoded, incremental)?;
    decode::rewrite_operators(circuit, &decoded, edits)
}

/// Insert stream distinct operators at multiset sinks in a pre-incremental
/// graph. For replay graphs, preserve the sink's final provenance and assign
/// the inserted operator the corresponding intermediate relation.
pub fn ensure_distinct_outputs(circuit: &Value) -> Result<Value> {
    if circuit["incremental"].as_bool() != Some(false) {
        return Err(Error(
            "output distinct requires a pre-incremental circuit".into(),
        ));
    }
    let decoded = decode::circuit(circuit)?;
    let facts = analysis::analyze(&decoded);
    let distinct = decoded.operators.iter().map(|operator| {
        matches!(operator.kind, model::OperatorKind::Sink) && {
            let port = operator.inputs[0];
            facts[port.operator.0][port.output]
        }
    });
    decode::insert_output_distinct(circuit, distinct)
}
