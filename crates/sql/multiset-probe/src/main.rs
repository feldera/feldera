use feldera_multiset_probe::{
    analyze, ensure_distinct_outputs, merge_sums, propagate_constants, propagate_empty_sources,
    remove_all_view_operators, remove_constant_filters, remove_dead_operators,
    remove_redundant_integrates, remove_unused_operators, remove_view_operators,
};
use feldera_sql_difftest::{compare_circuits, parse_json};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    env,
    ffi::OsStr,
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    process::ExitCode,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type Rewrite = fn(&Value) -> std::result::Result<Value, feldera_multiset_probe::Error>;

const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;

fn read_record(source: &mut BufReader<File>, line: &mut Vec<u8>) -> Result<Option<Value>> {
    read_frame(source, line, MAX_RECORD_BYTES)
}

fn read_frame(source: &mut impl BufRead, line: &mut Vec<u8>, limit: u64) -> Result<Option<Value>> {
    line.clear();
    let length = source.by_ref().take(limit + 1).read_until(b'\n', line)?;
    if length == 0 {
        return Ok(None);
    }
    if length as u64 > limit {
        return Err(format!("input record exceeds {limit} bytes").into());
    }
    Ok(Some(parse_json(line)?))
}

fn compare_tail(args: &[std::ffi::OsString]) -> Result<()> {
    let mut expected = BufReader::new(File::open(&args[0])?);
    let mut actual = BufReader::new(File::open(&args[1])?);
    let mut output = BufWriter::new(File::create_new(&args[2])?);
    let mut line = Vec::new();
    let mut ids = HashSet::new();
    loop {
        let a = read_record(&mut expected, &mut line)?;
        let b = read_record(&mut actual, &mut line)?;
        let (a, b) = match (a, b) {
            (None, None) => break,
            (Some(a), Some(b)) => (a, b),
            _ => return Err("tail record counts differ".into()),
        };
        let id = a["id"].as_str().ok_or("case id must be a string")?;
        if b["id"] != id || !ids.insert(id.to_owned()) {
            return Err("tail case identities differ or repeat".into());
        }
        serde_json::to_writer(&mut output, &tail_comparison(&a, &b)?)?;
        writeln!(output)?;
    }
    output.flush()?;
    Ok(())
}

fn tail_comparison(expected: &Value, actual: &Value) -> Result<Value> {
    if expected["stage"] != "S3" || !matches!(expected["mode"].as_str(), Some("classic" | "gen2")) {
        return Err("tail oracle needs an S3 stage and known engine mode".into());
    }
    for kind in [
        "inner-rust",
        "global-operator",
        "local-operator",
        "output-type",
    ] {
        if !expected["hash_inputs"][kind]
            .as_array()
            .is_some_and(|values| !values.is_empty() && values.iter().all(Value::is_string))
        {
            return Err("tail oracle lacks actual hash-input strings".into());
        }
    }
    if expected["mode"] == "classic"
        && !expected["rust"]
            .as_object()
            .is_some_and(|files| !files.is_empty())
    {
        return Err("classic tail oracle lacks Rust artifacts".into());
    }
    if expected["mode"] == "gen2"
        && !expected["dataflow_sha256"].as_str().is_some_and(|digest| {
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return Err("Gen-2 tail oracle lacks a dataflow fingerprint".into());
    }
    let mut differences = Vec::new();
    for field in ["mode", "stage", "schema", "diagnostics", "hash_inputs"] {
        if expected.get(field).is_none_or(Value::is_null)
            || expected.get(field) != actual.get(field)
        {
            differences.push(field);
        }
    }
    if expected.get("rust") != actual.get("rust") {
        differences.push("rust");
    }
    if expected.get("dataflow_sha256") != actual.get("dataflow_sha256") {
        differences.push("dataflow_sha256");
    }
    let graph = compare_circuits(&expected["circuit"], &actual["circuit"]);
    if graph.is_err() {
        differences.push("circuit");
    }
    Ok(
        json!({"id": expected["id"], "equal": differences.is_empty(),
        "differences": differences, "graph_difference": graph.err().map(|e| e.to_string())}),
    )
}

fn compare_scheduled_passes(args: &[std::ffi::OsString]) -> Result<()> {
    let mut source = BufReader::new(File::open(&args[0])?);
    let mut output = BufWriter::new(File::create_new(&args[1])?);
    let mut ids = HashSet::new();
    let mut line = Vec::new();
    while let Some(input) = read_record(&mut source, &mut line)? {
        let id = input["id"].as_str().ok_or("case id must be a string")?;
        if !ids.insert(id.to_owned()) {
            return Err("duplicate case id".into());
        }
        let rewrite = scheduled_rewrite(input["pass"].as_str())?;
        let rewritten = rewrite(&input["circuit"]).map_err(|error| format!("{id}: {error}"))?;
        let comparison = compare_circuits(&input["expected"], &rewritten);
        serde_json::to_writer(
            &mut output,
            &json!({
                "id": id,
                "pass": input["pass"],
                "equal": comparison.is_ok(),
                "difference": comparison.err().map(|error| error.to_string()),
                "rewritten": rewritten,
            }),
        )?;
        writeln!(output)?;
    }
    output.flush()?;
    Ok(())
}

fn compare_stream_kinds(source: &mut impl BufRead, output: &OsStr) -> Result<()> {
    let mut output = BufWriter::new(File::create_new(output)?);
    let mut line = Vec::new();
    let mut identities = HashSet::new();
    while let Some(input) = read_frame(source, &mut line, MAX_RECORD_BYTES)? {
        let id = input["id"].as_str().ok_or("case id must be a string")?;
        if !identities.insert(id.to_owned()) {
            return Err("duplicate stream-kind comparison id".into());
        }
        let result = match feldera_multiset_probe::stream_kinds(&input["circuit"]) {
            Ok(facts) => json!({"id": id, "equal": facts == input["expected"], "outputs": facts}),
            Err(error) => json!({"id": id, "equal": false, "error": error.to_string()}),
        };
        serde_json::to_writer(&mut output, &result)?;
        writeln!(output)?;
    }
    output.flush()?;
    Ok(())
}

fn scheduled_rewrite(name: Option<&str>) -> Result<Rewrite> {
    match name {
        Some("MergeSums") => Ok(merge_sums),
        Some("EnsureDistinctOutputs") => Ok(ensure_distinct_outputs),
        Some("PropagateEmptySources") => Ok(propagate_empty_sources),
        Some("PropagateConstants") => Ok(propagate_constants),
        Some("RemoveOperators") => Ok(remove_unused_operators),
        Some("RemoveOperatorsKeepSources") => Ok(remove_dead_operators),
        Some("RemoveViewOperators") => Ok(remove_view_operators),
        Some("RemoveAllViewOperators") => Ok(remove_all_view_operators),
        Some("RemoveIAfterD") => Ok(remove_redundant_integrates),
        Some("RemoveConstantFilters") => Ok(remove_constant_filters),
        Some("RemoveDeindexOperators") => Ok(feldera_multiset_probe::remove_deindex_operators),
        Some("RemoveNoops") => Ok(feldera_multiset_probe::remove_noops),
        Some("RemoveIdentityOperators") => Ok(feldera_multiset_probe::remove_identity_maps),
        Some("RemoveErrorTable") => Ok(feldera_multiset_probe::remove_error_table),
        Some("RemoveNowTable") => Ok(feldera_multiset_probe::remove_now_table),
        Some("CompactNames") => Ok(feldera_multiset_probe::compact_names),
        Some("InsertWeightValidation") => Ok(feldera_multiset_probe::insert_weight_validation),
        Some("NoIntegralVisitor") => Ok(feldera_multiset_probe::require_no_integrals),
        Some("PushDifferentialsUp") => Ok(feldera_multiset_probe::push_differentials),
        Some("ImplementJoins") => Ok(feldera_multiset_probe::implement_joins),
        Some("IncrementalizeVisitor") => Ok(feldera_multiset_probe::incrementalize),
        Some("OptimizeIncrementalVisitor") => Ok(feldera_multiset_probe::optimize_incremental),
        Some("SeparateIntegrators") => Ok(feldera_multiset_probe::separate_integrators),
        Some("OptimizeDistinctVisitor") => Ok(feldera_multiset_probe::optimize_distinct),
        Some("EarlySetOptimization") => Ok(feldera_multiset_probe::optimize_early_sets),
        Some("IncrementalStreamOptimization") => {
            Ok(|circuit| feldera_multiset_probe::optimize_streams(circuit, true))
        }
        Some("SnapshotStreamOptimization") => {
            Ok(|circuit| feldera_multiset_probe::optimize_streams(circuit, false))
        }
        Some("GroupWindows") => Ok(feldera_multiset_probe::group_windows),
        Some("ChainVisitor") => Ok(feldera_multiset_probe::fuse_chains),
        Some("InnerCSE") => Ok(feldera_multiset_probe::inner_cse),
        Some("EnsureExpressionTrees") => Ok(feldera_multiset_probe::ensure_expression_trees),
        Some("OptimizeProjections#1" | "OptimizeProjections#2" | "OptimizeProjections#3") => {
            Ok(feldera_multiset_probe::optimize_projections)
        }
        Some("Simplify#1" | "Simplify#2" | "Simplify#3" | "Simplify#4") => {
            Ok(feldera_multiset_probe::simplify)
        }
        Some("Gen2PassesEnabled") => Ok(feldera_multiset_probe::gen2_clones),
        Some("Gen2PassesDisabled") => Ok(|circuit| Ok(circuit.clone())),
        Some("CreateStarJoinsEnabled") => Ok(feldera_multiset_probe::analyze_star_joins),
        Some("CreateStarJoinsDisabled") => Ok(|circuit| Ok(circuit.clone())),
        Some("ImplementStaticsEnabled") => Ok(feldera_multiset_probe::implement_statics),
        Some("ImplementStaticsDisabled") => Ok(|circuit| Ok(circuit.clone())),
        Some("UnusedFields") => Ok(feldera_multiset_probe::analyze_unused_fields),
        Some("FindUnboundedState") => Ok(feldera_multiset_probe::find_unbounded_state),
        Some("CheckRetain") => Ok(feldera_multiset_probe::duplicate_retention),
        Some("MergeGC") => Ok(feldera_multiset_probe::merge_gc),
        Some("DeltaExpandOperators") => Ok(feldera_multiset_probe::expand_deltas),
        Some("RecursiveComponents") => Ok(|circuit| {
            feldera_multiset_probe::require_inactive_pass(circuit, "RecursiveComponents")
        }),
        Some("LeftJoinChains") => {
            Ok(|circuit| feldera_multiset_probe::require_inactive_pass(circuit, "LeftJoinChains"))
        }
        Some("RemoveUselessLeftJoins") => Ok(|circuit| {
            feldera_multiset_probe::require_inactive_pass(circuit, "RemoveUselessLeftJoins")
        }),
        Some("RemoveStarJoins") => {
            Ok(|circuit| feldera_multiset_probe::require_inactive_pass(circuit, "RemoveStarJoins"))
        }
        Some("ExpandHop") => {
            Ok(|circuit| feldera_multiset_probe::require_inactive_pass(circuit, "ExpandHop"))
        }
        Some("LowerAsof") => {
            Ok(|circuit| feldera_multiset_probe::require_inactive_pass(circuit, "LowerAsof"))
        }
        Some("ExpandAggregates") => {
            Ok(
                |circuit| {
                    feldera_multiset_probe::require_inactive_pass(circuit, "ExpandAggregates")
                },
            )
        }
        Some("UDFInliner") => {
            Ok(|circuit| feldera_multiset_probe::require_inactive_pass(circuit, "UDFInliner"))
        }
        Some("LineageDisabled") => {
            Ok(|circuit| feldera_multiset_probe::require_inactive_pass(circuit, "LineageDisabled"))
        }
        Some("FilterJoinVisitor") => Ok(|circuit| {
            feldera_multiset_probe::require_inactive_pass(circuit, "FilterJoinVisitor")
        }),
        Some("LowerCircuitClassic") => Ok(|circuit| {
            feldera_multiset_probe::require_inactive_pass(circuit, "LowerCircuitClassic")
        }),
        Some("LowerCircuitGen2") => {
            Ok(
                |circuit| {
                    feldera_multiset_probe::require_inactive_pass(circuit, "LowerCircuitGen2")
                },
            )
        }
        Some("InsertLimiters") => Ok(feldera_multiset_probe::insert_limiters),
        Some("Monotonicity") => Ok(feldera_multiset_probe::analyze_limiter_inputs),
        Some("ReencodeS3") => Ok(feldera_multiset_probe::reencode_s3),
        Some("CSE") => Ok(|circuit| feldera_multiset_probe::require_inactive_pass(circuit, "CSE")),
        Some("ShareIndexes") => {
            Ok(|circuit| feldera_multiset_probe::require_inactive_pass(circuit, "ShareIndexes"))
        }
        Some("ShareOutputIntegrators") => Ok(|circuit| {
            feldera_multiset_probe::require_inactive_pass(circuit, "ShareOutputIntegrators")
        }),
        Some("ComparatorDeclarations") => Ok(|circuit| {
            feldera_multiset_probe::require_inactive_pass(circuit, "ComparatorDeclarations")
        }),
        Some("FuseIntoArrayAgg") => {
            Ok(
                |circuit| {
                    feldera_multiset_probe::require_inactive_pass(circuit, "FuseIntoArrayAgg")
                },
            )
        }
        Some("OptimizeProjectionVisitor") => Ok(|circuit| {
            feldera_multiset_probe::require_inactive_pass(circuit, "OptimizeProjectionVisitor")
        }),
        Some("DecomposeExpensiveFilters") => Ok(|circuit| {
            feldera_multiset_probe::require_inactive_pass(circuit, "DecomposeExpensiveFilters")
        }),
        Some("FuseExpensiveMaps") => Ok(|circuit| {
            feldera_multiset_probe::require_inactive_pass(circuit, "FuseExpensiveMaps")
        }),
        Some("SimplifyConditionals") => Ok(|circuit| {
            feldera_multiset_probe::require_inactive_pass(circuit, "SimplifyConditionals")
        }),
        Some("SimplifyWaterline") => Ok(|circuit| {
            feldera_multiset_probe::require_inactive_pass(circuit, "SimplifyWaterline")
        }),
        Some("DeadCode") => Ok(feldera_multiset_probe::eliminate_dead_code),
        Some("ImplementNow") => Ok(feldera_multiset_probe::implement_now),
        Some("ExpandIndexedInputsClassic") => {
            Ok(|circuit| feldera_multiset_probe::expand_indexed_inputs(circuit, false))
        }
        Some("ExpandIndexedInputsGen2") => {
            Ok(|circuit| feldera_multiset_probe::expand_indexed_inputs(circuit, true))
        }
        Some("PullFilterVisitor") => Ok(feldera_multiset_probe::pull_filter),
        Some("CreateRuntimeErrorWrappers") => Ok(feldera_multiset_probe::wrap_runtime_cast_errors),
        Some("ExpandCasts") => Ok(feldera_multiset_probe::expand_casts),
        Some("CanonicalForm") => Ok(feldera_multiset_probe::canonical_form),
        Some("ImplementChains") => Ok(feldera_multiset_probe::collapse_chains),
        Some("AdjustSqlIndex") => Ok(feldera_multiset_probe::adjust_array_indexes),
        Some("ExpandAggregateZeros") => Ok(feldera_multiset_probe::expand_aggregate_zero),
        _ => Err("no Rust rewrite for scheduled pass".into()),
    }
}

fn read_circuit(path: &std::ffi::OsStr) -> Result<Value> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err("input circuit exceeds 64 MiB".into());
    }
    Ok(parse_json(&bytes)?)
}

fn write_json(path: &std::ffi::OsStr, value: &Value) -> Result<()> {
    let mut output = BufWriter::new(File::create_new(path)?);
    serde_json::to_writer(&mut output, value)?;
    output.flush()?;
    Ok(())
}

fn rewrite_file(pass: &str, input: &OsStr, output: &OsStr) -> Result<()> {
    let circuit = read_circuit(input)?;
    let value = match pass {
        "ConstantViews" => json!(feldera_multiset_probe::constant_view_sinks(&circuit)?),
        "ValidateRecursiveOperators" => feldera_multiset_probe::recursive_violation(&circuit)?,
        "NoConnectorMetadata" => feldera_multiset_probe::connector_metadata_violation(&circuit)?,
        "DeterministicFunctions" => {
            feldera_multiset_probe::nondeterministic_initializers(&circuit)?
        }
        "CheckHints" => feldera_multiset_probe::conflicting_join_hints(&circuit)?,
        "TagRegions" => feldera_multiset_probe::conflicting_regions(&circuit)?,
        "StopOnError" => json!(stop_on_error(&circuit)?),
        "StrayGC" => feldera_multiset_probe::trace_retention_violation(&circuit)?,
        "BalancedJoins" => json!(feldera_multiset_probe::select_balanced_joins(&circuit)?),
        "OrderErrorConsumers" => json!(feldera_multiset_probe::select_error_consumers(&circuit)?),
        "ValidateStreamKinds" => feldera_multiset_probe::stream_kinds(&circuit)?,
        "CloneOperatorsWithFanout" => {
            json!(feldera_multiset_probe::select_fanout_clones(&circuit)?)
        }
        "LinearPostprocessRetainKeys" => feldera_multiset_probe::linear_retention_plan(&circuit)?,
        "ValidateNoIntegrals" => json!(feldera_multiset_probe::first_integral(&circuit)?),
        _ => scheduled_rewrite(Some(pass))?(&circuit)?,
    };
    write_json(output, &value)
}

fn stop_on_error(status: &Value) -> Result<bool> {
    let exit_code = status["exit_code"]
        .as_i64()
        .and_then(|code| i32::try_from(code).ok())
        .filter(|_| status.as_object().is_some_and(|fields| fields.len() == 1))
        .ok_or("optimizer status must contain a signed 32-bit exit_code")?;
    Ok(exit_code != 0)
}

/// The file protocol keeps the same circuit bytes and evidence as one-shot
/// replay. Stdio carries only the request and an acknowledgement after flush.
fn serve_rewrites(source: &mut impl BufRead, output: &mut impl Write) -> Result<()> {
    let mut line = Vec::new();
    while let Some(request) = read_frame(source, &mut line, 64 * 1024)? {
        if line.last() != Some(&b'\n') {
            return Err("unterminated rewrite request".into());
        }
        let pass = request["pass"]
            .as_str()
            .ok_or("rewrite request needs a pass name")?;
        let input = request["input"]
            .as_str()
            .ok_or("rewrite request needs an input path")?;
        let target = request["output"]
            .as_str()
            .ok_or("rewrite request needs an output path")?;
        rewrite_file(pass, OsStr::new(input), OsStr::new(target))
            .map_err(|error| format!("{pass}: {error}"))?;
        output.write_all(b"\n")?;
        output.flush()?;
    }
    Ok(())
}

fn record_s2_rewrite(
    input: &Value,
    result: &mut Value,
    field: &str,
    rewrite: Rewrite,
) -> Result<()> {
    let Some(expected) = input.get(field) else {
        return Ok(());
    };
    let mut seam = input["seam"].clone();
    if seam["stage"] != "S2-subset" || seam["schema_version"] != 4 {
        return Err(format!("{field} requires a version-4 S2 file").into());
    }
    seam["circuit"] = rewrite(&seam["circuit"])?;
    let comparison = compare_circuits(&expected["circuit"], &seam["circuit"]);
    let envelope_equal = expected.as_object().is_some_and(|fields| {
        fields.len() == seam.as_object().expect("validated envelope").len()
            && fields
                .iter()
                .all(|(key, value)| key == "circuit" || seam.get(key) == Some(value))
    });
    let difference = if envelope_equal {
        comparison.err().map(|error| error.to_string())
    } else {
        Some("replay envelope differs outside the circuit".into())
    };
    result[format!("{field}_equal")] = difference.is_none().into();
    result[format!("{field}_difference")] = json!(difference);
    result[field] = seam;
    Ok(())
}

fn run() -> Result<()> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() == 1 && args[0] == "--serve-rewrites" {
        return serve_rewrites(&mut io::stdin().lock(), &mut io::stdout().lock());
    }
    if args.len() == 4 && args[0] == "--compare-tail" {
        return compare_tail(&args[1..]);
    }
    if args.len() == 3 && args[0] == "--compare-pass" {
        return compare_scheduled_passes(&args[1..]);
    }
    if args.len() == 2 && args[0] == "--compare-stream-kinds" {
        return compare_stream_kinds(&mut io::stdin().lock(), &args[1]);
    }
    if args.len() == 4 && args[0] == "--rewrite-pass" {
        let pass = args[1].to_str().ok_or("pass name must be UTF-8")?;
        return rewrite_file(pass, &args[2], &args[3]);
    }
    if args.len() == 3 && args[0] == "--constant-views" {
        return rewrite_file("ConstantViews", &args[1], &args[2]);
    }
    if args.len() == 3 && args[0] == "--validate-recursion" {
        return rewrite_file("ValidateRecursiveOperators", &args[1], &args[2]);
    }
    if args.len() == 3 && args[0] == "--validate-connector-metadata" {
        return rewrite_file("NoConnectorMetadata", &args[1], &args[2]);
    }
    if args.len() != 2 {
        return Err(concat!(
            "Usage: feldera-multiset-probe INPUT.jsonl OUTPUT.jsonl\n",
            "       feldera-multiset-probe --compare-tail EXPECTED.jsonl ACTUAL.jsonl REPORT.jsonl\n",
            "       feldera-multiset-probe --compare-pass INPUT.jsonl REPORT.jsonl\n",
            "       feldera-multiset-probe --compare-stream-kinds REPORT.jsonl < INPUT.jsonl\n",
            "       feldera-multiset-probe --rewrite-pass NAME INPUT.json OUTPUT.json\n",
            "       feldera-multiset-probe --constant-views INPUT.json OUTPUT.json\n",
            "       feldera-multiset-probe --validate-recursion INPUT.json OUTPUT.json\n",
            "       feldera-multiset-probe --validate-connector-metadata INPUT.json OUTPUT.json\n",
            "       feldera-multiset-probe --serve-rewrites"
        ).into());
    }
    let mut source = BufReader::new(File::open(&args[0])?);
    let mut output = BufWriter::new(File::create_new(&args[1])?);
    let mut ids = HashSet::new();
    let mut line = Vec::new();
    while let Some(input) = read_record(&mut source, &mut line)? {
        let id = input["id"].as_str().ok_or("case id must be a string")?;
        if !ids.insert(id.to_owned()) {
            return Err("duplicate case id".into());
        }
        let facts = analyze(&input["circuit"]).map_err(|error| format!("{id}: {error}"))?;
        let pruned =
            remove_dead_operators(&input["circuit"]).map_err(|error| format!("{id}: {error}"))?;
        let comparison = compare_circuits(&input["dead_code"], &pruned);
        let mut result = json!({
            "id": id, "multisets": facts, "dead_code": pruned,
            "dead_code_equal": comparison.is_ok(),
            "dead_code_difference": comparison.err().map(|e| e.to_string()),
        });
        if let Some(expected) = input.get("remove_views") {
            let rewritten = remove_view_operators(&input["circuit"])
                .map_err(|error| format!("{id}: {error}"))?;
            let comparison = compare_circuits(expected, &rewritten);
            result["remove_views_equal"] = comparison.is_ok().into();
            result["remove_views_difference"] = json!(comparison.err().map(|e| e.to_string()));
        }
        if let Some(expected) = input.get("merge_sums") {
            let rewritten =
                merge_sums(&input["circuit"]).map_err(|error| format!("{id}: {error}"))?;
            let comparison = compare_circuits(expected, &rewritten);
            result["merge_sums"] = rewritten;
            result["merge_sums_equal"] = comparison.is_ok().into();
            result["merge_sums_difference"] = json!(comparison.err().map(|e| e.to_string()));
        }
        if let Some(expected) = input.get("ensure_distinct_outputs") {
            let rewritten = ensure_distinct_outputs(&input["circuit"])
                .map_err(|error| format!("{id}: {error}"))?;
            let comparison = compare_circuits(expected, &rewritten);
            result["ensure_distinct_outputs_equal"] = comparison.is_ok().into();
            result["ensure_distinct_outputs_difference"] =
                json!(comparison.err().map(|e| e.to_string()));
        }
        if let Some(seam) = input.get("seam") {
            if !matches!(seam["stage"].as_str(), Some("S2-subset" | "S3-subset"))
                || seam["schema_version"] != 4
            {
                return Err("expected version-4 S2 or S3 subset file".into());
            }
            let rewritten = remove_dead_operators(&seam["circuit"])?;
            let mut seam = seam.clone();
            seam["circuit"] = rewritten;
            result["seam"] = seam;
        }
        record_s2_rewrite(
            &input,
            &mut result,
            "distinct_seam",
            ensure_distinct_outputs,
        )
        .map_err(|error| format!("{id}: {error}"))?;
        record_s2_rewrite(&input, &mut result, "sum_seam", merge_sums)
            .map_err(|error| format!("{id}: {error}"))?;
        serde_json::to_writer(&mut output, &result)?;
        writeln!(output)?;
    }
    output.flush()?;
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optimizer_stop_decision_reads_only_a_valid_compiler_status() {
        assert!(!stop_on_error(&json!({"exit_code": 0})).unwrap());
        for exit_code in [1, 2, -1, i32::MAX] {
            assert!(stop_on_error(&json!({"exit_code": exit_code})).unwrap());
        }
        for invalid in [
            json!({}),
            json!({"exit_code": true}),
            json!({"exit_code": 0, "error_count": 1}),
            json!({"exit_code": i64::from(i32::MAX) + 1}),
        ] {
            assert!(stop_on_error(&invalid).is_err());
        }
    }

    #[test]
    fn worker_rejects_incomplete_and_oversized_requests_without_acknowledging() {
        let mut input = b"{}".as_slice();
        let mut response = Vec::new();
        let error = serve_rewrites(&mut input, &mut response).unwrap_err();
        assert_eq!(error.to_string(), "unterminated rewrite request");
        assert!(response.is_empty());

        let huge = vec![b' '; 64 * 1024 + 1];
        let error = serve_rewrites(&mut huge.as_slice(), &mut response).unwrap_err();
        assert!(error.to_string().contains("exceeds 65536 bytes"));
        assert!(response.is_empty());
    }

    fn oracle() -> Value {
        json!({"id": "classic/S3/test", "mode": "classic", "stage": "S3",
        "schema": {}, "diagnostics": [], "rust": {"circuit.rs": "fingerprint"},
        "hash_inputs": {"inner-rust": ["inner"], "global-operator": ["global"],
            "local-operator": ["local"], "output-type": ["type"]},
        "circuit": {"class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
            {"class": "DBSPSourceMultisetOperator", "id": 1, "inputs": [],
                "annotations": [{"class": "OperatorHash", "hash": "persistent-id"}]}
        ]}})
    }

    #[test]
    fn tail_comparison_checks_preimages_ids_and_artifacts_independently() {
        let expected = oracle();
        assert_eq!(
            tail_comparison(&expected, &expected).unwrap()["equal"],
            true
        );
        let mut actual = expected.clone();
        actual["hash_inputs"]["inner-rust"][0] = json!("other");
        assert_eq!(
            tail_comparison(&expected, &actual).unwrap()["differences"],
            json!(["hash_inputs"])
        );
        actual = expected.clone();
        actual["circuit"]["allOperators"][0]["annotations"][0]["hash"] = json!("wrong-id");
        assert_eq!(
            tail_comparison(&expected, &actual).unwrap()["differences"],
            json!(["circuit"])
        );
        actual = expected.clone();
        actual["rust"]["circuit.rs"] = json!("other");
        assert_eq!(
            tail_comparison(&expected, &actual).unwrap()["differences"],
            json!(["rust"])
        );
    }

    #[test]
    fn equal_but_missing_evidence_cannot_pass() {
        let mut expected = oracle();
        expected["hash_inputs"] = json!({});
        assert!(tail_comparison(&expected, &expected).is_err());
        expected = oracle();
        expected.as_object_mut().unwrap().remove("rust");
        assert!(tail_comparison(&expected, &expected).is_err());
        expected["mode"] = json!("gen2");
        assert!(tail_comparison(&expected, &expected).is_err());
        expected["dataflow_sha256"] = "ab".repeat(32).into();
        let mut actual = expected.clone();
        actual["dataflow_sha256"] = "cd".repeat(32).into();
        assert_eq!(
            tail_comparison(&expected, &actual).unwrap()["differences"],
            json!(["dataflow_sha256"])
        );
    }
}
