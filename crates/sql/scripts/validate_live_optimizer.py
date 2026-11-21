#!/usr/bin/env python3
"""Compare a live Rust-pass S2 replay with independently compiled Java SQL artifacts."""
import argparse
from collections import Counter
import gzip
import json
from pathlib import Path
import re

from run_determinism import file_digest
from validate_corpus_tail import records


def check_artifacts(row):
    if "gen2" in row:
        files = row["gen2"]
        complete = set(files) == {"circuit.json", "dataflow.json"}
    else:
        files = row["rust"]
        single = {"circuit.rs", "stubs.rs"} <= files.keys()
        workspace = ("Cargo.toml" in files and any(f.endswith("/src/lib.rs") for f in files)
                     and any(f.endswith("/src/stubs.rs") for f in files))
        complete = "dataflow.json" in files and (single or workspace)
    if not complete or any(not isinstance(digest, str) or not re.fullmatch("[0-9a-f]{64}", digest)
                           for digest in files.values()):
        raise ValueError(f"Incomplete engine artifacts for {row['id']}")


def read_run(directory, entry):
    manifest = json.loads((directory / "complete.json").read_bytes())
    if (manifest["failed_cases"] or manifest["entry_point"] != entry
            or manifest.get("initial_generated_name") != 0 or manifest.get("dataflow_contract") != "all-engines-v1"
            or not manifest["capture"] or manifest["observation_contract"] != "final-s3-v1"
            or manifest["artifact_contract"] != "seam-v1"):
        raise ValueError(f"Incomplete {entry} comparison in {directory}")
    path = directory / manifest["records_file"]
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as stream:
        count = 0
        for line in stream:
            row = json.loads(line)
            if ("failure" in row or row["observations"]["S3"]["count"] != 1
                    or not row["exported_schema_equal"] or not row["exported_diagnostics_equal"]
                    or not {"inner-rust", "output-type", "local-operator", "global-operator"} <= row["hash_inputs"].keys()):
                raise ValueError(f"Missing compiler evidence for {row['id']} in {directory}")
            check_artifacts(row)
            yield row
            count += 1
    if count != manifest["cases"] or not count:
        raise ValueError(f"Invalid case count in {directory}")


# Counts for one uninterrupted top-level optimizer sequence in the frozen replay.
# The case options, rather than the population label, determine the Gen-2 placement.
FROZEN_SCHEDULE = {
    "TrimWindows": 1, "ReplaceCommonProjections": 1, "FindCommonProjections": 1, "FindUnusedInputFields": 1, "ExpandStreamAggregates": 1, "IndexedInputs": 1, "OptimizeProjectionVisitor": 1, "SimplifyConditionals": 1, "PullFilterVisitor": 1, "PropagateConstants": 1, "Simplify": 4, "ImplementChains": 1,
    "CheckPersistentIds": 1, "DeltaHashPrefixes": 1, "GcHashPrefixes": 1, "SetOperatorHashes": 2,
    "Graph": 13, "Fuse": 1, "RemoveStarJoins": 0, "FindJoinTrees": 1, "CanonicalForm": 1, "ReplaceStarJoins": 1, "MoveGC": 0, "OrderGcDependencies": 0, "MergeRetain": 1, "MergeNoops": 1, "FindMultipleRetains": 1, "FindEquivalentNoops": 1,
    "DetectStreamingOperations": 1, "FindGCedStreams": 1, "CollectUnbounded": 1, "FindBounded": 2, "EnsureExpressionTrees": 1, "MultisetAnalysis": 3, "OptimizeDistinctVisitor": 1,
    "AppendOnly": 1, "DecomposeExpensiveFilters": 1, "SimplifyWaterline": 1, "ContainsNow": 1,
    "OrderErrorConsumers": 1, "UDFInliner": 1, "ShareOutputIntegrators": 1,
    "CollapseSharedChains": 1, "FindWindowGroups": 1, "ImplementWindowGroups": 1,
    "RemoveUselessLeftJoinsVisitor": 1,
    "SubstituteLeftJoins": 1, "FindMapChains": 1, "DuplicateSharedIndexes": 1,
    "LinearPostprocessRetainKeys": 1, "FixupViewReferences": 1, "BuildNestedOperators": 1, "ChainVisitor": 2, "StrayGC": 1, "CheckRetain": 1,
    "ExpandHop": 1, "RemoveIdentityOperators": 1,
    "CreateRuntimeErrorWrappers": 1, "ExpandMetadataCasts": 1, "AdjustSqlIndex": 1, "CircuitStatistics": 1,
    "BalancedJoins": 1, "OptimizeIncrementalVisitor": 2, "CloneOperatorsWithFanout": 1, "ConstantViews": 1, "NoConnectorMetadata": 1, "DeterministicFunctions": 1,
    "CSE": 4, "CheckHints": 1, "CompactNames": 1, "ComparatorDeclarations": 1, "DeadCode": 11,
    "PropagateEmptySources": 3, "RemoveConstantFilters": 1, "PushDifferentialsUp": 1, "ImplementJoins": 1, "RemoveTable": 1, "ExpandAggregateZero": 1,
    "FuseIntoArrayAgg": 2, "MergeSums": 1, "RemoveDeindexOperators": 1, "RemoveIAfterD": 1,
    "RemoveNoops": 2, "RemoveViewOperators": 2, "TagRegions": 1, "ValidateRecursiveOperators": 2,
}


# Their Java iterations invoke Rust for cleanup. Fully ported graph passes prune internally.
GRAPH_CLEANUP_PASSES = (
    "OptimizeProjections", "OptimizeProjectionVisitor", "PullFilterVisitor",
    "FilterJoinVisitor", "TrimFilters", "TrimWindows",
)


def graph_cleanup_counts(case, record):
    counts = record.get("dead_code_by_container")
    if not isinstance(counts, dict) or not all(
            type(counts.get(name)) is int and counts[name] >= 1 for name in GRAPH_CLEANUP_PASSES):
        raise ValueError(f"{case}: missing graph cleanup placement")
    return {name: counts[name] for name in GRAPH_CLEANUP_PASSES}


def verify_placements(case, counts, gen2, incremental=False, schedule=FROZEN_SCHEDULE, *,
                      validate_weights=False, cast_iterations=None, unused_field_iterations=None,
                      graph_cleanups=None, clock_scans=None, temporal_splits=None, now_stream=None,
                      outputs_are_sets=False, trim_inputs=False, asof_retention_moves=0, star_joins_enabled=True):
    expected = dict(schedule)
    if "RemoveStarJoins" in expected:
        if star_joins_enabled:
            del expected["RemoveStarJoins"]
        else:
            expected["RemoveStarJoins"] = 1
    if "ReplaceStarJoins" in expected and not star_joins_enabled:
        expected.pop("FindJoinTrees", None)
        del expected["ReplaceStarJoins"]
        expected["Graph"] -= 1
    if "MoveGC" in schedule:
        if type(asof_retention_moves) is not int or asof_retention_moves not in (0, 1):
            raise ValueError(f"{case}: invalid ASOF retention placement {asof_retention_moves}")
        for name in ("MoveGC", "OrderGcDependencies"):
            if name in expected:
                if asof_retention_moves:
                    expected[name] = 1
                else:
                    del expected[name]
        expected["Graph"] += asof_retention_moves
    if trim_inputs:
        expected["TrimInputs"] = 1
    if outputs_are_sets:
        expected["EnsureDistinctOutputs"] = 1
        expected["MultisetAnalysis"] += 1
    if clock_scans is not None:
        if type(clock_scans) is not int or clock_scans not in (1, 2):
            raise ValueError(f"{case}: invalid number of clock scans {clock_scans}")
        expected["ContainsNow"] = clock_scans
        if clock_scans == 2:
            expected["DesugarNowEquality"] = 1
        if now_stream is not None and (clock_scans == 1 or not now_stream):
            expected["RemoveTable"] += 1
    if temporal_splits is not None:
        if type(temporal_splits) is not int or temporal_splits < 0 or bool(temporal_splits) != (clock_scans == 2):
            raise ValueError(f"{case}: invalid temporal filter iteration count {temporal_splits}")
        if temporal_splits:
            expected["BreakFilters"] = temporal_splits
    if cast_iterations is not None:
        if (set(cast_iterations) != {"ExpandSafeCasts", "ExpandUnsafeCasts"}
                or any(type(n) is not int or n < 1 for n in cast_iterations.values())):
            raise ValueError(f"{case}: invalid Java cast iterations {cast_iterations}")
        expected.update(cast_iterations)
    if unused_field_iterations is not None:
        if type(unused_field_iterations) is not int or unused_field_iterations < 1:
            raise ValueError(f"{case}: invalid Java unused-field iterations {unused_field_iterations}")
        # ExpandIndexedInputs also runs both shared-projection passes once after the loop.
        for name in ("FindCommonProjections", "ReplaceCommonProjections"):
            if name in expected:
                expected[name] += unused_field_iterations
        expected["CSE"] += unused_field_iterations
        expected["DeadCode"] += 2 * unused_field_iterations
        if "Graph" in expected:
            expected["Graph"] += 2 * unused_field_iterations
    if graph_cleanups is not None:
        expected["DeadCode"] += sum(graph_cleanups.values())
        if "Graph" in expected:
            expected["Graph"] += sum(graph_cleanups.values())
        expected["FilterJoinVisitor"] = graph_cleanups["FilterJoinVisitor"]
        for name in ("PullFilterVisitor", "OptimizeProjectionVisitor", "TrimWindows"):
            if name in expected:
                expected[name] = graph_cleanups[name]
    if incremental:
        if "Graph" in expected:
            expected["Graph"] += 1
        expected.update(IncrementalizeVisitor=1, NoIntegralVisitor=1, SeparateIntegrators=1)
    if gen2:
        expected["Gen2Passes"] = 1
    if validate_weights:
        expected["InsertWeightValidation"] = 1
    if counts != expected or any(type(n) is not int for n in counts.values()):
        raise ValueError(f"{case}: Rust placements {counts} differ from {expected}")


def compare(population, references, candidates, schedule=FROZEN_SCHEDULE):
    expected = {}
    for row in records(population):
        key = row["id"]
        if key in expected:
            raise ValueError(f"Duplicate frozen case {key}")
        expected[key] = (row["input_sha256"], row["options"]["ioOptions"]["gen2"],
                         row["options"]["languageOptions"]["incrementalize"], row["options"]["ioOptions"]["nowStream"],
                         row["options"]["languageOptions"]["outputsAreSets"], row["options"]["ioOptions"]["trimInputs"])
    controls = {}
    for reference in references:
        for row in read_run(reference, "SQL"):
            key = row["id"]
            if (key not in expected or key in controls or row["input_sha256"] != expected[key][0]
                    or row["rust_passes"]):
                raise ValueError(f"Wrong direct-Java control {key}")
            controls[key] = row
    if controls.keys() != expected.keys():
        raise ValueError("Java reference does not cover the frozen population")
    invocations = Counter()
    for directory in candidates:
        for row in read_run(directory, "S2"):
            key = row["id"]
            control = controls.pop(key, None)
            if control is None:
                raise ValueError(f"Unexpected or repeated candidate {key}")
            for field in ("input_sha256", "schema", "diagnostics", "observations", "hash_inputs",
                          "gen2" if expected[key][1] else "rust"):
                if row[field] != control[field]:
                    raise ValueError(f"{key}: {field} differs from direct Java")
            counts = row["rust_passes"]
            weights = control["validate_input_weights"]
            if type(weights) is not bool or row["validate_input_weights"] is not weights:
                raise ValueError(f"{key}: positive weight validation policy differs from direct Java")
            if not isinstance(control.get("cast_iterations"), dict) or control["cast_iterations"] != row.get("cast_iterations"):
                raise ValueError(f"{key}: candidate and Java cast iterations differ")
            unused_fields = control.get("unused_field_iterations")
            if type(unused_fields) is not int or unused_fields != row.get("unused_field_iterations"):
                raise ValueError(f"{key}: candidate and Java unused-field iterations differ")
            graph_cleanups = graph_cleanup_counts(key, control)
            if graph_cleanups != graph_cleanup_counts(key, row):
                raise ValueError(f"{key}: graph cleanup iterations differ from Java")
            clock_scans = control.get("clock_scans")
            if "ContainsNow" in schedule and (clock_scans is None or clock_scans != row.get("clock_scans")):
                raise ValueError(f"{key}: candidate and Java clock scans differ")
            splits = control.get("temporal_filter_iterations")
            if "ContainsNow" in schedule and (splits is None or splits != row.get("temporal_filter_iterations")):
                raise ValueError(f"{key}: candidate and Java temporal filter iterations differ")
            star_joins = control.get("star_joins_enabled")
            if "ReplaceStarJoins" in schedule and (type(star_joins) is not bool
                    or row.get("star_joins_enabled") is not star_joins):
                raise ValueError(f"{key}: star join policy differs from direct Java")
            asof_moves = control.get("asof_retention_moves")
            if "MoveGC" in schedule and (asof_moves is None or asof_moves != row.get("asof_retention_moves")):
                raise ValueError(f"{key}: candidate and Java ASOF retention placements differ")
            verify_placements(key, counts, expected[key][1], expected[key][2], schedule,
                              validate_weights=weights, cast_iterations=control["cast_iterations"],
                              unused_field_iterations=unused_fields, graph_cleanups=graph_cleanups,
                              clock_scans=clock_scans, temporal_splits=splits, now_stream=expected[key][3],
                              outputs_are_sets=expected[key][4], trim_inputs=expected[key][5],
                              asof_retention_moves=asof_moves, star_joins_enabled=star_joins)
            invocations.update(counts)
    if controls:
        raise ValueError(f"{len(controls)} cases missing from S2 replay")
    return {"cases": len(expected), "mismatches": 0, "population_sha256": file_digest(population),
            "rust_pass_invocations": dict(sorted(invocations.items())),
            "shards": [{"manifest_sha256": file_digest(d / "complete.json"),
                        "records_sha256": file_digest(d / json.loads((d / "complete.json").read_bytes())["records_file"])}
                       for d in [*references, *candidates]]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("population", type=Path)
    parser.add_argument("--reference", type=Path, required=True, action="append", help="direct Java SQL shard")
    parser.add_argument("candidates", nargs="+", type=Path)
    args = parser.parse_args()
    print(json.dumps(compare(args.population, args.reference, args.candidates), indent=2))
