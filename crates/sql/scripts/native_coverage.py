#!/usr/bin/env python3
"""Join the frozen native inventory to explicitly scoped implementation evidence.

Reconstruction, a DBSP API, or a type-compatible microtest never credits corpus C7.
The ledger preserves every program, type and explicit index key in the input.
"""
import argparse
import hashlib
import json
from pathlib import Path

RUNTIME_STARTS = {
    "SourceMultiset": "crates/dbsp/src/operator/dynamic/input.rs",
    "Sink": "crates/dbsp/src/operator/output.rs",
    "Map": "crates/dbsp/src/operator/dynamic/filter_map.rs",
    "MapIndex": "crates/dbsp/src/operator/dynamic/filter_map.rs",
    "FlatMap": "crates/dbsp/src/operator/dynamic/filter_map.rs",
    "FlatMapIndex": "crates/dbsp/src/operator/dynamic/filter_map.rs",
    "Filter": "crates/dbsp/src/operator/dynamic/filter_map.rs",
    "Join": "crates/dbsp/src/operator/dynamic/join.rs",
    "StreamJoin": "crates/dbsp/src/operator/dynamic/join.rs",
    "AntiJoin": "crates/dbsp/src/operator/dynamic/join.rs",
    "LeftJoin": "crates/dbsp/src/operator/dynamic/outer_join.rs",
    "AggregateLinearPostprocess": "crates/dbsp/src/operator/dynamic/aggregate.rs",
    "PartitionedRollingAggregate": "crates/dbsp/src/operator/dynamic/time_series/rolling_aggregate.rs",
    "Constant": "crates/dbsp/src/operator/generator.rs",
    "IncrementalConstant": "crates/dbsp/src/operator/generator.rs",
    "Sum": "crates/dbsp/src/operator/sum.rs",
    "Negate": "crates/dbsp/src/operator/neg.rs",
    "AtomicSum": "crates/dbsp/src/operator/dynamic/concat.rs",
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verified_executions(programs, expected, certificate):
    if certificate is None:
        return {}
    if type(certificate.get("schema_version")) is not int or certificate["schema_version"] != 1:
        raise ValueError("Unknown native execution evidence version")
    step_counts = {program["id"]: len(program["steps"]) for program in programs}
    verified = {}
    for record in certificate["programs"]:
        case = record["id"]
        if (case not in expected or case in verified
                or record["case_sha256"] != expected[case]["sha256"]
                or type(record["verified_steps"]) is not int
                or record["verified_steps"] < 1
                or record["verified_steps"] != step_counts[case]
                or record["compiled_and_loaded"] is not True
                or record["all_outputs_equal"] is not True
                or record["runtime"] != "dbsp"
                or not isinstance(record["evidence"], str) or not record["evidence"]):
            raise ValueError(f"Incomplete or mismatched native execution evidence for {case}")
        verified[case] = record
    return verified


def verify_connector(fixture, executions, certificate):
    connector = {**fixture, "connector_executed": False}
    if certificate is None:
        return connector
    if (type(certificate.get("schema_version")) is not int
            or certificate["schema_version"] != 1):
        raise ValueError("Unknown native connector evidence version")
    for key in ("case_id", "step", "input", "payload_sha256", "format", "records"):
        if (key not in fixture or key not in certificate
                or type(fixture[key]) is not type(certificate[key])
                or fixture[key] != certificate[key]):
            raise ValueError(f"Connector evidence differs from frozen fixture for {key}")
    if (fixture.get("case_id") not in executions
            or certificate.get("production_parser_and_encoder") is not True
            or certificate.get("decoded_inputs_equal") is not True
            or certificate.get("weighted_outputs_equal") is not True
            or certificate.get("errors_empty") is not True):
        raise ValueError("Incomplete connector execution evidence")
    classic = certificate.get("classic_output_sha256")
    if (not isinstance(classic, str) or len(classic) != 64
            or any(ch not in "0123456789abcdef" for ch in classic)
            or classic != certificate.get("native_output_sha256")):
        raise ValueError("Connector output bytes differ or have no digest")
    if not isinstance(certificate.get("archive_index"), str) or not certificate["archive_index"]:
        raise ValueError("Missing connector archive index")
    return {**connector, "connector_executed": True, "execution_evidence": certificate}


def ledger(inventory, selection, archive_certificate, execution_certificate=None,
           connector_certificate=None):
    programs = inventory["programs"]
    expected = {p["id"]: p for p in selection["programs"]}
    if len(expected) != 10 or len(programs) != 10 or {p["id"] for p in programs} != set(expected):
        raise ValueError("The complete frozen ten-program selection is required")
    types = {t["id"]: t for t in inventory["types"]}
    if len(types) != len(inventory["types"]):
        raise ValueError("Duplicate type identity")
    known_archives = {t["type_id"] for t in archive_certificate["selected_types"]}
    known_keys = {item["type_id"]: item for item in archive_certificate.get("normalized_keys", [])}
    if not known_archives <= types.keys() or not known_keys.keys() <= types.keys():
        raise ValueError("Component evidence references a missing type")
    executions = verified_executions(programs, expected, execution_certificate)
    result = []
    for program in programs:
        identity = program["id"]
        execution = executions.get(identity)
        used = {key: t for key, t in types.items() if any(u["case"] == identity for u in t["uses"])}
        keys = sorted(key for key, t in used.items() if any(u["case"] == identity and u["role"] == "index_key" for u in t["uses"]))
        ports = {x["row_type"] for step in program["steps"] for kind in ["inputs", "outputs"] for x in step[kind]}
        if not ports <= types.keys():
            raise ValueError("Port references a missing type")
        operators = []
        for operator in program["operators"]:
            name = operator["class"].removeprefix("DBSP").removesuffix("Operator")
            if name not in RUNTIME_STARTS:
                raise ValueError("Unclassified operator: " + name)
            if operator["output_type"] not in types:
                raise ValueError("Operator references a missing type")
            operators.append({
                "id": operator["id"], "class": operator["class"], "path": operator["path"],
                "runtime_start": RUNTIME_STARTS[name],
                "native_ir_builder": "verified_for_exact_case" if execution else "missing",
            })
        storage_roles = {"zset_row", "index_key", "index_value"}
        stored = ports | {
            key for key, item in used.items()
            if any(use["case"] == identity and use["role"] in storage_roles
                   for use in item["uses"])
        }
        result.append({
            "id": identity,
            "case_sha256": expected[identity]["sha256"],
            "features": expected[identity]["features"],
            "step_semantics": program["step_semantics"],
            "steps": program["steps"],
            "native_C7": "verified_on_dbsp" if execution else "not_executed",
            "execution_evidence": execution,
            "operators": operators,
            "function_path_segments": program["function_path_segments"],
            "native_expression_lowering": "verified_for_exact_case" if execution else "missing_for_this_IR",
            "explicit_type_ids": sorted(used),
            "port_row_type_ids": sorted(ports),
            "archive_component_verified_type_ids": sorted(known_archives & used.keys()),
            "explicit_stored_type_ids": sorted(stored),
            "stored_types_without_existing_archive_certificate": sorted(stored - known_archives),
            "type_classification": {
                key: {"class": item["shape"]["class"],
                      "role": "stored_row_or_key" if key in stored
                      else "referenced_scalar_or_compiler_type"}
                for key, item in used.items()
            },
            "implicit_state_factory_inventory": "incomplete",
            "normalized_index_keys": [
                {
                    "type_id": key,
                    "shape": types[key]["shape"],
                    "semantic_checks": known_keys.get(key, {}).get(
                        "semantic_checks", "not_credited_by_inventory"
                    ),
                    "sort_merge_measurement": known_keys.get(key, {}).get(
                        "sort_merge_measurement", "missing_for_this_key_type"
                    ),
                }
                for key in keys
            ],
        })
    return {
        "schema_version": 1,
        "scope": "Frozen case executions, connector replay and archive component evidence",
        "programs": result,
        "types": inventory["types"],
        "connector": verify_connector(inventory["connector"], executions, connector_certificate),
        "native_C7_programs": len(executions),
        "native_C7_steps": sum(record["verified_steps"] for record in executions.values()),
        "required_programs": 10,
        "required_steps": sum(len(program["steps"]) for program in programs),
        "explicit_type_shapes": len(types),
        "implicit_state_layouts_complete": False,
        "N1_complete": False,
        "N2_complete": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    parser.add_argument("selection", type=Path)
    parser.add_argument("archive_certificate", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--execution-evidence", type=Path)
    parser.add_argument("--connector-evidence", type=Path)
    args = parser.parse_args()
    inventory = json.loads(args.inventory.read_text())
    if inventory["selection_sha256"] != digest(args.selection):
        raise ValueError("Frozen selection hash differs")
    artifacts = [args.inventory, args.selection, args.archive_certificate]
    evidence = []
    for path in (args.execution_evidence, args.connector_evidence):
        if path is not None:
            artifacts.append(path)
        evidence.append(json.loads(path.read_text()) if path is not None else None)
    result = ledger(inventory, json.loads(args.selection.read_text()),
                    json.loads(args.archive_certificate.read_text()), *evidence)
    result["inputs"] = {str(p): digest(p) for p in artifacts}
    result["tool_sha256"] = digest(Path(__file__))
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    summary_keys = ["required_programs", "required_steps", "explicit_type_shapes",
                    "native_C7_programs", "native_C7_steps"]
    print(json.dumps({key: result[key] for key in summary_keys}
                     | {"connector_executed": result["connector"]["connector_executed"]}))


if __name__ == "__main__":
    main()
