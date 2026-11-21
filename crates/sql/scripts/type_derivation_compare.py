#!/usr/bin/env python3
"""Compare type-probe acceptance/result types, without claiming full C9.

Reads the original Java golden stream and Rust output in the same ID order.
Unknown types, duplicate IDs/keys, truncated populations, and compiler bugs
abort the run; they never become ignored or successful comparisons.
"""
import argparse
from collections import Counter
import hashlib
from itertools import zip_longest
import json
from pathlib import Path


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def records(path, digest):
    with path.open("rb") as stream:
        while line := stream.readline(32 * 1024 * 1024 + 1):
            if len(line) > 32 * 1024 * 1024:
                raise ValueError("record exceeds 32 MiB")
            digest.update(line)
            yield json.loads(line, object_pairs_hook=unique_object)


def sql_type(raw):
    """Map only the declared metadata schema, retaining every type attribute."""
    name = raw["type"]
    nullable = raw["nullable"]
    if type(nullable) is not bool:
        raise ValueError("non-boolean nullability")
    fields = {"type", "nullable"}
    kind = {"kind": name.lower()}
    if name in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT"):
        kind = {"kind": "integer", "bits": {"TINYINT": 8, "SMALLINT": 16, "INTEGER": 32, "BIGINT": 64}[name]}
    elif name in ("CHAR", "VARCHAR", "BINARY", "VARBINARY"):
        fields.add("precision")
        length = raw["precision"]
        if type(length) is not int or length == 0 or length < -1:
            raise ValueError("invalid character/binary precision")
        kind = {"kind": "character" if name in ("CHAR", "VARCHAR") else "binary",
                "fixed": name in ("CHAR", "BINARY"), "length": None if length == -1 else length}
    elif name == "DECIMAL":
        fields.update(("precision", "scale"))
        kind.update(precision=raw["precision"], scale=raw["scale"])
    elif name == "TIMESTAMP":
        fields.add("precision")
        kind["precision"] = raw["precision"]
    elif name == "ARRAY":
        if set(raw) != {"type", "nullable", "component"}:
            raise ValueError("unknown array metadata field")
        return {"component": sql_type(raw["component"]), "nullable": nullable}
    elif name not in ("BOOLEAN", "REAL", "DOUBLE", "DATE"):
        raise ValueError(f"unclassified result type: {name}")
    if set(raw) != fields:
        raise ValueError(f"unknown or missing {name} metadata fields: {set(raw) ^ fields}")
    return {"kind": kind, "nullable": nullable}


def java_result(record):
    if type(record["accepted"]) is not bool or record["compiler_bug"] is not False:
        raise ValueError("invalid Java verdict or compiler bug")
    if not record["accepted"]:
        return None
    outputs = [v for v in record["schema"]["outputs"] if v["name"] == "v"]
    if len(outputs) != 1 or len(outputs[0]["fields"]) != 1:
        raise ValueError("accepted probe must have exactly one result column in v")
    return sql_type(outputs[0]["fields"][0]["columntype"])


def compare(record, outcome):
    status = outcome["status"]
    if status == "unsupported":
        return "unsupported"
    if status not in ("bound", "rejected"):
        raise ValueError(f"unknown Rust outcome: {status}")
    if (status == "bound") != record["accepted"]:
        return "acceptance_difference"
    if status == "bound" and canonical(outcome["result"]) != canonical(java_result(record)):
        return "result_type_difference"
    return "equal"


def run(golden, outcomes, output, expected_count):
    if not 0 < expected_count <= 100_000:
        raise ValueError("expected population must be between 1 and 100000")
    output.mkdir()
    hashes = [hashlib.sha256(), hashlib.sha256()]
    counts = {name: Counter() for name in ("translated", "rewritten", "additional")}
    operations = {}
    seen = set()
    strategy_differences = 0
    uncovered = 0
    with (output / "differences.jsonl").open("x") as differences:
        for java, rust in zip_longest(records(golden, hashes[0]), records(outcomes, hashes[1])):
            if java is None or rust is None:
                raise ValueError("Java/Rust record counts differ")
            probe = java["probe"]
            ident = probe["id"]
            if (type(ident) is not int or type(rust["id"]) is not int
                    or ident < 0 or ident in seen or rust["id"] != ident):
                raise ValueError("duplicate, invalid, or reordered probe ID")
            if len(seen) >= 100_000:
                raise ValueError("probe population exceeds limit")
            seen.add(ident)
            if any(type(record["schema_version"]) is not int or record["schema_version"] != 1
                   for record in (java, probe, rust)):
                raise ValueError("unknown schema version")
            if rust.get("runtime_verified") is not False:
                raise ValueError("unsupported runtime verification claim")
            expected = java_result(java)
            operation = probe["operation"]
            if all(rust[name]["status"] == "unsupported" for name in counts):
                uncovered += 1
            if operation not in operations:
                operations[operation] = {name: Counter() for name in counts}
            for name in counts:
                category = compare(java, rust[name])
                counts[name][category] += 1
                operations[operation][name][category] += 1
                if category.endswith("difference"):
                    differences.write(json.dumps({"id": ident, "operation": operation,
                        "arguments": probe["arguments"], "strategy": name, "category": category,
                        "java_accepted": java["accepted"], "java_result": expected,
                        "java_diagnostics": java["diagnostics"], "rust": rust[name]}, sort_keys=True) + "\n")
            if canonical(rust["translated"]) != canonical(rust["rewritten"]):
                strategy_differences += 1
        if len(seen) != expected_count:
            raise ValueError(f"expected {expected_count} probes, received {len(seen)}")
    report = {"schema_version": 1, "probes": len(seen), "counts": counts,
        "operations": operations, "strategy_outcome_differences": strategy_differences,
        "uncovered_probes": uncovered,
        "java_source_sha256": hashes[0].hexdigest(), "rust_outcomes_sha256": hashes[1].hexdigest(),
        "compared": ["SQL acceptance", "result SQL type including nullability and parameters"],
        "not_compared": ["diagnostic parity", "inserted casts", "resolved function identity", "runtime values"],
        "runtime_verified": False, "full_c9_verified": False}
    (output / "complete.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("golden", type=Path)
    parser.add_argument("outcomes", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--expected-count", type=int, default=84_224)
    parser.add_argument("--require-equal", action="store_true")
    args = parser.parse_args()
    report = run(args.golden, args.outcomes, args.output, args.expected_count)
    print(json.dumps({"probes": report["probes"], "counts": report["counts"],
                      "strategy_outcome_differences": report["strategy_outcome_differences"]}))
    if args.require_equal and (report["uncovered_probes"] or report["strategy_outcome_differences"]
            or any(category.endswith("difference") and count for counts in report["counts"].values()
                   for category, count in counts.items())):
        raise SystemExit("type acceptance/result comparison differs; see retained report")
