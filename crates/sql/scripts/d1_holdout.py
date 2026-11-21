#!/usr/bin/env python3
"""Freeze a common D1 split and audit the current binders' corpus reach."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

SALT = b"feldera-p0-d1-heldout-v1\0"
MAX_MANIFEST_LINE = 65_536


def rows(path):
    with path.open() as stream:
        return [json.loads(line) for line in stream]


def line(record):
    return json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"


def write_rows(path, records):
    with path.open("x") as stream:
        for record in records:
            stream.write(line(record))


def capture(positives, diagnostics):
    cases = []
    for path in positives:
        for source in rows(path):
            method, separator, index = source["id"].rpartition("/")
            if not separator or not index.isdecimal():
                raise ValueError(f"missing source method in {source['id']}")
            cases.append({
                "source_id": source["id"],
                "method": method,
                "population": "classic_export",
                "program": source["program"],
                "submissions": source["submissions"],
                "java_options": source["options"],
                "java_diagnostics": source["diagnostics"],
                "java_schema": source["schema"],
            })
    negative_rows = rows(diagnostics)
    complete = json.loads(diagnostics.with_name("complete.json").read_text())
    if (complete["schema_version"] != 1 or complete["junit_successful"] is not True
            or complete["records"] != len(negative_rows)):
        raise ValueError("Java diagnostic archive is incomplete")
    for index, source in enumerate(negative_rows):
        submissions = source["submissions"]
        cases.append({
            "source_id": f"diagnostic/{index}/{source['source_test']}",
            "method": source["source_test"],
            "population": "java_diagnostic",
            "program": "\n".join(item["sql"].rstrip(";\n") + ";" for item in submissions),
            "submissions": submissions,
            "java_diagnostics": source["diagnostics"],
            "java_schema": None,
        })
    if not cases or len({case["source_id"] for case in cases}) != len(cases):
        raise ValueError("missing or duplicate source case")
    return sorted(cases, key=lambda case: case["source_id"])


def groups(cases):
    # Put every invocation of a test method together. Identical program strings
    # also share a partition when different tests expect different outcomes.
    roots = {case["method"]: case["method"] for case in cases}

    def root(method):
        while roots[method] != method:
            roots[method] = roots[roots[method]]
            method = roots[method]
        return method

    by_program = {}
    for case in cases:
        program = case["program"]
        method = case["method"]
        if not program.strip():
            continue
        other = root(by_program.setdefault(program, method))
        representative = root(method)
        first, second = sorted((representative, other))
        roots[second] = first
    return {method: root(method) for method in roots}


def prepare(cases, output):
    output.mkdir()
    components = groups(cases)
    inputs = []
    counts = Counter()
    for identity, case in enumerate(cases):
        group = components[case["method"]]
        prefix = hashlib.sha256(SALT + group.encode()).digest()[:8]
        partition = "held_out" if int.from_bytes(prefix, "big") % 5 == 0 else "development"
        manifest_record = {"id": identity, "schema_version": 1, "program": case["program"]}
        if len(line(manifest_record).encode()) > MAX_MANIFEST_LINE:
            transport = "manifest_limit"
        elif not case["submissions"] or not case["program"].strip():
            transport = "no_sql"
        else:
            transport = "available"
            inputs.append(manifest_record)
        errors = sum(not message["warning"] for message in case["java_diagnostics"])
        counts[(partition, case["population"], transport)] += 1
        case.update(id=identity, partition=partition, group=group, transport=transport,
                    java_error_count=errors)
    write_rows(output / "cases.jsonl", cases)
    write_rows(output / "binder-input.jsonl", inputs)
    return {
        "cases": len(cases),
        "groups": len(set(components.values())),
        "methods": len(components),
        "counts": [
            {"partition": partition, "population": population, "transport": transport,
             "cases": count}
            for (partition, population, transport), count in sorted(counts.items())
        ],
    }


def index(records):
    result = {record["id"]: record for record in records}
    if len(result) != len(records):
        raise ValueError("duplicate binder result")
    return result


def compare(cases, a_records, h_records):
    a, h = index(a_records), index(h_records)
    expected = {case["id"] for case in cases if case["transport"] == "available"}
    if expected != set(a) or expected != set(h):
        raise ValueError("binder result ids do not match the common manifest")
    counts = Counter()
    for case in cases:
        key = (case["partition"], case["population"])
        identity = case["id"]
        if identity not in expected:
            counts[key + (case["transport"],)] += 1
            continue
        pair = (a[identity]["accepted"], h[identity]["accepted"])
        if any(type(verdict) is not bool for verdict in pair):
            raise ValueError("binder verdict is not a boolean")
        status = {
            (True, True): "both_emit",
            (True, False): "a_only",
            (False, True): "h_only",
            (False, False): "neither_emits",
        }[pair]
        counts[key + (status,)] += 1
    return [
        {"partition": key[0], "population": key[1], "current_result": key[2], "cases": count}
        for key, count in sorted(counts.items())
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    freeze = modes.add_parser("prepare")
    freeze.add_argument("diagnostics", type=Path)
    freeze.add_argument("output", type=Path)
    freeze.add_argument("classic", type=Path, nargs="+")
    diff = modes.add_parser("compare")
    diff.add_argument("output", type=Path)
    diff.add_argument("cases", type=Path)
    diff.add_argument("a", type=Path)
    diff.add_argument("h", type=Path)
    args = parser.parse_args()
    if args.mode == "prepare":
        result = prepare(capture(args.classic, args.diagnostics), args.output)
        with (args.output / "selection.json").open("x") as stream:
            json.dump(result, stream, indent=2)
            stream.write("\n")
    else:
        result = compare(rows(args.cases), rows(args.a), rows(args.h))
        with args.output.open("x") as stream:
            json.dump(result, stream, indent=2)
            stream.write("\n")


if __name__ == "__main__":
    main()
