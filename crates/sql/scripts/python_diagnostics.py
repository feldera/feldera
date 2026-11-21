#!/usr/bin/env python3
"""Validate complete compiler-only results for extracted Python expectations."""

import argparse
import hashlib
import json
from pathlib import Path


def summarize(extraction, results):
    if extraction["schema_version"] != 1 or extraction["complete"] is not True:
        raise ValueError("completed input extraction required")
    inputs = extraction["records"]
    if [row["id"] for row in inputs] != [row["id"] for row in results]:
        raise ValueError("compiler results omit, reorder, or add cases")
    if len({row["id"] for row in inputs}) != len(inputs):
        raise ValueError("duplicate case identities")
    counts = dict(cases=len(inputs), disabled=0, compiler_rejected=0,
                  compiler_rejected_with_expected_text=0, compiler_accepted=0)
    mismatches, internal, cases = [], [], []
    for source, result in zip(inputs, results):
        if result.get("runtime_executed") is not False:
            raise ValueError("compiler probe cannot certify runtime execution")
        if not source["active"]:
            if result.get("disposition") != "not_registered" or "compiler_rejected" in result:
                raise ValueError("disabled case has a compiler result")
            counts["disabled"] += 1
            disposition = "not_registered"
        else:
            if type(result.get("compiler_rejected")) is not bool or type(result.get("circuit_produced")) is not bool:
                raise ValueError("missing compiler outcome")
            expected = source["expected"].strip().lower() in result["rendered"].lower()
            if expected != result["expected_text_found"]:
                raise ValueError("expectation match flag disagrees with rendered diagnostic")
            if result["compiler_rejected"]:
                counts["compiler_rejected"] += 1
                counts["compiler_rejected_with_expected_text"] += expected
                disposition = "compiler_rejection_matches" if expected else "compiler_rejection_differs"
                if not expected:
                    mismatches.append(source["id"])
            else:
                if not result["circuit_produced"]:
                    raise ValueError("accepted compilation produced no circuit")
                counts["compiler_accepted"] += 1
                disposition = "requires_runtime_evaluation"
            if any("bug in the compiler" in message["error_type"].lower() for message in result["diagnostics"]):
                internal.append(source["id"])
        cases.append({"id": source["id"], "disposition": disposition, "expected": source["expected"], "result": result})
    return {"schema_version": 1, "complete": True, "counts": counts,
            "compiler_expectation_mismatches": mismatches, "internal_compiler_failures": internal,
            "cases": cases, "runtime_executed": False, "python_tests_executed": False,
            "catalog_reviewed": False, "P0_complete": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("extraction", type=Path)
    parser.add_argument("results", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--require-compiler-matches", action="store_true")
    args = parser.parse_args()
    report = summarize(json.loads(args.extraction.read_text()), [json.loads(line) for line in args.results.read_text().splitlines()])
    report["inputs"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in [args.extraction, args.results]}
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({key: report[key] for key in ["counts", "compiler_expectation_mismatches", "internal_compiler_failures"]}))
    if args.require_compiler_matches and (report["compiler_expectation_mismatches"] or report["internal_compiler_failures"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
