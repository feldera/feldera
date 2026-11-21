#!/usr/bin/env python3
"""Validate direct diagnostic assertions without conflating their contracts.

Exact lists, absence predicates, field fragments, and snapshot stability remain
separate. Java regex predicates are retained with their captured target rather
than reinterpreted using Python regex syntax. This is not a template count.
"""

import argparse
import collections
import hashlib
import json
from pathlib import Path


TARGETS = {
    "message_body", "error_class", "rendered", "rendered_warning_lines",
    "source", "snapshot", "regex_explanation", "config_exception",
    "structured_warnings", "structured_errors",
}
COMPARISONS = {"equals", "ordered_list_equals", "contains", "starts_with", "not_contains", "no_regex_find"}


def classify(row):
    required = {"schema_version", "source_test", "call_site", "target", "comparison", "expected", "actual"}
    if (not isinstance(row, dict) or not required <= row.keys() or row.keys() - required - {"context"}
            or type(row["schema_version"]) is not int or row["schema_version"] != 1
            or not isinstance(row["target"], str) or row["target"] not in TARGETS
            or not isinstance(row["comparison"], str) or row["comparison"] not in COMPARISONS
            or any(not isinstance(row[key], str) or not row[key] for key in ["source_test", "call_site"])
            or ("context" in row and not isinstance(row["context"], str))):
        raise ValueError("invalid direct diagnostic assertion")
    expected, actual, comparison, target = (row[key] for key in ["expected", "actual", "comparison", "target"])
    if target == "rendered_warning_lines" and comparison != "ordered_list_equals":
        raise ValueError("warning lines require ordered list equality")
    if target in {"structured_warnings", "structured_errors"}:
        expected_fields = {"errorType", "line", "column", "snippet"}
        if target == "structured_errors":
            expected_fields.add("warning")
        for diagnostics in (expected, actual):
            if not isinstance(diagnostics, list) or not diagnostics:
                raise ValueError("structured assertions require a nonempty diagnostic list")
            for diagnostic in diagnostics:
                if (not isinstance(diagnostic, dict)
                        or diagnostic.keys() != expected_fields
                        or ("warning" in diagnostic and type(diagnostic["warning"]) is not bool)
                        or not isinstance(diagnostic["errorType"], str) or not diagnostic["errorType"]
                        or not isinstance(diagnostic["snippet"], str)
                        or any(type(diagnostic[pos]) is not int or diagnostic[pos] < 0
                               for pos in ("line", "column"))):
                    raise ValueError("invalid structured diagnostic")
            if target == "structured_errors" and all(diagnostic["warning"] for diagnostic in diagnostics):
                raise ValueError("a failed compilation must contain an error")
        if comparison != "equals" or expected != actual:
            raise ValueError("structured diagnostic assertion failed")
        return "non_body_contract"
    if target == "snapshot":
        if comparison != "equals" or not isinstance(expected, (dict, list)) or not isinstance(actual, type(expected)):
            raise ValueError("invalid snapshot equality")
        # Python equates True and 1; JSON snapshots must retain those types.
        if json.dumps(expected, sort_keys=True) != json.dumps(actual, sort_keys=True):
            raise ValueError("snapshot assertion failed")
        return "snapshot_stability"
    if comparison == "ordered_list_equals":
        if target != "rendered_warning_lines" or any(
                not isinstance(value, list) or any(not isinstance(item, str) for item in value)
                for value in [expected, actual]):
            raise ValueError("invalid ordered warning list")
        if expected != actual:
            raise ValueError("ordered warning assertion failed")
        return "rendered_text_requires_attribution" if expected else "absence_contract"
    if not isinstance(expected, str) or not isinstance(actual, str):
        raise ValueError("text assertion requires string operands")
    passes = {
        "equals": lambda: expected == actual,
        "contains": lambda: expected in actual,
        "starts_with": lambda: actual.startswith(expected),
        "not_contains": lambda: expected not in actual,
        # Its source-pinned Java helper has already asserted Pattern.find == false.
        "no_regex_find": lambda: True,
    }
    if not passes[comparison]():
        raise ValueError("direct diagnostic predicate failed")
    if comparison in {"not_contains", "no_regex_find"}:
        return "absence_contract"
    if target in {"error_class", "source"}:
        return "non_body_contract"
    if not expected:
        return "absence_contract" if comparison == "equals" else "empty_text_contract"
    if target == "message_body":
        return "message_body_requirement"
    if target in {"regex_explanation", "config_exception"}:
        return "formatter_fragment_requirement"
    return "rendered_text_requires_attribution"


def summarize(manifest, rows):
    if (manifest.get("direct_assertion_contract") != "typed-targets-v1"
            or manifest.get("junit_successful") is not True
            or type(manifest.get("direct_assertions")) is not int
            or manifest["direct_assertions"] != len(rows)):
        raise ValueError("direct assertions require a matching successful capture")
    classified = [{**row, "disposition": classify(row)} for row in rows]
    return {
        "schema_version": 1,
        "complete": True,
        "scope": "Captured direct assertion helpers; source-wide completeness is not inferred",
        "counts": {
            "assertions": len(rows),
            "call_sites": len({row["call_site"] for row in rows}),
            "tests": len({row["source_test"] for row in rows}),
            "targets": dict(collections.Counter(row["target"] for row in rows)),
            "comparisons": dict(collections.Counter(row["comparison"] for row in rows)),
            "dispositions": dict(collections.Counter(row["disposition"] for row in classified)),
        },
        "assertions": classified,
        "required_template_count": None,
        "limitations": [
            "Rendered strings and lists require attribution before counting message bodies.",
            "Snapshot equality and negative predicates do not imply positive template dependencies.",
            "Source test identity is captured; no compiler options or SQL submissions are inferred for these direct assertions.",
            "Java regex absence uses the captured successful Java assertion, not Python regex evaluation.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    paths = [args.capture / "complete.json", args.capture / "direct-assertions.jsonl"]
    manifest = json.loads(paths[0].read_text())
    rows = [json.loads(line) for line in paths[1].read_text().splitlines()]
    report = summarize(manifest, rows)
    report["inputs"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2, ensure_ascii=False)
        stream.write("\n")
    print(json.dumps(report["counts"]))


if __name__ == "__main__":
    main()
