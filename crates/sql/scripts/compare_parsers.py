#!/usr/bin/env python3
"""Compare completed raw-parser experiments; do not infer semantic equivalence."""

import argparse
import collections
import hashlib
import itertools
import json
import re
from pathlib import Path

VARIANTS = {
    (version, dialect)
    for version in ("0.62.0", "0.64.0")
    for dialect in ("generic", "feldera_prototype")
}


def java_line_advance(sql):
    """Mirror createSqlParser's Java split, which drops trailing empty lines."""
    if not sql:
        return 1
    stripped = sql.rstrip("\n")
    return len(stripped.split("\n")) if stripped else 0


def error_position(measurement, java, line_offset=0):
    """Compare reported starts only; no token shifts or column-unit correction.

    The disposable Rust parser currently exposes its location in the diagnostic
    suffix. This measures that evidence, not complete AST source spans.
    """
    position = [java.get("line"), java.get("column")]
    if any(value is not None and type(value) is not int for value in position):
        raise ValueError("Malformed Java parser position")
    java_position = position if all(value and value > 0 for value in position) else None
    if verdict(measurement):
        return {"outcome": "rust_accepts", "java": java_position, "rust": None}
    match = re.search(r"\bLine: ([0-9]+), Column: ([0-9]+)\s*$", measurement["error"])
    rust_position = list(map(int, match.groups())) if match else None
    if rust_position and not all(rust_position):
        rust_position = None
    if rust_position:
        rust_position[0] += line_offset
    if java_position is None:
        outcome = "java_position_missing"
    elif rust_position is None:
        outcome = "rust_position_missing"
    elif java_position == rust_position:
        outcome = "equal"
    else:
        outcome = "different"
    return {"outcome": outcome, "java": java_position, "rust": rust_position}


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verdict(row):
    value = row["verdict"]
    if value not in ("accepted", "rejected"):
        raise ValueError(f"unknown parser verdict: {value}")
    return value == "accepted"


def view_facts(rust, java):
    """Compare typed view fields, independently of either parser's SQL display."""
    expected = []
    for index, statement in enumerate(java["statements"]):
        if statement.get("node_class") != "SqlCreateView":
            continue
        kind, replace = statement.get("view_kind"), statement.get("replace")
        if kind not in ("STANDARD", "LOCAL", "MATERIALIZED") or type(replace) is not bool:
            raise ValueError("Java view AST fields are missing or malformed")
        expected.append({"statement_index": index, "kind": kind, "replace": replace})
    actual = rust.get("views", [])
    if not isinstance(actual, list):
        raise ValueError("Malformed Rust view AST fields")
    previous = -1
    for item in actual:
        if (not isinstance(item, dict) or set(item) != {"statement_index", "kind", "replace"}
                or type(item["statement_index"]) is not int
                or not previous < item["statement_index"] < len(rust["statements"])
                or item["kind"] not in ("STANDARD", "LOCAL", "MATERIALIZED")
                or type(item["replace"]) is not bool):
            raise ValueError("Malformed Rust view AST fields")
        previous = item["statement_index"]
    return actual, expected


def compare(rust_directory, java_directory, compare_view_fields=False):
    rust_manifest = json.loads((rust_directory / "complete.json").read_text())
    java_manifest = json.loads((java_directory / "complete.json").read_text())
    for manifest in (rust_manifest, java_manifest):
        if manifest["schema_version"] != 1 or manifest["complete"] is not True:
            raise ValueError("completed version-1 parser evidence is required")
    if java_manifest["stage"] != "raw_parser":
        raise ValueError("Java evidence must stop at the raw parser")
    if rust_manifest["input_sha256"] != java_manifest["input_sha256"]:
        raise ValueError("parser experiments used different inputs")
    counts = {f"{v}/{d}": collections.Counter() for v, d in sorted(VARIANTS)}
    positions = {key: collections.Counter() for key in counts}
    position_differences = []
    groups = {}
    identities = set()
    java_counts = collections.Counter()
    unparse_failures = []
    view_count = 0
    view_differences = []
    with (
        (rust_directory / "cases.jsonl").open() as rust_file,
        (java_directory / "cases.jsonl").open() as java_file,
    ):
        for rust_line, java_line in itertools.zip_longest(rust_file, java_file):
            if rust_line is None or java_line is None:
                raise ValueError("parser result lengths differ")
            rust, java = json.loads(rust_line), json.loads(java_line)
            identity = rust["id"]
            if identity != java["id"] or identity in identities:
                raise ValueError("parser identities differ or repeat")
            identities.add(identity)
            accepted_case = dict.fromkeys(counts, True)
            java_accepted_case = True
            line_offset = 0
            for index, (rust_submission, java_submission) in enumerate(
                itertools.zip_longest(rust["submissions"], java["submissions"])
            ):
                if rust_submission is None or java_submission is None:
                    raise ValueError("submission counts differ")
                if rust_submission["source"] != java_submission["source"]:
                    raise ValueError("SQL or submission modes differ")
                java_accepted = verdict(java_submission)
                for statement_index, statement in enumerate(
                    java_submission.get("statements", [])
                ):
                    if "unparse_failure" not in statement:
                        continue
                    failure = statement["unparse_failure"]
                    if (
                        not java_accepted
                        or "sql" in statement
                        or not isinstance(failure, dict)
                        or any(
                            not isinstance(failure.get(key), str)
                            for key in ("type", "message")
                        )
                        or not isinstance(failure.get("stack"), list)
                        or not failure["stack"]
                        or any(not isinstance(frame, str) for frame in failure["stack"])
                    ):
                        raise ValueError("Malformed Java unparse failure")
                    unparse_failures.append(
                        {
                            "id": identity,
                            "submission": index,
                            "statement": statement_index,
                            "failure": failure,
                        }
                    )
                java_accepted_case &= java_accepted
                java_counts[
                    "accepted_submissions" if java_accepted else "rejected_submissions"
                ] += 1
                seen = set()
                for measurement in rust_submission["measurements"]:
                    variant = (measurement["version"], measurement["dialect"])
                    if variant not in VARIANTS or variant in seen:
                        raise ValueError("unknown or repeated parser variant")
                    seen.add(variant)
                    key = "/".join(variant)
                    rust_accepted = verdict(measurement)
                    accepted_case[key] &= rust_accepted
                    outcome = {
                        (True, True): "both_accept",
                        (False, True): "java_only",
                        (True, False): "rust_only",
                        (False, False): "both_reject",
                    }[rust_accepted, java_accepted]
                    counts[key][outcome] += 1
                    if (compare_view_fields and key == "0.64.0/feldera_prototype"
                            and rust_accepted and java_accepted):
                        actual, expected = view_facts(measurement, java_submission)
                        view_count += len(expected)
                        if actual != expected:
                            view_differences.append({"id": identity, "submission": index,
                                                     "rust": actual, "java": expected})
                    if not java_accepted:
                        position = error_position(
                            measurement, java_submission, line_offset
                        )
                        positions[key]["java_rejected_submissions"] += 1
                        positions[key][position["outcome"]] += 1
                        if position["outcome"] != "equal":
                            position_differences.append(
                                {
                                    "id": identity,
                                    "submission": index,
                                    "variant": key,
                                    **position,
                                }
                            )
                    counts[key][
                        "accepted_submissions"
                        if rust_accepted
                        else "rejected_submissions"
                    ] += 1
                    if rust_accepted != java_accepted:
                        error = (
                            java_submission["error"]
                            if rust_accepted
                            else measurement["error"]
                        )
                        # Group only location differences, retaining actual tokens/messages.
                        reason = re.sub(
                            r"Line: \d+, Column: \d+", "Line: N, Column: N", error
                        )
                        group = groups.setdefault(
                            (key, outcome, reason), {"count": 0, "examples": []}
                        )
                        group["count"] += 1
                        if len(group["examples"]) < 5:
                            group["examples"].append(
                                {"id": identity, "submission": index}
                            )
                if seen != VARIANTS:
                    raise ValueError("missing parser variant")
                line_offset += java_line_advance(java_submission["source"]["sql"])
            if not rust["submissions"]:
                raise ValueError("empty submissions are not parser evidence")
            java_counts[
                "accepted_cases" if java_accepted_case else "rejected_cases"
            ] += 1
            if java["accepted"] is not java_accepted_case:
                raise ValueError("Java case verdict disagrees with submissions")
            for key, accepted in accepted_case.items():
                if rust["accepted"][key] is not accepted:
                    raise ValueError("Rust case verdict disagrees with submissions")
                counts[key]["accepted_cases" if accepted else "rejected_cases"] += 1
    if not identities or any(
        m["cases"] != len(identities) for m in (rust_manifest, java_manifest)
    ):
        raise ValueError("case count disagrees with completion manifest")
    for key, count in counts.items():
        for name in (
            "accepted_cases",
            "rejected_cases",
            "accepted_submissions",
            "rejected_submissions",
        ):
            if count[name] != rust_manifest["counts"][key][name]:
                raise ValueError("Rust counters disagree with completion manifest")
    for name in (
        "accepted_cases",
        "rejected_cases",
        "accepted_submissions",
        "rejected_submissions",
    ):
        if java_counts[name] != java_manifest[name]:
            raise ValueError("Java counters disagree with completion manifest")
    if java_manifest.get("unparse_failures", 0) != len(unparse_failures):
        raise ValueError("Java unparse failures disagree with completion manifest")
    if java_manifest.get("rendering_complete", True) is not (not unparse_failures):
        raise ValueError("Java rendering completion disagrees with failures")
    return {
        "schema_version": 1,
        "cases": len(identities),
        "input_sha256": rust_manifest["input_sha256"],
        "rust_build": rust_manifest,
        "java_build": java_manifest,
        "semantic_equivalence_checked": False,
        "view_fields": {"checked": compare_view_fields, "java_views": view_count,
                        "scope": "typed CREATE VIEW kind and replacement flag only; not S1 equivalence",
                        "differences": view_differences},
        "java_rendering": {
            "complete": not unparse_failures,
            "failures": unparse_failures,
        },
        "counts": counts,
        "error_positions": {
            "scope": "reported error start, with Java submission line accounting; without column-unit normalization",
            "complete_ast_spans_checked": False,
            "counts": positions,
            "differences": position_differences,
        },
        "evidence_sha256": {
            "rust_cases": digest(rust_directory / "cases.jsonl"),
            "java_cases": digest(java_directory / "cases.jsonl"),
            "rust_completion": digest(rust_directory / "complete.json"),
            "java_completion": digest(java_directory / "complete.json"),
            "comparison": digest(Path(__file__)),
        },
        "disagreement_groups": [
            {"variant": key, "outcome": outcome, "reason": reason, **group}
            for (key, outcome, reason), group in sorted(groups.items())
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rust", type=Path)
    parser.add_argument("java", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compare-view-fields", action="store_true",
                        help="Require typed CREATE VIEW kind and replacement agreement for the candidate")
    parser.add_argument(
        "--reject-overacceptance",
        choices=sorted(f"{version}/{dialect}" for version, dialect in VARIANTS),
        help="Fail after saving evidence if this variant accepts Java-rejected SQL",
    )
    args = parser.parse_args()
    report = compare(args.rust, args.java, args.compare_view_fields)
    with args.output.open("x") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    print(json.dumps(report["counts"], indent=2))
    if report["view_fields"]["differences"]:
        raise SystemExit(f"Typed view fields differ; evidence saved to {args.output}")
    if args.reject_overacceptance:
        count = report["counts"][args.reject_overacceptance]["rust_only"]
        if count:
            raise SystemExit(
                f"{args.reject_overacceptance} accepted {count} Java-rejected submissions; "
                f"evidence saved to {args.output}"
            )


if __name__ == "__main__":
    main()
