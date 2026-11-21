#!/usr/bin/env python3
"""Compare H's generated function probes with Java verdicts and result types."""
import argparse
from collections import Counter, defaultdict
from itertools import zip_longest
import json
from pathlib import Path

from type_binding_calls import FUNCTIONS
from type_binding_compare import canonical, digest, records
from type_derivation_compare import java_result


def compare(java_path, h_path, output_path, expected, a_path=None):
    counts = defaultdict(Counter)
    seen = set()
    with (output_path / "differences.jsonl").open("x") as errors:
        streams = [records(java_path), records(h_path)]
        if a_path is not None:
            streams.append(records(a_path))
        for row in zip_longest(*streams):
            if any(record is None for record in row):
                raise ValueError("oracle and binder streams have different lengths")
            java, h, *own = row
            probe = java["probe"]
            ident = probe["id"]
            operation = probe["operation"]
            if (type(ident) is not int or ident < 0 or ident in seen
                    or type(h["id"]) is not int or h["id"] != ident
                    or operation not in FUNCTIONS or type(h["accepted"]) is not bool):
                raise ValueError("invalid or mismatched probe record")
            seen.add(ident)
            reference = java_result(java)
            if h["accepted"]:
                if set(h) != {"id", "accepted", "field"}:
                    raise ValueError("unexpected accepted H record")
                field = h["field"]
                span = field["projection_span"]
                if len(span) != 4 or any(type(part) is not int or part <= 0 for part in span):
                    raise ValueError("invalid H projection span")
                if reference is None:
                    result = "h_only"
                elif field["name"] != "result" or canonical(field["type"]) != canonical(reference):
                    result = "different_output_field"
                else:
                    result = "equal_accepted"
            elif set(h) != {"id", "accepted", "diagnostic"} or type(h["diagnostic"]) is not str or not h["diagnostic"]:
                raise ValueError("missing H refusal diagnostic")
            else:
                result = "equal_rejected" if reference is None else "java_only"
            if own:
                a = own[0]
                if type(a["id"]) is not int or a["id"] != ident or type(a["accepted"]) is not bool:
                    raise ValueError("invalid or mismatched A record")
                if a["accepted"]:
                    projection = a["projection"]
                    exact = reference is not None and projection["name"] == "result" and (
                        canonical(projection["result"]) == canonical(reference)
                        == canonical(projection["expression"]["ty"]))
                    if not exact:
                        result = "different_a_output_field"
                elif reference is not None or a["diagnostic"]["category"] != "type_rejection":
                    result = "different_a_rejection"
            counts[operation][result] += 1
            if not result.startswith("equal_"):
                errors.write(json.dumps({"probe": probe, "java_result": reference, "h": h,
                                         "a": own[0] if own else None}) + "\n")
    if len(seen) != expected or set(counts) != FUNCTIONS:
        raise ValueError(f"incomplete function population: {len(seen)} probes, {len(counts)} functions")
    summary = {
        "schema_version": 1,
        "probes": len(seen),
        "scope": "function verdict and one named result field; no diagnostic, FRel, or C9 certification",
        "java_sha256": digest(java_path), "h_sha256": digest(h_path),
        "counts": dict(sum(counts.values(), Counter())),
        "operations": {op: dict(counts[op]) for op in sorted(counts)},
    }
    if a_path is not None:
        summary["a_sha256"] = digest(a_path)
    with (output_path / "summary.json").open("x") as stream:
        json.dump(summary, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return summary["counts"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("java", type=Path)
    parser.add_argument("predictions", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--expected", type=int, default=42880)
    parser.add_argument("--own-binder", type=Path)
    args = parser.parse_args()
    args.output.mkdir()
    print(compare(args.java, args.predictions, args.output, args.expected, args.own_binder))


if __name__ == "__main__":
    main()
