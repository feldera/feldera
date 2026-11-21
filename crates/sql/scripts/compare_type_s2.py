#!/usr/bin/env python3
"""Compare compact Java S2 expression and type text with the Rust lowerer."""
import argparse
from collections import Counter, defaultdict
from itertools import zip_longest
import json
from pathlib import Path

from type_binding_calls import FUNCTIONS
from type_binding_compare import digest, records

STRATEGIES = ("translated_s2_projection", "rewritten_s2_projection")
TEXT_FIELDS = ("expression_text", "type_text")
EXTRA_SCALARS = {"+", "-", "*", "/", "=", "<", "CASE"}
RELATIONAL = {"ARRAY_AGG", "AVG", "COUNT", "MAX", "MIN", "SUM",
              "EXCEPT ALL", "INTERSECT ALL", "UNION ALL"}
SETS = {"EXCEPT ALL", "INTERSECT ALL", "UNION ALL"}
AGGREGATES = RELATIONAL - SETS


def aggregate_components(projection):
    components = projection["components"]
    if len(components) != 1:
        raise ValueError("expected a single aggregate component")
    component = components[0]
    kind = component["kind"]
    required = {
        "LinearAggregate": {"map", "post"},
        "MinMaxAggregate": {"zero", "increment", "post", "semigroup", "compared_value"},
        "NonLinearAggregate": {"zero", "increment", "post", "semigroup"},
    }.get(kind)
    if required is None or set(component) != required | {"kind", "empty"}:
        raise ValueError("unexpected aggregate component kind or fields")
    target = {"kind": kind}
    for key, value in component.items():
        if key == "kind":
            continue
        if key == "semigroup":
            if type(value) is not str:
                raise ValueError("invalid aggregate semigroup text")
            target[key] = value
        elif key == "post" and kind != "LinearAggregate" and value is None:
            target[key] = None
        else:
            target[key] = {field: value[field] for field in TEXT_FIELDS}
            if not all(type(part) is str for part in target[key].values()):
                raise ValueError("invalid aggregate expression or type text")
    return [target]


SCOPES = {
    "functions": (FUNCTIONS, STRATEGIES, "function projections"),
    "extra-scalars": (EXTRA_SCALARS, ("additional_s2_projection",),
                      "arithmetic, comparison and CASE"),
    "sets": (SETS, ("set_s2_projections",), "set coercion maps and null filters"),
    "aggregates": (AGGREGATES, ("aggregate_s2_components",),
                   "global aggregate component expressions and types"),
}


def compare(java_path, rust_path, output_path, expected, scope="functions"):
    all_operations = FUNCTIONS | EXTRA_SCALARS | RELATIONAL
    wanted, _, description = SCOPES.get(scope, (all_operations, (), "all generated probes"))
    # A historical extra-scalar capture contains relational controls with no
    # corresponding projection. It remains usable for focused measurement.
    allowed = wanted | RELATIONAL if scope == "extra-scalars" else wanted
    counts = defaultdict(Counter)
    seen = set()
    with (output_path / "differences.jsonl").open("x") as differences:
        for java, rust in zip_longest(records(java_path), records(rust_path)):
            if java is None or rust is None:
                raise ValueError("Java and Rust streams have different lengths")
            probe = java["probe"]
            ident = probe["id"]
            if (type(ident) is not int or ident < 0 or ident in seen
                    or type(rust["id"]) is not int or rust["id"] != ident):
                raise ValueError("invalid, misaligned, or repeated probe ID")
            seen.add(ident)
            if any(type(row["schema_version"]) is not int or row["schema_version"] != 1
                   or row["runtime_verified"] is not False for row in (java, rust)):
                raise ValueError("unexpected schema version or runtime status")
            if (type(java["accepted"]) is not bool or java["compiler_bug"] is not False
                    or java["capture_complete"] is not True):
                raise ValueError("invalid Java verdict or incomplete capture")
            operation = probe["operation"]
            if operation not in allowed:
                raise ValueError(f"operation outside selected manifest: {operation}")
            category_scope, strategies = next((name, strategy) for name, (ops, strategy, _) in SCOPES.items()
                                              if operation in ops)
            origins = [argument["origin"] for argument in probe["arguments"]]
            if not origins or any(origin not in {"literal", "column"} for origin in origins):
                raise ValueError("invalid argument origins")
            projection = java.get("s2_projection")
            if java["accepted"]:
                if category_scope == "aggregates" and projection["status"] == "aggregate":
                    target = aggregate_components(projection)
                elif category_scope == "sets" and projection["status"] == "set_maps":
                    target = [{key: item[key] for key in ("operator", *TEXT_FIELDS)}
                              for item in projection["maps"]]
                    if len(target) < 2 or not all(
                            item["operator"] in {"map", "filter"}
                            and all(type(item[key]) is str for key in TEXT_FIELDS)
                            for item in target):
                        raise ValueError("invalid Java set map or filter")
                elif category_scope in {"functions", "extra-scalars"} and projection["status"] == "captured":
                    target = {key: projection[key] for key in TEXT_FIELDS}
                    if not all(type(value) is str for value in target.values()):
                        raise ValueError("invalid Java C6a printer text")
                elif projection["status"] in {"outside_scope", "set_maps", "captured", "aggregate"}:
                    target = None
                else:
                    raise ValueError("unknown Java projection status")
            elif projection is not None:
                raise ValueError("rejected probe contains a Java S2 projection")
            else:
                target = None
            comparison_strategies = SCOPES[scope][1] if scope == "extra-scalars" else strategies
            predicted = {strategy: rust[strategy] for strategy in comparison_strategies}
            if not java["accepted"]:
                category = "rejected"
            elif operation not in wanted:
                category = "relational_deferred"
            else:
                matched = target is not None and all(result == target for result in predicted.values())
                if matched:
                    category = "equal_with_literal" if "literal" in origins else "equal_columns"
                else:
                    category = "different"
            if category in {"rejected", "relational_deferred"} and any(
                    result is not None for result in predicted.values()):
                category = "unexpected_prediction"
            counts[operation][category] += 1
            if category in {"different", "unexpected_prediction"}:
                differences.write(json.dumps({
                    "id": ident, "operation": operation, "category": category,
                    "arguments": probe["arguments"], "java": projection, "rust": predicted,
                }, sort_keys=True) + "\n")
    if len(seen) != expected:
        raise ValueError(f"expected {expected} probes, got {len(seen)}")
    totals = sum(counts.values(), Counter())
    result = {
        "schema_version": 1,
        "probes": len(seen),
        "comparison": "exact compact C6a expression and type text for " + description,
        "shared_function_lowering": scope in {"all", "functions"},
        "runtime_verified": False,
        "full_C9_verified": False,
        "java_sha256": digest(java_path),
        "rust_sha256": digest(rust_path),
        "counts": dict(totals),
        "operations": {key: dict(value) for key, value in sorted(counts.items())},
    }
    with (output_path / "summary.json").open("x") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return totals


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("java", type=Path)
    parser.add_argument("rust", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--expected", type=int)
    default_count = {"functions": 42880, "extra-scalars": 41344, "sets": 12288, "aggregates": 384, "all": 84224}
    parser.add_argument("--scope", choices=tuple(default_count), default="functions")
    args = parser.parse_args()
    args.output.mkdir()
    expected = default_count[args.scope] if args.expected is None else args.expected
    totals = compare(args.java, args.rust, args.output, expected, args.scope)
    print(dict(totals))
    return bool(totals["different"] or totals["unexpected_prediction"]
                or not (totals["equal_columns"] and totals["equal_with_literal"]))


if __name__ == "__main__":
    raise SystemExit(main())
