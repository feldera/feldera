"""Classify captured constant folds without treating unsupported cases as passes."""

import argparse
import collections
import decimal
import hashlib
import json
import math
import struct
from pathlib import Path


def decimal_text(text):
    value = format(decimal.Decimal(text), "f")
    return value.rstrip("0").rstrip(".") if "." in value else value


def reference(expression):
    """Read Java results; do not execute the expression under test as its oracle."""
    kind = expression["kind"]
    ty = expression["type"]
    if kind == "CAST":
        args = expression.get("arguments", [])
        if len(args) == 1 and all(
            args[0]["type"][k] == ty[k] for k in ("name", "precision", "scale")
        ):
            return reference(args[0])  # Only the nullability wrapper is ignored.
        return None
    if kind == "ARRAY_VALUE_CONSTRUCTOR":
        values = [reference(arg) for arg in expression["arguments"]]
        return (
            {"type": "array", "value": values}
            if all(v is not None for v in values)
            else None
        )
    if kind == "MAP_VALUE_CONSTRUCTOR":
        values = [reference(arg) for arg in expression["arguments"]]
        if len(values) % 2 or any(v is None for v in values):
            return None
        return {"type": "map", "value": list(zip(values[::2], values[1::2]))}
    if kind != "LITERAL":
        return None
    text = expression["value"]
    if text is None:
        return {"type": "null"}
    name = ty["name"]
    if name == "BOOLEAN":
        assert text in ("true", "false")
        return {"type": "boolean", "value": text == "true"}
    if name in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT"):
        return {"type": "integer", "value": str(int(text))}
    if name in ("REAL", "FLOAT", "DOUBLE"):
        value = float(text)
        if name == "REAL":
            value = struct.unpack(">f", struct.pack(">f", value))[0]
        return {"type": "float", "value": struct.pack(">d", value).hex()}
    if name == "DECIMAL":
        return {"type": "decimal", "value": decimal_text(text)}
    if name in ("CHAR", "VARCHAR"):
        return {"type": "string", "value": text}
    if name == "UUID":
        return {"type": "uuid", "value": text}
    if name == "GEOMETRY" and text.startswith("POINT (") and text.endswith(")"):
        coordinates = [float(part) for part in text[7:-1].split()]
        if len(coordinates) == 2 and all(math.isfinite(n) for n in coordinates):
            return {
                "type": "point",
                "value": [struct.pack(">d", n).hex() for n in coordinates],
            }
        return None
    if name in ("BINARY", "VARBINARY"):
        return {"type": "binary", "value": bytes.fromhex(text).hex()}
    if name in ("DATE", "TIME", "TIMESTAMP"):
        return {"type": name.lower(), "value": text}
    if name.startswith("INTERVAL_"):
        if name in ("INTERVAL_YEAR", "INTERVAL_MONTH", "INTERVAL_YEAR_MONTH"):
            return {"type": "months", "value": str(int(decimal.Decimal(text)))}
        with decimal.localcontext() as context:
            context.prec = 100
            return {"type": "micros", "value": str(int(decimal.Decimal(text) * 1000))}
    return None


def normalized(value):
    if value["type"] in ("time", "timestamp"):
        text = value["value"]
        return {**value, "value": text.rstrip("0").rstrip(".") if "." in text else text}
    if value["type"] == "array":
        return {**value, "value": [normalized(v) for v in value["value"]]}
    if value["type"] == "map":
        entries = [[normalized(k), normalized(v)] for k, v in value["value"]]
        # Entry order is not part of SQL map equality. Preserve key/value
        # association and every entry while canonicalizing only that order.
        return {
            **value,
            "value": sorted(
                entries, key=lambda pair: json.dumps(pair[0], sort_keys=True)
            ),
        }
    return value


def census(folds, actual):
    if not folds or len(folds) != len(actual):
        raise ValueError("Missing or empty fold population")
    counts = collections.Counter()
    operators = collections.Counter()
    reasons = collections.Counter()
    mismatches = []
    unique = set()
    for index, (fold, result) in enumerate(zip(folds, actual)):
        if result.get("index") != index or type(result.get("index")) is not int:
            raise ValueError("Fold result order differs")
        if type(fold.get("changed")) is not bool:
            raise ValueError("Malformed Java fold verdict")
        expression = fold["expression"]
        unique.add(
            hashlib.sha256(json.dumps(expression, sort_keys=True).encode()).hexdigest()
        )
        operators[
            (fold["evaluator"], expression["kind"], expression.get("operator", ""))
        ] += 1
        expected = reference(fold["result"])
        status = result.get("status")
        if status not in ("value", "unsupported", "runtime_error", "budget"):
            raise ValueError("Unknown Rust result status")
        counts[status] += 1
        counts["java_changed"] += fold["changed"]
        if status == "value" and expected is not None:
            counts["comparable"] += 1
            if normalized(result["value"]) == normalized(expected):
                counts["equal"] += 1
            else:
                mismatches.append(
                    {
                        "index": index,
                        "case": fold["case"],
                        "expression": expression,
                        "java": expected,
                        "rust": result["value"],
                    }
                )
        elif status == "value":
            counts["java_result_not_literal"] += 1
        elif status == "runtime_error" and expected is not None:
            counts["java_value_rust_error"] += 1
            mismatches.append(
                {
                    "index": index,
                    "case": fold["case"],
                    "expression": expression,
                    "java": expected,
                    "rust": result,
                }
            )
        if status != "value":
            reasons[result.get("reason", status)] += 1
    return {
        "observations": len(folds),
        "unique_expression_records": len(unique),
        "counts": dict(counts),
        "operators": [
            {"evaluator": k[0], "kind": k[1], "operator": k[2], "count": v}
            for k, v in operators.most_common()
        ],
        "refusals": dict(reasons.most_common()),
        "differences": mismatches,
        "all_supported": counts["value"] == len(folds),
        "zero_comparable_differences": not mismatches,
        "verdict_relevant_coverage_proven": False,
        "deviations_reviewed": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folds", type=Path)
    parser.add_argument("actual", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    report = census(
        [json.loads(line) for line in args.folds.read_text().splitlines()],
        [json.loads(line) for line in args.actual.read_text().splitlines()],
    )
    with args.output.open("x") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    print(
        json.dumps(
            {
                k: v
                for k, v in report.items()
                if k not in ("operators", "differences", "refusals")
            }
        )
    )


if __name__ == "__main__":
    main()
