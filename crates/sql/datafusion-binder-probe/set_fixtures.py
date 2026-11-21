#!/usr/bin/env python3
"""Generate D1d fixtures with explicit bags and the independent Rust output."""
import argparse
import json
from pathlib import Path
import subprocess


def values(*rows):
    return "VALUES " + ", ".join(
        "(CAST(" + ("NULL" if row is None else str(row)) + " AS INT))"
        for row in rows
    )


def cases():
    left = values(None, None, None, 7, 7, 7, 9)
    right = values(None, None, 7, 7, 8)
    empty = "SELECT x FROM (VALUES (CAST(NULL AS INT))) AS t(x) WHERE FALSE"
    return (
        ("intersect_all", f"{left} INTERSECT ALL {right}", [(None, 2), (7, 2)]),
        ("except_all", f"{left} EXCEPT ALL {right}", [(None, 1), (7, 1), (9, 1)]),
        ("intersect_all_reversed", f"{right} INTERSECT ALL {left}", [(None, 2), (7, 2)]),
        ("except_all_reversed", f"{right} EXCEPT ALL {left}", [(8, 1)]),
        ("intersect_distinct", f"{left} INTERSECT DISTINCT {right}", [(None, 1), (7, 1)]),
        ("except_distinct", f"{left} EXCEPT DISTINCT {right}", [(9, 1)]),
        ("intersect_implicit_distinct", f"{left} INTERSECT {right}", [(None, 1), (7, 1)]),
        ("except_implicit_distinct", f"{left} EXCEPT {right}", [(9, 1)]),
        ("intersect_empty_left", f"{empty} INTERSECT ALL {right}", []),
        ("intersect_empty_right", f"{left} INTERSECT ALL {empty}", []),
        ("except_empty_left", f"{empty} EXCEPT ALL {right}", []),
        ("except_empty_right", f"{left} EXCEPT ALL {empty}", [(None, 3), (7, 3), (9, 1)]),
        ("except_both_empty", f"{empty} EXCEPT DISTINCT {empty}", []),
        ("intersect_nested", f"({left} EXCEPT ALL {right}) INTERSECT ALL {right}", [(None, 1), (7, 1)]),
        ("except_nested", f"{left} EXCEPT ALL ({left} INTERSECT ALL {right})", [(None, 1), (7, 1), (9, 1)]),
        ("intersect_nonnull_output", f"{left} INTERSECT ALL {values(7, 7)}", [(7, 2)]),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    records = []
    for name, sql, bag in cases():
        completed = subprocess.run(
            [str(args.executable), "--preserve-set-operations", sql],
            check=True, capture_output=True, text=True, timeout=30,
        )
        prediction = json.loads(completed.stdout)
        expected = [{"row": row, "weight": weight} for row, weight in bag]
        if prediction["rows"] != expected:
            raise ValueError(f"{name}: {prediction['rows']} != {expected}")
        records.append({"id": name, "sql": sql, "expected": expected, "rust": prediction})
    with args.output.open("x") as stream:
        json.dump(records, stream, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    main()
