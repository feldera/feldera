#!/usr/bin/env python3
"""Boundary rows and signed updates for the bounded C9 value comparison."""
import argparse
import json
from pathlib import Path


def change(ident, field, value, weight):
    return {"row": {"id": ident, field: value}, "weight": weight}


def manifest():
    scalar = [
        {"text": [], "integers": []},
        {"text": [change(i, "a", value, 2 if i == 2 else 1)
                  for i, value in enumerate([
                      None, "-9223372036854775808", "0", "9223372036854775807",
                      "-1", "+1", "001", "2", "-0",
                  ])], "integers": []},
        {"text": [change(2, "a", "0", -1), change(0, "a", None, -1),
                  change(10, "a", "-7", 1)], "integers": []},
        {"text": [change(2, "a", "0", -1), change(0, "a", None, 1)], "integers": []},
    ]
    relational = [
        {"text": [], "integers": []},
        {"text": [change(0, "a", "7", 3), change(1, "a", None, 3),
                  change(2, "a", "-9223372036854775808", 1),
                  change(3, "a", "9223372036854775807", 1)],
         "integers": [change(0, "b", 7, 2), change(1, "b", None, 2),
                      change(2, "b", -9223372036854775808, 2),
                      change(3, "b", 9223372036854775807, 1)]},
        {"text": [change(0, "a", "7", -1), change(1, "a", None, -1),
                  change(4, "a", "007", 1)],
         "integers": [change(0, "b", 7, -1), change(3, "b", 9223372036854775807, -1)]},
        {"text": [change(3, "a", "9223372036854775807", -1), change(4, "a", "007", -1)],
         "integers": [change(1, "b", None, -2), change(2, "b", -9223372036854775808, -2)]},
    ]
    for operation in ("equal", "less", "in", "between", "case"):
        yield {"id": operation, "operation": operation, "transactions": scalar}
    for operation in ("join", "union_all", "intersect_all", "except_all"):
        yield {"id": operation, "operation": operation, "transactions": relational}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    with args.output.open("x") as output:
        json.dump(list(manifest()), output, indent=2)
        output.write("\n")


if __name__ == "__main__":
    main()
