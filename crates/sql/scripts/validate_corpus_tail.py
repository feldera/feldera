#!/usr/bin/env python3
"""Verify that Java and Rust matched both sides of the compatibility hash for each frozen case."""
import argparse
import hashlib
import json
from pathlib import Path


def records(path):
    with path.open() as stream:
        for line in stream:
            yield json.loads(line)


def verify(population, results):
    expected = {}
    for record in records(population):
        key = record["id"]
        if key in expected:
            raise ValueError(f"Duplicate case in population: {key}")
        expected[key] = record["input_sha256"]
    seen = set()
    for path in results:
        for record in records(path):
            key = record["id"]
            if key in seen or key not in expected or record["input_sha256"] != expected[key]:
                raise ValueError(f"Invalid case identity in {path}: {key}")
            if record["failure"] is not None:
                raise ValueError(f"{key}: {record['failure']}")
            seen.add(key)
    if not seen or len(seen) != len(expected):
        raise ValueError(f"Expected {len(expected)} cases, found {len(seen)}")
    return {"cases": len(seen), "mismatches": 0,
            "population_sha256": hashlib.sha256(population.read_bytes()).hexdigest(),
            "result_shards_sha256": [hashlib.sha256(path.read_bytes()).hexdigest() for path in results]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("population", type=Path)
    parser.add_argument("results", type=Path, nargs="+", help="check_tail_corpus JSONL outputs")
    args = parser.parse_args()
    print(json.dumps(verify(args.population, args.results), indent=2))
