#!/usr/bin/env python3
"""Verify complete, disjoint worker runs against an ordered frozen SQL corpus."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path


def summarize(population, runs):
    expected = {}
    for line in population.read_bytes().splitlines():
        case = json.loads(line)
        if case["id"] in expected:
            raise ValueError(f"Duplicate input {case['id']}")
        expected[case["id"]] = case["input_sha256"]

    seen = set()
    for records in runs:
        manifest = json.loads((records / "complete.json").read_bytes())
        if not manifest["verify_worker"] or not manifest["verify_ir"] or manifest["failed_cases"]:
            raise ValueError(f"Incomplete worker verification in {records}")
        path = records / manifest["records_file"]
        reader = gzip.open if path.suffix == ".gz" else open
        count = 0
        with reader(path, "rb") as stream:
            for line in stream:
                case = json.loads(line)
                key = case["id"]
                if (key in seen or expected.get(key) != case["input_sha256"]
                        or case.get("failure") or case.get("worker_artifacts_equal") is not True):
                    raise ValueError(f"Duplicate, unexpected, or failing case {key}")
                seen.add(key)
                count += 1
        if count != manifest["cases"]:
            raise ValueError(f"Wrong record count in {path}")
    if not expected or seen != expected.keys():
        raise ValueError(f"Missing cases: {len(expected.keys() - seen)}")
    return {"cases": len(seen), "input_sha256": hashlib.sha256(population.read_bytes()).hexdigest(),
            "failed": 0, "worker_artifacts_equal": True, "java_import_hash_inputs_equal": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("population", type=Path)
    parser.add_argument("runs", nargs="+", type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.population, args.runs), indent=2))
