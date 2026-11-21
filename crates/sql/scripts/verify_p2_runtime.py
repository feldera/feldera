#!/usr/bin/env python3
"""Verify C7 case identities, expected errors and test outcomes against the frozen population."""
import argparse
import hashlib
import json
from pathlib import Path
import re

from run_p2_runtime import case_name, file_hash, inventory


def verify(population, corpus, directory):
    run = json.loads((directory / "complete.json").read_bytes())
    if file_hash(population) != run["population_sha256"]:
        raise ValueError("Population digest differs from the completed run")
    groups, compile_only = inventory(population, corpus)
    if json.loads((directory / "compile-only.json").read_bytes()) != compile_only:
        raise ValueError("Compile-only case list differs from the frozen population")

    batch_size = run["batch_size"]
    batches = [(catalog, entries[i:i + batch_size]) for (catalog, _), entries in groups.items()
               for i in range(0, len(entries), batch_size)]
    if len(batches) != run["batches"] or len(list(directory.glob("batch-*.json"))) != len(batches):
        raise ValueError("Runtime batch receipt count differs")
    receipts = hashlib.sha256()
    passed = panics = commands = catalog_cases = 0
    for number, (catalog, entries) in enumerate(batches):
        receipt = directory / f"batch-{number:04}.json"
        batch = json.loads(receipt.read_bytes())
        receipts.update(bytes.fromhex(file_hash(receipt)))
        if batch["ids"] != [key for key, _ in entries]:
            raise ValueError(f"Wrong sources in {receipt}")
        log = directory / batch["log"]
        if file_hash(log) != batch["log_sha256"]:
            raise ValueError(f"Log digest differs at {receipt}")
        text = log.read_text()
        declarations = re.findall(r"^(test[0-9]+_[0-9a-f]{64})\t(.+)$", text, re.M)
        successes = re.findall(r"^test (test[0-9]+_[0-9a-f]{64})( - should panic)? \.\.\. ok$", text, re.M)
        if len(declarations) != len(entries) or len(successes) != len(entries):
            raise ValueError(f"Expected one declared and one passing test per case at {log}")
        declared = dict(declarations)
        outcomes = dict(successes)
        cases_hash = hashlib.sha256()
        for i, (key, source) in enumerate(entries):
            raw = source.read_bytes()
            case = json.loads(raw)
            cases_hash.update(hashlib.sha256(raw).digest())
            test = case_name(i, key)
            panic = case["expected_runtime_error"] is not None
            if declared.get(test) != "#".join(key.rsplit("/", 1)) or test not in outcomes:
                raise ValueError(f"Missing named Rust test for {key} in {log}")
            if bool(outcomes[test]) != panic:
                raise ValueError(f"Runtime error expectation disagrees for {key}")
            passed += 1
            panics += int(panic)
            commands += len(case["commands"])
        if cases_hash.hexdigest() != batch["cases_sha256"]:
            raise ValueError(f"Captured runtime inputs changed at {receipt}")
        if catalog:
            catalog_cases += len(entries)
    if passed != run["executed"] or len(compile_only) != run["compile_only"]:
        raise ValueError("Execution count differs from completion record")
    return {**run, "total_frozen_cases": passed + len(compile_only),
            "input_commands": commands, "expected_runtime_errors": panics,
            "catalog_cases": catalog_cases, "handle_cases": passed - catalog_cases,
            "batch_receipts_sha256": receipts.hexdigest(),
            "environment": json.loads((directory / "environment.json").read_bytes())}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("population", "corpus", "run"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.population, args.corpus, args.run), indent=2))
