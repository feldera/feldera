#!/usr/bin/env python3
"""Verify OptimizerCoverageOracle records against the SQL or S2 population it read."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path

from run_determinism import file_digest


def checked_requests(case, requests):
    if not isinstance(requests, dict):
        raise ValueError(f"Missing optimizer request counts at {case}")
    for operation, counts in requests.items():
        if not isinstance(counts, dict):
            raise ValueError(f"Invalid {operation} request counts at {case}")
        calls, fallbacks = counts.get("calls"), counts.get("fallbacks")
        if type(calls) is not int or type(fallbacks) is not int or not 0 <= fallbacks <= calls or calls == 0:
            raise ValueError(f"Invalid {operation} request counts at {case}")
        yield operation, calls, fallbacks


def summarize(population, observations, checkpoints=None):
    seen = set()
    requests = defaultdict(Counter)
    transfers = defaultdict(Counter)
    fallbacks_by_operation = defaultdict(list)
    with population.open("rb") as inputs, observations.open("rb") as results:
        for source_line, result_line in zip(inputs, results, strict=True):
            source, result = json.loads(source_line), json.loads(result_line)
            case = source["id"]
            if (
                case in seen
                or result["id"] != case
                or result["input_sha256"] != source["input_sha256"]
                or type(result["gen2"]) is not bool
                or result["gen2"] is not source["options"]["ioOptions"]["gen2"]
            ):
                raise ValueError(f"Duplicate or mismatched compiler input at {case}")
            seen.add(case)
            if checkpoints is not None:
                name = hashlib.sha256(source_line.rstrip(b"\r\n")).hexdigest()
                if result.get("s2_sha256") != file_digest(checkpoints / f"{name}.json"):
                    raise ValueError(f"Wrong S2 checkpoint for {case}")
            elif "s2_sha256" in result:
                raise ValueError(f"S2 replay for {case} requires its checkpoint directory")
            for operation, calls, fallbacks in checked_requests(case, result["requests"]):
                requests[operation].update(calls=calls, fallbacks=fallbacks)
                if fallbacks:
                    fallbacks_by_operation[operation].append(case)
            for operator, rules in result["transfers"].items():
                if not rules or any(type(n) is not int or n < 1 for n in rules.values()):
                    raise ValueError(f"Invalid {operator} transfer counts at {case}")
                transfers[operator].update(rules)
    if not seen:
        raise ValueError("Empty optimizer coverage population")
    return {
        "programs": len(seen),
        "population_sha256": file_digest(population),
        "observations_sha256": file_digest(observations),
        "requests": requests,
        "transfer_rules": transfers,
        "programs_with_fallbacks": fallbacks_by_operation,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("population", type=Path)
    parser.add_argument("observations", type=Path)
    parser.add_argument("--checkpoints", type=Path, help="S2 directory passed to OptimizerCoverageOracle")
    args = parser.parse_args()
    print(json.dumps(summarize(args.population, args.observations, args.checkpoints), indent=2, sort_keys=True))
