"""Summarize actual seam forms and select coverage cases before replay."""

import argparse
import collections
import json
from pathlib import Path

from run_determinism import file_digest


def summarize(requests, observations, stage):
    if not requests or len(requests) != len(observations):
        raise ValueError("Missing, extra, or empty seam inventory")
    instances = collections.Counter()
    cases = collections.Counter()
    declarations = collections.Counter()
    owners = {}
    features = {}
    failures = []
    for request, observation in zip(requests, observations):
        identity = request["id"]
        if (
            identity in features
            or observation.get("id") != identity
            or observation.get("stage") != stage
        ):
            raise ValueError("Duplicate/mismatched seam identity or stage")
        if (
            type(observation.get("accepted")) is not bool
            or type(observation.get("compiler_bug")) is not bool
        ):
            raise ValueError("Missing seam compilation verdict")
        if not observation["accepted"] or observation["compiler_bug"]:
            failures.append(identity)
        for key in ("operators", "declarations"):
            if not isinstance(observation.get(key), dict):
                raise TypeError("Missing seam form counts")
            if any(type(n) is not int or n <= 0 for n in observation[key].values()):
                raise ValueError("Invalid seam form count")
        instances.update(observation["operators"])
        cases.update(observation["operators"].keys())
        declarations.update(observation["declarations"])
        features[identity] = set(observation["operators"]) | set(
            observation["declarations"]
        )
        if set(observation.get("decoder_owners", {})) != features[identity]:
            raise ValueError("Missing decoder inventory")
        for kind, owner in observation["decoder_owners"].items():
            if kind in owners and owners[kind] != owner:
                raise ValueError("Decoder ownership changed during inventory")
            owners[kind] = owner
    uncovered = set().union(*features.values())
    selected = []
    while uncovered:
        best = min(features, key=lambda k: (-len(features[k] & uncovered), k))
        selected.append(best)
        uncovered -= features[best]
    selected = [r["id"] for r in requests if r["id"] in selected]
    return {
        "stage": stage,
        "cases": len(requests),
        "failures": failures,
        "operator_instances": dict(sorted(instances.items())),
        "operator_case_counts": dict(sorted(cases.items())),
        "declaration_instances": dict(sorted(declarations.items())),
        "decoder_owners": dict(sorted(owners.items())),
        "cover_case_ids": selected,
        "selection": "greedy uncovered-form count; ties by case id; original input order",
        "covers_all_observed_forms": True,
        "replay_verified": False,
        "scope": "top-level operators and declarations; nested operator contents require replay validation",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("observations", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--stage", choices=("S2", "S3"), required=True)
    parser.add_argument("--selected", type=Path, required=True)
    args = parser.parse_args()
    requests = [json.loads(line) for line in args.input.read_text().splitlines()]
    observations = [
        json.loads(line) for line in args.observations.read_text().splitlines()
    ]
    result = summarize(requests, observations, args.stage)
    result["input_sha256"] = file_digest(args.input)
    result["observations_sha256"] = file_digest(args.observations)
    with args.output.open("x") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    with args.selected.open("x") as output:
        for request in requests:
            if request["id"] in result["cover_case_ids"]:
                output.write(json.dumps(request) + "\n")
    return 1 if result["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
