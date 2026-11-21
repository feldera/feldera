#!/usr/bin/env python3
"""Attach source-pinned stage purposes to every configured optimizer rule."""

import argparse
import collections
import hashlib
import json
from pathlib import Path

from rule_port_inventory import reconcile


def classify(contracts, inventory, census):
    if contracts["schema_version"] != 1:
        raise ValueError("unsupported stage-contract version")
    steps = {row["index"]: row for row in contracts["steps"]}
    if len(steps) != len(contracts["steps"]):
        raise ValueError("duplicate stage index")
    actual = set()
    for step in inventory["steps"]:
        index = step["index"]
        actual.add(index)
        if index not in steps:
            raise ValueError(f"stage has no contract: {index}")
        contract = steps[index]
        for field in ["step", "kind", "abort_on_error"]:
            if type(step[field]) is not type(contract[field]) or step[field] != contract[field]:
                raise ValueError(f"stage {index} changed {field}")
        if not contract["family"] or not contract["contract"]:
            raise ValueError("empty stage contract")
    if actual != set(steps):
        raise ValueError("stage contracts contain unconfigured stages")
    for index, step in steps.items():
        other = step["shared_instance_with"]
        if other is not None and (other == index or other not in steps
                                  or steps[other]["shared_instance_with"] != index
                                  or steps[other]["step"] != step["step"]):
            raise ValueError("inconsistent shared step-instance contract")
    rules, direct = reconcile(inventory, census)
    families = collections.Counter()
    for rule in rules:
        rule["stage_families"] = sorted({steps[placement["step_index"]]["family"] for placement in rule["placements"]})
        rule["contract_indices"] = sorted({placement["step_index"] for placement in rule["placements"]})
        families.update(rule["stage_families"])
    for step in direct:
        step["contract"] = steps[step["index"]]
    return {"schema_version": 1, "complete": True, "scope": contracts["scope"],
            "stage_count": len(steps), "rule_descriptions": len(rules),
            "implementation_classes": len({rule["implementation"] for rule in rules}),
            "rule_counts_by_stage_family": dict(sorted(families.items())),
            "family_counts_overlap": True,
            "rules": rules, "direct_steps": direct, "stage_contracts": contracts,
            "maintainer_review_complete": False, "translation_mode_decided": False, "P0_complete": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contracts", type=Path)
    parser.add_argument("inventory", type=Path)
    parser.add_argument("census", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args()
    contract = json.loads(args.contracts.read_text())
    source = (args.repo / contract["source"]["path"]).resolve()
    source.relative_to(args.repo.resolve())
    if hashlib.sha256(source.read_bytes()).hexdigest() != contract["source"]["sha256"]:
        raise ValueError("optimizer construction source changed; review stage contracts")
    report = classify(contract, json.loads(args.inventory.read_text()), json.loads(args.census.read_text()))
    report["inputs"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in [args.contracts, args.inventory, args.census]}
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({key: report[key] for key in ["stage_count", "rule_descriptions", "implementation_classes", "rule_counts_by_stage_family"]}))


if __name__ == "__main__":
    main()
