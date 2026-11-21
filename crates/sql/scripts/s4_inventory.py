#!/usr/bin/env python3
"""Link executed S4 paths to pinned composite-state source contracts."""

import argparse
import json
from pathlib import Path

from run_determinism import file_digest


def matches(frame, selector):
    return all(frame.get(key) == value for key, value in selector.items())


def evaluate(row, points):
    selected = [
        point
        for point in points
        if any(
            matches(frame, selector)
            for frame in point["path"]
            for selector in row["selectors"]
        )
    ]
    requirements = []
    for requirement in row["required_points"]:
        selector = {key: requirement[key] for key in ("container", "pass", "index")}
        observed = [
            point
            for point in selected
            if any(matches(frame, selector) for frame in point["path"])
        ]
        requirements.append(
            {
                **requirement,
                "observed": bool(observed),
                "matching_paths": len(observed),
                "examples": sorted(
                    {case for point in observed for case in point["examples"]}
                )[:8],
            }
        )
    return {
        **row,
        "matching_paths": len(selected),
        "matching_path_case_pairs": sum(point["cases"] for point in selected),
        "observations": sum(point["observations"] for point in selected),
        "required_points": requirements,
        "examples": sorted({case for point in selected for case in point["examples"]})[
            :8
        ],
        "state_contract_verified": False,
    }


def constructed_coverage(construction, observed, nonreturning=()):
    expected = {}
    for index, configuration in enumerate(construction["configurations"]):
        for point in configuration["points"]:
            key = json.dumps(point["path"], sort_keys=True, separators=(",", ":"))
            entry = expected.setdefault(
                key,
                {
                    "path": point["path"],
                    "configurations": [],
                    "implementations": set(),
                },
            )
            entry["configurations"].append(index)
            entry["implementations"].add(point["implementation"])
    actual = {
        json.dumps(point["path"], sort_keys=True, separators=(",", ":"))
        for point in observed
    }
    nonreturning = {entry["implementation"] for entry in nonreturning}
    unobserved = []
    for key, entry in sorted(expected.items()):
        if key in actual:
            continue
        always_throws = (
            bool(entry["implementations"]) and entry["implementations"] <= nonreturning
        )
        unobserved.append(
            {
                **entry,
                "implementations": sorted(entry["implementations"]),
                "always_throws": always_throws,
                "requires_activation": not always_throws,
            }
        )
    return {
        "configurations": len(construction["configurations"]),
        "unique_constructed_paths": len(expected),
        "observed_constructed_paths": len(expected.keys() & actual),
        "unobserved_constructed_paths": unobserved,
        "unobserved_paths_requiring_activation": sum(
            entry["requires_activation"] for entry in unobserved
        ),
        "other_observed_paths": len(actual - expected.keys()),
        "scope": "constructed Passes/Repeat/Conditional trees under the recorded options and default metadata",
        "manual_composites_included": False,
        "all_possible_configurations_enumerated": False,
    }


def link(inventory_path, coverage_path):
    inventory = json.loads(inventory_path.read_text())
    coverage = json.loads(coverage_path.read_text())
    if (
        inventory["schema_version"] != 1
        or coverage["schema_version"] != 1
        or coverage["complete"] is not True
    ):
        raise ValueError("Version-1 inventory and completed path audit required")
    for row in inventory["rows"] + inventory.get("nonreturning_implementations", []):
        for source in row["sources"]:
            if file_digest(Path(source["path"])) != source["sha256"]:
                raise ValueError(f"Inventory source changed: {source['path']}")
    rows = [evaluate(row, coverage["points"]) for row in inventory["rows"]]
    missing = [
        {"sources": row["sources"], "point": point}
        for row in rows
        for point in row["required_points"]
        if not point["observed"]
    ]
    return {
        "schema_version": 1,
        "complete": True,
        "scope": inventory["scope"],
        "cases": coverage["cases"],
        "inventory": {
            "path": str(inventory_path),
            "sha256": file_digest(inventory_path),
        },
        "coverage": {"path": str(coverage_path), "sha256": file_digest(coverage_path)},
        "rows": rows,
        "unobserved_required_points": missing,
        "exhaustive_per_point_inventory": inventory["exhaustive_per_point_inventory"],
        "independent_review_complete": False,
        "field_level_state_coverage_proven": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    parser.add_argument("coverage", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--constructed", type=Path)
    args = parser.parse_args()
    report = link(args.inventory, args.coverage)
    if args.constructed:
        construction = json.loads(args.constructed.read_text())
        coverage = json.loads(args.coverage.read_text())
        inventory = json.loads(args.inventory.read_text())
        report["construction"] = constructed_coverage(
            construction,
            coverage["points"],
            inventory.get("nonreturning_implementations", []),
        )
        report["construction"]["source"] = {
            "path": str(args.constructed),
            "sha256": file_digest(args.constructed),
        }
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(
        json.dumps(
            {
                "rows": len(report["rows"]),
                "unobserved_required_points": len(report["unobserved_required_points"]),
            }
        )
    )


if __name__ == "__main__":
    main()
