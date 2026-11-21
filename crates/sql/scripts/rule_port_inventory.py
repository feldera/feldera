#!/usr/bin/env python3
"""Reconcile measured optimizer rules with runtime classes and pinned sources."""

import argparse
import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def reconcile(inventory, census):
    if inventory["schema_version"] != 1 or census["schema_version"] != 1:
        raise ValueError("unsupported optimizer evidence version")
    implementations = defaultdict(set)
    placements = defaultdict(set)
    direct = {}
    for step in inventory["steps"]:
        if step["kind"] == "direct":
            direct[step["index"]] = {
                key: step[key] for key in ("index", "step", "implementation")
            }
        for rule in step["rules"]:
            name = rule["description"]
            implementations[name].add(rule["implementation"])
            placements[name].add(
                (step["optimization_level"], step["index"], step["step"])
            )
    configured = {name for step in census["steps"] for name in step["configured_rules"]}
    if not configured or len(configured) != census["configured_rules"]:
        raise ValueError("configured-rule count disagrees with census steps")
    if configured != set(implementations):
        raise ValueError(
            "inventory and census differ: "
            f"missing implementations={sorted(configured - implementations.keys())}; "
            f"unmeasured rules={sorted(implementations.keys() - configured)}"
        )
    productions = Counter()
    attempts = Counter()
    for step in census["steps"]:
        productions.update(step["productions"])
        attempts.update(step["attempts"])
    if dict(productions) != census["productions"]:
        raise ValueError("per-step productions disagree with census totals")
    if set(productions) - configured or set(attempts) - configured:
        raise ValueError("measurements contain an unconfigured rule")
    rows = []
    for name in sorted(configured):
        if len(implementations[name]) != 1:
            raise ValueError(f"ambiguous implementation for {name}")
        rows.append(
            {
                "description": name,
                "implementation": next(iter(implementations[name])),
                "attempts": attempts[name],
                "productions": productions[name],
                "placements": [
                    {"optimization_level": level, "step_index": index, "step": step}
                    for level, index, step in sorted(placements[name])
                ],
                "proposed_disposition": "retain_behavior_in_initial_parity_scope",
                "needs_targeted_production_test": productions[name] == 0,
            }
        )
    return rows, [direct[index] for index in sorted(direct)]


def source(implementation, repo, calcite):
    top_level = implementation.split("$", 1)[0]
    relative = Path(top_level.replace(".", "/") + ".java")
    if top_level.startswith("org.apache.calcite."):
        owner = "calcite"
        root = calcite
        path = root / "core/src/main/java" / relative
    elif top_level.startswith("org.dbsp."):
        owner = "feldera"
        root = repo
        path = root / "sql-to-dbsp-compiler/SQL-compiler/src/main/java" / relative
    else:
        raise ValueError(f"unknown rule implementation owner: {implementation}")
    if not path.is_file():
        raise ValueError(f"missing implementation source: {path}")
    return {
        "owner": owner,
        "path": str(path.relative_to(root)),
        "sha256": digest(path),
        "has_asf_header": "Licensed to the Apache Software Foundation"
        in path.read_text(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    parser.add_argument("census", type=Path)
    parser.add_argument("calcite", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parents[3]
    )
    args = parser.parse_args()
    rows, direct = reconcile(
        json.loads(args.inventory.read_text()), json.loads(args.census.read_text())
    )
    for row in rows + direct:
        row["source"] = source(row["implementation"], args.repo, args.calcite)
    report = {
        "schema_version": 1,
        "reviewed": False,
        "rule_descriptions": len(rows),
        "implementation_classes": len({row["implementation"] for row in rows}),
        "descriptions_without_productions": sum(
            row["productions"] == 0 for row in rows
        ),
        "description_counts_by_source_owner": dict(
            Counter(row["source"]["owner"] for row in rows)
        ),
        "scope": "Retain every configured rule and direct step. Zero productions do not justify removal. Translation versus rewrite remains a separate decision.",
        "identity_limit": "Descriptions can cover multiple configurations of one rule. The raw inventory preserves ordered instances per step, level, and shape; this summary does not equate those configurations.",
        "calcite_commit": subprocess.run(
            ["git", "-C", str(args.calcite), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
        "inputs": {str(path): digest(path) for path in (args.inventory, args.census)},
        "rules": rows,
        "direct_steps": direct,
    }
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "rule_descriptions",
                    "implementation_classes",
                    "descriptions_without_productions",
                    "description_counts_by_source_owner",
                    "reviewed",
                )
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
