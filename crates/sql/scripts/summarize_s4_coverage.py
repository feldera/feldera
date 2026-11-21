#!/usr/bin/env python3
"""Combine strict pass-path audits over an exact prepared replay population."""

import argparse
import json
from pathlib import Path

from s4_coverage import audit
from summarize_seam_corpus import summarize


def summarize_paths(inputs, shards):
    replay = summarize(inputs, shards)
    if replay["stage"] != "S2":
        raise ValueError("S4 coverage requires S2 prefix replay")
    points, reports = {}, []
    for shard in shards:
        checked = audit(shard / "capture", shard / "replay-s2")
        reports.append({"directory": str(shard), **checked})
        for point in checked["points"]:
            key = json.dumps(point["path"], sort_keys=True, separators=(",", ":"))
            if key not in points:
                points[key] = {**point, "examples": list(point["examples"])}
                continue
            merged = points[key]
            merged["cases"] += point["cases"]
            merged["observations"] += point["observations"]
            merged["largest_iteration"] = max(
                merged["largest_iteration"], point["largest_iteration"]
            )
            merged["examples"] = (merged["examples"] + point["examples"])[:8]
    if sum(report["cases"] for report in reports) != replay["cases"]:
        raise ValueError("Coverage and replay populations disagree")
    return {
        "schema_version": 1,
        "complete": True,
        "cases": replay["cases"],
        "S4_observations_per_side": sum(
            report["S4_observations_per_side"] for report in reports
        ),
        "unique_paths": len(points),
        "points": [points[key] for key in sorted(points)],
        "replay": replay,
        "shard_coverage_artifacts": [report["artifacts"] for report in reports],
        "artifact_byte_difference_cases": sum(
            len(report["artifact_byte_differences"]) for report in reports
        ),
        "runtime_executed": False,
        "field_level_state_coverage_proven": False,
        "all_conditional_branches_covered": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, nargs="+", required=True)
    parser.add_argument("--shards", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = summarize_paths(args.inputs, args.shards)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(
        json.dumps(
            {
                key: result[key]
                for key in ("cases", "S4_observations_per_side", "unique_paths")
            }
        )
    )


if __name__ == "__main__":
    main()
