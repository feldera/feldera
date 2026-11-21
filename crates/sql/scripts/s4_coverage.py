#!/usr/bin/env python3
"""Audit executed S4 paths against completed direct/replay fingerprints."""

import argparse
import itertools
import json
from pathlib import Path

from determinism import open_cases
from run_determinism import file_digest


def integer(value):
    return type(value) is int and value >= 0


def point_key(point):
    if set(point) != {"path", "observations", "largest_iteration"}:
        raise ValueError("Unexpected S4 point fields")
    path = point["path"]
    if not isinstance(path, list) or not path:
        raise ValueError("Missing S4 path")
    for frame in path:
        if (
            not isinstance(frame, dict)
            or set(frame) != {"container", "pass", "index"}
            or any(
                not isinstance(frame[key], str) or not frame[key].strip()
                for key in ("container", "pass")
            )
            or not integer(frame["index"])
        ):
            raise ValueError("Malformed S4 path frame")
    if (
        not integer(point["observations"])
        or point["observations"] == 0
        or not integer(point["largest_iteration"])
    ):
        raise ValueError("Invalid S4 point counters")
    return json.dumps(path, sort_keys=True, separators=(",", ":"))


def records(directory):
    manifest = json.loads((directory / "complete.json").read_text())
    if (
        manifest["schema_version"] != 1
        or manifest["failed_cases"] != 0
        or manifest["observation_contract"] != "full-prefix-v1"
        or manifest["capture"] is not True
        or manifest["cases"] <= 0
    ):
        raise ValueError("Successful full-prefix evidence required")
    seen = set()
    with (
        open_cases(directory, manifest) as cases,
        (directory / "s4-paths.jsonl").open() as paths,
    ):
        for case_line, path_line in itertools.zip_longest(cases, paths):
            if case_line is None or path_line is None:
                raise ValueError("Missing case or S4 sidecar record")
            case, coverage = json.loads(case_line), json.loads(path_line)
            identity = case["id"]
            if (
                not isinstance(identity, str)
                or not identity
                or identity in seen
                or coverage.get("id") != identity
            ):
                raise ValueError("Missing, duplicate or reordered S4 case identity")
            seen.add(identity)
            if set(coverage) != {"id", "observations", "points"}:
                raise ValueError("Unexpected S4 coverage fields")
            points = coverage["points"]
            if not isinstance(points, list) or not points:
                raise ValueError("Missing S4 points")
            keys = [point_key(point) for point in points]
            if len(set(keys)) != len(keys):
                raise ValueError("Duplicate S4 path")
            total = coverage["observations"]
            if (
                not integer(total)
                or total != sum(point["observations"] for point in points)
                or total != case["observations"]["S4"]["count"]
            ):
                raise ValueError("S4 counts disagree with fingerprint")
            yield case, coverage, dict(zip(keys, points, strict=True))
    if len(seen) != manifest["cases"]:
        raise ValueError("S4 case count disagrees with completion manifest")


def audit(direct, replay):
    points, cases, observations = {}, 0, 0
    contracts = [
        json.loads((directory / "complete.json").read_text()).get("artifact_contract")
        for directory in (direct, replay)
    ]
    if contracts[0] != contracts[1]:
        raise ValueError("Direct/replay artifact contracts differ")
    byte_differences = []
    for left, right in itertools.zip_longest(records(direct), records(replay)):
        if left is None or right is None:
            raise ValueError("Direct/replay coverage lengths differ")
        a, coverage, paths = left
        b, other, other_paths = right
        if contracts[0] == "seam-v1":
            # The existing seam contract compares semantic Gen-2 artifacts;
            # byte fingerprints remain evidence, not the equality criterion.
            raw_a, raw_b = a.pop("gen2_bytes", None), b.pop("gen2_bytes", None)
            if (raw_a is None) != (raw_b is None):
                raise ValueError("Missing Gen-2 byte fingerprints")
            if raw_a is not None and (
                not isinstance(raw_a, dict)
                or not raw_a
                or not isinstance(raw_b, dict)
                or not raw_b
                or not a.get("gen2")
                or not b.get("gen2")
            ):
                raise ValueError("Incomplete Gen-2 artifact fingerprints")
            if raw_a != raw_b:
                byte_differences.append(a["id"])
        if a != b or coverage != other or paths != other_paths:
            raise ValueError("Direct/replay records or S4 coverage differ")
        cases += 1
        observations += coverage["observations"]
        for key, point in paths.items():
            entry = points.setdefault(
                key,
                {
                    "path": point["path"],
                    "observations": 0,
                    "cases": 0,
                    "largest_iteration": 0,
                    "examples": [],
                },
            )
            entry["observations"] += point["observations"]
            entry["cases"] += 1
            entry["largest_iteration"] = max(
                entry["largest_iteration"], point["largest_iteration"]
            )
            if len(entry["examples"]) < 8:
                entry["examples"].append(a["id"])
    artifacts = {}
    for directory in (direct, replay):
        manifest = json.loads((directory / "complete.json").read_text())
        for name in (
            "complete.json",
            "s4-paths.jsonl",
            manifest.get("records_file", "cases.jsonl"),
        ):
            path = directory / name
            artifacts[str(path)] = file_digest(path)
    return {
        "schema_version": 1,
        "complete": True,
        "cases": cases,
        "S4_observations_per_side": observations,
        "unique_paths": len(points),
        "points": [points[key] for key in sorted(points)],
        "artifacts": artifacts,
        "artifact_byte_differences": byte_differences,
        "runtime_executed": False,
        "field_level_state_coverage_proven": False,
        "all_conditional_branches_covered": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("direct", type=Path)
    parser.add_argument("replay", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    report = audit(args.direct, args.replay)
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(
        json.dumps(
            {
                key: report[key]
                for key in ("cases", "S4_observations_per_side", "unique_paths")
            }
        )
    )


if __name__ == "__main__":
    main()
