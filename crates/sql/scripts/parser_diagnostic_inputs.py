#!/usr/bin/env python3
"""Prepare raw-parser inputs from completed negative-test diagnostic captures."""

import argparse
import json
from pathlib import Path

from compare_parsers import digest


def prepare(directory):
    manifest = json.loads((directory / "complete.json").read_text())
    if (
        manifest.get("schema_version") != 1
        or manifest.get("junit_successful") is not True
    ):
        raise ValueError("A successful version-1 diagnostic capture is required")
    rows = [
        json.loads(line)
        for line in (directory / "java-diagnostics.jsonl").read_text().splitlines()
    ]
    if (
        not rows
        or type(manifest.get("records")) is not int
        or manifest["records"] != len(rows)
    ):
        raise ValueError("Diagnostic record count disagrees with completion")
    cases, skipped = [], []
    for index, row in enumerate(rows):
        if row.get("schema_version") != 1 or not isinstance(
            row.get("submissions"), list
        ):
            raise ValueError("Diagnostic capture lacks original SQL submissions")
        if not row.get("source_test") or not row.get("call_site"):
            raise ValueError("Diagnostic capture lacks test identity")
        identity = f"diagnostic/{index}/{row['source_test']}"
        for submission in row["submissions"]:
            if (
                not isinstance(submission.get("sql"), str)
                or type(submission.get("many")) is not bool
                or type(submission.get("batch")) is not int
                or submission["batch"] < 0
            ):
                raise ValueError("Malformed diagnostic SQL submission")
        if not row["submissions"]:
            skipped.append(
                {
                    "id": identity,
                    "call_site": row["call_site"],
                    "reason": "No visible API submissions captured",
                }
            )
            continue
        cases.append({"id": identity, "submissions": row["submissions"]})
    if not cases:
        raise ValueError("Diagnostic capture contains no parser inputs")
    return cases, {
        "schema_version": 1,
        "scope": "captured diagnostic assertions; raw parser runs use their fixed dialect configuration",
        "records": len(rows),
        "cases": len(cases),
        "skipped": skipped,
        "capture": manifest,
        "capture_sha256": digest(directory / "complete.json"),
        "records_sha256": digest(directory / "java-diagnostics.jsonl"),
        "preparer_sha256": digest(Path(__file__)),
        "original_compiler_diagnostics_compared": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    cases, manifest = prepare(args.capture)
    args.output.mkdir()
    inputs = args.output / "input.jsonl"
    with inputs.open("x") as output:
        for case in cases:
            output.write(json.dumps(case, ensure_ascii=False) + "\n")
    manifest["input_sha256"] = digest(inputs)
    (args.output / "preparation.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
