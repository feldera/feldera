#!/usr/bin/env python3
"""Measure the default Gen-2 binder and lowerer on original SQL submission boundaries."""

import argparse
from collections import Counter
import json
from pathlib import Path
import subprocess

from run_determinism import file_digest


def measure(corpus, output, compiler):
    output.mkdir()
    counts = Counter()
    reasons = Counter()
    profile = json.loads((Path(__file__).resolve().parents[1] / "sql-binder/src/system_catalog.json").read_bytes())
    options = profile["context"]["options"]
    with (corpus.open() as cases, (output / "cases.jsonl").open("x") as records,
          (output / "bound.jsonl").open("xb") as bound,
          (output / "accepted.jsonl").open("x") as accepted):
        for line in cases:
            case = json.loads(line)
            request = {"id": case["id"], "submissions": case["submissions"]}
            result = subprocess.run([compiler, "--bind-submissions", "-"], input=json.dumps(request).encode(),
                                    capture_output=True)
            if result.returncode not in (0, 1) or b"panicked" in result.stderr:
                raise RuntimeError(f"Binder failed for {case['id']}: {result.stderr!r}")
            record = {"id": case["id"], "submissions": len(case["submissions"])}
            counts["cases"] += 1
            if result.returncode == 0:
                program = json.loads(result.stdout)
                if program["id"] != case["id"] or program["context"]["options"] != options:
                    raise ValueError(f"Unexpected profile or program identity for {case['id']}")
                counts["bound"] += 1
                bound.write(result.stdout)
                accepted.write(json.dumps({**case, "options": options}) + "\n")
                lowered = subprocess.run([compiler, "--lower-s1", "submission-audit", "submission-audit", "-"],
                                         input=result.stdout, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                if lowered.returncode not in (0, 1) or b"panicked" in lowered.stderr:
                    raise RuntimeError(f"Lowerer failed for {case['id']}: {lowered.stderr!r}")
                record["rust_s2"] = lowered.returncode == 0
                counts["lowered"] += record["rust_s2"]
                if lowered.returncode:
                    record["lowering_error"] = lowered.stderr.decode().strip().removeprefix("<stdin>: ")
            else:
                message = result.stderr.decode().strip().removeprefix("<stdin>: ")
                record["binding_error"] = message
                reasons[message.partition(":")[0]] += 1
            records.write(json.dumps(record) + "\n")
    report = {"scope": "Default Gen-2 options, original submission boundaries; availability before Java comparison",
              "corpus_sha256": file_digest(corpus), "compiler_sha256": file_digest(compiler),
              **counts, "binder_refusals_by_category": reasons}
    (output / "complete.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("corpus", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", required=True, type=Path)
    args = parser.parse_args()
    measure(args.corpus, args.output, args.compiler)
