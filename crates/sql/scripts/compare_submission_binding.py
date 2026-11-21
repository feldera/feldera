#!/usr/bin/env python3
"""Compare Java compilation of the original calls with Java replay of each Rust-bound program."""

import argparse
import json
from pathlib import Path
import subprocess


def compare(audit, output, comparator, classpath):
    output.mkdir()
    bound = audit / "bound.jsonl"
    inputs = output / "direct-inputs.jsonl"
    with (audit / "accepted.jsonl").open() as source, inputs.open("x") as target:
        for line in source:
            case = json.loads(line)
            # Raw SQL must run the optimizer. Detached S1 replay also clears this flag.
            case["options"]["ioOptions"]["inputCircuit"] = False
            target.write(json.dumps(case) + "\n")
    for side, flags in (("direct", ["capture"]), ("bound", ["replay-s1", bound])):
        subprocess.run(["java", "-ea", "-Xmx1g", "-Dfeldera.determinism.seamArtifacts=true",
                        "-Dfeldera.determinism.producer=submission-audit",
                        "-Dfeldera.determinism.runtime=submission-audit",
                        "-cp", classpath, "org.dbsp.sqlCompiler.compiler.sql.tools.CorpusDeterminism",
                        inputs, output / side, comparator, output / (side + "-artifacts"), *flags], check=True)
    count = 0
    mismatches = []
    with ((output / "direct/cases.jsonl").open() as left, (output / "bound/cases.jsonl").open() as right,
          inputs.open() as original):
        for direct, replay, source in zip(left, right, original, strict=True):
            a, b, sql = map(json.loads, (direct, replay, source))
            if a["id"] != b["id"] or a["id"] != sql["id"]:
                raise ValueError("Case identities differ")
            count += 1
            fields = [k for k in sorted(a.keys() | b.keys()) if a.get(k) != b.get(k)]
            if fields:
                mismatches.append({"id": a["id"], "different": fields})
    if not count or count != json.loads((audit / "complete.json").read_bytes())["bound"]:
        raise ValueError("Incomplete subset comparison")
    report = {"scope": "Original submission boundaries, default Gen-2 profile; all captured downstream observations",
              "cases": count, "matched": count-len(mismatches), "mismatches": mismatches}
    (output / "complete.json").write_text(json.dumps(report, indent=2) + "\n")
    return not mismatches


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("audit", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--comparator", type=Path, required=True)
    args = parser.parse_args()
    raise SystemExit(0 if compare(args.audit, args.output, args.comparator, args.classpath) else 1)
