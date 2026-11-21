#!/usr/bin/env python3
"""Exercise an overlapping and disconnected aggregate region on a real filter circuit."""

import argparse
import json
from pathlib import Path
import subprocess


def validate(source, output, compiler, classpath):
    output.mkdir()
    record = json.loads(source.read_bytes())
    operators = record["circuit"]["allOperators"]
    table = next(o for o in operators if o.get("tableName", {}).get("name") == "readings")
    projection = next(o for o in operators if o["class"] == "DBSPMapOperator")
    sink = next(o for o in operators if o["class"] == "DBSPSinkOperator" and o["viewName"]["name"] == "hot")
    for op, ids in ((table, [2, 10]), (projection, [10]), (sink, [99])):
        op["annotations"].extend({"class": "GlobalAggregate", "id": i} for i in ids)
    annotated, before, after = (output / name for name in ("input.json", "before.json", "after.json"))
    annotated.write_text(json.dumps(record["circuit"]))
    subprocess.run(["java", "-ea", "-cp", classpath, "org.dbsp.sqlCompiler.compiler.sql.tools.RegionPassOracle",
                    annotated, before, after], check=True)
    expected = json.loads(after.read_bytes())
    actual = json.loads(subprocess.check_output([compiler, "--tag-regions", before]))
    survivors = [a["id"] for op in expected["allOperators"] for a in op["annotations"] if a["class"] == "GlobalAggregate"]
    if actual != expected or survivors != [99]:
        raise ValueError("Region pass failed to preserve the independent sink region or remove both conflicts")
    (output / "complete.json").write_text(json.dumps({"equal": True, "removed": [2, 10], "survived": survivors}) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkpoint", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", type=Path, required=True)
    parser.add_argument("--classpath", required=True)
    args = parser.parse_args()
    validate(args.checkpoint, args.output, args.compiler, args.classpath)
