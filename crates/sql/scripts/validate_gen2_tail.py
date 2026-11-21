#!/usr/bin/env python3
"""Compare the Rust S3 worker with the public Java Gen-2 artifacts on relational workloads."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile

from run_determinism import file_digest


TABLES = "CREATE TABLE events(k INT NOT NULL, v BIGINT);\nCREATE TABLE labels(k INT NOT NULL, v BIGINT);\n"
QUERIES = {
    "inner_join": "SELECT e.k,e.v,l.v AS label FROM events e JOIN labels l ON e.k=l.k",
    "left_join": "SELECT e.k,e.v,l.v AS label FROM events e LEFT JOIN labels l ON e.k=l.k",
    "anti_join": "SELECT e.k FROM events e WHERE NOT EXISTS (SELECT 1 FROM labels l WHERE e.k=l.k)",
    "except_all": "SELECT k FROM events EXCEPT ALL SELECT k FROM labels",
    "intersection": "SELECT k FROM events INTERSECT SELECT k FROM labels",
    "lag": "SELECT k,LAG(v) OVER(PARTITION BY k ORDER BY v) AS previous FROM events",
    "rolling": "SELECT k,SUM(v) OVER(PARTITION BY k ORDER BY v RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS s FROM events WHERE v IS NOT NULL",
    "top_k": "SELECT k,v FROM (SELECT k,v,ROW_NUMBER() OVER(PARTITION BY k ORDER BY v DESC) AS r FROM events) WHERE r <= 3",
    "grouped_min": "SELECT k,MIN(v) AS m FROM events GROUP BY k",
    "three_way": "SELECT e.k,e.v FROM events e JOIN labels l ON l.k=e.k JOIN events f ON f.k=l.k",
}


def check_forge_reader(reader, program, directory, result):
    replies = {}
    # The pinned example reads the dataflow from a lowered/ subdirectory.
    for side, artifacts in (("java", directory), ("rust", result)):
        with tempfile.TemporaryDirectory(dir=directory) as scratch:
            root = Path(scratch)
            (root / "lowered").mkdir()
            (root / "circuit.json").symlink_to((artifacts / "circuit.json").resolve())
            (root / "lowered/dataflow.json").symlink_to((artifacts / "dataflow.json").resolve())
            process = subprocess.run([reader, root], capture_output=True, text=True)
            replies[side] = {"exit": process.returncode, "out": process.stdout.strip(),
                             "err": process.stderr.strip()}
    if replies["rust"] != replies["java"]:
        raise ValueError(f"Forge reader differs for {program}: {replies}")
    if replies["rust"]["exit"] and not (program == "except_all"
            and "unknown class `DBSPPositiveOperator`" in replies["rust"]["err"]):
        raise ValueError(f"Unexpected Forge rejection for {program}: {replies}")
    return replies


def validate(output, driver, comparator, classpath, forge_reader=None):
    output.mkdir()
    reports = []
    for name, query in QUERIES.items():
        run = output / name
        run.mkdir()
        source = run / "input.sql"
        source.write_text(TABLES + "CREATE VIEW result AS " + query + ";\n")
        java = ["java", "-ea", "-cp", classpath, "org.dbsp.sqlCompiler.CompilerMain", "-i", "--gen2",
                "--dump-producer-commit", "tail-parity", "--dump-runtime-abi", "tail-parity"]
        checkpoint = run / "s3.json"
        subprocess.run([*java, "--noRust", "--write-s3", checkpoint, source], check=True)
        subprocess.run([*java, "--je", "--errors", run / "diagnostics.json", "--js", run / "schema.json",
                        "--dataflow", run / "dataflow.json", "-o", run / "circuit.json", source], check=True)
        subprocess.run([driver, "--s3-worker", "tail-parity", "tail-parity", checkpoint, run / "attempt"], check=True)
        result = run / "attempt/result"
        subprocess.run([comparator, run / "circuit.json", result / "circuit.json"], check=True)
        for artifact in ("schema", "dataflow", "diagnostics"):
            filename = artifact + ".json"
            raw = (run / filename).read_bytes()
            reference = json.loads(raw) if raw.strip() else []
            if reference != json.loads((result / filename).read_bytes()):
                raise ValueError(f"{name}: different {artifact}")
        report = {"case": name, "source_sha256": file_digest(source)}
        if forge_reader:
            report["forge"] = check_forge_reader(forge_reader, name, run, result)
        reports.append(report)
    (output / "complete.json").write_text(json.dumps({"equal": True, "cases": reports}, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", required=True, type=Path)
    parser.add_argument("--comparator", required=True, type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--forge-reader", type=Path, help="pinned check_forge_output example")
    args = parser.parse_args()
    validate(args.output, args.compiler, args.comparator, args.classpath, args.forge_reader)
