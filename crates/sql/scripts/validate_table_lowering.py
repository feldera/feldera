#!/usr/bin/env python3
"""Compare Rust table lowering with direct Java S2 and resume it through Java."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from run_determinism import file_digest

CASES = {
    "nullable": "CREATE TABLE abc (n INT NOT NULL, label VARCHAR);",
    "scalars": "CREATE TABLE scalar_types (a DECIMAL(14,2) NOT NULL, b TIMESTAMP, d DOUBLE, "
               "e INT UNSIGNED, f BOOLEAN);",
    "widths": "CREATE TABLE numbers (field1 INT, field2 BIGINT NOT NULL, c BOOLEAN);",
    "multiple": "CREATE TABLE x (a SMALLINT, b INT UNSIGNED);\nCREATE TABLE y (a TINYINT NOT NULL);",
}


def compare_checkpoints(directory, golden, actual, comparator):
    graphs = []
    envelopes = []
    for checkpoint in (golden, actual):
        envelope = json.loads(checkpoint.read_bytes())
        graph = directory / (checkpoint.stem + "-graph.json")
        graph.write_text(json.dumps(envelope.pop("circuit")))
        graphs.append(graph)
        envelopes.append(envelope)
    if envelopes[0] != envelopes[1]:
        raise ValueError(f"{directory.name}: S2 compiler state differs")
    subprocess.run([comparator, *graphs], check=True)



def validate(output, compiler, comparator, jar):
    output.mkdir()
    records = []
    for name, sql in CASES.items():
        directory = output / name
        directory.mkdir()
        source = directory / "input.sql"
        source.write_text(sql)
        producer, runtime = "sql-lower-comparison", "local-abi"
        flags = ["java", "-ea", "-jar", jar, "--gen2", "--dump-producer-commit", producer,
                 "--dump-runtime-abi", runtime]
        golden = directory / "java.s2"
        actual = directory / "rust.s2"
        with actual.open("xb") as stream:
            subprocess.run([compiler, "--emit-s2", producer, runtime, source], stdout=stream, check=True)
        subprocess.run([*flags, "--input_circuit", "--noRust", "--write-s2", golden, source], check=True)
        compare_checkpoints(directory, golden, actual, comparator)
        for label, tail in (("direct", [source]), ("resumed", ["--read-s2", actual])):
            target = directory / label
            target.mkdir()
            subprocess.run([*flags, "--errors", target / "diagnostics.txt", "--js",
                            target / "schema.json", "--dataflow", target / "dataflow.json",
                            "-o", target / "circuit.json", *tail], check=True)
        direct, resumed = directory / "direct", directory / "resumed"
        for artifact in ("schema.json", "dataflow.json"):
            if json.loads((direct / artifact).read_bytes()) != json.loads((resumed / artifact).read_bytes()):
                raise ValueError(f"{name}: downstream {artifact} differs")
        if (direct / "diagnostics.txt").read_bytes() != (resumed / "diagnostics.txt").read_bytes():
            raise ValueError(f"{name}: downstream diagnostics differ")
        subprocess.run([comparator, direct / "circuit.json", resumed / "circuit.json"], check=True)
        records.append({"case": name, "source_sha256": hashlib.sha256(sql.encode()).hexdigest(),
                        "java_s2_sha256": file_digest(golden), "rust_s2_sha256": file_digest(actual)})
    (output / "complete.json").write_text(json.dumps({
        "equal": True, "cases": records, "driver_sha256": file_digest(compiler),
        "jar_sha256": file_digest(jar),
    }, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", type=Path, required=True)
    parser.add_argument("--comparator", type=Path, required=True)
    parser.add_argument("--jar", type=Path, required=True)
    args = parser.parse_args()
    validate(args.output, args.compiler, args.comparator, args.jar)
