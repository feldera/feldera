#!/usr/bin/env python3
"""Compare direct compilation with S2 or S3 file replay in fresh JVMs."""

import argparse
import json
import os
import subprocess
import threading
from pathlib import Path

from run_determinism import OutputBudget, classpath_digest, file_digest, run_process

PROGRAMS = {
    "constant": "CREATE VIEW v AS SELECT 1 AS result;",
    "projection": """
        CREATE TABLE t(id INT NOT NULL, value INT DEFAULT 7);
        CREATE VIEW v AS SELECT id FROM t WHERE id > 0;
    """,
    "decimal": """
        CREATE TABLE t(id INT NOT NULL PRIMARY KEY, value DECIMAL(12,2) DEFAULT 7.25);
        CREATE VIEW v AS SELECT id, value * 2 AS doubled FROM t WHERE value > 0;
    """,
    "settings": """
        SET FELDERA_AVOID_STAR_JOINS = true;
        SET custom_setting = 'retained warning';
        CREATE TABLE t(x VARCHAR);
        CREATE VIEW v AS SELECT x || '!' AS x FROM t;
    """,
    "union": """
        CREATE TABLE t(x INT);
        CREATE TABLE u(x INT);
        CREATE VIEW v AS SELECT x FROM t UNION ALL SELECT x FROM u;
    """,
    "nested": """
        CREATE TYPE nested AS ("Field" INT);
        CREATE TABLE t(x nested);
        CREATE VIEW v AS SELECT x FROM t;
    """,
    "provenance": """
        CREATE TABLE t(id INT NOT NULL, value INT);
        CREATE VIEW a AS SELECT id, value + 1 AS value FROM t WHERE value > 0;
        CREATE VIEW b AS SELECT id FROM a WHERE value < 10;
        CREATE VIEW c AS SELECT DISTINCT id FROM t;
        CREATE VIEW d AS SELECT id FROM b UNION ALL SELECT id FROM c;
    """,
    "lateness": """
        CREATE TABLE t(id INT NOT NULL, ts TIMESTAMP LATENESS INTERVAL 1 SECOND);
        CREATE VIEW v AS SELECT id, ts FROM t;
    """,
}


def run_case(args, directory, mode, sql, budget):
    seam = directory / f"{args.stage.lower()}.json"
    source = directory / "input.sql"
    source.write_text(sql)
    base = [
        "java",
        "-ea",
        "-Xmx1g",
        "-cp",
        args.classpath,
        "org.dbsp.sqlCompiler.CompilerMain",
        "-i",
        "--je",
        "--streaming",
        "--correlatedColumns",
        "--dump-producer-commit",
        args.producer,
        "--dump-runtime-abi",
        args.runtime,
    ]
    if mode == "gen2":
        base.append("--gen2")
    cancelled = threading.Event()

    def execute(command, label):
        with (directory / f"{label}.log").open("w") as log:
            run_process(command, directory, log, 120, cancelled, budget)

    for stage in ("direct", "export", "resumed"):
        command = base + ["--errors", str(directory / f"{stage}.messages.json")]
        if stage == "export":
            command += [
                "--noRust",
                f"--write-{args.stage.lower()}",
                str(seam),
                str(source),
            ]
            if args.stage == "S2":
                command.insert(-1, "--input_circuit")
        else:
            artifacts = directory / f"{stage}-output"
            artifacts.mkdir()
            command += [
                "--dump-circuits",
                str(directory / stage),
                "-o",
                str(
                    artifacts / ("program.rs" if mode == "classic" else "circuit.json")
                ),
                "--js",
                str(artifacts / "schema.json"),
                "--dataflow",
                str(artifacts / "dataflow.json"),
                "--plan",
                str(artifacts / "plan.json"),
            ]
            command += (
                [f"--read-{args.stage.lower()}", str(seam)]
                if stage == "resumed"
                else [str(source)]
            )
        execute(command, stage)

        if stage == "export" and args.through_rust:
            rust_seam = directory / f"rust-{seam.name}"
            with rust_seam.open("xb") as output, (directory / "rust.log").open("w") as errors:
                subprocess.run(
                    [args.comparator, "--roundtrip-circuit", seam],
                    cwd=directory, stdout=output, stderr=errors, check=True, timeout=120,
                )
            seam = rust_seam

    direct = directory / "direct/compilation-0000"
    resumed = directory / "resumed/compilation-0000"
    if args.stage == "S2":
        execute(
            [str(args.comparator), "--directories", str(direct), str(resumed)],
            "comparison",
        )
    else:
        execute(
            [
                str(args.comparator),
                "--final-s3",
                str(next(direct.glob("*-S3.json"))),
                str(next(resumed.glob("*-S3.json"))),
            ],
            "comparison",
        )
        execute(
            [str(args.comparator), "--directories", str(resumed), str(resumed)],
            "capture-validation",
        )
    compared = ["schema.json"]
    if mode == "classic":
        compared += ["program.rs", "stubs.rs"]
    artifacts = {
        name: (directory / "direct-output" / name).read_bytes()
        == (directory / "resumed-output" / name).read_bytes()
        for name in compared
    }
    if mode == "gen2":
        execute(
            [str(args.comparator), str(directory / "direct-output/circuit.json"),
             str(directory / "resumed-output/circuit.json")],
            "gen2-artifact-comparison",
        )
    semantic_artifacts = {
        name: json.loads((directory / "direct-output" / name).read_text())
        == json.loads((directory / "resumed-output" / name).read_text())
        for name in ("dataflow.json", "plan.json")
    }
    if mode == "gen2" and args.stage == "S3" and args.compiler:
        public = directory / "rust-gen2.json"
        with public.open("xb") as output:
            subprocess.run([args.compiler, "--s3-to-gen2", seam], stdout=output,
                           check=True, timeout=120)
        execute([str(args.comparator), str(directory / "direct-output/circuit.json"), str(public)],
                "rust-gen2-comparison")
        semantic_artifacts["rust-gen2.json"] = True
    if args.stage == "S3" and args.compiler:
        mir = subprocess.run([args.compiler, "--s3-to-dataflow", seam], check=True,
                             stdout=subprocess.PIPE, timeout=120)
        semantic_artifacts["rust-dataflow.json"] = (
            json.loads(mir.stdout) == json.loads((directory / "direct-output/dataflow.json").read_bytes())
        )
    diagnostics = (directory / "direct.messages.json").read_bytes() == (
        directory / "resumed.messages.json"
    ).read_bytes()
    manifest = json.loads(
        (directory / "direct/compilation-0000/complete.json").read_text()
    )
    return {
        "equal": all(artifacts.values())
        and all(semantic_artifacts.values())
        and diagnostics,
        "graphs_equal": True,
        "observations": manifest["dump_count"] if args.stage == "S2" else 1,
        "exact_artifacts": artifacts,
        "diagnostics_equal": diagnostics,
        "semantic_artifacts": semantic_artifacts,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", type=Path, help="Rust driver for the public Gen-2 writer check at S3")
    parser.add_argument("--stage", choices=("S2", "S3"), default="S2")
    parser.add_argument("--through-rust", action="store_true",
                        help="Decode and re-encode the exported circuit with sql-ir before resuming Java")
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--comparator", required=True, type=Path)
    parser.add_argument("--producer", required=True)
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--free-space-path", action="append", type=Path, default=[])
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.comparator = args.comparator.resolve()
    if args.compiler:
        args.compiler = args.compiler.resolve()
    args.classpath = os.pathsep.join(
        str(Path(entry).resolve()) for entry in args.classpath.split(os.pathsep)
    )
    args.output.mkdir()
    budget = OutputBudget(
        args.output,
        512 * 1024 * 1024,
        [args.output, *args.free_space_path],
        512 * 1024 * 1024,
    )
    report = {
        "schema_version": 1,
        "stage": args.stage,
        "rust_circuit_roundtrip": args.through_rust,
        "population": "eight focused programs in both modes; not the corpus",
        "compiler_options": ["-i", "--streaming", "--correlatedColumns"],
        "classpath": classpath_digest(args.classpath),
        "comparator_sha256": file_digest(args.comparator),
        "producer": args.producer,
        "runtime": args.runtime,
        "dataflow_compared": True,
        "limits": {"heap_mib": 1024, "output_mib": 512, "min_free_mib": 512},
        "cases": [],
    }
    for mode in ("classic", "gen2"):
        for name, sql in PROGRAMS.items():
            directory = args.output / mode / name
            directory.mkdir(parents=True)
            try:
                result = run_case(args, directory, mode, sql, budget)
            except subprocess.CalledProcessError as error:
                result = {"equal": False, "failed_command": [str(arg) for arg in error.cmd]}
            report["cases"].append({"mode": mode, "name": name, "sql": sql, **result})
            print(f"{mode} {name}: equal={result['equal']}", flush=True)
    report["equal"] = all(case["equal"] for case in report["cases"])
    if report["equal"]:
        report["jvm_runs"] = 3 * len(report["cases"])
        report["observations"] = sum(case["observations"] for case in report["cases"])
    (args.output / "complete.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["equal"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
