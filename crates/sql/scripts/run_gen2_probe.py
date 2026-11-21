#!/usr/bin/env python3
"""Run the full Java suite against pinned Gen-2 binaries and retain its verdicts."""

import argparse
import datetime
import hashlib
import json
import os
import subprocess
from pathlib import Path

from summarize_gen2 import digest, read_completed_run, summarize


def utc_now():
    return datetime.datetime.now(datetime.UTC).isoformat()


def compiler_sources(directory):
    # The compiler's sibling Maven modules own their src trees. Searching the
    # whole checkout by extension also collects generated Cargo documentation,
    # while missing parser configuration and fixtures with other extensions.
    poms = set(directory.glob("*/pom.xml"))
    if (directory / "pom.xml").is_file():
        poms.add(directory / "pom.xml")
    files = set(poms)
    for pom in poms:
        files.update(path for path in (pom.parent / "src").rglob("*") if path.is_file())
    for name in ("build.sh", "calcite_version.env", "check-keyword-drift.py"):
        path = directory / name
        if path.is_file():
            files.add(path)
    files.update(path for path in (directory / ".mvn").rglob("*") if path.is_file())
    return {str(path.relative_to(directory)): digest(path) for path in sorted(files)}


def verify_engine(engine, provenance):
    for name in ("pipeline", "ir-expander"):
        path = engine / name
        if not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError(f"Missing executable Gen-2 binary: {path}")
        if provenance.get("binaries_sha256", {}).get(name) != digest(path):
            raise ValueError(f"Gen-2 binary differs from its provenance: {name}")
    for key in ("forge_commit", "forge_platform_commit"):
        value = provenance.get(key, "")
        if len(value) != 40 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError(f"Expected a complete pinned {key}")


def run(output, compiler, engine, engine_provenance, calcite_revision):
    compiler, engine = compiler.resolve(), engine.resolve()
    output = output.resolve()
    provenance = json.loads(engine_provenance.read_text())
    verify_engine(engine, provenance)
    sources = compiler_sources(compiler)
    if not (compiler / "SQL-compiler/pom.xml").is_file() or not sources:
        raise ValueError("Expected a SQL compiler Maven checkout")
    output.mkdir()
    command = [
        "mvn",
        "-B",
        "--no-transfer-progress",
        "-pl",
        "SQL-compiler",
        "-DskipTests=false",
        "-Dmaven.test.skip=false",
        "-Dfeldera.corpus.only=false",
        "-Dmaven.test.failure.ignore=false",
        f"-Dfeldera.test.reports={output / 'reports'}",
        "test",
    ]
    record = {
        "started_utc": utc_now(),
        "command": command,
        "compiler_directory": str(compiler),
        "calcite_revision": calcite_revision,
        "compiler_sources_sha256": sources,
        "engine": provenance,
        "engine_directory": str(engine),
        "engine_provenance_sha256": digest(engine_provenance),
        "runner_sha256": digest(Path(__file__)),
        "runtime_executed": True,
    }
    (output / "provenance.json").write_text(json.dumps(record, indent=2) + "\n")
    environment = os.environ.copy()
    environment.update(GEN2_PATH=str(engine), CI="true")
    # Capture the same tree's complete test identities before enabling Gen-2.
    # A partial runtime run must not inflate its pass share by losing tests.
    baseline = output / "baseline"
    baseline.mkdir()
    baseline_command = [
        argument
        for argument in command
        if not argument.startswith(
            ("-Dfeldera.corpus.only=", "-Dfeldera.test.reports=")
        )
    ]
    baseline_command[1:1] = [
        "-Dfeldera.corpus.only=true",
        f"-Dfeldera.corpus.dir={baseline / 'corpus'}",
        f"-Dfeldera.test.reports={baseline / 'reports'}",
        "-Dfeldera.corpus.producer=source-sha256:"
        + hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest(),
        f"-Dfeldera.corpus.calcite={calcite_revision}",
        "-Dfeldera.corpus.runtime=not-executed",
    ]
    baseline_environment = environment.copy()
    baseline_environment.pop("GEN2_PATH")
    execute(baseline, baseline_command, compiler, baseline_environment)
    counts, _, _, baseline_completion = read_completed_run(baseline)
    if baseline_completion["returncode"] != 0 or counts["failures"] or counts["errors"]:
        raise ValueError("Compile/export baseline failed; Gen-2 was not measured")
    execute(output, command, compiler, environment)
    verify_engine(engine, provenance)
    if compiler_sources(compiler) != sources:
        raise ValueError("Compiler sources changed during the Gen-2 run")
    result = summarize(output, baseline / "reports")
    comparison = result["baseline_comparison"]
    if comparison["added_tests"] or comparison["removed_tests"]:
        raise ValueError("Gen-2 test identities differ from the complete baseline")
    result["baseline_runtime_executed"] = False
    (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def execute(output, command, compiler, environment):
    (output / "command.json").write_text(json.dumps(command, indent=2) + "\n")
    with (output / "maven.log").open("x") as log:
        process = subprocess.run(
            command,
            cwd=compiler,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
    (output / "process-result.json").write_text(
        json.dumps(
            {
                "returncode": process.returncode,
                "finished_utc": utc_now(),
            },
            indent=2,
        )
        + "\n"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", required=True, type=Path)
    parser.add_argument("--engine", required=True, type=Path)
    parser.add_argument("--engine-provenance", required=True, type=Path)
    parser.add_argument("--calcite-revision", required=True)
    args = parser.parse_args()
    result = run(
        args.output,
        args.compiler,
        args.engine,
        args.engine_provenance,
        args.calcite_revision,
    )
    # A completed experiment may fail the 80% criterion. Preserve that result;
    # infrastructure errors and incomplete reports fail above instead.
    report = (
        f"Gen-2: {result['passes']}/{result['counts']['tests']} tests pass "
        f"({result['pass_share_all_tests']:.2%}); "
        f"80% criterion met: {result['threshold_met_all_tests']}.\n"
    )
    print(report, end="")
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary_path).open("a") as output:
            output.write(report)


if __name__ == "__main__":
    main()
