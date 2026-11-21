"""Capture level-2 folds and compare values, real compilations, and runtime edges."""

import argparse
import json
import os
import shutil
import threading
from pathlib import Path

from fold_cases import runtime_cases, sql_cases
from fold_census import census, decimal_text, normalized
from run_determinism import OutputBudget, classpath_digest, file_digest, run_process

ROOT = Path(__file__).resolve().parents[3]


def records(path, semantics_profile=None):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    if semantics_profile is not None:
        require_semantics_profile(rows, semantics_profile)
    return rows


def require_semantics_profile(results, expected):
    if any(row.get("semantics_profile") != expected for row in results):
        raise ValueError("Missing or mismatched fold semantics profile")


def compare_compilations(requests, expected, actual):
    if not requests or len(expected) != len(requests) or len(actual) != len(requests):
        raise ValueError("Missing, extra, or empty compilation population")
    seen = set()
    differences = []
    bugs = []
    fields = ("stage", "accepted", "schema", "diagnostics")
    for request, java, rust in zip(requests, expected, actual):
        identity = request["id"]
        if identity in seen or java.get("id") != identity or rust.get("id") != identity:
            raise ValueError("Duplicate or mismatched compilation identity")
        seen.add(identity)
        for result in (java, rust):
            if result.get("stage") not in ("S2", "S3"):
                raise ValueError("Missing or invalid compilation stage")
            if (
                type(result.get("accepted")) is not bool
                or type(result.get("compiler_bug")) is not bool
            ):
                raise ValueError("Missing or malformed compilation verdict")
            if not isinstance(result.get("schema"), dict) or not isinstance(
                result.get("diagnostics"), list
            ):
                raise TypeError("Missing compilation observation")
        if java["compiler_bug"] or rust["compiler_bug"]:
            bugs.append(identity)
        changed = [field for field in fields if java[field] != rust[field]]
        if changed:
            differences.append({"id": identity, "fields": changed})
    return {
        "cases": len(requests),
        "accepted_java": sum(r["accepted"] for r in expected),
        "accepted_rust": sum(r["accepted"] for r in actual),
        "compiler_bugs": bugs,
        "differences": differences,
        "equal": not bugs and not differences,
    }


def compare_runtime(requests, expected, actual):
    if not requests or len(expected) != len(requests) or len(actual) != len(requests):
        raise ValueError("Missing, extra, or empty runtime population")
    differences = []
    seen = set()
    for index, (request, runtime, probe) in enumerate(zip(requests, expected, actual)):
        identity = request["id"]
        if (
            identity in seen
            or runtime.get("id") != identity
            or type(probe.get("index")) is not int
            or probe.get("index") != index
        ):
            raise ValueError("Duplicate or mismatched runtime identity")
        seen.add(identity)
        if runtime.get("status") not in ("value", "runtime_error"):
            raise ValueError("Invalid runtime result")
        equal = runtime["status"] == probe.get("status")
        if equal and runtime["status"] == "value":
            reference = runtime["value"]
            if reference["type"] == "decimal":
                reference = {**reference, "value": decimal_text(reference["value"])}
            equal = normalized(reference) == normalized(probe["value"])
        if not equal:
            differences.append({"id": identity, "runtime": runtime, "probe": probe})
    return {
        "cases": len(requests),
        "differences": differences,
        "equal": not differences,
    }


def source_digests():
    paths = []
    for folder, suffix in [
        ("crates/sql/fold-probe", ".rs"),
        ("crates/sql/scripts", ".py"),
        ("crates/fxp", ".rs"),
        ("crates/sqllib", ".rs"),
        ("sql-to-dbsp-compiler/SQL-compiler/src", ".java"),
    ]:
        paths.extend((ROOT / folder).rglob("*" + suffix))
    paths += [
        ROOT / path
        for path in (
            "crates/sql/fold-probe/Cargo.toml",
            "crates/sql/fold-probe/Cargo.lock",
            "crates/fxp/Cargo.toml",
            "crates/sqllib/Cargo.toml",
            "Cargo.toml",
            "Cargo.lock",
        )
    ]
    return {str(p.relative_to(ROOT)): file_digest(p) for p in sorted(paths)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--runtime-reference", type=Path, required=True)
    parser.add_argument("--stage", choices=("S2", "S3"), default="S2")
    parser.add_argument(
        "--semantics-profile",
        choices=("runtime-v1", "legacy-exp-log-libm-0.2.15-v1", "legacy-janino-v1"),
        default="runtime-v1",
    )
    args = parser.parse_args()
    args.input = args.input.resolve(strict=True)
    args.output = args.output.resolve()
    args.probe = args.probe.resolve(strict=True)
    args.runtime_reference = args.runtime_reference.resolve(strict=True)
    args.classpath = os.pathsep.join(
        str(Path(p).resolve(strict=True)) for p in args.classpath.split(os.pathsep)
    )
    args.output.mkdir()
    budget = OutputBudget(args.output, 1024**3, [args.output], 1024**3)
    budget.check(force=True)
    provenance = {
        "schema_version": 1,
        "observation_stage": args.stage,
        "semantics_profiles": {
            "compiler_and_java_values": args.semantics_profile,
            "runtime_edges": "runtime-v1",
        },
        "classpath": classpath_digest(args.classpath),
        "probe_sha256": file_digest(args.probe),
        "runtime_reference_sha256": file_digest(args.runtime_reference),
        "sources": source_digests(),
        "limits": {
            "heap_mib": 1024,
            "timeout_seconds_per_process": 1200,
            "output_mib": 1024,
            "min_free_mib": 1024,
        },
        "full_corpus_runtime_executed": False,
        "deviations_reviewed": False,
        "complete_sqllib_split": False,
        "verdict_relevant_coverage_proven": False,
    }
    (args.output / "provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )

    def run(command, name):
        with (
            (args.output / f"{name}.log").open("x") as log,
            (args.output / f"{name}.stderr.log").open("x") as errors,
        ):
            run_process(
                command,
                args.output,
                log,
                1200,
                threading.Event(),
                budget,
                stderr=errors,
            )

    def write(name, data):
        path = args.output / name
        with path.open("x") as output:
            for record in data:
                output.write(json.dumps(record, ensure_ascii=False) + "\n")
        return path

    java = [
        "java",
        "-ea",
        "-Xmx1g",
        f"-Dfeldera.fold.stage={args.stage}",
        f"-Dfeldera.fold.semantics={args.semantics_profile}",
        "-cp",
        args.classpath,
        "org.dbsp.sqlCompiler.compiler.sql.tools.FoldProbe",
    ]
    input_path = args.output / "corpus-input.jsonl"
    shutil.copyfile(args.input, input_path)
    focused_path = write("focused-input.jsonl", sql_cases())
    results = {}
    for population, path in (("corpus", input_path), ("focused", focused_path)):
        for mode in ("baseline", "java", "rust"):
            prefix = f"{population}-{mode}"
            command = java + [
                str(path),
                str(args.output / f"{prefix}-cases.jsonl"),
                str(args.output / f"{prefix}-folds.jsonl"),
            ]
            if mode == "baseline":
                command.append("baseline")
            elif mode == "rust":
                command += ["rust", str(args.probe)]
            run(command, prefix)
        requests = records(path)
        reference = records(args.output / f"{population}-java-cases.jsonl")
        if any(record.get("stage") != args.stage for record in reference):
            raise ValueError("Compiler did not report the requested observation stage")
        results[population] = compare_compilations(
            requests, reference, records(args.output / f"{population}-rust-cases.jsonl")
        )
        results[population + "_trace_transparency"] = compare_compilations(
            requests,
            reference,
            records(args.output / f"{population}-baseline-cases.jsonl"),
        )
        folds = args.output / f"{population}-java-folds.jsonl"
        actual = args.output / f"{population}-values.jsonl"
        run(
            [
                str(args.probe),
                "--semantics-profile",
                args.semantics_profile,
                str(folds),
                str(actual),
            ],
            f"{population}-values",
        )
        results[population + "_values"] = census(
            records(folds), records(actual, args.semantics_profile)
        )
        summary = {
            key: len(value) if key in ("differences", "compiler_bugs") else value
            for key, value in results[population].items()
        }
        print(json.dumps({population: summary}), flush=True)
    vectors = list(runtime_cases())
    runtime_input = write("runtime-input.jsonl", vectors)
    run([str(args.runtime_reference), str(runtime_input)], "runtime-values")
    run(
        [
            str(args.probe),
            "--semantics-profile",
            "runtime-v1",
            str(runtime_input),
            str(args.output / "runtime-probe.jsonl"),
        ],
        "runtime-probe",
    )
    results["runtime_edges"] = compare_runtime(
        vectors,
        records(args.output / "runtime-values.log"),
        records(args.output / "runtime-probe.jsonl", "runtime-v1"),
    )
    if (
        source_digests() != provenance["sources"]
        or classpath_digest(args.classpath) != provenance["classpath"]
        or file_digest(args.probe) != provenance["probe_sha256"]
        or file_digest(args.runtime_reference) != provenance["runtime_reference_sha256"]
        or file_digest(input_path) != file_digest(args.input)
    ):
        raise ValueError("Source, input, or executable changed during comparison")
    report = {
        **provenance,
        "results": results,
        "equal_on_measured_cases": all(
            r.get("equal", r.get("zero_comparable_differences", False))
            for r in results.values()
        ),
        "artifacts": {p.name: file_digest(p) for p in args.output.glob("*.jsonl")},
        "runtime_values_sha256": file_digest(args.output / "runtime-values.log"),
        "runtime_stderr_sha256": file_digest(args.output / "runtime-values.stderr.log"),
    }
    (args.output / "complete.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["equal_on_measured_cases"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
