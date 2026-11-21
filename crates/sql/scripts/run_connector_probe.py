"""Compare the compiler-local Rust decoder with Java and detect schema drift."""

import argparse
import json
import os
import threading
from pathlib import Path

from connector_cases import cases, sql_cases
from run_determinism import OutputBudget, classpath_digest, file_digest, run_process

ROOT = Path(__file__).resolve().parents[3]
PROBE = ROOT / "crates/sql/connector-probe"
JAVA = ROOT / "sql-to-dbsp-compiler/SQL-compiler/src"


def runtime_fields(schemas, name, visiting=()):
    if name in visiting:
        raise ValueError(f"Schema reference cycle: {name}")

    def collect(schema):
        if "$ref" in schema:
            return runtime_fields(
                schemas, schema["$ref"].rsplit("/", 1)[-1], (*visiting, name)
            )
        return set(schema.get("properties", {})).union(
            *(collect(part) for part in schema.get("allOf", []))
        )

    return collect(schemas[name])


def compare(requests, expected, observed):
    if not requests or len(expected) != len(requests) or len(observed) != len(requests):
        raise ValueError("Missing/extra records or empty population")
    ids = set()
    differences = []
    text_differences = 0
    warnings = 0
    for request, java, rust in zip(requests, expected, observed):
        identity = request["id"]
        if identity in ids or java.get("id") != identity or rust.get("id") != identity:
            raise ValueError("Duplicate or mismatched case identity")
        ids.add(identity)
        for record in (java, rust):
            if record.get("compiler_bug") is not False:
                raise ValueError(f"Internal compiler failure in {identity}: {record}")
            if "source" in request and type(record.get("accepted")) is not bool:
                raise ValueError("SQL verdict must be a boolean")
            if not isinstance(record.get("warnings"), list):
                raise TypeError("Missing warning list")
            for warning in record["warnings"]:
                if any(
                    type(warning.get(key)) is not bool
                    for key in ("warning", "continuation")
                ):
                    raise ValueError("Diagnostic flags must be booleans")
                if not isinstance(warning.get("error_type"), str) or not isinstance(
                    warning.get("message"), str
                ):
                    raise TypeError("Diagnostic category/text must be strings")
                position = warning.get("position", {})
                for key in (
                    "start_line_number",
                    "start_column",
                    "end_line_number",
                    "end_column",
                ):
                    if type(position.get(key)) is not int or position[key] < 0:
                        raise ValueError("Missing/invalid diagnostic position")
                if "source" in request and not isinstance(warning.get("snippet"), str):
                    raise ValueError("Missing SQL snippet")
        hard = lambda record: [
            {k: v for k, v in w.items() if k != "message"} for w in record["warnings"]
        ]
        if java.get("accepted") != rust.get("accepted") or hard(java) != hard(rust):
            differences.append(identity)
        text_differences += [w["message"] for w in java["warnings"]] != [
            w["message"] for w in rust["warnings"]
        ]
        warnings += len(java["warnings"])
    return {
        "cases": len(requests),
        "diagnostics": warnings,
        "equal": not differences,
        "differences": differences,
        "message_text_different_cases": text_differences,
    }


def source_digests():
    paths = (
        list(PROBE.glob("*.json"))
        + list(PROBE.glob("Cargo.*"))
        + list(PROBE.glob("src/*.rs"))
    )
    paths += list(
        (JAVA / "main/java/org/dbsp/sqlCompiler/compiler/frontend/connectors").rglob(
            "*.java"
        )
    )
    paths += [
        JAVA / "test/java/org/dbsp/sqlCompiler/compiler/sql/tools/ConnectorProbe.java",
        JAVA / "test/java/org/dbsp/sqlCompiler/compiler/sql/ConnectorTests.java",
        ROOT / "crates/feldera-types/examples/connector_schema.rs",
        Path(__file__),
        Path(__file__).with_name("connector_cases.py"),
    ]
    paths += [
        ROOT / f"crates/feldera-types/src/{p}.rs"
        for p in (
            "transport/postgres",
            "transport/kafka",
            "transport/iceberg",
            "transport/http",
            "format/avro",
        )
    ]
    return {str(path.relative_to(ROOT)): file_digest(path) for path in sorted(paths)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--runtime-schema", type=Path, required=True)
    parser.add_argument("--classpath", required=True)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.probe = args.probe.resolve(strict=True)
    args.runtime_schema = args.runtime_schema.resolve(strict=True)
    args.classpath = os.pathsep.join(
        str(Path(p).resolve(strict=True)) for p in args.classpath.split(os.pathsep)
    )
    args.output.mkdir()
    budget = OutputBudget(
        args.output, 512 * 1024 * 1024, [args.output], 512 * 1024 * 1024
    )
    budget.check(force=True)
    provenance = {
        "schema_version": 1,
        "classpath": classpath_digest(args.classpath),
        "probe_sha256": file_digest(args.probe),
        "runtime_schema_sha256": file_digest(args.runtime_schema),
        "sources": source_digests(),
        "limits": {
            "heap_mib": 512,
            "timeout_seconds": 600,
            "output_mib": 512,
            "min_free_mib": 512,
        },
        "runtime_executed": False,
        "message_text_reviewed": False,
    }
    (args.output / "provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )
    java = [
        "java",
        "-ea",
        "-Xmx512m",
        "-cp",
        args.classpath,
        "org.dbsp.sqlCompiler.compiler.sql.tools.ConnectorProbe",
    ]

    def run(command, name):
        with (args.output / f"{name}.log").open("x") as log:
            run_process(command, args.output, log, 600, threading.Event(), budget)

    inventory_path = args.output / "java-inventory.json"
    run(
        java
        + [
            "--inventory",
            str(
                JAVA
                / "main/java/org/dbsp/sqlCompiler/compiler/frontend/connectors/config"
            ),
            str(inventory_path),
        ],
        "inventory",
    )
    inventory = json.loads(inventory_path.read_text())
    if inventory != json.loads((PROBE / "java-inventory.json").read_text()):
        raise ValueError(
            "Java configuration contract drift: review and refresh the compiler schema"
        )
    runtime = args.output / "runtime-schema.json"
    run([str(args.runtime_schema)], "runtime-schema")
    # The schema exporter writes JSON to stdout, captured without shell redirection.
    runtime.write_bytes((args.output / "runtime-schema.log").read_bytes())
    schemas = json.loads(runtime.read_text())
    expected_fields = json.loads((PROBE / "runtime-fields.json").read_text())
    actual_fields = {
        name: sorted(runtime_fields(schemas, name)) for name in expected_fields
    }
    if actual_fields != expected_fields:
        raise ValueError(
            "Runtime field drift: review the compiler-local field inventory"
        )
    selected = cases(inventory)
    results = []
    for mode, population in [("config", selected), ("sql", list(sql_cases(selected)))]:
        inputs, oracle, actual = [
            args.output / f"{mode}-{suffix}.jsonl"
            for suffix in ("input", "java", "rust")
        ]
        with inputs.open("x") as output:
            for case in population:
                output.write(json.dumps(case, ensure_ascii=False) + "\n")
        run(java + [str(inputs), str(oracle)], f"{mode}-java")
        run([str(args.probe), str(inputs), str(actual)], f"{mode}-rust")
        result = {
            "mode": mode,
            **compare(
                population,
                [json.loads(line) for line in oracle.read_text().splitlines()],
                [json.loads(line) for line in actual.read_text().splitlines()],
            ),
        }
        results.append(result)
        (args.output / f"{mode}-comparison.json").write_text(
            json.dumps(result, indent=2) + "\n"
        )
        print(f"{mode}: {result['cases']} cases, equal={result['equal']}", flush=True)
    if (
        source_digests() != provenance["sources"]
        or classpath_digest(args.classpath) != provenance["classpath"]
        or file_digest(args.probe) != provenance["probe_sha256"]
        or file_digest(args.runtime_schema) != provenance["runtime_schema_sha256"]
    ):
        raise ValueError("Source or executable changed during comparison")
    report = {
        **provenance,
        "results": results,
        "equal": all(result["equal"] for result in results),
        "java_classes": len(inventory["classes"]),
        "java_fields": sum(len(c["fields"]) for c in inventory["classes"]),
        "runtime_field_drift": False,
        "java_field_drift": False,
        "artifacts": {p.name: file_digest(p) for p in args.output.glob("*.jsonl")},
    }
    (args.output / "complete.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["equal"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
