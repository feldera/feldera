#!/usr/bin/env python3
"""Compare Rust multiset facts with Java for 50 predeclared programs at S2/S3."""

import argparse
import json
import os
import threading
from pathlib import Path

from run_determinism import OutputBudget, classpath_digest, file_digest, run_process


def programs():
    result = {}
    projections = {
        "keep": "SELECT x, y FROM d",
        "drop": "SELECT x FROM d",
        "permute": "SELECT y, x FROM d",
        "duplicate": "SELECT x, x AS again, y FROM d",
        "compute": "SELECT x, y + 1 AS y FROM d",
        "union": "SELECT * FROM d UNION ALL SELECT * FROM d",
    }
    for name, ty in {
        "integer": "INT",
        "bigint": "BIGINT",
        "string": "VARCHAR(12)",
        "decimal": "DECIMAL(12,2)",
        "timestamp": "TIMESTAMP",
    }.items():
        for projection, query in projections.items():
            result[f"{name}/{projection}"] = (
                f"CREATE TABLE t(x {ty}, y INT); "
                "CREATE LOCAL VIEW d AS SELECT DISTINCT x, y FROM t; "
                f"CREATE VIEW v AS {query};"
            )
    for name, source, expression in [
        ("unsafe_narrow", "INT", "CAST(x AS TINYINT)"),
        ("safe_narrow", "INT", "SAFE_CAST(x AS TINYINT)"),
        ("safe_widen", "INT", "SAFE_CAST(x AS BIGINT)"),
        ("string_truncate", "VARCHAR(12)", "CAST(x AS VARCHAR(3))"),
        ("string_unlimited", "VARCHAR(12)", "CAST(x AS VARCHAR)"),
        ("fixed_string", "CHAR(12)", "CAST(x AS VARCHAR)"),
        ("decimal_round", "DECIMAL(12,2)", "CAST(x AS DECIMAL(10,1))"),
        ("nullable_identity", "INT", "COALESCE(x, x)"),
        ("if_identity", "INT", "CASE WHEN x > 0 THEN x ELSE x END"),
        ("if_collision", "INT", "CASE WHEN x > 0 THEN x ELSE 0 END"),
    ]:
        result[name] = (
            f"CREATE TABLE t(x {source}); "
            "CREATE LOCAL VIEW d AS SELECT DISTINCT x FROM t; "
            f"CREATE VIEW v AS SELECT {expression} AS x FROM d;"
        )
    result.update(
        {
            "constant_empty": "CREATE VIEW v AS SELECT x FROM (VALUES (1)) AS t(x) WHERE FALSE;",
            "constant_single": "CREATE VIEW v AS SELECT 1 AS x;",
            "constant_multiple": "CREATE VIEW v AS SELECT * FROM (VALUES (1), (2)) AS t(x);",
            "constant_duplicate": "CREATE VIEW v AS SELECT * FROM (VALUES (1), (1)) AS t(x);",
            "constant_distinct": "CREATE VIEW v AS SELECT DISTINCT * FROM (VALUES (1), (1)) AS t(x);",
            "keyed_keep": "CREATE TABLE t(x INT NOT NULL PRIMARY KEY, y INT); CREATE VIEW v AS SELECT * FROM t;",
            "keyed_drop": "CREATE TABLE t(x INT NOT NULL PRIMARY KEY, y INT); CREATE VIEW v AS SELECT y FROM t;",
            "filter_set": "CREATE TABLE t(x INT); CREATE LOCAL VIEW d AS SELECT DISTINCT x FROM t; CREATE VIEW v AS SELECT x FROM d WHERE x > 0;",
            "filter_bag": "CREATE TABLE t(x INT); CREATE VIEW v AS SELECT x FROM t WHERE x > 0;",
            "lateness": "CREATE TABLE t(x INT, ts TIMESTAMP LATENESS INTERVAL 1 SECOND); CREATE VIEW v AS SELECT * FROM t;",
        }
    )
    assert len(result) == 50
    # Every program has genuinely dead operators at S2. This prefix also places
    # shared inner definitions before live uses, exercising their relocation.
    return {
        name: "CREATE LOCAL VIEW spike_unused AS SELECT 123 AS n; " + sql
        for name, sql in result.items()
    }


def compare(expected, actual):
    differences = []
    dead_code_differences = []
    view_differences = []
    sum_differences = []
    distinct_differences = []
    distinct_seam_differences = []
    removed_operators = 0
    removed_views = 0
    inserted_output_distincts = 0
    output_ports = 0
    facts = {False: 0, True: 0}
    if len(expected) != len(actual):
        raise ValueError("Oracle and Rust record counts differ")
    ids = set()
    for oracle, rust in zip(expected, actual):
        if oracle["id"] != rust["id"]:
            raise ValueError("Oracle and Rust case order differs")
        if oracle["id"] in ids:
            raise ValueError("Duplicate case identity")
        ids.add(oracle["id"])
        for record in (oracle, rust):
            if not isinstance(record["multisets"], list) or any(
                not isinstance(ports, list)
                or any(type(fact) is not bool for fact in ports)
                for ports in record["multisets"]
            ):
                raise ValueError("Analysis facts must be arrays of boolean arrays")
        if oracle["multisets"] != rust["multisets"]:
            differences.append(oracle["id"])
        if rust.get("dead_code_equal") is not True:
            dead_code_differences.append(oracle["id"])
        removed_operators += oracle["removed_operators"]
        if "remove_views" in oracle:
            removed_views += oracle["removed_views"]
            if rust.get("remove_views_equal") is not True:
                view_differences.append(oracle["id"])
        if "merge_sums" in oracle and rust.get("merge_sums_equal") is not True:
            sum_differences.append(oracle["id"])
        if "ensure_distinct_outputs" in oracle:
            inserted_output_distincts += oracle["inserted_output_distincts"]
            if rust.get("ensure_distinct_outputs_equal") is not True:
                distinct_differences.append(oracle["id"])
        if "distinct_seam" in oracle and rust.get("distinct_seam_equal") is not True:
            distinct_seam_differences.append(oracle["id"])
        for operator in oracle["multisets"]:
            for multiset in operator:
                facts[multiset] += 1
                output_ports += 1
    return {
        "cases": len(expected),
        "ports": output_ports,
        "set_ports": facts[False],
        "multiset_ports": facts[True],
        "differences": differences,
        "dead_code_differences": dead_code_differences,
        "removed_operators": removed_operators,
        "removed_views": removed_views,
        "remove_views_differences": view_differences,
        "merge_sums_differences": sum_differences,
        "inserted_output_distincts": inserted_output_distincts,
        "ensure_distinct_outputs_differences": distinct_differences,
        "distinct_seam_differences": distinct_seam_differences,
        "equal": not (
            differences or dead_code_differences or view_differences
            or sum_differences or distinct_differences or distinct_seam_differences
        ),
    }


def source_digests():
    root = Path(__file__).resolve().parents[3]
    probe = root / "crates/sql/multiset-probe"
    java = root / "sql-to-dbsp-compiler/SQL-compiler/src"
    sources = [
        Path(__file__),
        probe / "Cargo.toml",
        probe / "Cargo.lock",
        *sorted((probe / "src").rglob("*.rs")),
        *sorted((root / "crates/sql/sql-difftest/src").rglob("*.rs")),
        java / "test/java/org/dbsp/sqlCompiler/compiler/sql/tools/MultisetProbe.java",
        java
        / "test/java/org/dbsp/sqlCompiler/compiler/sql/tools/CorpusDeterminism.java",
        *[
            java / "main/java/org/dbsp/sqlCompiler" / name
            for name in (
                "circuit/annotation/Annotation.java",
                "circuit/annotation/Annotations.java",
                "circuit/annotation/JoinStrategy.java",
                "compiler/backend/ToJsonOuterVisitor.java",
                "compiler/frontend/calciteObject/CalciteRelNode.java",
                "compiler/frontend/calciteObject/RelReference.java",
                "compiler/visitors/inner/Expensive.java",
            )
        ],
        *[
            java / "main/java/org/dbsp/sqlCompiler/compiler/visitors/outer" / name
            for name in (
                "keys/MultisetAnalysis.java",
                "keys/LosslessCastLineage.java",
                "keys/KeyAnalysis.java",
                "Lineage.java",
                "DeadCode.java",
                "FindDeadCode.java",
                "RemoveOperators.java",
                "RemoveViewOperators.java",
                "MergeSums.java",
                "EnsureDistinctOutputs.java",
                "PropagateConstants.java",
                "PropagateEmptySources.java",
                "RemoveConstantFilters.java",
                "RemoveIAfterD.java",
                "RemoveDeindexOperators.java",
                "RemoveNoops.java",
                "RemoveIdentityOperators.java",
                "recursive/ValidateRecursiveOperators.java",
                "NoConnectorMetadata.java",
                "DeterministicFunctions.java",
                "CheckHints.java",
                "TagRegions.java",
                "StrayGC.java",
                "Graph.java",
                "NoIntegralVisitor.java",
                "PushDifferentialsUp.java",
                "BalancedJoins.java",
                "LinearPostprocessRetainKeys.java",
                "CloneOperatorsWithFanout.java",
            )
        ],
    ]
    return {str(path.relative_to(root)): file_digest(path) for path in sources}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--probe", required=True, type=Path)
    parser.add_argument("--producer", required=True)
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--sets", action="store_true", help="also compare and resume output-distinct S2 rewrites")
    parser.add_argument("--free-space-path", action="append", type=Path, default=[])
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.probe = args.probe.resolve(strict=True)
    args.classpath = os.pathsep.join(
        str(Path(entry).resolve(strict=True))
        for entry in args.classpath.split(os.pathsep)
    )
    args.output.mkdir()
    budget = OutputBudget(
        args.output,
        512 * 1024 * 1024,
        [args.output, *args.free_space_path],
        512 * 1024 * 1024,
    )
    budget.check(force=True)
    selected = args.output / "input.jsonl"
    with selected.open("x") as output:
        for name, sql in programs().items():
            output.write(json.dumps({"id": name, "sql": sql}) + "\n")
    provenance = {
        "schema_version": 1,
        "producer": args.producer,
        "runtime": args.runtime,
        "outputs_are_sets": args.sets,
        "classpath": classpath_digest(args.classpath),
        "probe_sha256": file_digest(args.probe),
        "sources": source_digests(),
        "input_sha256": file_digest(selected),
        "programs": 50,
        "runtime_executed": False,
        "optimizer_ported": False,
        "typed_model": "analysis projection; other wire fields are preserved",
        "rust_passes": ["MultisetAnalysis subset", "DeadCode keepAllSources=true"],
        "isolated_rust_passes": ["RemoveViewOperators all=false", "MergeSums", "EnsureDistinctOutputs"],
        "remaining_optimizer": "Java",
        "limits": {
            "heap_mib": 1024,
            "output_mib": 512,
            "min_free_mib": 512,
            "timeout_seconds": 300,
        },
    }
    (args.output / "provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )
    results = []
    tail_results = []
    java = [
        "java",
        "-ea",
        "-Xmx1g",
        f"-Dfeldera.probe.producer={args.producer}",
        f"-Dfeldera.probe.runtime={args.runtime}",
        "-cp",
        args.classpath,
        "org.dbsp.sqlCompiler.compiler.sql.tools.MultisetProbe",
    ]
    output_options = ["sets"] if args.sets else []
    jvm_runs = 1
    sum_fixtures = args.output / "sum-fixtures-java.jsonl"
    sum_actual = args.output / "sum-fixtures-rust.jsonl"
    for name, command in (
        ("java", java + ["--sum-fixtures", str(sum_fixtures)]),
        ("rust", [str(args.probe), str(sum_fixtures), str(sum_actual)]),
    ):
        with (args.output / f"sum-fixtures-{name}.log").open("w") as log:
            run_process(command, args.output, log, 300, threading.Event(), budget)
    expected = [json.loads(line) for line in sum_fixtures.read_text().splitlines()]
    observed = [json.loads(line) for line in sum_actual.read_text().splitlines()]
    if len(expected) != 20 or any("merge_sums" not in record for record in expected):
        raise ValueError("Incomplete directed merge-sums fixture population")
    sum_results = compare(expected, observed)
    print(f"MergeSums fixtures: {sum_results['cases']} graphs, equal={sum_results['equal']}", flush=True)
    directed_results = []
    for kind, count in (("constant", 27), ("cleanup", 12)):
        name = f"{kind}-fixtures"
        oracle = args.output / f"{name}-java.jsonl"
        actual = args.output / f"{name}-rust.jsonl"
        commands = (
            java + [f"--{name}", str(oracle)],
            [str(args.probe), "--compare-pass", str(oracle), str(actual)],
        )
        for index, command in enumerate(commands):
            with (args.output / f"{name}-{index}.log").open("w") as log:
                run_process(command, args.output, log, 300, threading.Event(), budget)
        expected = [json.loads(line) for line in oracle.read_text().splitlines()]
        observed = [json.loads(line) for line in actual.read_text().splitlines()]
        expected_ids = [(row["id"], row["pass"]) for row in expected]
        observed_ids = [(row["id"], row["pass"]) for row in observed]
        if len(expected_ids) != count or len(set(expected_ids)) != count or expected_ids != observed_ids:
            raise ValueError(f"Incomplete or reordered {name} results")
        differences = [row["id"] for row in observed if row["equal"] is not True]
        directed_results.append({
            "name": name, "cases": len(observed), "equal": not differences,
            "differences": differences,
        })
        jvm_runs += 1
        print(f"{name}: {len(observed)} graphs, equal={not differences}", flush=True)
    for mode in ("classic", "gen2"):
        for stage in ("S2", "S3"):
            name = f"{mode}-{stage}"
            oracle = args.output / f"{name}-java.jsonl"
            actual = args.output / f"{name}-rust.jsonl"
            commands = [
                java + [str(selected), str(oracle), stage, mode] + output_options,
                [str(args.probe), str(oracle), str(actual)],
            ]
            for index, command in enumerate(commands):
                with (args.output / f"{name}-{index}.log").open("w") as log:
                    run_process(
                        command, args.output, log, 300, threading.Event(), budget
                    )
            expected = [json.loads(line) for line in oracle.read_text().splitlines()]
            observed = [json.loads(line) for line in actual.read_text().splitlines()]
            result = {"mode": mode, "stage": stage, **compare(expected, observed)}
            if result["cases"] != provenance["programs"]:
                raise ValueError("Oracle omitted a predeclared program")
            if any("remove_views" not in record for record in expected):
                raise ValueError("Oracle omitted the view-removal comparison")
            if any("merge_sums" not in record for record in expected):
                raise ValueError("Oracle omitted the merge-sums comparison")
            if stage == "S2" and any("ensure_distinct_outputs" not in record for record in expected):
                raise ValueError("Oracle omitted the distinct-output comparison")
            if args.sets and stage == "S2":
                if any("distinct_seam" not in record for record in expected):
                    raise ValueError("Oracle omitted the distinct-output replay comparison")
                if not result["inserted_output_distincts"]:
                    raise ValueError("Output-distinct fixtures exercised no insertions")
            results.append(result)
            jvm_runs += 1
            print(
                f"{name}: {result['ports']} ports, equal={result['equal']}", flush=True
            )
        # Both boundaries must reach direct S3. The distinct-output variant
        # starts from the same original S2 as the DeadCode variant.
        replays = [("S2", "S2"), ("S3", "S3")]
        if args.sets:
            replays.extend((("distinct-rust", "S2"), ("distinct-java", "S2")))
        for variant, start in replays:
            name = f"{mode}-{variant}"
            actual = args.output / f"{mode}-{start}-rust.jsonl"
            if variant.startswith("distinct-"):
                origin = variant.removeprefix("distinct-")
                source = args.output / f"{mode}-{start}-{origin}.jsonl"
                actual = args.output / f"{name}-input.jsonl"
                with source.open() as records, actual.open("x") as output:
                    for line in records:
                        record = json.loads(line)
                        transformed = {"id": record["id"], "seam": record["distinct_seam"]}
                        output.write(json.dumps(transformed) + "\n")
            oracle = args.output / f"{mode}-S3-java.jsonl"
            resumed = args.output / f"{name}-resumed.jsonl"
            comparison = args.output / f"{name}-tail-comparison.jsonl"
            commands = [
                java + [str(actual), str(resumed), f"resume-{start.lower()}", mode] + output_options,
                [
                    str(args.probe),
                    "--compare-tail",
                    str(oracle),
                    str(resumed),
                    str(comparison),
                ],
            ]
            for index, command in enumerate(commands):
                with (args.output / f"{name}-tail-{index}.log").open("w") as log:
                    run_process(
                        command, args.output, log, 300, threading.Event(), budget
                    )
            compared = [
                json.loads(line) for line in comparison.read_text().splitlines()
            ]
            if len(compared) != provenance["programs"]:
                raise ValueError("Tail comparison omitted a predeclared program")
            differences = [record for record in compared if record["equal"] is not True]
            tail_results.append(
                {
                    "mode": mode,
                    "start": start,
                    "variant": variant,
                    "cases": len(compared),
                    "equal": not differences,
                    "differences": differences,
                }
            )
            jvm_runs += 1
            print(
                f"{name} Java tail: {len(compared)} cases, equal={not differences}",
                flush=True,
            )
    if (
        classpath_digest(args.classpath) != provenance["classpath"]
        or file_digest(args.probe) != provenance["probe_sha256"]
        or source_digests() != provenance["sources"]
    ):
        raise ValueError("Oracle or Rust executable changed during comparison")
    report = {
        **provenance,
        "results": results,
        "tail_results": tail_results,
        "sum_fixture_results": sum_results,
        "directed_fixture_results": directed_results,
        "equal": all(r["equal"] for r in [sum_results, *directed_results, *results, *tail_results]),
        "jvm_runs": jvm_runs,
    }
    (args.output / "complete.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["equal"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
