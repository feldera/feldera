#!/usr/bin/env python3
"""Compare the Rust syntax diagnostic class, start and source snippet with the product compiler."""

import argparse
import json
from pathlib import Path
import subprocess

FIXTURES = {
    "type_parameter": "CREATE TABLE t(x INT(10));",
    "reserved_type": "SELECT CAST(NULL AS DATETIME)",
    "unterminated_literal": "SELECT 'x",
    "multiline": "CREATE TABLE t(\n  x VARCHAR(2147483648))",
    "custom_type": "SELECT CAST(NULL AS DECIMA(5,2))",
}
FIELDS = ("warning", "error_type", "start_line_number", "start_column", "snippet")


def rejected_submissions(path):
    with path.open() as source:
        for line in source:
            case = json.loads(line)
            for index, submission in enumerate(case["submissions"]):
                if submission["verdict"] == "rejected":
                    if not submission["source"]["many"] or index:
                        raise ValueError("Expected a standalone many-statement parser case")
                    yield case["id"], submission["source"]["sql"]


def validate(output, compiler, classpath, cases):
    output.mkdir()
    inputs, oracle = output / "inputs.jsonl", output / "java.jsonl"
    with inputs.open("x") as target:
        for name, sql in cases:
            target.write(json.dumps({"id": name, "sql": sql}) + "\n")
    subprocess.run(["java", "-ea", "-Xmx1g", "-cp", classpath,
                    "org.dbsp.sqlCompiler.compiler.sql.tools.SyntaxDiagnosticOracle",
                    inputs, oracle], check=True)
    total = equal = 0
    with inputs.open() as source, oracle.open() as java, (output / "cases.jsonl").open("x") as raw:
        for left, right in zip(source, java, strict=True):
            case, reference = json.loads(left), json.loads(right)
            if reference["id"] != case["id"] or not reference["messages"]:
                raise ValueError(f"Missing Java rejection for {case['id']}")
            rust = subprocess.run([compiler, "--syntax-diagnostics", "-"], input=case["sql"],
                                  text=True, capture_output=True)
            if rust.returncode != 1 or rust.stderr:
                raise ValueError(f"Expected a Rust syntax diagnostic for {case['id']}: {rust.stderr}")
            actual = json.loads(rust.stdout)
            differences = [key for key in FIELDS if [m[key] for m in actual]
                           != [m[key] for m in reference["messages"]]]
            equal += not differences
            total += 1
            raw.write(json.dumps({"id": case["id"], "differences": differences, "rust": actual}) + "\n")
    if not total:
        raise ValueError("Empty syntax diagnostic population")
    report = {"cases": total, "equal": equal, "different": total - equal, "fields": FIELDS}
    (output / "complete.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--compiler", required=True, type=Path)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--parser-results", type=Path, help="measure all Java rejections from a raw parser run")
    args = parser.parse_args()
    cases = rejected_submissions(args.parser_results) if args.parser_results else FIXTURES.items()
    result = validate(args.output, args.compiler, args.classpath, cases)
    print(json.dumps(result))
    if not args.parser_results and result["different"]:
        raise SystemExit(1)
