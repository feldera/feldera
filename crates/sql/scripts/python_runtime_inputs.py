#!/usr/bin/env python3
"""Freeze registered Python runtime-negative fixtures without importing tests.

The normal suite currently skips negative views. This prepares independent runs;
it neither changes that switch nor substitutes SQL INSERTs for JSON decoding.
"""

import argparse
import ast
import hashlib
import json
import math
import re
from pathlib import Path

from python_diagnostic_inputs import beautify, class_value


def literal_data(cls):
    for node in cls.body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            continue
        if not isinstance(node, ast.FunctionDef) or node.name != "__init__":
            raise ValueError("fixture class overrides behavior outside its constructor")
    methods = [node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "__init__"]
    if len(methods) != 1:
        raise ValueError("fixture requires one explicit constructor")
    values = []
    for statement in methods[0].body:
        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant) and isinstance(statement.value.value, str):
            continue
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            raise ValueError("fixture constructor contains executable or conditional logic")
        target = statement.targets[0]
        if not (isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name)
                and target.value.id == "self" and target.attr in {"sql", "data", "expected_error"}):
            raise ValueError("unknown fixture constructor assignment")
        if target.attr == "data":
            try:
                values.append(ast.literal_eval(statement.value))
            except (ValueError, TypeError) as error:
                raise ValueError("fixture data must be literal") from error
    if len(values) != 1 or not isinstance(values[0], list) or any(not isinstance(row, dict) for row in values[0]):
        raise ValueError("fixture requires one literal list of record objects")

    def validate(value):
        if isinstance(value, dict):
            if any(not isinstance(key, str) for key in value):
                raise ValueError("JSON fixture keys must be strings")
            for child in value.values():
                validate(child)
        elif isinstance(value, list):
            for child in value:
                validate(child)
        elif type(value) not in {str, int, float, bool, type(None)}:
            raise ValueError("fixture contains a non-JSON value")
        elif isinstance(value, float) and not math.isfinite(value):
            raise ValueError("fixture contains a nonfinite JSON number")

    validate(values[0])
    return values[0]


def extract(inputs, results, root):
    sources, parsed = {}, {}

    def parse(path):
        if path not in parsed:
            contents = (root / path).read_bytes()
            digest = hashlib.sha256(contents).hexdigest()
            if inputs["source_hashes"].get(path) != digest:
                raise ValueError(f"fixture source differs from pinned diagnostic input: {path}")
            sources[path] = digest
            parsed[path] = ast.parse(contents, filename=path)
        return parsed[path]

    base = parse("python/tests/runtime_aggtest/aggtst_base.py")
    accumulator = next(node for node in base.body if isinstance(node, ast.ClassDef) and node.name == "TstAccumulator")
    run = next(node for node in accumulator.body if isinstance(node, ast.FunctionDef) and node.name == "run_tests")
    enabled = any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                  and isinstance(node.func.value, ast.Name) and node.func.value.id == "self"
                  and node.func.attr == "run_expected_failures" for node in ast.walk(run))
    if enabled:
        raise ValueError("normal negative-view execution changed; review suite semantics")
    records = {record["id"]: record for record in inputs["records"]}
    if len(records) != len(inputs["records"]) or len({row["id"] for row in results}) != len(results):
        raise ValueError("duplicate diagnostic identity")
    if set(records) != {row["id"] for row in results}:
        raise ValueError("diagnostic results and inputs have different identities")
    fixtures, candidates = {}, []
    for result in results:
        if not result.get("circuit_produced"):
            continue
        record = records[result["id"]]
        if not record["active"] or result.get("compiler_rejected") or result.get("runtime_executed"):
            raise ValueError("expected an active, compile-success-only candidate")
        path, class_name = record["id"].rsplit(":", 1)
        cls = next(node for node in parse(path).body if isinstance(node, ast.ClassDef) and node.name == class_name)
        if [ast.unparse(node) for node in cls.bases] != ["TstView"]:
            raise ValueError("runtime extraction currently requires a negative view")
        _, expected = class_value(cls, "expected_error")
        _, view_sql = class_value(cls, "sql")
        if expected != record["expected"] or not expected.strip():
            raise ValueError("runtime expectation differs from pinned record")
        program = "".join(table["sql"] + ";\n\n" for table in record["tables"]) + "\n" + beautify(view_sql) + ";\n\n"
        if program != record["sql"]:
            raise ValueError("runtime SQL differs from registered tables and view")
        table_ids = []
        for table in record["tables"]:
            identity = table["source"] + ":" + table["class"]
            table_ids.append(identity)
            if identity in fixtures:
                continue
            table_class = next(node for node in parse(table["source"]).body
                               if isinstance(node, ast.ClassDef) and node.name == table["class"])
            if [ast.unparse(node) for node in table_class.bases] != ["TstTable"]:
                raise ValueError("unknown table fixture inheritance")
            _, sql = class_value(table_class, "sql")
            if beautify(sql) != table["sql"]:
                raise ValueError("table SQL differs from pinned registration")
            # Match SqlObject.extract_name, retaining its unquoted-name restriction.
            match = re.search(r"CREATE\s+TABLE\s+(\w+)", sql, re.IGNORECASE)
            if match is None:
                raise ValueError("fixture table name is unsupported")
            data = literal_data(table_class)
            envelopes = [{"insert": row} for row in data]
            payload = json.dumps(envelopes, ensure_ascii=False, allow_nan=False)
            fixtures[identity] = {"id": identity, "table_name": match[1], "sql": table["sql"],
                                  "records": data, "json_insert_delete": payload,
                                  "payload_sha256": hashlib.sha256(payload.encode()).hexdigest(),
                                  "record_count": len(data), "transaction_after_table": True,
                                  "integer_records_only": all(type(value) in {int, type(None)}
                                                              for row in data for value in row.values())}
        candidates.append({"id": record["id"], "sql": record["sql"], "expected": expected,
                           "expected_match": "case-insensitive substring, trimmed, as assert_expected_error",
                           "table_sequence": table_ids, "compiler_options": result["options"],
                           "registered": True, "normal_suite_executes_negative_view": False,
                           "runtime_executed": False, "route": "local_generated_circuit_with_real_json_record_decoder"})
    return {"schema_version": 1, "complete": True, "records": candidates, "fixtures": list(fixtures.values()),
            "source_hashes": sources, "normal_suite_executes_negative_views": False,
            "platform_transport_executed": False, "runtime_executed": False,
            "scope": "independent runtime experiment inputs; preserves registered table order and JSON values"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path)
    parser.add_argument("results", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args()
    report = extract(json.loads(args.inputs.read_text()),
                     [json.loads(line) for line in args.results.read_text().splitlines()], args.repo)
    report["input_sha256"] = hashlib.sha256(args.inputs.read_bytes()).hexdigest()
    report["results_sha256"] = hashlib.sha256(args.results.read_bytes()).hexdigest()
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")
    print(json.dumps({"candidates": len(report["records"]), "table_fixtures": len(report["fixtures"])}))


if __name__ == "__main__":
    main()
