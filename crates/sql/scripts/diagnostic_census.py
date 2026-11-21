#!/usr/bin/env python3
"""Collect message-catalog evidence without executing Python pipeline tests.

Calcite matches are candidates, not an approved message catalog. Keep ambiguous
matches and unresolved Python expressions visible for the spike 9 review.
"""

import argparse
import ast
import collections
import json
import re
from pathlib import Path


def message_pattern(template):
    """Translate MessageFormat literals/arguments; reject nested choice formats."""
    parts, literals, literal = [], [], []
    quoted = False
    index = 0

    def flush():
        text = "".join(literal)
        literals.append(text)
        parts.append(re.escape(text))
        literal.clear()

    while index < len(template):
        char = template[index]
        if char == "'":
            if template[index : index + 2] == "''":
                literal.append("'")
                index += 2
                continue
            quoted = not quoted
        elif char == "{" and not quoted:
            end = template.find("}", index)
            argument = template[index + 1 : end] if end >= 0 else ""
            if not re.fullmatch(r"\d+(?:,(?:number|date|time)(?:,[^{}]*)?)?", argument):
                raise ValueError("unsupported MessageFormat argument")
            flush()
            parts.append(".*?")
            index = end
        else:
            literal.append(char)
        index += 1
    flush()
    return re.compile("".join(parts), re.DOTALL), literals


def string_value(node):
    """Evaluate only literal strings and concatenation, never Python code."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = string_value(node.left), string_value(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def python_expectations(root):
    rows, unresolved, forwarded = [], [], []
    for path in sorted((root / "python/tests/runtime_aggtest").rglob("*.py")):
        source = path.read_text()
        for node in ast.walk(ast.parse(source, filename=str(path))):
            if isinstance(node, ast.Assign):
                targets, value = node.targets, node.value
            elif isinstance(node, ast.AnnAssign):
                targets, value = [node.target], node.value
            else:
                continue
            if not any(
                (isinstance(t, ast.Attribute) and t.attr == "expected_error")
                or (isinstance(t, ast.Name) and t.id == "expected_error")
                for t in targets
            ):
                continue
            if value is None or isinstance(value, ast.Constant) and value.value is None:
                continue
            location = {"path": str(path.relative_to(root)), "line": node.lineno}
            if (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Name)
                and value.func.id == "getattr"
                and len(value.args) == 3
                and isinstance(value.args[1], ast.Constant)
                and value.args[1].value == "expected_error"
                and isinstance(value.args[2], ast.Constant)
                and value.args[2].value is None
            ):
                forwarded.append(
                    {**location, "expression": ast.get_source_segment(source, value)}
                )
                continue
            expected = string_value(value)
            if expected is None:
                unresolved.append(
                    {**location, "expression": ast.get_source_segment(source, value)}
                )
            elif expected.strip():
                rows.append({**location, "expected": expected, "is_regex": False})
    return rows, unresolved, forwarded


def platform_expectation(root):
    """Retain the platform's exact rendered SQL error, including source carets.

    This assertion has a different contract from runtime_aggtest's case-insensitive
    fragments. Read its known call site explicitly and fail if its shape changes;
    treating an unrecognized assertion as absent would undercount the inventory.
    """
    path = root / "python/tests/platform/negative_test.py"
    source = path.read_text()
    methods = [
        method
        for cls in ast.parse(source).body
        if isinstance(cls, ast.ClassDef) and cls.name == "NegativeCompilationTests"
        for method in cls.body
        if isinstance(method, ast.FunctionDef) and method.name == "test_sql_error"
    ]
    if len(methods) != 1:
        raise ValueError("platform SQL-error test is missing or ambiguous")
    method = methods[0]
    assignments = {}
    for node in method.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assignments.setdefault(target.id, []).append(node)
    expected = assignments.get("expected", [])
    sql = assignments.get("sql", [])
    assertions = [
        node
        for node in method.body
        if isinstance(node, ast.Assert)
        and isinstance(node.test, ast.Compare)
        and len(node.test.ops) == 1
        and isinstance(node.test.ops[0], ast.Eq)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "expected"
        and isinstance(node.test.comparators[0], ast.Name)
        and node.test.comparators[0].id == "got_err"
    ]
    if len(expected) != 1 or len(sql) != 1 or len(assertions) != 1:
        raise ValueError(
            "platform SQL-error assertion changed; update the inventory reader"
        )
    value = expected[0].value
    if not (
        isinstance(value, ast.Call)
        and isinstance(value.func, ast.Attribute)
        and value.func.attr == "strip"
        and not value.args
        and not value.keywords
        and isinstance(value.func.value, ast.JoinedStr)
    ):
        raise ValueError("unsupported platform SQL-error expectation")
    parts = []
    for part in value.func.value.values:
        if isinstance(part, ast.Constant) and isinstance(part.value, str):
            parts.append({"literal": part.value})
        elif (
            isinstance(part, ast.FormattedValue)
            and isinstance(part.value, ast.Name)
            and part.conversion == -1
            and part.format_spec is None
        ):
            parts.append({"parameter": part.value.id})
        else:
            raise ValueError(
                "unsupported interpolation in platform SQL-error expectation"
            )
    query = string_value(sql[0].value)
    if query is None:
        raise ValueError("platform SQL-error query is no longer a literal")
    return {
        "path": str(path.relative_to(root)),
        "source_test": "NegativeCompilationTests.test_sql_error",
        "line": expected[0].lineno,
        "assertion_line": assertions[0].lineno,
        "comparison": "exact_after_strip",
        "parts": parts,
        "sql": query,
        "executed": False,
    }


def census(directory, root):
    manifest = json.loads((directory / "complete.json").read_text())
    if manifest["schema_version"] != 1 or not manifest["junit_successful"]:
        raise ValueError("a completed, passing JUnit export is required")
    resources = json.loads((directory / "calcite-messages.json").read_text())
    patterns, unsupported = {}, []
    for key, template in sorted(resources.items()):
        try:
            patterns[key] = message_pattern(template)
        except ValueError:
            unsupported.append(key)
    java = [
        json.loads(line)
        for line in (directory / "java-diagnostics.jsonl").read_text().splitlines()
    ]
    if not java or len(java) != manifest["records"]:
        raise ValueError("diagnostic record count disagrees with completion manifest")
    matched_keys = collections.Counter()
    unmatched_messages = collections.Counter()
    java_rows = []
    for row in java:
        if row["schema_version"] != 1:
            raise ValueError("unsupported diagnostic record version")
        matches = []
        for diagnostic in row["diagnostics"]:
            message = diagnostic["message"]
            keys = [
                key
                for key, (pattern, _) in patterns.items()
                if pattern.fullmatch(message)
            ]
            matches.append(
                {
                    "error_type": diagnostic["error_type"],
                    "message": message,
                    "calcite_candidates": keys,
                }
            )
            if len(keys) == 1:
                matched_keys[keys[0]] += 1
            elif not keys:
                unmatched_messages[message] += 1
        java_rows.append(
            {
                key: row[key]
                for key in ("source_test", "call_site", "expected", "is_regex")
            }
            | {"messages": matches}
            | {key: row[key] for key in ("assertion_kind", "rendered", "visible_diagnostics", "field_matches") if key in row}
        )
    python, unresolved, forwarded = python_expectations(root)
    platform = platform_expectation(root)
    for row in python:
        fragment = row["expected"].strip().lower()
        # Python tests compare lower-cased substrings. Literal-substring matches
        # can be ambiguous (e.g., "not found"); report every candidate.
        row["calcite_candidates"] = [
            key
            for key, (pattern, literals) in patterns.items()
            if re.fullmatch(pattern.pattern, fragment, re.DOTALL | re.IGNORECASE)
            or any(fragment in literal.lower() for literal in literals)
        ]
    return {
        "schema_version": 1,
        "build": manifest,
        "requires_catalog_review": True,
        "counts": {
            "java_invocations": len(java),
            "java_call_sites": len({r["call_site"] for r in java}),
            "java_tests": len({r["source_test"] for r in java}),
            "java_distinct_expectations": len(
                {(r["expected"], r["is_regex"]) for r in java}
            ),
            "python_expectations": len(python),
            "python_distinct_expectations": len(
                {r["expected"].strip().lower() for r in python}
            ),
            "unresolved_python_expressions": len(unresolved),
            "python_expectation_forwarders": len(forwarded),
            "platform_exact_expectations": 1,
            "calcite_resource_templates": len(resources),
            "unambiguously_matched_calcite_keys": len(matched_keys),
            "distinct_unmatched_java_messages": len(unmatched_messages),
        },
        "matched_calcite_keys": dict(sorted(matched_keys.items())),
        "unsupported_resource_formats": unsupported,
        "java": java_rows,
        "python": python,
        "unresolved_python": unresolved,
        "python_expectation_forwarders": forwarded,
        "platform": [platform],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("diagnostic_export", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parents[3]
    )
    args = parser.parse_args()
    report = census(args.diagnostic_export, args.repo)
    with args.output.open("x") as output:
        json.dump(report, output, indent=2, ensure_ascii=False)
        output.write("\n")
    print(json.dumps(report["counts"], indent=2))


if __name__ == "__main__":
    main()
