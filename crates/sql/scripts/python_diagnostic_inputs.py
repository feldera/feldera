#!/usr/bin/env python3
"""Extract literal negative-test SQL and imported table fixtures without imports.

This models the default APPEND_ONLY=False registration path. Disabled class
prefixes stay in the report; dynamic SQL and unknown registration shapes fail.
It does not run Python tests, load input records, or evaluate expected runtime errors.
"""

import argparse
import ast
import hashlib
import json
from pathlib import Path

from diagnostic_census import string_value


def attribute(node, name):
    return isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "self" and node.attr == name


def class_value(cls, name):
    values = []
    direct = {
        id(statement)
        for method in cls.body if isinstance(method, ast.FunctionDef) and method.name == "__init__"
        for statement in method.body
    }
    for node in ast.walk(cls):
        if isinstance(node, ast.Assign) and any(attribute(target, name) for target in node.targets):
            if id(node) not in direct:
                raise ValueError(f"conditional or indirect {name} assignment in {cls.name}")
            values.append((node.lineno, string_value(node.value)))
        elif isinstance(node, ast.AugAssign) and attribute(node.target, name):
            raise ValueError(f"dynamic {name} in {cls.name}")
    if len(values) != 1 or values[0][1] is None:
        raise ValueError(f"expected one literal {name} in {cls.name}")
    return values[0]


def beautify(sql):
    return "\n".join(line.strip() for line in sql.split("\n"))


def extract(census, root):
    sources, modules, suites = {}, {}, {}

    def parse(path):
        path = path.resolve()
        relative = str(path.relative_to(root.resolve()))
        if path not in modules:
            contents = path.read_bytes()
            sources[relative] = hashlib.sha256(contents).hexdigest()
            modules[path] = ast.parse(contents, filename=relative)
        return modules[path]

    base = parse(root / "python/tests/runtime_aggtest/aggtst_base.py")
    append_only = [node.value for node in base.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "APPEND_ONLY" for target in node.targets)]
    if len(append_only) != 1 or not isinstance(append_only[0], ast.Constant) or append_only[0].value is not False:
        raise ValueError("only the default APPEND_ONLY=False fixture path is supported")
    parse(root / "python/tests/runtime_aggtest/atest_register_tests.py")
    parse(root / "python/tests/runtime_aggtest/atest_run.py")

    def suite(directory):
        if directory in suites:
            return suites[directory]
        main = parse(directory / "main.py")
        calls = [node for node in ast.walk(main) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name) and node.func.id == "run"]
        if len(calls) != 1 or len(calls[0].args) != 2:
            raise ValueError(f"unknown registration shape in {directory}")
        prefix = string_value(calls[0].args[1])
        if not prefix:
            raise ValueError("dynamic registration prefix")
        module_prefixes = ["test_"]
        for keyword in calls[0].keywords:
            if keyword.arg != "prefix_matches" or not isinstance(keyword.value, ast.List):
                raise ValueError("unknown registration keyword")
            module_prefixes = [string_value(value) for value in keyword.value.elts]
            if not module_prefixes or any(not value for value in module_prefixes):
                raise ValueError("dynamic module prefixes")
        imports = []
        for node in main.body:
            if not isinstance(node, ast.ImportFrom) or not node.module:
                continue
            module = node.module
            if module.startswith("test_"):
                path = directory / (module + ".py")
            elif module.startswith("tests.runtime_aggtest.") and module.rsplit(".", 1)[-1].startswith("test_"):
                path = root / "python" / (module.replace(".", "/") + ".py")
            else:
                continue
            if node.level or [item.name for item in node.names] != ["*"]:
                raise ValueError("unknown test import shape")
            if any(module.startswith(value) for value in module_prefixes):
                imports.append(path.resolve())
        tables = []
        for path in imports:
            for cls in sorted((node for node in parse(path).body if isinstance(node, ast.ClassDef)), key=lambda node: node.name):
                if not cls.name.startswith(prefix):
                    continue
                bases = [ast.unparse(base) for base in cls.bases]
                if bases not in [["TstTable"], ["TstView"]]:
                    raise ValueError(f"unknown registered class bases: {cls.name}")
                if bases == ["TstTable"]:
                    if any(isinstance(node, ast.Assign) and any(attribute(target, "expected_error") for target in node.targets) for node in ast.walk(cls)):
                        continue
                    _, sql = class_value(cls, "sql")
                    tables.append({"source": str(path.relative_to(root.resolve())), "class": cls.name, "sql": beautify(sql)})
        suites[directory] = (prefix, set(imports), tables)
        return suites[directory]

    records = []
    for expected in census["python"]:
        path = (root / expected["path"]).resolve()
        classes = [cls for cls in parse(path).body if isinstance(cls, ast.ClassDef)
                   and cls.lineno <= expected["line"] <= cls.end_lineno]
        if len(classes) != 1:
            raise ValueError("expectation has no unique owning class")
        cls = classes[0]
        line, value = class_value(cls, "expected_error")
        if line != expected["line"] or value != expected["expected"]:
            raise ValueError("expectation differs from pinned census")
        _, sql = class_value(cls, "sql")
        prefix, imports, tables = suite(path.parent)
        active = path in imports and cls.name.startswith(prefix)
        bases = [ast.unparse(base) for base in cls.bases]
        if bases not in [["TstTable"], ["TstView"]]:
            raise ValueError("unknown negative-test class bases")
        inputs = tables if bases == ["TstView"] else []
        program = "".join(table["sql"] + ";\n\n" for table in inputs) + "\n" + beautify(sql) + ";\n\n"
        records.append({"id": expected["path"] + ":" + cls.name, "expectation_line": line,
                        "expected": value, "active": active,
                        "disposition": "compile_probe" if active else "not_registered_by_suite_prefix_or_imports",
                        "tables": inputs, "sql": program})
    return {"schema_version": 1, "complete": True, "records": records, "source_hashes": sources,
            "active": sum(row["active"] for row in records), "disabled": sum(not row["active"] for row in records),
            "runtime_executed": False, "python_tests_executed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("census", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args()
    report = extract(json.loads(args.census.read_text()), args.repo)
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({key: report[key] for key in ["active", "disabled"]}))


if __name__ == "__main__":
    main()
