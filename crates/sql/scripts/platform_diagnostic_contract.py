#!/usr/bin/env python3
"""Replay the platform SQL-error text contract through actual SDK source methods.

Only the SDK constructor and compilation-wait method are loaded. The response
provider is local and cannot make HTTP requests. This verifies formatting with
captured compiler diagnostics, not pipeline provisioning or a platform run.
"""

import argparse
import ast
import hashlib
import json
import logging
import time
from pathlib import Path

from diagnostic_census import platform_expectation


def load_method(path, class_name, method_name, namespace, *, as_class=False):
    tree = ast.parse(path.read_text(), filename=str(path))
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name]
    methods = [node for cls in classes for node in cls.body
               if isinstance(node, ast.FunctionDef) and node.name == method_name]
    if len(classes) != 1 or len(methods) != 1:
        raise ValueError("SDK method is missing or ambiguous")
    node = methods[0]
    if as_class:
        node = ast.ClassDef(name=class_name, bases=[], keywords=[], body=[node], decorator_list=[])
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), "exec"), namespace)
    return namespace[class_name if as_class else method_name]


def sdk_exception(root, sql, diagnostics, name):
    """Execute unchanged SDK method bodies against a deterministic local response."""
    namespace = {"time": time, "logger": logging.getLogger("p0-platform-contract")}
    for relative in ("python/feldera/enums.py", "python/feldera/_long_operation_warning.py"):
        path = root / relative
        exec(compile(path.read_text(), str(path), "exec"), namespace)
    pipeline_type = load_method(root / "python/feldera/rest/pipeline.py", "Pipeline", "__init__", namespace, as_class=True)
    pipeline = pipeline_type(name, sql, "", "", {}, {})
    pipeline.program_status = "SqlError"
    pipeline.program_error = {"sql_compilation": {"messages": diagnostics}}
    wait = load_method(root / "python/feldera/rest/feldera_client.py", "FelderaClient", "_wait_for_compilation", namespace)
    calls = []

    class LocalResponses:
        def get_pipeline(self, requested_name, selector):
            if requested_name != name or selector.name not in ("STATUS", "ALL"):
                raise AssertionError("unexpected SDK response request")
            calls.append({"name": requested_name, "selector": selector.name})
            return pipeline

    try:
        wait(LocalResponses(), name, poll_interval_s=0)
    except RuntimeError as error:
        return {"exception_type": type(error).__name__, "exception_text": error.args[0],
                "normalized_sql": pipeline.program_code, "response_requests": calls}
    raise AssertionError("SDK did not raise the expected SQL-compilation exception")


def verify(root, compiler_input, compiler_result):
    contract = platform_expectation(root)
    name = "p0_platform_sql_error"
    expected = "".join(part["literal"] if "literal" in part else name
                       for part in contract["parts"]
                       if "literal" in part or part.get("parameter") == "pipeline_name").strip()
    if any(part.get("parameter") not in (None, "pipeline_name") for part in contract["parts"]):
        raise ValueError("unreviewed platform interpolation")
    if not compiler_result["compiler_rejected"] or compiler_result["circuit_produced"]:
        raise ValueError("platform negative SQL unexpectedly compiled")
    replay = sdk_exception(root, contract["sql"], compiler_result["diagnostics"], name)
    if replay["normalized_sql"] != compiler_input["records"][0]["sql"]:
        raise ValueError("compiler SQL differs from SDK normalization")
    if replay["exception_text"].strip() != expected:
        raise ValueError("SDK output differs from exact platform test expectation")
    return {"schema_version": 1, "source_test": contract["source_test"], "expected": expected,
            "actual_after_strip": replay["exception_text"].strip(), "diagnostics": compiler_result["diagnostics"],
            "sdk_replay": replay, "compiler_options": compiler_result["options"],
            "comparison": "exact_after_strip", "matched": True,
            "body_root": "calcite:ObjectNotFound", "additional_body_roots": 0,
            "other_requirements": ["SDK pipeline-name prefix", "error-class line", "Code snippet label",
                                   "source-line selection", "caret coordinates", "newline/strip behavior"],
            "sdk_method_executed": True, "network_used": False, "full_platform_test_executed": False,
            "scope": "Frozen compiler output plus actual SDK method replay with local responses; no provisioning, HTTP, or deployment claim"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("root", "compiler_input", "compiler_result", "output"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    results = [json.loads(line) for line in args.compiler_result.read_text().splitlines()]
    if len(results) != 1:
        raise ValueError("exactly one platform compiler probe is required")
    report = verify(args.root, json.loads(args.compiler_input.read_text()), results[0])
    paths = ["python/tests/platform/negative_test.py", "python/feldera/rest/pipeline.py",
             "python/feldera/rest/feldera_client.py", "python/feldera/enums.py",
             "python/feldera/_long_operation_warning.py"]
    report["sources"] = {name: hashlib.sha256((args.root / name).read_bytes()).hexdigest() for name in paths}
    report["inputs"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                        for path in (args.compiler_input, args.compiler_result)}
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({k: report[k] for k in ("matched", "additional_body_roots", "full_platform_test_executed")}))


if __name__ == "__main__":
    main()
