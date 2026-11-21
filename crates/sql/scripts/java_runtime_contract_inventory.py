#!/usr/bin/env python3
"""Validate the selected v9 runtime contracts and anchor them in frozen Java tests.

This reads existing corpus exports. It neither executes runtime tests nor turns
expected substrings into message-template identities.
"""

import argparse
import collections
import hashlib
import json
import re
from pathlib import Path


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def method_anchor(java_sources, classname, method, sources):
    visited = set()
    while classname not in visited:
        visited.add(classname)
        relative = classname.replace(".", "/") + ".java"
        path = java_sources / relative
        text = path.read_text()
        sources[relative] = sha256(path)
        declarations = list(re.finditer(r"\bvoid\s+" + re.escape(method) + r"\s*\(", text))
        if len(declarations) == 1:
            return relative, text.count("\n", 0, declarations[0].start()) + 1
        if declarations:
            raise ValueError("ambiguous test method: " + classname + "#" + method)
        parent = re.search(r"\bclass\s+" + re.escape(classname.rsplit(".", 1)[-1])
                           + r"\s+extends\s+([\w.]+)", text)
        if parent is None:
            break
        parent = parent.group(1)
        imports = re.findall(r"\bimport\s+([\w.]+\." + re.escape(parent) + r");", text)
        classname = imports[0] if len(imports) == 1 else classname.rsplit(".", 1)[0] + "." + parent
    raise ValueError("test method missing: " + classname + "#" + method)


def inventory(selected, java_sources):
    initial = json.loads((selected / "inventory.json").read_text())
    rows, sources = [], {}
    for row in initial["rows"]:
        path = selected / "cases" / row["case_path"]
        if sha256(path) != row["sha256"]:
            raise ValueError("runtime case content differs")
        case = json.loads(path.read_text())
        for key in ("source_test", "case_index", "expected_runtime_error", "status", "runtime_verified", "engine_mode"):
            if case[key] != row[key]:
                raise ValueError("runtime case identity or disposition differs")
        producer = [case[k] for k in ("producer_commit", "calcite_commit", "runtime_commit")]
        if producer != initial["producer"]:
            raise ValueError("runtime corpus mixes producer snapshots")
        classname, method = case["source_test"].split("#", 1)
        relative, line = method_anchor(java_sources, classname, method, sources)
        rows.append(dict(row, source_file=relative,
                         source_line=line,
                         comparison="Rust should_panic(expected) substring",
                         source_template_identity=None))
    if len({(r["source_test"], r["case_index"]) for r in rows}) != len(rows):
        raise ValueError("duplicate runtime contract")
    for name in ("CorpusExporter", "TestCase", "BaseSQLTests", "SqlIoTest"):
        relative = "org/dbsp/sqlCompiler/compiler/sql/tools/" + name + ".java"
        sources[relative] = sha256(java_sources / relative)
    counts = {"runtime_contract_cases": len(rows),
              "runtime_contract_tests": len({r["source_test"] for r in rows}),
              "distinct_expectation_strings": len({r["expected_runtime_error"] for r in rows}),
              "runtime_verified_cases": sum(r["runtime_verified"] for r in rows)}
    for key, value in counts.items():
        if initial[key] != value:
            raise ValueError("initial runtime selection counts differ")
    groups = collections.defaultdict(list)
    for row in rows:
        groups[row["expected_runtime_error"]].append({"source_test": row["source_test"], "case_index": row["case_index"]})
    return {"schema_version": 1, "capture": initial["capture"], "producer": initial["producer"],
            "scope": "All selected v9 expected_runtime_error contracts; source anchors and exported predicates only",
            "counts": counts, "initial_selection_sha256": sha256(selected / "inventory.json"),
            "original_scan_total_exported_cases": initial["total_exported_cases"],
            "original_scan_total_rechecked_by_this_tool": False,
            "rows": rows, "expectation_groups": dict(sorted(groups.items())), "sources": sources,
            "required_template_count": None,
            "unresolved_template_contracts": len(rows),
            "runtime_execution_required_to_finish_count": False,
            "remaining_work": "Use typed circuit/helper source paths to select and deduplicate actual formatting roots; broad substrings alone do not prove a root."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("selected", "java_sources", "output"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    report = inventory(args.selected, args.java_sources)
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps(report["counts"]))


if __name__ == "__main__":
    main()
