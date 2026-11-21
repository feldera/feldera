#!/usr/bin/env python3
"""Account for all registered Python expectations and extend compiler roots only."""

import argparse
import collections
import copy
import hashlib
import json
from pathlib import Path

from diagnostic_catalog import digest
from diagnostic_expectation_attribution import field_matches
from python_diagnostics import summarize


def record_digest(record):
    return digest(json.dumps(record, sort_keys=True, ensure_ascii=False))


def integrate(base, inputs, results, review, runtime=None, platform=None):
    outcomes = summarize(inputs, results)
    if outcomes["compiler_expectation_mismatches"] or outcomes["internal_compiler_failures"]:
        raise ValueError("compiler outcomes are not fully classified")
    bindings = review["bindings"]
    if len(bindings) != len(inputs["records"]):
        raise ValueError("Python review population differs")
    roots = {r["id"]: copy.deepcopy(r) for r in base["roots"]}
    dependencies = {r["id"]: copy.deepcopy(r) for r in base["dependencies"]}
    for root in review["additional_body_roots"]:
        if root["id"] in roots:
            raise ValueError("additional Python root duplicates existing root")
        roots[root["id"]] = copy.deepcopy(root)
    for dep in review["additional_dependencies"]:
        if dependencies.setdefault(dep["id"], dep) != dep:
            raise ValueError("Python dependency conflicts with prior catalog")
    for source, result, binding in zip(inputs["records"], results, bindings):
        if (binding["id"] != source["id"] or binding["input_sha256"] != record_digest(source)
                or binding["result_sha256"] != record_digest(result)):
            raise ValueError("Python provenance differs")
        ids = set(binding["body_roots"])
        if not ids <= roots.keys():
            raise ValueError("unknown Python compiler root")
        if not source["active"] or not result.get("compiler_rejected"):
            if ids or binding["body_evidence"]:
                raise ValueError("disabled/runtime case invents compiler root")
            continue
        hits = field_matches(source["expected"], result["diagnostics"], ignore_case=True)
        expected_indices = {hit["diagnostic_index"] for hit in hits if "message" in hit["fields"]}
        evidence_indices, evidence_roots = set(), set()
        for evidence in binding["body_evidence"]:
            index = evidence["diagnostic_index"]
            actual = result["diagnostics"][index]
            if any(evidence[k] != actual[k] for k in ("message", "error_type")) or index not in expected_indices:
                raise ValueError("Python body attribution differs")
            evidence_indices.add(index)
            evidence_roots.update(evidence["roots"])
        if evidence_indices != expected_indices or evidence_roots != ids:
            raise ValueError("Python body attribution is incomplete")
        if ids and any(set(hit["fields"]) != {"message"} for hit in hits):
            raise ValueError("Python body has an unresolved non-body alternative")
        for identity in ids:
            roots[identity].setdefault("python_compiler_witnesses", []).append(source["id"])
    if any(not r.get("mandatory_witnesses") and not r.get("direct_mandatory_witnesses")
           and not r.get("python_compiler_witnesses") for r in roots.values()):
        raise ValueError("root has no assertion witness")
    result = copy.deepcopy(base)
    result["scope"] = "Captured Java contracts and extracted Python expectations, with optional platform replay; compiler body roots only"
    result["roots"] = sorted(roots.values(), key=lambda r: r["id"])
    result["dependencies"] = sorted(dependencies.values(), key=lambda r: r["id"])
    result["python_expectations"] = copy.deepcopy(bindings)
    runtime_ids = {source["id"] for source, outcome in zip(inputs["records"], results)
                   if source["active"] and not outcome["compiler_rejected"]}
    if runtime is not None:
        if runtime.get("complete") is not True:
            raise ValueError("runtime attribution is incomplete")
        cases = runtime["cases"]
        if len(cases) != len(runtime_ids) or {case["id"] for case in cases} != runtime_ids:
            raise ValueError("runtime attribution population differs")
        expected = {source["id"]: source["expected"] for source in inputs["records"]}
        if any(case["expected"] != expected[case["id"]]
               or case["root"] not in runtime["error_body_roots"] for case in cases):
            raise ValueError("runtime attribution contract differs")
        for binding in result["python_expectations"]:
            if binding["id"] in runtime_ids:
                binding["compiler_only_disposition"] = binding["disposition"]
                binding["disposition"] = "runtime_contract_accounted_separately"
        result["separate_runtime_attribution"] = {
            "certificate": "evidence/python-runtime-error-attribution.json",
            "certificate_sha256": record_digest(runtime),
            "cases": len(cases), "counts": runtime["counts"],
            "added_to_compiler_root_count": False,
        }
    result["python_ambiguity_resolutions"] = review["ambiguity_resolutions"]
    result["counts"].update({"java_required_body_roots": len(base["roots"]),
                             "java_covered_assertions": base["counts"].get("covered_assertions", 0),
                             "covered_assertions": base["counts"].get("covered_assertions", 0) + outcomes["counts"]["compiler_rejected"],
                             "python_expectations": len(bindings),
                             "python_dispositions": dict(collections.Counter(b["disposition"] for b in result["python_expectations"])),
                             "additional_python_compiler_body_roots": len(review["additional_body_roots"]),
                             "covered_required_body_roots": len(roots),
                             "covered_required_compiler_body_roots": len(roots),
                             "roots_by_kind": dict(collections.Counter(r["kind"] for r in roots.values())),
                             "compiler_roots_by_kind": dict(collections.Counter(r["kind"] for r in roots.values()))})
    result["uncounted_populations"] = [p for p in base["uncounted_populations"] if p["population"] != "Python compiler assertions"]
    if runtime is not None:
        result["uncounted_populations"] = [p for p in result["uncounted_populations"]
                                             if p["population"] != "Python runtime assertions"]
    if platform is not None:
        if (platform.get("matched") is not True or platform.get("additional_body_roots") != 0
                or platform.get("body_root") not in roots):
            raise ValueError("platform body attribution differs")
        result["platform_contract"] = copy.deepcopy(platform)
        result["counts"]["platform_contracts"] = 1
        result["counts"]["covered_assertions"] += 1
        result["uncounted_populations"] = [p for p in result["uncounted_populations"]
                                             if p["population"] != "platform SDK rendering"]
    result["uncounted_populations"].append({
        "population": "Java runtime expectations",
        "status": "232 exported runtime contracts in v9; source inventory is separate and template identities remain unresolved.",
    })
    result["normal_python_suite_executed"] = False
    result["runtime_execution_in_this_report"] = False
    result["whole_suite_required_template_count"] = None
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("base", "inputs", "results", "review", "output"):
        parser.add_argument(name, type=Path)
    parser.add_argument("--runtime-attribution", type=Path)
    parser.add_argument("--platform-report", type=Path)
    args = parser.parse_args()
    report = integrate(json.loads(args.base.read_text()), json.loads(args.inputs.read_text()),
                       [json.loads(line) for line in args.results.read_text().splitlines()], json.loads(args.review.read_text()),
                       json.loads(args.runtime_attribution.read_text()) if args.runtime_attribution else None,
                       json.loads(args.platform_report.read_text()) if args.platform_report else None)
    report["inputs"] = {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                        for p in (args.base, args.inputs, args.results, args.review)}
    if args.runtime_attribution:
        report["inputs"][str(args.runtime_attribution)] = hashlib.sha256(args.runtime_attribution.read_bytes()).hexdigest()
    if args.platform_report:
        report["inputs"][str(args.platform_report)] = hashlib.sha256(args.platform_report.read_bytes()).hexdigest()
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps(report["counts"], indent=2))


if __name__ == "__main__":
    main()
