#!/usr/bin/env python3
"""Integrate source-reviewed direct contracts without counting snapshots as roots."""

import argparse
import collections
import copy
import hashlib
import json
from pathlib import Path

from diagnostic_catalog import digest
from diagnostic_direct_assertions import classify


def record_digest(record):
    return digest(json.dumps(record, sort_keys=True, ensure_ascii=False))


def actual_texts(record):
    actual = record["actual"]
    if isinstance(actual, list):
        return [item for item in actual if isinstance(item, str)]
    texts = [actual] if isinstance(actual, str) else []
    if isinstance(actual, str) and actual.startswith("[{"):
        parsed = json.loads(actual)
        texts.extend(row["message"] for row in parsed if isinstance(row, dict) and isinstance(row.get("message"), str))
    return texts


def integrate(shared, records, review):
    bindings = review["bindings"]
    if len(records) != len(bindings):
        raise ValueError("direct population differs from review")
    roots = {r["id"]: copy.deepcopy(r) for r in shared["roots"]}
    dependencies = {r["id"]: copy.deepcopy(r) for r in shared["dependencies"]}
    for root in review["additional_body_roots"]:
        if root["id"] in roots:
            raise ValueError("additional root duplicates shared root")
        roots[root["id"]] = copy.deepcopy(root)
    for dependency in review["additional_dependencies"]:
        previous = dependencies.setdefault(dependency["id"], copy.deepcopy(dependency))
        if previous != dependency:
            raise ValueError("dependency conflicts with shared definition")
    fragments = {f["id"]: copy.deepcopy(f) for f in review["fragment_templates"]}
    if len(fragments) != len(review["fragment_templates"]):
        raise ValueError("duplicate fragment template")
    for fragment in fragments.values():
        fragment["direct_witnesses"] = []
    for index, (record, binding) in enumerate(zip(records, bindings)):
        disposition = classify(record)
        if (binding["record_index"] != index or binding["record_sha256"] != record_digest(record)
                or any(binding[k] != record[k] for k in ("source_test", "call_site"))):
            raise ValueError("direct assertion provenance differs")
        if binding["contract"] != disposition:
            # This one rendered assertion checks only a class name. Its explicit
            # review refines the capture's deliberately broader target label.
            if not (disposition == "rendered_text_requires_attribution"
                    and binding["contract"] == "non_body_contract"
                    and record["expected"] == "Error parsing SQL"):
                raise ValueError("direct contract differs from captured predicate")
        body_ids = set(binding["body_roots"])
        if not body_ids <= roots.keys():
            raise ValueError("direct assertion references an unknown body root")
        if binding["contract"] in ("absence_contract", "snapshot_stability", "non_body_contract"):
            if body_ids or binding["fragment_instances"] or binding["body_evidence"]:
                raise ValueError("non-body contract invents a positive body dependency")
        evidence_roots = set()
        for evidence in binding["body_evidence"]:
            if evidence["root"] not in body_ids or not any(evidence["message"] in text for text in actual_texts(record)):
                raise ValueError("reviewed body does not occur in the captured target")
            evidence_roots.add(evidence["root"])
        if evidence_roots != body_ids:
            raise ValueError("body root lacks captured evidence")
        for identity in body_ids:
            roots[identity].setdefault("direct_mandatory_witnesses", []).append(index)
        for instance in binding["fragment_instances"]:
            fragment = fragments.get(instance["template"])
            if fragment is None:
                raise ValueError("unknown fragment template")
            rendered = fragment["format"].format_map(instance["arguments"])
            if rendered != instance["message"] or rendered != record["actual"]:
                raise ValueError("fragment reconstruction differs from captured target")
            fragment["direct_witnesses"].append(index)
        if binding["contract"] == "formatter_fragment_requirement" and not binding["fragment_instances"]:
            raise ValueError("formatter assertion lacks a reviewed fragment")
        if binding["contract"] in ("message_body_requirement", "rendered_text_requires_attribution") and not body_ids:
            raise ValueError("positive diagnostic assertion has no body root")
    if any(not r.get("mandatory_witnesses") and not r.get("direct_mandatory_witnesses") for r in roots.values()):
        raise ValueError("catalog root lacks an assertion witness")
    if any(not f["direct_witnesses"] for f in fragments.values()):
        raise ValueError("fragment template lacks a direct witness")
    known_ids = roots.keys() | dependencies.keys()
    if any(dep not in known_ids for root in roots.values() for dep in root["dependencies"]):
        raise ValueError("root has an unresolved dependency reference")
    component_edges = review.get("component_edges", [])
    if any(edge["from"] not in roots or edge["to"] not in fragments for edge in component_edges):
        raise ValueError("component edge references an unknown root or fragment")
    return {"schema_version": 1,
            "scope": "Java v9 captured shared-helper and direct assertions; source-wide completeness is not inferred",
            "counting_rule": review["counting_rule"],
            "counts": {"shared_assertions": len(shared["assertions"]), "direct_assertions": len(records),
                       "covered_assertions": len(shared["assertions"]) + len(records),
                       "shared_body_roots": len(shared["roots"]),
                       "additional_direct_body_roots": len(review["additional_body_roots"]),
                       "covered_required_body_roots": len(roots),
                       "direct_fragment_templates": len(fragments),
                       "direct_fragment_assertions": sum(bool(b["fragment_instances"]) for b in bindings),
                       "direct_body_assertions": sum(bool(b["body_roots"]) for b in bindings),
                       "direct_contracts": dict(collections.Counter(b["contract"] for b in bindings)),
                       "roots_by_kind": dict(collections.Counter(r["kind"] for r in roots.values())),
                       "shared_dependency_nodes": len(dependencies)},
            "roots": sorted(roots.values(), key=lambda r: r["id"]),
            "dependencies": sorted(dependencies.values(), key=lambda r: r["id"]),
            "direct_fragment_templates": sorted(fragments.values(), key=lambda r: r["id"]),
            "component_edges": component_edges,
            "shared_assertions": shared["assertions"], "direct_assertions": bindings,
            "covered_threshold_result": "Under the declared counting rule, the covered body-root count alone exceeds 150; remaining populations cannot reverse that lower bound.",
            "uncounted_populations": [row for row in shared["uncounted_populations"] if row["population"] != "Java direct/specialized assertions"],
            "whole_suite_required_template_count": None, "human_maintainer_accepted": False, "P0_complete": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("shared", "direct", "review", "source_root", "output"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    review = json.loads(args.review.read_text())
    if hashlib.sha256(args.direct.read_bytes()).hexdigest() != review["input_sha256"]:
        raise ValueError("direct capture digest differs")
    for name, source in review["sources"].items():
        if hashlib.sha256((args.source_root / name).read_bytes()).hexdigest() != source["sha256"]:
            raise ValueError("reviewed source digest differs: " + name)
    result = integrate(json.loads(args.shared.read_text()),
                       [json.loads(line) for line in args.direct.read_text().splitlines()], review)
    result["inputs"] = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in (args.shared, args.direct, args.review)}
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps(result["counts"], indent=2))


if __name__ == "__main__":
    main()
