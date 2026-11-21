#!/usr/bin/env python3
"""Reconcile the full correlation census, prototype outcomes, and C7 witnesses.

Occurrence weights come from the census, including transport failures and
refusals. Runtime fixtures never create extra corpus occurrence weight.
"""

import argparse
from collections import Counter, defaultdict
import copy
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET

from correlation_inventory import STAGES, case_inventory

STAGE = "Decorrelate inner queries 1"


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text())


def lines(path):
    with path.open() as stream:
        return [json.loads(line) for line in stream]


def indexed(records):
    result = {record["id"]: record for record in records}
    if len(result) != len(records):
        raise ValueError("duplicate record identity")
    return result


def equal_without_id(expected, actual):
    left, right = copy.deepcopy(expected), copy.deepcopy(actual)
    left.pop("id", None)
    right.pop("id", None)
    return canonical(left) == canonical(right)


def census_signatures(graph):
    nodes = graph["relations"]
    return Counter((n["correlate"]["join_type"], n["correlate"]["correlation"],
                    tuple(n["correlate"]["required_columns"]),
                    tuple(nodes[i]["kind"] for i in n["inputs"]))
                   for n in nodes if n["kind"] == "LogicalCorrelate")


def fragment_signatures(fragment):
    nodes = fragment["producers"]["relations"]
    fields = fragment["properties"]["relations"]
    return Counter((fields[i]["fields"]["join_kind"], fields[i]["fields"]["correlation"],
                    tuple(fields[i]["fields"]["required"]),
                    tuple(nodes[child]["form"] for child in node["inputs"]))
                   for i, node in enumerate(nodes) if node["form"] == "LogicalCorrelate")


def validate_runtime(root, review, fragments, recipes):
    runs, witnesses = {}, {}
    for name, contract in review["runs"].items():
        directory = root / name
        log = (directory / "tests.log").read_text()
        report_path = directory / "surefire-reports" / ("TEST-" + contract["junit_class"] + ".xml")
        report = ET.parse(report_path).getroot()
        if (report.attrib["name"] != contract["junit_class"]
                or int(report.attrib["tests"]) != contract["junit_tests"]
                or any(int(report.attrib[k]) for k in ("failures", "errors", "skipped"))
                or "BUILD SUCCESS" not in log):
            raise ValueError("runtime test did not complete successfully")
        executions = re.findall(r"Executed (\d+), checked 0 Rust tests", log)
        if executions != [str(contract["rust_tests"])]:
            raise ValueError("actual Rust execution count differs")
        generated = (directory / "generated/lib.rs").read_text()
        if (generated.count("#[test]") != contract["rust_tests"]
                or len(re.findall(r"\.transaction\(", generated)) != contract["transactions"]):
            raise ValueError("generated Rust test or transaction count differs")
        runs[name] = dict(contract, log_sha256=sha(directory / "tests.log"),
                          surefire_sha256=sha(report_path), generated_rust_sha256=sha(directory / "generated/lib.rs"))
    for contract in review["cases"]:
        case = contract["id"]
        directory = root / contract["run"] / contract["path"]
        paths = sorted(directory.glob("invocation-*/fragment.jsonl"))
        if not paths:
            raise ValueError("runtime case has no transformation witness")
        for path in paths:
            invocation = int(path.parent.name.removeprefix("invocation-"))
            identity = f"{case}/correlation-stage/{invocation}"
            if identity in witnesses or identity not in fragments:
                raise ValueError("runtime witness is duplicate or absent from corpus")
            captured = lines(path)
            outcomes = lines(path.parent / "rust/recipes.jsonl")
            if len(captured) != 1 or len(outcomes) != 1:
                raise ValueError("runtime invocation must contain one fragment and recipe")
            if outcomes[0]["id"] != captured[0]["id"] or outcomes[0]["recipe"]["id"] != captured[0]["id"]:
                raise ValueError("runtime fragment and recipe identities differ")
            completion = read(path.parent / "rust/complete.json")
            if not completion["complete"] or completion["rewritten"] != 1 or completion["refused"] != 0:
                raise ValueError("runtime transformation was refused or incomplete")
            if not equal_without_id(fragments[identity], captured[0]):
                raise ValueError("runtime fragment differs from original corpus")
            if (outcomes[0]["status"] != "rewritten" or recipes[identity]["status"] != "rewritten"
                    or not equal_without_id(recipes[identity]["recipe"], outcomes[0]["recipe"])):
                raise ValueError("runtime recipe differs from final measured prototype")
            witnesses[identity] = {"run": contract["run"], "case": case,
                                   "fragment": str(path.relative_to(root)), "fragment_sha256": sha(path),
                                   "recipe_sha256": sha(path.parent / "rust/recipes.jsonl"),
                                   "typed_fragment_and_recipe_equal_except_id": True}
    return runs, witnesses


def build(root, review):
    census_completion = read(root / "p0-correlation-corpus-v2/run-complete.json")
    if (census_completion["complete"] is not True
            or {row["shard"] for row in census_completion["shards"]} != set(range(6))
            or any(row["exit_code"] != 0 for row in census_completion["shards"])):
        raise ValueError("full six-shard census is incomplete")
    records, inputs = [], {}
    for path in sorted((root / "p0-correlation-corpus-v2").glob("shard-*/output/cases.jsonl")):
        manifest = read(path.parent / "complete.json")
        if not all(manifest.get(key) is True for key in ("complete", "exported_schemas_equal", "exported_diagnostics_equal")):
            raise ValueError("census shard has no successful oracle checks")
        shard = lines(path)
        if len(shard) != manifest["cases"]:
            raise ValueError("census shard population differs")
        records.extend(shard)
        inputs[str(path.relative_to(root))] = sha(path)
    census = case_inventory(records)
    captured = read(root / "p0-correlation-cases-v1.json")
    if any(census[k] != captured[k] for k in ("cases", "observed_stages", "cases_with_retained_stages", "stages")):
        raise ValueError("census differs from its recorded inventory")
    transport = root / "p0-correlation-plans-v2/java"
    fragments = indexed(lines(transport / "cases.jsonl"))
    coverage = indexed(lines(transport / "coverage.jsonl"))
    recipes = indexed(lines(root / "p0-correlate-scalar-corpus-v1/recipes.jsonl"))
    reconstructed = indexed(lines(root / "p0-correlate-scalar-corpus-reconstruction-v1/cases.jsonl"))
    if set(fragments) != set(recipes) or set(fragments) != set(reconstructed):
        raise ValueError("prototype or reconstruction omits transported inputs")
    for identity in fragments:
        if recipes[identity]["status"] != reconstructed[identity]["status"]:
            raise ValueError("recipe reconstruction disposition differs")
    outcomes = Counter(row["status"] for row in recipes.values())
    for directory in (root / "p0-correlate-scalar-corpus-v1", root / "p0-correlate-scalar-corpus-reconstruction-v1"):
        manifest = read(directory / "complete.json")
        if (manifest["complete"] is not True or manifest["fragments"] != len(fragments)
                or manifest["rewritten"] != outcomes["rewritten"] or manifest["refused"] != outcomes["refused"]):
            raise ValueError("prototype or reconstruction completion disagrees with records")
    transport_complete = read(transport / "complete.json")
    if (transport_complete["complete"] is not True or transport_complete["fragments"] != len(fragments)
            or transport_complete["transport_failures"] != sum(len(row["failures"]) for row in coverage.values())):
        raise ValueError("transport completion disagrees with ledger population")
    runs, witnesses = validate_runtime(root, review, fragments, recipes)
    by_case = defaultdict(list)
    for identity, fragment in fragments.items():
        case, number = identity.rsplit("/correlation-stage/", 1)
        by_case[case].append({"invocation": int(number), "fragment": fragment})
    for case, row in coverage.items():
        if row["fragments"] != len(by_case[case]):
            raise ValueError("transport coverage count differs")
        for failure in row["failures"]:
            by_case[case].append({"invocation": failure["invocation"], "failure": failure["error"]})
    occurrences, invocations, related = [], [], []
    selected_cases = set()
    for record in records:
        if not record["stages"]:
            continue
        events = [event for event in record["stages"] if event["step"] == STAGE and event["before"]["correlates"]]
        related.append({"id": record["id"], "selected_first_inner_stage": bool(events),
                        "disposition": "first_inner_stage_ledger" if events else "related_at_other_stages_only",
                        "stages": [{"sequence": event["sequence"], "step": event["step"], "completed": event["completed"],
                                    "before_correlates": event["before"]["correlates"], "after_correlates": event["after"]["correlates"],
                                    "before_subqueries": event["before"]["subqueries"], "after_subqueries": event["after"]["subqueries"]}
                                   for event in record["stages"]]})
        if not events:
            continue
        case = record["id"]
        selected_cases.add(case)
        exported = sorted(by_case[case], key=lambda entry: entry["invocation"])
        if len(exported) != len(events):
            raise ValueError("census invocations differ from transported plus failed population")
        for event, entry in zip(events, exported):
            # The frozen census observes the same four callbacks in fixed order.
            # Fail closed if a future input skips or reorders those callbacks.
            if event["sequence"] != 4 * entry["invocation"] + 1:
                raise ValueError("census/export invocation identity cannot be aligned")
            identity = f"{case}/correlation-stage/{entry['invocation']}"
            count = event["before"]["correlates"]
            status, reason = "transport_failed", entry.get("failure")
            if "fragment" in entry:
                if census_signatures(event["before"]) != fragment_signatures(entry["fragment"]):
                    raise ValueError("typed fragment correlate signatures differ from census")
                outcome = recipes[identity]
                if outcome["status"] == "rewritten":
                    if outcome["recipe"]["removed_correlates"] != count:
                        raise ValueError("rewritten plan does not remove all census Correlates")
                    status = "c7_equal" if identity in witnesses else "rewritten_without_c7"
                elif outcome["status"] == "refused":
                    status, reason = "refused", outcome["reason"]
                else:
                    raise ValueError("unknown prototype disposition")
            row = {"id": identity, "case": case, "sequence": event["sequence"], "invocation": entry["invocation"],
                   "correlate_occurrences": count, "java_stage_completed": event["completed"],
                   "status": status, "reason": reason, "runtime_witness": witnesses.get(identity)}
            invocations.append(row)
            for node in event["before"]["relations"]:
                if node["kind"] != "LogicalCorrelate":
                    continue
                occurrences.append({"id": identity + f"/relation/{node['id']}", "fragment": identity, "case": case,
                                    "sequence": event["sequence"], "relation": node["id"], "correlate": node["correlate"],
                                    "row_type": node["row_type"], "input_kinds": [event["before"]["relations"][i]["kind"] for i in node["inputs"]],
                                    "status": status, "reason": reason})
    if selected_cases != set(coverage) or set(by_case) != selected_cases:
        raise ValueError("unaccounted selected source case")
    selection = read(root / "p0-correlation-plans-v2/selection.json")
    if set(selection["ids"]) != selected_cases:
        raise ValueError("selected source population differs")
    if set(witnesses) != {row["id"] for row in invocations if row["status"] == "c7_equal"}:
        raise ValueError("runtime witness not accounted in occurrence ledger")
    credited_cases = {contract["id"] for contract in review["cases"]}
    if any(row["status"] != "c7_equal" for row in invocations if row["case"] in credited_cases):
        raise ValueError("partially transformed case cannot receive whole-case credit")
    counts = Counter(row["status"] for row in occurrences)
    return {"schema_version": 1, "complete": True, "scope": "Full classic four-stage shape census; weighted C7 at the declared first-inner-stage boundary",
            "counts": {"corpus_cases": census["cases"], "related_cases": len(related), "selected_cases": len(selected_cases),
                       "other_related_cases": len(related) - len(selected_cases), "invocations": len(invocations),
                       "correlate_occurrences": len(occurrences), "occurrence_dispositions": dict(counts),
                       "invocation_dispositions": dict(Counter(row["status"] for row in invocations)),
                       "verified_original_cases": len(credited_cases)},
            "weighted_c7": {"numerator": counts["c7_equal"], "denominator": len(occurrences),
                            "fraction": counts["c7_equal"] / len(occurrences), "threshold": 0.95,
                            "current_prototype_ceiling_numerator": counts["c7_equal"] + counts["rewritten_without_c7"],
                            "passed": counts["c7_equal"] / len(occurrences) >= 0.95},
            "related_cases": related, "invocations": invocations, "occurrences": occurrences,
            "runtime_runs": runs, "census_inputs": inputs,
            "four_stage_global_c7_fraction": None, "fixture_weight_added": 0,
            "human_maintainer_accepted": False, "P0_complete": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("root", "runtime_review", "output"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    report = build(args.root, read(args.runtime_review))
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"counts": report["counts"], "weighted_c7": report["weighted_c7"]}, indent=2))


if __name__ == "__main__":
    main()
