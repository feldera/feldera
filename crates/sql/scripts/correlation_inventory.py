#!/usr/bin/env python3
"""Inventory recorded Correlate shapes without treating observations as execution.

Each stage has its own denominator. A relation observed at several stages is
not several independent queries, and the historical journal has no case IDs.
"""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


STAGES = (
    "Remove sub-queries",
    "Decorrelate inner queries 1",
    "Decorrelate inner queries 2",
    "Decorrelate",
)
SHAPE_FIELDS = {"join_type", "required_columns", "left", "right", "row_type"}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def inventory(records):
    stages = {}
    total = 0
    for line, record in enumerate(records, 1):
        total += 1
        if record.get("schema_version") != 1:
            raise ValueError(f"line {line}: unsupported trace version")
        if record.get("step") not in STAGES:
            continue
        if type(record.get("completed")) is not bool:
            raise ValueError(f"line {line}: missing completion state")
        level = record.get("optimization_level")
        if type(level) is not int or not 0 <= level <= 2:
            raise ValueError(f"line {line}: invalid optimization level")
        key = record["step"], level
        stage = stages.setdefault(key, dict(invocations=0, incomplete=0,
                                           before=Counter(), after=Counter(),
                                           witnesses={}))
        stage["invocations"] += 1
        stage["incomplete"] += not record["completed"]
        for side in ("before", "after"):
            value = record.get(side, {})
            shapes = value.get("correlates")
            counts = value.get("node_counts", {})
            if not isinstance(shapes, list):
                raise ValueError(f"line {line}: missing {side} correlate list")
            if counts.get("LogicalCorrelate", 0) != len(shapes):
                raise ValueError(f"line {line}: correlate count disagrees with nodes")
            for shape in shapes:
                if (not isinstance(shape, dict) or set(shape) != SHAPE_FIELDS
                        or any(not isinstance(v, str) or not v for v in shape.values())):
                    raise ValueError(f"line {line}: unknown correlate shape")
                identity = canonical(shape)
                stage[side][identity] += 1
                stage["witnesses"].setdefault((side, identity), line)
    if not stages or any(not any(key[0] == name for key in stages) for name in STAGES):
        raise ValueError("trace does not cover all four correlation stages")
    result = []
    for (name, level), stage in sorted(stages.items(), key=lambda item:
                                     (STAGES.index(item[0][0]), item[0][1])):
        row = dict(step=name, optimization_level=level,
                   invocations=stage["invocations"], incomplete=stage["incomplete"])
        for side in ("before", "after"):
            shapes = stage[side]
            clusters = Counter()
            for identity, count in shapes.items():
                shape = json.loads(identity)
                clusters[canonical({key: shape[key] for key in
                                    ("join_type", "left", "right")})] += count
            row[side] = dict(
                occurrences=sum(shapes.values()),
                shapes=[dict(shape=json.loads(identity), occurrences=count,
                             first_journal_line=stage["witnesses"][side, identity])
                        for identity, count in sorted(shapes.items(),
                                                      key=lambda item: (-item[1], item[0]))],
                coarse_clusters=[dict(shape=json.loads(identity), occurrences=count)
                                 for identity, count in sorted(clusters.items(),
                                                               key=lambda item: (-item[1], item[0]))])
        result.append(row)
    return dict(schema_version=1, journal_records=total, stages=result,
                C7_coverage=None, case_weighted_coverage=None,
                limitations=[
                    "The historical trace has no case identity; repeated compilations and stages are not deduplicated into corpus cases.",
                    "Shapes retain join kind, required columns, immediate input kinds and exact row types. They do not encode the complete right subtree or its expressions.",
                    "RexSubQuery plans nested in expressions are outside the historical RelNode input traversal.",
                    "After counts record retained Correlates, including intentional UNNEST forms. A falling count is not C7 equivalence.",
                    "No Rust decorrelator or runtime execution is measured by this inventory.",
                ])


def case_inventory(records):
    """Count each stage separately, retaining case/invocation/node witnesses."""
    identities = set()
    stages = {name: dict(retained_invocations=0, incomplete=0, cases=set(),
                        before=Counter(), after=Counter(), witnesses={},
                        subqueries_before=0, subqueries_after=0) for name in STAGES}
    observed = 0
    related = 0
    for record in records:
        identity = record.get("id")
        if (record.get("schema_version") != 1 or not isinstance(identity, str)
                or not identity or identity in identities):
            raise ValueError("invalid or duplicate case identity")
        identities.add(identity)
        count = record.get("stages_observed")
        retained = record.get("stages")
        if (type(count) is not int or count <= 0 or not isinstance(retained, list)
                or len(retained) > count):
            raise ValueError("invalid stage count")
        observed += count
        related += bool(retained)
        previous = -1
        for observation in retained:
            sequence = observation.get("sequence")
            name = observation.get("step")
            if (type(sequence) is not int or not previous < sequence < count
                    or name not in stages or type(observation.get("completed")) is not bool):
                raise ValueError("invalid or reordered stage observation")
            previous = sequence
            stage = stages[name]
            stage["retained_invocations"] += 1
            stage["incomplete"] += not observation["completed"]
            stage["cases"].add(identity)
            for side in ("before", "after"):
                graph = observation.get(side, {})
                nodes = graph.get("relations")
                if (not isinstance(nodes, list) or not nodes
                        or type(graph.get("root")) is not int or graph["root"] != 0):
                    raise ValueError("missing relation graph")
                correlates = subqueries = 0
                for index, node in enumerate(nodes):
                    if type(node.get("id")) is not int or node["id"] != index:
                        raise ValueError("relation identity differs from arena position")
                    inputs = node.get("inputs")
                    nested = node.get("subqueries")
                    if not isinstance(inputs, list) or not isinstance(nested, list):
                        raise ValueError("missing relational edges")
                    edges = inputs + [query.get("relation") for query in nested]
                    if any(type(edge) is not int or not 0 <= edge < len(nodes) for edge in edges):
                        raise ValueError("relation edge outside arena")
                    subqueries += len(nested)
                    if node.get("kind") == "LogicalCorrelate":
                        correlate = node.get("correlate", {})
                        if len(inputs) != 2 or not isinstance(correlate.get("join_type"), str):
                            raise ValueError("invalid correlate descriptor")
                        correlates += 1
                        shape = dict(join_type=correlate["join_type"],
                                     left=nodes[inputs[0]]["kind"], right=nodes[inputs[1]]["kind"])
                        key = canonical(shape)
                        stage[side][key] += 1
                        stage["witnesses"].setdefault((side, key),
                                                      dict(case=identity, sequence=sequence, relation=index))
                if (type(graph.get("correlates")) is not int
                        or type(graph.get("subqueries")) is not int
                        or graph["correlates"] != correlates or graph["subqueries"] != subqueries):
                    raise ValueError("graph counts disagree with represented nodes")
                stage["subqueries_" + side] += subqueries
    if not identities:
        raise ValueError("empty case inventory")
    result = []
    for name, stage in stages.items():
        row = {key: stage[key] for key in
               ("retained_invocations", "incomplete", "subqueries_before", "subqueries_after")}
        row.update(step=name, cases=len(stage["cases"]))
        for side in ("before", "after"):
            row[side] = dict(occurrences=sum(stage[side].values()), clusters=[
                dict(shape=json.loads(key), occurrences=count,
                     witness=stage["witnesses"][side, key])
                for key, count in sorted(stage[side].items(), key=lambda item: (-item[1], item[0]))])
        result.append(row)
    return dict(schema_version=1, cases=len(identities), observed_stages=observed,
                cases_with_retained_stages=related, stages=result, C7_coverage=None,
                limitations=[
                    "The denominator at each stage counts unique relation objects in each invocation. It includes failed optimizer attempts and later compiler retries.",
                    "Coarse clusters use join kind and immediate input kinds. Exact types, expressions, correlation fields and nested subqueries remain in the raw graphs.",
                    "No-correlation successful stages are counted but omitted from the detailed graph journal.",
                    "Neither transport nor removal of Correlates establishes C7 execution equivalence.",
                ])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--case-corpus", action="store_true",
                        help="read case-identified shards instead of the historical rule trace")
    args = parser.parse_args()
    digest = hashlib.sha256()

    def records():
        with args.journal.open("rb") as stream:
            for line in stream:
                digest.update(line)
                yield json.loads(line)

    if args.case_corpus:
        files = sorted(args.journal.glob("shard-*/output/cases.jsonl"))
        if not files:
            raise ValueError("missing case shards")
        manifests = []
        inputs = {}
        for path in files:
            manifest = json.loads((path.parent / "complete.json").read_text())
            if not all(manifest.get(key) is True for key in
                       ("complete", "exported_schemas_equal", "exported_diagnostics_equal")):
                raise ValueError("case shard lacks successful oracle checks")
            manifests.append(manifest)
            inputs[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()

        def case_records():
            for path in files:
                with path.open() as stream:
                    yield from (json.loads(line) for line in stream)

        result = case_inventory(case_records())
        if result["cases"] != sum(manifest["cases"] for manifest in manifests):
            raise ValueError("case count disagrees with completion manifests")
        if result["observed_stages"] != sum(manifest["stages_observed"] for manifest in manifests):
            raise ValueError("stage count disagrees with completion manifests")
        result["inputs"] = inputs
    else:
        result = inventory(records())
        result["input"] = dict(path=str(args.journal), sha256=digest.hexdigest(),
                               bytes=args.journal.stat().st_size)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps(dict(records=result.get("journal_records", result.get("cases")),
                          stages=len(result["stages"]), C7_coverage=None)))


if __name__ == "__main__":
    main()
