#!/usr/bin/env python3
"""Extract distinct column defaults, lateness expressions, or named row types from frozen classic exports."""
import argparse
import hashlib
import json
from pathlib import Path


def objects(node):
    if isinstance(node, list):
        for item in node:
            yield from objects(item)
    elif isinstance(node, dict):
        yield node
        for value in node.values():
            yield from objects(value)


def standalone(root, definitions):
    ids = {}

    def copy(node):
        if isinstance(node, list):
            return [copy(item) for item in node]
        if not isinstance(node, dict):
            return node
        if node.keys() == {"node"}:
            node = definitions[node["node"]]
        if "class" in node and "id" in node:
            original = node["id"]
            if original in ids:
                return {"node": ids[original]}
            ids[original] = len(ids)
            return {k: ids[original] if k == "id" else copy(v) for k, v in node.items()}
        return {k: copy(v) for k, v in node.items()}

    return copy(root)


def extract(corpus, population, directory, structs=False):
    cases = expressions = 0
    counts = {}
    directory.mkdir(parents=True, exist_ok=False)
    with population.open() as records, (directory / "inputs.jsonl").open("x") as output:
        for line in records:
            record = json.loads(line)
            method, index = record["id"].rsplit("/", 1)
            path = corpus / hashlib.sha256(method.encode()).hexdigest() / f"case-{int(index):04}.json"
            case = json.loads(path.read_bytes())
            if (case["source_test"] != method or case["case_index"] != int(index)
                    or case["submissions"] != record["submissions"] or case["status"] != "exported"):
                raise ValueError(f"Frozen SQL and corpus disagree at {path}")
            cases += 1
            graph = case["circuit"]
            if structs:
                roots = [node for node in objects(graph) if node.get("class") == "DBSPTypeStruct"]
            else:
                roots = [column[key] for owner in objects(graph)
                         for column in owner.get("columnMetadata", [])
                         for key in ("defaultValue", "lateness") if column.get(key) is not None]
            if not roots:
                continue
            definitions = {node["id"]: node for node in objects(graph) if "id" in node and "class" in node
                           and not node["class"].endswith(("Operator", "Circuit", "Declaration"))}
            for root in roots:
                value = standalone(root, definitions)
                digest = hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
                if digest not in counts:
                    print(json.dumps({"id": digest, "case": record["id"], "kind": "struct" if structs else "default", "before": value}), file=output)
                counts[digest] = counts.get(digest, 0) + 1
                expressions += 1
    report = {"cases": cases, "occurrences": expressions, "distinct": len(counts),
              "population_sha256": hashlib.sha256(population.read_bytes()).hexdigest()}
    (directory / "inventory.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ("corpus", "population", "output"):
        parser.add_argument(arg, type=Path)
    parser.add_argument("--structs", action="store_true", help="check the named-row to tuple conversion")
    args = parser.parse_args()
    extract(args.corpus, args.population, args.output, args.structs)
