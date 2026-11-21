#!/usr/bin/env python3
"""Freeze native spike inputs and inventory every explicit exported row/key type.

This reads compiler artifacts, not SQL text guesses. It does not run native C7,
infer implicit DBSP state layouts, or certify an operator's native implementation.
"""

import argparse
import collections
import hashlib
import json
import shutil
import subprocess
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def walk(value, path=""):
    if isinstance(value, dict):
        yield path, value
        for key, child in value.items():
            yield from walk(child, f"{path}/{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk(child, f"{path}/{index}")


class Graph:
    """One inner-node namespace; each exported circuit/Z-set has its own."""

    def __init__(self, root):
        self.root = root
        self.nodes = {}
        for _, node in walk(root):
            name = node.get("class", "")
            if "id" in node and name and not name.endswith("Operator") and name != "DBSPCircuit":
                identity = node["id"]
                if identity in self.nodes and self.nodes[identity] != node:
                    raise ValueError(f"conflicting inner definition {identity}")
                self.nodes[identity] = node

    def resolve(self, value):
        if isinstance(value, dict) and set(value) == {"node"}:
            try:
                return self.nodes[value["node"]]
            except KeyError as error:
                raise ValueError(f"missing inner node {value['node']}") from error
        return value

    def shape(self, value, active=frozenset()):
        value = self.resolve(value)
        if isinstance(value, dict):
            identity = value.get("id")
            if identity is not None and identity in active:
                raise ValueError("cyclic type shape")
            active = active | {identity} if identity is not None else active
            return {key: self.shape(child, active) for key, child in value.items()
                    if key not in {"id", "position"}}
        if isinstance(value, list):
            return [self.shape(child, active) for child in value]
        return value


class Types:
    def __init__(self):
        self.entries = {}

    def add(self, graph, node, case, path, role):
        shape = graph.shape(node)
        if not isinstance(shape, dict) or not shape.get("class", "").startswith("DBSPType"):
            raise ValueError(f"expected type at {case}:{path}")
        identity = hashlib.sha256(canonical(shape).encode()).hexdigest()
        row = self.entries.setdefault(identity, {"id": identity, "shape": shape, "uses": []})
        use = {"case": case, "path": path, "role": role}
        if use not in row["uses"]:
            row["uses"].append(use)
        return identity

    def collect(self, graph, case, prefix):
        for path, node in walk(graph.root):
            name = node.get("class", "")
            if name.startswith("DBSPType"):
                self.add(graph, node, case, prefix + path, "explicit_type")
                if name == "DBSPTypeIndexedZSet":
                    self.add(graph, node["keyType"], case, prefix + path + "/keyType", "index_key")
                    self.add(graph, node["elementType"], case, prefix + path + "/elementType", "index_value")
                elif name == "DBSPTypeZSet":
                    self.add(graph, node["elementType"], case, prefix + path + "/elementType", "zset_row")


def case_inventory(case, types):
    identity = f"{case['source_test']}/{case['case_index']}"
    if case.get("schema_version") != 1 or case.get("status") != "exported" or case.get("engine_mode") != "classic":
        raise ValueError("expected an exported classic corpus case")
    incremental = case["options"]["languageOptions"]["incrementalize"]
    if case["step_semantics"] != ("delta" if incremental else "whole_relation"):
        raise ValueError("step semantics disagree with compilation mode")
    graph = Graph(case["circuit"])
    types.collect(graph, identity, "/circuit")
    operators, functions = [], set()
    for path, node in walk(case["circuit"]):
        name = node.get("class", "")
        if name.endswith("Operator"):
            if "outputType" not in node:
                raise ValueError(f"operator requires explicit port inventory: {name}")
            operators.append({"class": name, "path": path, "id": node["id"],
                              "inputs": node["inputs"],
                              "output_type": types.add(graph, node["outputType"], identity,
                                                       "/circuit" + path + "/outputType", "operator_output")})
        if name == "DBSPSimplePathSegment":
            functions.add(node["identifier"])
    steps = []
    for index, command in enumerate(case["commands"]):
        if command["kind"] == "compact":
            steps.append({"index": index, "kind": "compact"})
            continue
        if command["kind"] != "step":
            raise ValueError("unknown corpus command")
        step = {"index": index, "kind": "step"}
        for direction in ["inputs", "outputs"]:
            sets = []
            for port, relation in enumerate(command[direction]):
                zset = relation["zset_ir"]
                if zset["class"] != "DBSPZSetExpression" or len(zset["data"]) != len(zset["weights"]):
                    raise ValueError("expected a complete weighted Z-set")
                if any(type(weight) is not int for weight in zset["weights"]):
                    raise ValueError("multiplicity must be an integer")
                prefix = f"/commands/{index}/{direction}/{port}/zset_ir"
                value_graph = Graph(zset)
                types.collect(value_graph, identity, prefix)
                sets.append({"port": port, "name": relation["name"],
                             "primary_keys": relation["primary_keys"],
                             "row_type": types.add(value_graph, zset["elementType"], identity,
                                                   prefix + "/elementType", direction + "_row"),
                             "rows": len(zset["data"]), "weights": zset["weights"],
                             "oracle_sha256": hashlib.sha256(canonical(zset).encode()).hexdigest()})
            step[direction] = sets
        steps.append(step)
    if not any(step["kind"] == "step" and step["outputs"] for step in steps):
        raise ValueError("native candidate must have an exported output oracle")
    return {"id": identity, "step_semantics": case["step_semantics"], "steps": steps,
            "operators": operators, "function_path_segments": sorted(functions),
            "schema": case["schema"], "input_order": case["input_order"], "output_order": case["output_order"],
            "circuit_inputs": case["circuit_inputs"], "circuit_outputs": case["circuit_outputs"],
            "expected_runtime_error": case["expected_runtime_error"],
            "build": {key: case[key] for key in ["producer_commit", "calcite_commit", "runtime_commit"]},
            "runtime_verified": False}


def string_value(graph, expression, expected_type):
    """Decode the selected VARCHAR fixture only; never approximate SQL casts."""
    expression = graph.resolve(expression)
    actual_type = graph.shape(expression["type"])
    if actual_type != expected_type:
        raise ValueError("connector cell type differs from row schema")
    if actual_type["class"] != "DBSPTypeString" or actual_type["fixed"] or actual_type["precision"] != -1:
        raise ValueError("connector fixture supports unbounded VARCHAR only")
    if expression["class"] == "DBSPCastExpression":
        source = graph.resolve(expression["source"])
        source_type = graph.shape(source["type"])
        # Only widening nullability is value-preserving without SQL evaluation.
        if {**source_type, "mayBeNull": True} != {**actual_type, "mayBeNull": True} or not actual_type["mayBeNull"]:
            raise ValueError("connector cast is not nullability widening")
        return string_value(graph, source, source_type)
    if expression["class"] != "DBSPStringLiteral" or expression.get("charset") != "UTF-8":
        raise ValueError("unsupported connector string expression")
    value = expression["value"]
    if value is not None and not isinstance(value, str):
        raise ValueError("invalid string literal")
    if value is None and not actual_type["mayBeNull"]:
        raise ValueError("NULL in a non-nullable cell")
    return value


def connector_fixture(case, specification):
    if specification["format"] != {"name": "json", "config": {"update_format": "insert_delete", "array": False}}:
        raise ValueError("connector fixture requires JSON insert_delete with array=false")
    if specification["case_id"] != f"{case['source_test']}/{case['case_index']}":
        raise ValueError("connector case identity differs from selection")
    index, port = specification["step"], specification["input"]
    if type(index) is not int or type(port) is not int or min(index, port) < 0:
        raise ValueError("connector indices must be nonnegative integers")
    if case["step_semantics"] == "whole_relation" and index != 0:
        raise ValueError("later whole-relation steps require state differencing")
    command = case["commands"][index]
    if command["kind"] != "step":
        raise ValueError("connector selection is not a step")
    relation = command["inputs"][port]
    name = relation["name"]
    schemas = [schema for schema in case["schema"]["inputs"]
               if schema["name"] == name["text"] and schema["case_sensitive"] == name["quoted"]]
    if len(schemas) != 1:
        raise ValueError("connector relation has no unambiguous schema")
    schema = schemas[0]
    zset = relation["zset_ir"]
    graph = Graph(zset)
    row_type = graph.shape(zset["elementType"])
    if row_type["class"] != "DBSPTypeTuple" or len(row_type["tupFields"]) != len(schema["fields"]):
        raise ValueError("connector row shape differs from schema")
    if row_type["mayBeNull"] or len({field["name"] for field in schema["fields"]}) != len(schema["fields"]):
        raise ValueError("connector requires a non-null row with distinct field names")
    for field, expected in zip(schema["fields"], row_type["tupFields"]):
        if (expected["class"] != "DBSPTypeString" or expected["fixed"] or expected["precision"] != -1
                or field["columntype"] != {"type": "VARCHAR", "precision": -1, "nullable": expected["mayBeNull"]}):
            raise ValueError("connector SQL schema differs from unbounded VARCHAR row type")
    if len(zset["data"]) != len(zset["weights"]):
        raise ValueError("connector weights are incomplete")
    records = []
    for expression, weight in zip(zset["data"], zset["weights"]):
        row = graph.resolve(expression)
        if row["class"] != "DBSPTupleExpression" or len(row["fields"]) != len(schema["fields"]):
            raise ValueError("connector fixture requires explicit tuple rows")
        if graph.shape(row["type"]) != row_type:
            raise ValueError("connector tuple type differs from row schema")
        if type(weight) is not int or abs(weight) > 10000 or len(records) + abs(weight) > 10000:
            raise ValueError("connector multiplicity exceeds fixture bounds")
        value = {field["name"]: string_value(graph, cell, expected)
                 for field, cell, expected in zip(schema["fields"], row["fields"], row_type["tupFields"])}
        records.extend([{("insert" if weight > 0 else "delete"): value}] * abs(weight))
    payload = "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in records)
    return {"format": specification["format"], "input_schema": schema, "records": len(records),
            "case_id": specification["case_id"], "step": index, "input": port,
            "step_semantics": case["step_semantics"], "payload": payload,
            "initial_empty_state_required": case["step_semantics"] == "whole_relation",
            "connector_executed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("selection", type=Path)
    parser.add_argument("corpus", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--comparator", required=True, type=Path)
    args = parser.parse_args()
    selection = json.loads(args.selection.read_text())
    programs = selection["programs"]
    if selection.get("schema_version") != 1 or len(programs) != 10 or len({p["id"] for p in programs}) != 10:
        raise ValueError("selection must freeze ten distinct corpus cases")
    args.output.mkdir()
    shutil.copyfile(args.selection, args.output / "selection.json")
    frozen = args.output / "corpus"
    frozen.mkdir()
    directories = {}
    for manifest in args.corpus.glob("*/complete.json"):
        source = json.loads(manifest.read_text())["source_test"]
        if source in directories:
            raise ValueError("duplicate corpus test identity")
        directories[source] = manifest.parent
    paths = {}
    for program in programs:
        source, index = program["id"].rsplit("/", 1)
        directory = directories[source]
        target = frozen / directory.name
        if not target.exists():
            shutil.copytree(directory, target)
        path = target / f"case-{int(index):04d}.json"
        if digest(path) != program["sha256"]:
            raise ValueError(f"frozen case changed: {program['id']}")
        paths[program["id"]] = path
    command = [str(args.comparator.resolve()), "--validate-corpus", str(frozen.resolve())]
    result = subprocess.run(command, capture_output=True, text=True, timeout=120)
    (args.output / "validation.stdout").write_text(result.stdout)
    (args.output / "validation.stderr").write_text(result.stderr)
    result.check_returncode()
    types, reports, raw = Types(), [], {}
    for program in programs:
        case = json.loads(paths[program["id"]].read_text())
        report = case_inventory(case, types)
        if report["id"] != program["id"]:
            raise ValueError("case identity differs from selection")
        reports.append(dict(report, features=program["features"], source_sha256=program["sha256"],
                            source_path=str(paths[program["id"]].relative_to(args.output))))
        raw[program["id"]] = case
    connector = connector_fixture(raw[selection["connector"]["case_id"]], selection["connector"])
    payload = connector.pop("payload")
    (args.output / "connector-input.jsonl").write_text(payload)
    connector["payload_sha256"] = digest(args.output / "connector-input.jsonl")
    report = {"schema_version": 1, "complete": True, "programs": reports,
              "types": sorted(types.entries.values(), key=lambda row: row["id"]),
              "operator_counts": dict(collections.Counter(op["class"] for row in reports for op in row["operators"])),
              "type_classes": sorted({row["shape"]["class"] for row in types.entries.values()}),
              "connector": connector, "selection_sha256": digest(args.selection),
              "inventory_tool_sha256": digest(Path(__file__)),
              "comparator_sha256": digest(args.comparator), "validation_command": command,
              "native_C7_executed": False, "implicit_operator_state_layouts_enumerated": False,
              "scope": "all explicit types in selected exported circuits and step values; not an S2 replay or native execution",
              "P0_complete": False}
    (args.output / "inventory.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({"programs": len(reports), "types": len(types.entries),
                      "operators": report["operator_counts"], "connector_records": connector["records"]}))


if __name__ == "__main__":
    main()
