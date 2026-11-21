#!/usr/bin/env python3
"""Pin declared pass fields to source files; do not infer state lifetimes."""

import argparse
import hashlib
import json
import re
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


MANUAL_CALL = re.compile(
    r"this\.(?:compiler\.)?(applyObservedPass|observeCircuit)\(this\.getName\(\),\s*(\d+),\s*(?:0|\"([^\"]+)\")"
)

# Discover calls independently of the shape accepted by MANUAL_CALL. Otherwise
# a new composite using another receiver or container name would disappear from
# the coverage check instead of requiring a contract.
OBSERVATION_CALL = re.compile(r"\b(?:applyObservedPass|observeCircuit)\s*\(")
OUTER = "org/dbsp/sqlCompiler/compiler/visitors/outer/"
INFRASTRUCTURE_CALLS = {
    OUTER + "Conditional.java": re.compile(
        r'this\.compiler\.applyObservedPass\("Conditional", 0, 0, this\.transform, circuit\)'
    ),
    OUTER + "Repeat.java": re.compile(
        r'this\.compiler\.applyObservedPass\("Repeat", 0, repeats, this\.transform, circuit\)'
    ),
    OUTER + "Passes.java": re.compile(
        r'this\.compiler\.applyObservedPass\(this\.name, passIndex, 0, pass, circuit\)'
    ),
    "org/dbsp/sqlCompiler/compiler/DBSPCompiler.java": re.compile(
        r"public (?:DBSPCircuit applyObservedPass|void observeCircuit)\(String container, int index, "
    ),
}


def manual_composite_sources(source_root, sources):
    """Reject an observation call that neither inventory mechanism accounts for.

    This deliberately conservative source check is not a Java parser. Even a
    new call-shaped comment requires inspection rather than silent exclusion.
    Infrastructure implementations are pinned alongside manual composites.
    """
    manual_sources = set()
    for path in source_root.rglob("*.java"):
        content = path.read_text()
        calls = list(OBSERVATION_CALL.finditer(content))
        if not calls:
            continue
        relative = str(path.relative_to(source_root))
        manual = list(MANUAL_CALL.finditer(content))
        infrastructure = INFRASTRUCTURE_CALLS.get(relative)
        known = manual + (list(infrastructure.finditer(content)) if infrastructure else [])
        if any(not any(match.start() <= call.start() and call.end() <= match.end()
                       for match in known) for call in calls):
            raise ValueError(f"unaccounted observation call: {relative}")
        sources[relative] = digest(path)
        if manual:
            manual_sources.add(relative)
    return manual_sources


def audit(inventory, source_root):
    if inventory.get("schema_version") != 2:
        raise ValueError("expected the field-bearing construction inventory")
    implementations = inventory["implementations"]
    fields = {}
    sources = {}
    for owner, contract in implementations.items():
        if contract.get("field_lifetimes_reviewed") is not False:
            raise ValueError("declaration inventory cannot certify field lifetimes")
        fields[owner] = contract["fields"]
    for owner, declarations in inventory["ambient_fields"].items():
        if owner in fields:
            raise ValueError("ambient class duplicates a pass contract")
        fields[owner] = declarations
    manual_sources = {}
    manual_points = inventory.get("manual_points", [])
    manual_children = 0
    for point in manual_points:
        owner = point["implementation"]
        if point["kind"] == "in_place":
            if owner is not None or point["fields"]:
                raise ValueError("in-place operation cannot claim a transform class")
        elif point["kind"] == "transform" and owner:
            if owner in fields and fields[owner] != point["fields"]:
                raise ValueError(f"inconsistent declarations for {owner}")
            fields[owner] = point["fields"]
        else:
            raise ValueError("invalid manual observation kind")
        for child in point.get("nested_transforms", []):
            owner = child["implementation"]
            if owner in fields and fields[owner] != child["fields"]:
                raise ValueError(f"inconsistent declarations for {owner}")
            fields[owner] = child["fields"]
            manual_children += 1
        if point["field_lifetimes_reviewed"] is not False or not point["local_and_shared_state"]:
            raise ValueError("manual points require explicit, unreviewed state contracts")
        points = manual_sources.setdefault(point["source"], {})
        if point["index"] in points:
            raise ValueError("duplicate manual observation index")
        points[point["index"]] = point
    for relative, points in manual_sources.items():
        source = (source_root / relative).resolve()
        source.relative_to(source_root.resolve())
        content = source.read_text()
        sources[relative] = digest(source)
        # Fail closed if these explicit call sites change shape or new indices appear.
        observed = MANUAL_CALL.findall(content)
        if len(observed) != len(points) or {int(index) for _, index, _ in observed} != set(points):
            raise ValueError(f"manual observations differ from source: {relative}")
        for method, index, name in observed:
            point = points[int(index)]
            if (method == "observeCircuit") != (point["kind"] == "in_place"):
                raise ValueError("manual observation kind differs from source")
            if name and name != point["pass"]:
                raise ValueError("in-place observation name differs from source")
    if inventory.get("manual_composites_included"):
        actual_sources = manual_composite_sources(source_root, sources)
        if not manual_sources or actual_sources != set(manual_sources):
            raise ValueError("manual composite source coverage is incomplete")
    declarations = set()
    static = set()
    for owner, members in fields.items():
        implementation_source = owner.split("$", 1)[0].replace(".", "/") + ".java"
        source = (source_root / implementation_source).resolve()
        source.relative_to(source_root.resolve())
        if not source.is_file():
            raise ValueError(f"missing implementation source: {source}")
        sources[implementation_source] = digest(source)
        seen = set()
        for member in members:
            identity = (member["declaring_class"], member["name"])
            if identity in seen:
                raise ValueError(f"duplicate field in {owner}: {identity}")
            seen.add(identity)
            if not member["type"] or type(member["static"]) is not bool:
                raise ValueError(f"malformed field: {identity}")
            declarations.add(identity)
            if member["static"]:
                static.add(identity)
            source = (source_root / member["source"]).resolve()
            source.relative_to(source_root.resolve())
            if not source.is_file():
                raise ValueError(f"missing declaration source: {source}")
            sources[member["source"]] = digest(source)
    paths = set()
    placements = 0
    referenced = set()
    for configuration in inventory["configurations"]:
        for point in configuration["points"]:
            implementation = point["implementation"]
            if implementation not in implementations:
                raise ValueError(f"pass has no field contract: {implementation}")
            referenced.add(implementation)
            paths.add(json.dumps(point["path"], sort_keys=True))
            placements += 1
    if not paths or referenced != set(implementations):
        raise ValueError("empty inventory or unreferenced pass contract")
    return {
        "schema_version": 1,
        "complete": True,
        "scope": "declared fields for constructed pass placements and explicit ambient classes",
        "configurations": len(inventory["configurations"]),
        "placements": placements,
        "unique_paths": len(paths),
        "implementations": len(implementations),
        "ambient_classes": len(inventory["ambient_fields"]),
        "manual_points": len(manual_points),
        "manual_composites": len(manual_sources),
        "manual_nested_placements": manual_children,
        "distinct_declarations": len(declarations),
        "static_declarations": len(static),
        "source_hashes": sources,
        "field_lifetimes_reviewed": False,
        "dynamic_referents_enumerated": False,
        "manual_composites_enumerated": bool(manual_points),
        "isolated_checkpoint_proven": False,
        "P0_complete": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    parser.add_argument("source_root", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    report = audit(json.loads(args.inventory.read_text()), args.source_root)
    report["inventory_sha256"] = digest(args.inventory)
    with args.output.open("x") as output:
        json.dump(report, output, indent=2)
        output.write("\n")


if __name__ == "__main__":
    main()
