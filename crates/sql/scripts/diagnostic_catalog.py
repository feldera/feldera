#!/usr/bin/env python3
"""Build a reviewed catalog for a declared, captured assertion population.

Roots are diagnostic-body formats. Constructor tails, forwarding wrappers, and
named formatters are graph dependencies, not additional roots. A root needs an
assertion witness; an incidental matching message is insufficient.
"""

import argparse
import collections
import hashlib
import json
import re
from pathlib import Path

from diagnostic_cross_fields import occurrences
from diagnostic_census import message_pattern
from diagnostic_formatter_review import render as render_formatter


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


def source_pattern(template):
    return re.compile("".join(re.escape(p["literal"]) if "literal" in p else ".*?"
                              for p in template["parts"]), re.DOTALL)


def source_signature(template):
    return digest(json.dumps({k: template[k] for k in ("family", "error_type", "parts", "sites")},
                             sort_keys=True, ensure_ascii=False))


def only_body_occurrences(raw, reviewed_fragment=None):
    """True only if every literal match lies inside an unambiguous body span."""
    if raw["is_regex"] and reviewed_fragment is None:
        return False
    rendered = raw["rendered"]
    counts = collections.Counter(raw["diagnostics"][i]["message"]
                                 for i in raw["visible_diagnostics"])
    spans = []
    for body, count in counts.items():
        starts = occurrences(rendered, body)
        if len(starts) == count:
            spans.extend((start, start + len(body)) for start in starts)
    fragment = raw["expected"] if reviewed_fragment is None else reviewed_fragment
    starts = occurrences(rendered, fragment)
    return bool(starts) and all(any(left <= start and start + len(fragment) <= right
                                   for left, right in spans) for start in starts)


class Catalog:
    def __init__(self, templates, resources, source_review, outer_review, formatter_review):
        self.templates = {t["id"]: t for t in templates["templates"]}
        self.messages = {(m["error_type"], m["message"]): m for m in templates["java"]}
        self.resources = resources
        self.source_review = source_review
        self.source_approved = set(source_review["approved_source_ids"])
        self.outer = {b["message"]: b for b in outer_review["reviewed_bodies"]}
        self.outer_templates = {t["id"]: t for t in outer_review["proposed_templates"]}
        self.formatters = {b["message"]: b for b in formatter_review["bodies"]}
        self.formatter_review = formatter_review
        self.ambiguities = {b["message"]: b for b in formatter_review["ambiguous_bodies"]}
        self.roots = {}
        self.dependencies = {}
        self.body_records = {}

    def dependency(self, identity, kind, description):
        entry = {"id": identity, "kind": kind, "description": description}
        prior = self.dependencies.setdefault(identity, entry)
        if prior != entry:
            raise ValueError("conflicting dependency definition: " + identity)
        return identity

    def root(self, identity, kind, definition, dependencies=()):
        entry = {"id": identity, "kind": kind, "definition": definition,
                 "dependencies": sorted(set(dependencies)), "body_ids": set(),
                 "assertion_records": set(), "mandatory_witnesses": set()}
        prior = self.roots.setdefault(identity, entry)
        if prior["definition"] != definition or prior["dependencies"] != entry["dependencies"]:
            raise ValueError("conflicting root definition: " + identity)
        return identity

    def source_root(self, identity):
        if identity not in self.source_approved:
            raise ValueError("source root lacks recorded review: " + identity)
        t = self.templates[identity]
        if source_signature(t) != self.source_review["source_signatures"].get(identity):
            raise ValueError("source expression or emission site changed after review")
        dependencies = []
        if t["family"] != "report":
            name = "constructor:" + t["family"]
            dependencies.append(self.dependency(name, "constructor", t["family"]))
        for site in t["sites"]:
            wrapper = site.get("wrapper_contract")
            if wrapper:
                dependencies.append(self.dependency("wrapper:" + wrapper, "forwarding_wrapper", wrapper))
            for part in site["parts"]:
                if "dynamic" in part:
                    expression = part["dynamic"]
                    key = "parameter:" + digest(site["source"] + "\n" + expression)
                    dependencies.append(self.dependency(key, "source_parameter_expression", {
                        "source": site["source"], "expression": expression}))
        return self.root("source:" + identity, "source_expression", {
            "parts": t["parts"], "family": t["family"], "error_type": t["error_type"],
            "sites": t["sites"], "review": "Pinned emission expression and captured body; competing candidates require explicit dispositions.",
        }, dependencies)

    def calcite_root(self, key):
        if key not in self.source_review["approved_calcite_keys"]:
            raise ValueError("Calcite resource lacks recorded review: " + key)
        if digest(self.resources[key]) != self.source_review["resource_digests"].get(key):
            raise ValueError("Calcite resource changed after review")
        return self.root("calcite:" + key, "calcite_resource", {"key": key, "format": self.resources[key]}, [
            self.dependency("formatter:MessageFormat", "formatter", "Pinned Java MessageFormat apostrophe and argument rules")])

    def reviewed_root(self, identity, definition, dependencies):
        references = []
        for name in dependencies:
            if name.startswith("calcite:"):
                references.append(self.calcite_root(name[len("calcite:"):]))
            elif name.startswith("constructor:"):
                references.append(self.dependency(name, "constructor", name[len("constructor:"):]))
            elif name.startswith("wrapper:"):
                references.append(self.dependency(name, "forwarding_wrapper", name[len("wrapper:"):]))
            else:
                references.append(self.dependency("formatter:" + name, "formatter", name))
        return self.root("reviewed:" + identity, "reviewed_formatter", definition, references)

    def roots_for(self, message, source_test):
        body = message["message"]
        if body in self.outer:
            row = self.outer[body]
            t = self.outer_templates[row["proposed_template_id"]]
            if t["format"].format_map(row["observed_arguments"]) != body:
                raise ValueError("outer formatter differs from reviewed body")
            identity = t["id"]
            dependencies = self.source_review["named_dependencies"][identity]
            return [self.reviewed_root(identity, t, dependencies)]
        if body in self.formatters:
            row = self.formatters[body]
            identity = row["identity"]
            if render_formatter(identity, row["arguments"], self.formatter_review["enums"]) != body:
                raise ValueError("formatter differs from reviewed body")
            definition = next(x for x in self.formatter_review["identities"] if x["id"] == identity)
            return [self.reviewed_root(identity, definition, self.source_review["named_dependencies"][identity])]
        # Cross-field review introduces more instances of already reviewed
        # unknown-field formatting, plus ordinary parser and unbounded-state roots.
        if message["error_type"] == "Invalid configuration" and body.startswith("unknown field "):
            field = json.loads(body[len("unknown field "):])
            if render_formatter("config.unknown_field", {"field": field}, {}) != body:
                raise ValueError("unknown field rendering differs")
            definition = next(x for x in self.formatter_review["identities"] if x["id"] == "config.unknown_field")
            return [self.reviewed_root("config.unknown_field", definition,
                                       self.source_review["named_dependencies"]["config.unknown_field"])]
        for special in self.source_review["additional_roots"]:
            if message["error_type"] == special["error_type"] and body in special["reviewed_bodies"]:
                examples = [example for example in special["definition"]["examples"] if example["message"] == body]
                if len(examples) != 1 or special["definition"]["format"].format_map(examples[0]["arguments"]) != body:
                    raise ValueError("additional formatter reconstruction differs")
                return [self.reviewed_root(special["id"], special["definition"], special["dependencies"])]
        candidate = self.messages.get((message["error_type"], body))
        if candidate is None:
            raise ValueError("body absent from reviewed template population")
        sources, calcite = candidate["source_candidates"], candidate["calcite_candidates"]
        if body in self.ambiguities:
            disposition = self.ambiguities[body]
            sources = [{"id": site["candidate_id"]} for site in disposition["selected_sites"]
                       if any(o["source_test"] == source_test for o in site["observations"])]
            sources = list({s["id"]: s for s in sources}.values())
        if len(sources) + len(calcite) != 1:
            raise ValueError("body does not have exactly one reviewed root: " + body[:100])
        if sources:
            t = self.templates[sources[0]["id"]]
            pattern = source_pattern(t)
            if not (pattern.fullmatch(body) or t["family"] in ("UnimplementedException", "UnsupportedException", "InternalCompilerError") and pattern.match(body)):
                raise ValueError("reviewed source no longer matches")
            return [self.source_root(sources[0]["id"])]
        key = calcite[0]
        if not message_pattern(self.resources[key])[0].fullmatch(body):
            raise ValueError("Calcite resource no longer matches")
        return [self.calcite_root(key)]

    def add(self, message, raw, record_index):
        roots = self.roots_for(message, raw["source_test"])
        body_id = digest(message["error_type"] + "\n" + message["message"])
        entry = self.body_records.setdefault(body_id, {"id": body_id, "error_type": message["error_type"],
                                                     "message": message["message"], "roots": set()})
        entry["roots"].update(roots)
        for identity in roots:
            self.roots[identity]["body_ids"].add(body_id)
            self.roots[identity]["assertion_records"].add(record_index)
        return set(roots)


def build(raw, attribution, cross_fields, templates, resources, review, outer, formatters):
    if len(raw) != len(attribution["java"]):
        raise ValueError("capture and attribution populations differ")
    catalog = Catalog(templates, resources, review, outer, formatters)
    crossed = {r["record_index"]: r for r in cross_fields["rows"]}
    if len(crossed) != len(cross_fields["rows"]) or any(not 0 <= i < len(raw) for i in crossed):
        raise ValueError("invalid cross-field record indices")
    assertions = []
    for index, (record, prior) in enumerate(zip(raw, attribution["java"])):
        if prior["record_index"] != index or any(record[k] != prior[k] for k in ("source_test", "call_site", "expected", "is_regex")):
            raise ValueError("assertion provenance differs")
        root_ids, mandatory = set(), set()
        messages = prior["message_candidates"]
        if index in crossed:
            cross = crossed[index]
            if any(cross[k] != record[k] for k in ("source_test", "call_site", "expected")):
                raise ValueError("cross-field provenance differs")
            if cross["disposition"] not in ("body_fields_resolved", "no_body_text_asserted"):
                raise ValueError("cross-field assertion is not resolved")
            messages = cross["body_matches"]
        for message in messages:
            diagnostic_index = message["diagnostic_index"]
            if type(diagnostic_index) is not int or not 0 <= diagnostic_index < len(record["diagnostics"]):
                raise ValueError("invalid attributed diagnostic index")
            actual = record["diagnostics"][diagnostic_index]
            if any(message[k] != actual[k] for k in ("error_type", "message")):
                raise ValueError("attributed body differs from captured diagnostic")
            root_ids.update(catalog.add(message, record, index))
        if index in crossed:
            mandatory.update(root_ids)
        elif len(root_ids) == 1:
            reviews = [r for r in review.get("reviewed_regex_body_fragments", [])
                       if r["source_test"] == record["source_test"] and r["expected"] == record["expected"]]
            if len(reviews) > 1:
                raise ValueError("duplicate regex review")
            fragment = reviews[0]["required_literal_fragment"] if reviews else None
            if only_body_occurrences(record, fragment):
                mandatory.update(root_ids)
        for identity in mandatory:
            catalog.roots[identity]["mandatory_witnesses"].add(index)
        assertions.append({"record_index": index, "source_test": record["source_test"],
                           "call_site": record["call_site"], "expected": record["expected"],
                           "body_roots": sorted(root_ids), "mandatory_roots": sorted(mandatory),
                           "reason": "reviewed composite body spans" if index in crossed and root_ids else
                           "literal or reviewed mandatory regex fragment matches only body text" if mandatory else
                           "body/non-body alternatives; not an independent root witness" if root_ids else
                           "no diagnostic-body text asserted"})
    roots = []
    for root in sorted(catalog.roots.values(), key=lambda r: r["id"]):
        roots.append({k: sorted(v) if isinstance(v, set) else v for k, v in root.items()})
    bodies = [{k: sorted(v) if isinstance(v, set) else v for k, v in row.items()}
              for row in sorted(catalog.body_records.values(), key=lambda r: r["id"])]
    return {"schema_version": 1, "scope": review["scope"], "counting_rule": review["counting_rule"],
            "counts": {"captured_assertions": len(raw), "distinct_selected_bodies": len(bodies),
                       "covered_body_roots": len(roots),
                       "roots_with_mandatory_assertion_witness": sum(bool(r["mandatory_witnesses"]) for r in roots),
                       "roots_by_kind": dict(collections.Counter(r["kind"] for r in roots)),
                       "shared_dependency_nodes": len(catalog.dependencies),
                       "assertions_without_body_roots": sum(not a["body_roots"] for a in assertions),
                       "body_assertions_with_non_body_alternatives": sum(bool(a["body_roots"]) and not a["mandatory_roots"] for a in assertions)},
            "roots": roots, "dependencies": sorted(catalog.dependencies.values(), key=lambda r: r["id"]),
            "bodies": bodies, "assertions": assertions, "uncounted_populations": review["uncounted_populations"],
            "whole_suite_required_template_count": None, "human_maintainer_accepted": False, "P0_complete": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("raw", "attribution", "cross_fields", "templates", "resources", "review", "outer", "formatters", "output"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    paths = [getattr(args, name) for name in ("raw", "attribution", "cross_fields", "templates", "resources", "review", "outer", "formatters")]
    values = [[json.loads(line) for line in path.read_text().splitlines()] if index == 0 else json.loads(path.read_text())
              for index, path in enumerate(paths)]
    result = build(*values)
    result["inputs"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps(result["counts"], indent=2))


if __name__ == "__main__":
    main()
