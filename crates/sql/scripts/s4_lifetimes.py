#!/usr/bin/env python3
"""Audit reviewed S4 contracts and summarize bounded identity observations.

This validates a source review, not an inference from field names or types. The
observer records reference identities only within one compilation. Its sample
limits cannot certify an isolated checkpoint or complete reachable heap.
"""
import argparse
import collections
import gzip
import hashlib
import json
from pathlib import Path


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def field_id(field):
    return field['declaring_class'] + '#' + field['name']


def declarations(inventory):
    result = {}

    def add(fields):
        for field in fields:
            key = field_id(field)
            if key in result and result[key] != field:
                raise ValueError('Conflicting declaration: ' + key)
            result[key] = field

    for implementation in inventory['implementations'].values():
        add(implementation['fields'])
    for fields in inventory['ambient_fields'].values():
        add(fields)
    for point in inventory['manual_points']:
        add(point['fields'])
        for child in point.get('nested_transforms', []):
            add(child['fields'])
    return result


def validate_contracts(ledger, inventory, source_root):
    """Reject missing/extra fields, changed sources or broken per-placement joins."""
    expected = declarations(inventory)
    rows = {row['id']: row for row in ledger['fields']}
    if len(rows) != len(ledger['fields']) or set(rows) != set(expected):
        raise ValueError('Contract population differs from the declaration inventory')
    categories = collections.Counter()
    for key, declaration in expected.items():
        row = rows[key]
        if any(row.get(k) != v for k, v in declaration.items()):
            raise ValueError('Declaration mismatch: ' + key)
        if not row.get('category') or not row.get('lifetime_contract'):
            raise ValueError('Missing reviewed contract: ' + key)
        if row.get('resume') != 'complete_prefix_only' or row.get('transitive_heap_complete') is not False:
            raise ValueError('Unsupported checkpoint claim: ' + key)
        source = Path(source_root) / row['source']
        if digest(source) != row['source_sha256']:
            raise ValueError('Changed source: ' + row['source'])
        lines = source.read_text().splitlines()
        for reference in row['lexical_references']:
            if lines[reference['line'] - 1].strip() != reference['text']:
                raise ValueError('Changed source reference: ' + key)
        categories[row['category']] += 1
    if dict(categories) != ledger['categories']:
        raise ValueError('Category totals differ')
    if set(ledger['implementations']) != set(inventory['implementations']):
        raise ValueError('Implementation population differs')
    for name, implementation in inventory['implementations'].items():
        if ledger['implementations'][name]['field_contract_ids'] != [field_id(f) for f in implementation['fields']]:
            raise ValueError('Broken implementation/field join: ' + name)
    if len(ledger['configurations']) != len(inventory['configurations']):
        raise ValueError('Configuration population differs')
    placements = 0
    for i, configuration in enumerate(inventory['configurations']):
        reviewed = ledger['configurations'][i]
        points = configuration['points']
        actual_digest = hashlib.sha256(json.dumps(points, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        if reviewed['configuration_index'] != i or reviewed['options'] != configuration['options'] or reviewed['points_sha256'] != actual_digest or reviewed['placements'] != len(points):
            raise ValueError('Changed constructed configuration')
        for point in points:
            if point['implementation'] not in ledger['implementations']:
                raise ValueError('Missing placement implementation')
        placements += len(points)
    if len(ledger['manual_points']) != len(inventory['manual_points']):
        raise ValueError('Manual population differs')
    nested = 0
    for row, original in zip(ledger['manual_points'], inventory['manual_points']):
        for key in ('container', 'index', 'pass', 'source', 'kind', 'local_and_shared_state'):
            if row[key] != original[key]:
                raise ValueError('Changed manual point: ' + key)
        if row['field_contract_ids'] != [field_id(f) for f in original['fields']]:
            raise ValueError('Broken manual field join')
        children = original.get('nested_transforms', [])
        if len(row['nested_transforms']) != len(children):
            raise ValueError('Missing nested manual transform')
        for child, original_child in zip(row['nested_transforms'], children):
            if child['field_contract_ids'] != [field_id(f) for f in original_child['fields']]:
                raise ValueError('Broken nested manual field join')
        nested += len(children)
    return {'declarations': len(rows), 'static_declarations': sum(f['static'] for f in expected.values()),
            'implementations': len(ledger['implementations']), 'configurations': len(ledger['configurations']),
            'placements': placements, 'manual_points': len(ledger['manual_points']), 'nested_manual_transforms': nested,
            'categories': dict(sorted(categories.items()))}


def event_summary(records, case):
    """Validate paired events; summarize identity/size changes without equality guesses."""
    stack = []
    seen = set()
    phases = collections.Counter()
    fields = {}
    references = collections.defaultdict(lambda: {'owners': set(), 'fields': set(), 'events': set()})
    mutations = []
    pass_classes = set()
    bounds = {'inaccessible_fields': 0, 'truncated_collections': 0, 'largest_collection': 0}

    def visit(entries, owner, event, depth):
        for entry in entries:
            key = field_id(entry)
            info = fields.setdefault(key, {'observations': 0, 'statuses': collections.Counter(), 'value_classes': set(), 'depths': set()})
            info['observations'] += 1
            info['statuses'][entry['status']] += 1
            info['depths'].add(depth)
            value = entry.get('value', {})
            if 'class' in value:
                info['value_classes'].add(value['class'])
            if entry['status'] == 'inaccessible':
                bounds['inaccessible_fields'] += 1
            if 'size' in entry:
                samples = entry.get('sample_entries', entry.get('sample_elements', []))
                if entry['size'] < len(samples) or len(samples) > 32 or entry['sample_complete'] != (len(samples) == entry['size']):
                    raise ValueError('Inconsistent bounded collection sample')
                bounds['largest_collection'] = max(bounds['largest_collection'], entry['size'])
                bounds['truncated_collections'] += entry['sample_complete'] is False
            if 'object' in value:
                ref = references[(value['object'], value['class'])]
                ref['owners'].add(owner)
                ref['fields'].add(key)
                ref['events'].add(event)
            if 'direct_referent_fields' in entry:
                if depth != 0 or 'object' not in value:
                    raise ValueError('Unexpected referent depth or missing identity')
                visit(entry['direct_referent_fields'], value['object'], event, depth + 1)

    for record in records:
        phase = record['phase']
        if phase not in ('before', 'after', 'compiler_before', 'compiler_after', 'in_place'):
            raise ValueError('Unknown observation phase')
        phases[phase] += 1
        event = record.get('event', phase)
        owner = record.get('pass_object', record.get('compiler', {}).get('object'))
        if 'pass_class' in record:
            pass_classes.add(record['pass_class'])
        if phase == 'before':
            if event in seen or record.get('parent_event') != (stack[-1]['event'] if stack else None):
                raise ValueError('Duplicate event or broken parent nesting')
            seen.add(event)
            stack.append(record)
        elif phase == 'after':
            if not stack:
                raise ValueError('Unmatched after event')
            before = stack.pop()
            for key in ('event', 'pass_object', 'pass_class', 'container', 'index', 'iteration'):
                if before[key] != record[key]:
                    raise ValueError('Mismatched paired event: ' + key)
            if record.get('parent_event') != (stack[-1]['event'] if stack else None):
                raise ValueError('Broken after parent nesting')
            if record.get('completed') is not True:
                raise ValueError('Activation pass did not complete')
            initial = {field_id(f): f for f in before['fields']}
            final = {field_id(f): f for f in record['fields']}
            if set(initial) != set(final):
                raise ValueError('Field declarations changed during a pass')
            for key, value in initial.items():
                # Only top-level reference/scalar and container-size changes are
                # counted. No claim about mutations hidden below sample depth.
                old = {k: value[k] for k in ('value', 'size') if k in value}
                new = {k: final[key][k] for k in ('value', 'size') if k in final[key]}
                if old != new:
                    mutations.append({'event': event, 'pass_object': owner, 'field': key, 'before': old, 'after': new})
        elif phase == 'in_place' and record.get('parent_event') != (stack[-1]['event'] if stack else None):
            raise ValueError('Broken in-place parent nesting')
        visit(record.get('fields', []), owner, event, 0)
    if stack or phases['compiler_before'] != 1 or phases['compiler_after'] != 1:
        raise ValueError('Incomplete compilation observations')
    aliases = []
    for (object_id, class_name), ref in references.items():
        if len(ref['owners']) > 1:
            aliases.append({'object': object_id, 'class': class_name, 'owner_objects': sorted(ref['owners']),
                            'fields': sorted(ref['fields']), 'event_count': len(ref['events'])})
    return {'case': case, 'phases': dict(phases), 'pass_classes': sorted(pass_classes),
            'fields': {key: {'observations': value['observations'], 'statuses': dict(value['statuses']),
                             'value_classes': sorted(value['value_classes']), 'depths': sorted(value['depths'])}
                       for key, value in sorted(fields.items())},
            'bounds': bounds, 'shared_references': aliases, 'top_level_changes': mutations}


def summarize_run(directory, ledger):
    directory = Path(directory)
    complete = json.loads((directory / 'complete.json').read_text())
    if complete.get('complete') is not True or complete.get('cases') != 32 or complete.get('heap_checkpoint') is not False:
        raise ValueError('Incomplete or unsupported activation run')
    comparisons = [json.loads(line) for line in (directory / 'comparisons.jsonl').read_text().splitlines()]
    if len(comparisons) != 32 or len({row['id'] for row in comparisons}) != 32:
        raise ValueError('Wrong activation identities')
    cases = []
    for index, comparison in enumerate(comparisons):
        if comparison.get('equal') is not True or comparison['baseline'] != comparison['observed']:
            raise ValueError('Observer changed compiler outputs')
        path = directory / ('state-' + str(index) + '.jsonl.gz')
        with gzip.open(path, 'rt') as stream:
            result = event_summary((json.loads(line) for line in stream), comparison['id'])
        result['raw_sha256'] = digest(path)
        cases.append(result)
    observed = set()
    phases = collections.Counter()
    classes = set()
    for case in cases:
        observed.update(case['fields'])
        phases.update(case['phases'])
        classes.update(case['pass_classes'])
    declarations_set = {row['id'] for row in ledger['fields']}
    return {'schema_version': 1, 'cases': cases, 'summary': {'cases': len(cases), 'phases': dict(phases),
            'observed_pass_classes': len(classes), 'observed_declaring_fields_including_direct_referents': len(observed),
            'historical_declarations_observed': len(observed & declarations_set),
            'historical_declarations_not_observed': sorted(declarations_set - observed),
            'additional_observed_declarations': sorted(observed - declarations_set),
            'inaccessible_field_observations': sum(c['bounds']['inaccessible_fields'] for c in cases),
            'truncated_collection_observations': sum(c['bounds']['truncated_collections'] for c in cases),
            'top_level_reference_scalar_or_size_changes': sum(len(c['top_level_changes']) for c in cases),
            'prefix_observations_per_side': sum(c['baseline']['observations'] for c in comparisons),
            'all_prefix_outputs_equal': True, 'heap_checkpoint': False, 'runtime_verified': False}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('ledger', type=Path)
    parser.add_argument('inventory', type=Path)
    parser.add_argument('sources', type=Path)
    parser.add_argument('activation', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    ledger = json.loads(args.ledger.read_text())
    inventory = json.loads(args.inventory.read_text())
    if digest(args.inventory) != ledger['source_inventory_sha256']:
        raise ValueError('Inventory bytes changed')
    source_audit = validate_contracts(ledger, inventory, args.sources)
    report = summarize_run(args.activation, ledger)
    report['source_audit'] = source_audit
    report['inputs'] = {'ledger_sha256': digest(args.ledger), 'inventory_sha256': digest(args.inventory)}
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'source_audit': source_audit, 'summary': {k: v for k, v in report['summary'].items() if not isinstance(v, list)}}, indent=2))


if __name__ == '__main__':
    main()
