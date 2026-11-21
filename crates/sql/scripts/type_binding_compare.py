#!/usr/bin/env python3
"""Audit read-only binding capture against the historical Java probe oracle.

This compares verdicts, schemas, diagnostics, options, and exact input manifests.
It inventories observed operators and casts without claiming Rust binding or C9
parity. The old golden's incomplete source provenance remains a separate fact.
"""
import argparse
from collections import Counter
import hashlib
from itertools import zip_longest
import json
from pathlib import Path


def no_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def records(path):
    with path.open() as stream:
        for line in stream:
            yield json.loads(line, object_pairs_hook=no_duplicates)


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def identity(operator):
    if operator.get('identity_status') == 'syntax_container':
        if operator.get('kind') != 'SELECT':
            raise ValueError('unexpected syntax-only operator')
        return None
    if operator.get('identity_status') != 'registered':
        raise ValueError('unregistered operator')
    return tuple(operator[key] for key in ('registry', 'registry_version', 'key'))


def check_references(node, fields, size):
    for field in fields:
        for reference in node.get(field, []):
            if reference is not None and (type(reference) is not int or not 0 <= reference < size):
                raise ValueError(f'invalid {field} reference')


def graph_inventory(bindings):
    operators = Counter()
    casts = Counter()
    stages = Counter()
    for graph in bindings:
        stage = graph['stage']
        if stage not in ('validated_sql', 'converted_rel'):
            raise ValueError('unknown observation stage')
        stages[stage] += 1
        table = graph['nodes'] if stage == 'validated_sql' else graph['expressions']
        for index, node in enumerate(table):
            if node['id'] != index:
                raise ValueError('nonsequential expression identity')
            check_references(node, ('operands', 'elements'), len(table))
            if 'reference' in node:
                check_references({'operand': [node['reference']]}, ('operand',), len(table))
            if 'operator' in node:
                key = identity(node['operator'])
                if key is not None:
                    operators[(stage, *key)] += 1
                if node['operator']['kind'] == 'CAST':
                    casts[stage] += 1
            if stage == 'converted_rel' and not isinstance(node.get('type'), dict):
                raise ValueError('converted expression lacks a type')
        if stage == 'converted_rel':
            relations = graph['relations']
            if type(graph['root']) is not int or not 0 <= graph['root'] < len(relations):
                raise ValueError('invalid relation root')
            for index, relation in enumerate(relations):
                if relation['id'] != index:
                    raise ValueError('nonsequential relation identity')
                check_references(relation, ('inputs',), len(relations))
                check_references(relation, ('expressions',), len(table))
                for call in relation.get('aggregate_calls', []):
                    key = identity(call['operator'])
                    operators[('converted_aggregate', *key)] += 1
                    check_references(call, ('rex_arguments',), len(table))
        elif type(graph['root']) is not int or not 0 <= graph['root'] < len(table):
            raise ValueError('invalid syntax root')
    if stages['validated_sql'] != stages['converted_rel']:
        raise ValueError('successful capture has unmatched observation stages')
    return operators, casts, stages


def compare(golden, captured, expected):
    count = accepted = 0
    operators, casts, stages, populations = Counter(), Counter(), Counter(), Counter()
    differences = []
    option_extensions = Counter()
    substantive_differences = 0
    for before, after in zip_longest(golden, captured):
        if before is None or after is None:
            raise ValueError('input populations have different lengths')
        if before['probe']['id'] != count or after['probe']['id'] != count:
            raise ValueError('input identities are duplicated, missing, or reordered')
        if before.get('compiler_bug') or after.get('compiler_bug'):
            raise ValueError('compiler bug cannot serve as a rejection oracle')
        if after.get('schema_version') != 1 or after.get('runtime_verified') is not False:
            raise ValueError('invalid capture version or runtime claim')
        if after.get('capture_complete') is not True or after.get('capture_errors') != []:
            raise ValueError('capture is incomplete')
        changed = [field for field in ('accepted', 'compiler_bug', 'probe', 'schema', 'diagnostics', 'options')
                   if canonical(before[field]) != canonical(after[field])]
        if changed:
            differences.append({'id': count, 'operation': before['probe']['operation'], 'fields': changed})
        if any(field != 'options' for field in changed):
            substantive_differences += 1
        previous = before['options']
        copy_options = json.loads(canonical(after['options']))
        additions = {}
        for group, fields in copy_options.items():
            if isinstance(fields, dict) and isinstance(previous.get(group), dict):
                additions[group] = {key: fields[key] for key in fields.keys() - previous[group].keys()}
                for key in additions[group]:
                    del copy_options[group][key]
        additions = {group: values for group, values in additions.items() if values}
        if canonical(previous) != canonical(copy_options):
            option_extensions['changed_or_removed_existing_option'] += 1
        elif additions:
            option_extensions[canonical(additions)] += 1
        counts = graph_inventory(after['bindings'])
        operators.update(counts[0]); casts.update(counts[1]); stages.update(counts[2])
        populations[before['probe']['operation']] += 1
        accepted += after['accepted']
        count += 1
    if count != expected:
        raise ValueError(f'expected {expected} records, got {count}')
    return {
        'schema_version': 1, 'complete': True, 'records': count, 'accepted': accepted,
        'rejected': count - accepted, 'differences': differences,
        'compared_fields': ['accepted', 'compiler_bug', 'probe', 'schema', 'diagnostics', 'options'],
        'all_compared_fields_equal': not differences,
        'all_verdict_schema_diagnostics_inputs_equal': substantive_differences == 0,
        'option_schema_extensions': dict(option_extensions), 'operation_populations': dict(sorted(populations.items())),
        'observation_counts': dict(stages), 'cast_counts': dict(casts),
        'operators': [{'stage': key[0], 'registry': key[1], 'version': key[2], 'key': key[3], 'count': value}
                      for key, value in sorted(operators.items())],
        'operator_count_includes_system_views_and_syntax': True,
        'rust_cast_or_operator_parity_verified': False, 'runtime_verified': False,
        'full_C9_verified': False,
        'old_golden_source_provenance': 'Historical identity only; not retroactively certified by new source capture',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('golden', type=Path)
    parser.add_argument('capture', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--expected', type=int, default=84224)
    args = parser.parse_args()
    report = compare(records(args.golden), records(args.capture), args.expected)
    report['inputs'] = {str(path): {'sha256': digest(path), 'bytes': path.stat().st_size}
                        for path in (args.golden, args.capture)}
    with args.output.open('x') as stream:
        json.dump(report, stream, indent=2)
        stream.write('\n')
    print(json.dumps({key: report[key] for key in ('records', 'accepted', 'rejected', 'all_compared_fields_equal')}))
    raise SystemExit(0 if report['all_compared_fields_equal'] else 1)


if __name__ == '__main__':
    main()
