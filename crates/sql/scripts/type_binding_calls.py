#!/usr/bin/env python3
"""Select exact validated calls from the declared 40-function probe grammar.

This retains every node/type/operator/cast field and uses manifest origins to
mark original literal casts versus inserted wrappers. It is a logical subtree
selection, not a type or cast normalizer and not a Rust comparison.
"""
import argparse
from collections import Counter
import copy
import json
from pathlib import Path

from type_binding_compare import digest, identity, records

FUNCTIONS = frozenset('''ABS CEIL FLOOR SIGN SQRT CBRT EXP LN LOG10 SIN COS TAN
ASIN ACOS ATAN COT DEGREES RADIANS ROUND TRUNCATE LENGTH CHAR_LENGTH OCTET_LENGTH
UPPER LOWER TRIM INITCAP MD5 REVERSE ASCII POWER MOD LOG ATAN2 CONCAT LEFT RIGHT
REPEAT NULLIF COALESCE'''.split())


def selected_graph(record):
    matches = []
    for index, graph in enumerate(record['bindings']):
        if graph['stage'] != 'validated_sql':
            continue
        nodes = graph['nodes']
        root = nodes[graph['root']]
        fields = root.get('type', {}).get('fields', [])
        if (root.get('kind') == 'SELECT' and len(fields) == 1
                and fields[0].get('name') == 'result'):
            matches.append((index, graph))
    if len(matches) != 1:
        raise ValueError('expected exactly one validated SELECT of result')
    return matches[0]


def extract_call(record):
    if not record['accepted'] or record['probe']['operation'] not in FUNCTIONS:
        raise ValueError('record is outside accepted scalar function scope')
    observation, graph = selected_graph(record)
    nodes = graph['nodes']
    root = nodes[graph['root']]

    def node(index):
        if type(index) is not int or not 0 <= index < len(nodes) or nodes[index]['id'] != index:
            raise ValueError('invalid validated syntax reference')
        return nodes[index]

    select_list = node(root['operands'][1])
    if (select_list['class'] != 'org.apache.calcite.sql.SqlNodeList'
            or len(select_list.get('elements', [])) != 1):
        raise ValueError('expected a single selected expression')
    alias = node(select_list['elements'][0])
    if (alias.get('kind') != 'AS' or len(alias.get('operands', [])) != 2
            or identity(alias['operator']) != ('calcite-s1-fixture', '1', 'SqlStdOperatorTable.AS')
            or node(alias['operands'][1]).get('names') != ['result']):
        raise ValueError('expected exact AS result wrapper')
    call = node(alias['operands'][0])
    if 'operator' not in call or identity(call['operator']) is None or not call.get('type_known'):
        raise ValueError('selected call lacks registered operator or known result type')

    def subtree(index, active=frozenset()):
        if index in active:
            raise ValueError('cycle in validated syntax')
        current = node(index)
        result = copy.deepcopy({key: value for key, value in current.items()
                                if key not in ('id', 'operands', 'elements')})
        result['capture_node'] = index
        if 'operator' in result:
            identity(result['operator'])
        for field in ('operands', 'elements'):
            if field in current:
                result[field] = [None if child is None else subtree(child, active | {index})
                                 for child in current[field]]
        return result

    if call['operator'].get('name', '').upper() != record['probe']['operation']:
        raise ValueError('selected operator does not match original operation')

    tree = subtree(call['id'])
    arguments = record['probe']['arguments']
    operands = tree['operands']
    trim = record['probe']['operation'] == 'TRIM'
    if len(operands) != (3 if trim else len(arguments)) or any(x is None for x in operands):
        raise ValueError('unexpected function argument layout')
    if trim:
        if len(arguments) != 1 or operands[0].get('literal_type') != 'SYMBOL' or operands[1].get('literal_type') != 'CHAR':
            raise ValueError('unexpected TRIM defaults')
        operands[0]['source_role'] = 'trim_default_flag'
        operands[1]['source_role'] = 'trim_default_character'
    mappings = []
    for argument_index, argument in enumerate(arguments):
        operand_index = 2 if trim else argument_index
        current = operands[operand_index]
        wrappers = []
        while current.get('kind') == 'CAST':
            if (identity(current['operator']) != ('calcite-s1-fixture', '1', 'SqlStdOperatorTable.CAST')
                    or len(current.get('operands', [])) != 2
                    or current['operands'][1].get('class') != 'org.apache.calcite.sql.SqlDataTypeSpec'):
                raise ValueError('unexpected cast shape')
            wrappers.append(current)
            current = current['operands'][0]
        if argument['origin'] == 'column':
            if current.get('names') != ['t', chr(ord('a') + argument_index)]:
                raise ValueError('column does not match original manifest argument')
            original = current
        elif argument['origin'] == 'literal':
            if not wrappers or 'literal_type' not in current:
                raise ValueError('original literal CAST is absent or ambiguous')
            original = wrappers.pop()
            original['source_role'] = 'original_literal_cast'
            # The explicit target is written in the source; inferred targets
            # are generated at SqlParserPos.ZERO. Both remain in the tree.
            position = original['operands'][1]['position']
            if position['line'] <= 0 or position['column'] <= 0:
                raise ValueError('original literal cast has no source target position')
        else:
            raise ValueError('unknown original argument origin')
        original['original_argument'] = argument_index
        for wrapper in wrappers:
            wrapper['source_role'] = 'inferred_cast'
            if wrapper['operands'][1]['position']['line'] != 0:
                raise ValueError('inferred cast target unexpectedly comes from source')
        mappings.append({'argument': argument_index, 'operand': operand_index,
                         'origin': argument['origin'], 'original_node': original['capture_node'],
                         'inferred_cast_nodes': [wrapper['capture_node'] for wrapper in wrappers]})
    return {'schema_version': 1, 'id': record['probe']['id'], 'probe': record['probe'],
            'observation': observation, 'validated_call': tree, 'argument_sources': mappings,
            'boundary': 'validated_sql', 'rust_binding_verified': False, 'full_C9_verified': False}


def extract(input_path, output_path):
    selected = rejected = outside = 0
    registry = Counter()
    examples = {}
    with output_path.open('x') as output:
        for record in records(input_path):
            operation = record['probe']['operation']
            if operation not in FUNCTIONS:
                outside += 1
                continue
            if not record['accepted']:
                rejected += 1
                continue
            value = extract_call(record)
            operator = value['validated_call']['operator']
            registry[(operation, operator['registry'], operator['registry_version'], operator['key'])] += 1
            examples.setdefault(operation, value['id'])
            output.write(json.dumps(value, separators=(',', ':')) + '\n')
            selected += 1
    return {'schema_version': 1, 'complete': True, 'selected_accepted': selected,
            'selected_rejected': rejected, 'outside_function_slice': outside,
            'functions': len(examples), 'example_ids': dict(sorted(examples.items())),
            'operators': [{'operation': key[0], 'registry': key[1], 'version': key[2], 'key': key[3], 'records': value}
                          for key, value in sorted(registry.items())],
            'input_sha256': digest(input_path), 'output_sha256': digest(output_path),
            'projection': 'Exact selected validated call tree; casts/type metadata preserved; origins mapped structurally',
            'full_C9_verified': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('input', 'output', 'report'):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    result = extract(args.input, args.output)
    with args.report.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps({k: result[k] for k in ('selected_accepted', 'selected_rejected', 'functions')}))


if __name__ == '__main__':
    main()
