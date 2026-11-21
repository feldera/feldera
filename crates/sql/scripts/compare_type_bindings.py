#!/usr/bin/env python3
"""Compare Rust predictions with observed validated-SQL function bindings.

This boundary precedes SQL-to-Rel and S2. It checks function identity, full type
descriptors and added casts around original arguments. Literal parsing, source
positions, S2 lowering and runtime values are explicitly separate obligations.
"""
import argparse
from collections import Counter
from itertools import zip_longest
import json
from pathlib import Path

from type_binding_calls import FUNCTIONS, extract_call
from type_binding_compare import canonical, digest, identity, records


def project(extracted):
    call = extracted['validated_call']
    registry, version, key = identity(call['operator'])
    arguments = []
    used_operands = set()
    for source in extracted['argument_sources']:
        operand = source['operand']
        if operand in used_operands:
            raise ValueError('argument operands overlap')
        used_operands.add(operand)
        root = call['operands'][operand]
        current = root
        casts = []
        for node_id in source['inferred_cast_nodes']:
            if (current.get('kind') != 'CAST' or current['capture_node'] != node_id
                    or identity(current['operator']) != ('calcite-s1-fixture', '1', 'SqlStdOperatorTable.CAST')
                    or len(current['operands']) != 2):
                raise ValueError('inferred casts do not form the recorded argument chain')
            target = current['operands'][1]
            if canonical(target['type']) != canonical(current['type']):
                raise ValueError('cast target and result type differ')
            casts.append(current['type'])
            current = current['operands'][0]
        if current['capture_node'] != source['original_node'] or current['original_argument'] != source['argument']:
            raise ValueError('cast chain does not terminate at its original argument')
        arguments.append({'argument': source['argument'], 'operand': operand,
                          'original_type': current['type'], 'coerced_type': root['type'],
                          'inferred_casts': casts})
    defaults = []
    for operand, node in enumerate(call['operands']):
        if operand not in used_operands:
            if extracted['probe']['operation'] != 'TRIM' or operand not in (0, 1):
                raise ValueError('unclassified default argument')
            defaults.append({'operand': operand, 'sql': node['sql'], 'type': node['type']})
    return {'status': 'bound', 'operator': {'registry': registry, 'registry_version': version, 'key': key},
            'result_type': call['type'], 'arguments': arguments, 'defaults': defaults}


def run(java_path, rust_path, output, expected=84_224):
    if not 0 < expected <= 100_000:
        raise ValueError('expected count outside experiment limits')
    output.mkdir()
    counts = {name: Counter() for name in ('translated_binding', 'rewritten_binding')}
    functions = Counter()
    outside = count = 0
    with (output / 'differences.jsonl').open('x') as differences:
        for java, rust in zip_longest(records(java_path), records(rust_path)):
            if java is None or rust is None:
                raise ValueError('binding populations differ')
            probe = java['probe']
            if (type(probe['id']) is not int or type(rust['id']) is not int
                    or probe['id'] != count or rust['id'] != count):
                raise ValueError('missing, duplicated or reordered binding identity')
            count += 1
            if any(type(record.get('schema_version')) is not int or record['schema_version'] != 1
                   for record in (java, rust, probe)):
                raise ValueError('unknown binding schema version')
            if java.get('runtime_verified') is not False or rust.get('runtime_verified') is not False:
                raise ValueError('unsupported runtime verification claim')
            if (type(java['accepted']) is not bool or java['compiler_bug'] is not False
                    or java['capture_complete'] is not True or java['capture_errors'] != []):
                raise ValueError('incomplete Java binding oracle')
            if probe['operation'] not in FUNCTIONS:
                outside += 1
                continue
            functions[probe['operation']] += 1
            observed = project(extract_call(java)) if java['accepted'] else {'status': 'rejected'}
            for strategy in counts:
                predicted = rust[strategy]
                category = 'equal' if canonical(predicted) == canonical(observed) else 'different'
                counts[strategy][category] += 1
                if category == 'different':
                    differences.write(json.dumps({'id': probe['id'], 'operation': probe['operation'],
                        'arguments': probe['arguments'], 'strategy': strategy,
                        'observed': observed, 'predicted': predicted}, sort_keys=True) + '\n')
        if count != expected:
            raise ValueError(f'expected {expected} records, got {count}')
    result = {'schema_version': 1, 'complete': True, 'records': count, 'outside_function_slice': outside,
              'counts': counts, 'functions': dict(sorted(functions.items())),
              'java_sha256': digest(java_path), 'rust_sha256': digest(rust_path),
              'boundary': 'validated_sql',
              'compared': ['SQL acceptance', 'selected operator registry identity', 'full result type',
                           'original and coerced operand types', 'inferred cast chains and operand positions',
                           'TRIM default values and types'],
              'not_compared': ['literal parsing', 'source positions', 'S2 cast/function lowering', 'runtime values'],
              'shared_binding_encoder': True, 'runtime_verified': False, 'full_C9_verified': False}
    (output / 'complete.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('java', 'rust', 'output'):
        parser.add_argument(name, type=Path)
    parser.add_argument('--expected', type=int, default=84_224)
    parser.add_argument('--require-equal', action='store_true')
    args = parser.parse_args()
    result = run(args.java, args.rust, args.output, args.expected)
    print(json.dumps({'records': result['records'], 'counts': result['counts']}))
    if args.require_equal and any(c['different'] for c in result['counts'].values()):
        raise SystemExit('validated bindings differ; see retained report')
