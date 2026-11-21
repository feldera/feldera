#!/usr/bin/env python3
"""Compare AST-derived SQL through S1 in matching independent JVM lifetimes.

Each ordered shard gets one original and one emitted JVM, with identical
capture and strict-decode operations. Both raw streams also pass the strict
Rust decoder and must survive its transport exactly. Only then does the pinned
schema projection separate explicitly named source/coordinate contracts.
"""

import argparse
import concurrent.futures
import itertools
import json
from pathlib import Path
import subprocess
import sys

from parser_semantic_run import load_shards


def verify_transport(source, transported, identities):
    observed = []
    with source.open() as original, transported.open() as decoded:
        for left, right in itertools.zip_longest(original, decoded):
            if left is None or right is None:
                raise ValueError('Rust transport stream lengths differ')
            left, right = json.loads(left), json.loads(right)
            if left != right:
                raise ValueError('Rust transport changed a program')
            observed.append(left['id'])
    if observed != identities:
        raise ValueError('Transported case identities differ')


def run(inputs, output, classpath, decoder):
    manifest, shards = load_shards(inputs)
    output.mkdir()
    results = []
    for path, identities in shards:
        target = output / path.stem
        target.mkdir()

        def execute(command, name):
            (target / (name + '-command.json')).write_text(json.dumps(command) + '\n')
            with (target / (name + '.log')).open('x') as log:
                result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=1800)
            if result.returncode:
                raise ValueError(f'{path.stem}/{name}: process failed with {result.returncode}')

        def capture(side):
            execute(['java', '-ea', '-Xmx1g', '-XX:ActiveProcessorCount=1', '-cp', classpath,
                     'org.dbsp.sqlCompiler.compiler.sql.tools.ParserS1Probe',
                     side, str(path), str(target / side)], side)

        # Never interleave sides within one JVM: Calcite type caches outlive compilers.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(capture, ['original', 'emitted']))
        for side in ('original', 'emitted'):
            source = target / side / 'cases.jsonl'
            transported = target / (side + '-rust')
            execute([str(decoder), str(source), str(transported)], side + '-rust')
            marker = json.loads((transported / 'complete.json').read_text())
            if marker.get('complete') is not True or marker['cases'] != len(identities):
                raise ValueError('Rust decoder completion count differs')
            verify_transport(source, transported / 'cases.jsonl', identities)
        execute([sys.executable, str(Path(__file__).with_name('parser_s1_compare.py')),
                 str(target / 'original/cases.jsonl'), str(target / 'comparison'),
                 '--emitted', str(target / 'emitted/cases.jsonl')], 'comparison')
        result = json.loads((target / 'comparison/complete.json').read_text())
        if result.get('complete') is not True or result['cases'] != len(identities) or result['semantic_differences']:
            raise ValueError('S1 semantic comparison failed')
        results.append({'shard': path.stem, 'result': result})
        print(json.dumps(results[-1]), flush=True)
    result = {'schema_version': 1, 'complete': True, 'cases': manifest['accepted_cases'],
              'semantic_differences': 0, 'strict_java_decode': True, 'strict_rust_transport': True,
              'parser_refused_cases': manifest['parser_refused_cases'], 'shards': results}
    (output / 'complete.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inputs', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--classpath', required=True)
    parser.add_argument('--decoder', required=True, type=Path)
    args = parser.parse_args()
    run(args.inputs, args.output, args.classpath, args.decoder)
