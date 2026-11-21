#!/usr/bin/env python3
"""Run frozen semantic shards with bounded JVMs and verify every case identity."""

import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess


def load_shards(inputs):
    """Verify frozen input identity, counts and bytes before running either oracle."""
    manifest = json.loads((inputs / 'complete.json').read_text())
    if manifest.get('complete') is not True:
        raise ValueError('Input preparation is incomplete')
    all_ids = set()
    shards = []
    for item in manifest['shards']:
        path = inputs / item['file']
        if path.parent != inputs or hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']:
            raise ValueError('Invalid shard path or digest')
        with path.open() as source:
            ids = [json.loads(line)['id'] for line in source]
        if len(ids) != item['cases'] or any(not identity or identity in all_ids for identity in ids):
            raise ValueError('Invalid shard count or identity')
        if len(set(ids)) != len(ids):
            raise ValueError('Repeated shard identity')
        all_ids.update(ids)
        shards.append((path, ids))
    if len(all_ids) != manifest['accepted_cases']:
        raise ValueError('Prepared case count differs')
    return manifest, shards


def run(inputs, output, classpath, workers, ast_only=False):
    if workers not in (1, 2):
        raise ValueError('Semantic replay supports one or two bounded workers')
    manifest, shards = load_shards(inputs)
    output.mkdir()

    def execute(shard):
        path, ids = shard
        target = output / path.stem
        command = ['java', '-ea', '-Xmx1g', '-XX:ActiveProcessorCount=1', '-cp', classpath,
                   'org.dbsp.sqlCompiler.compiler.sql.tools.ParserSemanticProbe',
                   str(path), str(target), '--continue-after-failure']
        if ast_only:
            command.append('--ast-only')
        (output / (path.stem + '-command.json')).write_text(json.dumps(command) + '\n')
        with (output / (path.stem + '.log')).open('x') as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=1800)
        with (target / 'cases.jsonl').open() as source:
            records = [json.loads(line) for line in source]
        if [record['id'] for record in records] != ids:
            raise ValueError(f'{path.stem}: result identities differ')
        complete = json.loads((target / 'complete.json').read_text())
        failures = sum(not record['equal'] for record in records)
        if (complete.get('complete') is not True or complete['cases'] != len(ids)
                or complete.get('binding_executed') is not (not ast_only)
                or complete['mismatches'] != failures or (result.returncode == 0) != (failures == 0)):
            raise ValueError(f'{path.stem}: inconsistent completion status')
        record = {'shard': path.stem, 'exit_code': result.returncode, 'result': complete}
        (output / (path.stem + '-status.json')).write_text(json.dumps(record, indent=2) + '\n')
        print(json.dumps(record), flush=True)
        return record

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(execute, shards))
    mismatches = sum(record['result']['mismatches'] for record in results)
    result = {'schema_version': 1, 'complete': True, 'passed': mismatches == 0,
              'cases': manifest['accepted_cases'], 'mismatches': mismatches,
              'binding_executed': not ast_only,
              'parser_refused_cases': manifest['parser_refused_cases'], 'shards': results}
    (output / 'complete.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inputs', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--classpath', required=True)
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--ast-only', action='store_true', help='Compare parsed ASTs without binding grammar fixtures')
    args = parser.parse_args()
    result = run(args.inputs, args.output, args.classpath, args.workers, args.ast_only)
    raise SystemExit(0 if result['passed'] else 1)
