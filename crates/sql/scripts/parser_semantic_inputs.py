#!/usr/bin/env python3
"""Pair original programs with AST-emitted SQL, retaining API boundaries/options."""

import argparse
import copy
import hashlib
import itertools
import json
from pathlib import Path


def pair(original, parsed):
    if original['id'] != parsed['id']:
        raise ValueError('Parser identity differs from original')
    emitted = copy.deepcopy(original)
    if len(original['submissions']) != len(parsed['submissions']):
        raise ValueError('Parser submission count differs from original')
    for index, (source, observation) in enumerate(zip(original['submissions'], parsed['submissions'])):
        if observation['source'] != source:
            raise ValueError('Parser source or API boundary differs from original')
        candidates = [m for m in observation['measurements']
                      if (m['version'], m['dialect']) == ('0.64.0', 'feldera_prototype')]
        if len(candidates) != 1:
            raise ValueError('Missing or repeated Feldera parser observation')
        candidate = candidates[0]
        emission = candidate.get('feldera_sql', {})
        if candidate['verdict'] != 'accepted' or emission.get('status') != 'emitted':
            raise ValueError(f'No AST-derived SQL for {original["id"]}/{index}: {emission}')
        statements = emission['statements']
        if (not isinstance(statements, list) or any(not isinstance(s, str) or not s for s in statements)
                or (not source['many'] and len(statements) != 1)):
            raise ValueError('Malformed AST-emitted statements')
        # The emitter supplies every byte of SQL. Original text is retained only
        # in the independent baseline; it never fills gaps in emitted syntax.
        emitted['submissions'][index]['sql'] = ';\n'.join(statements)
        if statements and source['many']:
            emitted['submissions'][index]['sql'] += ';'
    return {'id': original['id'], 'original': original, 'emitted': emitted}


def prepare(source, rust, output):
    with source.open('rb') as stream:
        checksum = hashlib.file_digest(stream, 'sha256').hexdigest()
    manifest = json.loads((rust / 'complete.json').read_text())
    if manifest.get('complete') is not True or manifest.get('input_sha256') != checksum:
        raise ValueError('Parser evidence is incomplete or has different input')
    seen = set()
    with source.open() as inputs, (rust / 'cases.jsonl').open() as results, output.open('x') as target:
        for original, parsed in itertools.zip_longest(inputs, results):
            if original is None or parsed is None:
                raise ValueError('Parser case count differs from original')
            record = pair(json.loads(original), json.loads(parsed))
            if record['id'] in seen:
                raise ValueError('Repeated semantic case identity')
            seen.add(record['id'])
            target.write(json.dumps(record) + '\n')
    if not seen:
        raise ValueError('Empty semantic input')
    return len(seen)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('rust', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    print(json.dumps({'cases': prepare(args.source, args.rust, args.output)}))


if __name__ == '__main__':
    main()
