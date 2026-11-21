#!/usr/bin/env python3
"""Freeze semantic-oracle shards and explicitly inventory parser refusals."""

import argparse
import hashlib
import itertools
import json
from pathlib import Path

from parser_semantic_inputs import pair


def classify(original, parsed):
    if original['id'] != parsed['id'] or len(original['submissions']) != len(parsed['submissions']):
        raise ValueError('Parser identity or submission count differs')
    refusals = []
    for index, (source, observation) in enumerate(zip(original['submissions'], parsed['submissions'])):
        if source != observation['source']:
            raise ValueError('Parser source/API boundary differs')
        candidates = [m for m in observation['measurements']
                      if (m['version'], m['dialect']) == ('0.64.0', 'feldera_prototype')]
        if len(candidates) != 1:
            raise ValueError('Missing or repeated candidate observation')
        candidate = candidates[0]
        if candidate['verdict'] == 'rejected':
            refusals.append({'submission': index, 'observation': candidate})
        elif candidate['verdict'] != 'accepted':
            raise ValueError('Unknown candidate verdict')
    if refusals:
        return {'id': original['id'], 'parser_refusals': refusals}
    # Emission failure is a tooling gap, not a reason to drop an accepted case.
    return pair(original, parsed)


def prepare(source, rust, output, shard_size):
    if shard_size < 1:
        raise ValueError('Shard size must be positive')
    with source.open('rb') as stream:
        checksum = hashlib.file_digest(stream, 'sha256').hexdigest()
    manifest = json.loads((rust / 'complete.json').read_text())
    if manifest.get('complete') is not True or manifest.get('input_sha256') != checksum:
        raise ValueError('Parser evidence incomplete or input differs')
    output.mkdir()
    accepted, excluded, seen, shard = 0, 0, set(), []
    shards = []

    def write_shard():
        path = output / f'shard-{len(shards):03}.jsonl'
        path.write_text(''.join(json.dumps(record) + '\n' for record in shard))
        shards.append({'file': path.name, 'cases': len(shard),
                       'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
        shard.clear()

    with source.open() as originals, (rust / 'cases.jsonl').open() as parsed, \
            (output / 'parser-refusals.jsonl').open('x') as refusals:
        for left, right in itertools.zip_longest(originals, parsed):
            if left is None or right is None:
                raise ValueError('Parser evidence case count differs')
            record = classify(json.loads(left), json.loads(right))
            if not record['id'] or record['id'] in seen:
                raise ValueError('Missing or repeated case identity')
            seen.add(record['id'])
            if 'parser_refusals' in record:
                refusals.write(json.dumps(record) + '\n')
                excluded += 1
            else:
                shard.append(record)
                accepted += 1
                if len(shard) == shard_size:
                    write_shard()
    if shard:
        write_shard()
    if not accepted:
        raise ValueError('No accepted cases')
    result = {'schema_version': 1, 'complete': True, 'input_sha256': checksum,
              'cases': len(seen), 'accepted_cases': accepted, 'parser_refused_cases': excluded,
              'shards': shards}
    (output / 'complete.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('rust', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--shard-size', type=int, default=128)
    args = parser.parse_args()
    print(json.dumps(prepare(args.source, args.rust, args.output, args.shard_size)))
