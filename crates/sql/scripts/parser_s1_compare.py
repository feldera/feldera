#!/usr/bin/env python3
"""Compare S1 content by its pinned schema, retaining source coordinates separately.

Only the named Position, Coordinates, Source and SourceState contracts separate
coordinates or text. Source IDs, quote flags, diagnostic text, statement text,
options, identities, ordering and every other field still compare exactly.
Unknown fields or schema constructs fail closed.
"""

import argparse
import hashlib
import json
import itertools
from pathlib import Path


class Projection:
    def __init__(self, schema):
        self.schema = schema

    def project(self, value):
        return self.walk(value, self.schema, '')

    def walk(self, value, schema, path, named=None):
        if '$ref' in schema:
            prefix = '#/definitions/'
            if not schema['$ref'].startswith(prefix):
                raise ValueError('External schema reference')
            name = schema['$ref'][len(prefix):]
            return self.walk(value, self.schema['definitions'][name], path, name)
        for union in ('oneOf', 'anyOf'):
            if union in schema:
                matches = []
                for branch in schema[union]:
                    try:
                        matches.append(self.walk(value, branch, path, named))
                    except ValueError:
                        pass
                if not matches or (union == 'oneOf' and len(matches) != 1):
                    raise ValueError(f'{path}: no unique {union} schema branch')
                if any(match != matches[0] for match in matches):
                    raise ValueError(f'{path}: ambiguous semantic projection')
                return matches[0]
        allowed = {'$schema', 'title', 'definitions', 'description', 'type', 'enum', 'format',
                   'properties', 'required', 'additionalProperties', 'items', 'minimum', 'maximum',
                   'minItems', 'maxItems'}
        if schema.keys() - allowed:
            raise ValueError(f'{path}: unreviewed schema keys {schema.keys() - allowed}')
        actual = ('null' if value is None else 'boolean' if type(value) is bool else
                  'integer' if type(value) is int else 'number' if type(value) is float else
                  'string' if isinstance(value, str) else 'array' if isinstance(value, list) else
                  'object' if isinstance(value, dict) else 'unknown')
        expected = schema.get('type')
        if expected is not None and actual not in (expected if isinstance(expected, list) else [expected]):
            raise ValueError(f'{path}: expected {expected}, got {actual}')
        if 'enum' in schema and value not in schema['enum']:
            raise ValueError(f'{path}: unknown enum')
        if actual in ('integer', 'number'):
            if value < schema.get('minimum', value) or value > schema.get('maximum', value):
                raise ValueError(f'{path}: number out of range')
        observations = []
        if actual == 'object':
            if set(schema.get('required', [])) - value.keys():
                raise ValueError(f'{path}: missing required fields')
            properties = schema.get('properties', {})
            projected = {}
            for key, child in value.items():
                contract = properties.get(key, schema.get('additionalProperties', False))
                if not isinstance(contract, dict):
                    raise ValueError(f'{path}/{key}: unknown field or unconstrained contract')
                projected[key], fields = self.walk(child, contract, path + '/' + key.replace('~', '~0').replace('/', '~1'))
                observations.extend(fields)
        elif actual == 'array':
            if len(value) < schema.get('minItems', 0) or len(value) > schema.get('maxItems', len(value)):
                raise ValueError(f'{path}: invalid array length')
            if 'items' not in schema:
                raise ValueError(f'{path}: unconstrained array')
            projected = []
            for index, child in enumerate(value):
                item, fields = self.walk(child, schema['items'], path + '/' + str(index))
                projected.append(item)
                observations.extend(fields)
        else:
            projected = value
        fields = {'Position': ('line', 'column'),
                  'Coordinates': ('line', 'column', 'end_line', 'end_column'),
                  'Source': ('text',), 'SourceState': ('program', 'lines')}.get(named, ())
        if fields:
            observations.append({'path': path, 'contract': named,
                                 'fields': {key: projected.pop(key) for key in fields}})
        return projected, observations


def compare(record, projection):
    if record.get('strict_decode_verified') is not True:
        raise ValueError('Both S1 programs must pass their strict Java decoder')
    original, before = projection.project(record['original'])
    emitted, after = projection.project(record['emitted'])
    return {'id': record['id'], 'semantic_equal': original == emitted,
            'raw_equal': record['original'] == record['emitted'],
            'source_coordinates_equal': before == after,
            'original_sources_and_coordinates': before, 'emitted_sources_and_coordinates': after}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--emitted', type=Path, help='Separate emitted stream; input is the original stream')
    parser.add_argument('--schema', type=Path, default=Path(__file__).resolve().parents[1] / 'sql-rel-model/program-schema.json')
    args = parser.parse_args()
    projection = Projection(json.loads(args.schema.read_text()))
    args.output.mkdir()
    identities = set()
    differences = 0
    def records():
        if args.emitted is None:
            with args.input.open() as source:
                yield from (json.loads(line) for line in source)
            return
        counts = []
        for side, path in [('original', args.input), ('emitted', args.emitted)]:
            marker = json.loads((path.parent / 'complete.json').read_text())
            if marker.get('complete') is not True or marker.get('strict_decode_verified') is not True or marker.get('side') != side:
                raise ValueError('Missing strict side completion')
            counts.append(marker['cases'])
        count = 0
        with args.input.open() as original, args.emitted.open() as emitted:
            for left, right in itertools.zip_longest(original, emitted):
                if left is None or right is None:
                    raise ValueError('S1 stream lengths differ')
                left, right = json.loads(left), json.loads(right)
                if left['id'] != right['id']:
                    raise ValueError('S1 stream identities differ')
                count += 1
                yield {'id': left['id'], 'original': left, 'emitted': right, 'strict_decode_verified': True}
        if counts != [count, count]:
            raise ValueError('S1 stream counts differ from completion markers')

    with (args.output / 'cases.jsonl').open('x') as target:
        for record in records():
            result = compare(record, projection)
            if not result['id'] or result['id'] in identities:
                raise ValueError('Missing or repeated case identity')
            identities.add(result['id'])
            differences += not result['semantic_equal']
            target.write(json.dumps(result) + '\n')
    if not identities:
        raise ValueError('Empty S1 semantic input')
    result = {'schema_version': 1, 'complete': True, 'cases': len(identities),
              'semantic_differences': differences,
              'schema_sha256': hashlib.sha256(args.schema.read_bytes()).hexdigest(),
              'scope': __doc__}
    (args.output / 'complete.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
    raise SystemExit(1 if differences else 0)


if __name__ == '__main__':
    main()
