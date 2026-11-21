#!/usr/bin/env python3
"""Inventory every public upstream AST struct field and enum variant payload.

This is a source inventory, not a claim that every upstream field is reachable
through the Feldera dialect. Guard coverage means explicit field enumeration;
allowed nested values still require their own grammar/representation policy.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re

AST = Path('crates/sql/parser-probe/vendor/sqlparser/src/ast')
ADAPTER = Path('crates/sql/sql-parser/src')
GUARDS = {
    'Query': ('ast_fields.rs', 'query'),
    'Select': ('ast_fields.rs', 'select'),
    'Fetch': ('ast_fields.rs', 'query'),
    'Insert': ('ast_fields.rs', 'insert'),
    'Update': ('ast_fields.rs', 'update'),
    'Delete': ('ast_fields.rs', 'delete'),
    'CreateIndex': ('ast_fields.rs', 'create_index'),
    'Merge': ('ast_fields.rs', 'merge'),
    'CreateTable': ('ddl.rs', 'table'),
    'CreateView': ('ddl.rs', 'view'),
    'ViewColumnDef': ('ddl.rs', 'view'),
    'ColumnDef': ('ddl.rs', 'column'),
    'PrimaryKeyConstraint': ('ddl.rs', 'primary_key'),
    'ForeignKeyConstraint': ('ddl.rs', 'foreign_key'),
    'IndexColumn': ('ddl.rs', 'primary_key'),
    'OrderByExpr': ('ddl.rs', 'primary_key'),
    'TableSample': ('grammar.rs', 'table_factor'),
    'TableSampleQuantity': ('grammar.rs', 'table_factor'),
    'TableSampleSeed': ('grammar.rs', 'table_factor'),
}
EXHAUSTIVE_TYPE_POLICIES = {
    'DataType', 'ArrayElemTypeDef', 'CharacterLength', 'BinaryLength',
    'ExactNumberInfo', 'MapBracketKind', 'StructBracketKind', 'TimezoneInfo',
}
EXHAUSTIVE_EXPRESSION_POLICIES = {'Expr', 'ExtractSyntax', 'CeilFloorKind', 'InTarget'}


def mask_comments(source):
    """Keep coordinates while removing nested Rust comments and string bodies."""
    chars = list(source)
    i = 0
    while i < len(source):
        start = i
        if source.startswith('//', i):
            i = source.find('\n', i)
            if i < 0:
                i = len(source)
        elif source.startswith('/*', i):
            i += 2
            depth = 1
            while depth:
                if i >= len(source):
                    raise ValueError('unterminated block comment')
                if source.startswith('/*', i):
                    depth += 1
                    i += 2
                elif source.startswith('*/', i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
        elif source[i] == '"':
            i += 1
            while i < len(source):
                if source[i] == '\\':
                    i += 2
                elif source[i] == '"':
                    i += 1
                    break
                else:
                    i += 1
        else:
            i += 1
            continue
        for j in range(start, min(i, len(chars))):
            if chars[j] != '\n':
                chars[j] = ' '
    return ''.join(chars)


def close_delimiter(source, start):
    opening = source[start]
    closing = {'{': '}', '(': ')', '[': ']'}[opening]
    depth = 1
    for end in range(start + 1, len(source)):
        if source[end] == opening:
            depth += 1
        elif source[end] == closing:
            depth -= 1
            if depth == 0:
                return end
    raise ValueError(f'unclosed {opening} at {start}')


def split_items(body):
    """Split Rust fields at commas outside groups and generic type arguments."""
    stack = []
    start = 0
    for i, char in enumerate(body):
        if char in '{([<':
            stack.append(char)
        elif char in '})]>':
            if char == '>' and i and body[i - 1] == '-':
                continue
            if not stack or '{([<'.index(stack.pop()) != '})]>'.index(char):
                raise ValueError(f'unbalanced field syntax: {body!r}')
        elif char == ',' and not stack:
            if body[start:i].strip():
                yield body[start:i].strip()
            start = i + 1
    if stack:
        raise ValueError(f'unclosed field syntax: {body!r}')
    if body[start:].strip():
        yield body[start:].strip()


def without_attributes(item):
    item = item.strip()
    while item.startswith('#['):
        item = item[close_delimiter(item, 1) + 1:].strip()
    return item


def named_fields(body):
    fields = []
    for item in split_items(body):
        item = without_attributes(item)
        match = re.fullmatch(r'(?:pub(?:\([^)]*\))?\s+)?(r#\w+|\w+)\s*:\s*(.+)', item, re.S)
        if match is None:
            raise ValueError(f'unrecognized named field: {item!r}')
        fields.append({'name': match[1], 'type': ' '.join(match[2].split())})
    return fields


def declarations(source):
    clean = mask_comments(source)
    declaration = re.compile(r'\bpub\s+(struct|enum)\s+(\w+)\b')
    result = []
    for match in declaration.finditer(clean):
        # Generic bounds can contain parentheses. Find the declaration body
        # after the optional generic parameter list, rather than its first `(`.
        cursor = match.end()
        while clean[cursor].isspace():
            cursor += 1
        if clean[cursor] == '<':
            depth = 1
            cursor += 1
            while depth:
                if clean[cursor] == '<':
                    depth += 1
                elif clean[cursor] == '>':
                    depth -= 1
                cursor += 1
        while clean[cursor].isspace():
            cursor += 1
        if clean[cursor:].startswith('where'):
            cursor = clean.index('{', cursor)
        if clean[cursor] not in '{(;':
            raise ValueError(f'unsupported declaration header: {match[2]}')
        end = cursor if clean[cursor] == ';' else close_delimiter(clean, cursor)
        body = clean[cursor + 1:end]
        record = {'name': match[2], 'kind': match[1],
                  'line': source.count('\n', 0, match.start()) + 1,
                  'fields': [], 'variants': []}
        if match[1] == 'struct':
            if clean[cursor] == '{':
                record['fields'] = named_fields(body)
            elif clean[cursor] == '(':
                record['fields'] = [{'name': str(i), 'type': re.sub(r'^pub\s+', '', without_attributes(item))}
                                    for i, item in enumerate(split_items(body))]
        else:
            for item in split_items(body):
                item = without_attributes(item)
                variant = re.match(r'(\w+)', item)
                if variant is None:
                    raise ValueError(f'unrecognized variant: {item!r}')
                tail = item[variant.end():].strip()
                fields = []
                if tail.startswith('{'):
                    fields = named_fields(tail[1:close_delimiter(tail, 0)])
                elif tail.startswith('('):
                    fields = [{'name': str(i), 'type': ' '.join(field.split())}
                              for i, field in enumerate(split_items(tail[1:close_delimiter(tail, 0)]))]
                elif tail and not tail.startswith('='):
                    raise ValueError(f'unrecognized variant payload: {item!r}')
                record['variants'].append({'name': variant[1], 'fields': fields})
        result.append(record)
    return result


def inventory(repository):
    sources, types = {}, []
    guards = {file: (repository / ADAPTER / file).read_text() for file, _ in GUARDS.values()}
    type_policy = repository / ADAPTER / 'type_policy.rs'
    policy_text = mask_comments(type_policy.read_text())
    if re.search(r'\b_\s*=>|\.\.', policy_text):
        raise ValueError('Type policy must explicitly enumerate variants and fields')
    expression_policy = repository / ADAPTER / 'expression_policy.rs'
    if re.search(r'\b_\s*=>|\.\.', mask_comments(expression_policy.read_text())):
        raise ValueError('Expression policy must explicitly enumerate variants and fields')
    for path in sorted((repository / AST).rglob('*.rs')):
        source = path.read_text()
        relative = str(path.relative_to(repository))
        sources[relative] = hashlib.sha256(source.encode()).hexdigest()
        for record in declarations(source):
            record['source'] = relative
            guard = GUARDS.get(record['name'])
            if guard:
                file, function = guard
                text = mask_comments(guards[file])
                # Explicit patterns must name every upstream field and may not
                # use `..`; this proves enumeration, not nested acceptance.
                patterns = list(re.finditer(r'\b' + record['name'] + r'\s*\{', text))
                fields = {f['name'] for f in record['fields']}
                matches = []
                for pattern in patterns:
                    start = pattern.end() - 1
                    body = text[start + 1:close_delimiter(text, start)]
                    names = {part.split(':')[0].strip() for part in split_items(body)}
                    if names == fields:
                        matches.append(text.count('\n', 0, start) + 1)
                if not matches:
                    raise ValueError(f'{record["name"]}: missing exhaustive guard pattern')
                record['guard'] = {'file': str(ADAPTER / file), 'function': function,
                                   'pattern_lines': matches,
                                   'scope': 'explicit field enumeration; nested policy is separate'}
                record['classification'] = 'enumerated_by_guard'
            elif record['name'] in EXHAUSTIVE_TYPE_POLICIES:
                record['classification'] = 'exhaustive_variant_policy'
                record['policy'] = {
                    'file': str(ADAPTER / 'type_policy.rs'),
                    'scope': 'Compiled exhaustive matches, without wildcard arms or field elision; grammar context is documented separately',
                }
            elif record['name'] in EXHAUSTIVE_EXPRESSION_POLICIES:
                record['classification'] = 'exhaustive_variant_policy'
                record['policy'] = {
                    'file': str(ADAPTER / 'expression_policy.rs'),
                    'scope': 'Compiled exhaustive field/variant enumeration; delegated nested operator/function/literal policies remain separately reviewable',
                }
            else:
                record['classification'] = 'requires_context_review'
            types.append(record)
    for file, source in guards.items():
        sources[str(ADAPTER / file)] = hashlib.sha256(source.encode()).hexdigest()
    sources[str(type_policy.relative_to(repository))] = hashlib.sha256(type_policy.read_bytes()).hexdigest()
    sources[str(expression_policy.relative_to(repository))] = hashlib.sha256(expression_policy.read_bytes()).hexdigest()
    return {'schema_version': 1, 'scope': __doc__, 'sources': sources, 'types': types,
            'counts': {'types': len(types), 'named_and_tuple_fields': sum(len(t['fields']) + sum(len(v['fields']) for v in t['variants']) for t in types),
                       'enum_variants': sum(len(t['variants']) for t in types),
                       'types_with_explicit_field_guards': sum('guard' in t for t in types),
                       'types_with_exhaustive_variant_policies': sum('policy' in t for t in types)},
            'outside_grammar_field_coverage_complete': False,
            'limitations': ['Public AST helper types are inventoried too; this is not a parser reachability proof.',
                            'An enum variant can be rejected by its parent statement or dialect before any nested fields are reached.',
                            'Each outside-grammar field still needs a negative fixture or a source-backed unreachable classification.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args()
    print(json.dumps(inventory(args.repository), indent=2))


if __name__ == '__main__':
    main()
