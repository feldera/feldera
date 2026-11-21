import copy
import json
from pathlib import Path
import tempfile
import unittest

from compare_type_bindings import project, run


def sample():
    source_type = {'sql_type': 'INTEGER', 'nullable': False}
    target_type = {'sql_type': 'DECIMAL', 'nullable': False, 'precision': 38, 'scale': 19}
    identity = {'identity_status': 'registered', 'registry': 'calcite-s1-fixture',
                'registry_version': '1', 'key': 'SqlStdOperatorTable.CAST'}
    original = {'capture_node': 3, 'original_argument': 0, 'type': source_type}
    cast = {'kind': 'CAST', 'capture_node': 1, 'operator': identity, 'type': target_type,
            'operands': [original, {'capture_node': 2, 'type': copy.deepcopy(target_type)}]}
    return {'probe': {'operation': 'ABS'},
            'validated_call': {'operator': dict(identity, key='custom-abs'),
                               'type': target_type, 'operands': [cast]},
            'argument_sources': [{'argument': 0, 'operand': 0, 'original_node': 3,
                                  'inferred_cast_nodes': [1]}]}


class BindingComparisonTest(unittest.TestCase):
    def test_boolean_ids_schema_versions_and_runtime_claims_fail_closed(self):
        java = {'schema_version': 1, 'runtime_verified': False, 'compiler_bug': False,
                'capture_complete': True, 'capture_errors': [], 'accepted': False,
                'probe': {'schema_version': 1, 'id': 0, 'operation': 'ABS', 'arguments': []}}
        rust = {'schema_version': 1, 'id': 0, 'runtime_verified': False,
                'translated_binding': {'status': 'rejected'}, 'rewritten_binding': {'status': 'rejected'}}
        for owner, field, value in [('rust', 'id', False), ('probe', 'id', False),
                                    ('rust', 'schema_version', True), ('java', 'schema_version', True),
                                    ('probe', 'schema_version', True), ('java', 'runtime_verified', 0),
                                    ('rust', 'runtime_verified', True), ('java', 'accepted', 0)]:
            with self.subTest(owner=owner, field=field, value=value), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                j, r = copy.deepcopy(java), copy.deepcopy(rust)
                target = r if owner == 'rust' else j['probe'] if owner == 'probe' else j
                target[field] = value
                (root / 'java').write_text(json.dumps(j) + '\n')
                (root / 'rust').write_text(json.dumps(r) + '\n')
                with self.assertRaises(ValueError):
                    run(root / 'java', root / 'rust', root / 'out', 1)
                self.assertFalse((root / 'out/complete.json').exists())

    def test_added_cast_and_original_type_remain_distinct(self):
        result = project(sample())
        argument = result['arguments'][0]
        self.assertEqual(argument['original_type']['sql_type'], 'INTEGER')
        self.assertEqual(argument['coerced_type']['sql_type'], 'DECIMAL')
        self.assertEqual(argument['inferred_casts'], [argument['coerced_type']])
        self.assertEqual(result['operator']['key'], 'custom-abs')

    def test_missing_or_reordered_cast_chain_fails(self):
        for ids in ([], [2], [1, 1]):
            value = sample()
            value['argument_sources'][0]['inferred_cast_nodes'] = ids
            with self.assertRaises(ValueError):
                project(value)

    def test_changed_target_type_is_not_hidden_by_cast_result(self):
        value = sample()
        value['validated_call']['operands'][0]['operands'][1]['type']['precision'] = 10
        with self.assertRaises(ValueError):
            project(value)
        value = sample()
        value['validated_call']['operands'][0]['operands'][1]['type']['nullable'] = 0
        with self.assertRaises(ValueError):
            project(value)

    def test_unmapped_default_or_overlapping_arguments_fail(self):
        value = sample()
        value['validated_call']['operands'].append({'sql': 'invented'})
        with self.assertRaises(ValueError):
            project(value)
        value = sample()
        value['argument_sources'].append(copy.deepcopy(value['argument_sources'][0]))
        with self.assertRaises(ValueError):
            project(value)


if __name__ == '__main__':
    unittest.main()
