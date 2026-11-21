import copy
import json
import unittest

from type_binding_compare import compare, graph_inventory, no_duplicates


def fixture():
    common = {'accepted': True, 'compiler_bug': False, 'probe': {'id': 0, 'operation': 'ABS'},
              'schema': {'nullable': False}, 'diagnostics': [], 'options': {}}
    capture = dict(copy.deepcopy(common), schema_version=1, runtime_verified=False,
                   capture_complete=True, capture_errors=[], bindings=[])
    return common, capture


class BindingComparisonTests(unittest.TestCase):
    def test_equal_fields_do_not_claim_c9(self):
        before, after = fixture()
        result = compare([before], [after], 1)
        self.assertTrue(result['all_compared_fields_equal'])
        self.assertFalse(result['full_C9_verified'])

    def test_changed_schema_and_diagnostic_are_differences(self):
        before, after = fixture()
        after['schema']['nullable'] = True
        after['diagnostics'] = [{'message': 'new warning'}]
        result = compare([before], [after], 1)
        self.assertEqual(result['differences'][0]['fields'], ['schema', 'diagnostics'])

    def test_truncation_reorder_bug_and_capture_failure_are_invalid(self):
        before, after = fixture()
        with self.assertRaises(ValueError):
            compare([before], [], 1)
        for key, value in [('compiler_bug', True), ('capture_complete', False)]:
            changed = dict(after, **{key: value})
            with self.assertRaises(ValueError):
                compare([before], [changed], 1)
        after['probe']['id'] = 1
        with self.assertRaises(ValueError):
            compare([before], [after], 1)

    def test_boolean_and_integer_metadata_do_not_compare_equal(self):
        before, after = fixture()
        after['schema']['nullable'] = 0
        self.assertFalse(compare([before], [after], 1)['all_compared_fields_equal'])

    def test_new_default_options_stay_explicit_differences(self):
        before, after = fixture()
        before['options'] = {'ioOptions': {'quiet': True}}
        after['options'] = {'ioOptions': {'quiet': True, 'readS2': False}}
        result = compare([before], [after], 1)
        self.assertFalse(result['all_compared_fields_equal'])
        self.assertTrue(result['all_verdict_schema_diagnostics_inputs_equal'])
        self.assertEqual(sum(result['option_schema_extensions'].values()), 1)

    def test_duplicate_json_key_is_invalid(self):
        with self.assertRaises(ValueError):
            json.loads('{"id":0,"id":1}', object_pairs_hook=no_duplicates)

    def test_missing_expression_reference_is_invalid(self):
        graph = {'stage': 'validated_sql', 'root': 0, 'nodes': [{'id': 0, 'operands': [1]}]}
        with self.assertRaisesRegex(ValueError, 'reference'):
            graph_inventory([graph])

    def test_operator_names_cannot_replace_registered_identity(self):
        graph = {'stage': 'validated_sql', 'root': 0,
                 'nodes': [{'id': 0, 'operator': {'kind': 'OTHER_FUNCTION', 'name': 'ABS'}}]}
        with self.assertRaisesRegex(ValueError, 'unregistered'):
            graph_inventory([graph])


if __name__ == '__main__':
    unittest.main()
