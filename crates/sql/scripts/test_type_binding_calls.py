import copy
import unittest

from type_binding_calls import extract_call


def fixture(origin='column'):
    def operator(kind, name=None):
        return {'kind': kind, 'name': name or kind, 'identity_status': 'registered',
                'registry': 'calcite-s1-fixture', 'registry_version': '1',
                'key': 'SqlStdOperatorTable.' + (name or kind)}
    def spec(identifier, line):
        return {'id': identifier, 'kind': 'OTHER', 'class': 'org.apache.calcite.sql.SqlDataTypeSpec',
                'position': {'line': line, 'column': line}, 'sql': 'VARCHAR(5)'}
    nodes = [
        {'id': 0, 'kind': 'SELECT', 'operands': [None, 1],
         'type': {'fields': [{'name': 'result'}]}},
        {'id': 1, 'class': 'org.apache.calcite.sql.SqlNodeList', 'elements': [2]},
        {'id': 2, 'kind': 'AS', 'operator': operator('AS'), 'operands': [3, 4]},
        {'id': 3, 'kind': 'OTHER_FUNCTION', 'operator': operator('OTHER_FUNCTION', 'ABS'),
         'type_known': True, 'type': {'sql_type': 'DECIMAL', 'precision': 38, 'scale': 19}, 'operands': [5]},
        {'id': 4, 'kind': 'IDENTIFIER', 'names': ['result']},
        {'id': 5, 'kind': 'CAST', 'operator': operator('CAST'), 'operands': [6, 7]},
        {'id': 6, 'kind': 'IDENTIFIER', 'names': ['t', 'a']},
        spec(7, 0),
    ]
    if origin == 'literal':
        nodes[6] = {'id': 6, 'kind': 'CAST', 'operator': operator('CAST'), 'operands': [8, 9]}
        nodes.extend([{'id': 8, 'kind': 'LITERAL', 'literal_type': 'CHAR', 'sql': "'12.5'"}, spec(9, 2)])
    return {'accepted': True, 'probe': {'id': 7, 'operation': 'ABS', 'arguments': [{'origin': origin}]},
            'bindings': [{'stage': 'validated_sql', 'root': 0, 'nodes': nodes}]}


class ValidatedCallTests(unittest.TestCase):
    def test_column_cast_and_type_metadata_are_preserved(self):
        record = fixture()
        original = copy.deepcopy(record)
        result = extract_call(record)
        self.assertEqual(result['validated_call']['type']['precision'], 38)
        operand = result['validated_call']['operands'][0]
        self.assertEqual(operand['source_role'], 'inferred_cast')
        self.assertEqual(operand['operands'][0]['names'], ['t', 'a'])
        self.assertEqual(record, original)

    def test_literal_original_cast_is_distinct_from_inferred_wrapper(self):
        result = extract_call(fixture('literal'))
        wrapper = result['validated_call']['operands'][0]
        self.assertEqual(wrapper['source_role'], 'inferred_cast')
        self.assertEqual(wrapper['operands'][0]['source_role'], 'original_literal_cast')
        self.assertEqual(wrapper['operands'][0]['operands'][0]['sql'], "'12.5'")
        self.assertEqual(result['argument_sources'][0]['inferred_cast_nodes'], [5])

    def test_ambiguous_result_query_is_rejected(self):
        record = fixture()
        record['bindings'].append(copy.deepcopy(record['bindings'][0]))
        with self.assertRaisesRegex(ValueError, 'exactly one'):
            extract_call(record)

    def test_wrong_alias_or_column_cannot_be_silently_selected(self):
        for index in (4, 6):
            record = fixture()
            record['bindings'][0]['nodes'][index]['names'] = ['wrong']
            with self.assertRaises(ValueError):
                extract_call(record)

    def test_literal_cast_provenance_requires_original_source_target(self):
        record = fixture('literal')
        record['bindings'][0]['nodes'][9]['position']['line'] = 0
        with self.assertRaisesRegex(ValueError, 'source target'):
            extract_call(record)

    def test_trim_defaults_are_retained_as_distinct_operands(self):
        record = fixture()
        record['probe']['operation'] = 'TRIM'
        nodes = record['bindings'][0]['nodes']
        nodes.extend([{'id': 8, 'literal_type': 'SYMBOL', 'sql': 'BOTH'},
                      {'id': 9, 'literal_type': 'CHAR', 'sql': "' '"}])
        nodes[3]['operator']['name'] = 'TRIM'
        nodes[3]['operator']['key'] = 'SqlStdOperatorTable.TRIM'
        nodes[3]['operands'] = [8, 9, 5]
        result = extract_call(record)
        operands = result['validated_call']['operands']
        self.assertEqual(operands[0]['source_role'], 'trim_default_flag')
        self.assertEqual(operands[1]['sql'], "' '")
        self.assertEqual(result['argument_sources'][0]['operand'], 2)

    def test_different_selected_operation_is_rejected(self):
        record = fixture()
        record['probe']['operation'] = 'SQRT'
        with self.assertRaisesRegex(ValueError, 'original operation'):
            extract_call(record)

    def test_cycle_or_unknown_operator_fails(self):
        record = fixture()
        record['bindings'][0]['nodes'][5]['operands'][0] = 5
        with self.assertRaisesRegex(ValueError, 'cycle'):
            extract_call(record)
        record = fixture()
        del record['bindings'][0]['nodes'][3]['operator']['identity_status']
        with self.assertRaisesRegex(ValueError, 'unregistered'):
            extract_call(record)


if __name__ == '__main__':
    unittest.main()
