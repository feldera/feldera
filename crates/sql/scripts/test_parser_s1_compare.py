import copy
import json
from pathlib import Path
import unittest

from parser_s1_compare import Projection


class ParserS1ComparisonTests(unittest.TestCase):
    def fixture(self):
        schema = json.loads((Path(__file__).resolve().parents[1] / 'sql-rel-model/program-schema.json').read_text())
        schema.update(type='object', required=['position', 'source', 'literal'], additionalProperties=False,
                      properties={'position': {'$ref': '#/definitions/Coordinates'},
                                  'source': {'$ref': '#/definitions/Source'}, 'literal': {'type': 'string'}})
        value = {'position': {'line': 1, 'column': 2, 'end_line': 1, 'end_column': 3, 'quoted': True},
                 'source': {'name': 'input', 'text': 'original SQL'}, 'literal': 'user value'}
        return Projection(schema), value

    def test_only_named_coordinate_and_source_contracts_separate(self):
        projection, original = self.fixture()
        moved = copy.deepcopy(original)
        moved['position']['line'] = 5
        moved['source']['text'] = 'formatted SQL'
        a, before = projection.project(original)
        b, after = projection.project(moved)
        self.assertEqual(a, b)
        self.assertNotEqual(before, after)
        for field, replacement in [('literal', 'changed'), ('source', {'name': 'other', 'text': 'formatted SQL'})]:
            changed = copy.deepcopy(moved)
            changed[field] = replacement
            self.assertNotEqual(a, projection.project(changed)[0])
        moved['position']['quoted'] = False
        self.assertNotEqual(a, projection.project(moved)[0])

    def test_unknown_fields_and_bad_coordinate_types_fail_closed(self):
        projection, original = self.fixture()
        original['position']['unexpected'] = 0
        with self.assertRaises(ValueError):
            projection.project(original)
        del original['position']['unexpected']
        original['position']['line'] = True
        with self.assertRaises(ValueError):
            projection.project(original)


if __name__ == '__main__':
    unittest.main()
