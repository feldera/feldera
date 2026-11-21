import unittest
from pathlib import Path

from parser_ast_inventory import declarations, inventory


class ParserAstInventoryTests(unittest.TestCase):
    def test_fields_ignore_comments_and_keep_nested_types_and_variant_payloads(self):
        source = '''
// pub struct Invented { hidden: Fake }
/* nested /* pub enum Fake { X } */ comment */
pub struct Holder<T> {
    #[cfg_attr(feature = "visitor", visit(with = "ignore"))]
    pub entries: Vec<(String, Option<Box<T>>)>,
    pub callback: fn(u64, u64) -> bool,
}
pub enum Shape {
    Unit,
    Tuple(Vec<(u64, String)>, Box<Holder<u64>>),
    Named { first: Option<u64>, second: Vec<String> },
}
pub struct Pair(pub u64, pub Option<String>);
'''
        records = declarations(source)
        self.assertEqual([r['name'] for r in records], ['Holder', 'Shape', 'Pair'])
        self.assertEqual(records[0]['fields'], [
            {'name': 'entries', 'type': 'Vec<(String, Option<Box<T>>)>'},
            {'name': 'callback', 'type': 'fn(u64, u64) -> bool'},
        ])
        self.assertEqual([v['name'] for v in records[1]['variants']], ['Unit', 'Tuple', 'Named'])
        self.assertEqual(records[1]['variants'][2]['fields'][1]['name'], 'second')
        self.assertEqual(len(records[2]['fields']), 2)

    def test_current_source_is_fully_enumerated_without_claiming_complete_policy(self):
        result = inventory(Path(__file__).resolve().parents[3])
        self.assertGreater(result['counts']['named_and_tuple_fields'], 2000)
        types = {t['name']: t for t in result['types']}
        self.assertEqual(types['CreateTable']['classification'], 'enumerated_by_guard')
        self.assertEqual(types['Expr']['classification'], 'exhaustive_variant_policy')
        self.assertEqual(types['DataType']['classification'], 'exhaustive_variant_policy')
        self.assertIn('local', [f['name'] for f in types['CreateView']['fields']])
        self.assertFalse(result['outside_grammar_field_coverage_complete'])


if __name__ == '__main__':
    unittest.main()
