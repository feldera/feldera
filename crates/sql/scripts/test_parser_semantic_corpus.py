import unittest

from parser_semantic_corpus import classify
import test_parser_semantic_inputs as fixtures


class ParserSemanticCorpusTests(unittest.TestCase):
    def test_actual_parser_refusal_is_explicit(self):
        fixture = fixtures.ParserSemanticInputsTests()
        source = fixture.source()
        parsed = fixture.observation(source)
        parsed['submissions'][0]['measurements'][0].update(verdict='rejected', message='not supported')
        result = classify(source, parsed)
        self.assertEqual(result['id'], source['id'])
        self.assertEqual(result['parser_refusals'][0]['submission'], 0)
        self.assertNotIn('emitted', result)

    def test_accepted_case_without_emission_cannot_be_excluded(self):
        fixture = fixtures.ParserSemanticInputsTests()
        source = fixture.source()
        parsed = fixture.observation(source)
        del parsed['submissions'][0]['measurements'][0]['feldera_sql']
        with self.assertRaisesRegex(ValueError, 'No AST-derived SQL'):
            classify(source, parsed)


if __name__ == '__main__':
    unittest.main()
