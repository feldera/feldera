import unittest

from parser_semantic_inputs import pair


class ParserSemanticInputsTests(unittest.TestCase):
    def source(self):
        return {'id': 'one', 'options': {'languageOptions': {'optimizationLevel': 0}},
                'submissions': [{'sql': 'SELECT   1', 'many': False, 'batch': 0}]}

    def observation(self, source):
        return {'id': source['id'], 'submissions': [
            {'source': source['submissions'][0], 'measurements': [
                {'version': '0.64.0', 'dialect': 'feldera_prototype', 'verdict': 'accepted',
                 'statements': ['THIS DISPLAY IS NOT THE EMITTER'],
                 'feldera_sql': {'status': 'emitted', 'statements': ['SELECT 1']}}]}]}

    def test_emission_preserves_options_boundaries_and_never_uses_display_or_source(self):
        source = self.source()
        result = pair(source, self.observation(source))
        self.assertEqual(result['original'], source)
        self.assertEqual(result['emitted']['submissions'], [{'sql': 'SELECT 1', 'many': False, 'batch': 0}])
        self.assertEqual(result['emitted']['options'], source['options'])
        self.assertEqual(source['submissions'][0]['sql'], 'SELECT   1')

    def test_refusal_never_falls_back_to_original_sql(self):
        source = self.source()
        observation = self.observation(source)
        observation['submissions'][0]['measurements'][0]['feldera_sql'] = {
            'status': 'unsupported', 'reason': 'unknown field'}
        with self.assertRaisesRegex(ValueError, 'No AST-derived SQL'):
            pair(source, observation)

    def test_single_statement_mode_and_identity_cannot_change(self):
        source = self.source()
        observation = self.observation(source)
        observation['submissions'][0]['measurements'][0]['feldera_sql']['statements'].append('SELECT 2')
        with self.assertRaisesRegex(ValueError, 'Malformed AST-emitted'):
            pair(source, observation)
        observation['id'] = 'other'
        with self.assertRaisesRegex(ValueError, 'identity'):
            pair(source, observation)


if __name__ == '__main__':
    unittest.main()
