import json
from pathlib import Path
import tempfile
import unittest

from correlation_coverage import equal_without_id, indexed, validate_runtime


def fixture(root):
    case = 'Case#test/0'
    identity = case + '/correlation-stage/1'
    fragment = {'id': identity, 'typed_value': {'kind': 'integer', 'value': 1}}
    recipe = {'id': identity, 'removed_correlates': 1, 'nodes': [{'kind': 'project'}]}
    directory = root / 'run'
    (directory / 'surefire-reports').mkdir(parents=True)
    (directory / 'surefire-reports/TEST-Example.xml').write_text(
        '<testsuite name="Example" tests="1" failures="0" errors="0" skipped="0"/>')
    (directory / 'surefire-reports/TEST-Unrelated.xml').write_text(
        '<testsuite name="Unrelated" tests="1" failures="1" errors="0" skipped="0"/>')
    (directory / 'tests.log').write_text('Executed 1, checked 0 Rust tests\nBUILD SUCCESS\n')
    (directory / 'generated').mkdir()
    (directory / 'generated/lib.rs').write_text('#[test]\nfn test0() { circuit.transaction(); }\n')
    invocation = directory / 'rewrites/test/invocation-1'
    (invocation / 'rust').mkdir(parents=True)
    captured = dict(fragment, id='invocation-1')
    emitted = dict(recipe, id='invocation-1')
    (invocation / 'fragment.jsonl').write_text(json.dumps(captured) + '\n')
    (invocation / 'rust/recipes.jsonl').write_text(json.dumps({'id': 'invocation-1', 'status': 'rewritten', 'recipe': emitted}) + '\n')
    (invocation / 'rust/complete.json').write_text(json.dumps({'complete': True, 'rewritten': 1, 'refused': 0}))
    review = {'runs': {'run': {'junit_class': 'Example', 'junit_tests': 1, 'rust_tests': 1, 'transactions': 1}},
              'cases': [{'id': case, 'run': 'run', 'path': 'rewrites/test'}]}
    return review, {identity: fragment}, {identity: {'status': 'rewritten', 'recipe': recipe}}


class CorrelationCoverageTests(unittest.TestCase):
    def test_ignore_label_but_preserve_types_and_array_order(self):
        self.assertTrue(equal_without_id({'id': 'a', 'n': 1}, {'n': 1, 'id': 'b'}))
        self.assertFalse(equal_without_id({'n': 1}, {'n': True}))
        self.assertFalse(equal_without_id({'n': [1, 2]}, {'n': [2, 1]}))

    def test_duplicate_id_fails(self):
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            indexed([{'id': 'a'}, {'id': 'a'}])

    def test_named_runtime_report_and_exact_payload_establish_witness(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs, witnesses = validate_runtime(root, *fixture(root))
            self.assertEqual(len(witnesses), 1)
            self.assertEqual(runs['run']['transactions'], 1)

    def test_changed_typed_fragment_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            review, fragments, recipes = fixture(root)
            next(iter(fragments.values()))['typed_value']['value'] = True
            with self.assertRaisesRegex(ValueError, 'fragment differs'):
                validate_runtime(root, review, fragments, recipes)

    def test_changed_final_recipe_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            review, fragments, recipes = fixture(root)
            next(iter(recipes.values()))['recipe']['nodes'][0]['kind'] = 'filter'
            with self.assertRaisesRegex(ValueError, 'recipe differs'):
                validate_runtime(root, review, fragments, recipes)

    def test_skipped_runtime_or_unexecuted_generated_test_fails(self):
        for skipped in (True, False):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                args = fixture(root)
                path = root / ('run/surefire-reports/TEST-Example.xml' if skipped else 'run/generated/lib.rs')
                path.write_text(path.read_text().replace('skipped="0"', 'skipped="1"') if skipped else '#[test]\nfn test0() {}')
                with self.assertRaises(ValueError):
                    validate_runtime(root, *args)

    def test_internal_witness_label_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = fixture(root)
            path = root / 'run/rewrites/test/invocation-1/rust/recipes.jsonl'
            record = json.loads(path.read_text()); record['id'] = 'other'
            path.write_text(json.dumps(record) + '\n')
            with self.assertRaisesRegex(ValueError, 'identities differ'):
                validate_runtime(root, *args)


if __name__ == '__main__':
    unittest.main()
