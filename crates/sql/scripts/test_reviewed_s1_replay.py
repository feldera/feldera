import json
import tempfile
import unittest
from pathlib import Path

from reviewed_s1_replay import IDENTITY, program_inventory, sha, verify_replacement


class ReviewedS1ProvenanceTests(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory)
        original = root / 'original.jsonl'
        replacement = root / 'replacement.jsonl'
        output = root / 'output.jsonl'
        rows = [{"id": "unrelated", "statements": []}, {"id": IDENTITY, "statements": ["old"]}]
        updated = dict(rows[1], statements=["reviewed"])
        original.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        replacement.write_text(json.dumps(updated) + '\n')
        output.write_text(json.dumps(rows[0]) + '\n' + replacement.read_text())
        provenance = {"original_sha256": sha(original), "replacement_sha256": sha(replacement),
                      "output_sha256": sha(output), "programs": 2, "changed_programs": 1,
                      "changed_identity": IDENTITY}
        return original, replacement, output, provenance

    def test_exactly_one_reviewed_replacement_passes(self):
        with tempfile.TemporaryDirectory() as directory:
            result = verify_replacement(*self.fixture(directory))
            self.assertEqual(result['changed_programs'], 1)

    def test_unrelated_bytes_cannot_change_even_with_updated_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            original, replacement, output, provenance = self.fixture(directory)
            output.write_text(output.read_text().replace('"statements": []', '"statements": [1]'))
            provenance['output_sha256'] = sha(output)
            with self.assertRaisesRegex(ValueError, 'unreviewed'):
                verify_replacement(original, replacement, output, provenance)

    def test_missing_replacement_and_population_truncation_fail(self):
        for missing in (False, True):
            with tempfile.TemporaryDirectory() as directory:
                original, replacement, output, provenance = self.fixture(directory)
                output.write_text(original.read_text() if missing else replacement.read_text())
                provenance['output_sha256'] = sha(output)
                with self.assertRaises(ValueError):
                    verify_replacement(original, replacement, output, provenance)

    def test_sql_and_program_order_must_match(self):
        with tempfile.TemporaryDirectory() as directory:
            original, replacement, output, _ = self.fixture(directory)
            self.assertEqual(program_inventory(output, original), (2, 1))
            original.write_text(''.join(reversed(original.read_text().splitlines(keepends=True))))
            with self.assertRaisesRegex(ValueError, 'identity, order'):
                program_inventory(output, original)


if __name__ == '__main__':
    unittest.main()
