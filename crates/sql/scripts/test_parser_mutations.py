import json
import tempfile
import unittest
from pathlib import Path

from parser_mutations import mutations, prepare


class MutationTests(unittest.TestCase):
    def test_each_edit_is_reproducible_and_reconstructs_the_input(self):
        for sql in ["SELECT '🙂'", "VALUES 1;\n", "x", "-- comment\nSELECT 1"]:
            first = list(mutations(sql))
            self.assertEqual(first, list(mutations(sql)))
            self.assertEqual(4, len(first))
            for _, changed, edit in first:
                offset = edit["offset_codepoints"]
                self.assertNotEqual(sql, changed)
                self.assertEqual(
                    sql,
                    changed[:offset]
                    + edit["removed"]
                    + changed[offset + len(edit["inserted"]) :],
                )

    def test_deduplication_preserves_modes_origins_and_order_independence(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "input.jsonl"
            cases = [
                {
                    "id": identity,
                    "submissions": [
                        {"sql": "SELECT 1", "many": many, "batch": 2},
                        {"sql": "", "many": True, "batch": 0},
                    ],
                }
                for identity, many in [("b", True), ("a", True), ("c", False)]
            ]
            path.write_text("".join(json.dumps(case) + "\n" for case in cases))
            inputs, report = prepare(path)
            self.assertEqual(10, len(inputs))
            self.assertEqual(3, report["empty_submissions_skipped"])
            self.assertEqual(2, report["distinct_submissions"])
            originals = [row for row in report["provenance"] if "origins" in row]
            self.assertEqual(3, sum(len(row["origins"]) for row in originals))
            path.write_text(
                "".join(json.dumps(case) + "\n" for case in reversed(cases))
            )
            reordered_inputs, reordered_report = prepare(path)
            self.assertEqual(inputs, reordered_inputs)
            self.assertEqual(report["provenance"], reordered_report["provenance"])

    def test_bad_source_cannot_become_mutation_evidence(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "input.jsonl"
            case = {
                "id": "a",
                "submissions": [{"sql": "SELECT 1", "many": True, "batch": 0}],
            }
            path.write_text((json.dumps(case) + "\n") * 2)
            with self.assertRaisesRegex(ValueError, "duplicate"):
                prepare(path)
            case["submissions"][0]["batch"] = False
            path.write_text(json.dumps(case) + "\n")
            with self.assertRaisesRegex(ValueError, "Malformed"):
                prepare(path)
