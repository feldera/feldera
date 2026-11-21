"""Verify that the D1 input split retains its common denominator."""

import tempfile
import unittest
from pathlib import Path

from d1_holdout import compare, groups, prepare, rows


def case(source_id, method, program):
    return {
        "source_id": source_id,
        "method": method,
        "population": "classic_export",
        "program": program,
        "submissions": [{"sql": program, "batch": 0, "many": False}],
        "java_diagnostics": [],
    }


class D1HoldoutTests(unittest.TestCase):
    def test_linked_methods_and_duplicate_programs_share_a_partition(self):
        records = [
            case("a/0", "a", "SELECT 0"),
            case("b/0", "b", "SELECT 0"),
            case("b/1", "b", "SELECT 1"),
            case("c/0", "c", "SELECT 1"),
            case("d/0", "d", "SELECT 2"),
        ]
        expected = {"a": "a", "b": "a", "c": "a", "d": "d"}
        self.assertEqual(groups(records), expected)
        self.assertEqual(groups(list(reversed(records))), expected)

    def test_limit_is_counted_in_the_denominator(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "split"
            prepare([case("a/0", "a", "x" * 65_536), case("a/1", "a", "SELECT 1")], target)
            corpus = rows(target / "cases.jsonl")
            manifest = rows(target / "binder-input.jsonl")
            self.assertEqual([record["id"] for record in manifest], [1])
            self.assertEqual(len({record["partition"] for record in corpus}), 1)
            outputs = compare(corpus, [{"id": 1, "accepted": False}],
                              [{"id": 1, "accepted": True}])
            self.assertEqual({row["current_result"]: row["cases"] for row in outputs},
                             {"manifest_limit": 1, "h_only": 1})

    def test_truncated_and_duplicate_binder_results_fail(self):
        corpus = [{"id": 0, "transport": "available"}]
        observed = [{"id": 0, "accepted": True}]
        with self.assertRaisesRegex(ValueError, "ids do not match"):
            compare(corpus, observed, [])
        with self.assertRaisesRegex(ValueError, "duplicate binder result"):
            compare(corpus, observed + observed, observed)


if __name__ == "__main__":
    unittest.main()
