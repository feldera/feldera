import copy
import unittest
from pathlib import Path
from unittest.mock import patch

from summarize_s4_coverage import summarize_paths


class PopulationCoverageTests(unittest.TestCase):
    def test_shared_paths_merge_counts_and_keep_bounded_examples(self):
        point = {
            "path": [{"container": "Repeat", "pass": "Graph", "index": 0}],
            "observations": 4,
            "cases": 2,
            "largest_iteration": 3,
            "examples": ["a/0", "a/1"],
        }
        first = {
            "cases": 2,
            "S4_observations_per_side": 4,
            "points": [point],
            "artifacts": {"first": "sha"},
            "artifact_byte_differences": [],
        }
        second = copy.deepcopy(first)
        second["points"][0].update(
            observations=6, largest_iteration=5, examples=["b/0", "b/1"]
        )
        second["S4_observations_per_side"] = 6
        with (
            patch(
                "summarize_s4_coverage.summarize",
                return_value={"stage": "S2", "cases": 4},
            ),
            patch("summarize_s4_coverage.audit", side_effect=[first, second]),
        ):
            report = summarize_paths([Path("input")], [Path("a"), Path("b")])
        self.assertEqual(10, report["S4_observations_per_side"])
        self.assertEqual(1, report["unique_paths"])
        self.assertEqual(4, report["points"][0]["cases"])
        self.assertEqual(5, report["points"][0]["largest_iteration"])
        self.assertEqual(["a/0", "a/1", "b/0", "b/1"], report["points"][0]["examples"])
        self.assertEqual(4, first["points"][0]["observations"])
        self.assertFalse(report["field_level_state_coverage_proven"])

    def test_wrong_stage_and_population_cannot_pass(self):
        with (
            patch(
                "summarize_s4_coverage.summarize",
                return_value={"stage": "S3", "cases": 1},
            ),
            self.assertRaisesRegex(ValueError, "S2 prefix"),
        ):
            summarize_paths([], [])
        with (
            patch(
                "summarize_s4_coverage.summarize",
                return_value={"stage": "S2", "cases": 1},
            ),
            self.assertRaisesRegex(ValueError, "populations disagree"),
        ):
            summarize_paths([], [])


if __name__ == "__main__":
    unittest.main()
