import copy
import json
import tempfile
import unittest
from pathlib import Path

from s4_coverage import audit


class S4CoverageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.direct, self.replay = self.root / "direct", self.root / "replay"
        self.manifest = {
            "schema_version": 1,
            "failed_cases": 0,
            "observation_contract": "full-prefix-v1",
            "capture": True,
            "cases": 1,
        }
        self.case = {"id": "test/0", "observations": {"S4": {"count": 2}}}
        self.coverage = {
            "id": "test/0",
            "observations": 2,
            "points": [
                {
                    "path": [{"container": "Repeat", "pass": "Graph", "index": 0}],
                    "observations": 2,
                    "largest_iteration": 1,
                }
            ],
        }
        for directory in (self.direct, self.replay):
            directory.mkdir()
            self.write(directory)

    def write(self, directory, coverage=None):
        (directory / "complete.json").write_text(json.dumps(self.manifest))
        (directory / "cases.jsonl").write_text(json.dumps(self.case) + "\n")
        (directory / "s4-paths.jsonl").write_text(
            json.dumps(self.coverage if coverage is None else coverage) + "\n"
        )

    def test_counts_match_fingerprints_without_claiming_unobserved_branches(self):
        report = audit(self.direct, self.replay)
        self.assertEqual(1, report["unique_paths"])
        self.assertEqual(2, report["S4_observations_per_side"])
        self.assertEqual(["test/0"], report["points"][0]["examples"])
        self.assertFalse(report["all_conditional_branches_covered"])

    def test_missing_case_and_duplicate_path_fail(self):
        (self.replay / "s4-paths.jsonl").write_text("")
        with self.assertRaisesRegex(ValueError, "Missing case"):
            audit(self.direct, self.replay)
        coverage = copy.deepcopy(self.coverage)
        coverage["points"] *= 2
        self.write(self.replay, coverage)
        with self.assertRaisesRegex(ValueError, "Duplicate S4 path"):
            audit(self.direct, self.replay)

    def test_false_counts_and_reordered_identity_fail(self):
        for field, value, message in [
            ("observations", 3, "counts disagree"),
            ("id", "other/0", "identity"),
        ]:
            coverage = copy.deepcopy(self.coverage)
            coverage[field] = value
            self.write(self.replay, coverage)
            with self.assertRaisesRegex(ValueError, message):
                audit(self.direct, self.replay)

    def test_gen2_semantic_contract_retains_raw_byte_differences(self):
        self.manifest["artifact_contract"] = "seam-v1"
        self.case.update(
            gen2={"circuit.json": "semantic-hash"}, gen2_bytes={"circuit.json": "raw-a"}
        )
        self.write(self.direct)
        self.case["gen2_bytes"]["circuit.json"] = "raw-b"
        self.write(self.replay)
        report = audit(self.direct, self.replay)
        self.assertEqual(["test/0"], report["artifact_byte_differences"])
        self.case["gen2"]["circuit.json"] = "different-semantics"
        self.write(self.replay)
        with self.assertRaisesRegex(ValueError, "records or S4 coverage differ"):
            audit(self.direct, self.replay)
        self.case["gen2"]["circuit.json"] = "semantic-hash"
        del self.case["gen2_bytes"]
        self.write(self.replay)
        with self.assertRaisesRegex(ValueError, "Missing Gen-2 byte"):
            audit(self.direct, self.replay)

    def test_path_changes_and_noninteger_indexes_fail(self):
        coverage = copy.deepcopy(self.coverage)
        coverage["points"][0]["path"][0]["pass"] = "Other"
        self.write(self.replay, coverage)
        with self.assertRaisesRegex(ValueError, "coverage differ"):
            audit(self.direct, self.replay)
        for value in (True, 1.5, -1):
            coverage["points"][0]["path"][0]["index"] = value
            self.write(self.replay, coverage)
            with self.assertRaisesRegex(ValueError, "Malformed S4"):
                audit(self.direct, self.replay)


if __name__ == "__main__":
    unittest.main()
