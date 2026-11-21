import json
import tempfile
import unittest
from pathlib import Path

from determinism import compare
from run_determinism import file_digest
from summarize_seam_corpus import summarize


def write_json(path, value):
    path.write_text(json.dumps(value))


class SeamCorpusTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.inputs = [self.root / "population.jsonl"]
        self.cases = [
            {"id": name, "input_sha256": name * 64, "program": "SELECT 1"}
            for name in ("a", "b")
        ]
        self.inputs[0].write_text("".join(json.dumps(c) + "\n" for c in self.cases))
        self.shards = [
            self.make_shard(index, case) for index, case in enumerate(self.cases)
        ]

    def make_shard(self, index, case):
        shard = self.root / str(index)
        shard.mkdir()
        selected = shard / "input.jsonl"
        selected.write_text(json.dumps(case) + "\n")
        manifest = {
            "schema_version": 1,
            "producer": "compiler",
            "runtime": "runtime",
            "input_sha256": file_digest(selected),
            "cases": 1,
            "capture": True,
            "artifact_contract": "seam-v1",
            "dataflow_contract": "all-engines-v1",
            "observation_contract": "full-prefix-v1",
            "failed_cases": 0,
        }
        record = {
            "id": case["id"],
            "input_sha256": case["input_sha256"],
            "schema": "schema",
            "diagnostics": "diagnostics",
            "exported_schema_equal": True,
            "exported_diagnostics_equal": True,
            "rust": {"dataflow.json": "dataflow", "circuit.rs": "rust"},
            "hash_inputs": "hashes",
            "observations": {
                stage: {"count": 1, "sha256": "observations"}
                for stage in ("S2", "S3", "S4")
            },
        }
        for mode in ("capture", "replay-s2"):
            directory = shard / mode
            directory.mkdir()
            write_json(directory / "complete.json", manifest)
            (directory / "cases.jsonl").write_text(json.dumps(record) + "\n")
        report = {
            **manifest,
            **compare(shard / "capture", shard / "replay-s2"),
            "classpath": {"classes": "digest"},
            "comparator_sha256": "comparator",
            "stage": "S2",
            "jvm_runs": 2,
            "runtime_executed": False,
        }
        write_json(shard / "complete.json", report)
        return shard

    def test_complete_disjoint_population_is_rechecked(self):
        result = summarize(self.inputs, self.shards)
        self.assertEqual(result["cases"], 2)
        self.assertEqual(result["observations_per_side"], 6)
        self.assertTrue(result["complete_prepared_population"])
        self.assertFalse(result["runtime_executed"])

    def test_missing_and_overlapping_shards_fail(self):
        for shards in (self.shards[:1], [*self.shards, self.shards[0]]):
            with self.assertRaises(ValueError):
                summarize(self.inputs, shards)
        with self.assertRaises(ValueError):
            summarize(self.inputs * 2, self.shards)

    def test_changed_sql_cannot_keep_its_original_identity(self):
        self.cases[0]["program"] = "SELECT 2"
        self.inputs[0].write_text("".join(json.dumps(c) + "\n" for c in self.cases))
        with self.assertRaisesRegex(ValueError, "differs from prepared"):
            summarize(self.inputs, self.shards)

    def test_changed_fingerprints_invalidate_a_passing_summary(self):
        path = self.shards[0] / "replay-s2/cases.jsonl"
        record = json.loads(path.read_text())
        record["rust"]["dataflow.json"] = "changed"
        write_json(path, record)
        with self.assertRaisesRegex(ValueError, "Fresh comparison"):
            summarize(self.inputs, self.shards)

    def test_weaker_contract_and_mixed_builds_fail(self):
        path = self.shards[1] / "complete.json"
        original = json.loads(path.read_text())
        for key, value in [
            ("dataflow_contract", "gen2-only-v1"),
            ("observation_contract", "final-s3-v1"),
            ("producer", "other"),
            ("equal", False),
        ]:
            write_json(path, {**original, key: value})
            with self.assertRaises(ValueError):
                summarize(self.inputs, self.shards)


if __name__ == "__main__":
    unittest.main()
