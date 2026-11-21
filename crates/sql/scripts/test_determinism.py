import copy
import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from determinism import compare, prepare


class DeterminismTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.left = Path(self.temporary.name) / "left"
        self.right = Path(self.temporary.name) / "right"
        self.manifest = {
            "schema_version": 1,
            "cases": 1,
            "capture": True,
            "producer": "compiler",
            "runtime": "runtime",
            "input_sha256": "input",
        }
        self.case = {
            "id": "test/0",
            "input_sha256": "case",
            "schema": {},
            "diagnostics": [],
            "exported_schema_equal": True,
            "exported_diagnostics_equal": True,
            "rust": {"circuit.rs": "rust-bytes"},
            "hash_inputs": {"inner": {"sha256": "hash"}},
            "observations": {
                stage: {"count": count, "bytes": 10, "sha256": stage}
                for stage, count in (("S2", 1), ("S3", 1), ("S4", 5))
            },
        }
        for directory in (self.left, self.right):
            directory.mkdir()
            self.write(directory, self.manifest, [self.case])

    @staticmethod
    def write(directory, manifest, cases):
        (directory / "complete.json").write_text(json.dumps(manifest))
        (directory / "cases.jsonl").write_text(
            "".join(json.dumps(case) + "\n" for case in cases)
        )

    def test_seam_artifacts_keep_raw_metrics_and_enforce_semantic_fingerprints(self):
        manifest = {**self.manifest, "artifact_contract": "seam-v1"}
        case = copy.deepcopy(self.case)
        case.pop("rust")
        case["gen2"] = {
            "circuit.json": "canonical graph",
            "dataflow.json": "parsed plan",
        }
        case["gen2_bytes"] = {"circuit.json": "original allocation ids"}
        self.write(self.left, manifest, [case])
        changed = copy.deepcopy(case)
        changed["gen2_bytes"]["circuit.json"] = "decoded allocation ids"
        self.write(self.right, manifest, [changed])
        result = compare(self.left, self.right)
        self.assertTrue(result["equal"])
        self.assertEqual(result["artifact_byte_differences"], [case["id"]])
        for field in ("gen2", "hash_inputs", "observations"):
            invalid = copy.deepcopy(changed)
            if field == "gen2":
                invalid[field]["circuit.json"] = "changed sharing"
            elif field == "hash_inputs":
                invalid[field]["inner"]["sha256"] = "changed preimage"
            else:
                invalid[field]["S3"]["sha256"] = "changed graph"
            self.write(self.right, manifest, [invalid])
            self.assertFalse(compare(self.left, self.right)["equal"], field)
        self.write(self.right, self.manifest, [self.case])
        with self.assertRaisesRegex(ValueError, "contracts"):
            compare(self.left, self.right)

    def test_detects_changes_and_checks_capture_transparency(self):
        self.assertTrue(compare(self.left, self.right)["equal"])
        changed = copy.deepcopy(self.case)
        changed["observations"]["S4"]["sha256"] = "different"
        self.write(self.right, self.manifest, [changed])
        self.assertEqual(
            compare(self.left, self.right)["differences"][0]["fields"], ["observations"]
        )
        baseline = {**self.manifest, "capture": False}
        changed.pop("observations")
        self.write(self.right, baseline, [changed])
        self.assertTrue(compare(self.left, self.right)["equal"])
        changed["rust"]["circuit.rs"] = "changed Rust"
        self.write(self.right, baseline, [changed])
        self.assertFalse(compare(self.left, self.right)["equal"])

    def test_classic_dataflow_is_required_and_compared_in_the_new_contract(self):
        manifest = {**self.manifest, "dataflow_contract": "all-engines-v1"}
        case = copy.deepcopy(self.case)
        case["rust"]["dataflow.json"] = "annotation source range"
        for directory in (self.left, self.right):
            self.write(directory, manifest, [case])
        self.assertTrue(compare(self.left, self.right)["equal"])
        changed = copy.deepcopy(case)
        changed["rust"]["dataflow.json"] = "view source range"
        self.write(self.right, manifest, [changed])
        self.assertFalse(compare(self.left, self.right)["equal"])
        for directory in (self.left, self.right):
            self.write(directory, manifest, [self.case])
        with self.assertRaisesRegex(ValueError, "Missing dataflow"):
            compare(self.left, self.right)
        self.write(self.right, self.manifest, [self.case])
        with self.assertRaisesRegex(ValueError, "dataflow comparison contracts"):
            compare(self.left, self.right)

    def test_final_s3_contract_cannot_hide_missing_prefix_stages(self):
        manifest = {**self.manifest, "observation_contract": "final-s3-v1"}
        case = copy.deepcopy(self.case)
        case["observations"] = {"S3": case["observations"]["S3"]}
        for directory in (self.left, self.right):
            self.write(directory, manifest, [case])
        self.assertTrue(compare(self.left, self.right)["equal"])
        changed = copy.deepcopy(case)
        changed["observations"]["S3"]["sha256"] = "changed sharing"
        self.write(self.right, manifest, [changed])
        self.assertFalse(compare(self.left, self.right)["equal"])
        self.write(self.right, self.manifest, [case])
        with self.assertRaisesRegex(ValueError, "observation comparison contracts"):
            compare(self.left, self.right)
        for directory in (self.left, self.right):
            self.write(directory, self.manifest, [case])
        with self.assertRaisesRegex(ValueError, "Missing observation stage"):
            compare(self.left, self.right)
        for invalid in ({}, {"S3": {"count": 2}}):
            case["observations"] = invalid
            for directory in (self.left, self.right):
                self.write(directory, manifest, [case])
            with self.assertRaises(ValueError):
                compare(self.left, self.right)

    def test_identical_failures_never_count_as_agreement(self):
        failed = {
            "id": "failed/0",
            "input_sha256": "failed-input",
            "failure": {"class": "CompilerError", "message": "missing binding"},
        }
        manifest = {**self.manifest, "cases": 2, "failed_cases": 1}
        for directory in (self.left, self.right):
            self.write(directory, manifest, [failed, self.case])
        result = compare(self.left, self.right)
        self.assertFalse(result["equal"])
        self.assertEqual(result["cases"], 2)
        self.assertEqual(
            result["failed_cases"], {"expected": ["failed/0"], "actual": ["failed/0"]}
        )
        self.assertEqual(
            result["differences"], [{"id": "failed/0", "fields": ["failure"]}]
        )

    def test_failed_case_counts_and_records_are_validated(self):
        failed = {
            "id": self.case["id"],
            "input_sha256": self.case["input_sha256"],
            "failure": {"class": "CompilerError", "message": "missing binding"},
        }
        self.write(self.right, {**self.manifest, "failed_cases": 1}, [failed])
        result = compare(self.left, self.right)
        self.assertFalse(result["equal"])
        self.assertEqual(result["failed_cases"]["actual"], [self.case["id"]])
        self.write(self.right, self.manifest, [failed])
        with self.assertRaisesRegex(ValueError, "Failed-case count"):
            compare(self.left, self.right)
        failed["failure"]["message"] = ""
        self.write(self.right, {**self.manifest, "failed_cases": 1}, [failed])
        with self.assertRaisesRegex(ValueError, "Invalid failed-case record"):
            compare(self.left, self.right)

    def test_rejects_truncation_missing_stages_and_incompatible_inputs(self):
        self.write(self.right, self.manifest, [])
        with self.assertRaises(ValueError):
            compare(self.left, self.right)
        changed = copy.deepcopy(self.case)
        changed["observations"].pop("S3")
        self.write(self.right, self.manifest, [changed])
        with self.assertRaises(ValueError):
            compare(self.left, self.right)
        self.write(self.right, {**self.manifest, "input_sha256": "other"}, [self.case])
        with self.assertRaises(ValueError):
            compare(self.left, self.right)

    def test_gen2_artifacts_are_compared_without_rust_output(self):
        record = copy.deepcopy(self.case)
        record.pop("rust")
        record["gen2"] = {"circuit.json": "ir", "dataflow.json": "dataflow"}
        for directory in (self.left, self.right):
            self.write(directory, self.manifest, [record])
        self.assertTrue(compare(self.left, self.right)["equal"])
        record["gen2"]["dataflow.json"] = "changed"
        self.write(self.right, self.manifest, [record])
        self.assertEqual(
            compare(self.left, self.right)["differences"][0]["fields"], ["gen2"]
        )
        record["rust"] = {"circuit.rs": "unexpected Rust"}
        self.write(self.right, self.manifest, [record])
        with self.assertRaises(ValueError):
            compare(self.left, self.right)

    def test_duplicate_cases_and_forged_counts_do_not_pass(self):
        for directory in (self.left, self.right):
            self.write(directory, self.manifest, [self.case, self.case])
        with self.assertRaises(ValueError):
            compare(self.left, self.right)
        for directory in (self.left, self.right):
            self.write(directory, {**self.manifest, "cases": 2}, [self.case])
        with self.assertRaises(ValueError):
            compare(self.left, self.right)

    def test_compressed_records_match_plain_records_and_reject_truncation(self):
        compressed = self.right / "cases.jsonl.gz"
        compressed.write_bytes(gzip.compress((self.right / "cases.jsonl").read_bytes()))
        manifest = {**self.manifest, "records_file": compressed.name}
        (self.right / "complete.json").write_text(json.dumps(manifest))
        self.assertTrue(compare(self.left, self.right)["equal"])
        compressed.write_bytes(compressed.read_bytes()[:-8])
        with self.assertRaises(EOFError):
            compare(self.left, self.right)


class PreparationTests(unittest.TestCase):
    def test_skips_require_an_explicit_separate_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            corpus = root / "corpus" / "test"
            corpus.mkdir(parents=True)
            exported = {
                "source_test": "test",
                "case_index": 0,
                "status": "exported",
                "engine_mode": "classic",
                "program": "SELECT 1",
                "submissions": [{"sql": "SELECT 1", "many": False, "batch": 0}],
                "options": {},
                "schema": {},
                "diagnostics": [],
            }
            (corpus / "case-0.json").write_text(json.dumps(exported))
            skipped = {
                **exported,
                "case_index": 1,
                "status": "skipped",
                "reason": "test assertion failed",
            }
            (corpus / "case-1.json").write_text(json.dumps(skipped))
            # The Rust validator owns structural validation; this tests the
            # selection policy after it succeeds, not a second validator.
            with patch("determinism.subprocess.run") as validator:
                with self.assertRaisesRegex(ValueError, "Cannot replay skipped"):
                    prepare(root / "corpus", root / "strict.jsonl", "validator")
                report = prepare(
                    root / "corpus",
                    root / "selected.jsonl",
                    "validator",
                    root / "skips.json",
                )
                self.assertEqual(2, validator.call_count)
            self.assertEqual(1, report["cases"])
            self.assertEqual(1, report["skipped_registered_cases"])
            exclusions = json.loads((root / "skips.json").read_text())
            self.assertFalse(exclusions["approved_dispositions"])
            self.assertEqual("test/1", exclusions["skipped"][0]["id"])
            self.assertEqual(
                "test assertion failed", exclusions["skipped"][0]["reason"]
            )


if __name__ == "__main__":
    unittest.main()
