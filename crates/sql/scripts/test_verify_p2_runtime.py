import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from run_p2_runtime import case_name, file_hash
from verify_p2_runtime import verify


class RuntimeReceiptTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.population = self.root / "population.jsonl"
        self.key = "Example#count/0"
        program = {"program": "SELECT 1", "submissions": ["SELECT 1"]}
        self.population.write_text(json.dumps({"id": self.key, **program}) + "\n")
        method, _ = self.key.split("/")
        directory = self.root / hashlib.sha256(method.encode()).hexdigest()
        directory.mkdir()
        source = directory / "case-0000.json"
        source.write_text(json.dumps({**program, "source_test": method, "case_index": 0,
                                      "status": "exported", "engine_mode": "classic",
                                      "options": {"ioOptions": {"emitHandles": True}},
                                      "commands": [{}], "expected_runtime_error": None}))
        self.case_digest = hashlib.sha256(bytes.fromhex(file_hash(source))).hexdigest()
        (self.root / "compile-only.json").write_text("[]")
        (self.root / "environment.json").write_text("{}")
        (self.root / "complete.json").write_text(json.dumps({
            "population_sha256": file_hash(self.population), "batches": 1,
            "batch_size": 32, "executed": 1, "compile_only": 0,
        }))

    def check_cargo_result(self, result):
        name = case_name(0, self.key)
        log = self.root / "batch-0000.0.log"
        log.write_text(f"{name}\tExample#count#0\ntest {result} ... ok\n")
        (self.root / "batch-0000.json").write_text(json.dumps({
            "ids": [self.key], "log": log.name, "log_sha256": file_hash(log),
            "cases_sha256": self.case_digest,
        }))
        return verify(self.population, self.root, self.root)

    def test_matching_identity(self):
        self.assertEqual(self.check_cargo_result(case_name(0, self.key))["executed"], 1)

    def test_stale_executable_with_same_number_of_passing_tests(self):
        with self.assertRaisesRegex(ValueError, "Missing named Rust test"):
            self.check_cargo_result(case_name(0, "Example#different/0"))

    def test_unexpected_should_panic(self):
        with self.assertRaisesRegex(ValueError, "Runtime error expectation disagrees"):
            self.check_cargo_result(case_name(0, self.key) + " - should panic")


if __name__ == "__main__":
    unittest.main()
