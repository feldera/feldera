import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from summarize_optimizer_coverage import summarize


class OptimizerCoverageTests(unittest.TestCase):
    def test_rejects_counts_and_engine_modes_that_cannot_describe_the_input(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            population, observations = root / "population", root / "observations"
            identity = {"id": "case", "input_sha256": "sql"}
            population.write_text(json.dumps({**identity, "options": {"ioOptions": {"gen2": True}}}))
            for gen2, calls, fallbacks in ((False, 1, 0), (True, 1, 2), (True, True, 0)):
                with self.subTest(gen2=gen2, calls=calls, fallbacks=fallbacks):
                    observations.write_text(json.dumps({**identity, "gen2": gen2, "transfers": {},
                        "requests": {"closure": {"calls": calls, "fallbacks": fallbacks}}}))
                    with self.assertRaises(ValueError):
                        summarize(population, observations)

    def test_sql_and_checkpoint_inputs_prove_the_same_case_identity(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = {"id": "case", "input_sha256": "sql", "options": {"ioOptions": {"gen2": False}}}
            line = json.dumps(source).encode()
            population, observations = root / "population", root / "observations"
            population.write_bytes(line + b"\r\n")
            record = {"id": "case", "input_sha256": "sql", "gen2": False,
                      "requests": {"closure": {"calls": 2, "fallbacks": 1}},
                      "transfers": {"map": {"apply": 1}}}
            observations.write_text(json.dumps(record))
            result = summarize(population, observations)
            self.assertEqual(result["requests"]["closure"], {"calls": 2, "fallbacks": 1})
            self.assertEqual(result["programs_with_fallbacks"], {"closure": ["case"]})
            path = root / (hashlib.sha256(line).hexdigest() + ".json")
            path.write_bytes(b"checkpoint")
            record["s2_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
            observations.write_text(json.dumps(record))
            self.assertEqual(summarize(population, observations, root)["programs"], 1)
            path.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "Wrong S2"):
                summarize(population, observations, root)
            with self.assertRaisesRegex(ValueError, "requires its checkpoint directory"):
                summarize(population, observations)
            observations.write_bytes(b"")
            with self.assertRaises(ValueError):
                summarize(population, observations)


if __name__ == "__main__":
    unittest.main()
