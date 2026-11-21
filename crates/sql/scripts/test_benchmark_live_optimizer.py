import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from benchmark_live_optimizer import artifact_json, compare
from run_determinism import file_digest


class MeasuredArtifacts(unittest.TestCase):
    def test_public_json_parity_preserves_types_and_precision(self):
        self.assertEqual(artifact_json('{"rows": [1, false], "n": 2}'),
                         artifact_json('{"n":2,"rows":[1,false]}'))
        for left, right in [('1', 'true'), ('0', 'false'), ('3', '"3"'),
                            ('0.10000000000000001', '0.10000000000000002')]:
            with self.subTest(left=left, right=right):
                self.assertNotEqual(artifact_json(left), artifact_json(right))
        for invalid in ('{"a": {"type": 1, "type": 2}}', 'NaN'):
            with self.assertRaises(ValueError):
                artifact_json(invalid)

    def test_manifest_requires_the_published_bytes_and_the_complete_file_set(self):
        with TemporaryDirectory() as root:
            direct, hybrid = (Path(root) / name for name in ("direct", "hybrid"))
            direct.mkdir()
            hybrid.mkdir()
            for directory in (direct, hybrid):
                (directory / "circuit.rs").write_bytes(b"compiled program")
                (directory / "dataflow.json").write_text("{}")
                (directory / "schema.json").write_text("{}")
            digests = {path.name: file_digest(path) for path in hybrid.iterdir()}
            receipt = {"compiler": "hybrid", "attempts": 1, "status": "ok",
                       "rust_optimizer_passes": True, "artifacts": digests, "messages": []}
            (hybrid / "result.json").write_text(json.dumps(receipt))
            self.assertEqual(compare(direct, hybrid), digests)
            extra = hybrid / "partial.rs"
            extra.touch()
            with self.assertRaisesRegex(ValueError, "file sets differ"):
                compare(direct, hybrid)
            extra.unlink()
            (hybrid / "circuit.rs").write_bytes(b"stale program")
            with self.assertRaisesRegex(ValueError, "Incorrect output digest"):
                compare(direct, hybrid)
            (hybrid / "circuit.rs").write_bytes(b"compiled program")
            for directory in (direct, hybrid):
                (directory / "schema.json").unlink()
            del digests["schema.json"]
            (hybrid / "result.json").write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "file sets differ"):
                compare(direct, hybrid)
