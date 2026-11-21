import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from benchmark_live_optimizer import compare
from run_determinism import file_digest


class MeasuredArtifacts(unittest.TestCase):
    def test_manifest_requires_the_published_bytes_and_the_complete_file_set(self):
        with TemporaryDirectory() as root:
            direct, hybrid = (Path(root) / name for name in ("direct", "hybrid"))
            direct.mkdir()
            hybrid.mkdir()
            for directory in (direct, hybrid):
                (directory / "circuit.rs").write_bytes(b"compiled program")
            digests = {"circuit.rs": file_digest(hybrid / "circuit.rs")}
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
