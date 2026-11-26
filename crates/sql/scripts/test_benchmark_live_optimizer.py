import json
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from benchmark_live_optimizer import artifact_json, compare, direct_java, jvm_options, summarize, trial
from run_determinism import file_digest


class DirectProfiles(unittest.TestCase):
    def test_matched_jvm_tracks_the_driver_options(self):
        compiler = Path("/package/sql2dbsp")
        hybrid = (Path(__file__).resolve().parents[1] / "sql2dbsp/src/hybrid.rs").read_text()
        jvm = hybrid.split("fn jvm(", 1)[1].split("Ok(command)", 1)[0]
        # The driver builds the library path from its own executable.
        prefix = re.search(r'OsString::from\("([^"]+)"\)', jvm)
        arguments = jvm.replace(prefix[0], "")
        options = re.findall(r'"(-[^"]+)"|\.arg\((library_path)\)', arguments)
        expected = [literal or prefix[1] + str(compiler.parent) for literal, _ in options]
        self.assertEqual(jvm_options(compiler) + ["-cp"], expected)

    def test_production_uses_the_packaged_jar_and_both_ratios_name_the_profile(self):
        self.assertEqual(direct_java("production", Path("/package/worker"), "compiler.jar"),
                         ["java", "-jar", "compiler.jar"])
        matched = direct_java("matched", Path("/package/worker"), "compiler.jar")
        self.assertIn("-XX:TieredStopAtLevel=1", matched)
        self.assertIn("-XX:ActiveProcessorCount=1", matched)
        self.assertEqual(matched[-3:], ["-cp", "compiler.jar", "org.dbsp.sqlCompiler.CompilerMain"])
        measurements = {p: {"hybrid": [6] * 20, "direct": [d] * 20}
                        for p, d in [("matched", 2), ("production", 3)]}
        result = summarize(measurements)
        self.assertEqual(result["matched"]["p95_ratio"], 3)
        self.assertEqual(result["production"]["p95_ratio"], 2)

    def test_demo_validator_can_call_trial_with_the_default_profile(self):
        args = SimpleNamespace(classpath="compiler.jar", engine="classic", compiler=Path("/package/worker"),
                               producer="test", workspace_runtime=None)
        with TemporaryDirectory() as scratch, patch("benchmark_live_optimizer.subprocess.run") as run:
            run.return_value = SimpleNamespace(returncode=1, stderr=b"compiler diagnostic")
            trial(args, Path("example.sql"), ["direct", "hybrid"], Path(scratch))
            command = run.call_args_list[0].args[0]
            self.assertEqual(command[:command.index("-i")], direct_java("matched", args.compiler, args.classpath))


class MeasuredArtifacts(unittest.TestCase):
    def test_workspace_checks_nested_sources_and_gen2_uses_the_circuit_comparator(self):
        for artifact in ("rust/Cargo.toml", "circuit.json"):
            with self.subTest(artifact=artifact), TemporaryDirectory() as root:
                direct, hybrid = (Path(root) / name for name in ("direct", "hybrid"))
                files = {artifact: b"{}", "schema.json": b"{}", "dataflow.json": b"{}"}
                if artifact == "rust/Cargo.toml":
                    files.update({"rust/crates/baseline/src/lib.rs": b"mod stubs;",
                                  "rust/crates/baseline/src/stubs.rs": b""})
                for directory in (direct, hybrid):
                    for name, contents in files.items():
                        path = directory / name
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(contents)
                digests = {name: file_digest(hybrid / name) for name in files}
                (hybrid / "result.json").write_text(json.dumps({
                    "compiler": "hybrid", "attempts": 1, "status": "ok",
                    "rust_optimizer_passes": True, "artifacts": digests, "messages": [],
                }))
                if artifact == "rust/Cargo.toml":
                    self.assertEqual(compare(direct, hybrid, artifact), digests)
                    (direct / "rust/crates/baseline/src/lib.rs").write_text("mod other;")
                    with self.assertRaisesRegex(ValueError, "lib.rs differs"):
                        compare(direct, hybrid, artifact)
                else:
                    (direct / artifact).write_text('{"id": 1}')
                    with patch("benchmark_live_optimizer.subprocess.run") as run:
                        run.return_value.returncode = 0
                        self.assertEqual(compare(direct, hybrid, artifact, "sql-difftest"), digests)
                        self.assertEqual(run.call_args.args[0], ["sql-difftest", direct / artifact, hybrid / artifact])
                        run.return_value.returncode = 1
                        run.return_value.stderr = "different graphs"
                        (direct / "dataflow.json").write_text('{"mir": {}}')
                        with self.assertRaises(ValueError) as error:
                            compare(direct, hybrid, artifact, "sql-difftest")
                        self.assertEqual(str(error.exception).splitlines(), [
                            "Gen-2 circuit comparison failed: different graphs", "dataflow.json differs",
                        ])

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
