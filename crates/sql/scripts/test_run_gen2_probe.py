import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from run_gen2_probe import compiler_sources, run, verify_engine
from summarize_gen2 import digest


class Gen2RunnerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.compiler = self.root / "compiler"
        (self.compiler / "SQL-compiler").mkdir(parents=True)
        (self.compiler / "SQL-compiler/pom.xml").write_text("<project/>")
        self.engine = self.root / "engine"
        self.engine.mkdir()
        for name in ("pipeline", "ir-expander"):
            binary = self.engine / name
            binary.write_text("engine fixture")
            binary.chmod(0o700)
        self.provenance = self.root / "engine.json"
        self.provenance.write_text(
            json.dumps(
                {
                    "forge_commit": "a" * 40,
                    "forge_platform_commit": "b" * 40,
                    "binaries_sha256": {
                        name: digest(self.engine / name)
                        for name in ("pipeline", "ir-expander")
                    },
                }
            )
        )

    def execute_fixture(self, command, *, env, stdout, **kwargs):
        baseline = "GEN2_PATH" not in env
        directory = Path(
            next(
                arg.split("=", 1)[1]
                for arg in command
                if arg.startswith("-Dfeldera.test.reports=")
            )
        )
        directory.mkdir()
        failure = "" if baseline else '<failure message="runtime differs"/>'
        failures = int(not baseline)
        (directory / "TEST-example.xml").write_text(
            f'<testsuite tests="2" failures="{failures}" errors="0" skipped="0">'
            '<testcase classname="C" name="pass"/>'
            f'<testcase classname="C" name="fail">{failure}</testcase></testsuite>'
        )
        stdout.write(f"Tests run: 2, Failures: {failures}, Errors: 0, Skipped: 0\n")
        return SimpleNamespace(returncode=failures)

    def test_complete_failed_experiment_keeps_all_verdicts(self):
        output = self.root / "result"
        with patch("run_gen2_probe.subprocess.run", side_effect=self.execute_fixture):
            result = run(output, self.compiler, self.engine, self.provenance, "c" * 40)
        self.assertEqual(result["pass_share_all_tests"], 0.5)
        self.assertFalse(result["threshold_met_all_tests"])
        self.assertEqual(result["completion"]["returncode"], 1)
        self.assertFalse(result["baseline_runtime_executed"])
        self.assertEqual(result["baseline_comparison"]["removed_tests"], [])

    def test_lost_tests_cannot_improve_the_pass_share(self):
        def incomplete(*args, **kwargs):
            result = self.execute_fixture(*args, **kwargs)
            if "GEN2_PATH" in kwargs["env"]:
                report = self.root / "result/reports/TEST-example.xml"
                report.write_text(
                    report.read_text().replace('name="fail"', 'name="other"')
                )
            return result

        with (
            patch("run_gen2_probe.subprocess.run", side_effect=incomplete),
            self.assertRaisesRegex(ValueError, "test identities differ"),
        ):
            run(
                self.root / "result",
                self.compiler,
                self.engine,
                self.provenance,
                "c" * 40,
            )
        self.assertFalse((self.root / "result/summary.json").exists())

    def test_changed_binary_cannot_reuse_provenance(self):
        (self.engine / "pipeline").write_text("different engine")
        with self.assertRaisesRegex(ValueError, "differs from its provenance"):
            verify_engine(self.engine, json.loads(self.provenance.read_text()))

    def test_source_manifest_tracks_resources_without_generated_build_files(self):
        names = (
            "SQL-compiler/src/main/codegen/config.fmpp",
            "SQL-compiler/src/test/resources/input.csv",
            "temp/target-check/debug/build/dependency/doc/library.xml",
            "SQL-compiler/target/generated-sources/Parser.java",
        )
        for name in names:
            path = self.compiler / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("initial")
        before = compiler_sources(self.compiler)
        self.assertEqual(set(before), {"SQL-compiler/pom.xml", *names[:2]})
        for name in names[2:]:
            (self.compiler / name).write_text("generated output changed")
        self.assertEqual(compiler_sources(self.compiler), before)
        for name in names[:2]:
            (self.compiler / name).write_text("source changed")
            self.assertNotEqual(compiler_sources(self.compiler)[name], before[name])

    def test_incomplete_baseline_cannot_start_runtime_measurement(self):
        def incomplete(*args, **kwargs):
            result = self.execute_fixture(*args, **kwargs)
            kwargs["stdout"].write("Tests run: 3, Failures: 0, Errors: 0, Skipped: 0\n")
            return result

        with (
            patch("run_gen2_probe.subprocess.run", side_effect=incomplete) as execute,
            self.assertRaisesRegex(ValueError, "Maven final totals disagree"),
        ):
            run(
                self.root / "result",
                self.compiler,
                self.engine,
                self.provenance,
                "c" * 40,
            )
        self.assertEqual(execute.call_count, 1)


if __name__ == "__main__":
    unittest.main()
