import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from compare_parsers import VARIANTS, compare, error_position, java_line_advance, view_facts


class ParserComparisonTests(unittest.TestCase):
    def test_local_view_semantics_are_not_inferred_from_identical_sql_text(self):
        java = {"statements": [{"node_class": "SqlCreateView", "view_kind": "LOCAL",
                                "replace": True, "sql": "CREATE LOCAL VIEW v AS SELECT 1"}]}
        rust = {"statements": ["CREATE LOCAL VIEW v AS SELECT 1"],
                "views": [{"statement_index": 0, "kind": "STANDARD", "replace": True}]}
        actual, expected = view_facts(rust, java)
        self.assertNotEqual(actual, expected)
        rust["views"][0]["kind"] = "LOCAL"
        self.assertEqual(*view_facts(rust, java))
        rust["views"][0]["replace"] = False
        self.assertNotEqual(*view_facts(rust, java))

    def test_view_observation_requires_typed_fields_and_valid_statement_identity(self):
        java = {"statements": [{"node_class": "SqlCreateView", "sql": "CREATE VIEW v AS SELECT 1"}]}
        with self.assertRaisesRegex(ValueError, "Java view AST fields"):
            view_facts({"statements": []}, java)
        java["statements"][0].update(view_kind="STANDARD", replace=False)
        for index in [-1, 1, True]:
            with self.assertRaisesRegex(ValueError, "Rust view AST fields"):
                view_facts({"statements": ["SELECT 1"],
                            "views": [{"statement_index": index, "kind": "STANDARD", "replace": False}]}, java)

    def test_java_line_accounting_discards_trailing_empty_lines(self):
        for sql, lines in [
            ("", 1),
            ("\n", 0),
            ("\n\n", 0),
            ("SELECT 1\n\n", 1),
            ("SELECT\n1", 2),
            ("\nSELECT 1\n", 2),
        ]:
            self.assertEqual(lines, java_line_advance(sql))
        self.assertEqual(
            "equal",
            error_position(
                {"verdict": "rejected", "error": "bad input at Line: 1, Column: 8"},
                {"line": 3, "column": 8},
                line_offset=2,
            )["outcome"],
        )

    def test_error_positions_keep_missing_and_shifted_starts_visible(self):
        java = {"verdict": "rejected", "line": 2, "column": 7}
        for error, outcome in [
            ("bad input at Line: 2, Column: 7", "equal"),
            ("bad input at Line: 2, Column: 8", "different"),
            ("unsupported expression", "rust_position_missing"),
            ("bad token 'Line: 2, Column: 7'", "rust_position_missing"),
        ]:
            self.assertEqual(
                outcome,
                error_position({"verdict": "rejected", "error": error}, java)[
                    "outcome"
                ],
            )
        self.assertEqual(
            "rust_accepts", error_position({"verdict": "accepted"}, java)["outcome"]
        )
        self.assertEqual(
            "java_position_missing",
            error_position({"verdict": "rejected", "error": "bad input"}, {})[
                "outcome"
            ],
        )
        with self.assertRaisesRegex(ValueError, "Malformed Java parser position"):
            error_position({"verdict": "accepted"}, {"line": "2", "column": 7})

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.rust = Path(self.temporary.name) / "rust"
        self.java = Path(self.temporary.name) / "java"
        self.rust.mkdir()
        self.java.mkdir()
        source = {"sql": "SELECT 1", "many": True, "batch": 0}
        counts = {
            "accepted_cases": 1,
            "rejected_cases": 0,
            "accepted_submissions": 1,
            "rejected_submissions": 0,
        }
        measurements = [
            {
                "version": v,
                "dialect": d,
                "verdict": "accepted",
                "statements": ["SELECT 1"],
            }
            for v, d in sorted(VARIANTS)
        ]
        self.rust_case = {
            "id": "case",
            "accepted": {f"{v}/{d}": True for v, d in VARIANTS},
            "submissions": [{"source": source, "measurements": measurements}],
        }
        self.java_case = {
            "id": "case",
            "accepted": True,
            "submissions": [
                {"source": source, "verdict": "accepted", "statements": [{}]}
            ],
        }
        common = {
            "schema_version": 1,
            "complete": True,
            "cases": 1,
            "input_sha256": "same",
        }
        self.rust_manifest = {
            **common,
            "counts": {f"{v}/{d}": counts.copy() for v, d in VARIANTS},
        }
        self.java_manifest = {**common, **counts, "stage": "raw_parser"}

    def run_comparison(self):
        for directory, case, manifest in (
            (self.rust, self.rust_case, self.rust_manifest),
            (self.java, self.java_case, self.java_manifest),
        ):
            (directory / "cases.jsonl").write_text(json.dumps(case) + "\n")
            (directory / "complete.json").write_text(json.dumps(manifest))
        return compare(self.rust, self.java)

    def test_equal_acceptance_does_not_claim_semantic_equivalence(self):
        result = self.run_comparison()
        self.assertFalse(result["semantic_equivalence_checked"])
        self.assertEqual([], result["disagreement_groups"])
        self.assertTrue(
            all(count["both_accept"] == 1 for count in result["counts"].values())
        )

    def test_rejection_is_counted_and_retained(self):
        measurement = self.rust_case["submissions"][0]["measurements"][0]
        measurement.update(
            verdict="rejected", error="Expected expression, Line: 2, Column: 3"
        )
        key = measurement["version"] + "/" + measurement["dialect"]
        self.rust_case["accepted"][key] = False
        self.rust_manifest["counts"][key] = {
            "accepted_cases": 0,
            "rejected_cases": 1,
            "accepted_submissions": 0,
            "rejected_submissions": 1,
        }
        result = self.run_comparison()
        self.assertEqual(1, result["counts"][key]["java_only"])
        self.assertEqual("java_only", result["disagreement_groups"][0]["outcome"])
        self.assertEqual(
            [{"id": "case", "submission": 0}],
            result["disagreement_groups"][0]["examples"],
        )

    def test_missing_variant_fails(self):
        self.rust_case["submissions"][0]["measurements"].pop()
        with self.assertRaisesRegex(ValueError, "missing parser variant"):
            self.run_comparison()

    def test_unparse_failure_retains_acceptance_but_cannot_claim_rendering_completion(
        self,
    ):
        failure = {
            "type": "java.lang.ClassCastException",
            "message": "table hint literal chain",
            "stack": ["org.apache.calcite.sql.SqlHint.getOptionList(SqlHint.java:137)"],
        }
        self.java_case["submissions"][0]["statements"] = [{"unparse_failure": failure}]
        self.java_manifest.update(unparse_failures=1, rendering_complete=False)
        report = self.run_comparison()
        self.assertEqual(1, report["counts"]["0.64.0/feldera_prototype"]["both_accept"])
        self.assertFalse(report["java_rendering"]["complete"])
        self.assertEqual(failure, report["java_rendering"]["failures"][0]["failure"])
        self.java_manifest["rendering_complete"] = True
        with self.assertRaisesRegex(ValueError, "rendering completion"):
            self.run_comparison()
        self.java_manifest["rendering_complete"] = False
        self.java_manifest["unparse_failures"] = 0
        with self.assertRaisesRegex(ValueError, "unparse failures"):
            self.run_comparison()

    def test_forged_completion_fails(self):
        self.java_manifest["accepted_cases"] = 2
        with self.assertRaisesRegex(ValueError, "counters disagree"):
            self.run_comparison()

    def test_overacceptance_gate_saves_failure_and_only_gates_selected_variant(self):
        self.java_case["accepted"] = False
        self.java_case["submissions"][0].update(verdict="rejected", error="invalid SQL")
        self.java_manifest.update(
            accepted_cases=0,
            accepted_submissions=0,
            rejected_cases=1,
            rejected_submissions=1,
        )
        self.run_comparison()
        variant = "0.64.0/feldera_prototype"
        command = [
            sys.executable,
            str(Path(__file__).with_name("compare_parsers.py")),
            str(self.rust),
            str(self.java),
        ]
        output = self.rust.parent / "failed.json"
        result = subprocess.run(
            [*command, str(output), "--reject-overacceptance", variant],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(0, result.returncode)
        self.assertIn("evidence saved", result.stderr)
        self.assertEqual(
            1, json.loads(output.read_text())["counts"][variant]["rust_only"]
        )

        for measurement in self.rust_case["submissions"][0]["measurements"]:
            if f"{measurement['version']}/{measurement['dialect']}" == variant:
                measurement.update(verdict="rejected", error="invalid SQL")
        self.rust_case["accepted"][variant] = False
        self.rust_manifest["counts"][variant].update(
            accepted_cases=0,
            accepted_submissions=0,
            rejected_cases=1,
            rejected_submissions=1,
        )
        self.run_comparison()
        result = subprocess.run(
            [
                *command,
                str(self.rust.parent / "passed.json"),
                "--reject-overacceptance",
                variant,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)


if __name__ == "__main__":
    unittest.main()
