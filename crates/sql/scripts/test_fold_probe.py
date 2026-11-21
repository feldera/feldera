import copy
import unittest

from fold_cases import runtime_cases, sql_cases
from fold_census import census, normalized, reference
from run_fold_probe import (
    compare_compilations,
    compare_runtime,
    require_semantics_profile,
)


class FoldProbeTests(unittest.TestCase):
    def test_pre_and_post_executor_fixtures_exercise_their_boundaries(self):
        cases = list(sql_cases())
        numeric = [c for c in cases if c["id"].startswith("focused/numeric_interval_tumble/")]
        self.assertTrue(any("CAST(0.9 AS INTERVAL DAY)" in c["program"] for c in numeric))
        chains = [c for c in cases if c["id"].startswith("focused/regex_literal_chain_")]
        self.assertTrue(any("'a'\n'(?=b)'" in c["program"] for c in chains))
        warnings = [c for c in cases if c["id"].startswith("focused/post_simplify_warning/")]
        self.assertTrue(warnings)
        for case in warnings:
            self.assertIn("SELECT x, ts, ", case["program"])
            self.assertEqual("SET FELDERA_IGNORE_WARNING_DIVISION_BY_ZERO = true;" in case["program"],
                             case["id"].endswith("suppress=True"))

    def test_literal_consumer_cases_include_computed_and_invalid_selectors(self):
        cases = [c for c in sql_cases() if any(
            c["id"].startswith("focused/" + site + "/")
            for site in ("row_selector", "keyword_selector", "date_trunc_selector"))]
        self.assertTrue(cases)
        for case in cases:
            self.assertIn("CREATE VIEW v AS SELECT x, ts, ", case["program"])
        programs = "\n".join(case["program"] for case in cases)
        self.assertIn("[CONCAT('a', '')]", programs)
        self.assertIn("[1 + 0]", programs)
        self.assertIn("[CAST(NULL AS INT)]", programs)
        self.assertIn("[x]", programs)
        self.assertIn("DATE_TRUNC(CONCAT('d', 'ay'), ts)", programs)
        self.assertIn("TIMESTAMPDIFF(CONCAT('D', 'AY'), ts, ts)", programs)

    def test_regex_warning_sites_keep_dynamic_input_and_control_the_actual_warning(self):
        cases = [c for c in sql_cases() if c["id"].startswith("focused/regex_")]
        self.assertTrue(cases)
        shapes = {}
        for case in cases:
            identity, promote, suppress = case["id"].rsplit("/", 2)
            shapes.setdefault(identity, set()).add((promote, suppress))
            program = case["program"]
            self.assertIn("CREATE TABLE t(", program)
            # Keep both columns live even when the regex result becomes NULL;
            # unused-column warnings would mask warning-promotion verdicts.
            self.assertIn("CREATE VIEW v AS SELECT x, ts, ", program)
            self.assertIn(" AS y FROM t;", program)
            self.assertTrue(
                "CAST(x AS VARCHAR) RLIKE " in program
                or "REGEXP_REPLACE(CAST(x AS VARCHAR), " in program
            )
            self.assertEqual(
                "SET FELDERA_IGNORE_WARNING_UNSUPPORTED_REGULAR_EXPRESSION_FEATURE = true;"
                in program,
                suppress == "suppress=True",
            )
            self.assertEqual(
                "SET FELDERA_WARNINGS_ARE_ERRORS = true;" in program,
                promote == "promote=True",
            )
        for policies in shapes.values():
            self.assertEqual(
                policies,
                {(f"promote={promote}", f"suppress={suppress}")
                 for promote in (False, True) for suppress in (False, True)},
            )
        # Both a computed unsupported pattern and an unreachable bad pattern are
        # needed to distinguish fold eligibility from value-only comparisons.
        for site in ("regex_rlike", "regex_replace"):
            programs = [c["program"] for c in cases if f"focused/{site}/" in c["id"]]
            self.assertTrue(any("CONCAT('a', '(?=b)')" in p for p in programs))
            self.assertTrue(any("CASE WHEN false THEN 'a(?=b)' ELSE 'ab' END" in p for p in programs))

    def test_constant_view_warning_cases_have_no_unused_input_to_mask_them(self):
        cases = list(sql_cases())
        pure = [c for c in cases if c["id"].startswith("focused/value_selection/")]
        self.assertTrue(pure)
        for case in pure:
            self.assertNotIn("CREATE TABLE t(", case["program"])
        windows = [c for c in cases if c["id"].startswith("focused/rows/")]
        self.assertTrue(windows)
        for case in windows:
            self.assertIn("CREATE TABLE t(", case["program"])

    def test_map_comparison_ignores_entry_order_but_retains_association(self):
        one = {"type": "integer", "value": "1"}
        two = {"type": "integer", "value": "2"}
        null = {"type": "null"}
        left = {"type": "map", "value": [[one, two], [two, null]]}
        self.assertEqual(
            normalized(left),
            normalized({**left, "value": list(reversed(left["value"]))}),
        )
        self.assertNotEqual(
            normalized(left), normalized({**left, "value": [[one, null], [two, two]]})
        )
        self.assertNotEqual(
            normalized(left), normalized({**left, "value": left["value"] * 2})
        )

    def test_semantics_profiles_cannot_be_silently_substituted_or_omitted(self):
        require_semantics_profile([{"semantics_profile": "runtime-v1"}], "runtime-v1")
        for rows in ([{}], [{"semantics_profile": "legacy-exp-log-libm-0.2.15-v1"}]):
            with self.assertRaises(ValueError):
                require_semantics_profile(rows, "runtime-v1")

    def test_census_distinguishes_refusals_errors_and_unfolded_java_results(self):
        literal = {
            "kind": "LITERAL",
            "type": {"name": "INTEGER", "precision": 10, "scale": 0},
            "value": "7",
        }
        folded = {
            "case": "test",
            "expression": literal,
            "result": literal,
            "changed": False,
            "evaluator": "Janino",
        }
        result = census(
            [folded], [{"index": 0, "status": "unsupported", "reason": "test"}]
        )
        self.assertEqual(result["counts"]["unsupported"], 1)
        self.assertNotIn("equal", result["counts"])
        self.assertFalse(result["all_supported"])
        self.assertFalse(result["verdict_relevant_coverage_proven"])
        result = census(
            [folded], [{"index": 0, "status": "runtime_error", "reason": "test"}]
        )
        self.assertFalse(result["zero_comparable_differences"])
        self.assertEqual(result["counts"]["java_value_rust_error"], 1)
        folded["result"] = {"kind": "OTHER_FUNCTION", "type": literal["type"]}
        result = census(
            [folded],
            [
                {
                    "index": 0,
                    "status": "value",
                    "value": {"type": "integer", "value": "7"},
                }
            ],
        )
        self.assertEqual(result["counts"]["java_result_not_literal"], 1)
        self.assertNotIn("equal", result["counts"])

    def test_reference_reader_does_not_evaluate_casts(self):
        literal = {
            "kind": "LITERAL",
            "type": {"name": "INTEGER", "precision": 10, "scale": 0},
            "value": "7",
        }
        cast = {
            "kind": "CAST",
            "type": {**literal["type"], "nullable": True},
            "arguments": [literal],
        }
        self.assertEqual(reference(cast), {"type": "integer", "value": "7"})
        cast["type"]["name"] = "VARCHAR"
        self.assertIsNone(reference(cast))

    def test_equal_compiler_bugs_do_not_pass(self):
        requests = [{"id": "one"}]
        result = {
            "id": "one",
            "stage": "S2",
            "accepted": False,
            "compiler_bug": True,
            "schema": {},
            "diagnostics": [],
        }
        comparison = compare_compilations(requests, [result], [result])
        self.assertFalse(comparison["equal"])
        self.assertEqual(comparison["compiler_bugs"], ["one"])

    def test_positions_and_warning_promotions_are_hard_differences(self):
        requests = [{"id": "one"}]
        expected = {
            "id": "one",
            "stage": "S2",
            "accepted": True,
            "compiler_bug": False,
            "schema": {},
            "diagnostics": [{"warning": True, "start_column": 1}],
        }
        actual = copy.deepcopy(expected)
        actual["diagnostics"][0]["start_column"] = 2
        self.assertEqual(
            compare_compilations(requests, [expected], [actual])["differences"][0][
                "fields"
            ],
            ["diagnostics"],
        )
        actual["accepted"] = False
        actual["diagnostics"][0]["warning"] = False
        self.assertEqual(
            compare_compilations(requests, [expected], [actual])["differences"][0][
                "fields"
            ],
            ["accepted", "diagnostics"],
        )

    def test_missing_duplicate_and_malformed_records_fail(self):
        result = {
            "id": "one",
            "stage": "S2",
            "accepted": True,
            "compiler_bug": False,
            "schema": {},
            "diagnostics": [],
        }
        for requests, expected, actual in [
            ([], [], []),
            ([{"id": "one"}], [result], []),
            ([{"id": "one"}] * 2, [result] * 2, [result] * 2),
            ([{"id": "one"}], [result], [{**result, "accepted": 1}]),
            ([{"id": "one"}], [result], [{**result, "stage": "S4"}]),
        ]:
            with self.assertRaises(ValueError):
                compare_compilations(requests, expected, actual)

    def test_frontend_only_acceptance_cannot_certify_final_compilation(self):
        result = {
            "id": "one",
            "stage": "S2",
            "accepted": True,
            "compiler_bug": False,
            "schema": {},
            "diagnostics": [],
        }
        comparison = compare_compilations(
            [{"id": "one"}], [result], [{**result, "stage": "S3"}]
        )
        self.assertFalse(comparison["equal"])
        self.assertEqual(comparison["differences"][0]["fields"], ["stage"])

    def test_runtime_errors_are_not_interchangeable_with_refusals(self):
        requests = [{"id": "one"}]
        expected = [{"id": "one", "status": "runtime_error"}]
        actual = [{"index": 0, "status": "unsupported"}]
        self.assertFalse(compare_runtime(requests, expected, actual)["equal"])
        actual[0]["status"] = "runtime_error"
        self.assertTrue(compare_runtime(requests, expected, actual)["equal"])

    def test_preselected_cases_cover_sites_and_every_warning_policy(self):
        cases = list(sql_cases())
        self.assertEqual(len({case["id"] for case in cases}), len(cases))
        for site in (
            "tumble",
            "lead",
            "lag",
            "rows",
            "range",
            "limit",
            "offset",
            "warnings",
            "constant_view",
            "interval_cast_unrestricted",
            "lazy",
            "default_integer",
            "default_timestamp",
            "lateness_integer",
            "lateness_timestamp",
            "watermark_integer",
            "watermark_timestamp",
        ):
            subset = [
                case for case in cases if case["id"].startswith(f"focused/{site}/")
            ]
            self.assertTrue(subset)
            for promote in (False, True):
                for suppress in (False, True):
                    self.assertEqual(
                        sum(
                            case["id"].endswith(
                                f"promote={promote}/suppress={suppress}"
                            )
                            for case in subset
                        ),
                        len(subset) // 4,
                    )
        vectors = list(runtime_cases())
        self.assertEqual(len({v["id"] for v in vectors}), len(vectors))


if __name__ == "__main__":
    unittest.main()
