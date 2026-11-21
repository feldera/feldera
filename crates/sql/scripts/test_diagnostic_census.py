import ast
import tempfile
import unittest
from pathlib import Path

from diagnostic_census import (
    message_pattern,
    platform_expectation,
    python_expectations,
    string_value,
)


class DiagnosticCensusTests(unittest.TestCase):
    def test_message_format_quotes_and_arguments(self):
        pattern, _ = message_pattern("Column ''{0}'' at {1,number,#} is missing")
        self.assertIsNotNone(pattern.fullmatch("Column 'x' at 12 is missing"))
        self.assertIsNone(pattern.fullmatch("Column x at 12 is missing"))
        pattern, _ = message_pattern("Expected '{' then ''quoted''")
        self.assertIsNotNone(pattern.fullmatch("Expected { then 'quoted'"))
        with self.assertRaises(ValueError):
            message_pattern("{0,choice,0#none|1#{1}}")

    def test_only_literal_expectations_are_evaluated(self):
        parse = lambda text: ast.parse(text, mode="eval").body
        self.assertEqual(
            string_value(parse("'one ' + ('two' ' three')")), "one two three"
        )
        self.assertIsNone(string_value(parse("f'column {name}'")))
        self.assertIsNone(string_value(parse("dangerous()")))

    def test_inventory_distinguishes_forwarders_and_dynamic_expectations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tests = root / "python/tests/runtime_aggtest"
            tests.mkdir(parents=True)
            (tests / "example.py").write_text("""
self.expected_error = 'constant error'
expected_error = getattr(obj, 'expected_error', None)
self.expected_error = f'column {name}'
expected_error = None
""")
            rows, unresolved, forwarded = python_expectations(root)
            self.assertEqual([r["expected"] for r in rows], ["constant error"])
            self.assertEqual(len(unresolved), 1)
            self.assertEqual(len(forwarded), 1)

    def test_platform_preserves_exact_snippet_and_rejects_unknown_assertions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "python/tests/platform/negative_test.py"
            path.parent.mkdir(parents=True)
            source = '''
class NegativeCompilationTests:
    def test_sql_error(self):
        sql = "SELECT * FROM missing;"
        expected = f"""\nPipeline {pipeline_name} failed:\n  1|SELECT * FROM missing;\n                 ^^^^^^^""".strip()
        assert expected == got_err
'''
            path.write_text(source)
            row = platform_expectation(root)
            self.assertEqual(row["comparison"], "exact_after_strip")
            rendered = "".join(
                part.get("literal", "test-pipeline") for part in row["parts"]
            ).strip()
            self.assertEqual(
                rendered,
                "Pipeline test-pipeline failed:\n  1|SELECT * FROM missing;\n                 ^^^^^^^",
            )
            self.assertFalse(row["executed"])
            path.write_text(
                source.replace("expected == got_err", "expected in got_err")
            )
            with self.assertRaises(ValueError):
                platform_expectation(root)


if __name__ == "__main__":
    unittest.main()
