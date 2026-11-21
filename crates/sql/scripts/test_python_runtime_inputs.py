import ast
import hashlib
import tempfile
import unittest
from pathlib import Path

from python_runtime_inputs import extract, literal_data


def parse_data(expression, extra=""):
    return ast.parse("class Fixture(TstTable):\n    def __init__(self):\n"
                     "        self.sql = 'CREATE TABLE t(x BIGINT)'\n"
                     f"        self.data = {expression}\n" + extra).body[0]


class PythonRuntimeInputTests(unittest.TestCase):
    def test_exact_values_preserve_large_integers_nulls_unicode_and_field_order(self):
        value = literal_data(parse_data("[{'b': 18446744073709551615, 'a': None, 's': '🐍', 'f': -57681.18}]"))
        self.assertEqual(value[0]["b"], 18446744073709551615)
        self.assertEqual(list(value[0]), ["b", "a", "s", "f"])
        self.assertIsNone(value[0]["a"])
        self.assertEqual(value[0]["s"], "🐍")
        self.assertEqual(value[0]["f"], -57681.18)

    def test_dynamic_and_non_json_data_are_rejected(self):
        for expression in ["make_data()", "[{'x': 1e309}]", "[{'x': (1, 2)}]", "[{1: 'x'}]", "[1]", "{'x': 1}"]:
            with self.subTest(expression=expression), self.assertRaises(ValueError):
                literal_data(parse_data(expression))

    def test_conditional_mutation_and_registration_overrides_are_rejected(self):
        for extra in ["        self.data.append({'x': 2})\n",
                      "        if True:\n            self.data = []\n",
                      "    def register(self, accumulator):\n        pass\n",
                      "        self.data = []\n"]:
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                literal_data(parse_data("[]", extra))

    def test_extraction_preserves_dormant_state_and_table_sequence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = "python/tests/runtime_aggtest/aggtst_base.py"
            cases = "python/tests/runtime_aggtest/suite/test_cases.py"
            texts = {
                base: "class TstAccumulator:\n    def run_tests(self, prefix):\n        pass\n",
                cases: "class table(TstTable):\n    def __init__(self):\n        self.sql = 'CREATE TABLE t(x BIGINT)'\n        self.data = [{'x': 9223372036854775807}]\n"
                       "class view(TstView):\n    def __init__(self):\n        self.sql = 'CREATE MATERIALIZED VIEW v AS SELECT x+1 FROM t'\n        self.expected_error = 'overflow'\n",
            }
            for path, text in texts.items():
                (root / path).parent.mkdir(parents=True, exist_ok=True)
                (root / path).write_text(text)
            identity = cases + ":view"
            inputs = {"source_hashes": {path: hashlib.sha256(text.encode()).hexdigest() for path, text in texts.items()},
                      "records": [{"id": identity, "active": True, "expected": "overflow",
                                   "sql": "CREATE TABLE t(x BIGINT);\n\n\nCREATE MATERIALIZED VIEW v AS SELECT x+1 FROM t;\n\n",
                                   "tables": [{"source": cases, "class": "table", "sql": "CREATE TABLE t(x BIGINT)"}]}]}
            results = [{"id": identity, "circuit_produced": True, "compiler_rejected": False, "options": {}}]
            report = extract(inputs, results, root)
            self.assertFalse(report["normal_suite_executes_negative_views"])
            self.assertFalse(report["runtime_executed"])
            self.assertEqual(report["records"][0]["table_sequence"], [cases + ":table"])
            self.assertEqual(report["fixtures"][0]["records"], [{"x": 9223372036854775807}])
            with self.assertRaisesRegex(ValueError, "different identities"):
                extract(inputs, [], root)
            (root / cases).write_text(texts[cases] + "\n")
            with self.assertRaisesRegex(ValueError, "differs from pinned"):
                extract(inputs, results, root)


if __name__ == "__main__":
    unittest.main()
