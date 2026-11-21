import tempfile
import unittest
from pathlib import Path

from python_diagnostic_inputs import extract


class PythonDiagnosticInputTests(unittest.TestCase):
    def test_imported_tables_and_disabled_prefixes_are_preserved_without_imports(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "python/tests/runtime_aggtest"
            suite = base / "suite"
            suite.mkdir(parents=True)
            (base / "aggtst_base.py").write_text("APPEND_ONLY = False\n")
            (base / "atest_register_tests.py").write_text("raise RuntimeError('never import')\n")
            (base / "atest_run.py").write_text("raise RuntimeError('never import')\n")
            (suite / "main.py").write_text("from test_cases import *\nrun('suite', 'active_')\n")
            source = """class active_table(TstTable):
    def __init__(self):
        self.sql = 'CREATE TABLE t(x INT)'
class active_view(TstView):
    def __init__(self):
        self.sql = 'CREATE VIEW v AS SELECT missing FROM t'
        self.expected_error = 'not found'
class ignored_view(TstView):
    def __init__(self):
        self.sql = 'CREATE VIEW v AS SELECT 1'
        self.expected_error = 'intentionally wrong'
"""
            path = suite / "test_cases.py"
            path.write_text(source)
            census = {"python": [
                {"path": str(path.relative_to(root)), "line": 7, "expected": "not found"},
                {"path": str(path.relative_to(root)), "line": 11, "expected": "intentionally wrong"},
            ]}
            report = extract(census, root)
            self.assertEqual((report["active"], report["disabled"]), (1, 1))
            self.assertEqual(report["records"][0]["sql"], "CREATE TABLE t(x INT);\n\n\nCREATE VIEW v AS SELECT missing FROM t;\n\n")
            self.assertFalse(report["python_tests_executed"])
            path.write_text(source.replace("self.sql = 'CREATE VIEW v AS SELECT 1'", "self.sql = make_query()"))
            with self.assertRaisesRegex(ValueError, "literal sql"):
                extract(census, root)


if __name__ == "__main__":
    unittest.main()
