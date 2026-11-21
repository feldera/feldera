import tempfile
import unittest
from pathlib import Path

from java_runtime_contract_inventory import method_anchor


class RuntimeSourceAnchorTests(unittest.TestCase):
    def test_inherited_test_binds_defining_source_and_subclass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "example").mkdir()
            (root / "example/Parent.java").write_text("class Parent {\n void testFailure() {}\n}\n")
            (root / "example/Child.java").write_text("class Child extends Parent {}\n")
            hashes = {}
            self.assertEqual(method_anchor(root, "example.Child", "testFailure", hashes),
                             ("example/Parent.java", 2))
            self.assertEqual(set(hashes), {"example/Child.java", "example/Parent.java"})

    def test_ambiguous_method_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "example").mkdir()
            (root / "example/Test.java").write_text("class Test { void check() {} void check(int n) {} }")
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                method_anchor(root, "example.Test", "check", {})


if __name__ == "__main__":
    unittest.main()
