import copy
import tempfile
import unittest
from pathlib import Path

from s4_fields import audit, manual_composite_sources


class FieldInventoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "Pass.java").write_text("class Pass {}\n")
        (self.root / "Child.java").write_text("class Child {}\n")
        field = {"declaring_class": "Parent", "name": "state", "type": "int",
                 "static": False, "source": "Pass.java"}
        shadow = dict(field, declaring_class="Child")
        self.inventory = {
            "schema_version": 2,
            "implementations": {"Child": {"fields": [field, shadow], "field_lifetimes_reviewed": False}},
            "ambient_fields": {},
            "configurations": [{"points": [{"implementation": "Child", "path": [{"pass": "Child"}]}]}],
        }

    def test_hidden_fields_are_distinct_and_do_not_certify_lifetimes(self):
        report = audit(self.inventory, self.root)
        self.assertEqual(report["distinct_declarations"], 2)
        self.assertEqual(report["unique_paths"], 1)
        self.assertFalse(report["field_lifetimes_reviewed"])
        self.assertFalse(report["P0_complete"])

    def test_missing_contract_or_duplicate_declaration_fails(self):
        invalid = copy.deepcopy(self.inventory)
        invalid["implementations"].clear()
        with self.assertRaisesRegex(ValueError, "no field contract"):
            audit(invalid, self.root)
        invalid = copy.deepcopy(self.inventory)
        fields = invalid["implementations"]["Child"]["fields"]
        fields.append(fields[0])
        with self.assertRaisesRegex(ValueError, "duplicate field"):
            audit(invalid, self.root)

    def test_inventory_cannot_claim_review_and_must_pin_existing_sources(self):
        invalid = copy.deepcopy(self.inventory)
        invalid["implementations"]["Child"]["field_lifetimes_reviewed"] = True
        with self.assertRaisesRegex(ValueError, "cannot certify"):
            audit(invalid, self.root)
        invalid = copy.deepcopy(self.inventory)
        invalid["implementations"]["Child"]["fields"][0]["source"] = "missing.java"
        with self.assertRaisesRegex(ValueError, "missing declaration source"):
            audit(invalid, self.root)

    def test_manual_points_pin_local_state_and_reject_source_drift(self):
        (self.root / "Composite.java").write_text(
            'this.compiler.observeCircuit(this.getName(), 0, "Order", circuit);\n'
        )
        point = {"source": "Composite.java", "index": 0, "pass": "Order",
                 "kind": "in_place", "implementation": None, "fields": [],
                 "local_and_shared_state": "ordered dependency edges", "field_lifetimes_reviewed": False}
        self.inventory["manual_points"] = [point]
        self.inventory["manual_composites_included"] = True
        report = audit(self.inventory, self.root)
        self.assertEqual(report["manual_points"], 1)
        self.assertIn("Composite.java", report["source_hashes"])
        (self.root / "Composite.java").write_text(
            'this.observeCircuit(this.getName(), 1, "Order", circuit);\n'
        )
        with self.assertRaisesRegex(ValueError, "differ from source"):
            audit(self.inventory, self.root)

    def test_new_manual_composite_and_unpinned_fieldless_implementation_fail(self):
        self.inventory["manual_composites_included"] = True
        with self.assertRaisesRegex(ValueError, "coverage is incomplete"):
            audit(self.inventory, self.root)
        self.inventory["manual_composites_included"] = False
        self.inventory["implementations"]["Child"]["fields"] = []
        (self.root / "Child.java").unlink()
        with self.assertRaisesRegex(ValueError, "missing implementation source"):
            audit(self.inventory, self.root)

    def test_unrecognized_call_shape_cannot_escape_source_coverage(self):
        for call in [
            'compiler.observeCircuit("NewComposite", 0, "Order", circuit)',
            'applyObservedPass(getName(), 0, 0, transform, circuit)',
            'this.compiler.observeCircuit (this.getName(), 0, "Order", circuit)',
        ]:
            with self.subTest(call=call):
                (self.root / "NewComposite.java").write_text(call)
                with self.assertRaisesRegex(ValueError, "unaccounted observation call"):
                    manual_composite_sources(self.root, {})

    def test_infrastructure_calls_are_pinned_and_checked(self):
        relative = "org/dbsp/sqlCompiler/compiler/visitors/outer/Repeat.java"
        path = self.root / relative
        path.parent.mkdir(parents=True)
        path.write_text('this.compiler.applyObservedPass("Repeat", 0, repeats, this.transform, circuit)')
        sources = {}
        self.assertEqual(manual_composite_sources(self.root, sources), set())
        self.assertIn(relative, sources)
        path.write_text(path.read_text().replace("repeats,", "0,"))
        with self.assertRaisesRegex(ValueError, "unaccounted observation call"):
            manual_composite_sources(self.root, {})


if __name__ == "__main__":
    unittest.main()
