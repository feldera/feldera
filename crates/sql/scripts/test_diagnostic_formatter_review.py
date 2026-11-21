import copy
import json
from pathlib import Path
import unittest

from diagnostic_formatter_review import part_of, render, validate


REVIEW = Path(__file__).resolve().parents[1] / "diagnostic-formatter-dispositions.json"


class FormatterReviewTests(unittest.TestCase):
    def setUp(self):
        self.review = json.loads(REVIEW.read_text())

    def test_all_recorded_bodies_reconstruct(self):
        result = validate(self.review)
        self.assertEqual(result["reconstructed_bodies"], 30)
        self.assertEqual(result["reviewed_ambiguous_bodies"], 12)
        self.assertIsNone(result["required_template_count"])

    def test_corrupted_body_or_digest_fails(self):
        for field in ("message", "sha256"):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.review)
                changed["bodies"][0][field] += "changed"
                with self.assertRaises(ValueError):
                    validate(changed)

    def test_primitive_and_boxed_booleans_remain_distinct(self):
        args = {"field": "headers", "value": "yes"}
        primitive = render("config.boolean_primitive", args, {})
        boxed = render("config.boolean_boxed", args, {})
        self.assertIn('`boolean` from String "yes"', primitive)
        self.assertIn('"True"/"TRUE"', primitive)
        self.assertIn('`java.lang.Boolean` from String "yes"', boxed)
        self.assertNotIn('"True"', boxed)

    def test_quantity_part_policy_and_field_path(self):
        self.assertEqual(part_of("forever", "forever", "duration"), "'forever'")
        self.assertEqual(part_of(" s", "30 s", "duration"), "' s' in duration '30 s'")
        result = render("quantity.unknown_unit", {
            "path": "connection_config/connection_timeout", "kind": "duration",
            "unit": " s", "text": "30 s",
        }, {})
        self.assertTrue(result.startswith('"connection_config.connection_timeout": unknown unit'))

    def test_enum_identity_is_selected_by_input(self):
        args = {"field": "mode", "value": "batch", "enum": "IcebergIngestMode"}
        result = render("config.enum_invalid", args, self.review["enums"])
        self.assertEqual(result, 'field "mode": invalid value "batch"; valid values are: '
                                '"snapshot", "follow", "snapshot_and_follow"')
        args["enum"] = "unreviewed_enum"
        with self.assertRaises(KeyError):
            render("config.enum_invalid", args, self.review["enums"])

    def test_candidate_accounting_fails_closed(self):
        changed = copy.deepcopy(self.review)
        changed["ambiguous_bodies"][0]["candidate_ids"].append("unknown-candidate")
        with self.assertRaises(ValueError):
            validate(changed)
        with self.assertRaises(ValueError):
            render("unreviewed", {}, {})


if __name__ == "__main__":
    unittest.main()
