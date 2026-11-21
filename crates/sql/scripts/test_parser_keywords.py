import unittest

from parser_keywords import verify_reserved


class ReservedGrammarTests(unittest.TestCase):
    def test_missing_and_extra_keywords_fail_the_drift_gate(self):
        verify_reserved(["SELECT", "FROM"], {"SELECT", "FROM"})
        for actual in (["SELECT"], ["SELECT", "FROM", "NEW_KEYWORD"]):
            with self.assertRaisesRegex(ValueError, "differ from pinned grammar"):
                verify_reserved(actual, {"SELECT", "FROM"})

    def test_empty_duplicate_and_malformed_inventories_are_not_evidence(self):
        for actual in ([], None, {}, "SELECT", ["SELECT", "SELECT"], [42]):
            with self.assertRaisesRegex(ValueError, "Malformed"):
                verify_reserved(actual, {"SELECT"})
