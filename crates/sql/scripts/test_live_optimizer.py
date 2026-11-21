import unittest

from validate_live_optimizer import FROZEN_SCHEDULE, check_artifacts, verify_placements


class FrozenPlacements(unittest.TestCase):
    def test_two_compilers_omitting_the_same_artifact_cannot_pass(self):
        for engine, missing in [("rust", "stubs.rs"), ("gen2", "circuit.json")]:
            files = {"dataflow.json": "a" * 64, missing: "b" * 64}
            if engine == "rust":
                files["circuit.rs"] = "c" * 64
            row = {"id": "sample", engine: files}
            check_artifacts(row)
            del files[missing]
            with self.assertRaisesRegex(ValueError, "Incomplete engine artifacts"):
                check_artifacts(row)

    def test_gen2_follows_each_case_options(self):
        verify_placements("classic", FROZEN_SCHEDULE, False)
        verify_placements("gen2", {**FROZEN_SCHEDULE, "Gen2Passes": 1}, True)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("missing conditional pass", FROZEN_SCHEDULE, True)

    def test_trim_inputs_runs_only_for_programs_with_the_option(self):
        enabled = {**FROZEN_SCHEDULE, "TrimInputs": 1}
        verify_placements("trim enabled", enabled, False, trim_inputs=True)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("trim skipped", FROZEN_SCHEDULE, False, trim_inputs=True)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("trim disabled", enabled, False)

    def test_missing_repeated_placement_is_detected_even_if_s3_matches(self):
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("missing final dead-code pass", {**FROZEN_SCHEDULE, "DeadCode": 4}, False)

    def test_fixed_point_uses_observed_java_iteration_counts(self):
        iterations = {"ExpandSafeCasts": 1, "ExpandUnsafeCasts": 3}
        counts = {**FROZEN_SCHEDULE, **iterations}
        verify_placements("casts", counts, False, cast_iterations=iterations)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("early convergence", {**counts, "ExpandUnsafeCasts": 2},
                              False, cast_iterations=iterations)

    def test_unused_fields_runs_cse_and_two_dead_code_passes_per_iteration(self):
        counts = {**FROZEN_SCHEDULE, "FindCommonProjections": 4, "CSE": FROZEN_SCHEDULE["CSE"] + 3,
                  "DeadCode": FROZEN_SCHEDULE["DeadCode"] + 6, "Graph": FROZEN_SCHEDULE["Graph"] + 6}
        verify_placements("three iterations", counts, False, unused_field_iterations=3)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("skipped nested pass", {**counts, "DeadCode": counts["DeadCode"] - 1},
                              False, unused_field_iterations=3)
