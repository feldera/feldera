import unittest

from validate_live_optimizer import FROZEN_SCHEDULE, check_artifacts, verify_placements


BASE_COUNTS = {name: count for name, count in FROZEN_SCHEDULE.items() if count}


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
        verify_placements("classic", BASE_COUNTS, False)
        verify_placements("gen2", {**BASE_COUNTS, "Gen2Passes": 1}, True)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("missing conditional pass", BASE_COUNTS, True)

    def test_asof_retention_rebuilds_the_graph_before_moving_predicates(self):
        counts = {**BASE_COUNTS, "Graph": BASE_COUNTS["Graph"] + 1, "OrderGcDependencies": 1, "MoveGC": 1}
        verify_placements("asof retention", counts, False, asof_retention_moves=1)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("missing second graph", {**counts, "Graph": BASE_COUNTS["Graph"]},
                              False, asof_retention_moves=1)

    def test_disabling_star_joins_skips_the_graph_and_replacement(self):
        counts = BASE_COUNTS.copy()
        del counts["ReplaceStarJoins"]
        counts["Graph"] -= 1
        verify_placements("disabled star joins", counts, False, star_joins_enabled=False)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("skipped star joins", counts, False)

    def test_trim_inputs_runs_only_for_programs_with_the_option(self):
        enabled = {**BASE_COUNTS, "TrimInputs": 1}
        verify_placements("trim enabled", enabled, False, trim_inputs=True)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("trim skipped", BASE_COUNTS, False, trim_inputs=True)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("trim disabled", enabled, False)

    def test_missing_repeated_placement_is_detected_even_if_s3_matches(self):
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("missing final dead-code pass", {**BASE_COUNTS, "DeadCode": 4}, False)

    def test_fixed_point_uses_observed_java_iteration_counts(self):
        iterations = {"ExpandSafeCasts": 1, "ExpandUnsafeCasts": 3}
        counts = {**BASE_COUNTS, **iterations}
        verify_placements("casts", counts, False, cast_iterations=iterations)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("early convergence", {**counts, "ExpandUnsafeCasts": 2},
                              False, cast_iterations=iterations)

    def test_unused_fields_runs_cse_and_two_dead_code_passes_per_iteration(self):
        counts = {**BASE_COUNTS, "FindCommonProjections": 4, "ReplaceCommonProjections": 4, "CSE": FROZEN_SCHEDULE["CSE"] + 3,
                  "DeadCode": FROZEN_SCHEDULE["DeadCode"] + 6, "Graph": FROZEN_SCHEDULE["Graph"] + 6}
        verify_placements("three iterations", counts, False, unused_field_iterations=3)
        with self.assertRaisesRegex(ValueError, "Rust placements"):
            verify_placements("skipped nested pass", {**counts, "DeadCode": counts["DeadCode"] - 1},
                              False, unused_field_iterations=3)
