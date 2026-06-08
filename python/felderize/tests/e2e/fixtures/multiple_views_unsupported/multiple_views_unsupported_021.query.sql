-- Stage 1: clean base rows.
CREATE VIEW multiple_views_unsupported_021_clean AS
SELECT id, nums, ts, code, m FROM base WHERE id > 0;

-- Stage 2: uses the UNSUPPORTED Spark feature `zip_with`.
CREATE VIEW multiple_views_unsupported_021_derived AS
SELECT id, zip_with(nums, nums, (a, b) -> a + b + 1) AS combined
FROM multiple_views_unsupported_021_clean;

-- Stage 3: depends on the unsupported view's output.
CREATE VIEW multiple_views_unsupported_021_kept AS
SELECT id, combined FROM multiple_views_unsupported_021_derived WHERE id >= 1;

-- Stage 4: final output (depends on _kept).
CREATE VIEW multiple_views_unsupported_021 AS
SELECT id, combined FROM multiple_views_unsupported_021_kept;
