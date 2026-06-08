-- Stage 1: clean base rows.
CREATE VIEW multiple_views_unsupported_008_clean AS
SELECT id, nums, ts, code, m FROM base WHERE id > 0;

-- Stage 2: uses the UNSUPPORTED Spark feature `filter`.
CREATE VIEW multiple_views_unsupported_008_derived AS
SELECT id, filter(nums, x -> x > 3) AS kept
FROM multiple_views_unsupported_008_clean;

-- Stage 3: depends on the unsupported view's output.
CREATE VIEW multiple_views_unsupported_008_kept AS
SELECT id, kept FROM multiple_views_unsupported_008_derived WHERE id >= 1;

-- Stage 4: final output (depends on _kept).
CREATE VIEW multiple_views_unsupported_008 AS
SELECT id, kept FROM multiple_views_unsupported_008_kept;
