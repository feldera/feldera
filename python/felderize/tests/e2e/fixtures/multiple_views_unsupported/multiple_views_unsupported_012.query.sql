-- Stage 1: clean base rows.
CREATE VIEW multiple_views_unsupported_012_clean AS
SELECT id, nums, ts, code, m FROM base WHERE id > 0;

-- Stage 2: uses the UNSUPPORTED Spark feature `exists`.
CREATE VIEW multiple_views_unsupported_012_derived AS
SELECT id, exists(nums, x -> x > 2) AS has_big
FROM multiple_views_unsupported_012_clean;

-- Stage 3: depends on the unsupported view's output.
CREATE VIEW multiple_views_unsupported_012_kept AS
SELECT id, has_big FROM multiple_views_unsupported_012_derived WHERE id >= 1;

-- Stage 4: final output (depends on _kept).
CREATE VIEW multiple_views_unsupported_012 AS
SELECT id, has_big FROM multiple_views_unsupported_012_kept;
