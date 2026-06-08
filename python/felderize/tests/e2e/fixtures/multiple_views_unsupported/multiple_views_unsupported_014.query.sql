-- Stage 1: clean base rows.
CREATE VIEW multiple_views_unsupported_014_clean AS
SELECT id, nums, ts, code, m FROM base WHERE id > 0;

-- Stage 2: uses the UNSUPPORTED Spark feature `exists`.
CREATE VIEW multiple_views_unsupported_014_derived AS
SELECT id, exists(nums, x -> x > 4) AS has_big
FROM multiple_views_unsupported_014_clean;

-- Stage 3: depends on the unsupported view's output.
CREATE VIEW multiple_views_unsupported_014_kept AS
SELECT id, has_big FROM multiple_views_unsupported_014_derived WHERE id >= 1;

-- Stage 4: final output (depends on _kept).
CREATE VIEW multiple_views_unsupported_014 AS
SELECT id, has_big FROM multiple_views_unsupported_014_kept;
