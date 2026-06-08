-- Stage 1: clean base rows.
CREATE VIEW multiple_views_unsupported_018_clean AS
SELECT id, nums, ts, code, m FROM base WHERE id > 0;

-- Stage 2: uses the UNSUPPORTED Spark feature `aggregate`.
CREATE VIEW multiple_views_unsupported_018_derived AS
SELECT id, aggregate(nums, 3, (acc, x) -> acc + x) AS total
FROM multiple_views_unsupported_018_clean;

-- Stage 3: depends on the unsupported view's output.
CREATE VIEW multiple_views_unsupported_018_kept AS
SELECT id, total FROM multiple_views_unsupported_018_derived WHERE id >= 1;

-- Stage 4: final output (depends on _kept).
CREATE VIEW multiple_views_unsupported_018 AS
SELECT id, total FROM multiple_views_unsupported_018_kept;
