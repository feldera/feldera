-- Stage 1: clean base rows.
CREATE VIEW multiple_views_unsupported_030_clean AS
SELECT id, nums, ts, code, m FROM base WHERE id > 0;

-- Stage 2: uses the UNSUPPORTED Spark feature `quarter`.
CREATE VIEW multiple_views_unsupported_030_derived AS
SELECT id, quarter(ts) AS q
FROM multiple_views_unsupported_030_clean;

-- Stage 3: depends on the unsupported view's output.
CREATE VIEW multiple_views_unsupported_030_kept AS
SELECT id, q FROM multiple_views_unsupported_030_derived WHERE id >= 1;

-- Stage 4: final output (depends on _kept).
CREATE VIEW multiple_views_unsupported_030 AS
SELECT id, q FROM multiple_views_unsupported_030_kept;
