-- Stage 1: clean base rows.
CREATE VIEW multiple_views_unsupported_050_clean AS
SELECT id, nums, ts, code, m FROM base WHERE id > 0;

-- Stage 2: uses the UNSUPPORTED Spark feature `transform_values`.
CREATE VIEW multiple_views_unsupported_050_derived AS
SELECT id, transform_values(m, (k, val) -> val * 5) AS scaled
FROM multiple_views_unsupported_050_clean;

-- Stage 3: depends on the unsupported view's output.
CREATE VIEW multiple_views_unsupported_050_kept AS
SELECT id, scaled FROM multiple_views_unsupported_050_derived WHERE id >= 1;

-- Stage 4: final output (depends on _kept).
CREATE VIEW multiple_views_unsupported_050 AS
SELECT id, scaled FROM multiple_views_unsupported_050_kept;
