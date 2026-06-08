-- Stage 1: clean base rows.
CREATE VIEW multiple_views_unsupported_004_clean AS
SELECT id, nums, ts, code, m FROM base WHERE id > 0;

-- Stage 2: uses the UNSUPPORTED Spark feature `transform`.
CREATE VIEW multiple_views_unsupported_004_derived AS
SELECT id, transform(nums, x -> x + 4) AS bumped
FROM multiple_views_unsupported_004_clean;

-- Stage 3: depends on the unsupported view's output.
CREATE VIEW multiple_views_unsupported_004_kept AS
SELECT id, bumped FROM multiple_views_unsupported_004_derived WHERE id >= 1;

-- Stage 4: final output (depends on _kept).
CREATE VIEW multiple_views_unsupported_004 AS
SELECT id, bumped FROM multiple_views_unsupported_004_kept;
