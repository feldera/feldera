-- Stage 1: clean base rows.
CREATE VIEW multiple_views_unsupported_035_clean AS
SELECT id, nums, ts, code, m FROM base WHERE id > 0;

-- Stage 2: uses the UNSUPPORTED Spark feature `to_number`.
CREATE VIEW multiple_views_unsupported_035_derived AS
SELECT id, to_number(code, '99999') AS num
FROM multiple_views_unsupported_035_clean;

-- Stage 3: depends on the unsupported view's output.
CREATE VIEW multiple_views_unsupported_035_kept AS
SELECT id, num FROM multiple_views_unsupported_035_derived WHERE id >= 1;

-- Stage 4: final output (depends on _kept).
CREATE VIEW multiple_views_unsupported_035 AS
SELECT id, num FROM multiple_views_unsupported_035_kept;
