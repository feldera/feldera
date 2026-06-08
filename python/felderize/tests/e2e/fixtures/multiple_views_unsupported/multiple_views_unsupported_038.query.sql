-- Stage 1: clean base rows.
CREATE VIEW multiple_views_unsupported_038_clean AS
SELECT id, nums, ts, code, m FROM base WHERE id > 0;

-- Stage 2: uses the UNSUPPORTED Spark feature `luhn_check`.
CREATE VIEW multiple_views_unsupported_038_derived AS
SELECT id, luhn_check(code) AS is_valid
FROM multiple_views_unsupported_038_clean;

-- Stage 3: depends on the unsupported view's output.
CREATE VIEW multiple_views_unsupported_038_kept AS
SELECT id, is_valid FROM multiple_views_unsupported_038_derived WHERE id >= 1;

-- Stage 4: final output (depends on _kept).
CREATE VIEW multiple_views_unsupported_038 AS
SELECT id, is_valid FROM multiple_views_unsupported_038_kept;
