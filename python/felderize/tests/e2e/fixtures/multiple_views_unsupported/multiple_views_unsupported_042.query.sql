-- Stage 1: clean base rows.
CREATE VIEW multiple_views_unsupported_042_clean AS
SELECT id, nums, ts, code, m FROM base WHERE id > 0;

-- Stage 2: uses the UNSUPPORTED Spark feature `map_filter`.
CREATE VIEW multiple_views_unsupported_042_derived AS
SELECT id, map_filter(m, (k, val) -> val > 2) AS pos
FROM multiple_views_unsupported_042_clean;

-- Stage 3: depends on the unsupported view's output.
CREATE VIEW multiple_views_unsupported_042_kept AS
SELECT id, pos FROM multiple_views_unsupported_042_derived WHERE id >= 1;

-- Stage 4: final output (depends on _kept).
CREATE VIEW multiple_views_unsupported_042 AS
SELECT id, pos FROM multiple_views_unsupported_042_kept;
