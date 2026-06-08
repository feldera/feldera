-- Stage 1: clean rows from the base table.
CREATE VIEW multiple_views_015_clean AS
SELECT emp_id AS id, dept AS dim, salary_k AS val
FROM employees
WHERE salary_k > 0;

-- Stage 2: keep rows above a threshold (depends on _clean).
CREATE VIEW multiple_views_015_filtered AS
SELECT id, dim, val FROM multiple_views_015_clean WHERE val >= 5;

-- Stage 3: aggregate per dimension (depends on _filtered).
CREATE VIEW multiple_views_015_agg AS
SELECT dim, MAX(val) AS agg_val, COUNT(*) AS n
FROM multiple_views_015_filtered GROUP BY dim;

-- Stage 4: enrich with the lookup table (depends on _agg).
CREATE VIEW multiple_views_015_enriched AS
SELECT a.dim AS dim, d.division AS label, a.agg_val AS agg_val, a.n AS n
FROM multiple_views_015_agg a JOIN departments d ON a.dim = d.dept;

-- Stage 5: final output (depends on _enriched).
CREATE VIEW multiple_views_015 AS
SELECT label, agg_val, n
FROM multiple_views_015_enriched
WHERE n >= 2;
