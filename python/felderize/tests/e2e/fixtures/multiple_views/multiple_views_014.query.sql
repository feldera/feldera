-- Stage 1: clean rows from the base table.
CREATE VIEW multiple_views_014_clean AS
SELECT emp_id AS id, dept AS dim, salary_k AS val
FROM employees
WHERE salary_k > 0;

-- Stage 2: keep rows above a threshold (depends on _clean).
CREATE VIEW multiple_views_014_filtered AS
SELECT id, dim, val FROM multiple_views_014_clean WHERE val >= 20;

-- Stage 3: aggregate per dimension (depends on _filtered).
CREATE VIEW multiple_views_014_agg AS
SELECT dim, SUM(val) AS agg_val, COUNT(*) AS n
FROM multiple_views_014_filtered GROUP BY dim;

-- Stage 4: enrich with the lookup table (depends on _agg).
CREATE VIEW multiple_views_014_enriched AS
SELECT a.dim AS dim, d.division AS label, a.agg_val AS agg_val, a.n AS n
FROM multiple_views_014_agg a JOIN departments d ON a.dim = d.dept;

-- Stage 5: final output (depends on _enriched).
CREATE VIEW multiple_views_014 AS
SELECT label, agg_val, n
FROM multiple_views_014_enriched
WHERE n >= 1;
